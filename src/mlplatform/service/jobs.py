"""A small async job queue on Redis: idempotent submission, leases, crash resume.

Analysts ask for bulk work (export features for 500 entities as of a date)
that should not block an HTTP request. ``POST /jobs`` enqueues it and returns
at once; a worker process takes jobs off the queue.

Every state change is one Lua script, so it is atomic on the Redis server:

* **submit**: create the job hash only if it does not exist, and push it on
  the queue in the same script. Resubmitting the same key never queues it
  twice, never re-runs a finished job, and a crash cannot leave a job that
  exists but was never queued.
* **claim**: move the oldest key from the queue to the processing list and,
  only if the job is still ``queued``, mark it ``running`` with a worker id, a
  lease deadline and a fencing token (the attempt number).
* **heartbeat / complete / fail**: compare-and-set on state, worker id *and*
  token. A worker whose lease was reclaimed (even if the same job is later
  re-claimed by the same worker id) cannot extend, complete or fail the new
  attempt.
* **reclaim**: jobs in the processing list whose lease has passed go back to
  the queue (or to ``failed`` after ``max_attempts``). Atomic against
  ``complete``: whichever script runs first wins and the other is refused.

The worker renews its lease from a background thread every ``lease / 3``
while the handler runs, so a slow job is not stolen. A Redis error during a
renewal is logged and retried until the lease would run out. Renewals stop
when the worker dies, or when a job exceeds ``max_runtime_seconds`` (a hung
handler); the job is then reclaimed after the lease. Delivery is
at-least-once, so handlers must be idempotent (the built-in export is).
Invalid jobs (unknown kind, bad parameters) fail at once, without retries.

Finished jobs keep their record for ``done_ttl_seconds`` (7 days by default):
resubmitting within that window returns the stored result; after it the same
key can run again.

The scripts address job hashes by a prefix passed as an argument, which is
fine on a non-clustered Redis (the Terraform creates one) but not on Redis
Cluster. Lease times come from the Redis server clock (``TIME``), so workers
with skewed clocks agree; tests inject a clock instead, so they never sleep.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from collections.abc import Callable
from pathlib import Path

import redis

log = logging.getLogger(__name__)

Handler = Callable[[dict, str], dict]  # (params, job_key) -> result

JOB_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
ENTITY_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
MAX_EXPORT_ENTITIES = 1000


class InvalidJob(ValueError):
    """A job that can never succeed (unknown kind, bad parameters): fails without retries."""


def validate_job_key(key: str) -> str:
    if not isinstance(key, str) or not JOB_KEY_RE.fullmatch(key) or ".." in key:
        raise InvalidJob("job_key must match [A-Za-z0-9][A-Za-z0-9._-]{0,127} and must not contain '..'")
    return key


def validate_feature_export(params: dict) -> dict:
    from ..pit import to_us

    if not isinstance(params, dict) or set(params) - {"as_of", "entities", "features"}:
        raise InvalidJob("params must be an object with as_of, entities and optional features")
    try:
        to_us(params["as_of"])
    except Exception as exc:
        raise InvalidJob("params.as_of must be an ISO-8601 timestamp") from exc
    ents = params.get("entities")
    if not isinstance(ents, list) or not 1 <= len(ents) <= MAX_EXPORT_ENTITIES:
        raise InvalidJob(f"params.entities must be a list of 1 to {MAX_EXPORT_ENTITIES} ids")
    if not all(isinstance(e, str) and ENTITY_RE.fullmatch(e) for e in ents):
        raise InvalidJob("params.entities must be ids matching [A-Za-z0-9._-]{1,64}")
    feats = params.get("features")
    if feats is not None:
        from ..features import ALL_FEATURES

        if not (isinstance(feats, list) and feats and all(isinstance(f, str) for f in feats)):
            raise InvalidJob("params.features must be a non-empty list of names")
        unknown = sorted(set(feats) - set(ALL_FEATURES))
        if unknown:
            raise InvalidJob(f"unknown feature(s): {', '.join(unknown)}")
    return params


VALIDATORS: dict[str, Callable[[dict], dict]] = {"feature_export": validate_feature_export}


def job_key_for(kind: str, params: dict) -> str:
    canonical = json.dumps({"kind": kind, "params": params}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:24]


# KEYS: job hash, queue.  ARGV: key, kind, params json, submitted_at
_NOW = """local function now_or_time(v)
  local n = tonumber(v)
  if n then return n end
  local t = redis.call('TIME')
  return tonumber(t[1]) + tonumber(t[2]) / 1000000
end
"""

_SUBMIT = (
    _NOW
    + """
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'key', ARGV[1], 'kind', ARGV[2], 'params', ARGV[3],
           'state', 'queued', 'attempts', 0, 'submitted_at', now_or_time(ARGV[4]))
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
"""
)

# KEYS: queue, processing.  ARGV: job prefix, worker, now, lease seconds
_CLAIM = (
    _NOW
    + """
while true do
  local key = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
  if not key then return false end
  local job = ARGV[1] .. key
  if redis.call('HGET', job, 'state') == 'queued' then
    local attempts = redis.call('HINCRBY', job, 'attempts', 1)
    redis.call('HSET', job, 'state', 'running', 'worker', ARGV[2],
               'lease_until', now_or_time(ARGV[3]) + tonumber(ARGV[4]))
    return {key, attempts}
  end
  redis.call('LREM', KEYS[2], 1, key)  -- the entry just moved: never run a job that is not queued
end
"""
)

_CAS = (
    _NOW
    + """
local s = redis.call('HMGET', KEYS[1], 'state', 'worker', 'attempts')
if s[1] ~= 'running' or s[2] ~= ARGV[1] or s[3] ~= ARGV[2] then return 0 end
"""
)

# KEYS: job hash.  ARGV: worker, token, now, lease seconds
_HEARTBEAT = (
    _CAS
    + """
redis.call('HSET', KEYS[1], 'lease_until', now_or_time(ARGV[3]) + tonumber(ARGV[4]))
return 1
"""
)

# KEYS: job hash, processing.  ARGV: worker, token, key, result json, now, ttl
_COMPLETE = (
    _CAS
    + """
redis.call('HSET', KEYS[1], 'state', 'done', 'result', ARGV[4], 'finished_at', now_or_time(ARGV[5]))
redis.call('HDEL', KEYS[1], 'lease_until')
redis.call('LREM', KEYS[2], 0, ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[6])
return 1
"""
)

# KEYS: job hash, processing, queue.  ARGV: worker, token, key, error, max attempts, ttl, terminal (1/0)
_FAIL = (
    _CAS
    + """
if redis.call('LREM', KEYS[2], 0, ARGV[3]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'last_error', ARGV[4])
if ARGV[7] == '1' or tonumber(s[3]) >= tonumber(ARGV[5]) then
  redis.call('HSET', KEYS[1], 'state', 'failed')
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  return 2
end
redis.call('HSET', KEYS[1], 'state', 'queued')
redis.call('HDEL', KEYS[1], 'worker', 'lease_until')
redis.call('RPUSH', KEYS[3], ARGV[3])  -- the right end is claimed next
return 1
"""
)

# KEYS: processing, queue.  ARGV: job prefix, now, max attempts, ttl
_RECLAIM = (
    _NOW
    + """
local now = now_or_time(ARGV[2])
local moved = {}
for _, key in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
  local job = ARGV[1] .. key
  local s = redis.call('HMGET', job, 'state', 'lease_until', 'attempts', 'worker')
  if s[1] ~= 'running' then
    redis.call('LREM', KEYS[1], 0, key)
  elseif tonumber(s[2]) <= now then
    redis.call('LREM', KEYS[1], 0, key)
    redis.call('HSET', job, 'last_error', 'lease expired (worker ' .. s[4] .. ')')
    if tonumber(s[3]) >= tonumber(ARGV[3]) then
      redis.call('HSET', job, 'state', 'failed')
      redis.call('EXPIRE', job, ARGV[4])
    else
      redis.call('HSET', job, 'state', 'queued')
      redis.call('HDEL', job, 'worker', 'lease_until')
      redis.call('RPUSH', KEYS[2], key)
    end
    table.insert(moved, key)
  end
end
return moved
"""
)


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


class JobQueue:
    def __init__(
        self,
        client: redis.Redis,
        prefix: str = "mlpi:jobs",
        lease_seconds: float = 120.0,
        max_attempts: int = 3,
        done_ttl_seconds: int = 7 * 86_400,
        clock: Callable[[], float] | None = None,
        kinds: set[str] | None = None,
    ):
        """``clock=None`` (production) uses the Redis server's clock inside the
        scripts. ``kinds`` (if given) limits which job kinds may be submitted."""
        self.r = client
        self.prefix = prefix
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.done_ttl_seconds = done_ttl_seconds
        self.clock = clock
        self.kinds = kinds
        self.queue = f"{prefix}:queue"
        self.processing = f"{prefix}:processing"
        self.job_prefix = f"{prefix}:job:"
        self._submit = client.register_script(_SUBMIT)
        self._claim = client.register_script(_CLAIM)
        self._heartbeat = client.register_script(_HEARTBEAT)
        self._complete = client.register_script(_COMPLETE)
        self._fail = client.register_script(_FAIL)
        self._reclaim = client.register_script(_RECLAIM)

    def _now(self):
        return self.clock() if self.clock is not None else ""

    def _job(self, key: str) -> str:
        return self.job_prefix + key

    def get(self, key: str) -> dict | None:
        raw = self.r.hgetall(self._job(key))
        if not raw:
            return None
        job = {_s(k): _s(v) for k, v in raw.items()}
        job["params"] = json.loads(job["params"])
        job["attempts"] = int(job["attempts"])
        for f in ("submitted_at", "finished_at", "lease_until"):
            if f in job:
                job[f] = float(job[f])
        if "result" in job:
            job["result"] = json.loads(job["result"])
        return job

    def submit(self, kind: str, params: dict, job_key: str | None = None) -> tuple[dict, bool]:
        """Return ``(job, created)``. ``created`` is False when the job already existed."""
        if self.kinds is not None and kind not in self.kinds:
            raise InvalidJob(f"unknown job kind {kind!r}")
        if kind in VALIDATORS:
            VALIDATORS[kind](params)
        key = validate_job_key(job_key) if job_key is not None else job_key_for(kind, params)
        created = bool(
            self._submit(
                keys=[self._job(key), self.queue],
                args=[key, kind, json.dumps(params, sort_keys=True), self._now()],
            )
        )
        return self.get(key), created

    def claim(self, worker_id: str) -> dict | None:
        """Atomically take the oldest queued job and lease it to ``worker_id``."""
        got = self._claim(
            keys=[self.queue, self.processing], args=[self.job_prefix, worker_id, self._now(), self.lease_seconds]
        )
        if not got:
            return None
        job = self.get(_s(got[0]))
        job["token"] = int(got[1])
        return job

    def heartbeat(self, key: str, worker_id: str, token: int) -> bool:
        return bool(self._heartbeat(keys=[self._job(key)], args=[worker_id, token, self._now(), self.lease_seconds]))

    def complete(self, key: str, worker_id: str, token: int, result: dict) -> bool:
        """False if the lease was lost (reclaimed); the result is then discarded."""
        return bool(
            self._complete(
                keys=[self._job(key), self.processing],
                args=[worker_id, token, key, json.dumps(result, sort_keys=True), self._now(), self.done_ttl_seconds],
            )
        )

    def fail(self, key: str, worker_id: str, token: int, error: str, terminal: bool = False) -> bool:
        """Requeue (or fail after max_attempts). ``terminal=True`` fails at once, no retry."""
        return bool(
            self._fail(
                keys=[self._job(key), self.processing, self.queue],
                args=[worker_id, token, key, error[:2000], self.max_attempts, self.done_ttl_seconds, int(terminal)],
            )
        )

    def reclaim_expired(self) -> list[str]:
        """Move jobs whose lease ran out (their worker died) back to the queue."""
        moved = self._reclaim(
            keys=[self.processing, self.queue],
            args=[self.job_prefix, self._now(), self.max_attempts, self.done_ttl_seconds],
        )
        return [_s(k) for k in moved]

    def depth(self) -> dict:
        return {"queued": self.r.llen(self.queue), "processing": self.r.llen(self.processing)}


def _default_wait(stop: threading.Event, seconds: float) -> bool:
    return stop.wait(seconds)


class Worker:
    """Runs jobs. ``clock`` (monotonic by default) only measures the heartbeat
    retry window and ``max_runtime_seconds``; lease times live in Redis."""

    def __init__(
        self,
        queue: JobQueue,
        handlers: dict[str, Handler],
        worker_id: str,
        heartbeat_interval: float | None = None,
        wait: Callable[[threading.Event, float], bool] = _default_wait,
        alive_file: str | Path | None = None,
        max_runtime_seconds: float = 3600.0,
        clock: Callable[[], float] | None = None,
    ):
        self.queue = queue
        self.handlers = handlers
        self.worker_id = worker_id
        self.heartbeat_interval = heartbeat_interval or queue.lease_seconds / 3
        self._wait = wait  # injectable so tests control when heartbeats happen
        self.alive_file = Path(alive_file) if alive_file else None
        self.max_runtime_seconds = max_runtime_seconds
        self.clock = clock or queue.clock or time.monotonic
        self.heartbeats = 0
        self.heartbeat_errors = 0
        self.lost_leases = 0
        self.overran = 0

    def touch_alive(self) -> None:
        if self.alive_file is not None:
            self.alive_file.touch()

    def _heartbeat_loop(self, key: str, token: int, stop: threading.Event) -> None:
        started = self.clock()
        renewed = started  # when the lease was last known good
        while not self._wait(stop, self.heartbeat_interval):
            now = self.clock()
            if now - started > self.max_runtime_seconds:
                # A hung handler: stop renewing (the job is reclaimed when the
                # lease runs out) and stop touching the alive file (liveness
                # restarts the pod). The handler thread cannot be killed safely.
                self.overran += 1
                log.error("job %s exceeded max_runtime_seconds=%s; giving up its lease", key, self.max_runtime_seconds)
                return
            try:
                ok = self.queue.heartbeat(key, self.worker_id, token)
            except redis.RedisError:
                self.heartbeat_errors += 1
                if now - renewed >= self.queue.lease_seconds:
                    log.error("could not renew the lease on job %s before it ran out", key)
                    return
                log.warning("heartbeat for job %s failed; retrying", key, exc_info=True)
                continue
            if not ok:
                self.lost_leases += 1
                log.warning("lost the lease on job %s", key)
                return
            renewed = now
            self.touch_alive()  # a long job inside max_runtime keeps the pod live
            self.heartbeats += 1  # counted after the touch, so a test can wait on it

    def run_once(self) -> str | None:
        """Reclaim expired leases, then run at most one job. Returns the job key run, if any."""
        self.touch_alive()
        self.queue.reclaim_expired()
        job = self.queue.claim(self.worker_id)
        if job is None:
            return None
        key, token = job["key"], job["token"]
        handler = self.handlers.get(job["kind"])
        if handler is None:
            self.queue.fail(key, self.worker_id, token, f"unknown job kind {job['kind']!r}", terminal=True)
            return key
        stop = threading.Event()
        beat = threading.Thread(target=self._heartbeat_loop, args=(key, token, stop), daemon=True)
        beat.start()
        try:
            result = handler(job["params"], key)
        except Exception as exc:  # recorded on the job; retried up to max_attempts unless it can never succeed
            log.exception("job %s failed", key)
            stop.set()
            beat.join()
            terminal = isinstance(exc, InvalidJob) or type(exc).__name__ == "UnknownFeature"
            self.queue.fail(key, self.worker_id, token, repr(exc), terminal=terminal)
            return key
        stop.set()
        beat.join()
        if not self.queue.complete(key, self.worker_id, token, result):
            log.warning("job %s finished after its lease was reclaimed; result discarded", key)
        return key

    def run_forever(self, idle_sleep: float = 1.0) -> None:  # pragma: no cover - process loop
        while True:
            if self.run_once() is None:
                time.sleep(idle_sleep)


def worker_alive(path: str | Path, max_age_seconds: float, now: float | None = None) -> bool:
    """Liveness for the worker pod: the loop or the heartbeat thread touched the file recently."""
    p = Path(path)
    if not p.exists():
        return False
    return ((now if now is not None else time.time()) - p.stat().st_mtime) <= max_age_seconds


def feature_export_handler(service, artifacts) -> Handler:
    """Built-in job: export as-of features for many entities to ``jobs/<key>/features.json``.

    Re-running it for the same job key writes the same file (idempotent)."""
    from ..pit import to_us
    from .feature_service import UnknownEntity

    def handle(params: dict, job_key: str) -> dict:
        validate_job_key(job_key)
        as_of_us = to_us(params["as_of"])
        out, unknown = {}, []
        for ent in params["entities"]:
            try:
                fv = service.get(ent, as_of_us, params.get("features")).to_dict()
            except UnknownEntity:
                unknown.append(ent)
                continue
            fv.pop("cache")  # cache hit/miss differs between runs; the content must not
            out[ent] = fv
        key = f"jobs/{job_key}/features.json"
        artifacts.write_json(key, {"as_of": params["as_of"], "features": out, "unknown_entities": unknown})
        return {"output_key": key, "entities": len(out), "unknown_entities": unknown}

    return handle

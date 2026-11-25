"""Async jobs: idempotent submission, crash resume, bounded retries (fakeredis, fake clock)."""

import threading

import pytest
import redis
from conftest import publish
from fastapi.testclient import TestClient

from mlplatform.pit import to_us
from mlplatform.service.app import create_feature_app
from mlplatform.service.cache import FeatureCache
from mlplatform.service.feature_service import FeatureService
from mlplatform.service.jobs import (
    InvalidJob,
    JobQueue,
    Worker,
    feature_export_handler,
    job_key_for,
    worker_alive,
)
from mlplatform.service.snapshots import SnapshotManager


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def queue(redis_client, clock):
    return JobQueue(redis_client, lease_seconds=60, max_attempts=3, clock=clock)


def counting_handler(calls):
    def handle(params, key):
        calls.append(key)
        return {"n": params["n"] * 2}

    return handle


def test_same_job_is_queued_once_and_never_rerun(queue):
    calls = []
    w = Worker(queue, {"double": counting_handler(calls)}, "w1")
    job, created = queue.submit("double", {"n": 2})
    again, created_again = queue.submit("double", {"n": 2})
    assert created and not created_again and job["key"] == again["key"]
    assert queue.depth() == {"queued": 1, "processing": 0}
    assert w.run_once() == job["key"]
    assert w.run_once() is None
    # Resubmitting a finished job returns it as done and queues nothing.
    done, created3 = queue.submit("double", {"n": 2})
    assert not created3 and done["state"] == "done" and done["result"] == {"n": 4}
    assert w.run_once() is None
    assert calls == [job["key"]]


def test_key_is_order_independent_and_caller_key_wins(queue):
    assert job_key_for("k", {"a": 1, "b": 2}) == job_key_for("k", {"b": 2, "a": 1})
    assert job_key_for("k", {"a": 1}) != job_key_for("k", {"a": 2})
    job, _ = queue.submit("double", {"n": 1}, job_key="my-key")
    assert job["key"] == "my-key"
    _, created = queue.submit("double", {"n": 999}, job_key="my-key")
    assert not created


def test_crash_resume_after_lease_expiry(queue, clock):
    calls = []
    job, _ = queue.submit("double", {"n": 5})
    crashed = queue.claim("w-crashed")  # claims, then the process dies: no complete, no heartbeat
    assert crashed["state"] == "running" and queue.depth() == {"queued": 0, "processing": 1}
    survivor = Worker(queue, {"double": counting_handler(calls)}, "w2")
    clock.t += 59
    assert survivor.run_once() is None  # lease still valid: not stolen early
    clock.t += 2
    assert survivor.run_once() == job["key"]
    final = queue.get(job["key"])
    assert final["state"] == "done" and final["worker"] == "w2" and final["attempts"] == 2
    assert "lease expired" in final["last_error"]
    assert queue.depth() == {"queued": 0, "processing": 0}
    # The crashed worker comes back late and tries to complete: refused, result unchanged.
    assert queue.complete(job["key"], "w-crashed", crashed["token"], {"n": -1}) is False
    assert queue.get(job["key"])["result"] == {"n": 10}


def test_heartbeat_keeps_the_lease(queue, clock):
    job, _ = queue.submit("double", {"n": 1})
    token = queue.claim("w1")["token"]
    for _ in range(3):
        clock.t += 50
        assert queue.heartbeat(job["key"], "w1", token)
        assert queue.reclaim_expired() == []
    assert not queue.heartbeat(job["key"], "someone-else", token)


def test_failures_retry_then_stop(queue):
    def boom(params, key):
        raise RuntimeError("bad input")

    w = Worker(queue, {"boom": boom}, "w1")
    job, _ = queue.submit("boom", {})
    for _ in range(3):
        assert w.run_once() == job["key"]
    final = queue.get(job["key"])
    assert final["state"] == "failed" and final["attempts"] == 3 and "bad input" in final["last_error"]
    assert w.run_once() is None
    assert queue.depth() == {"queued": 0, "processing": 0}


def test_repeated_crashes_end_in_failed(queue, clock):
    job, _ = queue.submit("double", {"n": 1})
    for i in range(3):
        assert queue.claim(f"w{i}") is not None
        clock.t += 61
        queue.reclaim_expired()
    assert queue.get(job["key"])["state"] == "failed"
    assert queue.claim("w9") is None


@pytest.fixture
def feature_stack(artifacts, small_records, redis_client, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "serving", refresh_seconds=0)
    service = FeatureService(snaps, FeatureCache(redis_client))
    yield service, artifacts
    snaps.close()


def test_api_and_worker_end_to_end(feature_stack, redis_client, clock):
    service, artifacts = feature_stack
    queue = JobQueue(redis_client, clock=clock)
    api = TestClient(create_feature_app(service, queue))
    body = {"kind": "feature_export", "params": {"as_of": "2023-03-01T22:00:00", "entities": ["SYN000", "NOPE"]}}
    r1 = api.post("/jobs", json=body)
    r2 = api.post("/jobs", json=body)
    assert (r1.status_code, r2.status_code) == (202, 200)
    assert r1.json()["job_key"] == r2.json()["job_key"] and r2.json()["created"] is False
    key = r1.json()["job_key"]
    assert api.get(f"/jobs/{key}").json()["state"] == "queued"

    handler = feature_export_handler(service, artifacts)
    worker = Worker(queue, {"feature_export": handler}, "w1")
    assert worker.run_once() == key
    job = api.get(f"/jobs/{key}").json()
    assert job["state"] == "done" and job["result"]["unknown_entities"] == ["NOPE"]
    out = artifacts.read_json(job["result"]["output_key"])
    direct = service.get("SYN000", to_us("2023-03-01T22:00:00")).to_dict()
    assert out["features"]["SYN000"]["features"] == direct["features"]
    # A re-run (as after a crash) writes byte-identical output.
    first = artifacts.read_bytes(job["result"]["output_key"])
    handler(body["params"], key)
    assert artifacts.read_bytes(job["result"]["output_key"]) == first

    assert api.post("/jobs", json={"kind": "rm -rf", "params": {}}).status_code == 400
    assert api.get("/jobs/unknown").status_code == 404


def test_jobs_disabled_without_redis(feature_stack):
    service, _ = feature_stack
    api = TestClient(create_feature_app(service))
    assert api.post("/jobs", json={"kind": "feature_export", "params": {}}).status_code == 503


# --- regression tests for the audit findings ---------------------------------


class SignallingQueue(JobQueue):
    """Lets a test wait for the next successful heartbeat."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.beat = threading.Event()

    def heartbeat(self, key, worker_id, token):
        ok = super().heartbeat(key, worker_id, token)
        if ok:
            self.beat.set()
        return ok


def test_slow_job_is_kept_alive_by_the_heartbeat_and_runs_once(redis_client, clock):
    """B3: a job longer than its lease must not be reclaimed and run a second time."""
    queue = SignallingQueue(redis_client, lease_seconds=60, clock=clock)
    tick = threading.Semaphore(0)

    def controlled_wait(stop, seconds):  # one heartbeat per tick released by the handler
        while not stop.is_set():
            if tick.acquire(timeout=0.05):
                return False
        return True

    calls, stolen = [], []
    other = Worker(queue, {"slow": lambda p, k: stolen.append(k) or {}}, "w-other")

    def slow(params, key):
        calls.append(key)
        for _ in range(4):  # 4 x 50 s = 200 s of fake time, more than three leases
            clock.t += 50
            queue.beat.clear()
            tick.release()
            assert queue.beat.wait(timeout=10), "heartbeat did not run"
            assert other.run_once() is None  # another worker finds nothing to steal
        return {"ok": True}

    w = Worker(queue, {"slow": slow}, "w-main", wait=controlled_wait)
    job, _ = queue.submit("slow", {})
    assert w.run_once() == job["key"]
    assert calls == [job["key"]] and stolen == []
    final = queue.get(job["key"])
    assert final["state"] == "done" and final["attempts"] == 1
    assert w.heartbeats == 4


class CountingRedis:
    """Wraps a client and records which commands a call sends."""

    def __init__(self, inner):
        self.inner, self.calls = inner, []

    def __getattr__(self, name):
        attr = getattr(self.inner, name)
        if callable(attr):

            def wrapped(*a, **k):
                self.calls.append(name)
                return attr(*a, **k)

            return wrapped
        return attr


def test_submit_is_a_single_atomic_script(redis_client, clock):
    """S1: create-and-enqueue is one server-side script, so a crash between
    the two steps cannot leave a job that exists but is never queued."""
    spy = CountingRedis(redis_client)
    queue = JobQueue(spy, clock=clock)
    queue._submit.registered_client = spy  # send the script call through the spy too
    spy.calls.clear()
    job, created = queue.submit("double", {"n": 1})
    assert created
    submit_calls = spy.calls[: spy.calls.index("hgetall")]  # everything before reading the job back
    assert submit_calls and set(submit_calls) <= {"evalsha", "eval", "script_load"}
    assert "set" not in spy.calls and "lpush" not in spy.calls and "hset" not in spy.calls
    assert redis_client.lrange(queue.queue, 0, -1) == [job["key"].encode()]


def test_reclaim_beats_a_late_complete_and_a_stale_heartbeat(queue, clock):
    """S1: after reclaim, the old attempt's token is dead even for the same worker id."""
    job, _ = queue.submit("double", {"n": 1})
    first = queue.claim("w1")
    clock.t += 61
    assert queue.reclaim_expired() == [job["key"]]
    second = queue.claim("w1")  # same worker id, new attempt
    assert second["token"] == first["token"] + 1
    lease = queue.get(job["key"])["lease_until"]
    clock.t += 5
    assert not queue.heartbeat(job["key"], "w1", first["token"])  # stale heartbeat refused
    assert queue.get(job["key"])["lease_until"] == lease
    assert not queue.complete(job["key"], "w1", first["token"], {"n": -1})
    assert not queue.fail(job["key"], "w1", first["token"], "late failure")
    assert queue.complete(job["key"], "w1", second["token"], {"n": 2})
    assert queue.get(job["key"])["result"] == {"n": 2}


def test_complete_first_then_reclaim_does_nothing(queue, clock):
    job, _ = queue.submit("double", {"n": 1})
    c = queue.claim("w1")
    assert queue.complete(job["key"], "w1", c["token"], {"n": 2})
    clock.t += 1000
    assert queue.reclaim_expired() == []
    assert queue.get(job["key"])["state"] == "done"


def test_fail_after_reclaim_is_refused(queue, clock):
    """S1: fail checks that it removed the job from the processing list."""
    job, _ = queue.submit("double", {"n": 1})
    c = queue.claim("w1")
    clock.t += 61
    queue.reclaim_expired()
    assert queue.fail(job["key"], "w1", c["token"], "boom") is False
    assert queue.get(job["key"])["state"] == "queued"
    assert queue.depth() == {"queued": 1, "processing": 0}


def test_claim_skips_entries_that_are_not_queued(queue, redis_client):
    job, _ = queue.submit("double", {"n": 1})
    c = queue.claim("w1")
    queue.complete(job["key"], "w1", c["token"], {"n": 2})
    redis_client.lpush(queue.queue, job["key"])  # a stray duplicate entry
    assert queue.claim("w2") is None
    assert queue.depth() == {"queued": 0, "processing": 0}


def test_finished_jobs_expire(queue, redis_client):
    job, _ = queue.submit("double", {"n": 1})
    c = queue.claim("w1")
    assert redis_client.ttl(queue._job(job["key"])) == -1  # no TTL while queued or running
    queue.complete(job["key"], "w1", c["token"], {"n": 2})
    assert 0 < redis_client.ttl(queue._job(job["key"])) <= 7 * 86_400


@pytest.mark.parametrize("bad", ["../../etc", "a/b", "..", "x..y", "", "a" * 129, "sp ace", "c:x\\y"])
def test_job_key_path_traversal_rejected(queue, bad):
    """S4: the job key becomes part of an artifact path; anything but a plain token is refused."""
    with pytest.raises(InvalidJob):
        queue.submit("double", {"n": 1}, job_key=bad)


def test_api_rejects_bad_keys_and_params(feature_stack, redis_client, clock):
    service, _ = feature_stack
    api = TestClient(create_feature_app(service, JobQueue(redis_client, clock=clock)))
    ok = {"as_of": "2023-03-01T22:00:00", "entities": ["SYN000"]}
    assert api.post("/jobs", json={"kind": "feature_export", "params": ok, "job_key": "../x"}).status_code == 422
    bad_params = [
        {},
        {"as_of": "nope", "entities": ["SYN000"]},
        {"as_of": ok["as_of"], "entities": []},
        {"as_of": ok["as_of"], "entities": ["../x"]},
        {**ok, "extra": 1},
    ]
    for params in bad_params:
        assert api.post("/jobs", json={"kind": "feature_export", "params": params}).status_code == 422, params
    assert api.get("/jobs/bad..key").status_code == 422
    assert redis_client.llen("mlpi:jobs:queue") == 0


def test_worker_alive_file(tmp_path, queue):
    f = tmp_path / "alive"
    assert not worker_alive(f, 60)
    Worker(queue, {}, "w1", alive_file=f).run_once()
    assert worker_alive(f, 60)
    assert not worker_alive(f, 60, now=f.stat().st_mtime + 61)


# --- round 2 audit ------------------------------------------------------------


class FlakyQueue(SignallingQueue):
    """The first ``fail_times`` heartbeats raise a Redis error."""

    def __init__(self, *a, fail_times=1, **k):
        super().__init__(*a, **k)
        self.fail_times = fail_times

    def heartbeat(self, key, worker_id, token):
        if self.fail_times > 0:
            self.fail_times -= 1
            self.beat.set()
            raise redis.ConnectionError("blip")
        return super().heartbeat(key, worker_id, token)


def ticking_wait(tick):
    def wait(stop, seconds):
        while not stop.is_set():
            if tick.acquire(timeout=0.05):
                return False
        return True

    return wait


def step(queue, tick, clock, seconds):
    clock.t += seconds
    queue.beat.clear()
    tick.release()
    assert queue.beat.wait(timeout=10), "heartbeat did not run"


def test_heartbeat_survives_a_redis_error(redis_client, clock):
    queue = FlakyQueue(redis_client, lease_seconds=60, clock=clock, fail_times=1)
    tick = threading.Semaphore(0)
    other = Worker(queue, {"slow": lambda p, k: {}}, "w-other")

    def slow(params, key):
        step(queue, tick, clock, 20)  # this renewal hits a Redis error
        step(queue, tick, clock, 20)  # the next one succeeds
        step(queue, tick, clock, 50)
        assert other.run_once() is None  # 90 s in, lease still held
        return {"ok": True}

    w = Worker(queue, {"slow": slow}, "w-main", wait=ticking_wait(tick))
    job, _ = queue.submit("slow", {})
    assert w.run_once() == job["key"]
    assert (w.heartbeat_errors, w.heartbeats) == (1, 2)
    assert queue.get(job["key"])["state"] == "done"


def test_heartbeat_gives_up_once_the_lease_has_run_out(redis_client, clock):
    queue = FlakyQueue(redis_client, lease_seconds=60, clock=clock, fail_times=99)
    tick = threading.Semaphore(0)

    def slow(params, key):
        for _ in range(3):  # errors at 20 and 40 s are retried; at 60 s the lease is gone
            step(queue, tick, clock, 20)
        queue.beat.clear()
        tick.release()
        assert not queue.beat.wait(timeout=0.5)  # the heartbeat thread has stopped
        return {"ok": True}

    w = Worker(queue, {"slow": slow}, "w-main", wait=ticking_wait(tick))
    queue.submit("slow", {})
    w.run_once()
    assert w.heartbeat_errors == 3 and w.heartbeats == 0


def test_hung_handler_loses_its_lease_after_max_runtime(redis_client, clock, tmp_path):
    queue = SignallingQueue(redis_client, lease_seconds=60, clock=clock)
    tick = threading.Semaphore(0)
    alive = tmp_path / "alive"
    stolen = []
    other = Worker(queue, {"hang": lambda p, k: stolen.append(k) or {}}, "w-other")

    def hang(params, key):
        step(queue, tick, clock, 50)  # renewed at 50 s
        step(queue, tick, clock, 50)  # renewed at 100 s
        for _ in range(200):  # wait until the thread has also touched the alive file
            if w.heartbeats == 2:
                break
            threading.Event().wait(0.05)
        touched = alive.stat().st_mtime_ns
        clock.t += 50  # 150 s > max_runtime: the next tick gives up instead of renewing
        tick.release()
        for _ in range(200):
            if w.overran:
                break
            threading.Event().wait(0.05)
        assert w.overran == 1
        assert alive.stat().st_mtime_ns == touched  # the alive file is no longer refreshed
        clock.t += 61  # lease (renewed at 100 s) has run out
        assert other.run_once() == key  # reclaimed and run elsewhere
        return {"late": True}

    w = Worker(queue, {"hang": hang}, "w-main", wait=ticking_wait(tick), alive_file=alive, max_runtime_seconds=120)
    job, _ = queue.submit("hang", {})
    w.run_once()
    final = queue.get(job["key"])
    assert stolen == [job["key"]] and final["worker"] == "w-other" and final["result"] == {}


def test_leases_use_the_redis_clock_without_an_injected_clock(redis_client):
    queue = JobQueue(redis_client, lease_seconds=60)  # production default: clock=None
    queue.submit("double", {"n": 1})
    job = queue.claim("w1")
    server_now = float(redis_client.time()[0])
    assert server_now + 50 < job["lease_until"] <= server_now + 61


def test_unknown_kind_fails_at_once_without_retries(queue):
    w = Worker(queue, {}, "w1")
    job, _ = queue.submit("mystery", {})
    assert w.run_once() == job["key"]
    final = queue.get(job["key"])
    assert final["state"] == "failed" and final["attempts"] == 1
    assert w.run_once() is None


def test_submit_rejects_unknown_kinds_and_features(redis_client, clock):
    queue = JobQueue(redis_client, clock=clock, kinds={"feature_export"})
    with pytest.raises(InvalidJob):
        queue.submit("mystery", {})
    with pytest.raises(InvalidJob, match="unknown feature"):
        queue.submit("feature_export", {"as_of": "2023-01-01", "entities": ["SYN000"], "features": ["nope"]})
    assert queue.depth() == {"queued": 0, "processing": 0}


@pytest.mark.parametrize("bad", ["-x", ".x", "_x"])
def test_job_key_must_start_with_a_letter_or_digit(queue, bad):
    with pytest.raises(InvalidJob):
        queue.submit("double", {"n": 1}, job_key=bad)


def test_job_queue_has_its_own_redis_client_with_longer_timeouts():
    from mlplatform.service.app import Settings, build_feature_service, build_job_queue

    s = Settings(redis_url="redis://127.0.0.1:1/0")
    q = build_job_queue(s)
    cache = build_feature_service(s).cache.client
    assert q.r is not cache
    assert q.r.connection_pool.connection_kwargs["socket_timeout"] == 5.0
    assert cache.connection_pool.connection_kwargs["socket_timeout"] == 0.2
    assert q.kinds == {"feature_export"}

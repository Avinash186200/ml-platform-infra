# Feature service: point in time, caching, async jobs

## The as-of rule

Every stored value is a record `(entity, feature, event_time, available_at, value)`:

- `event_time`: what the value describes (the close of 2024-03-28; the quarter ending 2024-03-31).
- `available_at`: when it could first be known (the close is published 15 minutes after the bell; the quarterly score 25 to 45 days after the quarter ends; a restatement later still).

As of time `t`:

1. Keep only records with `available_at <= t` (inclusive: a value published at exactly `t` is known at `t`).
2. Per feature, take the record with the latest `event_time`.
3. If several records share that `event_time` (restatements), take the latest `available_at`.

Rule 2 before rule 3 matters. If quarter 1 is restated after quarter 2 was first reported, the restatement has the newest `available_at` but an older `event_time`; a naive "latest published row" query would return quarter 1. The rule returns quarter 2.

The SQL in `store.py`:

```sql
SELECT feature, value, event_time, available_at FROM (
    SELECT ..., ROW_NUMBER() OVER (PARTITION BY feature
                                   ORDER BY event_time DESC, available_at DESC) AS rn
    FROM feature_records
    WHERE entity = ? AND available_at <= ? AND feature IN (...)
) WHERE rn = 1
```

with an index on `(entity, feature, available_at)`. The training pipeline uses a vectorised version (`pit.asof_join`): sort a feature's records by `(event_time, available_at)`, walk them in `available_at` order keeping a running maximum of that rank per entity, and `merge_asof` the query times onto it. Both are tested against a plain loop (`pit.asof_bruteforce`) with hypothesis-generated records and queries, including queries exactly at and 1 µs before each publication.

### How look-ahead is prevented, end to end

- **Ingestion.** Raw data is cut at the run date by publication time, so a restatement published after the run does not exist in that run.
- **Features.** Price features for a day use closes up to that day only (a test truncates the history and checks that earlier rows do not change). Fundamental records keep their publication time.
- **Serving.** The API never returns a record with `available_at > as_of` (checked on every response in the tests and in the 300-query audit in `results/pit_audit.json`).
- **Training.** Decision time is 22:00 UTC, after the 21:15 publication. The training set is built with the same rule from the same snapshot; a blocking asset check compares sampled rows with live lookups. Labels look forward by design, and the split purges and embargoes around the test start.

## Cache policy

Read-through: on a request, compute the key, try Redis, and on a miss read the snapshot and write the answer to Redis with a TTL (default 24 hours).

**Key:** `mlpi:feat:v{snapshot_version}-{sha256[:12]}:{entity}:{effective_as_of_us}:{sorted features}`. Version numbers are never reused (the next one is the highest file ever written + 1, and snapshot writes refuse to overwrite), and the content hash in the key makes that a second guarantee rather than the only one.

**Effective as-of:** `min(as_of, snapshot.max_available_at)`. Every record in the snapshot is visible to any `as_of` at or after its newest `available_at`, so all such requests have the same answer. In practice this folds every "latest" request (no `as_of`, or `as_of` = now) for an entity into one key per snapshot.

**Why there is no delete-based invalidation.** A snapshot file is never modified after it is written. An answer cached under version N is therefore correct for version N forever. When the nightly job publishes N+1 (which may contain late corrections with past `available_at`, i.e. history changed), pods switch to N+1 and compute N+1 keys, which are all misses. Old keys are never read again and expire by TTL. There is no window where a pod serves N+1 data from an N key or the reverse, because the version in the key is the version of the reader that produced it.

Rollback works the same way in reverse: pointing `features/CURRENT` back to N makes the N keys (still in Redis until their TTL) valid again; a test checks this.

**Fail-open.** Any Redis error is caught and counted (`/stats` → `cache.errors`); the request is answered from the snapshot. Redis is never needed for a correct answer. Unknown entities are not cached.

**What the replay shows** (`results/cache.json`): with a naive key `(entity, as_of)`, publishing a snapshot with 60 late corrections makes 215 of the next 1,500 responses wrong; with the versioned key, 0.

## Async jobs

`POST /jobs {"kind": "feature_export", "params": {"as_of": ..., "entities": [...]}, "job_key": optional}`

Each step is a single Lua script, so it is atomic on the Redis server. Jobs are hashes (`state`, `attempts`, `worker`, `lease_until`, `result`, ...).

| step | script | guarantee |
|---|---|---|
| submit | if the job hash does not exist: create it as `queued` and `LPUSH` the key, in one script | the same key is queued at most once; a crash cannot leave a job that exists but was never queued; resubmitting a done job returns it (HTTP 200 instead of 202) and runs nothing |
| claim | `LMOVE queue processing`; only if the job is `queued`: `running`, `worker`, `attempts+1` (the fencing token), `lease_until` | a job is in exactly one list; stray entries for finished jobs are dropped, never run |
| heartbeat | only if state, worker and token all match: extend `lease_until` | the worker renews from a background thread every lease/3, so a slow job is not stolen; a stale heartbeat from an old attempt is refused |
| complete | same compare-and-set; `done`, result, `LREM processing`, TTL on the record | a worker that lost its lease cannot overwrite the result |
| fail | same compare-and-set, and only if `LREM processing` removed the key; requeue, or `failed` after `max_attempts` | a late failure cannot requeue a job another worker now owns |
| reclaim | for every expired lease in `processing`: requeue or `failed` | crash resume; atomic against complete, so exactly one of them wins |

Lease times come from the Redis server clock (`TIME` inside the scripts), so workers with skewed clocks agree. The heartbeat retries Redis errors until the lease would run out, uses the queue's own client (5 s timeouts), and stops after `max_runtime_seconds` so a hung handler loses its job. Unknown kinds and invalid parameters (including unknown feature names) are rejected at submission; a job that still turns out invalid fails at once, without retries.

Finished and failed job records expire after 7 days; within that window a resubmission returns the stored result, after it the key can run again. Job keys must match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` without `..` (the key becomes part of the output path), and `feature_export` parameters are validated at submission (timestamp, 1 to 1,000 entity ids, optional feature list).

The job key defaults to a hash of the kind and parameters (key order does not matter), or the caller supplies one: the nightly CronJob uses `nightly-export-YYYY-MM-DD`, so a retried CronJob run cannot start a second export for the same day.

Delivery is at-least-once, so handlers must be idempotent. The built-in `feature_export` writes `jobs/{key}/features.json` and drops the cache hit/miss field, so a re-run after a crash writes byte-identical output (tested).

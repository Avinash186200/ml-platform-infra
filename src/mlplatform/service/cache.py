"""Read-through cache for as-of feature lookups.

Policy (details and the correctness argument in docs/feature-service.md):

* **Key** = snapshot version and content hash + entity + effective as-of +
  sorted feature list.
  Snapshots are immutable, so an entry can never become wrong; publishing a
  new snapshot changes the version, and old entries are simply never read
  again (they expire by TTL). There is no delete-based invalidation to get
  wrong.
* **Effective as-of** = ``min(as_of, snapshot.max_available_at)``. Every
  record in the snapshot is visible to any as-of at or after its newest
  ``available_at``, so all "latest" queries for an entity share one key.
* **Fail open.** If Redis errors, the lookup goes to the snapshot and the
  error is counted; Redis is never required to answer correctly.
"""

from __future__ import annotations

import json
import logging

import redis

log = logging.getLogger(__name__)

KEY_PREFIX = "mlpi:feat"


def cache_key(snapshot_id: str, entity: str, effective_as_of_us: int, features: list[str]) -> str:
    """``snapshot_id`` is ``"<version>-<first 12 hex of the file's sha256>"``."""
    return f"{KEY_PREFIX}:v{snapshot_id}:{entity}:{effective_as_of_us}:{','.join(sorted(features))}"


class FeatureCache:
    def __init__(self, client: redis.Redis | None, ttl_seconds: int = 86_400):
        self.client = client
        self.ttl_seconds = ttl_seconds
        self.hits = 0
        self.misses = 0
        self.errors = 0

    @property
    def enabled(self) -> bool:
        return self.client is not None

    def get(self, key: str) -> dict | None:
        if self.client is None:
            return None
        try:
            raw = self.client.get(key)
        except redis.RedisError:
            self.errors += 1
            log.warning("redis get failed; serving from the snapshot", exc_info=True)
            return None
        if raw is None:
            self.misses += 1
            return None
        self.hits += 1
        return json.loads(raw)

    def set(self, key: str, value: dict) -> None:
        if self.client is None:
            return
        try:
            self.client.set(key, json.dumps(value, separators=(",", ":")), ex=self.ttl_seconds)
        except redis.RedisError:
            self.errors += 1
            log.warning("redis set failed; response still served", exc_info=True)

    def stats(self) -> dict:
        return {"enabled": self.enabled, "hits": self.hits, "misses": self.misses, "errors": self.errors}

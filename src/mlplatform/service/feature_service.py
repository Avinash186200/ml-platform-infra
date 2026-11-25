"""As-of feature lookups: snapshot + cache, and the clients the model API uses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

import httpx

from ..pit import from_us
from .cache import FeatureCache, cache_key
from .snapshots import SnapshotManager


class NotReady(Exception):
    """No feature snapshot is loaded yet."""


class UnknownEntity(Exception):
    pass


class UnknownFeature(Exception):
    pass


@dataclass
class FeatureVector:
    entity: str
    as_of_us: int
    snapshot_version: int
    values: dict[str, dict] = field(default_factory=dict)  # feature -> {value, event_time, available_at}
    missing: list[str] = field(default_factory=list)
    cache: str = "disabled"  # hit | miss | disabled

    def to_dict(self) -> dict:
        return {
            "entity": self.entity,
            "as_of": from_us(self.as_of_us).isoformat(),
            "snapshot_version": self.snapshot_version,
            "features": self.values,
            "missing": self.missing,
            "cache": self.cache,
        }


class FeatureService:
    def __init__(self, snapshots: SnapshotManager, cache: FeatureCache):
        self.snapshots = snapshots
        self.cache = cache

    def get(self, entity: str, as_of_us: int, features: list[str] | None = None) -> FeatureVector:
        with self.snapshots.lease() as reader:
            if reader is None:
                raise NotReady("no feature snapshot loaded")
            return self._get(reader, entity, as_of_us, features)

    def _get(self, reader, entity: str, as_of_us: int, features: list[str] | None) -> FeatureVector:
        feats = sorted(set(features)) if features else list(reader.features)
        unknown = [f for f in feats if f not in reader.features]
        if unknown:
            raise UnknownFeature(", ".join(unknown))

        effective = min(int(as_of_us), reader.max_available_at)
        key = cache_key(reader.cache_id, entity, effective, feats)
        status = "disabled"
        payload = self.cache.get(key) if self.cache.enabled else None
        if payload is not None:
            status = "hit"
        else:
            if self.cache.enabled:
                status = "miss"
            if not reader.has_entity(entity):
                raise UnknownEntity(entity)
            rows = reader.get_asof(entity, effective, feats)
            payload = {
                f: {
                    "value": v,
                    "event_time": from_us(et).isoformat(),
                    "available_at": from_us(av).isoformat(),
                    "available_at_us": int(av),
                }
                for f, (v, et, av) in sorted(rows.items())
            }
            self.cache.set(key, payload)
        return FeatureVector(
            entity=entity,
            as_of_us=int(as_of_us),
            snapshot_version=reader.version,
            values=payload,
            missing=[f for f in feats if f not in payload],
            cache=status,
        )


class FeatureClient(Protocol):
    def fetch(self, entity: str, as_of_us: int, features: list[str]) -> dict: ...


class InProcessFeatureClient:
    def __init__(self, service: FeatureService):
        self.service = service

    def fetch(self, entity: str, as_of_us: int, features: list[str]) -> dict:
        return self.service.get(entity, as_of_us, features).to_dict()


class HttpFeatureClient:
    """Calls the feature API over HTTP (how the model API runs in Kubernetes)."""

    def __init__(self, base_url: str, client: httpx.Client | None = None, timeout: float = 2.0):
        self.base_url = base_url.rstrip("/")
        self.client = client or httpx.Client(timeout=timeout)

    def fetch(self, entity: str, as_of_us: int, features: list[str]) -> dict:
        resp = self.client.get(
            f"{self.base_url}/features/{entity}",
            params={"as_of": from_us(as_of_us).isoformat(), "features": ",".join(features)},
        )
        if resp.status_code == 404:
            raise UnknownEntity(entity)
        if resp.status_code == 503:
            raise NotReady(resp.text)
        resp.raise_for_status()
        return resp.json()

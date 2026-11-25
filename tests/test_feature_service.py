"""Feature API: strictly point in time, and a cache that cannot serve stale answers."""

import numpy as np
import pandas as pd
import pytest
import redis
from conftest import publish
from fastapi.testclient import TestClient

from mlplatform.features import ALL_FEATURES, MODEL_FEATURES
from mlplatform.pit import asof_bruteforce, to_us
from mlplatform.service.app import create_feature_app
from mlplatform.service.cache import FeatureCache, cache_key
from mlplatform.service.feature_service import FeatureService, UnknownEntity
from mlplatform.service.snapshots import SnapshotManager


@pytest.fixture
def service(artifacts, small_records, redis_client, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "serving", refresh_seconds=0)
    yield FeatureService(snaps, FeatureCache(redis_client, ttl_seconds=600))
    snaps.close()


@pytest.fixture
def client(service):
    return TestClient(create_feature_app(service))


def test_no_look_ahead_on_random_queries(client, small_records):
    rng = np.random.default_rng(42)
    ents = sorted(small_records["entity"].unique())
    fund = small_records[small_records["feature"] == "fundamental_score"]
    edges = fund["available_at"].to_numpy()
    lo, hi = small_records["available_at"].min(), small_records["available_at"].max()
    for k in range(60):
        ent = ents[rng.integers(len(ents))]
        if k % 2:
            t = int(edges[rng.integers(len(edges))]) - int(rng.integers(0, 2))  # at, or 1 us before, a publication
        else:
            t = int(rng.integers(lo, hi))
        as_of = pd.Timestamp(t * 1000, tz="UTC").isoformat()
        body = client.get(f"/features/{ent}", params={"as_of": as_of}).json()
        sub = small_records[small_records["entity"] == ent]
        ref = asof_bruteforce(sub, ent, t, ALL_FEATURES)
        assert {f: d["value"] for f, d in body["features"].items()} == {f: v[0] for f, v in ref.items()}
        for d in body["features"].values():
            assert d["available_at_us"] <= t


def test_restatement_served_only_after_publication(client, small_raw):
    f = small_raw["fundamentals"]
    # Find a restatement published before the next quarter's first report, so
    # the restated quarter is still the latest one when the restatement lands.
    case = None
    for r in f[f["is_restatement"]].itertuples(index=False):
        ent_f = f[f["entity"] == r.entity]
        newer = ent_f[(ent_f["event_time"] > r.event_time)]["published_at"]
        if newer.empty or newer.min() > r.published_at:
            first = ent_f[(ent_f["event_time"] == r.event_time) & ~ent_f["is_restatement"]].iloc[0]
            case = (r, first)
            break
    assert case is not None
    restated, first = case

    def score(ts):
        body = client.get(
            f"/features/{restated.entity}", params={"as_of": ts.isoformat(), "features": "fundamental_score"}
        ).json()
        return body["features"]["fundamental_score"]["value"]

    one_us = pd.Timedelta(microseconds=1)
    assert score(first["published_at"]) == pytest.approx(first["score"])
    assert score(restated.published_at - one_us) == pytest.approx(first["score"])
    assert score(restated.published_at) == pytest.approx(restated.score)
    assert first["score"] != restated.score


def test_read_through_cache_hit_after_miss(client, service, redis_client):
    params = {"as_of": "2023-03-01T22:00:00Z", "features": "ret_5d,fundamental_score"}
    first = client.get("/features/SYN001", params=params).json()
    reader = service.snapshots.current()
    q = reader.queries
    second = client.get("/features/SYN001", params=params).json()
    assert (first["cache"], second["cache"]) == ("miss", "hit")
    assert reader.queries == q  # the hit did not touch the snapshot
    assert first["features"] == second["features"]
    key = cache_key(reader.cache_id, "SYN001", to_us("2023-03-01T22:00:00"), ["ret_5d", "fundamental_score"])
    assert 0 < redis_client.ttl(key) <= 600


def test_latest_queries_share_one_key(client, service):
    """Any as_of at or after the snapshot's newest record has the same answer, so one key serves them all."""
    a = client.get("/features/SYN002", params={"as_of": "2030-01-01T00:00:00"}).json()
    b = client.get("/features/SYN002", params={"as_of": "2031-06-01T00:00:00"}).json()
    c = client.get("/features/SYN002").json()  # as_of omitted = now
    assert (a["cache"], b["cache"], c["cache"]) == ("miss", "hit", "hit")
    assert a["features"] == b["features"] == c["features"]
    assert a["as_of"] != b["as_of"]  # the response still echoes the requested time


def test_new_snapshot_is_never_answered_from_the_old_cache(client, service, artifacts, small_records, tmp_path):
    ent = "SYN003"
    as_of = "2023-05-01T22:00:00Z"
    before = client.get(f"/features/{ent}", params={"as_of": as_of}).json()
    assert before["snapshot_version"] == 1
    # v2: a late-arriving correction with a past available_at changes the historical answer.
    t = to_us(as_of)
    cur = before["features"]["fundamental_score"]
    corr = pd.DataFrame(
        [
            {
                "entity": ent,
                "feature": "fundamental_score",
                "event_time": to_us(cur["event_time"]),
                "available_at": cur["available_at_us"] + 1,
                "value": cur["value"] + 10.0,
            }
        ]
    )
    assert corr["available_at"].iloc[0] <= t
    publish(artifacts, pd.concat([small_records, corr], ignore_index=True), 2, tmp_path)
    after = client.get(f"/features/{ent}", params={"as_of": as_of}).json()
    assert after["snapshot_version"] == 2
    assert after["cache"] == "miss"
    assert after["features"]["fundamental_score"]["value"] == pytest.approx(cur["value"] + 10.0)
    # rollback: move the pointer back, the v1 answer (and its cache entry) come back
    artifacts.set_pointer("features/CURRENT", {"version": 1, "key": "features/v000001.sqlite"})
    rolled = client.get(f"/features/{ent}", params={"as_of": as_of}).json()
    assert (rolled["snapshot_version"], rolled["cache"]) == (1, "hit")
    assert rolled["features"] == before["features"]


class BrokenRedis:
    def get(self, *a, **k):
        raise redis.ConnectionError("down")

    def set(self, *a, **k):
        raise redis.ConnectionError("down")


def test_redis_down_fails_open(artifacts, small_records, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "s2", refresh_seconds=0)
    svc = FeatureService(snaps, FeatureCache(BrokenRedis()))
    c = TestClient(create_feature_app(svc))
    r = c.get("/features/SYN000", params={"as_of": "2023-01-10T22:00:00"})
    assert r.status_code == 200 and r.json()["features"]
    assert svc.cache.errors == 2  # one failed get, one failed set
    assert c.get("/stats").json()["cache"]["errors"] == 2
    snaps.close()


def test_cache_disabled(artifacts, small_records, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "s3", refresh_seconds=0)
    svc = FeatureService(snaps, FeatureCache(None))
    assert svc.get("SYN000", to_us("2023-01-10 22:00"), MODEL_FEATURES).cache == "disabled"
    snaps.close()


def test_errors_and_probes(client, artifacts, tmp_path):
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").json()["snapshot_version"] == 1
    assert client.get("/features/NOPE").status_code == 404
    assert client.get("/features/SYN000", params={"features": "nope"}).status_code == 400
    assert client.get("/features/SYN000", params={"as_of": "not-a-time"}).status_code == 422
    # tz-aware input is converted to UTC
    a = client.get("/features/SYN000", params={"as_of": "2023-03-01T17:00:00-05:00"}).json()
    b = client.get("/features/SYN000", params={"as_of": "2023-03-01T22:00:00"}).json()
    assert a["features"] == b["features"] and b["cache"] == "hit"


def test_before_any_history_returns_missing(client):
    body = client.get("/features/SYN000", params={"as_of": "2000-01-01T00:00:00"}).json()
    assert body["features"] == {} and sorted(body["missing"]) == sorted(ALL_FEATURES)


def test_not_ready_without_snapshot(tmp_path):
    from mlplatform.artifacts import ArtifactStore

    snaps = SnapshotManager(ArtifactStore(str(tmp_path / "empty")), tmp_path / "s", refresh_seconds=0)
    c = TestClient(create_feature_app(FeatureService(snaps, FeatureCache(None))))
    assert c.get("/healthz").status_code == 200
    assert c.get("/readyz").status_code == 503
    assert c.get("/features/SYN000").status_code == 503


def test_unknown_entity_is_not_cached(service, redis_client):
    with pytest.raises(UnknownEntity):
        service.get("NOPE", to_us("2023-01-01"), MODEL_FEATURES)
    assert redis_client.dbsize() == 0

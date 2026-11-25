import pytest
from conftest import publish
from fastapi.testclient import TestClient

from mlplatform.artifacts import MODELS_POINTER
from mlplatform.features import MODEL_FEATURES
from mlplatform.model import LinearModel
from mlplatform.pit import to_us
from mlplatform.service.app import create_feature_app, create_model_app
from mlplatform.service.cache import FeatureCache
from mlplatform.service.feature_service import FeatureService, HttpFeatureClient, InProcessFeatureClient
from mlplatform.service.snapshots import ModelRegistry, SnapshotManager


def make_model(shift: float) -> LinearModel:
    n = len(MODEL_FEATURES)
    return LinearModel(
        features=list(MODEL_FEATURES),
        mean=[0.0] * n,
        scale=[1.0] * n,
        coef=[0.1 * (i + 1) for i in range(n)],
        intercept=shift,
        trained_on={},
    )


def deploy(artifacts, model_id: str, model: LinearModel, previous: str | None = None):
    key = f"models/approved/{model_id}/model.json"
    artifacts.write_bytes(key, model.to_json().encode())
    artifacts.set_pointer(MODELS_POINTER, {"model_id": model_id, "key": key, "previous": previous})


@pytest.fixture
def feature_service(artifacts, small_records, redis_client, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "serving", refresh_seconds=0)
    yield FeatureService(snaps, FeatureCache(redis_client))
    snaps.close()


@pytest.mark.parametrize("transport", ["in_process", "http"])
def test_predict_uses_point_in_time_features(transport, artifacts, feature_service):
    model = make_model(0.0)
    deploy(artifacts, "m-a", model)
    if transport == "http":
        fclient = HttpFeatureClient("http://feature-api", client=TestClient(create_feature_app(feature_service)))
    else:
        fclient = InProcessFeatureClient(feature_service)
    app = TestClient(create_model_app(ModelRegistry(artifacts, refresh_seconds=0), fclient))
    as_of = "2023-04-03T22:00:00+00:00"
    body = app.get("/predict/SYN004", params={"as_of": as_of}).json()
    fv = feature_service.get("SYN004", to_us(as_of), MODEL_FEATURES)
    expected = model.predict_one({f: fv.values[f]["value"] for f in MODEL_FEATURES})
    assert body["probability_outperform_5d"] == pytest.approx(expected, abs=1e-12)
    assert body["model_id"] == "m-a" and body["snapshot_version"] == 1
    assert body["as_of"] == as_of


def test_model_promotion_and_rollback_by_pointer(artifacts, feature_service):
    app = TestClient(
        create_model_app(ModelRegistry(artifacts, refresh_seconds=0), InProcessFeatureClient(feature_service))
    )
    assert app.get("/readyz").status_code == 503
    assert app.get("/predict/SYN000").status_code == 503
    deploy(artifacts, "m-a", make_model(0.0))
    p_a = app.get("/predict/SYN000", params={"as_of": "2023-02-01"}).json()
    deploy(artifacts, "m-b", make_model(1.0), previous="m-a")
    p_b = app.get("/predict/SYN000", params={"as_of": "2023-02-01"}).json()
    assert (p_a["model_id"], p_b["model_id"]) == ("m-a", "m-b")
    assert p_b["probability_outperform_5d"] > p_a["probability_outperform_5d"]
    # rollback = point models/CURRENT back at the previous approved model
    deploy(artifacts, "m-a", make_model(0.0))
    assert app.get("/readyz").json()["model_id"] == "m-a"


def test_missing_features_is_422_not_a_guess(artifacts, feature_service):
    deploy(artifacts, "m-a", make_model(0.0))
    app = TestClient(
        create_model_app(ModelRegistry(artifacts, refresh_seconds=0), InProcessFeatureClient(feature_service))
    )
    r = app.get("/predict/SYN000", params={"as_of": "2021-01-01"})
    assert r.status_code == 422
    assert set(r.json()["detail"]["missing"]) == set(MODEL_FEATURES)
    assert app.get("/predict/NOPE").status_code == 404


def test_broken_model_file_keeps_serving_the_loaded_one(artifacts, feature_service):
    deploy(artifacts, "m-a", make_model(0.0))
    reg = ModelRegistry(artifacts, refresh_seconds=0)
    assert reg.current()[0] == "m-a"
    artifacts.write_bytes("models/approved/bad/model.json", b"{not json")
    artifacts.set_pointer(MODELS_POINTER, {"model_id": "bad", "key": "models/approved/bad/model.json"})
    assert reg.current()[0] == "m-a"
    assert reg.refresh_errors == 1


def test_refresh_interval_uses_the_clock_not_sleep(artifacts):
    now = [0.0]
    deploy(artifacts, "m-a", make_model(0.0))
    reg = ModelRegistry(artifacts, refresh_seconds=30, clock=lambda: now[0])
    assert reg.current()[0] == "m-a"
    deploy(artifacts, "m-b", make_model(1.0))
    now[0] = 29.9
    assert reg.current()[0] == "m-a"  # not due yet
    now[0] = 30.0
    assert reg.current()[0] == "m-b"


def test_snapshot_manager_ignores_a_bad_pointer(artifacts, small_records, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "s", refresh_seconds=0)
    assert snaps.current().version == 1
    artifacts.set_pointer("features/CURRENT", {"version": 9, "key": "features/v000009.sqlite"})  # file missing
    assert snaps.current().version == 1
    assert snaps.refresh_errors == 1
    # a file whose own version disagrees with the pointer is refused
    artifacts.upload_file(tmp_path / "build-v1.sqlite", "features/v000005.sqlite")
    artifacts.set_pointer("features/CURRENT", {"version": 5, "key": "features/v000005.sqlite"})
    assert snaps.current().version == 1
    assert snaps.refresh_errors == 2
    snaps.close()


def test_snapshot_with_wrong_hash_is_refused(artifacts, small_records, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "s", refresh_seconds=0)
    assert snaps.current().version == 1
    publish(artifacts, small_records.head(500), 2, tmp_path)
    pointer = artifacts.get_pointer("features/CURRENT")
    artifacts.set_pointer("features/CURRENT", {**pointer, "sha256": "0" * 64})
    assert snaps.current().version == 1
    assert snaps.refresh_errors == 1
    snaps.close()


def test_old_local_snapshots_are_deleted_only_when_unused(artifacts, small_records, tmp_path):
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "s", refresh_seconds=0)
    with snaps.lease() as held:  # a request still using v1 while four new versions arrive
        assert held.version == 1
        for v in range(2, 6):
            publish(artifacts, small_records.head(1000 + v), v, tmp_path)
            snaps.current()
        assert held.path.exists()  # not deleted under a running request
        assert held.get_asof("SYN000", held.max_available_at, ["ret_5d"])
    files = snaps.local_files()
    assert len(files) == 3, files  # current v5 + the two most recent retired (v3, v4)
    assert not held.path.exists()
    assert [f[:7] for f in files] == ["v000003", "v000004", "v000005"]
    snaps.close()


def test_requests_are_not_blocked_while_a_new_snapshot_downloads(artifacts, small_records, tmp_path):
    """The pointer read and the download happen outside the reader lock (single-flight)."""
    import threading

    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "s", refresh_seconds=0)
    assert snaps.current().version == 1
    publish(artifacts, small_records.head(2000), 2, tmp_path)

    started, release = threading.Event(), threading.Event()
    real_download = artifacts.download_file

    def slow_download(key, local):
        started.set()
        assert release.wait(timeout=10)
        real_download(key, local)

    artifacts.download_file = slow_download
    t = threading.Thread(target=snaps.refresh)
    t.start()
    assert started.wait(timeout=10)
    # While v2 downloads, requests still get v1 at once (they would block on the lock otherwise).
    done = threading.Event()

    def request():
        with snaps.lease() as r:
            assert r.version == 1
        done.set()

    threading.Thread(target=request).start()
    assert done.wait(timeout=10)
    release.set()
    t.join(timeout=10)
    assert snaps.current().version == 2
    snaps.close()


@pytest.mark.parametrize(
    "bad", [{"key": "features/v000001.sqlite"}, {"version": None, "key": "x"}, {"version": "three"}]
)
def test_malformed_pointer_keeps_the_loaded_snapshot(artifacts, small_records, tmp_path, bad):
    """A broken CURRENT (e.g. a rollback written when there was no previous version) is counted, not raised."""
    publish(artifacts, small_records, 1, tmp_path)
    snaps = SnapshotManager(artifacts, tmp_path / "s", refresh_seconds=0)
    assert snaps.current().version == 1
    artifacts.set_pointer("features/CURRENT", bad)
    with snaps.lease() as r:
        assert r.version == 1
    assert snaps.refresh_errors == 1
    snaps.close()

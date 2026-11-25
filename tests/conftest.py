from __future__ import annotations

import fakeredis
import pytest

from mlplatform.artifacts import FEATURES_POINTER, ArtifactStore, sha256_file
from mlplatform.features import build_feature_records
from mlplatform.store import write_snapshot
from mlplatform.synthetic import MarketConfig, generate

SMALL = MarketConfig(seed=3, n_entities=6, start="2022-01-01", end="2023-06-30")


@pytest.fixture(scope="session")
def small_raw():
    return generate(SMALL)


@pytest.fixture(scope="session")
def small_records(small_raw):
    return build_feature_records(small_raw["prices"], small_raw["fundamentals"])


@pytest.fixture(scope="session")
def small_snapshot(small_records, tmp_path_factory):
    path = tmp_path_factory.mktemp("snap") / "v000001.sqlite"
    write_snapshot(small_records, path, version=1)
    return path


def publish(store: ArtifactStore, records, version: int, tmp_path) -> None:
    """Write a snapshot file, upload it and move features/CURRENT, as the pipeline does."""
    local = tmp_path / f"build-v{version}.sqlite"
    write_snapshot(records, local, version)
    key = f"features/v{version:06d}.sqlite"
    store.upload_file(local, key, overwrite=False)
    store.set_pointer(FEATURES_POINTER, {"version": version, "key": key, "sha256": sha256_file(local)})


@pytest.fixture
def artifacts(tmp_path):
    return ArtifactStore(str(tmp_path / "artifacts"))


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis()

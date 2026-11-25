import sqlite3

import pandas as pd
import pytest

from mlplatform.artifacts import ArtifactStore
from mlplatform.pit import RECORD_COLUMNS
from mlplatform.store import SnapshotReader, write_snapshot


def test_snapshot_meta_and_lookup(small_snapshot, small_records):
    r = SnapshotReader(small_snapshot)
    try:
        assert r.version == 1
        assert r.meta["n_records"] == len(small_records)
        assert r.max_available_at == int(small_records["available_at"].max())
        assert r.has_entity("SYN000") and not r.has_entity("NOPE")
        assert r.entities() == sorted(small_records["entity"].unique())
        assert r.get_asof("SYN000", r.max_available_at, []) == {}
        got = r.all_records().sort_values(RECORD_COLUMNS[:4]).reset_index(drop=True)
        want = small_records.sort_values(RECORD_COLUMNS[:4]).reset_index(drop=True)
        pd.testing.assert_frame_equal(got, want, check_dtype=False)
    finally:
        r.close()


def test_snapshot_is_never_overwritten(small_records, tmp_path):
    p = tmp_path / "s.sqlite"
    write_snapshot(small_records.head(10), p, 1)
    with pytest.raises(FileExistsError):
        write_snapshot(small_records.head(10), p, 2)


def test_duplicate_records_rejected(tmp_path):
    recs = pd.DataFrame([("X", "a", 1, 2, 1.0), ("X", "a", 1, 2, 9.0)], columns=RECORD_COLUMNS)
    with pytest.raises(ValueError, match="duplicate"):
        write_snapshot(recs, tmp_path / "s.sqlite", 1)


def test_snapshot_is_read_only(small_snapshot):
    r = SnapshotReader(small_snapshot)
    try:
        with pytest.raises(sqlite3.OperationalError):
            r._con.execute("DELETE FROM feature_records")
    finally:
        r.close()


@pytest.mark.parametrize("kind", ["local", "memory"])
def test_artifact_store_roundtrip(kind, tmp_path):
    url = str(tmp_path / "a") if kind == "local" else f"memory://mlpi-test-{tmp_path.name}"
    s = ArtifactStore(url)
    s.write_json("x/y.json", {"a": 1})
    assert s.read_json("x/y.json") == {"a": 1}
    df = pd.DataFrame({"a": [1, 2], "b": ["p", "q"]})
    s.write_parquet("t/df.parquet", df)
    pd.testing.assert_frame_equal(s.read_parquet("t/df.parquet"), df, check_dtype=False)
    assert s.get_pointer("models/CURRENT") is None
    s.set_pointer("models/CURRENT", {"model_id": "m1"})
    s.set_pointer("models/CURRENT", {"model_id": "m2"})
    assert s.get_pointer("models/CURRENT") == {"model_id": "m2"}
    keys = s.list_keys("models")
    assert keys == ["models/CURRENT"], keys  # no temp files left behind
    s.download_file("x/y.json", tmp_path / "out" / "y.json")
    assert (tmp_path / "out" / "y.json").read_bytes() == s.read_bytes("x/y.json")


@pytest.mark.parametrize(
    "key",
    [
        "jobs/../models/approved/evil/features.json",
        "../x",
        "/etc/passwd",
        "",
        "c:/x",
        "a//b",
        "./x",
        "a/./b",
        "~/x",
        "%2e%2e/x",
        "x/C:/y",
    ],
)
def test_artifact_keys_cannot_escape_the_root(tmp_path, key):
    s = ArtifactStore(str(tmp_path / "a"))
    with pytest.raises(ValueError):
        s.write_json(key, {"x": 1})
    assert not list(tmp_path.rglob("*.json"))


@pytest.mark.parametrize("kind", ["local", "memory"])
def test_immutable_write_is_an_exclusive_create(kind, tmp_path):
    url = str(tmp_path / "a") if kind == "local" else f"memory://mlpi-xb-{tmp_path.name}"
    s = ArtifactStore(url)
    s.write_bytes("features/v000001.sqlite", b"one", overwrite=False)
    with pytest.raises(FileExistsError):
        s.write_bytes("features/v000001.sqlite", b"two", overwrite=False)
    assert s.read_bytes("features/v000001.sqlite") == b"one"
    assert s.get_pointer("features/CURRENT") is None  # missing pointer = None, via a plain read
    assert s.list_keys("nothing-here") == []

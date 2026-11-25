"""The Dagster asset graph, run in-process on small synthetic data (no daemon)."""

from pathlib import Path

import pandas as pd
import pytest
from dagster import DagsterInstance, Definitions, build_sensor_context, materialize

import mlplatform.orchestration.assets as assets_mod
from mlplatform.artifacts import FEATURES_POINTER, MODELS_POINTER, ArtifactStore, next_snapshot_version, sha256_file
from mlplatform.bench import PIPELINE_ASSETS
from mlplatform.orchestration.definitions import (
    ALL_ASSETS,
    ALL_CHECKS,
    defs,
    nightly_features_schedule,
    promote_on_approval,
    weekly_retrain_schedule,
)
from mlplatform.orchestration.resources import ArtifactStoreResource, MarketDataResource, TrainingResource

MARKET = {"seed": 7, "n_entities": 12, "start": "2021-01-01", "end": "2023-12-31"}
TRAINING = {"test_start": "2023-01-01", "min_test_rows": 500, "bootstrap_resamples": 100}


def resources(base, signal=True):
    return {
        "artifacts": ArtifactStoreResource(base_url=str(base)),
        "market": MarketDataResource(signal=signal, **MARKET),
        "training": TrainingResource(**TRAINING),
    }


def run(base, signal=True, selection=PIPELINE_ASSETS, instance=None, raise_on_error=True):
    return materialize(
        [*ALL_ASSETS, *ALL_CHECKS],
        resources=resources(base, signal),
        instance=instance or DagsterInstance.ephemeral(),
        selection=selection,
        raise_on_error=raise_on_error,
    )


def materialized(result):
    return {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}


@pytest.fixture(scope="module")
def signal_run(tmp_path_factory):
    base = tmp_path_factory.mktemp("sig") / "artifacts"
    instance = DagsterInstance.ephemeral()
    result = run(base, True, instance=instance)
    return base, instance, result


def test_definitions_load():
    Definitions.validate_loadable(defs)
    jobs = {j.name for j in defs.resolve_all_job_defs()}
    assert {"nightly_features_job", "weekly_retrain_job", "promote_model_job"} <= jobs
    assert nightly_features_schedule.cron_schedule == "0 2 * * 2-6"
    assert weekly_retrain_schedule.cron_schedule == "0 4 * * 0"


def test_full_graph_signal_mode(signal_run):
    base, _, result = signal_run
    assert result.success
    assert materialized(result) == set(PIPELINE_ASSETS)
    checks = {c.check_name: c.passed for c in result.get_asset_check_evaluations()}
    assert checks == {
        "raw_prices_valid": True,
        "raw_fundamentals_valid": True,
        "feature_snapshot_point_in_time": True,
        "training_set_no_leakage": True,
        "evaluation_not_too_good": True,
    }
    store = ArtifactStore(str(base))
    assert store.get_pointer(FEATURES_POINTER)["version"] == 1
    assert store.exists("models/approved/LATEST")
    assert store.get_pointer(MODELS_POINTER) is None  # promotion is a separate step


def test_sensor_promotes_once(signal_run):
    base, instance, _ = signal_run
    ctx = build_sensor_context(instance=instance)
    first = promote_on_approval.evaluate_tick(ctx)
    assert len(first.run_requests) == 1
    again = promote_on_approval.evaluate_tick(build_sensor_context(instance=instance, cursor=first.cursor))
    assert again.run_requests == []
    promote = materialize(ALL_ASSETS, resources=resources(base), instance=instance, selection=["deployed_model"])
    assert promote.success
    store = ArtifactStore(str(base))
    current = store.get_pointer(MODELS_POINTER)
    assert current["model_id"] == store.read_json("models/approved/LATEST")["model_id"]
    assert current["previous"] is None


def test_rerun_publishes_a_new_snapshot_version(signal_run):
    base, _, _ = signal_run
    result = run(
        base, True, selection=["raw_prices", "raw_fundamentals", "feature_snapshot", "publish_feature_snapshot"]
    )
    assert result.success
    store = ArtifactStore(str(base))
    pointer = store.get_pointer(FEATURES_POINTER)
    assert (pointer["version"], pointer["previous"]) == (2, 1)
    assert store.exists("features/v000001.sqlite") and store.exists("features/v000002.sqlite")


def test_control_mode_gate_blocks_promotion(tmp_path):
    instance = DagsterInstance.ephemeral()
    result = run(tmp_path / "artifacts", False, instance=instance)
    assert result.success
    assert "model_artifact" not in materialized(result)
    assert "model_evaluation" in materialized(result)
    tick = promote_on_approval.evaluate_tick(build_sensor_context(instance=instance))
    assert tick.run_requests == []
    store = ArtifactStore(str(tmp_path / "artifacts"))
    assert not store.exists("models/approved/LATEST")


def test_blocking_check_stops_downstream(tmp_path, monkeypatch):
    real = assets_mod.generate

    def corrupt(cfg):
        raw = real(cfg)
        p = raw["prices"].copy()
        p.loc[5, "close"] = -1.0
        return {"prices": p, "fundamentals": raw["fundamentals"]}

    monkeypatch.setattr(assets_mod, "generate", corrupt)
    result = run(tmp_path / "artifacts", True, raise_on_error=False)
    checks = {c.check_name: c.passed for c in result.get_asset_check_evaluations()}
    assert checks["raw_prices_valid"] is False
    assert "feature_snapshot" not in materialized(result)
    assert not ArtifactStore(str(tmp_path / "artifacts")).exists(FEATURES_POINTER)


def test_training_set_check_catches_a_leak(signal_run, tmp_path):
    """Tamper with the stored training set the way a bad join would, and the check fails."""
    from mlplatform.orchestration.checks import training_set_no_leakage

    base, _, _ = signal_run
    store = ArtifactStore(str(base))
    train = store.read_parquet("training/train.parquet")
    original = train.copy()
    train.loc[1, "label_end"] = pd.Timestamp(TRAINING["test_start"]) + pd.Timedelta(days=2)
    store.write_parquet("training/train.parquet", train)
    try:
        res = training_set_no_leakage(artifacts=resources(base)["artifacts"], training=resources(base)["training"])
        assert res.passed is False
        assert res.metadata["train_label_ends_in_test"].value == 1
    finally:
        store.write_parquet("training/train.parquet", original)


# --- regression tests for the audit findings ---------------------------------

NIGHTLY = ["raw_prices", "raw_fundamentals", "feature_snapshot", "publish_feature_snapshot"]


def test_nightly_after_rollback_never_overwrites_a_snapshot(tmp_path):
    """B1: v1, v2, roll back to v1, rebuild -> the new build is v3 and v2 is untouched."""
    base = tmp_path / "artifacts"
    store = ArtifactStore(str(base))
    assert run(base, True, selection=NIGHTLY).success
    assert run(base, True, selection=NIGHTLY).success
    v1 = store.get_pointer(FEATURES_POINTER)["previous_key"]
    v2_bytes = store.read_bytes("features/v000002.sqlite")
    # runbook rollback: CURRENT back to v1 (a hand-written pointer, no hash)
    store.set_pointer(FEATURES_POINTER, {"version": 1, "key": v1})
    assert run(base, True, selection=NIGHTLY).success
    pointer = store.get_pointer(FEATURES_POINTER)
    assert pointer["version"] == 3 and pointer["key"] == "features/v000003.sqlite"
    assert pointer["previous"] == 1
    assert store.read_bytes("features/v000002.sqlite") == v2_bytes
    assert sha256_file(base / "features" / "v000003.sqlite") == pointer["sha256"]


def test_snapshot_files_cannot_be_overwritten(tmp_path):
    store = ArtifactStore(str(tmp_path / "a"))
    store.write_bytes("features/v000001.sqlite", b"one", overwrite=False)
    with pytest.raises(FileExistsError):
        store.write_bytes("features/v000001.sqlite", b"two", overwrite=False)
    assert store.read_bytes("features/v000001.sqlite") == b"one"
    assert next_snapshot_version(store) == 2


def test_failed_snapshot_check_leaves_current_unchanged(tmp_path, monkeypatch):
    """B2: the blocking point-in-time check runs before CURRENT moves."""
    base = tmp_path / "artifacts"
    store = ArtifactStore(str(base))
    assert run(base, True, selection=NIGHTLY).success
    before = store.get_pointer(FEATURES_POINTER)
    real = assets_mod.build_feature_records

    def leaky(prices, fundamentals):  # raw data is fine; one record is "available" before its event
        recs = real(prices, fundamentals)
        recs.loc[0, "available_at"] = recs.loc[0, "event_time"] - 1
        return recs

    monkeypatch.setattr(assets_mod, "build_feature_records", leaky)
    result = run(base, True, selection=NIGHTLY, raise_on_error=False)
    checks = {c.check_name: c.passed for c in result.get_asset_check_evaluations()}
    assert checks["feature_snapshot_point_in_time"] is False
    assert "feature_snapshot" in materialized(result)  # the file was written (as v2, staged) ...
    assert "publish_feature_snapshot" not in materialized(result)  # ... but never published
    assert store.get_pointer(FEATURES_POINTER) == before
    assert store.get_pointer("features/STAGED")["version"] == 2


def test_nightly_job_includes_the_publish_step():
    nightly = {n.name for n in defs.resolve_job_def("nightly_features_job").nodes}
    retrain = {n.name for n in defs.resolve_job_def("weekly_retrain_job").nodes}
    assert {"feature_snapshot", "publish_feature_snapshot"} <= nightly
    assert "publish_feature_snapshot" not in retrain and "training_set" in retrain


# --- round 2: publish only what the check verified ----------------------------


def test_standalone_publish_after_a_failed_check_is_refused(tmp_path, monkeypatch):
    """B1-A: running publish_feature_snapshot on its own cannot publish a snapshot that failed its check."""
    base = tmp_path / "artifacts"
    store = ArtifactStore(str(base))
    assert run(base, True, selection=NIGHTLY).success
    before = store.get_pointer(FEATURES_POINTER)
    real = assets_mod.build_feature_records

    def leaky(prices, fundamentals):
        recs = real(prices, fundamentals)
        recs.loc[0, "available_at"] = recs.loc[0, "event_time"] - 1
        return recs

    monkeypatch.setattr(assets_mod, "build_feature_records", leaky)
    run(base, True, selection=NIGHTLY, raise_on_error=False)  # v2 staged, check fails
    monkeypatch.setattr(assets_mod, "build_feature_records", real)
    again = run(base, True, selection=["publish_feature_snapshot"], raise_on_error=False)
    assert not again.success and "publish_feature_snapshot" not in materialized(again)
    assert store.get_pointer(FEATURES_POINTER) == before
    assert store.get_pointer("features/CHECKED")["version"] == 1


def test_interleaved_runs_cannot_publish_an_unchecked_snapshot(tmp_path):
    """B1-B: run 1 stages and checks v2; run 2 stages v3 before its own check;
    run 1's publish step then finds STAGED (v3) != CHECKED (v2) and refuses."""
    base = tmp_path / "artifacts"
    store = ArtifactStore(str(base))
    assert run(base, True, selection=NIGHTLY).success  # v1 published
    assert run(base, True, selection=["raw_prices", "raw_fundamentals", "feature_snapshot"]).success  # v2 checked
    assert store.get_pointer("features/CHECKED")["version"] == 2
    # run 2 stages v3; its check has not run yet (assets only, no check definitions)
    staged_only = materialize(
        ALL_ASSETS, resources=resources(base), instance=DagsterInstance.ephemeral(), selection=["feature_snapshot"]
    )
    assert staged_only.success and store.get_pointer("features/STAGED")["version"] == 3
    publish = run(base, True, selection=["publish_feature_snapshot"], raise_on_error=False)
    assert not publish.success
    assert store.get_pointer(FEATURES_POINTER)["version"] == 1


def test_nightly_runs_are_serialised_by_the_instance_config(tmp_path):
    import shutil

    from mlplatform.orchestration.definitions import nightly_features_job

    root = Path(__file__).resolve().parents[1]
    shutil.copy(root / "deploy" / "dagster" / "dagster.yaml", tmp_path / "dagster.yaml")
    instance = DagsterInstance.from_config(str(tmp_path))
    limits = instance.run_coordinator.get_run_queue_config().tag_concurrency_limits
    assert {"key": "mlpi/serial", "value": "nightly-features", "limit": 1} in limits
    assert nightly_features_job.tags == {"mlpi/serial": "nightly-features"}
    instance.dispose()


def test_republishing_the_current_snapshot_keeps_the_rollback_target(tmp_path):
    base = tmp_path / "artifacts"
    store = ArtifactStore(str(base))
    assert run(base, True, selection=NIGHTLY).success
    assert run(base, True, selection=NIGHTLY).success
    before = store.get_pointer(FEATURES_POINTER)
    assert (before["version"], before["previous"]) == (2, 1)
    assert run(base, True, selection=["publish_feature_snapshot"]).success  # re-run: no-op
    assert store.get_pointer(FEATURES_POINTER) == before


def test_repromoting_the_same_model_keeps_the_rollback_target(signal_run):
    base, instance, _ = signal_run
    store = ArtifactStore(str(base))
    store.set_pointer(MODELS_POINTER, {"model_id": "m-old", "key": "models/approved/m-old/model.json"})
    for _ in range(2):  # promote, then promote the same model again
        assert materialize(
            ALL_ASSETS, resources=resources(base), instance=instance, selection=["deployed_model"]
        ).success
    current = store.get_pointer(MODELS_POINTER)
    assert current["model_id"] == store.read_json("models/approved/LATEST")["model_id"]
    assert current["previous"] == "m-old"

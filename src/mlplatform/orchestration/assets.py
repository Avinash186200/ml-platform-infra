"""The asset graph.

raw_prices, raw_fundamentals -> feature_snapshot -> publish_feature_snapshot -> training_set
-> candidate_model -> model_evaluation -> model_artifact (only if the gate passes) -> deployed_model

``feature_snapshot`` writes a new immutable file and records it as STAGED; the
blocking point-in-time check runs on it; only then does
``publish_feature_snapshot`` move ``features/CURRENT``. A failed check leaves
CURRENT, and so every reader, where it was.

Every asset reads its inputs from the artifact store and writes its outputs
there, returning only metadata to Dagster.
"""

import contextlib
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from dagster import AssetExecutionContext, AssetSpec, Failure, MaterializeResult, asset, multi_asset

from ..artifacts import (
    FEATURES_CHECKED,
    FEATURES_POINTER,
    FEATURES_STAGED,
    MODELS_POINTER,
    ArtifactStore,
    next_snapshot_version,
    sha256_file,
    snapshot_key,
)
from ..features import build_feature_records
from ..model import LinearModel
from ..store import SnapshotReader, write_snapshot
from ..synthetic import generate
from ..training import build_training_frame, evaluate, fit, gate, purged_split
from .resources import ArtifactStoreResource, MarketDataResource, TrainingResource

CANDIDATE_POINTER = "models/candidates/LATEST"
APPROVED_POINTER = "models/approved/LATEST"


@contextlib.contextmanager
def open_snapshot(
    store: ArtifactStore, pointer_key: str = FEATURES_POINTER, pointer: dict | None = None
) -> Iterator[SnapshotReader]:
    """Download the snapshot a pointer names (CURRENT by default, or an explicit
    ``{"key", "sha256"}``) to a temp dir, verify its hash, and open it."""
    if pointer is None:
        pointer = store.get_pointer(pointer_key)
    if pointer is None:
        raise RuntimeError(f"{pointer_key} does not exist yet")
    with tempfile.TemporaryDirectory(prefix="mlpi-snap-") as tmp:
        local = Path(tmp) / "snapshot.sqlite"
        store.download_file(pointer["key"], local)
        if pointer.get("sha256") and sha256_file(local) != pointer["sha256"]:
            raise RuntimeError(f"{pointer['key']} does not match the hash in {pointer_key}")
        reader = SnapshotReader(local)
        try:
            yield reader
        finally:
            reader.close()


@multi_asset(
    specs=[
        AssetSpec("raw_prices", group_name="ingest", description="Daily closes with publication times."),
        AssetSpec(
            "raw_fundamentals",
            group_name="ingest",
            description="Quarterly scores: reports 25-45 days after quarter end, some restated later.",
        ),
    ]
)
def raw_market_data(market: MarketDataResource, artifacts: ArtifactStoreResource):
    store = artifacts.store()
    raw = generate(market.config())
    store.write_parquet("raw/prices.parquet", raw["prices"])
    store.write_parquet("raw/fundamentals.parquet", raw["fundamentals"])
    yield MaterializeResult(
        asset_key="raw_prices",
        metadata={"rows": len(raw["prices"]), "entities": int(raw["prices"]["entity"].nunique())},
    )
    yield MaterializeResult(
        asset_key="raw_fundamentals",
        metadata={
            "rows": len(raw["fundamentals"]),
            "restatements": int(raw["fundamentals"]["is_restatement"].sum()),
        },
    )


@asset(
    deps=["raw_prices", "raw_fundamentals"],
    group_name="features",
    description="Point-in-time feature records as a new immutable SQLite snapshot (STAGED, not yet served).",
)
def feature_snapshot(context: AssetExecutionContext, artifacts: ArtifactStoreResource) -> MaterializeResult:
    store = artifacts.store()
    prices = store.read_parquet("raw/prices.parquet")
    fundamentals = store.read_parquet("raw/fundamentals.parquet")
    records = build_feature_records(prices, fundamentals)
    # Number from the files, not from CURRENT: after a rollback CURRENT is behind.
    version = next_snapshot_version(store)
    with tempfile.TemporaryDirectory(prefix="mlpi-build-") as tmp:
        local = Path(tmp) / "snapshot.sqlite"
        meta = write_snapshot(records, local, version)
        sha = sha256_file(local)
        store.upload_file(local, snapshot_key(version), overwrite=False)  # never replace a snapshot
    store.set_pointer(FEATURES_STAGED, {"version": version, "key": snapshot_key(version), "sha256": sha})
    context.log.info(f"wrote feature snapshot v{version} ({sha[:12]}) with {meta['n_records']} records")
    return MaterializeResult(
        metadata={
            "version": version,
            "sha256": sha,
            "records": meta["n_records"],
            "entities": meta["n_entities"],
            "features": ", ".join(meta["features"]),
        }
    )


@asset(
    deps=["feature_snapshot"],
    group_name="features",
    description="Moves features/CURRENT to the staged snapshot, only if features/CHECKED says the check passed on that exact file.",
)
def publish_feature_snapshot(context: AssetExecutionContext, artifacts: ArtifactStoreResource) -> MaterializeResult:
    store = artifacts.store()
    staged = store.read_json(FEATURES_STAGED)
    checked = store.get_pointer(FEATURES_CHECKED)
    # Publish only the exact file the blocking check passed. Running this asset
    # on its own after a failed check, or after another run staged a newer
    # file, finds STAGED and CHECKED disagreeing and refuses.
    if not checked or (checked["version"], checked["sha256"]) != (staged["version"], staged["sha256"]):
        raise Failure(
            description=f"refusing to publish v{staged['version']}: it has not passed feature_snapshot_point_in_time",
            metadata={"staged": str(staged), "checked": str(checked)},
        )
    current = store.get_pointer(FEATURES_POINTER)
    if current and (current.get("version"), current.get("sha256")) == (staged["version"], staged["sha256"]):
        # Re-running publish must not rewrite CURRENT with previous = itself (that would lose the rollback target).
        context.log.info(f"features/CURRENT is already v{staged['version']}; nothing to do")
        return MaterializeResult(metadata={"version": staged["version"], "sha256": staged["sha256"], "changed": False})
    store.set_pointer(
        FEATURES_POINTER,
        {
            **staged,
            "previous": current["version"] if current else None,
            "previous_key": current["key"] if current else None,
            "previous_sha256": current.get("sha256") if current else None,
            "published_at": datetime.now(UTC).isoformat(timespec="seconds"),
        },
    )
    context.log.info(f"features/CURRENT -> v{staged['version']}")
    return MaterializeResult(metadata={"version": staged["version"], "sha256": staged["sha256"]})


@asset(
    deps=["publish_feature_snapshot", "raw_prices"],
    group_name="training",
    description="Decision rows joined to features as of 22:00 UTC; purged and embargoed time split.",
)
def training_set(artifacts: ArtifactStoreResource, training: TrainingResource) -> MaterializeResult:
    store = artifacts.store()
    cfg = training.config()
    prices = store.read_parquet("raw/prices.parquet")
    pointer = store.get_pointer(FEATURES_POINTER)  # read once: the published snapshot, never a staged one
    with open_snapshot(store, pointer=pointer) as reader:
        records = reader.all_records()
    frame = build_training_frame(records, prices, cfg)
    train, test, info = purged_split(frame, cfg)
    info["snapshot_version"] = pointer["version"]
    info["snapshot_key"] = pointer["key"]
    info["snapshot_sha256"] = pointer.get("sha256")
    store.write_parquet("training/train.parquet", train)
    store.write_parquet("training/test.parquet", test)
    store.write_json("training/info.json", info)
    return MaterializeResult(metadata=info)


@asset(deps=["training_set"], group_name="training", description="Standardised logistic regression, exported to JSON.")
def candidate_model(artifacts: ArtifactStoreResource, training: TrainingResource) -> MaterializeResult:
    store = artifacts.store()
    info = store.read_json("training/info.json")
    train = store.read_parquet("training/train.parquet")
    model = fit(train, training.config(), trained_on=info)
    model_id = model.model_id(info["snapshot_version"])
    key = f"models/candidates/{model_id}/model.json"
    store.write_bytes(key, model.to_json().encode())
    store.write_json(CANDIDATE_POINTER, {"model_id": model_id, "key": key})
    return MaterializeResult(metadata={"model_id": model_id, "n_train": len(train)})


@asset(
    deps=["candidate_model"],
    group_name="training",
    description="Held-out metrics with a block bootstrap, and the promotion gate decision.",
)
def model_evaluation(artifacts: ArtifactStoreResource, training: TrainingResource) -> MaterializeResult:
    store = artifacts.store()
    cfg = training.config()
    cand = store.read_json(CANDIDATE_POINTER)
    model = LinearModel.from_json(store.read_bytes(cand["key"]))
    train = store.read_parquet("training/train.parquet")
    test = store.read_parquet("training/test.parquet")
    metrics = evaluate(model, train, test, cfg)
    decision = gate(metrics, cfg)
    report = {"model_id": cand["model_id"], "metrics": metrics, "gate": decision}
    store.write_json(f"models/candidates/{cand['model_id']}/evaluation.json", report)
    return MaterializeResult(
        metadata={
            "model_id": cand["model_id"],
            "auc": round(metrics["auc"], 4),
            "auc_ci95_low": round(metrics["auc_ci95_low"], 4),
            "log_loss": round(metrics["log_loss"], 4),
            "baseline_log_loss": round(metrics["baseline_log_loss"], 4),
            "gate_passed": decision["passed"],
        }
    )


@asset(
    deps=["model_evaluation"],
    group_name="training",
    output_required=False,
    description="The approved model. Not materialized when the evaluation gate fails.",
)
def model_artifact(context: AssetExecutionContext, artifacts: ArtifactStoreResource):
    store = artifacts.store()
    cand = store.read_json(CANDIDATE_POINTER)
    report = store.read_json(f"models/candidates/{cand['model_id']}/evaluation.json")
    if not report["gate"]["passed"]:
        failed = [k for k, ok in report["gate"]["checks"].items() if not ok]
        context.log.warning(f"gate failed for {cand['model_id']}: {failed}; nothing promoted")
        return
    key = f"models/approved/{cand['model_id']}/model.json"
    store.write_bytes(key, store.read_bytes(cand["key"]))
    store.write_json(f"models/approved/{cand['model_id']}/evaluation.json", report)
    store.write_json(APPROVED_POINTER, {"model_id": cand["model_id"], "key": key})
    yield MaterializeResult(metadata={"model_id": cand["model_id"], "key": key})


@asset(
    deps=["model_artifact"],
    group_name="serving",
    description="Moves models/CURRENT to the newest approved model. Run by the promotion sensor.",
)
def deployed_model(context: AssetExecutionContext, artifacts: ArtifactStoreResource) -> MaterializeResult:
    store = artifacts.store()
    approved = store.read_json(APPROVED_POINTER)
    current = store.get_pointer(MODELS_POINTER)
    if current and current.get("model_id") == approved["model_id"]:
        # Re-promoting the same model must not set previous = itself (that would lose the rollback target).
        context.log.info(f"models/CURRENT is already {approved['model_id']}; nothing to do")
        return MaterializeResult(metadata={"model_id": approved["model_id"], "changed": False})
    pointer = {
        "model_id": approved["model_id"],
        "key": approved["key"],
        "previous": current["model_id"] if current else None,
        "previous_key": current["key"] if current else None,
        "promoted_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    store.set_pointer(MODELS_POINTER, pointer)
    context.log.info(f"models/CURRENT -> {approved['model_id']}")
    return MaterializeResult(metadata={"model_id": approved["model_id"], "previous": str(pointer["previous"])})

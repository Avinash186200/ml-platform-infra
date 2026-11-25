"""Asset checks: data quality and leakage guards, run after each materialization.

The point-in-time and purge checks are ``blocking``: if one fails, the assets
downstream of it are not materialized in that run.
"""

import numpy as np
import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from ..artifacts import FEATURES_CHECKED, FEATURES_STAGED
from ..features import ALL_FEATURES, MODEL_FEATURES
from ..pit import asof_bruteforce
from ..synthetic import PRICE_PUBLICATION_LAG
from .assets import open_snapshot
from .resources import ArtifactStoreResource, TrainingResource


@asset_check(
    asset="raw_prices",
    blocking=True,
    description="Closes positive and finite; published after the close; one bar per entity and day.",
)
def raw_prices_valid(artifacts: ArtifactStoreResource) -> AssetCheckResult:
    p = artifacts.store().read_parquet("raw/prices.parquet")
    problems = {
        "non_positive_or_missing_close": int((~(p["close"] > 0)).sum()),
        "non_finite_close": int((~np.isfinite(p["close"])).sum()),
        "published_before_close": int((p["published_at"] < p["event_time"] + PRICE_PUBLICATION_LAG).sum()),
        "duplicate_bars": int(p.duplicated(subset=["entity", "date"]).sum()),
    }
    return AssetCheckResult(passed=not any(problems.values()), metadata={**problems, "rows": len(p)})


@asset_check(
    asset="raw_fundamentals",
    blocking=True,
    description="Reports come after the quarter end; restatements come after the first report.",
)
def raw_fundamentals_valid(artifacts: ArtifactStoreResource) -> AssetCheckResult:
    f = artifacts.store().read_parquet("raw/fundamentals.parquet")
    first = f[~f["is_restatement"]].set_index(["entity", "event_time"])["published_at"]
    rest = f[f["is_restatement"]].set_index(["entity", "event_time"])["published_at"]
    common = rest.index.intersection(first.index)
    orphan = len(rest.index.difference(first.index))
    problems = {
        "published_before_quarter_end": int((f["published_at"] <= f["event_time"]).sum()),
        "restatement_before_first_report": int((rest.loc[common] <= first.loc[common]).sum()),
        "restatement_without_report": orphan,
        "missing_score": int(f["score"].isna().sum()),
    }
    return AssetCheckResult(passed=not any(problems.values()), metadata={**problems, "rows": len(f)})


@asset_check(
    asset="feature_snapshot",
    blocking=True,
    description="No record is available before its event; values finite; the SQL as-of lookup matches the reference rule on sampled queries.",
)
def feature_snapshot_point_in_time(artifacts: ArtifactStoreResource) -> AssetCheckResult:
    store = artifacts.store()
    staged = store.read_json(FEATURES_STAGED)  # read once: this exact file is what gets verified
    with open_snapshot(store, pointer=staged) as reader:  # the new file, before it is published
        recs = reader.all_records()
        rng = np.random.default_rng(reader.version)
        entities = recs["entity"].unique()
        lo, hi = int(recs["available_at"].min()), int(recs["available_at"].max())
        mismatches = 0
        n_queries = 40
        for _ in range(n_queries):
            ent = entities[rng.integers(len(entities))]
            as_of = int(rng.integers(lo - 86_400_000_000, hi + 86_400_000_000))
            sub = recs[recs["entity"] == ent]
            expected = asof_bruteforce(sub, ent, as_of, ALL_FEATURES)
            got = reader.get_asof(ent, as_of, ALL_FEATURES)
            if got != expected:
                mismatches += 1
        version = reader.version
    problems = {
        "available_before_event": int((recs["available_at"] < recs["event_time"]).sum()),
        "non_finite_values": int((~np.isfinite(recs["value"])).sum()),
        "asof_mismatches": mismatches,
    }
    passed = not any(problems.values())
    if passed:  # publish_feature_snapshot will only publish this version and hash
        store.set_pointer(FEATURES_CHECKED, {"version": staged["version"], "sha256": staged["sha256"]})
    return AssetCheckResult(
        passed=passed,
        metadata={**problems, "sampled_queries": n_queries, "version": version, "sha256": staged["sha256"]},
    )


@asset_check(
    asset="training_set",
    blocking=True,
    description="Training labels end before the test block starts; sampled rows equal what the feature store returns as of the decision time.",
)
def training_set_no_leakage(artifacts: ArtifactStoreResource, training: TrainingResource) -> AssetCheckResult:
    store = artifacts.store()
    cfg = training.config()
    train = store.read_parquet("training/train.parquet")
    test = store.read_parquet("training/test.parquet")
    info = store.read_json("training/info.json")
    test_start = pd.Timestamp(cfg.test_start)
    embargo_start = test_start - pd.offsets.BDay(cfg.embargo_days)
    problems = {
        "train_label_ends_in_test": int((train["label_end"] >= test_start).sum()),
        "train_rows_in_embargo": int((train["date"] >= embargo_start).sum()),
        "test_rows_before_test_start": int((test["date"] < test_start).sum()),
    }
    rng = np.random.default_rng(0)
    sample = pd.concat([train, test]).iloc[rng.choice(len(train) + len(test), size=50, replace=False)]
    mismatches = 0
    with open_snapshot(store, pointer={"key": info["snapshot_key"], "sha256": info["snapshot_sha256"]}) as reader:
        for row in sample.itertuples(index=False):
            got = reader.get_asof(row.entity, row.as_of, MODEL_FEATURES)
            if any(got[f][0] != getattr(row, f) for f in MODEL_FEATURES):
                mismatches += 1
    problems["rows_not_matching_store"] = mismatches
    return AssetCheckResult(passed=not any(problems.values()), metadata={**problems, **info})


@asset_check(
    asset="model_evaluation",
    description="Warn (not block) when the held-out AUC is suspiciously high for daily equity returns.",
)
def evaluation_not_too_good(artifacts: ArtifactStoreResource) -> AssetCheckResult:
    store = artifacts.store()
    cand = store.read_json("models/candidates/LATEST")
    report = store.read_json(f"models/candidates/{cand['model_id']}/evaluation.json")
    auc = report["metrics"]["auc"]
    return AssetCheckResult(
        passed=auc < 0.70,
        severity=AssetCheckSeverity.WARN,
        metadata={"auc": auc, "threshold": 0.70},
        description="An AUC this high on 5-day returns usually means leakage; investigate before trusting it.",
    )


ALL_CHECKS = [
    raw_prices_valid,
    raw_fundamentals_valid,
    feature_snapshot_point_in_time,
    training_set_no_leakage,
    evaluation_not_too_good,
]

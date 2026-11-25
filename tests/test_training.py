import json

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from mlplatform.features import MODEL_FEATURES
from mlplatform.model import LinearModel
from mlplatform.store import SnapshotReader
from mlplatform.training import (
    TrainingConfig,
    build_training_frame,
    decision_rows,
    evaluate,
    fit,
    gate,
    leaky_training_frame,
    purged_split,
)

CFG = TrainingConfig(test_start="2023-01-01", min_test_rows=100, bootstrap_resamples=50)


@pytest.fixture(scope="module")
def frame(small_raw, small_records):
    return build_training_frame(small_records, small_raw["prices"], CFG)


def test_labels_are_balanced_per_day_and_end_after_decision(small_raw):
    rows = decision_rows(small_raw["prices"], 5)
    assert (rows["label_end"] > rows["as_of_ts"]).all()
    per_day = rows.groupby("date")["label"].mean()
    assert per_day.between(0.3, 0.7).all()


def test_split_purges_and_embargoes(frame):
    train, test, info = purged_split(frame, CFG)
    test_start = pd.Timestamp(CFG.test_start)
    assert (train["label_end"] < test_start).all()
    assert (train["date"] < test_start - pd.offsets.BDay(CFG.embargo_days)).all()
    assert (test["date"] >= test_start).all()
    assert info["n_dropped_purge_or_embargo"] > 0
    assert info["n_train"] + info["n_test"] + info["n_dropped_purge_or_embargo"] == len(frame)


def test_training_rows_equal_feature_store_lookups(frame, small_snapshot):
    """Training/serving consistency: each row is what the store returns as of the decision time."""
    r = SnapshotReader(small_snapshot)
    try:
        for row in frame.sample(80, random_state=1).itertuples(index=False):
            got = r.get_asof(row.entity, row.as_of, MODEL_FEATURES)
            for f in MODEL_FEATURES:
                assert got[f][0] == getattr(row, f)
                assert got[f][2] <= row.as_of  # published no later than the decision
    finally:
        r.close()


def test_leaky_join_uses_values_not_yet_published(frame, small_raw):
    leaky = leaky_training_frame(frame, small_raw["fundamentals"])
    merged = frame.merge(leaky[["entity", "date", "fundamental_score"]], on=["entity", "date"], suffixes=("", "_leaky"))
    assert (merged["fundamental_score"] != merged["fundamental_score_leaky"]).mean() > 0.5


def test_json_model_matches_sklearn(frame):
    train, test, _ = purged_split(frame, CFG)
    model = fit(train, CFG, trained_on={"k": "v"})
    X = train[MODEL_FEATURES].to_numpy(float)
    sc = StandardScaler().fit(X)
    ref = LogisticRegression(C=CFG.C, max_iter=1000).fit(sc.transform(X), train["label"])
    Xt = test[MODEL_FEATURES].to_numpy(float)
    np.testing.assert_allclose(model.predict_proba(Xt), ref.predict_proba(sc.transform(Xt))[:, 1], rtol=0, atol=1e-12)
    again = LinearModel.from_json(model.to_json())
    assert again == model
    assert again.model_id(3) == model.model_id(3)
    assert model.predict_one(dict(zip(MODEL_FEATURES, Xt[0], strict=True))) == pytest.approx(
        model.predict_proba(Xt[:1])[0]
    )


@pytest.mark.parametrize(
    "patch",
    [{"format": "pickle"}, {"coef": [1.0]}, {"scale": [0.0, 1, 1, 1, 1]}],
)
def test_model_json_validation(frame, patch):
    train, _, _ = purged_split(frame, CFG)
    obj = json.loads(fit(train, CFG, trained_on={}).to_json())
    obj.update(patch)
    with pytest.raises(ValueError):
        LinearModel.from_json(json.dumps(obj))


def test_evaluate_and_gate_on_small_data(frame):
    train, test, _ = purged_split(frame, CFG)
    m = evaluate(fit(train, CFG, trained_on={}), train, test, CFG)
    assert 0 <= m["auc_ci95_low"] <= m["auc"] <= m["auc_ci95_high"] <= 1
    assert m["bootstrap_block_days"] == 10
    assert set(gate(m, CFG)["checks"]) == {
        "enough_test_rows",
        "auc_ci95_low_above_0.5",
        "beats_constant_prior_log_loss",
    }


@pytest.mark.parametrize(
    "metrics,passed",
    [
        ({"n_test": 5000, "auc_ci95_low": 0.51, "log_loss": 0.68, "baseline_log_loss": 0.69}, True),
        ({"n_test": 5000, "auc_ci95_low": 0.49, "log_loss": 0.68, "baseline_log_loss": 0.69}, False),
        ({"n_test": 5000, "auc_ci95_low": 0.51, "log_loss": 0.70, "baseline_log_loss": 0.69}, False),
        ({"n_test": 50, "auc_ci95_low": 0.51, "log_loss": 0.68, "baseline_log_loss": 0.69}, False),
    ],
)
def test_gate_rules(metrics, passed):
    assert gate(metrics, CFG)["passed"] is passed

"""Training set, purged time split, model fit, evaluation and the promotion gate.

Leakage controls, all tested:

* Features come from the feature store through the as-of rule, at a decision
  time of 22:00 UTC on each business day (after that day's close is
  published at 21:15). A test checks every training row against the store.
* The label is the 5-business-day forward return relative to the
  cross-sectional median. It uses future closes by design; its end time is
  kept so the split can purge.
* Time split: test rows are decisions on or after ``test_start``. Training
  rows must have their label fully realised before ``test_start`` (purge)
  and their decision time at least ``embargo_days`` business days before it
  (embargo).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler

from .features import MODEL_FEATURES
from .model import LinearModel
from .pit import asof_join, series_to_us

DECISION_HOUR = pd.Timedelta(hours=22)


@dataclass(frozen=True)
class TrainingConfig:
    test_start: str = "2024-01-01"
    horizon_days: int = 5
    embargo_days: int = 5
    min_test_rows: int = 1000
    bootstrap_resamples: int = 200
    bootstrap_seed: int = 11
    C: float = 1.0


def decision_rows(prices: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """One row per entity and business day with a label and the label's end time."""
    p = prices.sort_values(["entity", "date"], kind="mergesort").copy()
    g = p.groupby("entity", sort=False)
    p["fwd_ret"] = np.log(g["close"].shift(-horizon_days)) - np.log(p["close"])
    p["label_end"] = g["event_time"].shift(-horizon_days)
    p = p.dropna(subset=["fwd_ret", "label_end"])
    median = p.groupby("date")["fwd_ret"].transform("median")
    p["label"] = (p["fwd_ret"] > median).astype(int)
    p["as_of_ts"] = p["date"] + DECISION_HOUR
    out = p[["entity", "date", "as_of_ts", "label_end", "fwd_ret", "label"]].reset_index(drop=True)
    out["as_of"] = series_to_us(out["as_of_ts"])
    out["label_end_us"] = series_to_us(out["label_end"])
    return out


def build_training_frame(records: pd.DataFrame, prices: pd.DataFrame, cfg: TrainingConfig) -> pd.DataFrame:
    rows = decision_rows(prices, cfg.horizon_days)
    joined = asof_join(records, rows[["entity", "as_of"]], MODEL_FEATURES)
    frame = pd.concat([rows, joined.drop(columns=["entity", "as_of"])], axis=1)
    return frame.dropna(subset=MODEL_FEATURES).reset_index(drop=True)


def leaky_training_frame(frame: pd.DataFrame, fundamentals: pd.DataFrame) -> pd.DataFrame:
    """The common mistake, for illustration only: join the fundamental score by
    quarter (the quarter the decision date falls in, last value ever published)
    and ignore when it was published."""
    last = fundamentals.sort_values("published_at").groupby(["entity", "event_time"], as_index=False).last()
    leaky = frame.copy()
    leaky["quarter_end"] = leaky["date"] + pd.offsets.QuarterEnd(0)
    leaky = leaky.merge(
        last[["entity", "event_time", "score"]].rename(columns={"event_time": "quarter_end"}),
        on=["entity", "quarter_end"],
        how="left",
    )
    leaky["fundamental_score"] = leaky["score"]
    return leaky.drop(columns=["score", "quarter_end"]).dropna(subset=MODEL_FEATURES).reset_index(drop=True)


def purged_split(frame: pd.DataFrame, cfg: TrainingConfig) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    test_start = pd.Timestamp(cfg.test_start)
    embargo_start = test_start - pd.offsets.BDay(cfg.embargo_days)
    is_test = frame["date"] >= test_start
    before = frame[~is_test]
    keep = (before["label_end"] < test_start) & (before["date"] < embargo_start)
    train = before[keep].reset_index(drop=True)
    test = frame[is_test].reset_index(drop=True)
    info = {
        "test_start": cfg.test_start,
        "n_train": len(train),
        "n_test": len(test),
        "n_dropped_purge_or_embargo": int((~keep).sum()),
        "train_last_label_end": str(train["label_end"].max()) if len(train) else None,
        "test_first_decision": str(test["as_of_ts"].min()) if len(test) else None,
    }
    return train, test, info


def fit(train: pd.DataFrame, cfg: TrainingConfig, trained_on: dict) -> LinearModel:
    X = train[MODEL_FEATURES].to_numpy(dtype=float)
    y = train["label"].to_numpy()
    scaler = StandardScaler().fit(X)
    clf = LogisticRegression(C=cfg.C, max_iter=1000).fit(scaler.transform(X), y)
    return LinearModel(
        features=list(MODEL_FEATURES),
        mean=[float(v) for v in scaler.mean_],
        scale=[float(v) for v in scaler.scale_],
        coef=[float(v) for v in clf.coef_[0]],
        intercept=float(clf.intercept_[0]),
        trained_on=trained_on,
    )


def evaluate(model: LinearModel, train: pd.DataFrame, test: pd.DataFrame, cfg: TrainingConfig) -> dict:
    """Score the exported model on the held-out block, with a block bootstrap for the AUC."""
    p = model.predict_proba(test[model.features].to_numpy(dtype=float))
    y = test["label"].to_numpy()
    prior = float(train["label"].mean())
    auc = float(roc_auc_score(y, p))

    # Moving-block bootstrap over test days. Labels overlap for horizon_days
    # days and the features are persistent, so neighbouring days are not
    # independent; blocks of 2 x horizon consecutive days keep that structure.
    rng = np.random.default_rng(cfg.bootstrap_seed)
    days = test["date"].to_numpy()
    uniq, inv = np.unique(days, return_inverse=True)
    by_day = [np.flatnonzero(inv == k) for k in range(len(uniq))]
    block = min(2 * cfg.horizon_days, len(uniq))
    n_blocks = -(-len(uniq) // block)
    boot = []
    for _ in range(cfg.bootstrap_resamples):
        starts = rng.integers(0, len(uniq) - block + 1, size=n_blocks)
        picked = np.concatenate([np.arange(s0, s0 + block) for s0 in starts])[: len(uniq)]
        idx = np.concatenate([by_day[k] for k in picked])
        boot.append(roc_auc_score(y[idx], p[idx]))
    lo, hi = np.percentile(boot, [2.5, 97.5])

    return {
        "n_train": len(train),
        "n_test": len(test),
        "test_days": len(uniq),
        "bootstrap_block_days": int(block),
        "auc": auc,
        "auc_ci95_low": float(lo),
        "auc_ci95_high": float(hi),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "baseline_log_loss": float(log_loss(y, np.full(len(y), prior), labels=[0, 1])),
        "brier": float(brier_score_loss(y, p)),
        "accuracy": float(((p > 0.5).astype(int) == y).mean()),
        "test_up_rate": float(y.mean()),
        "coef": dict(zip(model.features, model.coef, strict=True)),
    }


def gate(metrics: dict, cfg: TrainingConfig) -> dict:
    """Promotion rule. All three must hold; the reasons are recorded either way."""
    checks = {
        "enough_test_rows": metrics["n_test"] >= cfg.min_test_rows,
        "auc_ci95_low_above_0.5": metrics["auc_ci95_low"] > 0.5,
        "beats_constant_prior_log_loss": metrics["log_loss"] < metrics["baseline_log_loss"],
    }
    return {"passed": all(checks.values()), "checks": checks}

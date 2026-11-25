"""Raw tables -> point-in-time feature records.

Price features are computed per entity from closes in date order, using only
closes up to and including the row's own date, so a feature row's
``available_at`` is the publication time of that day's close. Fundamental
records are passed through as-is (one record per report or restatement), so
restatements stay visible to the as-of rule in ``pit.py``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .pit import RECORD_COLUMNS, series_to_us

PRICE_FEATURES = ["ret_1d", "ret_5d", "ret_20d", "vol_20d", "volume_z_20d"]
FUNDAMENTAL_FEATURES = ["fundamental_score"]
ALL_FEATURES = PRICE_FEATURES + FUNDAMENTAL_FEATURES
MODEL_FEATURES = ["ret_5d", "ret_20d", "vol_20d", "volume_z_20d", "fundamental_score"]


def price_feature_frame(prices: pd.DataFrame) -> pd.DataFrame:
    """Wide price features per (entity, date). Rows without 20 days of history are dropped."""
    p = prices.sort_values(["entity", "date"], kind="mergesort").copy()
    g = p.groupby("entity", sort=False)
    logc = np.log(p["close"])
    p["_r"] = logc - g["close"].shift(1).pipe(np.log)
    p["ret_1d"] = p["_r"]
    p["ret_5d"] = logc - np.log(g["close"].shift(5))
    p["ret_20d"] = logc - np.log(g["close"].shift(20))
    p["vol_20d"] = g["_r"].transform(lambda r: r.rolling(20, min_periods=20).std())
    logv = np.log(p["volume"])
    mean_v = logv.groupby(p["entity"], sort=False).transform(lambda v: v.rolling(20, min_periods=20).mean())
    std_v = logv.groupby(p["entity"], sort=False).transform(lambda v: v.rolling(20, min_periods=20).std())
    p["volume_z_20d"] = (logv - mean_v) / std_v
    p = p.dropna(subset=PRICE_FEATURES)
    return p[["entity", "date", "event_time", "published_at", *PRICE_FEATURES]].reset_index(drop=True)


def build_feature_records(prices: pd.DataFrame, fundamentals: pd.DataFrame) -> pd.DataFrame:
    """Long-format records ``entity, feature, event_time, available_at, value`` (times in microseconds)."""
    wide = price_feature_frame(prices)
    long = wide.melt(
        id_vars=["entity", "event_time", "published_at"],
        value_vars=PRICE_FEATURES,
        var_name="feature",
        value_name="value",
    )
    long = long.rename(columns={"published_at": "available_at"})
    fund = fundamentals.rename(columns={"published_at": "available_at", "score": "value"}).assign(
        feature="fundamental_score"
    )
    recs = pd.concat([long, fund[["entity", "event_time", "available_at", "feature", "value"]]], ignore_index=True)
    recs["event_time"] = series_to_us(recs["event_time"])
    recs["available_at"] = series_to_us(recs["available_at"])
    recs["value"] = recs["value"].astype(float)
    recs = recs[RECORD_COLUMNS].sort_values(["entity", "feature", "event_time", "available_at"], kind="mergesort")
    return recs.reset_index(drop=True)

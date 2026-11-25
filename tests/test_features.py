import numpy as np
import pandas as pd

from mlplatform.features import ALL_FEATURES, PRICE_FEATURES, build_feature_records, price_feature_frame
from mlplatform.pit import to_us


def test_records_shape_and_times(small_raw, small_records):
    assert set(small_records["feature"]) == set(ALL_FEATURES)
    assert (small_records["available_at"] >= small_records["event_time"]).all()
    assert np.isfinite(small_records["value"]).all()
    assert not small_records.duplicated(subset=["entity", "feature", "event_time", "available_at"]).any()
    # Every report and restatement becomes its own record.
    n_fund = (small_records["feature"] == "fundamental_score").sum()
    assert n_fund == len(small_raw["fundamentals"])


def test_price_features_never_use_future_closes(small_raw):
    """Truncating the price history after day D must not change any feature row up to D."""
    prices = small_raw["prices"]
    full = price_feature_frame(prices)
    cut_day = pd.Timestamp("2022-11-15")
    truncated = price_feature_frame(prices[prices["date"] <= cut_day])
    a = full[full["date"] <= cut_day].reset_index(drop=True)
    pd.testing.assert_frame_equal(a, truncated.reset_index(drop=True))


def test_price_feature_values_by_hand(small_raw):
    p = small_raw["prices"]
    one = p[p["entity"] == "SYN002"].sort_values("date").reset_index(drop=True)
    wide = price_feature_frame(one)
    row = wide.iloc[10]
    k = one.index[one["date"] == row["date"]][0]
    c = one["close"].to_numpy()
    assert np.isclose(row["ret_5d"], np.log(c[k] / c[k - 5]))
    assert np.isclose(row["ret_20d"], np.log(c[k] / c[k - 20]))
    r = np.diff(np.log(c))[k - 20 : k]
    assert np.isclose(row["vol_20d"], np.std(r, ddof=1))


def test_feature_available_at_is_close_publication(small_raw, small_records):
    price_recs = small_records[small_records["feature"].isin(PRICE_FEATURES)]
    lag = price_recs["available_at"] - price_recs["event_time"]
    assert (lag == 15 * 60 * 1_000_000).all()
    first = small_records[small_records["feature"] == "ret_20d"]["event_time"].min()
    assert first >= to_us(small_raw["prices"]["event_time"].min()) + 20 * 86_400_000_000


def test_build_is_deterministic(small_raw, small_records):
    again = build_feature_records(small_raw["prices"], small_raw["fundamentals"])
    pd.testing.assert_frame_equal(again, small_records)

import numpy as np
import pandas as pd

from mlplatform.synthetic import PRICE_PUBLICATION_LAG, MarketConfig, generate, hidden_signal_by_quarter


def test_deterministic_for_a_seed():
    cfg = MarketConfig(seed=1, n_entities=3, start="2022-01-01", end="2022-06-30")
    a, b = generate(cfg), generate(cfg)
    pd.testing.assert_frame_equal(a["prices"], b["prices"])
    pd.testing.assert_frame_equal(a["fundamentals"], b["fundamentals"])


def test_prefix_stable_when_end_moves_forward():
    """A nightly run with a later end date must reproduce every earlier row exactly."""
    short = generate(MarketConfig(seed=2, n_entities=4, start="2022-01-01", end="2022-09-30"))
    long = generate(MarketConfig(seed=2, n_entities=4, start="2022-01-01", end="2023-03-31"))
    cut = long["prices"][long["prices"]["published_at"] < pd.Timestamp("2022-10-01")].reset_index(drop=True)
    pd.testing.assert_frame_equal(short["prices"], cut)
    f_cut = long["fundamentals"][long["fundamentals"]["published_at"] < pd.Timestamp("2022-10-01")]
    pd.testing.assert_frame_equal(short["fundamentals"], f_cut.reset_index(drop=True))


def test_nothing_published_after_end(small_raw):
    end = pd.Timestamp("2023-07-01")
    assert (small_raw["prices"]["published_at"] < end).all()
    assert (small_raw["fundamentals"]["published_at"] < end).all()


def test_publication_times(small_raw):
    p, f = small_raw["prices"], small_raw["fundamentals"]
    assert ((p["published_at"] - p["event_time"]) == PRICE_PUBLICATION_LAG).all()
    lag = f["published_at"] - f["event_time"]
    assert (lag >= pd.Timedelta(days=25)).all()
    first = f[~f["is_restatement"]].set_index(["entity", "event_time"])["published_at"]
    rest = f[f["is_restatement"]].set_index(["entity", "event_time"])["published_at"]
    assert len(rest) > 0
    assert (rest > first.loc[rest.index]).all()


def test_signal_mode_returns_follow_the_hidden_quarter_and_control_does_not():
    cfgs = [MarketConfig(seed=5, n_entities=30, start="2021-01-01", end="2024-12-31", signal=s) for s in (True, False)]
    corrs = []
    for cfg in cfgs:
        p = generate(cfg)["prices"].copy()
        p["ret"] = np.log(p["close"]).groupby(p["entity"]).diff()
        p["quarter_end"] = p["date"] + pd.offsets.QuarterEnd(0)
        q = p.groupby(["entity", "quarter_end"], as_index=False)["ret"].mean()
        q = q.merge(hidden_signal_by_quarter(cfg), on=["entity", "quarter_end"])
        corrs.append(np.corrcoef(q["ret"], q["signal"])[0, 1])
    assert corrs[0] > 0.3
    assert abs(corrs[1]) < 0.1

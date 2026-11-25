"""Synthetic market data with publication times and restatements.

Two raw tables come out of here:

* ``prices``: one daily bar per entity and business day. The close is the
  event (``event_time`` = 21:00 UTC that day) and the vendor publishes it 15
  minutes later (``published_at`` = 21:15 UTC).
* ``fundamentals``: one quarterly score per entity. The score describes the
  quarter itself, so it is only reported 25 to 45 days after the quarter ends,
  and about 30% of reports are restated 10 to 60 days later with a more
  accurate value. ``event_time`` is the quarter end, ``published_at`` is the
  report (or restatement) time.

The hidden quarterly signal ``s`` follows an AR(1) across quarters. In
``signal`` mode a stock's daily return drifts with the ``s`` of the quarter it
is in, which nobody can know until the report arrives after that quarter ends.
A point-in-time model can only use the last *reported* quarter, which is
correlated with the current one through the AR(1). A model that joins
fundamentals by quarter and ignores publication times sees the current
quarter's value early: that is the look-ahead this repo is built to prevent.
In ``control`` mode returns do not depend on ``s`` at all.

Everything is generated on a fixed calendar and then cut at ``end``, so a run
with a later ``end`` reproduces every earlier row exactly (prefix stable).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

CALENDAR_START = pd.Timestamp("2020-01-01")
CALENDAR_END = pd.Timestamp("2027-12-31")
CLOSE_HOUR = pd.Timedelta(hours=21)
PRICE_PUBLICATION_LAG = pd.Timedelta(minutes=15)
REPORT_HOUR = pd.Timedelta(hours=12)


@dataclass(frozen=True)
class MarketConfig:
    seed: int = 7
    n_entities: int = 40
    start: str = "2021-01-01"
    end: str = "2024-12-31"
    signal: bool = True
    beta: float = 0.002  # daily drift per unit of the hidden quarterly signal
    rho: float = 0.8  # AR(1) of the hidden signal across quarters
    idio_vol: float = 0.015
    market_vol: float = 0.01
    report_noise: float = 0.5
    restatement_noise: float = 0.2
    restatement_prob: float = 0.3

    def entities(self) -> list[str]:
        return [f"SYN{i:03d}" for i in range(self.n_entities)]


def _quarter_ends() -> pd.DatetimeIndex:
    return pd.date_range(CALENDAR_START - pd.offsets.QuarterEnd(1), CALENDAR_END, freq="QE")


def _business_days() -> pd.DatetimeIndex:
    return pd.bdate_range(CALENDAR_START, CALENDAR_END)


def _hidden_signal(cfg: MarketConfig, rng: np.random.Generator, n_q: int) -> np.ndarray:
    s = np.empty((cfg.n_entities, n_q))
    s[:, 0] = rng.standard_normal(cfg.n_entities)
    shock = np.sqrt(1.0 - cfg.rho**2)
    for q in range(1, n_q):
        s[:, q] = cfg.rho * s[:, q - 1] + shock * rng.standard_normal(cfg.n_entities)
    return s


def generate(cfg: MarketConfig) -> dict[str, pd.DataFrame]:
    """Return ``{"prices": ..., "fundamentals": ...}`` cut at ``cfg.end``."""
    rng = np.random.default_rng(cfg.seed)
    entities = cfg.entities()
    qends = _quarter_ends()
    days = _business_days()
    n_e, n_q, n_d = len(entities), len(qends), len(days)

    s = _hidden_signal(cfg, rng, n_q)

    # Quarter index of each business day: the quarter whose end is the first
    # quarter end on or after the day.
    q_of_day = np.searchsorted(qends.values, days.values, side="left")
    beta = cfg.beta if cfg.signal else 0.0
    market = cfg.market_vol * rng.standard_normal(n_d)
    idio = cfg.idio_vol * rng.standard_normal((n_e, n_d))
    rets = beta * s[:, q_of_day] + market[None, :] + idio
    close = 100.0 * np.exp(np.cumsum(rets, axis=1))
    volume = np.exp(13.0 + 0.4 * rng.standard_normal((n_e, n_d))).round()

    event_time = days + CLOSE_HOUR
    prices = pd.DataFrame(
        {
            "entity": np.repeat(entities, n_d),
            "date": np.tile(days.values, n_e),
            "event_time": np.tile(event_time.values, n_e),
            "published_at": np.tile((event_time + PRICE_PUBLICATION_LAG).values, n_e),
            "close": close.ravel(),
            "volume": volume.ravel(),
        }
    )

    # Fundamentals: one report per quarter, some restated later.
    lag_days = rng.integers(25, 46, size=(n_e, n_q))
    report_noise = cfg.report_noise * rng.standard_normal((n_e, n_q))
    restated = rng.random((n_e, n_q)) < cfg.restatement_prob
    restate_after = rng.integers(10, 61, size=(n_e, n_q))
    restate_noise = cfg.restatement_noise * rng.standard_normal((n_e, n_q))

    rows = []
    for i, ent in enumerate(entities):
        for q, qe in enumerate(qends):
            first = qe + pd.Timedelta(days=int(lag_days[i, q])) + REPORT_HOUR
            rows.append((ent, qe, first, s[i, q] + report_noise[i, q], False))
            if restated[i, q]:
                later = first + pd.Timedelta(days=int(restate_after[i, q]))
                rows.append((ent, qe, later, s[i, q] + restate_noise[i, q], True))
    fundamentals = pd.DataFrame(rows, columns=["entity", "event_time", "published_at", "score", "is_restatement"])

    start = pd.Timestamp(cfg.start)
    cutoff = pd.Timestamp(cfg.end) + pd.Timedelta(days=1)  # end of the end day
    prices = prices[(prices["date"] >= start) & (prices["published_at"] < cutoff)]
    # Keep one quarter of fundamentals history before ``start`` so the first
    # days of prices already have a reported score.
    fundamentals = fundamentals[
        (fundamentals["event_time"] >= start - pd.offsets.QuarterEnd(2)) & (fundamentals["published_at"] < cutoff)
    ]
    return {
        "prices": prices.reset_index(drop=True),
        "fundamentals": fundamentals.sort_values(["entity", "published_at"]).reset_index(drop=True),
    }


def hidden_signal_by_quarter(cfg: MarketConfig) -> pd.DataFrame:
    """The true quarterly signal (for tests and the leakage illustration only)."""
    rng = np.random.default_rng(cfg.seed)
    qends = _quarter_ends()
    s = _hidden_signal(cfg, rng, len(qends))
    return pd.DataFrame(
        {
            "entity": np.repeat(cfg.entities(), len(qends)),
            "quarter_end": np.tile(qends.values, cfg.n_entities),
            "signal": s.ravel(),
        }
    )

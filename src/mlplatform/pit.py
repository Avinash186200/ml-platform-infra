"""Point-in-time ("as of") semantics, in one place.

A feature record is ``(entity, feature, event_time, available_at, value)``.
``event_time`` is the moment the value describes (a close, a quarter end);
``available_at`` is when it could first be known. A restatement is a second
record with the same ``event_time`` and a later ``available_at``.

**The rule.** The value of feature ``f`` for entity ``e`` as of time ``t`` is
taken from the records with ``available_at <= t`` only. Among those, the one
with the latest ``event_time`` wins, and among equal ``event_time`` the latest
``available_at`` (the newest restatement known at ``t``). A restatement of an
old quarter that arrives after a newer quarter was reported therefore does
not replace the newer quarter.

Three implementations follow the rule and are tested against each other:
``asof_bruteforce`` (the reference, loops), ``asof_join`` (vectorised, for
building training sets) and the SQL query in ``store.py`` (for serving).
"""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

RECORD_COLUMNS = ["entity", "feature", "event_time", "available_at", "value"]


def to_us(ts) -> int:
    """A timestamp (naive = UTC, or tz-aware) to integer microseconds since the epoch."""
    t = pd.Timestamp(ts)
    if t.tzinfo is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    return int(t.value // 1000)


def from_us(us: int) -> pd.Timestamp:
    return pd.Timestamp(int(us) * 1000, tz="UTC")


def series_to_us(s: pd.Series) -> np.ndarray:
    return (pd.to_datetime(s).astype("datetime64[us]").astype("int64")).to_numpy()


def asof_bruteforce(records: pd.DataFrame, entity: str, as_of_us: int, features: Iterable[str]) -> dict:
    """Reference implementation: ``{feature: (value, event_time, available_at)}``."""
    out = {}
    for f in features:
        best = None
        for row in records.itertuples(index=False):
            if row.entity != entity or row.feature != f or row.available_at > as_of_us:
                continue
            key = (row.event_time, row.available_at)
            if best is None or key > best[0]:
                best = (key, row.value)
        if best is not None:
            out[f] = (best[1], best[0][0], best[0][1])
    return out


def asof_join(records: pd.DataFrame, queries: pd.DataFrame, features: Iterable[str]) -> pd.DataFrame:
    """Vectorised as-of join.

    ``queries`` has columns ``entity`` and ``as_of`` (int64 microseconds). The
    result has the query columns plus, per feature ``f``, the columns ``f``
    (value, NaN when nothing is known yet), ``f__event_time`` and
    ``f__available_at``. Row order follows ``queries``.

    Method: sort a feature's records by ``(event_time, available_at)`` and call
    the position the record's rank. Walking the records in ``available_at``
    order, the record the rule picks at any moment is the one with the highest
    rank seen so far, so a running maximum of the rank per entity, looked up
    with ``merge_asof`` at each query time, gives the answer.
    """
    q = queries[["entity", "as_of"]].copy()
    q["_row"] = np.arange(len(q))
    q = q.sort_values("as_of", kind="mergesort")
    out = q.copy()
    for f in features:
        rec = records[records["feature"] == f]
        cols = [f, f"{f}__event_time", f"{f}__available_at"]
        if rec.empty:
            for c in cols:
                out[c] = np.nan
            continue
        rec = rec.sort_values(["event_time", "available_at"], kind="mergesort").reset_index(drop=True)
        rec["_rank"] = np.arange(len(rec))
        walk = rec.sort_values(["available_at", "_rank"], kind="mergesort")
        walk["_best"] = walk.groupby("entity", sort=False)["_rank"].cummax()
        # Several records can share an available_at; the last running max wins.
        walk = walk.groupby(["entity", "available_at"], as_index=False)["_best"].max()
        walk = walk.sort_values("available_at", kind="mergesort")
        merged = pd.merge_asof(
            out[["_row", "entity", "as_of"]],
            walk,
            left_on="as_of",
            right_on="available_at",
            by="entity",
            direction="backward",
            allow_exact_matches=True,
        )
        best = merged["_best"].to_numpy()
        has = ~np.isnan(best)
        idx = np.where(has, best, 0).astype(np.int64)
        values = np.where(has, rec["value"].to_numpy()[idx], np.nan)
        et = np.where(has, rec["event_time"].to_numpy()[idx], np.nan)
        av = np.where(has, rec["available_at"].to_numpy()[idx], np.nan)
        out[cols[0]] = values
        out[cols[1]] = et
        out[cols[2]] = av
    out = out.sort_values("_row", kind="mergesort").drop(columns="_row").reset_index(drop=True)
    return out

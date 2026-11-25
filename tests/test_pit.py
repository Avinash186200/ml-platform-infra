"""The as-of rule, three implementations, one reference."""

import numpy as np
import pandas as pd
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from mlplatform.pit import RECORD_COLUMNS, asof_bruteforce, asof_join, from_us, to_us
from mlplatform.store import SnapshotReader, write_snapshot

FEATS = ["a", "b"]


def frame(rows):
    return pd.DataFrame(rows, columns=RECORD_COLUMNS)


def test_restatement_visible_only_after_it_is_published():
    recs = frame(
        [
            ("X", "a", 100, 200, 1.0),  # first report of period 100 at t=200
            ("X", "a", 100, 300, 1.5),  # restated at t=300
        ]
    )
    assert asof_bruteforce(recs, "X", 199, FEATS) == {}
    assert asof_bruteforce(recs, "X", 200, FEATS)["a"][0] == 1.0  # inclusive at the publication time
    assert asof_bruteforce(recs, "X", 299, FEATS)["a"][0] == 1.0
    assert asof_bruteforce(recs, "X", 300, FEATS)["a"][0] == 1.5


def test_late_restatement_of_an_old_period_does_not_replace_a_newer_period():
    recs = frame(
        [
            ("X", "a", 100, 200, 1.0),  # Q1 reported
            ("X", "a", 110, 250, 2.0),  # Q2 reported
            ("X", "a", 100, 260, 1.1),  # Q1 restated after Q2 was reported
        ]
    )
    for impl in ("brute", "join", "sql"):
        got = lookup(impl, recs, "X", 270)
        assert got["a"][0] == 2.0, impl


def test_entities_and_features_do_not_mix():
    recs = frame([("X", "a", 1, 1, 1.0), ("Y", "a", 2, 2, 2.0), ("X", "b", 3, 3, 3.0)])
    got = asof_bruteforce(recs, "X", 10, FEATS)
    assert got == {"a": (1.0, 1, 1), "b": (3.0, 3, 3)}


def test_to_us_handles_timezones():
    assert to_us("2024-03-01T17:00:00-05:00") == to_us("2024-03-01T22:00:00")
    assert to_us(pd.Timestamp("2024-03-01 22:00", tz="UTC")) == to_us("2024-03-01 22:00")
    assert from_us(to_us("2024-03-01T22:00:00.000123")).isoformat() == "2024-03-01T22:00:00.000123+00:00"


def lookup(impl, recs, entity, t):
    if impl == "brute":
        return asof_bruteforce(recs, entity, t, FEATS)
    if impl == "join":
        j = asof_join(recs, pd.DataFrame({"entity": [entity], "as_of": [t]}), FEATS).iloc[0]
        return {
            f: (j[f], int(j[f"{f}__event_time"]), int(j[f"{f}__available_at"])) for f in FEATS if not np.isnan(j[f])
        }
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "s.sqlite"
        write_snapshot(recs, p, 1)
        r = SnapshotReader(p)
        try:
            return r.get_asof(entity, t, FEATS)
        finally:
            r.close()


record = st.tuples(
    st.sampled_from(["X", "Y"]),
    st.sampled_from(FEATS),
    st.integers(0, 30),  # event_time
    st.integers(0, 40),  # publication delay
    st.floats(-5, 5, allow_nan=False),
)


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    rows=st.lists(record, min_size=1, max_size=40),
    queries=st.lists(st.tuples(st.sampled_from(["X", "Y"]), st.integers(-5, 80)), min_size=1, max_size=15),
)
def test_all_implementations_agree_with_the_reference(rows, queries, tmp_path_factory):
    recs = frame([(e, f, et, et + d, v) for e, f, et, d, v in rows])
    recs = recs.drop_duplicates(subset=["entity", "feature", "event_time", "available_at"]).reset_index(drop=True)
    q = pd.DataFrame(queries, columns=["entity", "as_of"])
    joined = asof_join(recs, q, FEATS)
    path = tmp_path_factory.mktemp("h") / "s.sqlite"
    write_snapshot(recs, path, 1)
    reader = SnapshotReader(path)
    try:
        for i, (ent, t) in enumerate(queries):
            ref = asof_bruteforce(recs, ent, t, FEATS)
            assert reader.get_asof(ent, t, FEATS) == ref
            row = joined.iloc[i]
            via_join = {
                f: (row[f], int(row[f"{f}__event_time"]), int(row[f"{f}__available_at"]))
                for f in FEATS
                if not np.isnan(row[f])
            }
            assert via_join == ref
            # and never anything published after t
            assert all(av <= t for (_, _, av) in ref.values())
    finally:
        reader.close()


def test_join_preserves_query_order_and_unknown_entities(small_records):
    q = pd.DataFrame({"entity": ["SYN001", "NOPE", "SYN000"], "as_of": [to_us("2023-03-01 22:00")] * 3})
    j = asof_join(small_records, q, ["ret_5d", "fundamental_score"])
    assert list(j["entity"]) == ["SYN001", "NOPE", "SYN000"]
    assert np.isnan(j.loc[1, "ret_5d"]) and not np.isnan(j.loc[0, "ret_5d"])


@pytest.mark.parametrize("feature", ["ret_5d", "fundamental_score"])
def test_join_on_real_records_matches_reference(small_records, feature):
    rng = np.random.default_rng(0)
    lo, hi = small_records["available_at"].min(), small_records["available_at"].max()
    ents = sorted(small_records["entity"].unique())
    q = pd.DataFrame(
        {
            "entity": [ents[i] for i in rng.integers(0, len(ents), 30)],
            "as_of": rng.integers(lo, hi, 30),
        }
    )
    j = asof_join(small_records, q, [feature])
    sub = small_records[small_records["feature"] == feature]
    for i, r in q.iterrows():
        ref = asof_bruteforce(sub[sub["entity"] == r["entity"]], r["entity"], int(r["as_of"]), [feature])
        if ref:
            assert j.loc[i, feature] == ref[feature][0]
        else:
            assert np.isnan(j.loc[i, feature])

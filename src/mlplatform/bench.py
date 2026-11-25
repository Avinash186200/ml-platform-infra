"""The full run behind every number in README.md and results/RESULTS.md.

``python -m mlplatform.cli bench --out results`` writes:

* ``pipeline.json``  the Dagster asset graph run in-process, signal and control mode
* ``leakage.json``   the same model with a look-ahead fundamental join, for contrast
* ``pit_audit.json`` random as-of queries checked against the reference rule
* ``cache.json``     a request replay across a snapshot switch, versioned key vs a naive key

Nothing here is timed, so every field is deterministic and CI can compare a
fresh run against the committed files. Needs the ``dev`` extra (fakeredis).
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from .artifacts import FEATURES_POINTER, MODELS_POINTER, ArtifactStore
from .features import ALL_FEATURES, MODEL_FEATURES
from .pit import asof_bruteforce, asof_join, to_us
from .store import SnapshotReader, write_snapshot
from .training import TrainingConfig, evaluate, fit, leaky_training_frame, purged_split

BENCH_MARKET = {"seed": 7, "n_entities": 40, "start": "2021-01-01", "end": "2024-12-31"}
BENCH_TRAINING = {"test_start": "2024-01-01"}
PIPELINE_ASSETS = [
    "raw_prices",
    "raw_fundamentals",
    "feature_snapshot",
    "publish_feature_snapshot",
    "training_set",
    "candidate_model",
    "model_evaluation",
    "model_artifact",
]


def _resources(base_url: str, signal: bool, market: dict, training: dict) -> dict:
    from .orchestration.resources import ArtifactStoreResource, MarketDataResource, TrainingResource

    return {
        "artifacts": ArtifactStoreResource(base_url=base_url),
        "market": MarketDataResource(signal=signal, **market),
        "training": TrainingResource(**training),
    }


def run_pipeline(base_url: str, signal: bool, market: dict = BENCH_MARKET, training: dict = BENCH_TRAINING) -> dict:
    """Materialize the asset graph in-process, then run the promotion sensor once."""
    from dagster import DagsterInstance, build_sensor_context, materialize

    from .orchestration.definitions import ALL_ASSETS, ALL_CHECKS, promote_on_approval

    resources = _resources(base_url, signal, market, training)
    instance = DagsterInstance.ephemeral()
    result = materialize(
        [*ALL_ASSETS, *ALL_CHECKS],
        resources=resources,
        instance=instance,
        selection=PIPELINE_ASSETS,
        raise_on_error=True,
    )
    materialized = sorted({e.asset_key.to_user_string() for e in result.get_asset_materialization_events()})
    checks = {c.check_name: bool(c.passed) for c in result.get_asset_check_evaluations()}

    tick = promote_on_approval.evaluate_tick(build_sensor_context(instance=instance))
    promoted = False
    if tick.run_requests:
        promote = materialize(
            [*ALL_ASSETS], resources=resources, instance=instance, selection=["deployed_model"], raise_on_error=True
        )
        promoted = promote.success

    store = ArtifactStore(base_url)
    info = store.read_json("training/info.json")
    cand = store.read_json("models/candidates/LATEST")
    report = store.read_json(f"models/candidates/{cand['model_id']}/evaluation.json")
    raw_f = store.read_parquet("raw/fundamentals.parquet")
    return {
        "mode": "signal" if signal else "control",
        "run_success": bool(result.success),
        "materialized": materialized,
        "checks": dict(sorted(checks.items())),
        "raw_price_rows": len(store.read_parquet("raw/prices.parquet")),
        "raw_fundamental_rows": len(raw_f),
        "restatements": int(raw_f["is_restatement"].sum()),
        "snapshot_version": int(store.get_pointer(FEATURES_POINTER)["version"]),
        # the snapshot's key and hash depend on SQLite's file layout, not on the data
        "training": {k: v for k, v in info.items() if not k.startswith("snapshot_")},
        "metrics": report["metrics"],
        "gate": report["gate"],
        "sensor_run_requests": len(tick.run_requests),
        "promoted": bool(promoted),
        "models_current_set": store.get_pointer(MODELS_POINTER) is not None,
    }


def leakage_illustration(base_url: str) -> dict:
    """Same rows, same model, two ways of joining the fundamental score."""
    store = ArtifactStore(base_url)
    cfg = TrainingConfig(**BENCH_TRAINING)
    train = store.read_parquet("training/train.parquet")
    test = store.read_parquet("training/test.parquet")
    fundamentals = store.read_parquet("raw/fundamentals.parquet")
    frame = pd.concat([train, test], ignore_index=True)
    leaky = leaky_training_frame(frame, fundamentals)
    # Restrict both versions to the rows the leaky join can fill, so they are scored on identical rows.
    keys = leaky[["entity", "date"]]
    pit = frame.merge(keys, on=["entity", "date"], how="inner")
    out = {}
    for name, f in [("point_in_time", pit), ("look_ahead_join", leaky)]:
        tr, te, _ = purged_split(f, cfg)
        model = fit(tr, cfg, trained_on={})
        m = evaluate(model, tr, te, cfg)
        out[name] = {k: m[k] for k in ["n_train", "n_test", "auc", "auc_ci95_low", "auc_ci95_high", "log_loss"]}
        out[name]["coef_fundamental_score"] = m["coef"]["fundamental_score"]
    return out


def pit_audit(snapshot_path: Path, n_queries: int = 300, seed: int = 3) -> dict:
    """Random and adversarial as-of queries: SQL lookup vs vectorised join vs the reference loop."""
    reader = SnapshotReader(snapshot_path)
    try:
        recs = reader.all_records()
        rng = np.random.default_rng(seed)
        entities = sorted(recs["entity"].unique())
        fund = recs[recs["feature"] == "fundamental_score"]
        # Adversarial times: exactly at, and one microsecond before, each fundamental publication.
        edges = {
            ent: np.concatenate([g["available_at"].to_numpy(), g["available_at"].to_numpy() - 1])
            for ent, g in fund.groupby("entity")
        }
        lo, hi = int(recs["available_at"].min()), int(recs["available_at"].max())
        queries = []
        for k in range(n_queries):
            ent = entities[rng.integers(len(entities))]
            if k % 2 == 0:
                t = int(rng.integers(lo - 86_400_000_000, hi + 86_400_000_000))
            else:
                t = int(edges[ent][rng.integers(len(edges[ent]))])
            queries.append((ent, t))
        groups = {key: g for key, g in recs.groupby(["entity", "feature"])}
        qdf = pd.DataFrame(queries, columns=["entity", "as_of"])
        joined = asof_join(recs, qdf, ALL_FEATURES)

        mismatch_sql = mismatch_join = lookahead = 0
        restatement_window_queries = restatement_window_original = 0
        for i, (ent, t) in enumerate(queries):
            expected = {}
            for f in ALL_FEATURES:
                sub = groups.get((ent, f))
                if sub is not None:
                    expected.update(asof_bruteforce(sub, ent, t, [f]))
            got = reader.get_asof(ent, t, ALL_FEATURES)
            if got != expected:
                mismatch_sql += 1
            row = joined.iloc[i]
            via_join = {
                f: (row[f], int(row[f"{f}__event_time"]), int(row[f"{f}__available_at"]))
                for f in ALL_FEATURES
                if not np.isnan(row[f])
            }
            if via_join != expected:
                mismatch_join += 1
            if any(av > t for (_, _, av) in got.values()):
                lookahead += 1
            # Was this query between a first report and its restatement?
            fe = fund[fund["entity"] == ent]
            for _, g in fe.groupby("event_time"):
                if len(g) > 1:
                    first, last = g["available_at"].min(), g["available_at"].max()
                    if (
                        first <= t < last
                        and "fundamental_score" in got
                        and got["fundamental_score"][1] == g["event_time"].iloc[0]
                    ):
                        restatement_window_queries += 1
                        if got["fundamental_score"][2] == first:
                            restatement_window_original += 1
        return {
            "queries": n_queries,
            "queries_at_publication_edges": n_queries // 2,
            "records_in_snapshot": len(recs),
            "sql_vs_reference_mismatches": mismatch_sql,
            "vectorised_join_vs_reference_mismatches": mismatch_join,
            "responses_with_available_at_after_as_of": lookahead,
            "queries_inside_a_restatement_window": restatement_window_queries,
            "of_which_returned_the_original_report": restatement_window_original,
        }
    finally:
        reader.close()


def cache_replay(snapshot_path: Path, workdir: Path, n_requests: int = 3000, seed: int = 5) -> dict:
    """Replay requests through FeatureService with fakeredis; publish a corrected
    snapshot halfway; compare every response with an uncached lookup."""
    import fakeredis

    from .service.cache import FeatureCache
    from .service.feature_service import FeatureService
    from .service.snapshots import SnapshotManager

    base = SnapshotReader(snapshot_path)
    recs = base.all_records()
    base.close()
    rng = np.random.default_rng(seed)
    entities = sorted(recs["entity"].unique())

    # v2 = v1 plus late-arriving corrections: new fundamental records whose
    # available_at is in the past (a vendor correction loaded after the fact).
    fund = recs[recs["feature"] == "fundamental_score"]
    picks = fund.iloc[rng.choice(len(fund), size=60, replace=False)].copy()
    picks["available_at"] = picks["available_at"] + 3_600_000_000  # one hour after the original
    picks["value"] = picks["value"] + 1.0
    picks = picks[
        ~picks.set_index(["entity", "feature", "event_time", "available_at"]).index.isin(
            recs.set_index(["entity", "feature", "event_time", "available_at"]).index
        )
    ]
    v2 = pd.concat([recs, picks], ignore_index=True)

    store = ArtifactStore(str(workdir / "artifacts"))
    for version, frame in [(1, recs), (2, v2)]:
        local = workdir / f"build-v{version}.sqlite"
        write_snapshot(frame, local, version)
        store.upload_file(local, f"features/v{version:06d}.sqlite")
    store.set_pointer(FEATURES_POINTER, {"version": 1, "key": "features/v000001.sqlite"})

    snapshots = SnapshotManager(store, workdir / "serving", refresh_seconds=0)
    service = FeatureService(snapshots, FeatureCache(fakeredis.FakeRedis(), ttl_seconds=86_400))
    truth = {v: SnapshotReader(workdir / f"build-v{v}.sqlite") for v in (1, 2)}

    # Historical queries focus on times just after the corrected publications, where v1 and v2 differ.
    hist_pool = [
        (r.entity, int(r.available_at) + 3_600_000_000 + int(rng.integers(0, 86_400_000_000)))
        for r in picks.itertuples(index=False)
    ]
    hist_pool += [
        (
            entities[rng.integers(len(entities))],
            int(rng.integers(fund["available_at"].min(), fund["available_at"].max())),
        )
        for _ in range(240)
    ]
    latest_as_of = to_us("2026-01-01")

    naive: dict = {}
    phase = {
        "before": {"requests": 0, "hits": 0, "store_queries": 0, "stale": 0, "naive_stale": 0},
        "after": {"requests": 0, "hits": 0, "store_queries": 0, "stale": 0, "naive_stale": 0},
    }
    for k in range(n_requests):
        if k == n_requests // 2:
            store.set_pointer(FEATURES_POINTER, {"version": 2, "key": "features/v000002.sqlite"})
        ph = phase["before" if k < n_requests // 2 else "after"]
        if rng.random() < 0.6:
            ent, t = entities[rng.integers(len(entities))], latest_as_of
        else:
            ent, t = hist_pool[rng.integers(len(hist_pool))]
        reader = snapshots.current()
        q0 = reader.queries
        fv = service.get(ent, t, MODEL_FEATURES)
        ph["requests"] += 1
        ph["hits"] += fv.cache == "hit"
        ph["store_queries"] += snapshots.current().queries - q0
        expected = truth[fv.snapshot_version].get_asof(
            ent, min(t, truth[fv.snapshot_version].max_available_at), MODEL_FEATURES
        )
        got = {f: (d["value"], d["available_at_us"]) for f, d in fv.values.items()}
        want = {f: (v, av) for f, (v, _, av) in expected.items()}
        ph["stale"] += got != want
        # A naive cache keyed only on (entity, as_of), with no snapshot version.
        nkey = (ent, t)
        if nkey not in naive:
            naive[nkey] = want
        ph["naive_stale"] += naive[nkey] != want
    for r in truth.values():
        r.close()
    snapshots.close()
    for ph in phase.values():
        ph["hit_rate"] = ph["hits"] / ph["requests"]
    return {
        "requests": n_requests,
        "share_latest_queries": 0.6,
        "historical_query_pool": len(hist_pool),
        "late_corrections_in_v2": len(picks),
        "snapshot_switch_at_request": n_requests // 2,
        "versioned_key": phase,
        "cache_errors": service.cache.errors,
    }


def run_bench(out_dir: str | Path) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mlpi-bench-") as tmp:
        tmp = Path(tmp)
        pipeline = {}
        for signal in (True, False):
            base = tmp / ("signal" if signal else "control")
            pipeline["signal" if signal else "control"] = run_pipeline(str(base), signal)
        leakage = {
            "signal": leakage_illustration(str(tmp / "signal")),
            "control": leakage_illustration(str(tmp / "control")),
        }
        snap = tmp / "signal" / "features" / "v000001.sqlite"
        audit = pit_audit(snap)
        cache = cache_replay(snap, tmp / "replay")
    results = {
        "pipeline": {"config": {"market": BENCH_MARKET, "training": BENCH_TRAINING}, **pipeline},
        "leakage": leakage,
        "pit_audit": audit,
        "cache": cache,
    }
    for name, obj in results.items():
        (out / f"{name}.json").write_text(
            json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
        )
    (out / "RESULTS.md").write_text(render(results), encoding="utf-8", newline="\n")
    return results


def load_results(out_dir: str | Path) -> dict:
    out = Path(out_dir)
    return {
        n: json.loads((out / f"{n}.json").read_text(encoding="utf-8"))
        for n in ["pipeline", "leakage", "pit_audit", "cache"]
    }


def render(r: dict) -> str:
    p, lk, a, c = r["pipeline"], r["leakage"], r["pit_audit"], r["cache"]
    s, ctl = p["signal"], p["control"]
    lines = [
        "# Results",
        "",
        "Synthetic data only (generator in `src/mlplatform/synthetic.py`). Rebuild with `make bench`;",
        "`tests/test_bench.py` reruns it and compares with these files.",
        "",
        (
            f"Market: {p['config']['market']['n_entities']} entities, {p['config']['market']['start']} to "
            f"{p['config']['market']['end']}, seed {p['config']['market']['seed']}. Test block: decisions from "
            f"{p['config']['training']['test_start']}."
        ),
        "",
        "## 1. Pipeline run (Dagster asset graph, in-process)",
        "",
        "| | signal | control |",
        "|---|---|---|",
        f"| raw price rows | {s['raw_price_rows']:,} | {ctl['raw_price_rows']:,} |",
        f"| raw fundamental rows (restatements) | {s['raw_fundamental_rows']:,} ({s['restatements']}) | {ctl['raw_fundamental_rows']:,} ({ctl['restatements']}) |",
        f"| training rows / test rows | {s['training']['n_train']:,} / {s['training']['n_test']:,} | {ctl['training']['n_train']:,} / {ctl['training']['n_test']:,} |",
        f"| rows dropped by purge + embargo | {s['training']['n_dropped_purge_or_embargo']} | {ctl['training']['n_dropped_purge_or_embargo']} |",
        f"| held-out AUC | {s['metrics']['auc']:.4f} | {ctl['metrics']['auc']:.4f} |",
        f"| AUC 95% CI (10-day block bootstrap) | {s['metrics']['auc_ci95_low']:.4f} to {s['metrics']['auc_ci95_high']:.4f} | {ctl['metrics']['auc_ci95_low']:.4f} to {ctl['metrics']['auc_ci95_high']:.4f} |",
        f"| log loss vs constant prior | {s['metrics']['log_loss']:.4f} vs {s['metrics']['baseline_log_loss']:.4f} | {ctl['metrics']['log_loss']:.4f} vs {ctl['metrics']['baseline_log_loss']:.4f} |",
        f"| accuracy | {s['metrics']['accuracy']:.4f} | {ctl['metrics']['accuracy']:.4f} |",
        f"| gate passed | {s['gate']['passed']} | {ctl['gate']['passed']} |",
        f"| model_artifact materialized | {'model_artifact' in s['materialized']} | {'model_artifact' in ctl['materialized']} |",
        f"| promotion sensor run requests | {s['sensor_run_requests']} | {ctl['sensor_run_requests']} |",
        f"| models/CURRENT set | {s['models_current_set']} | {ctl['models_current_set']} |",
        f"| asset checks passed | {sum(s['checks'].values())} / {len(s['checks'])} | {sum(ctl['checks'].values())} / {len(ctl['checks'])} |",
        "",
        "Gate checks (signal): " + ", ".join(f"`{k}`={v}" for k, v in s["gate"]["checks"].items()),
        "",
        "Gate checks (control): " + ", ".join(f"`{k}`={v}" for k, v in ctl["gate"]["checks"].items()),
        "",
        "Signal-mode coefficients (standardised): "
        + ", ".join(f"`{k}` {v:+.4f}" for k, v in s["metrics"]["coef"].items()),
        "",
        "## 2. Look-ahead join, for contrast (not a result to trust)",
        "",
        "| mode | join | train rows | test rows | AUC | AUC 95% CI | fundamental coef |",
        "|---|---|---|---|---|---|---|",
    ]
    for mode in ["signal", "control"]:
        for name, label in [
            ("point_in_time", "point in time"),
            ("look_ahead_join", "by quarter, ignoring publication"),
        ]:
            m = lk[mode][name]
            lines.append(
                f"| {mode} | {label} | {m['n_train']:,} | {m['n_test']:,} | {m['auc']:.4f} | "
                f"{m['auc_ci95_low']:.4f} to {m['auc_ci95_high']:.4f} | {m['coef_fundamental_score']:+.4f} |"
            )
    lines += [
        "",
        "## 3. Point-in-time audit of the feature snapshot",
        "",
        "| check | value |",
        "|---|---|",
        f"| queries (half at a publication time or 1 µs before it) | {a['queries']} |",
        f"| records in snapshot | {a['records_in_snapshot']:,} |",
        f"| SQL lookup vs reference rule, mismatches | {a['sql_vs_reference_mismatches']} |",
        f"| vectorised join vs reference rule, mismatches | {a['vectorised_join_vs_reference_mismatches']} |",
        f"| responses containing a value published after as_of | {a['responses_with_available_at_after_as_of']} |",
        f"| queries between a first report and its restatement | {a['queries_inside_a_restatement_window']} |",
        f"| ... that returned the original report, as they should | {a['of_which_returned_the_original_report']} |",
        "",
        "## 4. Cache replay across a snapshot switch",
        "",
        (
            f"{c['requests']:,} requests through the feature service with fakeredis; {int(c['share_latest_queries'] * 100)}% ask for the latest values, "
            f"the rest repeat {c['historical_query_pool']} historical (entity, as_of) pairs. At request {c['snapshot_switch_at_request']:,} "
            f"snapshot v2 is published with {c['late_corrections_in_v2']} late-arriving corrections. Every response is compared with an uncached lookup "
            "on the snapshot it was served from. The last column replays the same requests through a cache keyed only on (entity, as_of)."
        ),
        "",
        "| phase | requests | cache hits | hit rate | snapshot queries | stale responses (versioned key) | stale responses (naive key) |",
        "|---|---|---|---|---|---|---|",
    ]
    for name in ["before", "after"]:
        ph = c["versioned_key"][name]
        lines.append(
            f"| {name} switch | {ph['requests']:,} | {ph['hits']:,} | {ph['hit_rate']:.3f} | {ph['store_queries']:,} | {ph['stale']} | {ph['naive_stale']} |"
        )
    lines.append("")
    return "\n".join(lines)

"""Command line: ``mlplatform bench | report | demo | submit-export | worker | worker-alive``."""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mlplatform")
    sub = parser.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bench", help="run everything behind results/ and write the JSON files and RESULTS.md")
    b.add_argument("--out", default="results")

    r = sub.add_parser("report", help="re-render RESULTS.md from the committed JSON")
    r.add_argument("--out", default="results")

    d = sub.add_parser("demo", help="run the asset graph once on small data into a local artifact dir")
    d.add_argument("--artifacts", default="data/artifacts")
    d.add_argument("--entities", type=int, default=20)

    j = sub.add_parser("submit-export", help="ask the feature API to export features for many entities (async job)")
    j.add_argument("--api", required=True, help="feature API base URL")
    j.add_argument("--entities", required=True, help="comma-separated entity ids")
    j.add_argument("--as-of", required=True, help="ISO-8601 timestamp")
    j.add_argument("--job-key", default=None, help="idempotency key, e.g. nightly-export-2024-12-31")

    w = sub.add_parser("worker", help="run the async job worker (needs MLPI_REDIS_URL)")
    w.add_argument("--worker-id", default=None)

    al = sub.add_parser("worker-alive", help="exit 0 if the worker touched its liveness file recently")
    al.add_argument("--file", default="/tmp/mlpi-worker-alive")
    al.add_argument("--max-age", type=float, default=300.0, help="seconds")

    args = parser.parse_args(argv)
    if args.cmd == "submit-export":
        import httpx

        body = {
            "kind": "feature_export",
            "params": {"as_of": args.as_of, "entities": [e for e in args.entities.split(",") if e]},
            "job_key": args.job_key,
        }
        resp = httpx.post(f"{args.api.rstrip('/')}/jobs", json=body, timeout=10)
        resp.raise_for_status()
        print(json.dumps(resp.json()))
        return 0
    if args.cmd == "worker-alive":
        from .service.jobs import worker_alive

        return 0 if worker_alive(args.file, args.max_age) else 1
    if args.cmd == "worker":
        import os
        import socket

        from .service.app import worker_from_env

        worker_from_env(args.worker_id or f"{socket.gethostname()}-{os.getpid()}").run_forever()
        return 0
    if args.cmd == "bench":
        from .bench import run_bench

        res = run_bench(args.out)
        print(json.dumps({k: res["pipeline"][k]["gate"]["passed"] for k in ("signal", "control")}))
        return 0
    if args.cmd == "report":
        from pathlib import Path

        from .bench import load_results, render

        Path(args.out, "RESULTS.md").write_text(render(load_results(args.out)), encoding="utf-8", newline="\n")
        return 0
    if args.cmd == "demo":
        from .bench import run_pipeline

        summary = run_pipeline(
            args.artifacts,
            signal=True,
            market={"seed": 7, "n_entities": args.entities, "start": "2021-01-01", "end": "2024-12-31"},
            training={"test_start": "2024-01-01", "min_test_rows": 1000},
        )
        print(json.dumps({k: summary[k] for k in ("snapshot_version", "promoted", "gate")}, indent=2))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())

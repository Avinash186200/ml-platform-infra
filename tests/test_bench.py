"""A full rerun of the bench must reproduce the committed results/ files."""

import math
from pathlib import Path

import pytest

from mlplatform.bench import load_results, render, run_bench

RESULTS = Path(__file__).resolve().parents[1] / "results"


def assert_close(fresh, committed, path="results"):
    if isinstance(committed, dict):
        assert isinstance(fresh, dict) and set(fresh) == set(committed), path
        for k in committed:
            assert_close(fresh[k], committed[k], f"{path}.{k}")
    elif isinstance(committed, list):
        assert isinstance(fresh, list) and len(fresh) == len(committed), path
        for i, (a, b) in enumerate(zip(fresh, committed, strict=True)):
            assert_close(a, b, f"{path}[{i}]")
    elif isinstance(committed, float):
        # float noise across platforms and BLAS builds, nothing more
        assert math.isclose(fresh, committed, rel_tol=1e-6, abs_tol=1e-9), (path, fresh, committed)
    else:
        assert fresh == committed, (path, fresh, committed)


def test_results_md_is_rendered_from_the_committed_json():
    committed = (RESULTS / "RESULTS.md").read_text(encoding="utf-8")
    assert render(load_results(RESULTS)) == committed


@pytest.mark.slow
def test_full_rerun_matches_committed_results(tmp_path):
    fresh = run_bench(tmp_path)
    assert_close(fresh, load_results(RESULTS))

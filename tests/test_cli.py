from mlplatform.cli import main


def test_report_rerenders(tmp_path):
    import shutil
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "results"
    for f in src.glob("*.json"):
        shutil.copy(f, tmp_path / f.name)
    assert main(["report", "--out", str(tmp_path)]) == 0
    assert (tmp_path / "RESULTS.md").read_text(encoding="utf-8") == (src / "RESULTS.md").read_text(encoding="utf-8")

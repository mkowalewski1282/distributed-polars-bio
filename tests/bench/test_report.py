"""Series report."""

from __future__ import annotations

import json

import polars as pl
import pytest

from bench import report
from bench.results import SCHEMA, ResultsWriter, to_parquet


def row(**kw) -> dict:
    r = dict.fromkeys(SCHEMA)
    r.update(
        op="overlap", pair="1-2", variant="polars_bio", parallelism=1, cpus="[0]", smt=False,
        rep=1, attempt=1, is_warmup=False, is_control=False, is_reference=False,
        valid=True, t_total_s=1.0,
    )
    r.update(kw)
    return r


def frame(rows):
    return pl.DataFrame(rows, schema=SCHEMA)


def cell(df, variant, p):
    return df.filter((pl.col("variant") == variant) & (pl.col("parallelism") == p)).row(0, named=True)


def test_summary_uses_only_valid_measured_runs():
    rows = [row(t_total_s=t, rep=i + 1) for i, t in enumerate([1.0, 3.0, 2.0])]
    rows += [
        row(is_warmup=True, rep=0, t_total_s=100.0),
        row(is_control=True, parallelism=2, t_total_s=100.0),
        row(is_reference=True, t_total_s=100.0),
        row(valid=False, invalid_reason="timeout", t_total_s=100.0),
    ]
    s = report.summarize(frame(rows))
    assert s.height == 1
    c = s.row(0, named=True)
    assert (c["n_valid"], c["median_s"], c["min_s"], c["max_s"]) == (3, 2.0, 1.0, 3.0)
    assert c["missing_reason"] is None


def test_speedups():
    rows = [
        row(t_total_s=2.0),
        row(variant="ballista", parallelism=1, t_total_s=4.0),
        row(variant="ballista", parallelism=2, t_total_s=1.0),
    ]
    s = report.summarize(frame(rows))
    b2 = cell(s, "ballista", 2)
    assert b2["speedup_vs_polars_bio_1"] == 2.0
    assert b2["speedup_self"] == 4.0


def test_missing_cell_shows_reason(tmp_path):
    reason = "skipped: P exceeds available CPUs"
    rows = [row(), row(variant="sail", parallelism=8, cpus="[]", smt=None, rep=0, valid=False,
                       invalid_reason=reason, t_total_s=None)]
    s = report.summarize(frame(rows))
    c = cell(s, "sail", 8)
    assert c["n_valid"] == 0 and c["missing_reason"] == reason
    _write_jsonl(tmp_path, rows)
    report.write_report(tmp_path)
    assert reason in (tmp_path / "summary.md").read_text()


def _write_jsonl(path, rows):
    w = ResultsWriter(path / "runs.jsonl")
    for r in rows:
        w.write(r)


def test_report_from_interrupted_series(tmp_path):
    _write_jsonl(tmp_path, [row(), row(parallelism=2, t_total_s=0.6)])
    assert not (tmp_path / "runs.parquet").exists()
    written = report.write_report(tmp_path)
    assert (tmp_path / "summary.md").exists() and (tmp_path / "summary.csv").exists()
    assert list((tmp_path / "plots").glob("*.png"))
    assert set(written) >= {tmp_path / "summary.md", tmp_path / "summary.csv"}


def test_engine_without_valid_runs_still_reports(tmp_path):
    rows = [row(), row(variant="sail", valid=False, invalid_reason="timeout", t_total_s=None)]
    _write_jsonl(tmp_path, rows)
    report.write_report(tmp_path)
    assert "timeout" in (tmp_path / "summary.md").read_text()


def test_smt_flag_is_carried():
    rows = [row(parallelism=8, smt=True, t_total_s=t) for t in (1.0, 1.2)]
    assert cell(report.summarize(frame(rows)), "polars_bio", 8)["smt"] is True


def test_old_schema_is_rejected(tmp_path):
    (tmp_path / "runs.jsonl").write_text(json.dumps({"n_nodes": 2}) + "\n")
    with pytest.raises(ValueError, match="schema"):
        report.load_runs(tmp_path)

"""Noise of the control run: how often two medians of 10 timings differ by more than the
threshold on an unchanged machine. The sample is the control runs from the start of a block
(the control variant differs from the series variants)."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench import control_noise as cn
from bench.config import Scenario, load
from bench.engines import CONTROL_VARIANT
from bench.results import SCHEMA, ResultsWriter, to_parquet

REPO = Path(__file__).resolve().parents[2]


def test_identical_times_never_drift():
    assert cn.false_drift_rate([1.5] * 20, group=10, tolerance=0.10, samples=500) == 0.0


def test_bimodal_times_sometimes_drift_and_result_is_reproducible():
    times = [1.0] * 10 + [2.0] * 10
    rate = cn.false_drift_rate(times, group=10, tolerance=0.10, samples=2000, seed=3)
    assert 0.0 < rate < 1.0
    assert rate == cn.false_drift_rate(times, group=10, tolerance=0.10, samples=2000, seed=3)


def test_too_few_times_is_an_error():
    with pytest.raises(ValueError, match="needs at least 20 times"):
        cn.false_drift_rate([1.0] * 19, group=10, tolerance=0.10)


def _row(**values) -> dict:
    row = dict.fromkeys(SCHEMA)
    row.update(
        variant=CONTROL_VARIANT, scenario_id="overlap/7-0", is_reference=False, is_warmup=False,
        is_control=True, control_position="start", valid=True, t_total_s=1.0,
    )
    row.update(values)
    return row


def _series_dir(tmp_path: Path, rows: list[dict]) -> Path:
    writer = ResultsWriter(tmp_path / "runs.jsonl")
    for row in rows:
        writer.write(row)
    to_parquet(writer.path, tmp_path / "runs.parquet")
    return tmp_path


def test_load_times_takes_valid_control_runs_from_block_start(tmp_path):
    measured = dict(variant="polars_bio", is_control=False, control_position=None)
    directory = _series_dir(tmp_path, [
        _row(t_total_s=1.1), _row(t_total_s=1.2),
        _row(t_total_s=9.0, valid=False), _row(t_total_s=9.0, control_position="end"),
        _row(t_total_s=9.0, **measured), _row(t_total_s=9.0, is_warmup=True, **measured),
        _row(t_total_s=9.0, is_reference=True, variant="polars_bio_ref", is_control=False,
             control_position=None),
    ])
    assert cn.load_times(directory) == [1.1, 1.2]


def test_main_reports_spread_and_false_drift_rate(tmp_path, capsys):
    directory = _series_dir(tmp_path, [_row(t_total_s=1.0 + i / 100) for i in range(20)])
    assert cn.main([str(directory)]) == 0
    out = capsys.readouterr().out
    assert "runs: 20" in out and "false drift rate" in out


def test_main_rejects_too_few_runs(tmp_path, capsys):
    directory = _series_dir(tmp_path, [_row() for _ in range(5)])
    assert cn.main([str(directory)]) == 2
    assert "needs at least 20 times" in capsys.readouterr().err


def test_noise_study_config():
    cfg = load(REPO / "bench" / "conf" / "control_noise.yaml")
    assert cfg.series == "control-noise" and cfg.scenarios == (Scenario("overlap", "7-0"),)
    assert (cfg.variants, cfg.parallelism, cfg.repeats) == (("polars_bio",), (1,), 1)
    assert cfg.control_repeats == 20 and cfg.control == Scenario("overlap", "7-0")
    assert cfg.control_tolerance == 100.0
    assert cfg.settle_s == 3.0

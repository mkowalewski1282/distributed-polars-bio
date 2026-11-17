"""The orchestrator on real engines and a test dataset in the databio-8p layout (pair 1-2,
dataset 1): all variants, P = 1 and 2, one run each; block CPUs from this machine's layout
(bench/cpus.py).

Reference result: polars-bio on 1 partition. With polars-bio 0.36 all runs are expected to be
valid, including polars-bio on multiple partitions, which in 0.28 computed merge and subtract
separately in each partition (bug #372, fixed in 0.29.0).

Requires the ballista_node and bench_client binaries (debug). Swap is excluded from the criteria
(pswpout and pswpin replaced by a constant): its detection is covered by unit tests, and here an
accidental swap by another application must not cause false failures."""

from __future__ import annotations

import functools
import json
import os

import polars as pl
import pytest
import yaml

from bench import metrics, preflight
from bench import orchestrator as orch
from bench.config import VARIANTS, parse
from bench.cpus import control_cpus, cpus_for, read_layout, uses_smt
from bench.data.datasets import DATASETS
from bench.engines import CONTROL_VARIANT, REFERENCE_VARIANT
from bench.ops import OPS, UNARY_OPS
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts

SCENARIOS = [{"op": op, "dataset": 1} if op in UNARY_OPS else {"op": op, "pair": "1-2"} for op in OPS]


@pytest.fixture(scope="module")
def data_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("data")
    write_parts(FIXTURE_A, root / "databio-8p" / DATASETS[1])
    write_parts(FIXTURE_B, root / "databio-8p" / DATASETS[2])
    return root


@pytest.fixture(scope="module")
def series(data_root, tmp_path_factory):
    cfg = parse({
        "series": "integration", "scenarios": SCENARIOS, "variants": list(VARIANTS),
        "parallelism": [1, 2], "repeats": 1, "warmup": 0, "seed": 1, "control_tolerance": 100.0,
        "control": {"op": "overlap", "pair": "1-2"}, "control_repeats": 1,
    })
    summary = orch.run_series(
        cfg, data_root=data_root, results_dir=tmp_path_factory.mktemp("results"),
        profile="debug", probe=orch.Probe(pswpout=lambda: 0, pswpin=lambda: 0),
    )
    return summary, pl.read_parquet(summary.parquet)


@pytest.mark.parallelism(2)
def test_every_planned_run_is_recorded(series):
    summary, df = series
    # reference: 5 scenarios (the overlap/1-2 control is among them); blocks: polars-bio, Ballista
    # and Sail at P = 1 and 2, each with 2 control runs + 5 scenarios
    assert df.height == 5 + 6 * (2 + 5)
    assert summary.rows == df.height and not summary.failures


@pytest.mark.parallelism(2)
def test_every_run_is_valid(series):
    _, df = series
    bad = df.filter(~pl.col("valid")).select("variant", "parallelism", "op", "invalid_reason")
    assert bad.is_empty(), bad.to_dicts()


@pytest.mark.parallelism(2)
def test_rows_record_parallelism_cpus_and_smt(series):
    """Columns in runs.parquet: the block's P, the engine CPUs from this machine's layout and the SMT flag."""
    _, df = series
    layout = read_layout()
    assert set(zip(df["variant"], df["parallelism"])) == {
        (REFERENCE_VARIANT, 1), (CONTROL_VARIANT, 2), ("polars_bio", 1), ("polars_bio", 2),
        ("ballista", 1), ("ballista", 2), ("sail", 1), ("sail", 2),
    }
    for row in df.iter_rows(named=True):
        if row["variant"] == CONTROL_VARIANT:
            expected = control_cpus(layout)
        else:
            expected = cpus_for(layout, row["parallelism"])
        assert json.loads(row["cpus"]) == list(expected), row
        assert row["smt"] is uses_smt(layout, expected), row


@pytest.mark.parallelism(2)
def test_partitions_follow_variant_and_parallelism(series):
    """Partitions = P; Ballista at least 2 (the binaries reject BIO_TARGET_PARTITIONS < 2)."""
    _, df = series
    partitions = {
        key: {json.loads(e)["target_partitions"] for e in group["extra"]}
        for key, group in df.filter(pl.col("variant") != "sail").group_by(["variant", "parallelism"])
    }
    assert partitions == {
        (REFERENCE_VARIANT, 1): {1}, (CONTROL_VARIANT, 2): {2}, ("polars_bio", 1): {1},
        ("polars_bio", 2): {2}, ("ballista", 1): {2}, ("ballista", 2): {2},
    }


@pytest.mark.parallelism(2)
def test_every_runner_reports_checksum_time_and_first_batch(series):
    """The checksum cost depends on the engine (Python or Rust); every runner reports it
    separately from t_total_s; the phase up to the first batch fits within t_total_s."""
    _, df = series
    assert all(json.loads(e)["checksum_s"] > 0 for e in df["extra"])
    first = [json.loads(p)["first_batch_s"] for p in df["phases"]]
    assert all(0 <= f <= t for f, t in zip(first, df["t_total_s"]))


@pytest.mark.parallelism(2)
def test_memory_is_measured_per_process(series):
    _, df = series

    def processes(variant: str, p: int) -> set[frozenset]:
        rows = df.filter((pl.col("variant") == variant) & (pl.col("parallelism") == p) & ~pl.col("is_control"))
        return {frozenset(json.loads(r)) for r in rows["peak_rss"]}

    assert processes("ballista", 2) == {frozenset({"runner", "scheduler", "executor_1", "executor_2"})}
    assert processes("sail", 1) == {frozenset({"runner", "sail_server"})}
    assert processes("polars_bio", 2) == {frozenset({"runner"})}
    assert (df["peak_rss_sum"] > 10 * 2**20).all()


@pytest.mark.parallelism(2)
def test_ballista_shuffle_volume_is_measured(series):
    _, df = series
    ballista = df.filter(pl.col("variant") == "ballista")
    assert (ballista.filter(pl.col("op").is_in(["merge", "subtract"]))["shuffle_bytes"] > 0).all()
    assert df.filter(pl.col("variant") != "ballista")["shuffle_bytes"].is_null().all()


@pytest.mark.parallelism(1)
def test_main_resolves_relative_data_root(data_root, tmp_path, monkeypatch):
    """Python runners run in the repository directory, and Ballista ones in the Ballista project
    directory (engines.BALLISTA_DIR); a relative data path must be converted to an absolute one
    before the series starts."""
    monkeypatch.chdir(data_root.parent)
    monkeypatch.setattr(preflight, "check_cpus", lambda *args, **kwargs: [])
    monkeypatch.setattr(orch, "Probe", functools.partial(orch.Probe, pswpout=lambda: 0, pswpin=lambda: 0))
    monkeypatch.setattr(metrics, "swap_used", lambda: 0)
    cfg = tmp_path / "series.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "paths", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["polars_bio"], "parallelism": [1], "repeats": 1, "warmup": 0, "seed": 1,
        "control_tolerance": 100.0,
        "control": {"op": "overlap", "pair": "1-2"}, "control_repeats": 1,
    }))
    affinity = os.sched_getaffinity(0)
    try:
        code = orch.main([str(cfg), "--data-root", data_root.name, "--results-dir", str(tmp_path / "results")])
    finally:
        os.sched_setaffinity(0, affinity)
    assert code == 0


@pytest.mark.parallelism(2)
def test_sail_reports_workers_and_phases(series):
    """Every Sail session has P workers (Sail 0.7.3, 1 slot per worker); Sail measures polars-bio
    time (4 chromosomes with a non-empty left side)."""
    _, df = series
    sail = df.filter(pl.col("variant").str.starts_with("sail") & ~pl.col("is_control"))
    assert set(sail["variant"]) == {"sail"}
    for row in sail.iter_rows(named=True):
        extra, phases = json.loads(row["extra"]), json.loads(row["phases"])
        assert extra["sail_workers"] == row["parallelism"], row
        assert phases["pb_s"] > 0 and extra["udf_groups"] == 4
        assert "lock_wait_s" not in phases


@pytest.mark.parallelism(2)
def test_ballista_broadcast_volume_is_measured(series):
    _, df = series
    ballista = df.filter((pl.col("variant") == "ballista") & ~pl.col("is_reference"))
    broadcast = ballista.filter(pl.col("op").is_in(["nearest", "coverage"]))
    assert (broadcast["broadcast_bytes"] > 0).all()
    assert (ballista.filter(~pl.col("op").is_in(["nearest", "coverage"]))["broadcast_bytes"] == 0).all()
    assert df.filter(pl.col("variant") != "ballista")["broadcast_bytes"].is_null().all()

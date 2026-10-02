"""Orkiestrator serii (plan 3a, Zadanie 9) na fałszywych silnikach i runnerze: logika bloków,
ważności, limitu czasu, strażnika pamięci, swapu, dryfu i przerwań — bez uruchamiania silników."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest
import yaml

from bench import engines, metrics
from bench import orchestrator as orch
from bench.config import parse
from bench.data.datasets import DATASETS
from bench.engines import Command
from bench.procutil import EngineError

REPO = Path(__file__).resolve().parents[2]
FAKE_RUNNER = Path(__file__).resolve().parent / "fake_runner.py"


class FakeEngine:
    """Silnik bez procesów: runnerem jest fake_runner.py z argumentami zależnymi od wariantu
    i scenariusza; start, stop i restart trafiają do wspólnego dziennika."""

    def __init__(self, variant, n_nodes, args_for, log):
        self.variant, self.n, self.args_for, self.log = variant, n_nodes, args_for, log
        self.pids: dict[str, int] = {}

    def start(self):
        self.log.append(("start", self.variant, self.n))

    def stop(self):
        self.log.append(("stop", self.variant, self.n))

    def restart(self):
        self.log.append(("restart", self.variant, self.n))

    def command(self, scenario):
        args = self.args_for(self.variant, self.n, scenario)
        return Command([sys.executable, str(FAKE_RUNNER), *args], dict(os.environ), REPO)

    def server_pids(self):
        return dict(self.pids)

    def shuffle_dirs(self):
        return []


def make_factory(args_for=lambda variant, n, scenario: [], customize=None):
    log: list[tuple] = []

    def factory(variant, n_nodes):
        engine = FakeEngine(variant, n_nodes, args_for, log)
        if customize:
            customize(engine)
        return engine

    factory.log = log
    return factory


def probe(**overrides) -> orch.Probe:
    functions = dict(
        mem_available=lambda: 4 << 30,
        pswpout=lambda: 0,
        pswpin=lambda: 0,
        peak_rss=lambda pid: 2048,
        reset_peak_rss=lambda pid: None,
        dir_size=lambda path: 0,
    )
    functions.update(overrides)
    return orch.Probe(**functions)


def config(**overrides):
    raw = {
        "series": "test",
        "scenarios": [{"op": "overlap", "pair": "1-2"}, {"op": "merge", "dataset": 1}],
        "variants": ["polars_bio_a", "ballista"],
        "nodes": [1],
        "repeats": 2,
        "warmup": 1,
        "timeout_s": 30,
        "seed": 7,
        "control_tolerance": 0.5,
    }
    raw.update(overrides)
    return parse(raw)


def run(tmp_path, cfg=None, **kwargs):
    kwargs.setdefault("factory", make_factory())
    kwargs.setdefault("probe", probe())
    return orch.run_series(
        cfg or config(), data_root=tmp_path, results_dir=tmp_path / "wyniki",
        out=lambda line: None, **kwargs,
    )


def rows(summary) -> pl.DataFrame:
    return pl.read_parquet(summary.parquet)


def block(df: pl.DataFrame, variant: str) -> pl.DataFrame:
    """Przebiegi bloku wariantu bez wzorca i przebiegów kontrolnych."""
    return df.filter(
        (pl.col("variant") == variant) & ~pl.col("is_control") & ~pl.col("is_reference")
    )


def test_series_runs_reference_then_blocks_with_control_warmup_and_rounds(tmp_path):
    factory = make_factory()
    summary = run(tmp_path, factory=factory)
    df = rows(summary)
    reference = df.filter(pl.col("is_reference"))
    assert reference["scenario_id"].to_list() == ["overlap/1-2", "merge/1"]
    assert set(reference["variant"]) == {"polars_bio_ref"}
    for variant in ("polars_bio_a", "ballista"):
        b = block(df, variant)
        assert b.filter(pl.col("is_warmup"))["rep"].to_list() == [0, 0]
        assert sorted(b.filter(~pl.col("is_warmup"))["rep"].to_list()) == [1, 1, 2, 2]
    control = df.filter(pl.col("is_control"))
    assert control["rep"].to_list() == [0, 1, 0, 1]
    assert set(control["scenario_id"]) == {"overlap/1-2"} and set(control["variant"]) == {"polars_bio_a"}
    assert df.height == 2 + 2 * (2 + 2 + 4)
    assert df["valid"].all() and summary.ok and (summary.rows, summary.invalid) == (df.height, 0)
    assert set(df["attempt"]) == {1}
    assert json.loads(df["peak_rss"][0]) == {"runner": 1234}
    assert factory.log == [
        ("start", "polars_bio_ref", 1), ("stop", "polars_bio_ref", 1),
        ("start", "polars_bio_a", 1), ("stop", "polars_bio_a", 1),
        ("start", "ballista", 1), ("stop", "ballista", 1),
    ]
    assert not (summary.directory / "tmp").exists()


def test_wrong_result_invalidates_only_that_scenario(tmp_path):
    def args_for(variant, n, scenario):
        return ["--checksum", "0x00000000000000bb"] if (variant, scenario.op) == ("ballista", "merge") else []

    summary = run(tmp_path, factory=make_factory(args_for))
    bad = rows(summary).filter(~pl.col("valid"))
    assert set(zip(bad["variant"], bad["op"])) == {("ballista", "merge")}
    assert bad.height == 3  # rozgrzewka i dwie rundy
    assert set(bad["invalid_reason"]) == {
        "suma kontrolna 0x00000000000000bb ≠ wzorzec 0x00000000000000aa"
    }
    assert not summary.ok and summary.invalid == 3


def test_engine_error_is_recorded_with_its_message(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "1"] if (variant, scenario.op) == ("ballista", "overlap") else []

    bad = rows(run(tmp_path, factory=make_factory(args_for))).filter(~pl.col("valid"))
    assert set(bad["scenario_id"]) == {"overlap/1-2"} and set(bad["variant"]) == {"ballista"}
    assert set(bad["invalid_reason"]) == {"kod wyjścia 1: silnik padł: błąd testowy"}


def test_garbage_on_stdout_invalidates_run(tmp_path):
    def args_for(variant, n, scenario):
        return ["--garbage"] if variant == "ballista" else []

    bad = rows(run(tmp_path, factory=make_factory(args_for))).filter(~pl.col("valid"))
    assert bad.height == 6
    assert all(r.startswith("stdout runnera: niepoprawny JSON") for r in bad["invalid_reason"])


def test_timeout_kills_runner_and_skips_scenario_for_rest_of_block(tmp_path):
    def args_for(variant, n, scenario):
        return ["--sleep", "60"] if (variant, scenario.op) == ("ballista", "merge") else []

    factory = make_factory(args_for)
    summary = run(tmp_path, cfg=config(timeout_s=1), factory=factory)
    merge = block(rows(summary), "ballista").filter(pl.col("op") == "merge").sort("rep")
    assert merge["invalid_reason"].to_list() == ["timeout", "pominięty: timeout", "pominięty: timeout"]
    assert merge["wall_s"][0] < 10 and merge["wall_s"][1:].is_null().all()
    assert ("restart", "ballista", 1) in factory.log


def test_memory_watchdog_kills_runner(tmp_path):
    marker = tmp_path / "pamiec_sie_konczy"

    def mem_available():
        if marker.exists():
            marker.unlink()
            return 100 * 2**20
        return 4 << 30

    def args_for(variant, n, scenario):
        if (variant, scenario.op) == ("ballista", "merge"):
            return ["--touch", str(marker), "--sleep", "60"]
        return []

    summary = run(tmp_path, factory=make_factory(args_for), probe=probe(mem_available=mem_available))
    merge = block(rows(summary), "ballista").filter(pl.col("op") == "merge").sort("rep")
    reason = "strażnik pamięci: MemAvailable 100 MiB < 300 MiB"
    assert merge["invalid_reason"].to_list() == [reason, f"pominięty: {reason}", f"pominięty: {reason}"]


def test_swap_during_run_invalidates_it(tmp_path):
    marker = tmp_path / "swap"

    def args_for(variant, n, scenario):
        return ["--touch", str(marker)] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(
        tmp_path, factory=make_factory(args_for),
        probe=probe(pswpout=lambda: 10 if marker.exists() else 0),
    )
    bad = rows(summary).filter(~pl.col("valid"))
    assert bad.height == 1
    assert bad["invalid_reason"][0] == "swap: pswpout +10" and bad["pswpout_delta"][0] == 10


def test_swap_in_is_recorded_without_invalidating(tmp_path):
    """Wczytanie stron ze swapu (pswpin) zapisuje się w wynikach, ale — inaczej niż pswpout
    (specyfikacja 6.5) — nie unieważnia przebiegu: przy niepustym swapie na starcie serii
    wczytują go także inne procesy. Regułę ustala plan 3b."""
    marker = tmp_path / "swapin"

    def args_for(variant, n, scenario):
        return ["--touch", str(marker)] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(
        tmp_path, factory=make_factory(args_for),
        probe=probe(pswpin=lambda: 7 if marker.exists() else 0),
    )
    df = rows(summary)
    hit = df.filter(pl.col("pswpin_delta") > 0)
    assert hit["pswpin_delta"].to_list() == [7] and hit["valid"].all()
    assert summary.ok


def _control_times(tmp_path, times: list[float]):
    path = tmp_path / "czasy_kontroli.txt"
    path.write_text("\n".join(map(str, times)))

    def args_for(variant, n, scenario):
        # Bez wariantu polars_bio_a w serii jedynymi przebiegami polars_bio_a są kontrolne.
        return ["--t-file", str(path)] if variant == "polars_bio_a" else []

    return make_factory(args_for)


def test_control_drift_repeats_block_once(tmp_path):
    factory = _control_times(tmp_path, [1.0, 2.0, 1.0, 1.0])
    summary = run(tmp_path, cfg=config(variants=["ballista"], control_tolerance=0.1), factory=factory)
    df = rows(summary).filter(~pl.col("is_reference"))
    first, second = df.filter(pl.col("attempt") == 1), df.filter(pl.col("attempt") == 2)
    assert first.height == second.height == 8
    assert set(first["invalid_reason"]) == {"dryf kontrolny 100%"} and not first["valid"].any()
    assert second["valid"].all()
    assert summary.invalid == 8 and not summary.ok


def test_drift_twice_leaves_block_invalid_without_third_attempt(tmp_path):
    factory = _control_times(tmp_path, [1.0, 2.0, 1.0, 2.0])
    summary = run(tmp_path, cfg=config(variants=["ballista"], control_tolerance=0.1), factory=factory)
    df = rows(summary).filter(~pl.col("is_reference"))
    assert set(df["attempt"]) == {1, 2} and not df["valid"].any()


def test_reference_failure_aborts_series_but_keeps_written_rows(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "3"] if (variant, scenario.op) == ("polars_bio_ref", "merge") else []

    with pytest.raises(orch.SeriesError, match="merge/1"):
        run(tmp_path, factory=make_factory(args_for))
    df = pl.read_parquet(next((tmp_path / "wyniki").rglob("runs.parquet")))
    assert df["scenario_id"].to_list() == ["overlap/1-2", "merge/1"] and df["is_reference"].all()


def test_engine_start_failure_is_reported_and_other_blocks_run(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            def start():
                raise EngineError("executor nie wstał")
            engine.start = start

    summary = run(tmp_path, factory=make_factory(customize=customize))
    assert summary.failures == ["ballista N=1: executor nie wstał"] and not summary.ok
    df = rows(summary)
    assert "ballista" not in set(df["variant"]) and block(df, "polars_bio_a").height == 6


def test_interrupt_keeps_finished_rows_and_stops_engine(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            def command(scenario):
                raise KeyboardInterrupt
            engine.command = command

    factory = make_factory(customize=customize)
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, factory=factory)
    df = pl.read_parquet(next((tmp_path / "wyniki").rglob("runs.parquet")))
    assert block(df, "polars_bio_a")["valid"].all()
    assert df["invalid_reason"][-1] == "seria przerwana"  # kontrola przerwanego bloku Ballisty
    assert factory.log[-1] == ("stop", "ballista", 1)


def test_runner_flooding_stderr_does_not_block(tmp_path):
    def args_for(variant, n, scenario):
        return ["--stderr-mb", "20"] if variant == "ballista" else []

    assert run(tmp_path, factory=make_factory(args_for)).ok


def test_dead_server_process_invalidates_run_and_restarts_engine(tmp_path):
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()

    def customize(engine):
        if engine.variant == "ballista":
            engine.pids = {"executor_1": dead.pid}

    factory = make_factory(customize=customize)
    summary = run(
        tmp_path, factory=factory,
        probe=probe(peak_rss=metrics.peak_rss, reset_peak_rss=metrics.reset_peak_rss),
    )
    reasons = block(rows(summary), "ballista")["invalid_reason"].to_list()
    died = "proces executor_1 zakończył się w trakcie przebiegu"
    assert reasons.count(died) == 2  # pierwszy przebieg każdego scenariusza
    assert all(r in (died, f"pominięty: {died}") for r in reasons)
    assert factory.log.count(("restart", "ballista", 1)) == 2


def _fixture_data(root: Path) -> Path:
    for idx in (1, 2):
        d = root / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "part-00000.parquet").write_bytes(b"")
    return root


def test_main_fails_fast_without_ballista_binaries(tmp_path, monkeypatch, capsys):
    root = _fixture_data(tmp_path / "dane")
    cfg = tmp_path / "seria.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "b", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["ballista"], "nodes": [1], "seed": 1,
    }))
    monkeypatch.setattr(engines, "BALLISTA_DIR", tmp_path / "brak")
    code = orch.main([str(cfg), "--data-root", str(root), "--results-dir", str(tmp_path / "wyniki")])
    assert code == 2
    assert "brak binarki" in capsys.readouterr().err
    assert not (tmp_path / "wyniki").exists()


def test_main_reports_config_error_with_code_2(tmp_path, capsys):
    cfg = tmp_path / "zla.yaml"
    cfg.write_text("series: x\n")
    assert orch.main([str(cfg), "--results-dir", str(tmp_path / "wyniki")]) == 2
    assert "brak wymaganych kluczy" in capsys.readouterr().err
    assert not (tmp_path / "wyniki").exists()

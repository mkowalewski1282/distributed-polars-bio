"""Orkiestrator serii (plan 3a, Zadanie 9) na fałszywych silnikach i runnerze: logika bloków,
ważności, limitu czasu, strażnika pamięci, swapu, dryfu i przerwań — bez uruchamiania silników."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import pytest
import yaml

from bench import engines, metrics, preflight
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
        self.logged: dict = {}
        self.explain_args: list[str] | None = None
        self.dead_once: Path | None = None
        self.graces: list = []

    def env_record(self):
        return {"ENGINE": f"{self.variant}-{self.n}"}

    def start(self):
        self.log.append(("start", self.variant, self.n))

    def stop(self):
        self.log.append(("stop", self.variant, self.n))

    def restart(self, grace_s=None):
        self.log.append(("restart", self.variant, self.n))
        self.graces.append(grace_s)

    def dead(self):
        """Martwy proces silnika — raz, gdy runner utworzy plik `dead_once` (sygnał z testu)."""
        if self.dead_once is not None and self.dead_once.exists():
            self.dead_once.unlink()
            return ["executor_1"]
        return []

    def command(self, scenario):
        args = self.args_for(self.variant, self.n, scenario)
        return Command([sys.executable, str(FAKE_RUNNER), *args], dict(os.environ), REPO)

    def explain_command(self, scenario, target):
        if self.variant != "ballista":
            return None
        args = self.explain_args or ["--write", str(target)]
        return Command([sys.executable, str(FAKE_RUNNER), *args], dict(os.environ), REPO)

    def log_mark(self):
        return {}

    def log_metrics(self, mark):
        return dict(self.logged)

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
        rss=lambda pid: 1000,
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
        "control": {"op": "overlap", "pair": "1-2"},
        "control_repeats": 1,
    }
    raw.update(overrides)
    return parse(raw)


def run(tmp_path, cfg=None, **kwargs):
    kwargs.setdefault("factory", make_factory())
    kwargs.setdefault("probe", probe())
    kwargs.setdefault("out", lambda line: None)
    return orch.run_series(cfg or config(), data_root=tmp_path, results_dir=tmp_path / "results", **kwargs)


def test_series_records_engine_environment(tmp_path):
    summary = run(tmp_path)
    assert json.loads((summary.directory / "env.json").read_text()) == {
        "control": {"ENGINE": "polars_bio_a-1"},
        "polars_bio_a N=1": {"ENGINE": "polars_bio_a-1"},
        "ballista N=1": {"ENGINE": "ballista-1"},
    }


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
    assert control["rep"].to_list() == [1, 1, 1, 1]
    assert control["control_position"].to_list() == ["start", "end", "start", "end"]
    assert df.filter(~pl.col("is_control"))["control_position"].is_null().all()
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
        "checksum 0x00000000000000bb ≠ reference 0x00000000000000aa"
    }
    assert not summary.ok and summary.invalid == 3


def test_engine_error_is_recorded_with_its_message(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "1"] if (variant, scenario.op) == ("ballista", "overlap") else []

    bad = rows(run(tmp_path, factory=make_factory(args_for))).filter(~pl.col("valid"))
    assert set(bad["scenario_id"]) == {"overlap/1-2"} and set(bad["variant"]) == {"ballista"}
    assert set(bad["invalid_reason"]) == {"exit code 1: engine crashed: test error"}


def test_garbage_on_stdout_invalidates_run(tmp_path):
    def args_for(variant, n, scenario):
        return ["--garbage"] if variant == "ballista" else []

    bad = rows(run(tmp_path, factory=make_factory(args_for))).filter(~pl.col("valid"))
    assert bad.height == 6
    assert all(r.startswith("runner stdout: invalid JSON") for r in bad["invalid_reason"])


def test_timeout_kills_runner_and_skips_scenario_for_rest_of_block(tmp_path):
    def args_for(variant, n, scenario):
        return ["--sleep", "60"] if (variant, scenario.op) == ("ballista", "merge") else []

    factory = make_factory(args_for)
    summary = run(tmp_path, cfg=config(timeout_s=1), factory=factory)
    merge = block(rows(summary), "ballista").filter(pl.col("op") == "merge").sort("rep")
    assert merge["invalid_reason"].to_list() == ["timeout", "skipped: timeout", "skipped: timeout"]
    assert merge["wall_s"][0] < 10 and merge["wall_s"][1:].is_null().all()
    assert ("restart", "ballista", 1) in factory.log


def test_memory_watchdog_kills_runner(tmp_path):
    marker = tmp_path / "memory_running_out"

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
    reason = "memory guard: MemAvailable 100 MiB < 300 MiB"
    assert merge["invalid_reason"].to_list() == [reason, f"skipped: {reason}", f"skipped: {reason}"]


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


def test_swap_in_invalidates_run(tmp_path):
    """Seria startuje od pustego swapu (sprawdzenie przed serią), więc każde wczytanie ze swapu
    w trakcie przebiegu pochodzi z pomiaru — przebieg jest nieważny (specyfikacja 3b-2, 4.3)."""
    marker = tmp_path / "swapin"

    def args_for(variant, n, scenario):
        return ["--touch", str(marker)] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(
        tmp_path, factory=make_factory(args_for),
        probe=probe(pswpin=lambda: 7 if marker.exists() else 0),
    )
    bad = rows(summary).filter(~pl.col("valid"))
    assert bad.height == 1
    assert bad["invalid_reason"][0] == "swap: pswpin +7" and bad["pswpin_delta"][0] == 7


def _control_times(tmp_path, times: list[float]):
    path = tmp_path / "control_times.txt"
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
    assert set(first["invalid_reason"]) == {"control drift 100%"} and not first["valid"].any()
    assert second["valid"].all()
    assert summary.invalid == 8 and not summary.ok


def test_drift_twice_leaves_block_invalid_without_third_attempt(tmp_path):
    factory = _control_times(tmp_path, [1.0, 2.0, 1.0, 2.0])
    summary = run(tmp_path, cfg=config(variants=["ballista"], control_tolerance=0.1), factory=factory)
    df = rows(summary).filter(~pl.col("is_reference"))
    assert set(df["attempt"]) == {1, 2} and not df["valid"].any()


def test_control_medians_ignore_single_outliers(tmp_path):
    factory = _control_times(tmp_path, [1.0, 5.0, 1.1, 1.05, 1.0, 9.0])
    cfg = config(variants=["ballista"], control_repeats=3, control_tolerance=0.1)
    summary = run(tmp_path, cfg=cfg, factory=factory)
    df = rows(summary).filter(~pl.col("is_reference"))
    assert set(df["attempt"]) == {1} and df["valid"].all()
    control = df.filter(pl.col("is_control"))
    assert control["rep"].to_list() == [1, 2, 3, 1, 2, 3]
    assert control["control_position"].to_list() == ["start"] * 3 + ["end"] * 3


def test_control_drift_of_medians_repeats_block(tmp_path):
    factory = _control_times(tmp_path, [1.0, 1.0, 1.0, 1.3, 1.3, 1.0] + [1.0] * 6)
    cfg = config(variants=["ballista"], control_repeats=3, control_tolerance=0.1)
    df = rows(run(tmp_path, cfg=cfg, factory=factory)).filter(~pl.col("is_reference"))
    first = df.filter(pl.col("attempt") == 1)
    assert set(first["invalid_reason"]) == {"control drift 30%"}
    assert df.filter(pl.col("attempt") == 2)["valid"].all()


def test_invalid_control_runs_invalidate_the_block(tmp_path):
    """Wszystkie przebiegi kontrolne nieważne (zła suma kontrolna polars-bio A) — blok dostaje
    przyczynę „control run invalid” zamiast wyjątku z mediany pustej listy, także po powtórzeniu."""
    def args_for(variant, n, scenario):
        return ["--checksum", "0x00000000000000bb"] if variant == "polars_bio_a" else []

    cfg = config(variants=["ballista"], control_repeats=2)
    df = rows(run(tmp_path, cfg=cfg, factory=make_factory(args_for))).filter(~pl.col("is_reference"))
    measured = df.filter(~pl.col("is_control"))
    assert set(measured["attempt"]) == {1, 2}
    assert set(measured["invalid_reason"]) == {"control run invalid"}


def test_reference_failure_aborts_series_but_keeps_written_rows(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "3"] if (variant, scenario.op) == ("polars_bio_ref", "merge") else []

    with pytest.raises(orch.SeriesError, match="merge/1"):
        run(tmp_path, factory=make_factory(args_for))
    df = pl.read_parquet(next((tmp_path / "results").rglob("runs.parquet")))
    assert df["scenario_id"].to_list() == ["overlap/1-2", "merge/1"] and df["is_reference"].all()


def test_engine_start_failure_is_reported_and_other_blocks_run(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            def start():
                raise EngineError("executor did not start")
            engine.start = start

    summary = run(tmp_path, factory=make_factory(customize=customize))
    assert summary.failures == ["ballista N=1: executor did not start"] and not summary.ok
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
    df = pl.read_parquet(next((tmp_path / "results").rglob("runs.parquet")))
    assert block(df, "polars_bio_a")["valid"].all()
    assert df["invalid_reason"][-1] == "series interrupted"  # kontrola przerwanego bloku Ballisty
    assert factory.log[-1] == ("stop", "ballista", 1)


#: Seria w osobnym procesie (jak `python -m bench.orchestrator`), której runner Ballisty „wisi”:
#: test wysyła sygnał w trakcie przebiegu.
SIGNAL_SERIES = """
import sys
from pathlib import Path
from bench import orchestrator as orch
from tests.bench.test_orchestrator import config, make_factory, probe

tmp = Path(sys.argv[1])

def args_for(variant, n, scenario):
    return ["--touch", str(tmp / "runner_started"), "--sleep", "60"] if variant == "ballista" else []

factory = make_factory(args_for)
orch.handle_termination_signals()
try:
    orch.run_series(config(), data_root=tmp, results_dir=tmp / "results", factory=factory,
                    probe=probe(), out=lambda line: None)
finally:
    (tmp / "journal.txt").write_text(repr(factory.log))
"""


def _cmdline_contains(proc_dir: Path, text: str) -> bool:
    try:
        return text in (proc_dir / "cmdline").read_bytes().decode(errors="replace")
    except OSError:
        return False


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGHUP])
def test_termination_signal_keeps_rows_and_stops_runner_and_engine(tmp_path, sig):
    """Zamknięcie terminala (SIGHUP) albo `kill` (SIGTERM) w trakcie przebiegu działa jak Ctrl-C:
    wiersze przerwanego bloku i runs.parquet zostają, runner (osobna sesja, więc sam sygnału
    nie dostaje) i silnik są zatrzymane. Domyślnie oba sygnały kończą proces bez sprzątania."""
    proc = subprocess.Popen(
        [sys.executable, "-c", SIGNAL_SERIES, str(tmp_path)], cwd=REPO,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    started = tmp_path / "runner_started"
    deadline = time.monotonic() + 60
    while not started.exists():
        assert proc.poll() is None, proc.communicate()[1][-2000:]
        assert time.monotonic() < deadline, "Ballista runner did not start"
        time.sleep(0.1)
    proc.send_signal(sig)
    proc.communicate(timeout=30)
    df = pl.read_parquet(next((tmp_path / "results").rglob("runs.parquet")))
    assert df["invalid_reason"][-1] == "series interrupted"  # kontrola przerwanego bloku Ballisty
    assert "('stop', 'ballista', 1)" in (tmp_path / "journal.txt").read_text()
    assert not [p for p in Path("/proc").glob("[0-9]*") if _cmdline_contains(p, str(started))]


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
    died = "process executor_1 exited during the run"
    assert reasons.count(died) == 2  # pierwszy przebieg każdego scenariusza
    assert all(r in (died, f"skipped: {died}") for r in reasons)
    assert factory.log.count(("restart", "ballista", 1)) == 2


def _fixture_data(root: Path) -> Path:
    for idx in (1, 2):
        d = root / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "part-00000.parquet").write_bytes(b"")
    return root


def test_main_fails_fast_without_ballista_binaries(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(preflight, "check_cpus", lambda *args, **kwargs: None)
    monkeypatch.setattr(metrics, "swap_used", lambda: 0)
    root = _fixture_data(tmp_path / "data")
    cfg = tmp_path / "series.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "b", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["ballista"], "nodes": [1], "seed": 1,
        "control": {"op": "overlap", "pair": "1-2"},
    }))
    monkeypatch.setattr(engines, "BALLISTA_DIR", tmp_path / "missing")
    code = orch.main([str(cfg), "--data-root", str(root), "--results-dir", str(tmp_path / "results")])
    assert code == 2
    assert "missing binary" in capsys.readouterr().err
    assert not (tmp_path / "results").exists()


def test_main_reports_config_error_with_code_2(tmp_path, capsys):
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("series: x\n")
    assert orch.main([str(cfg), "--results-dir", str(tmp_path / "results")]) == 2
    assert "missing required keys" in capsys.readouterr().err
    assert not (tmp_path / "results").exists()


def test_main_refuses_to_start_with_used_swap(tmp_path, monkeypatch, capsys):
    """Niepusty swap na starcie: każdy przebieg mógłby wczytywać cudze strony — seria nie startuje
    (kod 2), zanim powstanie katalog wyników."""
    monkeypatch.setattr(preflight, "check_cpus", lambda *args, **kwargs: None)
    root = _fixture_data(tmp_path / "data")
    cfg = tmp_path / "series.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "s", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "control": {"op": "overlap", "pair": "1-2"},
        "variants": ["polars_bio_a"], "nodes": [1], "seed": 1,
    }))
    monkeypatch.setattr(metrics, "swap_used", lambda: 300 * 2**20)
    code = orch.main([str(cfg), "--data-root", str(root), "--results-dir", str(tmp_path / "results")])
    assert code == 2
    assert "swap is not empty (300 MiB used)" in capsys.readouterr().err
    assert not (tmp_path / "results").exists()


def test_engine_log_metrics_reach_results(tmp_path):
    """Liczniki z logów silnika: broadcast_bytes → kolumna, pozostałe (np. sail_workers) → extra."""
    def customize(engine):
        if engine.variant == "ballista":
            engine.logged = {"broadcast_bytes": 10, "sail_workers": 2}

    df = rows(run(tmp_path, factory=make_factory(customize=customize)))
    ballista = block(df, "ballista")
    assert set(ballista["broadcast_bytes"]) == {10}
    assert all(json.loads(e)["sail_workers"] == 2 for e in ballista["extra"])
    assert block(df, "polars_bio_a")["broadcast_bytes"].is_null().all()


def test_explain_pass_writes_plans_of_ballista_blocks(tmp_path):
    summary = run(tmp_path, cfg=config(explain=True))
    explain = summary.directory / "explain"
    assert sorted(p.relative_to(explain).as_posix() for p in explain.rglob("*.txt")) == [
        "ballista-n1/merge_1.txt", "ballista-n1/overlap_1-2.txt",
    ]
    assert rows(summary).height == 2 + 2 * (2 + 2 + 4)  # przebiegi explain nie trafiają do wyników
    assert summary.ok


def test_failed_explain_is_reported_as_series_failure(tmp_path):
    def customize(engine):
        engine.explain_args = ["--exit", "1"]

    summary = run(tmp_path, cfg=config(explain=True), factory=make_factory(customize=customize))
    assert summary.failures == [
        "ballista N=1: EXPLAIN ANALYZE overlap/1-2: engine crashed: test error",
        "ballista N=1: EXPLAIN ANALYZE merge/1: engine crashed: test error",
    ]
    assert not summary.ok


def test_engine_death_during_explain_stops_runner_at_once(tmp_path):
    """Śmierć procesu silnika w przebiegu EXPLAIN ANALYZE przerywa runner od razu, a nie po
    timeoucie; każdy scenariusz EXPLAIN dostaje własną porażkę (runner za każdym razem tworzy
    znacznik od nowa, więc `dead_once` zgłasza śmierć przy każdym), wyniki pomiarów bez zmian."""
    marker = tmp_path / "executor_died"

    def customize(engine):
        if engine.variant == "ballista":
            engine.explain_args = ["--touch", str(marker), "--sleep", "60"]
            engine.dead_once = marker

    t0 = time.monotonic()
    summary = run(tmp_path, cfg=config(explain=True, timeout_s=120),
                  factory=make_factory(customize=customize))
    assert time.monotonic() - t0 < 60
    died = "process executor_1 exited during the run"
    assert summary.failures == [
        f"ballista N=1: EXPLAIN ANALYZE overlap/1-2: {died}",
        f"ballista N=1: EXPLAIN ANALYZE merge/1: {died}",
    ]
    assert rows(summary).height == 2 + 2 * (2 + 2 + 4)
    assert block(rows(summary), "ballista")["valid"].all()


def test_no_explain_pass_by_default(tmp_path):
    assert not (run(tmp_path).directory / "explain").exists()


def test_engine_versions_record_algorithm_crate_and_binary_hashes(tmp_path, monkeypatch):
    import hashlib

    root = tmp_path / "ballista_genomics"
    (root / "target" / "release").mkdir(parents=True)
    (root / "Cargo.lock").write_text(
        'name = "ballista"\nversion = "53.0.0"\n\nname = "datafusion"\nversion = "53.0.0"\n\n'
        'name = "datafusion-bio-function-ranges"\nversion = "0.22.2"\n'
    )
    (root / "target" / "release" / "ballista_node").write_bytes(b"node")
    monkeypatch.setattr(engines, "BALLISTA_DIR", root)
    monkeypatch.setattr(orch, "BALLISTA_DIR", root)
    versions = orch.engine_versions("release", ballista=True)
    assert versions["datafusion_bio_function_ranges"] == "0.22.2"
    assert versions["ballista_node_sha256"] == hashlib.sha256(b"node").hexdigest()
    assert versions["bench_client_sha256"] == "missing"
    assert "ballista_node_sha256" not in orch.engine_versions("release", ballista=False)


def test_engine_death_during_run_kills_runner_at_once(tmp_path):
    """Śmierć procesu silnika w trakcie przebiegu: runner przerwany od razu (nie po timeoucie),
    scenariusz pominięty do końca bloku, silnik zatrzymany twardo i uruchomiony od nowa."""
    marker = tmp_path / "executor_died"
    engines_by_variant = {}

    def args_for(variant, n, scenario):
        if (variant, scenario.op) == ("ballista", "overlap"):
            return ["--touch", str(marker), "--sleep", "60"]
        return []

    def customize(engine):
        engines_by_variant[engine.variant] = engine
        if engine.variant == "ballista":
            engine.dead_once = marker

    summary = run(tmp_path, cfg=config(timeout_s=120), factory=make_factory(args_for, customize))
    overlap = block(rows(summary), "ballista").filter(pl.col("op") == "overlap").sort("rep")
    died = "process executor_1 exited during the run"
    assert overlap["invalid_reason"].to_list() == [died, f"skipped: {died}", f"skipped: {died}"]
    assert overlap["wall_s"][0] < 30
    assert engines_by_variant["ballista"].graces == [orch.HARD_STOP_GRACE_S]
    assert block(rows(summary), "ballista").filter(pl.col("op") == "merge")["valid"].all()


def test_every_run_is_announced_before_it_starts(tmp_path):
    lines: list[str] = []
    summary = run(tmp_path, out=lines.append)
    starts = [line for line in lines if line.endswith(" start")]
    assert len(starts) == rows(summary).height
    assert lines[1].startswith("polars_bio_ref") and lines[1].endswith(" start")
    assert lines[2].startswith("polars_bio_ref") and lines[2].endswith("OK")


def test_stderr_of_failed_runs_is_kept(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "1"] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(tmp_path, factory=make_factory(args_for))
    kept = sorted((summary.directory / "logs" / "failed").iterdir())
    assert len(kept) == 3  # rozgrzewka i dwie rundy
    assert kept[0].name.endswith("-ballista-n1-overlap_1-2.stderr.txt")
    assert "engine crashed: test error" in kept[0].read_text()


def test_base_rss_of_long_lived_processes_is_recorded(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            engine.pids = {"executor_1": 4242}

    df = rows(run(tmp_path, factory=make_factory(customize=customize)))
    assert set(block(df, "ballista")["base_rss"]) == {json.dumps({"executor_1": 1000})}
    assert block(df, "polars_bio_a")["base_rss"].is_null().all()


def test_base_rss_of_zombie_process_does_not_abort_the_run(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            engine.pids = {"executor_1": 4242}

    def rss(pid):
        raise KeyError("VmRSS")

    summary = run(tmp_path, factory=make_factory(customize=customize), probe=probe(rss=rss))
    df = rows(summary)
    assert block(df, "ballista")["base_rss"].is_null().all()
    assert block(df, "ballista")["peak_rss"].is_not_null().all()


def _wrong_ballista_merge(variant, n, scenario):
    return ["--checksum", "0x00000000000000bb"] if (variant, scenario.op) == ("ballista", "merge") else []


def test_expected_invalid_runs_do_not_fail_the_series(tmp_path):
    cfg = config(expected_invalid=[{"scenario": "merge/1", "variant": "ballista", "reason": "checksum"}])
    summary = run(tmp_path, cfg=cfg, factory=make_factory(_wrong_ballista_merge))
    assert summary.ok and (summary.invalid, summary.expected, summary.unexpected) == (3, 3, [])
    assert orch.summary_lines("test", summary)[0].startswith("test OK: runs 18, invalid 3 (expected 3)")


def test_unexpected_invalid_runs_fail_the_series_and_are_listed(tmp_path):
    summary = run(tmp_path, factory=make_factory(_wrong_ballista_merge))
    assert not summary.ok and summary.expected == 0 and len(summary.unexpected) == 3
    lines = orch.summary_lines("test", summary)
    assert lines[0].startswith("test FAILED:")
    assert lines[1] == (
        "  unexpected: ballista N=1 merge/1: checksum 0x00000000000000bb ≠ reference 0x00000000000000aa"
    )

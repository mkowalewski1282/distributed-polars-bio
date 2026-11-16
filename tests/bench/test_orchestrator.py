"""Series orchestrator (P semantics) on fake engines and runner: block logic, validity,
timeout, memory guard, swap, drift, interruptions and skipping P larger than the CPU layout,
without starting engines."""

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
from bench.cpus import CpuLayout, control_cpus, cpus_for, read_layout
from bench.data.datasets import DATASETS
from bench.engines import CONTROL_VARIANT, REFERENCE_VARIANT, Command
from bench.procutil import EngineError
from tests.bench.fake_sysfs import make

REPO = Path(__file__).resolve().parents[2]
FAKE_RUNNER = Path(__file__).resolve().parent / "fake_runner.py"
#: Laptop layout (6 cores, core k = CPUs {2k, 2k+1}), the same one read_layout reads
#: from the fake sysfs (test_laptop_constant_matches_the_fake_sysfs).
LAPTOP = CpuLayout(
    system=(0, 1),
    order=(2, 4, 6, 8, 10, 3, 5, 7, 9, 11),
    siblings=tuple(frozenset({2 * k, 2 * k + 1}) for k in range(1, 6)),
)


def fake_cpus(layout: CpuLayout, variant: str, p: int) -> tuple[int, ...]:
    """Engine CPUs as in engines.make_engine."""
    if variant == CONTROL_VARIANT:
        return control_cpus(layout)
    return cpus_for(layout, 1 if variant == REFERENCE_VARIANT else p)


class FakeEngine:
    """Engine without processes: the runner is fake_runner.py with arguments depending on the
    variant and scenario; start, stop and restart go to a shared journal."""

    def __init__(self, variant, parallelism, args_for, log, cpus):
        self.variant, self.n, self.args_for, self.log = variant, parallelism, args_for, log
        self.cpus = cpus
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
        """Dead engine process, once, when the runner creates the `dead_once` file (signal from the test)."""
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


def make_factory(args_for=lambda variant, n, scenario: [], customize=None, layout=LAPTOP):
    log: list[tuple] = []

    def factory(variant, parallelism):
        engine = FakeEngine(variant, parallelism, args_for, log, fake_cpus(layout, variant, parallelism))
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
        "variants": ["polars_bio", "ballista"],
        "parallelism": [1],
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
    kwargs.setdefault("layout", LAPTOP)
    return orch.run_series(cfg or config(), data_root=tmp_path, results_dir=tmp_path / "results", **kwargs)


def test_series_records_engine_environment(tmp_path):
    summary = run(tmp_path)
    assert json.loads((summary.directory / "env.json").read_text()) == {
        "control": {"ENGINE": "polars_bio_control-2"},
        "polars_bio P=1": {"ENGINE": "polars_bio-1"},
        "ballista P=1": {"ENGINE": "ballista-1"},
    }


def rows(summary) -> pl.DataFrame:
    return pl.read_parquet(summary.parquet)


def block(df: pl.DataFrame, variant: str) -> pl.DataFrame:
    """Runs of a variant block, excluding the reference and control runs."""
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
    for variant in ("polars_bio", "ballista"):
        b = block(df, variant)
        assert b.filter(pl.col("is_warmup"))["rep"].to_list() == [0, 0]
        assert sorted(b.filter(~pl.col("is_warmup"))["rep"].to_list()) == [1, 1, 2, 2]
    control = df.filter(pl.col("is_control"))
    assert control["rep"].to_list() == [1, 1, 1, 1]
    assert control["control_position"].to_list() == ["start", "end", "start", "end"]
    assert df.filter(~pl.col("is_control"))["control_position"].is_null().all()
    assert set(control["scenario_id"]) == {"overlap/1-2"} and set(control["variant"]) == {"polars_bio_control"}
    assert df.height == 2 + 2 * (2 + 2 + 4)
    assert df["valid"].all() and summary.ok and (summary.rows, summary.invalid) == (df.height, 0)
    assert set(df["attempt"]) == {1}
    assert json.loads(df["peak_rss"][0]) == {"runner": 1234}
    assert factory.log == [
        ("start", "polars_bio_ref", 1), ("stop", "polars_bio_ref", 1),
        ("start", "polars_bio", 1), ("stop", "polars_bio", 1),
        ("start", "ballista", 1), ("stop", "ballista", 1),
    ]
    assert not (summary.directory / "tmp").exists()


def test_wrong_result_invalidates_only_that_scenario(tmp_path):
    def args_for(variant, n, scenario):
        return ["--checksum", "0x00000000000000bb"] if (variant, scenario.op) == ("ballista", "merge") else []

    summary = run(tmp_path, factory=make_factory(args_for))
    bad = rows(summary).filter(~pl.col("valid"))
    assert set(zip(bad["variant"], bad["op"])) == {("ballista", "merge")}
    assert bad.height == 3  # warm-up and two rounds
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
    """The series starts with empty swap (checked before the series), so every swap-in during a
    run comes from the measurement and the run is invalid."""
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
        return ["--t-file", str(path)] if variant == CONTROL_VARIANT else []

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
    """All control runs invalid (wrong checksum): the block gets the reason
    "control run invalid" instead of an exception from the median of an empty list, also after a retry."""
    def args_for(variant, n, scenario):
        return ["--checksum", "0x00000000000000bb"] if variant == CONTROL_VARIANT else []

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
    assert summary.failures == ["ballista P=1: executor did not start"] and not summary.ok
    df = rows(summary)
    assert "ballista" not in set(df["variant"]) and block(df, "polars_bio").height == 6


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
    assert block(df, "polars_bio")["valid"].all()
    assert df["invalid_reason"][-1] == "series interrupted"  # control of the interrupted Ballista block
    assert factory.log[-1] == ("stop", "ballista", 1)


#: A series in a separate process (like `python -m bench.orchestrator`) whose Ballista runner
#: "hangs": the test sends a signal mid-run.
SIGNAL_SERIES = """
import sys
from pathlib import Path
from bench import orchestrator as orch
from tests.bench.test_orchestrator import LAPTOP, config, make_factory, probe

tmp = Path(sys.argv[1])

def args_for(variant, n, scenario):
    return ["--touch", str(tmp / "runner_started"), "--sleep", "60"] if variant == "ballista" else []

factory = make_factory(args_for)
orch.handle_termination_signals()
try:
    orch.run_series(config(), data_root=tmp, results_dir=tmp / "results", factory=factory,
                    probe=probe(), out=lambda line: None, layout=LAPTOP)
finally:
    (tmp / "journal.txt").write_text(repr(factory.log))
"""


def _cmdline_contains(proc_dir: Path, text: str) -> bool:
    try:
        return text in (proc_dir / "cmdline").read_bytes().decode(errors="replace")
    except OSError:
        return False


def test_interrupt_during_a_run_kills_the_running_runner(tmp_path):
    """Ctrl-C (KeyboardInterrupt) during a run: the runner's process group is killed, so no
    runner is left on the block's CPUs."""
    started = tmp_path / "runner_started"

    def args_for(variant, n, scenario):
        return ["--touch", str(started), "--sleep", "60"] if variant == "ballista" else []

    def mem_available():
        if started.exists():
            raise KeyboardInterrupt
        return 4 << 30

    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, factory=make_factory(args_for), probe=probe(mem_available=mem_available))
    deadline = time.monotonic() + 10
    while [p for p in Path("/proc").glob("[0-9]*") if _cmdline_contains(p, str(started))]:
        assert time.monotonic() < deadline, "runner still alive after the interrupt"
        time.sleep(0.1)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGHUP])
def test_termination_signal_keeps_rows_and_stops_runner_and_engine(tmp_path, sig):
    """Closing the terminal (SIGHUP) or `kill` (SIGTERM) during a run acts like Ctrl-C: the rows
    of the interrupted block and runs.parquet are kept, and the runner (a separate session, so
    it gets no signal itself) and the engine are stopped. By default both signals end the
    process without cleanup."""
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
    assert df["invalid_reason"][-1] == "series interrupted"  # control of the interrupted Ballista block
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
    assert reasons.count(died) == 2  # first run of every scenario
    assert all(r in (died, f"skipped: {died}") for r in reasons)
    assert factory.log.count(("restart", "ballista", 1)) == 2


def _fixture_data(root: Path) -> Path:
    for idx in (1, 2):
        d = root / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "part-00000.parquet").write_bytes(b"")
    return root


def test_main_fails_fast_without_ballista_binaries(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(preflight, "check_cpus", lambda *args, **kwargs: [])
    monkeypatch.setattr(metrics, "swap_used", lambda: 0)
    root = _fixture_data(tmp_path / "data")
    cfg = tmp_path / "series.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "b", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["ballista"], "parallelism": [1], "seed": 1,
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
    """Non-empty swap at start: every run could be swapping in foreign pages, so the series does
    not start (exit code 2) before the results directory is created."""
    monkeypatch.setattr(preflight, "check_cpus", lambda *args, **kwargs: [])
    root = _fixture_data(tmp_path / "data")
    cfg = tmp_path / "series.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "s", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "control": {"op": "overlap", "pair": "1-2"},
        "variants": ["polars_bio"], "parallelism": [1], "seed": 1,
    }))
    monkeypatch.setattr(metrics, "swap_used", lambda: 300 * 2**20)
    code = orch.main([str(cfg), "--data-root", str(root), "--results-dir", str(tmp_path / "results")])
    assert code == 2
    assert "swap is not empty (300 MiB used)" in capsys.readouterr().err
    assert not (tmp_path / "results").exists()


def test_engine_log_metrics_reach_results(tmp_path):
    """Counters from the engine log: broadcast_bytes goes to a column, the rest (e.g. sail_workers) to extra."""
    def customize(engine):
        if engine.variant == "ballista":
            engine.logged = {"broadcast_bytes": 10, "sail_workers": 2}

    df = rows(run(tmp_path, factory=make_factory(customize=customize)))
    ballista = block(df, "ballista")
    assert set(ballista["broadcast_bytes"]) == {10}
    assert all(json.loads(e)["sail_workers"] == 2 for e in ballista["extra"])
    assert block(df, "polars_bio")["broadcast_bytes"].is_null().all()


def test_explain_pass_writes_plans_of_ballista_blocks(tmp_path):
    summary = run(tmp_path, cfg=config(explain=True))
    explain = summary.directory / "explain"
    assert sorted(p.relative_to(explain).as_posix() for p in explain.rglob("*.txt")) == [
        "ballista-p1/merge_1.txt", "ballista-p1/overlap_1-2.txt",
    ]
    assert rows(summary).height == 2 + 2 * (2 + 2 + 4)  # explain runs do not go into the results
    assert summary.ok


def test_failed_explain_is_reported_as_series_failure(tmp_path):
    def customize(engine):
        engine.explain_args = ["--exit", "1"]

    summary = run(tmp_path, cfg=config(explain=True), factory=make_factory(customize=customize))
    assert summary.failures == [
        "ballista P=1: EXPLAIN ANALYZE overlap/1-2: engine crashed: test error",
        "ballista P=1: EXPLAIN ANALYZE merge/1: engine crashed: test error",
    ]
    assert not summary.ok


def test_engine_death_during_explain_stops_runner_at_once(tmp_path):
    """An engine process dying during an EXPLAIN ANALYZE run aborts the runner at once rather
    than after the timeout; every EXPLAIN scenario gets its own failure (the runner recreates
    the marker each time, so `dead_once` reports a death for each), measurement results unchanged."""
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
        f"ballista P=1: EXPLAIN ANALYZE overlap/1-2: {died}",
        f"ballista P=1: EXPLAIN ANALYZE merge/1: {died}",
    ]
    assert rows(summary).height == 2 + 2 * (2 + 2 + 4)
    assert block(rows(summary), "ballista")["valid"].all()


def test_explain_pass_skips_scenarios_skipped_in_measured_runs(tmp_path):
    """A scenario skipped in the measurements (timeout) is not run under EXPLAIN ANALYZE and does
    not add a series failure; the second scenario gets its plan."""
    def args_for(variant, n, scenario):
        return ["--sleep", "60"] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(tmp_path, cfg=config(explain=True, timeout_s=1), factory=make_factory(args_for))
    explain = summary.directory / "explain"
    assert sorted(p.relative_to(explain).as_posix() for p in explain.rglob("*.txt")) == [
        "ballista-p1/merge_1.txt",
    ]
    assert summary.failures == []


def test_killed_explain_run_restarts_the_engine(tmp_path):
    """A killed EXPLAIN ANALYZE run (here: executor death) ends with an engine restart using a
    hard stop, so that the next scenario does not compete with an orphaned task."""
    marker = tmp_path / "executor_died"
    engines_by_variant = {}

    def customize(engine):
        engines_by_variant[engine.variant] = engine
        if engine.variant == "ballista":
            engine.explain_args = ["--touch", str(marker), "--sleep", "60"]
            engine.dead_once = marker

    run(tmp_path, cfg=config(explain=True, timeout_s=120), factory=make_factory(customize=customize))
    assert engines_by_variant["ballista"].graces == [orch.HARD_STOP_GRACE_S] * 2


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


def test_engine_versions_without_a_ballista_checkout(tmp_path, monkeypatch):
    """Uninitialized Ballista submodule (no Cargo.lock): a series without Ballista must start,
    and the Ballista versions are "unknown", not a FileNotFoundError."""
    monkeypatch.setattr(orch, "BALLISTA_DIR", tmp_path / "missing")
    monkeypatch.setattr(engines, "BALLISTA_DIR", tmp_path / "missing")
    versions = orch.engine_versions("release", ballista=False)
    assert versions["ballista"] == "unknown (release)" and versions["datafusion_ballista"] == "unknown"


def test_engine_death_during_run_kills_runner_at_once(tmp_path):
    """Engine process death during a run: the runner is aborted at once (not after the timeout),
    the scenario is skipped until the end of the block, and the engine is hard-stopped and restarted."""
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
    assert len(kept) == 3  # warm-up and two rounds
    assert kept[0].name.endswith("-ballista-p1-overlap_1-2.stderr.txt")
    assert "engine crashed: test error" in kept[0].read_text()


def test_base_rss_of_long_lived_processes_is_recorded(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            engine.pids = {"executor_1": 4242}

    df = rows(run(tmp_path, factory=make_factory(customize=customize)))
    assert set(block(df, "ballista")["base_rss"]) == {json.dumps({"executor_1": 1000})}
    assert block(df, "polars_bio")["base_rss"].is_null().all()


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
        "  unexpected: ballista P=1 merge/1: checksum 0x00000000000000bb ≠ reference 0x00000000000000aa"
    )


def _record_settle(monkeypatch, events: list) -> list:
    """Replaces the pause with an event record; returns the list of pause durations."""
    pauses: list[float] = []

    def settle(seconds):
        pauses.append(seconds)
        events.append("settle")

    monkeypatch.setattr(orch, "_settle", settle)
    return pauses


def test_settle_pause_precedes_every_measured_run(tmp_path, monkeypatch):
    events: list[str] = []
    pauses = _record_settle(monkeypatch, events)

    def customize(engine):
        original = engine.server_pids

        def server_pids():
            events.append("run")
            return original()

        engine.server_pids = server_pids

    summary = run(tmp_path, cfg=config(settle_s=3), factory=make_factory(customize=customize))
    # Reference, warm-up, measurement and control runs, each run once.
    assert pauses == [3.0] * rows(summary).height
    assert events == ["settle", "run"] * rows(summary).height


def test_no_settle_call_without_settle_s(tmp_path, monkeypatch):
    events: list[str] = []
    pauses = _record_settle(monkeypatch, events)
    run(tmp_path)
    assert pauses == []


def test_explain_pass_has_no_settle_pause(tmp_path, monkeypatch):
    pauses = _record_settle(monkeypatch, [])
    summary = run(tmp_path, cfg=config(explain=True, settle_s=1.5))
    assert (summary.directory / "explain").exists()
    assert len(pauses) == rows(summary).height  # explain (2 runs) adds no pauses


def test_laptop_constant_matches_the_fake_sysfs(tmp_path):
    cores = [[2 * k, 2 * k + 1] for k in range(6)]
    assert read_layout(make(tmp_path, cores), affinity=set(range(12))) == LAPTOP


SKIPPED_P = "skipped: P exceeds available CPUs"


def test_oversized_parallelism_is_skipped_not_failed(tmp_path):
    """Machine with 4 CPUs outside the system core (3 SMT cores), series with P = 4 and 8: the
    P = 8 block does not start the engine, gets one row per scenario with a skip reason, and
    the series counts as passed (exit code 0); the report will show the gap."""
    small = read_layout(make(tmp_path, [[0, 1], [2, 3], [4, 5]]), affinity=set(range(6)))
    factory = make_factory(layout=small)
    lines: list[str] = []
    cfg = config(variants=["ballista"], parallelism=[4, 8])
    summary = run(tmp_path, cfg=cfg, factory=factory, layout=small, out=lines.append)
    df = rows(summary)
    skipped = df.filter(pl.col("parallelism") == 8)
    assert skipped.height == 2 and sorted(skipped["scenario_id"]) == ["merge/1", "overlap/1-2"]
    assert set(skipped["variant"]) == {"ballista"} and not skipped["valid"].any()
    assert set(skipped["invalid_reason"]) == {SKIPPED_P} == {orch.SKIPPED_P_REASON}
    assert set(skipped["cpus"]) == {"[]"} and skipped["smt"].is_null().all()
    assert not skipped["is_control"].any() and not skipped["is_reference"].any()
    assert block(df, "ballista").filter(pl.col("parallelism") == 4)["valid"].all()
    assert ("start", "ballista", 8) not in factory.log
    assert summary.ok and (summary.invalid, summary.expected, summary.unexpected) == (2, 2, [])
    assert orch.summary_lines("test", summary)[0].startswith("test OK:")
    assert any("ballista P=8" in line and SKIPPED_P in line for line in lines)


def test_rows_record_cpus_and_smt(tmp_path):
    """P = 8 on the laptop reaches for the second threads of cores 1-3; rows carry the CPU list
    and the SMT flag; the reference (1 CPU) has no SMT, the control run (both threads of the
    first core) has SMT."""
    df = rows(run(tmp_path, cfg=config(variants=["ballista"], parallelism=[8])))
    measured = block(df, "ballista")
    assert set(measured["parallelism"]) == {8}
    assert [json.loads(c) for c in set(measured["cpus"])] == [[2, 4, 6, 8, 10, 3, 5, 7]]
    assert measured["smt"].all()
    reference = df.filter(pl.col("is_reference"))
    assert set(reference["cpus"]) == {"[2]"} and not reference["smt"].any()
    assert set(reference["parallelism"]) == {1}
    control = df.filter(pl.col("is_control"))
    assert set(control["cpus"]) == {"[2, 3]"} and control["smt"].all()
    assert set(control["parallelism"]) == {2}


def test_progress_lines_name_parallelism(tmp_path):
    lines: list[str] = []
    run(tmp_path, out=lines.append)
    assert any(line.startswith("ballista") and " P=1 " in line for line in lines)
    assert not any(" N=" in line for line in lines)

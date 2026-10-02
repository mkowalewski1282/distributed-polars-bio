"""Orkiestrator serii pomiarowej (specyfikacja 6 i 8): przebieg wzorcowy, potem bloki
(wariant × N) — przebieg kontrolny, rozgrzewka, rundy w losowej kolejności z ziarna,
przebieg kontrolny — z walidacją każdego przebiegu i zapisem wiersza na przebieg.

Uruchomienie (seria pomiarowa: z Windows Terminal, VS Code zamknięty — checklista RAM):
    python -m bench.orchestrator bench/conf/smoke.yaml [--ballista-profile debug]
Kody wyjścia: 0 — wszystkie przebiegi ważne; 1 — są przebiegi nieważne albo przerwane
bloki; 2 — błąd konfiguracji, brak danych albo binarek (przed pierwszym pomiarem)."""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from typing import Callable

from bench import metrics
from bench.config import (
    CONTROL, Block, ConfigError, Scenario, SeriesConfig, blocks, check_data, load, round_order,
)
from bench.data.datasets import data_dir
from bench.engines import (
    BALLISTA_DIR, CONTROL_VARIANT, PROFILES, REFERENCE_VARIANT, REPO, SYSTEM_CPUS, Command,
    cluster_cpus, make_engine, require_ballista_binaries,
)
from bench.procutil import EngineError
from bench.results import ResultsWriter, to_parquet
from bench.validity import Expected, block_drifted, drift, invalid_reasons, parse_report

#: Co ile sekund orkiestrator sprawdza runner (limit czasu, strażnik pamięci).
POLL_S = 0.2
#: Strażnik pamięci: przy MemAvailable poniżej tej wartości przebieg jest przerywany, zanim
#: system zacznie intensywnie swapować albo wywróci WSL.
MEM_FLOOR_BYTES = 300 * 2**20
#: Blok z dryfem przebiegu kontrolnego powtarzany jest raz (specyfikacja 6.3).
MAX_BLOCK_ATTEMPTS = 2

_METRIC_KEYS = (
    "valid", "invalid_reason", "rows", "checksum", "t_total_s", "wall_s", "phases", "extra",
    "peak_rss", "peak_rss_sum", "shuffle_bytes", "broadcast_bytes", "pswpout_delta", "pswpin_delta",
)


@dataclass
class Probe:
    """Źródła metryk systemowych (bench/metrics.py) — w testach podmieniane."""

    mem_available: Callable[[], int] = metrics.mem_available
    pswpout: Callable[[], int] = metrics.pswpout
    pswpin: Callable[[], int] = metrics.pswpin
    peak_rss: Callable[[int], int] = metrics.peak_rss
    reset_peak_rss: Callable[[int], None] = metrics.reset_peak_rss
    dir_size: Callable[[Path], int] = metrics.dir_size


@dataclass
class Outcome:
    """Wynik jednego wykonania runnera."""

    returncode: int
    stdout: str
    stderr: str
    killed: str | None
    wall_s: float


@dataclass
class SeriesSummary:
    directory: Path
    parquet: Path
    rows: int = 0
    invalid: int = 0
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.invalid == 0 and not self.failures


class SeriesError(RuntimeError):
    """Seria przerwana: scenariusz bez wyniku wzorcowego."""


def execute(command: Command, *, timeout_s: float, mem_floor: int, probe: Probe, scratch: Path) -> Outcome:
    """Uruchamia runner z wyjściem do plików — potoki mogłyby zakleszczyć runner zalewający
    stderr — pilnując limitu czasu i strażnika pamięci; zabija całą grupę procesów runnera."""
    out_path, err_path = scratch / "stdout.txt", scratch / "stderr.txt"
    killed: str | None = None
    t0 = time.monotonic()
    with out_path.open("w") as out, err_path.open("w") as err:
        proc = subprocess.Popen(
            command.argv, cwd=command.cwd, env=command.env, stdout=out, stderr=err,
            start_new_session=True,
        )
        try:
            while proc.poll() is None:
                if time.monotonic() - t0 > timeout_s:
                    killed = "timeout"
                else:
                    available = probe.mem_available()
                    if available < mem_floor:
                        killed = (
                            f"strażnik pamięci: MemAvailable {available // 2**20} MiB "
                            f"< {mem_floor // 2**20} MiB"
                        )
                if killed:
                    break
                time.sleep(POLL_S)
        finally:
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            proc.wait()
    return Outcome(
        proc.returncode,
        out_path.read_text(errors="replace"),
        err_path.read_text(errors="replace"),
        killed,
        time.monotonic() - t0,
    )


def git_commit() -> str:
    """HEAD repozytorium; sufiks -dirty, gdy śledzone pliki mają zmiany."""

    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True).stdout.strip()

    head = git("rev-parse", "HEAD") or "nieznany"
    return f"{head}-dirty" if git("status", "--porcelain", "--untracked-files=no") else head


def engine_versions(profile: str) -> dict[str, str]:
    lock = (BALLISTA_DIR / "Cargo.lock").read_text()

    def locked(name: str) -> str:
        m = re.search(rf'name = "{re.escape(name)}"\nversion = "([^"]+)"', lock)
        return m.group(1) if m else "nieznana"

    return {
        "polars_bio": version("polars-bio"),
        "pysail": version("pysail"),
        "pyspark": version("pyspark"),
        "ballista": f"{locked('ballista')} ({profile})",
        "datafusion_ballista": locked("datafusion"),
        "python": platform.python_version(),
    }


def _skipped(reason: str) -> dict:
    return {**dict.fromkeys(_METRIC_KEYS), "valid": False, "invalid_reason": reason}


def _invalidate(rows: list[dict], reason: str) -> list[dict]:
    for row in rows:
        row["valid"] = False
        row["invalid_reason"] = "; ".join(filter(None, [row["invalid_reason"], reason]))
    return rows


def _progress(row: dict) -> str:
    if row["is_reference"]:
        kind = "wzorzec"
    elif row["is_control"]:
        kind = "kontrola"
    elif row["is_warmup"]:
        kind = "rozgrzewka"
    else:
        kind = f"runda {row['rep']}"
    t = f"{row['t_total_s']:9.3f} s" if row["t_total_s"] is not None else "        – s"
    status = "OK" if row["valid"] else f"NIEWAŻNY: {row['invalid_reason']}"
    return f"{row['variant']:<14} N={row['n_nodes']} {kind:<10} {row['scenario_id']:<14} {t}  {status}"


class _Series:
    """Stan jednej serii: konfiguracja, silniki, źródła metryk, zapis i podsumowanie."""

    def __init__(self, cfg, factory, probe, mem_floor, scratch, writer, out, summary, profile):
        self.cfg, self.factory, self.probe, self.mem_floor = cfg, factory, probe, mem_floor
        self.scratch, self.writer, self.out, self.summary = scratch, writer, out, summary
        self.git_commit = git_commit()
        self.versions = json.dumps(engine_versions(profile))

    def row(self, scenario: Scenario, variant: str, n_nodes: int, rep: int, attempt: int,
            measured: dict, *, warmup=False, control=False, reference=False) -> dict:
        return {
            "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
            "git_commit": self.git_commit,
            "engine_versions": self.versions,
            "series": self.cfg.series,
            "seed": self.cfg.seed,
            "scenario_id": scenario.id,
            "op": scenario.op,
            "pair": scenario.data,
            "algorithm": None,
            "variant": variant,
            "n_nodes": n_nodes,
            "rep": rep,
            "attempt": attempt,
            "is_warmup": warmup,
            "is_control": control,
            "is_reference": reference,
            **measured,
        }

    def write(self, rows: list[dict]) -> None:
        for row in rows:
            self.writer.write(row)
            self.summary.rows += 1
            self.summary.invalid += not row["valid"]

    def measure(self, engine, scenario: Scenario, expected: Expected | None) -> tuple[dict, bool]:
        """Jeden przebieg z metrykami (specyfikacja 5). Drugi element: czy silnik trzeba
        uruchomić od nowa (runner zabity albo padł proces silnika)."""
        pids = engine.server_pids()
        for pid in pids.values():
            try:
                self.probe.reset_peak_rss(pid)
            except OSError:
                pass  # martwy proces wykryje odczyt szczytu po przebiegu
        dirs = engine.shuffle_dirs()
        shuffle_before = sum(self.probe.dir_size(d) for d in dirs)
        swap_before, swap_in_before = self.probe.pswpout(), self.probe.pswpin()
        outcome = execute(
            engine.command(scenario), timeout_s=self.cfg.timeout_s, mem_floor=self.mem_floor,
            probe=self.probe, scratch=self.scratch,
        )
        swap_delta = self.probe.pswpout() - swap_before
        # Wczytania ze swapu tylko zapisywane: przy niepustym swapie na starcie serii robią to
        # także inne procesy, więc reguła nieważności (specyfikacja 6.5) dotyczy pswpout.
        swap_in_delta = self.probe.pswpin() - swap_in_before
        report = report_error = None
        if outcome.killed is None and outcome.returncode == 0:
            try:
                report = parse_report(outcome.stdout)
            except ValueError as e:
                report_error = str(e)
        reasons = invalid_reasons(
            returncode=outcome.returncode, killed=outcome.killed, report=report,
            report_error=report_error, expected=expected, pswpout_delta=swap_delta,
            stderr=outcome.stderr,
        )
        peak = {"runner": report["peak_rss_bytes"]} if report else {}
        dead = []
        for name, pid in pids.items():
            try:
                peak[name] = self.probe.peak_rss(pid)
            except (OSError, KeyError):
                dead.append(name)
        if dead:
            reasons.append(f"proces {', '.join(dead)} zakończył się w trakcie przebiegu")
        measured = {
            "valid": not reasons,
            "invalid_reason": "; ".join(reasons) or None,
            "rows": report["rows"] if report else None,
            "checksum": report["checksum"] if report else None,
            "t_total_s": float(report["t_total_s"]) if report else None,
            "wall_s": outcome.wall_s,
            "phases": json.dumps(report["phases"]) if report else None,
            "extra": json.dumps(report["extra"]) if report else None,
            "peak_rss": json.dumps(peak) if peak else None,
            "peak_rss_sum": sum(peak.values()) if peak else None,
            "shuffle_bytes": sum(self.probe.dir_size(d) for d in dirs) - shuffle_before if dirs else None,
            "broadcast_bytes": None,
            "pswpout_delta": swap_delta,
            "pswpin_delta": swap_in_delta,
        }
        return measured, bool(outcome.killed or dead)

    def reference_pass(self) -> dict[str, Expected]:
        """Wynik wzorcowy każdego scenariusza i przebiegu kontrolnego: polars-bio na 1 partycji."""
        engine = self.factory(REFERENCE_VARIANT, 1)
        expected: dict[str, Expected] = {}
        engine.start()
        try:
            for scenario in dict.fromkeys([*self.cfg.scenarios, CONTROL]):
                measured, _ = self.measure(engine, scenario, None)
                row = self.row(scenario, REFERENCE_VARIANT, 1, 0, 1, measured, reference=True)
                self.write([row])
                self.out(_progress(row))
                if measured["rows"] is None:
                    raise SeriesError(
                        f"brak wyniku wzorcowego {scenario.id}: {measured['invalid_reason']}"
                    )
                expected[scenario.id] = Expected(measured["rows"], measured["checksum"])
        finally:
            engine.stop()
        return expected

    def run_block(self, block: Block, attempt: int, expected: dict[str, Expected], rows: list[dict]) -> bool:
        """Blok (specyfikacja 6.2): kontrola → rozgrzewka → rundy → kontrola. Wiersze trafiają
        do `rows` na bieżąco (wołający zapisuje je także po przerwaniu). True = dryf."""
        engine = self.factory(block.variant, block.n_nodes)
        control = self.factory(CONTROL_VARIANT, 1)
        skipped: dict[str, str] = {}

        def run(scenario: Scenario, rep: int, *, warmup: bool = False, is_control: bool = False) -> dict:
            if is_control:
                eng, variant, n = control, CONTROL_VARIANT, 1
            else:
                eng, variant, n = engine, block.variant, block.n_nodes
            if not is_control and scenario.id in skipped:
                measured = _skipped(f"pominięty: {skipped[scenario.id]}")
            else:
                measured, restart = self.measure(eng, scenario, expected[scenario.id])
                if restart:
                    if not is_control:
                        skipped[scenario.id] = measured["invalid_reason"]
                    eng.restart()
            row = self.row(scenario, variant, n, rep, attempt, measured, warmup=warmup, control=is_control)
            rows.append(row)
            self.out(_progress(row))
            return row

        engine.start()
        try:
            first = run(CONTROL, 0, is_control=True)
            for w in range(1, self.cfg.warmup + 1):
                for scenario in round_order(self.cfg, block, f"rozgrzewka-{w}"):
                    run(scenario, 0, warmup=True)
            for r in range(1, self.cfg.repeats + 1):
                for scenario in round_order(self.cfg, block, str(r)):
                    run(scenario, r)
            last = run(CONTROL, 1, is_control=True)
        finally:
            engine.stop()
        t_start = first["t_total_s"] if first["valid"] else None
        t_end = last["t_total_s"] if last["valid"] else None
        if not block_drifted(t_start, t_end, self.cfg.control_tolerance):
            return False
        if t_start is not None and t_end is not None:
            reason = f"dryf kontrolny {drift(t_start, t_end):.0%}"
        else:
            reason = "przebieg kontrolny nieważny"
        _invalidate(rows, reason)
        return True

    def run_block_with_retry(self, block: Block, expected: dict[str, Expected]) -> None:
        for attempt in range(1, MAX_BLOCK_ATTEMPTS + 1):
            rows: list[dict] = []
            try:
                drifted = self.run_block(block, attempt, expected, rows)
            except EngineError as e:
                self.write(_invalidate(rows, f"blok przerwany: {e}"))
                self.summary.failures.append(f"{block.label}: {e}")
                self.out(f"{block.label}: blok przerwany — {e}")
                return
            except BaseException:
                self.write(_invalidate(rows, "seria przerwana"))
                raise
            self.write(rows)
            if not drifted:
                return
            last = attempt == MAX_BLOCK_ATTEMPTS
            self.out(f"{block.label}: dryf przebiegu kontrolnego — {'blok nieważny' if last else 'powtarzam blok'}")


def _new_directory(results_dir: Path, series: str) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    for i in itertools.count():
        directory = results_dir / series / (stamp if i == 0 else f"{stamp}-{i}")
        try:
            directory.mkdir(parents=True)
            return directory
        except FileExistsError:
            continue


def run_series(
    cfg: SeriesConfig,
    *,
    data_root: Path,
    results_dir: Path,
    profile: str = "release",
    factory=None,
    probe: Probe | None = None,
    mem_floor: int = MEM_FLOOR_BYTES,
    out: Callable[[str], None] = print,
) -> SeriesSummary:
    """Seria: przebieg wzorcowy, potem bloki (blok z dryfem powtarzany raz). Wiersze trafiają
    do runs.jsonl po każdym bloku, a na końcu — także po przerwaniu — do runs.parquet."""
    directory = _new_directory(results_dir, cfg.series)
    scratch = directory / "tmp"
    scratch.mkdir()
    if factory is None:
        def factory(variant: str, n_nodes: int):
            return make_engine(
                variant, n_nodes, data_root=data_root,
                log_dir=directory / "logs" / f"{variant}-n{n_nodes}", profile=profile,
            )
    summary = SeriesSummary(directory=directory, parquet=directory / "runs.parquet")
    writer = ResultsWriter(directory / "runs.jsonl")
    series = _Series(cfg, factory, probe or Probe(), mem_floor, scratch, writer, out, summary, profile)
    out(f"seria {cfg.series}: wyniki w {directory}")
    try:
        expected = series.reference_pass()
        for block in blocks(cfg):
            series.run_block_with_retry(block, expected)
    finally:
        to_parquet(writer.path, summary.parquet)
        shutil.rmtree(scratch, ignore_errors=True)
    return summary


def preflight(cfg: SeriesConfig, profile: str) -> None:
    """Sprawdzenia przed pierwszym pomiarem: rdzenie emulowanych węzłów i binarki Ballisty."""
    needed = max(cluster_cpus(max(cfg.nodes))) + 1
    if (os.cpu_count() or 0) < needed:
        raise ConfigError(
            f"za mało rdzeni: N = {max(cfg.nodes)} wymaga {needed} wątków, jest {os.cpu_count()}"
        )
    if "ballista" in cfg.variants:
        require_ballista_binaries(profile)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bench.orchestrator",
        description="Seria pomiarowa według konfiguracji YAML (specyfikacja 8).",
    )
    parser.add_argument("config", type=Path, help="plik YAML serii, np. bench/conf/smoke.yaml")
    parser.add_argument(
        "--data-root", type=Path,
        help="katalog nadrzędny databio-8p (domyślnie $BENCH_DATA_ROOT albo ~/bench_data)",
    )
    parser.add_argument("--results-dir", type=Path, default=REPO / "bench" / "results")
    parser.add_argument("--ballista-profile", choices=PROFILES, default="release")
    args = parser.parse_args(argv)
    try:
        cfg = load(args.config)
        root = args.data_root if args.data_root is not None else data_dir().parent
        root = root.expanduser().resolve()
        check_data(cfg, root)
        preflight(cfg, args.ballista_profile)
    except (ConfigError, EngineError, OSError) as e:
        print(f"błąd: {e}", file=sys.stderr)
        return 2
    os.sched_setaffinity(0, SYSTEM_CPUS)
    try:
        summary = run_series(
            cfg, data_root=root, results_dir=args.results_dir.expanduser().resolve(),
            profile=args.ballista_profile,
        )
    except SeriesError as e:
        print(f"seria przerwana: {e}", file=sys.stderr)
        return 1
    print(
        f"przebiegi: {summary.rows}, nieważne: {summary.invalid}, "
        f"przerwane bloki: {len(summary.failures)}; wyniki: {summary.parquet}"
    )
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())

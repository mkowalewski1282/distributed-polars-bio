"""Orkiestrator serii pomiarowej (specyfikacja 6 i 8; etap 4: semantyka P): przebieg wzorcowy,
potem bloki (wariant × P) — przebiegi kontrolne, rozgrzewka, rundy w losowej kolejności z ziarna,
przebiegi kontrolne — z walidacją każdego przebiegu i zapisem wiersza na przebieg. CPU bloków
pochodzą z układu maszyny (bench/cpus.py); blok z P większym niż liczba CPU poza rdzeniem
systemowym jest pomijany z powodem (wiersz na scenariusz) i nie psuje kodu wyjścia.

Uruchomienie (seria pomiarowa: z Windows Terminal, VS Code zamknięty — checklista RAM):
    uv run python -m bench.orchestrator bench/conf/smoke.yaml [--ballista-profile debug]
Kody wyjścia: 0 — seria zaliczona (każdy nieważny przebieg jest na liście expected_invalid);
1 — nieoczekiwane nieważne przebiegi albo przerwane bloki; 2 — błąd konfiguracji, brak danych albo binarek (przed pierwszym pomiarem)."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
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

from bench import metrics, preflight as preflight_checks
from bench.config import (
    Block, ConfigError, Scenario, SeriesConfig, blocks, check_data, load, round_order,
)
from bench.cpus import CpuLayout, max_parallelism, read_layout, uses_smt
from bench.data.datasets import data_dir
from bench.engines import (
    BALLISTA_DIR, CONTROL_THREADS, CONTROL_VARIANT, PROFILES, REFERENCE_VARIANT, REPO, Command,
    ballista_binary, make_engine,
)
from bench.procutil import EngineError
from bench.results import ResultsWriter, to_parquet
from bench.validity import Expected, control_drift, invalid_reasons, parse_report, stderr_tail

#: Co ile sekund orkiestrator sprawdza runner (limit czasu, strażnik pamięci).
POLL_S = 0.2
#: Strażnik pamięci: przy MemAvailable poniżej tej wartości przebieg jest przerywany, zanim
#: system zacznie intensywnie swapować albo wywróci WSL.
MEM_FLOOR_BYTES = 300 * 2**20
#: Blok z dryfem przebiegu kontrolnego powtarzany jest raz (specyfikacja 6.3).
MAX_BLOCK_ATTEMPTS = 2
#: Po timeoucie, strażniku pamięci albo śmierci procesu silnik jest restartowany; proces, który
#: nie zakończy się w tym czasie po SIGTERM, dostaje SIGKILL (przegląd 3a).
HARD_STOP_GRACE_S = 3.0
#: Przyczyna w wierszach bloku z P większym niż liczba CPU poza rdzeniem systemowym (specyfikacja
#: etapu 4, 5.2) — wiersze oczekiwane: seria trwa dalej i jest zaliczona, raport pokazuje lukę.
SKIPPED_P_REASON = "skipped: P exceeds available CPUs"

_METRIC_KEYS = (
    "valid", "invalid_reason", "rows", "checksum", "t_total_s", "wall_s", "phases", "extra",
    "peak_rss", "peak_rss_sum", "base_rss", "shuffle_bytes", "broadcast_bytes", "pswpout_delta", "pswpin_delta",
)


@dataclass
class Probe:
    """Źródła metryk systemowych (bench/metrics.py) — w testach podmieniane."""

    mem_available: Callable[[], int] = metrics.mem_available
    pswpout: Callable[[], int] = metrics.pswpout
    pswpin: Callable[[], int] = metrics.pswpin
    peak_rss: Callable[[int], int] = metrics.peak_rss
    reset_peak_rss: Callable[[int], None] = metrics.reset_peak_rss
    rss: Callable[[int], int] = metrics.rss
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
    #: Nieważne przebiegi z listy expected_invalid konfiguracji.
    expected: int = 0
    #: Pozostałe nieważne przebiegi: „wariant P=p scenariusz: przyczyna”.
    unexpected: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.unexpected and not self.failures


class SeriesError(RuntimeError):
    """Seria przerwana: scenariusz bez wyniku wzorcowego."""


def _settle(seconds: float) -> None:
    """Pauza przed przebiegiem (osobna funkcja, żeby testy mogły ją podmienić)."""
    time.sleep(seconds)


def execute(command: Command, *, timeout_s: float, mem_floor: int, probe: Probe, scratch: Path,
            dead: Callable[[], list[str]] = lambda: []) -> Outcome:
    """Uruchamia runner z wyjściem do plików — potoki mogłyby zakleszczyć runner zalewający
    stderr — pilnując limitu czasu, strażnika pamięci i procesów silnika (`dead`: nazwy procesów,
    które padły); zabija całą grupę procesów runnera."""
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
                            f"memory guard: MemAvailable {available // 2**20} MiB "
                            f"< {mem_floor // 2**20} MiB"
                        )
                    else:
                        gone = dead()
                        if gone:
                            killed = f"process {', '.join(gone)} exited during the run"
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

    head = git("rev-parse", "HEAD") or "unknown"
    return f"{head}-dirty" if git("status", "--porcelain", "--untracked-files=no") else head


def engine_versions(profile: str, *, ballista: bool) -> dict[str, str]:
    """Wersje silników i bibliotek przebiegu; dla serii z Ballistą — sumy SHA-256 binarek
    (dokładnie wiadomo, czym mierzono; przegląd 3a). Bez Cargo.lock (niezainicjowany submoduł
    Ballisty) wersje Ballisty są „unknown” — seria bez Ballisty nie potrzebuje jej źródeł."""
    lock_file = BALLISTA_DIR / "Cargo.lock"
    lock = lock_file.read_text() if lock_file.is_file() else ""

    def locked(name: str) -> str:
        m = re.search(rf'name = "{re.escape(name)}"\nversion = "([^"]+)"', lock)
        return m.group(1) if m else "unknown"

    versions = {
        "polars_bio": version("polars-bio"),
        "pysail": version("pysail"),
        "pyspark": version("pyspark"),
        "ballista": f"{locked('ballista')} ({profile})",
        "datafusion_ballista": locked("datafusion"),
        "datafusion_bio_function_ranges": locked("datafusion-bio-function-ranges"),
        "python": platform.python_version(),
    }
    if ballista:
        for name in ("ballista_node", "bench_client"):
            path = ballista_binary(name, profile)
            versions[f"{name}_sha256"] = (
                hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "missing"
            )
    return versions


def _skipped(reason: str) -> dict:
    return {**dict.fromkeys(_METRIC_KEYS), "valid": False, "invalid_reason": reason}


def _invalidate(rows: list[dict], reason: str) -> list[dict]:
    for row in rows:
        row["valid"] = False
        row["invalid_reason"] = "; ".join(filter(None, [row["invalid_reason"], reason]))
    return rows


def _print(line: str) -> None:
    """Postęp serii na bieżąco — także przy przekierowaniu do pliku (przegląd 3a)."""
    print(line, flush=True)


def _kind(*, reference: bool = False, position: str | None = None, warmup: bool = False, rep: int = 0) -> str:
    if reference:
        return "reference"
    if position is not None:
        return f"control {position} {rep}"
    return "warmup" if warmup else f"round {rep}"


def _start_line(variant: str, parallelism: int, kind: str, scenario_id: str) -> str:
    return f"{variant:<18} P={parallelism:<2} {kind:<16} {scenario_id:<14}   start"


def _progress(row: dict) -> str:
    kind = _kind(reference=row["is_reference"], position=row["control_position"],
                 warmup=row["is_warmup"], rep=row["rep"])
    t = f"{row['t_total_s']:9.3f} s" if row["t_total_s"] is not None else "        – s"
    status = "OK" if row["valid"] else f"INVALID: {row['invalid_reason']}"
    return (f"{row['variant']:<18} P={row['parallelism']:<2} {kind:<16} {row['scenario_id']:<14} "
            f"{t}  {status}")


class _Series:
    """Stan jednej serii: konfiguracja, silniki, źródła metryk, zapis i podsumowanie."""

    def __init__(self, cfg, factory, probe, mem_floor, directory, scratch, writer, out, summary, profile,
                 layout: CpuLayout):
        self.cfg, self.factory, self.probe, self.mem_floor = cfg, factory, probe, mem_floor
        self.layout = layout
        self.directory, self.scratch, self.writer, self.out, self.summary = directory, scratch, writer, out, summary
        self.env_records: dict[str, dict[str, str]] = {}
        self.failed_logs = 0
        self.git_commit = git_commit()
        self.versions = json.dumps(engine_versions(profile, ballista="ballista" in cfg.variants))

    def record_env(self, label: str, engine) -> None:
        """Zmienne środowiska ustawione przez narzędzie dla bloku → env.json serii (przegląd 3a)."""
        self.env_records[label] = engine.env_record()
        (self.directory / "env.json").write_text(json.dumps(self.env_records, indent=2, sort_keys=True))

    def row(self, scenario: Scenario, variant: str, parallelism: int, cpus: tuple[int, ...] | None,
            rep: int, attempt: int, measured: dict, *, warmup=False,
            control_position: str | None = None, reference=False) -> dict:
        """Wiersz wyniku; `cpus` — CPU silnika, None — blok pominięty (cpus "[]", smt null)."""
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
            "parallelism": parallelism,
            "cpus": json.dumps(list(cpus or ())),
            "smt": None if cpus is None else uses_smt(self.layout, cpus),
            "rep": rep,
            "attempt": attempt,
            "is_warmup": warmup,
            "is_control": control_position is not None,
            "is_reference": reference,
            "control_position": control_position,
            **measured,
        }

    def write(self, rows: list[dict]) -> None:
        for row in rows:
            self.writer.write(row)
            self.summary.rows += 1
            if row["valid"]:
                continue
            self.summary.invalid += 1
            if row["invalid_reason"] == SKIPPED_P_REASON or any(
                e.matches(row) for e in self.cfg.expected_invalid
            ):
                self.summary.expected += 1
            else:
                self.summary.unexpected.append(
                    f"{row['variant']} P={row['parallelism']} {row['scenario_id']}: {row['invalid_reason']}"
                )

    def skip_block(self, block: Block) -> None:
        """Blok z P większym niż układ CPU: silnik nie startuje, każdy scenariusz dostaje jeden
        wiersz z przyczyną SKIPPED_P_REASON (raport pokaże lukę zamiast pustego miejsca)."""
        available = max_parallelism(self.layout)
        self.out(f"{block.label}: {SKIPPED_P_REASON} ({available} CPUs outside the system core)")
        self.write([
            self.row(scenario, block.variant, block.parallelism, None, 0, 1, _skipped(SKIPPED_P_REASON))
            for scenario in self.cfg.scenarios
        ])

    def explain_pass(self, engine, block: Block, skipped: dict[str, str]) -> None:
        """Dodatkowy, niemierzony przebieg EXPLAIN ANALYZE każdego scenariusza (specyfikacja 3b-2,
        6.4): plan z metrykami operatorów do pliku; nie trafia do wyników. Niepowodzenie →
        podsumowanie serii (seria kończy się kodem 1). Scenariusze pominięte w pomiarach bloku
        (`skipped`: id → przyczyna) nie są uruchamiane i nie dodają porażki — wiersze pomiarowe
        niosą już przyczynę. Po zabitym przebiegu silnik jest restartowany."""
        for scenario in self.cfg.scenarios:
            if scenario.id in skipped:
                self.out(f"{block.label}: EXPLAIN ANALYZE {scenario.id} skipped ({skipped[scenario.id]})")
                continue
            target = (self.directory / "explain" / f"{block.variant}-p{block.parallelism}"
                      / f"{scenario.id.replace('/', '_')}.txt")
            command = engine.explain_command(scenario, target)
            if command is None:
                return
            target.parent.mkdir(parents=True, exist_ok=True)
            self.out(f"{block.label}: EXPLAIN ANALYZE {scenario.id}")
            outcome = execute(
                command, timeout_s=self.cfg.timeout_s, mem_floor=self.mem_floor, probe=self.probe,
                scratch=self.scratch, dead=engine.dead,
            )
            if outcome.killed or outcome.returncode != 0 or not target.exists() or not target.stat().st_size:
                why = outcome.killed or stderr_tail(outcome.stderr)
                self.summary.failures.append(f"{block.label}: EXPLAIN ANALYZE {scenario.id}: {why}")
            if outcome.killed:
                engine.restart(grace_s=HARD_STOP_GRACE_S)

    def measure(self, engine, scenario: Scenario, expected: Expected | None,
                label: str) -> tuple[dict, bool]:
        """Jeden przebieg z metrykami (specyfikacja 5). Drugi element: czy silnik trzeba
        uruchomić od nowa (runner zabity albo padł proces silnika)."""
        # WSL2: pamięć zwolniona przez poprzedni przebieg wraca do Hyper-V po ok. 2 s, więc
        # przebiegi startowane w różnych odstępach trafiają na przemian na strony ciepłe i zimne.
        # Pauza daje każdemu przebiegowi ten sam stan startowy; poza WSL decyduje badanie szumu.
        if self.cfg.settle_s > 0:
            _settle(self.cfg.settle_s)
        pids = engine.server_pids()
        base = {}
        for name, pid in pids.items():
            try:
                self.probe.reset_peak_rss(pid)
                base[name] = self.probe.rss(pid)
            except (OSError, KeyError):
                pass  # martwy proces wykryje odczyt szczytu po przebiegu
        dirs = engine.shuffle_dirs()
        shuffle_before = sum(self.probe.dir_size(d) for d in dirs)
        swap_before, swap_in_before = self.probe.pswpout(), self.probe.pswpin()
        mark = engine.log_mark()
        outcome = execute(
            engine.command(scenario), timeout_s=self.cfg.timeout_s, mem_floor=self.mem_floor,
            probe=self.probe, scratch=self.scratch, dead=engine.dead,
        )
        swap_delta = self.probe.pswpout() - swap_before
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
            pswpin_delta=swap_in_delta, stderr=outcome.stderr,
        )
        if reasons and outcome.stderr.strip():
            self.failed_logs += 1
            path = (self.directory / "logs" / "failed"
                    / f"{self.failed_logs:04d}-{label}-{scenario.id.replace('/', '_')}.stderr.txt")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(outcome.stderr)
        peak = {"runner": report["peak_rss_bytes"]} if report else {}
        dead = []
        for name, pid in pids.items():
            try:
                peak[name] = self.probe.peak_rss(pid)
            except (OSError, KeyError):
                dead.append(name)
        if dead and outcome.killed is None:
            reasons.append(f"process {', '.join(dead)} exited during the run")
        logged = engine.log_metrics(mark)
        broadcast = logged.pop("broadcast_bytes", None)
        extra = {**report["extra"], **logged} if report else None
        measured = {
            "valid": not reasons,
            "invalid_reason": "; ".join(reasons) or None,
            "rows": report["rows"] if report else None,
            "checksum": report["checksum"] if report else None,
            "t_total_s": float(report["t_total_s"]) if report else None,
            "wall_s": outcome.wall_s,
            "phases": json.dumps(report["phases"]) if report else None,
            "extra": json.dumps(extra) if extra is not None else None,
            "peak_rss": json.dumps(peak) if peak else None,
            "peak_rss_sum": sum(peak.values()) if peak else None,
            "base_rss": json.dumps(base) if base else None,
            "shuffle_bytes": sum(self.probe.dir_size(d) for d in dirs) - shuffle_before if dirs else None,
            "broadcast_bytes": broadcast,
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
            for scenario in dict.fromkeys([*self.cfg.scenarios, self.cfg.control]):
                self.out(_start_line(REFERENCE_VARIANT, 1, "reference", scenario.id))
                measured, _ = self.measure(engine, scenario, None, f"{REFERENCE_VARIANT}-p1")
                row = self.row(scenario, REFERENCE_VARIANT, 1, engine.cpus, 0, 1, measured, reference=True)
                self.write([row])
                self.out(_progress(row))
                if measured["rows"] is None:
                    raise SeriesError(
                        f"no reference result for {scenario.id}: {measured['invalid_reason']}"
                    )
                expected[scenario.id] = Expected(measured["rows"], measured["checksum"])
        finally:
            engine.stop()
        return expected

    def run_block(self, block: Block, attempt: int, expected: dict[str, Expected], rows: list[dict]) -> bool:
        """Blok (specyfikacja 6.2; 3b-2, 4.2): przebiegi kontrolne → rozgrzewka → rundy →
        przebiegi kontrolne. Wiersze trafiają do `rows` na bieżąco (wołający zapisuje je także
        po przerwaniu). True = dryf albo nieważne przebiegi kontrolne."""
        engine = self.factory(block.variant, block.parallelism)
        control = self.factory(CONTROL_VARIANT, CONTROL_THREADS)
        self.record_env("control", control)
        self.record_env(block.label, engine)
        skipped: dict[str, str] = {}

        def run(scenario: Scenario, rep: int, *, warmup: bool = False,
                position: str | None = None) -> dict:
            is_control = position is not None
            if is_control:
                eng, variant, p = control, CONTROL_VARIANT, CONTROL_THREADS
            else:
                eng, variant, p = engine, block.variant, block.parallelism
            if not is_control and scenario.id in skipped:
                measured = _skipped(f"skipped: {skipped[scenario.id]}")
            else:
                self.out(_start_line(variant, p, _kind(position=position, warmup=warmup, rep=rep), scenario.id))
                measured, restart = self.measure(eng, scenario, expected[scenario.id], f"{variant}-p{p}")
                if restart:
                    if not is_control:
                        skipped[scenario.id] = measured["invalid_reason"]
                    eng.restart(grace_s=HARD_STOP_GRACE_S)
            row = self.row(scenario, variant, p, eng.cpus, rep, attempt, measured, warmup=warmup,
                           control_position=position)
            rows.append(row)
            self.out(_progress(row))
            return row

        def controls(position: str) -> list[float]:
            """Czasy ważnych przebiegów kontrolnych z jednego końca bloku."""
            times = []
            for rep in range(1, self.cfg.control_repeats + 1):
                row = run(self.cfg.control, rep, position=position)
                if row["valid"]:
                    times.append(row["t_total_s"])
            return times

        engine.start()
        try:
            start = controls("start")
            for w in range(1, self.cfg.warmup + 1):
                for scenario in round_order(self.cfg, block, f"warmup-{w}"):
                    run(scenario, 0, warmup=True)
            for r in range(1, self.cfg.repeats + 1):
                for scenario in round_order(self.cfg, block, str(r)):
                    run(scenario, r)
            end = controls("end")
            if self.cfg.explain:
                self.explain_pass(engine, block, skipped)
        finally:
            engine.stop()
        reason = control_drift(
            start, end, repeats=self.cfg.control_repeats, tolerance=self.cfg.control_tolerance
        )
        if reason is None:
            return False
        _invalidate(rows, reason)
        return True

    def run_block_with_retry(self, block: Block, expected: dict[str, Expected]) -> None:
        for attempt in range(1, MAX_BLOCK_ATTEMPTS + 1):
            rows: list[dict] = []
            try:
                drifted = self.run_block(block, attempt, expected, rows)
            except EngineError as e:
                self.write(_invalidate(rows, f"block interrupted: {e}"))
                self.summary.failures.append(f"{block.label}: {e}")
                self.out(f"{block.label}: block interrupted - {e}")
                return
            except BaseException:
                self.write(_invalidate(rows, "series interrupted"))
                raise
            self.write(rows)
            if not drifted:
                return
            last = attempt == MAX_BLOCK_ATTEMPTS
            self.out(f"{block.label}: control run drift - {'block invalid' if last else 'repeating the block'}")


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
    layout: CpuLayout | None = None,
    factory=None,
    probe: Probe | None = None,
    mem_floor: int = MEM_FLOOR_BYTES,
    out: Callable[[str], None] = _print,
) -> SeriesSummary:
    """Seria: przebieg wzorcowy, potem bloki (blok z dryfem powtarzany raz; blok z P większym niż
    układ CPU — pominięty z powodem). Wiersze trafiają do runs.jsonl po każdym bloku, a na końcu —
    także po przerwaniu — do runs.parquet. `layout`: układ CPU (domyślnie odczytany z tej maszyny)."""
    if layout is None:
        layout = read_layout()
    directory = _new_directory(results_dir, cfg.series)
    scratch = directory / "tmp"
    scratch.mkdir()
    if factory is None:
        def factory(variant: str, parallelism: int):
            return make_engine(
                variant, parallelism, layout=layout, data_root=data_root,
                log_dir=directory / "logs" / f"{variant}-p{parallelism}", profile=profile,
            )
    summary = SeriesSummary(directory=directory, parquet=directory / "runs.parquet")
    writer = ResultsWriter(directory / "runs.jsonl")
    series = _Series(cfg, factory, probe or Probe(), mem_floor, directory, scratch, writer, out, summary,
                     profile, layout)
    out(f"series {cfg.series}: results in {directory}")
    try:
        expected = series.reference_pass()
        for block in blocks(cfg):
            if block.parallelism > max_parallelism(layout):
                series.skip_block(block)
            else:
                series.run_block_with_retry(block, expected)
    finally:
        to_parquet(writer.path, summary.parquet)
        shutil.rmtree(scratch, ignore_errors=True)
    return summary


def _interrupt(signum, frame) -> None:
    raise KeyboardInterrupt(f"signal {signal.Signals(signum).name}")


def handle_termination_signals() -> dict:
    """SIGTERM (`kill`) i SIGHUP (zamknięcie terminala) działają jak Ctrl-C: przerywają serię tą
    samą ścieżką — zabicie grupy runnera, zatrzymanie silnika, zapis wierszy przerwanego bloku
    i runs.parquet. Domyślnie oba sygnały kończą proces natychmiast, bez sprzątania, a runner
    (osobna sesja) i procesy silnika zostają na CPU bloku. Zwraca poprzednie obsługi."""
    return {sig: signal.signal(sig, _interrupt) for sig in (signal.SIGTERM, signal.SIGHUP)}


def preflight(cfg: SeriesConfig, profile: str, layout: CpuLayout) -> list[int]:
    """Sprawdzenia przed pierwszym pomiarem (bench/preflight.py); zwraca P, które seria pominie."""
    return preflight_checks.run(cfg, profile, layout)


#: Ile nieoczekiwanych nieważnych przebiegów wypisać w podsumowaniu.
SHOWN_UNEXPECTED = 20


def summary_lines(series: str, summary: SeriesSummary) -> list[str]:
    """Podsumowanie serii: zaliczona (OK) albo nie (FAILED) i lista przyczyn (specyfikacja 3b-2,
    pozycja 17 — automatyczne zaliczenie smoke)."""
    status = "OK" if summary.ok else "FAILED"
    lines = [
        f"{series} {status}: runs {summary.rows}, invalid {summary.invalid} (expected "
        f"{summary.expected}), failures {len(summary.failures)}; results: {summary.parquet}"
    ]
    lines += [f"  unexpected: {u}" for u in summary.unexpected[:SHOWN_UNEXPECTED]]
    if len(summary.unexpected) > SHOWN_UNEXPECTED:
        lines.append(f"  ... and {len(summary.unexpected) - SHOWN_UNEXPECTED} more unexpected invalid runs")
    lines += [f"  failure: {f}" for f in summary.failures]
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bench.orchestrator",
        description="Measurement series from a YAML configuration (methodology spec, section 8).",
    )
    parser.add_argument("config", type=Path, help="series YAML file, e.g. bench/conf/smoke.yaml")
    parser.add_argument(
        "--data-root", type=Path,
        help="parent directory of databio-8p (default: $BENCH_DATA_ROOT or ~/bench_data)",
    )
    parser.add_argument("--results-dir", type=Path, default=REPO / "bench" / "results")
    parser.add_argument("--ballista-profile", choices=PROFILES, default="release")
    args = parser.parse_args(argv)
    try:
        cfg = load(args.config)
        root = args.data_root if args.data_root is not None else data_dir().parent
        root = root.expanduser().resolve()
        check_data(cfg, root)
        # Układ przed zawężeniem affinity orkiestratora do rdzenia systemowego (niżej).
        layout = preflight_checks.cpu_layout()
        preflight(cfg, args.ballista_profile, layout)
    except (ConfigError, EngineError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    os.sched_setaffinity(0, layout.system)
    previous = handle_termination_signals()
    try:
        summary = run_series(
            cfg, data_root=root, results_dir=args.results_dir.expanduser().resolve(),
            profile=args.ballista_profile, layout=layout,
        )
    except SeriesError as e:
        print(f"series interrupted: {e}", file=sys.stderr)
        return 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    for line in summary_lines(cfg.series, summary):
        print(line)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())

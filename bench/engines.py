"""Silniki bloku pomiarowego (specyfikacja, sekcje 3 i 6): przypięcie do rdzeni (emulacja
węzłów), klaster Ballisty, serwer Saila i polecenia runnerów.

Silnik ma interfejs: start(), stop(), restart(), command(scenariusz) → Command,
server_pids() → {nazwa: PID} (procesy długożyjące, których szczyt pamięci mierzy
orkiestrator) i shuffle_dirs() → katalogi robocze executorów (wolumen shuffle).
log_mark() i log_metrics(mark) → liczniki z logów procesów silnika z czasu przebiegu
(specyfikacja 3b-2: workery Saila, broadcast Ballisty).
explain_command(scenariusz, plik) → Command albo None (EXPLAIN ANALYZE; tylko Ballista).
dead() → nazwy procesów silnika, które padły; stop(grace_s) i restart(grace_s) — SIGKILL po
grace_s sekundach od SIGTERM."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from bench import logscan
from bench.config import Scenario
from bench.data.datasets import resolve
from bench.procutil import EngineError, check_alive, free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent
BALLISTA_DIR = REPO / "ballista_genomics"
PROFILES = ("release", "debug")
#: Rdzeń 0 (wątki 0–1): system, scheduler Ballisty, klienci, orkiestrator (specyfikacja 3).
SYSTEM_CPUS = (0, 1)
#: Sloty zadań i wątki na węzeł: 1 rdzeń fizyczny = 2 wątki SMT.
SLOTS_PER_NODE = 2
#: Limit wiadomości gRPC Ballisty (specyfikacja 3b-2, 6.2): w nearest i coverage każde zadanie
#: niesie całą tabelę rozsyłaną (~29 MiB dla 2-7, ~56 MiB dla 7-0), a wiadomość do executora —
#: kilka zadań; domyślne 16 MiB Ballisty nie wystarcza.
GRPC_MAX_MESSAGE_BYTES = 512 * 2**20
#: Wynik wzorcowy: polars-bio na 1 partycji — przy większej liczbie partycji polars-bio 0.28
#: psuje merge i subtract (bench/runners/polars_bio_runner.py).
REFERENCE_VARIANT = "polars_bio_ref"
#: Przebieg kontrolny (specyfikacja 6.3): polars-bio A.
CONTROL_VARIANT = "polars_bio_a"
#: Próby startu klastra Ballisty. Ballista 53 rejestruje executor w schedulerze, zanim jego
#: serwer gRPC przyjmuje połączenia (ballista_executor::executor_server: „TODO the executor
#: registration should happen only after the executor grpc server started”), a scheduler
#: w trybie push od razu łączy się z executorem. Przy obciążonym CPU rejestracja bywa
#: odrzucona („Connection refused”) i executor kończy się kodem 1 (smoke 02.10.2026).
BALLISTA_START_ATTEMPTS = 3
#: Czas na zakończenie po SIGTERM przy zwykłym zatrzymaniu silnika (potem SIGKILL).
BALLISTA_STOP_GRACE_S = 10.0
SAIL_STOP_GRACE_S = 30.0
#: Próby startu serwera Saila (jak BALLISTA_START_ATTEMPTS; przegląd 3a: Sail nie ponawiał startu).
SAIL_START_ATTEMPTS = 3
#: Wariant Saila → ścieżka runnera (bench/runners/sail_runner.py, specyfikacja 3b-2, 5.3).
SAIL_PATHS = {"sail": "arrow", "sail_lock": "arrow-lock", "sail_udtf": "udtf"}


def node_cpus(k: int) -> tuple[int, int]:
    """Węzeł k (1..3) = rdzeń fizyczny k = wątki {2k, 2k+1} (lscpu -e)."""
    if k not in (1, 2, 3):
        raise ValueError(f"node {k!r}: allowed 1–3")
    return (2 * k, 2 * k + 1)


def cluster_cpus(n_nodes: int) -> tuple[int, ...]:
    return tuple(cpu for k in range(1, n_nodes + 1) for cpu in node_cpus(k))


def taskset(cpus) -> list[str]:
    return ["taskset", "-c", ",".join(str(c) for c in cpus)]


@dataclass(frozen=True)
class Command:
    argv: list[str]
    env: dict[str, str]
    cwd: Path


def scenario_args(scenario: Scenario, data_root: Path) -> list[str]:
    left, right = resolve(scenario.data, data_root)
    args = ["--op", scenario.op, "--left", str(left)]
    return args if right is None else [*args, "--right", str(right)]


def _python(module: str, cpus, args: list[str], env: dict[str, str]) -> Command:
    return Command(
        [*taskset(cpus), sys.executable, "-m", module, *args],
        {**os.environ, "MPLBACKEND": "Agg", **env},
        REPO,
    )


class PolarsBioEngine:
    """polars-bio: świeży proces na przebieg, bez procesów długożyjących."""

    def __init__(self, threads: int, cpus, data_root: Path):
        self.threads, self.cpus, self.data_root = threads, tuple(cpus), data_root

    def start(self) -> None:
        pass

    def stop(self, grace_s: float = 0.0) -> None:
        pass

    def restart(self, grace_s: float = 0.0) -> None:
        pass

    def dead(self) -> list[str]:
        return []

    def command(self, scenario: Scenario) -> Command:
        return _python(
            "bench.runners.polars_bio_runner",
            self.cpus,
            [*scenario_args(scenario, self.data_root), "--threads", str(self.threads)],
            {"POLARS_MAX_THREADS": str(self.threads)},
        )

    def explain_command(self, scenario: Scenario, target: Path) -> Command | None:
        return None

    def log_mark(self) -> dict[Path, int]:
        return {}

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        return {}

    def server_pids(self) -> dict[str, int]:
        return {}

    def shuffle_dirs(self) -> list[Path]:
        return []


def ballista_binary(name: str, profile: str) -> Path:
    return BALLISTA_DIR / "target" / profile / name


def ballista_sources(binary: str | None = None) -> list[Path]:
    """Źródła wspólne binarek Ballisty: Cargo.toml, Cargo.lock, kod Rusta w src/ (poza src/bin/)
    i w vendor/ (też manifesty) — bez dokumentacji, której zmiana nie wymaga przebudowy.
    Z nazwą binarki dochodzi jej własny punkt wejścia src/bin/<nazwa>.rs: cargo przelinkowuje
    tylko binarki, których wejścia się zmieniły, więc cudze punkty wejścia nie liczą się."""
    files = [BALLISTA_DIR / "Cargo.toml", BALLISTA_DIR / "Cargo.lock"]
    src, bin_dir = BALLISTA_DIR / "src", BALLISTA_DIR / "src" / "bin"
    files.extend(p for p in src.rglob("*.rs") if p.is_file() and bin_dir not in p.parents)
    vendor = BALLISTA_DIR / "vendor"
    files.extend(p for p in vendor.rglob("*.rs") if p.is_file())
    files.extend(p for p in vendor.rglob("Cargo.toml") if p.is_file())
    if binary is not None:
        files.append(bin_dir / f"{binary}.rs")
    return [p for p in files if p.exists()]


def require_ballista_binaries(profile: str) -> None:
    """Binarki istnieją i każda jest nowsza od własnych źródeł (wspólnych i swojego punktu
    wejścia) — inaczej pomiar objąłby kod sprzed ostatniej zmiany (przegląd 3a)."""
    names = ("ballista_node", "bench_client")
    binaries = {name: ballista_binary(name, profile) for name in names}
    flag = "--release " if profile == "release" else ""
    build = (
        f"cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build {flag}--bin ballista_node --bin bench_client"
        " (if cargo has nothing to rebuild, touch ballista_genomics/src/lib.rs first)"
    )
    missing = [str(b) for b in binaries.values() if not b.exists()]
    if missing:
        raise EngineError(f"missing binary {', '.join(missing)} - build it: {build}")
    for name, binary in binaries.items():
        newest = max(ballista_sources(name), key=lambda p: p.stat().st_mtime, default=None)
        if newest is not None and binary.stat().st_mtime < newest.stat().st_mtime:
            raise EngineError(f"binary {binary} is older than {newest} - rebuild it: {build}")


def _registered_executors(port: int) -> int:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/executors", timeout=2) as response:
        return len(json.loads(response.read()))


class BallistaEngine:
    """Scheduler na rdzeniu systemowym + N executorów (executor k na rdzeniu węzła k,
    po 2 sloty); BIO_TARGET_PARTITIONS = 2N i BALLISTA_GRPC_MAX_MESSAGE_BYTES we wszystkich
    procesach i w kliencie."""

    def __init__(self, n_nodes: int, *, profile: str, data_root: Path, log_dir: Path,
                 grpc_max_message_bytes: int = GRPC_MAX_MESSAGE_BYTES):
        self.n, self.profile, self.data_root, self.log_dir = n_nodes, profile, data_root, log_dir
        self.env = {
            **os.environ,
            "BIO_TARGET_PARTITIONS": str(SLOTS_PER_NODE * n_nodes),
            "BALLISTA_GRPC_MAX_MESSAGE_BYTES": str(grpc_max_message_bytes),
        }
        self.procs: dict[str, subprocess.Popen] = {}
        self.work_dirs: dict[str, Path] = {}
        self.port: int | None = None
        self._tmp: Path | None = None

    def start(self) -> None:
        """Start klastra; po porażce (np. executor odrzucony przy rejestracji) — od nowa, na
        nowych portach, do BALLISTA_START_ATTEMPTS razy. Logi kolejnych prób są dopisywane."""
        require_ballista_binaries(self.profile)
        for attempt in range(1, BALLISTA_START_ATTEMPTS + 1):
            try:
                self._start_once()
                return
            except EngineError:
                if attempt == BALLISTA_START_ATTEMPTS:
                    raise

    def _start_once(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._tmp = Path(tempfile.mkdtemp(prefix="bench_ballista_"))
        self.port = free_port()
        try:
            self._spawn("scheduler", SYSTEM_CPUS, ["scheduler", "--port", str(self.port)])
            # Executor łączy się ze schedulerem tylko raz, przy starcie (P0) — najpierw scheduler.
            self._wait(lambda: _registered_executors(self.port) >= 0, "scheduler does not respond")
            for k in range(1, self.n + 1):
                name = f"executor_{k}"
                work = self._tmp / name
                work.mkdir()
                self.work_dirs[name] = work
                self._spawn(name, node_cpus(k), [
                    "executor", "--scheduler-port", str(self.port),
                    "--port", str(free_port()), "--grpc-port", str(free_port()),
                    "--work-dir", str(work), "--concurrent-tasks", str(SLOTS_PER_NODE),
                ])
            self._wait(
                lambda: _registered_executors(self.port) == self.n,
                f"scheduler does not see {self.n} executors",
            )
        except BaseException:
            self.stop()
            raise

    def _spawn(self, name: str, cpus, args: list[str]) -> None:
        with (self.log_dir / f"{name}.log").open("a") as log:
            self.procs[name] = subprocess.Popen(
                [*taskset(cpus), str(ballista_binary("ballista_node", self.profile)), *args],
                cwd=BALLISTA_DIR, env=self.env, stdout=log, stderr=subprocess.STDOUT,
            )

    def _wait(self, ready, message: str, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for name, proc in self.procs.items():
                check_alive(name, proc, self.log_dir / f"{name}.log")
            try:
                if ready():
                    return
            except (OSError, ValueError):
                pass  # scheduler jeszcze nie odpowiada albo odpowiada niepełnym JSON-em
            time.sleep(0.3)
        raise EngineError(f"{message} after {timeout:.0f} s; logs: {self.log_dir}")

    def stop(self, grace_s: float = BALLISTA_STOP_GRACE_S) -> None:
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + grace_s
        for proc in self.procs.values():
            try:
                proc.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        self.procs.clear()
        self.work_dirs.clear()
        if self._tmp is not None:
            shutil.rmtree(self._tmp, ignore_errors=True)
            self._tmp = None

    def restart(self, grace_s: float = BALLISTA_STOP_GRACE_S) -> None:
        self.stop(grace_s)
        self.start()

    def dead(self) -> list[str]:
        return [name for name, proc in self.procs.items() if proc.poll() is not None]

    def command(self, scenario: Scenario) -> Command:
        return Command(
            [*taskset(SYSTEM_CPUS), str(ballista_binary("bench_client", self.profile)),
             *scenario_args(scenario, self.data_root)],
            {**self.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{self.port}"},
            BALLISTA_DIR,
        )

    def explain_command(self, scenario: Scenario, target: Path) -> Command:
        """Dodatkowy, niemierzony przebieg EXPLAIN ANALYZE (specyfikacja 3b-2, 6.4)."""
        command = self.command(scenario)
        return Command([*command.argv, "--explain-analyze", str(target)], command.env, command.cwd)

    def log_mark(self) -> dict[Path, int]:
        return logscan.mark(self.log_dir / f"executor_{k}.log" for k in range(1, self.n + 1))

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        """Bajty ładunków broadcastu odebranych przez executory w czasie przebiegu
        (bio_phys_codec.rs loguje każde dekodowanie węzła z broadcastem)."""
        return {"broadcast_bytes": logscan.broadcast_bytes(logscan.text_since(mark))}

    def server_pids(self) -> dict[str, int]:
        return {name: proc.pid for name, proc in self.procs.items()}

    def shuffle_dirs(self) -> list[Path]:
        return list(self.work_dirs.values())


class SailEngine:
    """Jeden proces serwera Saila (local-cluster, N workerów) na rdzeniach węzłów 1..N;
    runner-klient na rdzeniu systemowym."""

    def __init__(self, n_nodes: int, *, data_root: Path, log_dir: Path, path: str = "arrow"):
        self.n, self.data_root, self.log_dir, self.path = n_nodes, data_root, log_dir, path
        self.proc: subprocess.Popen | None = None
        self.port: int | None = None

    def start(self) -> None:
        """Start serwera; po porażce (np. zajęty port) — od nowa, na nowym porcie, do
        SAIL_START_ATTEMPTS razy. Log kolejnych prób jest dopisywany."""
        for attempt in range(1, SAIL_START_ATTEMPTS + 1):
            try:
                self._start_once()
                return
            except EngineError:
                if attempt == SAIL_START_ATTEMPTS:
                    raise

    def _start_once(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.port = free_port()
        log = self.log_dir / "sail_server.log"
        with log.open("a") as f:
            self.proc = subprocess.Popen(
                [*taskset(cluster_cpus(self.n)), sys.executable, "-m", "bench.runners.sail_server",
                 "--port", str(self.port)],
                cwd=REPO, env={**os.environ, "MPLBACKEND": "Agg", **sail_env(self.n)},
                stdout=f, stderr=subprocess.STDOUT,
            )
        try:
            wait_for_port(self.port, self.proc, name="sail_server", log=log)
        except BaseException:
            self.stop()
            raise

    def stop(self, grace_s: float = SAIL_STOP_GRACE_S) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=grace_s)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None

    def restart(self, grace_s: float = SAIL_STOP_GRACE_S) -> None:
        self.stop(grace_s)
        self.start()

    def dead(self) -> list[str]:
        return ["sail_server"] if self.proc is not None and self.proc.poll() is not None else []

    def command(self, scenario: Scenario) -> Command:
        return _python(
            "bench.runners.sail_runner",
            SYSTEM_CPUS,
            [*scenario_args(scenario, self.data_root), "--remote", f"sc://127.0.0.1:{self.port}", "--path", self.path],
            {},
        )

    def explain_command(self, scenario: Scenario, target: Path) -> Command | None:
        return None

    def log_mark(self) -> dict[Path, int]:
        return logscan.mark([self.log_dir / "sail_server.log"])

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        """Workery sesji przebiegu: Sail tworzy driver i N workerów dla każdej sesji Spark Connect
        (przy pierwszym RPC sesji) — liczba z logu pokazuje, czy Sail nie dołożył workera."""
        return {"sail_workers": logscan.sail_workers(logscan.text_since(mark))}

    def server_pids(self) -> dict[str, int]:
        return {"sail_server": self.proc.pid} if self.proc is not None else {}

    def shuffle_dirs(self) -> list[Path]:
        return []


def make_engine(variant: str, n_nodes: int, *, data_root: Path, log_dir: Path, profile: str):
    """Silnik bloku (wariant × N) według specyfikacji 3."""
    if variant == REFERENCE_VARIANT:
        return PolarsBioEngine(1, node_cpus(1), data_root)
    if variant == "polars_bio_a":
        return PolarsBioEngine(SLOTS_PER_NODE, node_cpus(1), data_root)
    if variant == "polars_bio_b":
        return PolarsBioEngine(SLOTS_PER_NODE * n_nodes, cluster_cpus(n_nodes), data_root)
    if variant == "ballista":
        return BallistaEngine(n_nodes, profile=profile, data_root=data_root, log_dir=log_dir)
    if variant in SAIL_PATHS:
        return SailEngine(n_nodes, data_root=data_root, log_dir=log_dir, path=SAIL_PATHS[variant])
    raise ValueError(f"unknown variant {variant!r}")

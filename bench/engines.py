"""Silniki bloku pomiarowego (specyfikacja, sekcje 3 i 6): przypięcie do rdzeni (emulacja
węzłów), klaster Ballisty, serwer Saila i polecenia runnerów.

Silnik ma interfejs: start(), stop(), restart(), command(scenariusz) → Command,
server_pids() → {nazwa: PID} (procesy długożyjące, których szczyt pamięci mierzy
orkiestrator) i shuffle_dirs() → katalogi robocze executorów (wolumen shuffle)."""

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
#: Wynik wzorcowy: polars-bio na 1 partycji — przy większej liczbie partycji polars-bio 0.28
#: psuje merge i subtract (bench/runners/polars_bio_runner.py).
REFERENCE_VARIANT = "polars_bio_ref"
#: Przebieg kontrolny (specyfikacja 6.3): polars-bio A.
CONTROL_VARIANT = "polars_bio_a"


def node_cpus(k: int) -> tuple[int, int]:
    """Węzeł k (1..3) = rdzeń fizyczny k = wątki {2k, 2k+1} (lscpu -e)."""
    if k not in (1, 2, 3):
        raise ValueError(f"węzeł {k!r}: dozwolone 1–3")
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

    def stop(self) -> None:
        pass

    def restart(self) -> None:
        pass

    def command(self, scenario: Scenario) -> Command:
        return _python(
            "bench.runners.polars_bio_runner",
            self.cpus,
            [*scenario_args(scenario, self.data_root), "--threads", str(self.threads)],
            {"POLARS_MAX_THREADS": str(self.threads)},
        )

    def server_pids(self) -> dict[str, int]:
        return {}

    def shuffle_dirs(self) -> list[Path]:
        return []


def ballista_binary(name: str, profile: str) -> Path:
    return BALLISTA_DIR / "target" / profile / name


def require_ballista_binaries(profile: str) -> None:
    missing = [
        str(b)
        for b in (ballista_binary("ballista_node", profile), ballista_binary("bench_client", profile))
        if not b.exists()
    ]
    if missing:
        flag = "--release " if profile == "release" else ""
        raise EngineError(
            f"brak binarki {', '.join(missing)} — zbuduj: cd ballista_genomics && "
            f"CARGO_BUILD_JOBS=1 cargo build {flag}--bin ballista_node --bin bench_client"
        )


def _registered_executors(port: int) -> int:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/executors", timeout=2) as response:
        return len(json.loads(response.read()))


class BallistaEngine:
    """Scheduler na rdzeniu systemowym + N executorów (executor k na rdzeniu węzła k,
    po 2 sloty); BIO_TARGET_PARTITIONS = 2N we wszystkich procesach i w kliencie."""

    def __init__(self, n_nodes: int, *, profile: str, data_root: Path, log_dir: Path):
        self.n, self.profile, self.data_root, self.log_dir = n_nodes, profile, data_root, log_dir
        self.env = {**os.environ, "BIO_TARGET_PARTITIONS": str(SLOTS_PER_NODE * n_nodes)}
        self.procs: dict[str, subprocess.Popen] = {}
        self.work_dirs: dict[str, Path] = {}
        self.port: int | None = None
        self._tmp: Path | None = None

    def start(self) -> None:
        require_ballista_binaries(self.profile)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._tmp = Path(tempfile.mkdtemp(prefix="bench_ballista_"))
        self.port = free_port()
        try:
            self._spawn("scheduler", SYSTEM_CPUS, ["scheduler", "--port", str(self.port)])
            # Executor łączy się ze schedulerem tylko raz, przy starcie (P0) — najpierw scheduler.
            self._wait(lambda: _registered_executors(self.port) >= 0, "scheduler nie odpowiada")
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
                f"scheduler nie widzi {self.n} executorów",
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
            except OSError:
                pass
            time.sleep(0.3)
        raise EngineError(f"{message} po {timeout:.0f} s; logi: {self.log_dir}")

    def stop(self) -> None:
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        self.procs.clear()
        self.work_dirs.clear()
        if self._tmp is not None:
            shutil.rmtree(self._tmp, ignore_errors=True)
            self._tmp = None

    def restart(self) -> None:
        self.stop()
        self.start()

    def command(self, scenario: Scenario) -> Command:
        return Command(
            [*taskset(SYSTEM_CPUS), str(ballista_binary("bench_client", self.profile)),
             *scenario_args(scenario, self.data_root)],
            {**self.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{self.port}"},
            BALLISTA_DIR,
        )

    def server_pids(self) -> dict[str, int]:
        return {name: proc.pid for name, proc in self.procs.items()}

    def shuffle_dirs(self) -> list[Path]:
        return list(self.work_dirs.values())


class SailEngine:
    """Jeden proces serwera Saila (local-cluster, N workerów) na rdzeniach węzłów 1..N;
    runner-klient na rdzeniu systemowym."""

    def __init__(self, n_nodes: int, *, data_root: Path, log_dir: Path):
        self.n, self.data_root, self.log_dir = n_nodes, data_root, log_dir
        self.proc: subprocess.Popen | None = None
        self.port: int | None = None

    def start(self) -> None:
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

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None

    def restart(self) -> None:
        self.stop()
        self.start()

    def command(self, scenario: Scenario) -> Command:
        return _python(
            "bench.runners.sail_runner",
            SYSTEM_CPUS,
            [*scenario_args(scenario, self.data_root), "--remote", f"sc://127.0.0.1:{self.port}"],
            {},
        )

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
    if variant == "sail":
        return SailEngine(n_nodes, data_root=data_root, log_dir=log_dir)
    raise ValueError(f"nieznany wariant {variant!r}")

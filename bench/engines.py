"""Engines of a measurement block: pinning to CPUs from the machine layout (bench/cpus.py), the
Ballista cluster, the Sail server and the runner commands.

Equal resources at parallelism P: polars-bio — one process with P threads on P CPUs; Ballista —
P executors with 1 slot each, every one pinned to a single CPU; Sail — one server process with
P workers of 1 slot each, pinned to P CPUs. The Ballista scheduler and the clients run on the
system core. An engine has a `cpus` attribute — the block's CPUs (recorded in the results,
SMT flag).

An engine's interface: start(), stop(), restart(), command(scenario) → Command,
server_pids() → {name: PID} (long-lived processes whose peak memory the orchestrator
measures) and shuffle_dirs() → the executors' working directories (shuffle volume).
log_mark() and log_metrics(mark) → counters from the engine process logs for the duration of a
run (Sail workers, Ballista broadcast).
explain_command(scenario, file) → Command or None (EXPLAIN ANALYZE; Ballista only).
dead() → names of engine processes that died; stop(grace_s) and restart(grace_s) — SIGKILL
grace_s seconds after SIGTERM.
env_record() → environment variables set by the tool (recorded in the series' env.json)."""

from __future__ import annotations

import http.client
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
from bench.cpus import CpuLayout, control_cpus, cpus_for
from bench.data.datasets import resolve
from bench.procutil import EngineError, check_alive, free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent


def _ballista_submodule() -> Path:
    return REPO / "engines" / "ballista-genomics"


def ballista_dir() -> Path:
    """Ballista project directory: the engines/ballista-genomics submodule (in the benchmark
    repository), if it has a Cargo.toml, otherwise a single-repository layout with
    `ballista_genomics/` next to `bench/`. An uninitialised submodule is an empty directory: the
    fallback is used then too, and require_ballista_binaries asks to initialise the submodule."""
    submodule = _ballista_submodule()
    return submodule if (submodule / "Cargo.toml").is_file() else REPO / "ballista_genomics"


BALLISTA_DIR = ballista_dir()
PROFILES = ("release", "debug")
#: Ballista gRPC message limit: in nearest and coverage the stage plan carries the whole
#: broadcast table (~29 MiB for 2-7, ~56 MiB for 7-0). Ballista 55 encodes the plan separately for
#: each task and sends the tasks assigned to an executor in one round as a single message; an
#: executor has 1 slot, so the message carries one copy of the table (Ballista 53: one per
#: executor and stage). Ballista's default 16 MiB is not enough; the broadcast guard counts with
#: a margin MESSAGE_SIZE_MARGIN = 2 copies, so with 1 slot it has headroom.
GRPC_MAX_MESSAGE_BYTES = 512 * 2**20
#: Task slots of a Ballista executor: P executors with 1 slot each, every one on a single CPU.
BALLISTA_EXECUTOR_SLOTS = 1
#: Smallest number of partitions of a Ballista session: the binaries reject
#: BIO_TARGET_PARTITIONS < 2 (ballista_genomics/src/runner.rs, target_partitions — with 1
#: partition DataFusion inserts no hash repartitioning and the query runs in a single stage,
#: without distribution). At P = 1 the only executor, with 1 slot, therefore computes 2
#: partitions one after the other.
BALLISTA_MIN_TARGET_PARTITIONS = 2
#: Reference result: polars-bio on 1 partition (1 thread, 1 CPU) — with more partitions
#: polars-bio 0.28 broke merge and subtract (bench/runners/polars_bio_runner.py).
REFERENCE_VARIANT = "polars_bio_ref"
#: Control run: polars-bio with 2 threads on both hardware threads of the first core outside
#: the system core (bench.cpus.control_cpus) — independent of the block's P.
CONTROL_VARIANT = "polars_bio_control"
CONTROL_THREADS = 2
#: Start attempts of the Ballista cluster — a safeguard, not a workaround for a known bug.
#: Ballista 53 registered an executor with the scheduler before its gRPC server accepted
#: connections, and the scheduler in push mode connected to it at once: under CPU load the
#: registration was sometimes refused ("Connection refused") and the executor exited with
#: code 1 (smoke run, 2026-10-02). Ballista 55 binds the executor's gRPC port before
#: registering (#2225; ballista_executor::executor_server, step 1 before step 2), so that cause
#: is gone; retrying stays for other failed starts (port in use, registration wait timeout).
BALLISTA_START_ATTEMPTS = 3
#: Time to exit after SIGTERM on a normal engine stop (then SIGKILL).
BALLISTA_STOP_GRACE_S = 10.0
SAIL_STOP_GRACE_S = 30.0
#: Start attempts of the Sail server (as BALLISTA_START_ATTEMPTS; a single failed start, e.g. a busy port, should not abort a series).
SAIL_START_ATTEMPTS = 3
#: Variables that steer the engines, removed from the environment inherited from the shell: a
#: value exported e.g. in .bashrc (SAIL_MODE, POLARS_MAX_THREADS, RUST_LOG…) would silently
#: change the run's configuration. An engine sets the ones it needs itself — see env_record().
INHERITED_PREFIXES = (
    "SAIL_", "BALLISTA_", "POLARS_", "BIO_", "SPARK_", "PYSPARK_", "RUST_LOG", "DIST_OUTPUT_DIR",
    "DATAFUSION_", "TOKIO_", "RAYON_", "OMP_",
)


def base_env() -> dict[str, str]:
    """Environment of engine and runner processes: inherited without the engine-steering
    variables, with a windowless matplotlib backend (bench/__init__.py)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(INHERITED_PREFIXES)}
    env["MPLBACKEND"] = "Agg"
    return env


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
        {**base_env(), **env},
        REPO,
    )


class PolarsBioEngine:
    """polars-bio: a fresh process per run, no long-lived processes."""

    def __init__(self, threads: int, cpus, data_root: Path):
        self.threads, self.cpus, self.data_root = threads, tuple(cpus), data_root

    def env_record(self) -> dict[str, str]:
        return {"POLARS_MAX_THREADS": str(self.threads)}

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


def _shown(path: Path) -> str:
    """Path in a message: relative to the repository if it lies in it (a command to paste)."""
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


def ballista_binary(name: str, profile: str) -> Path:
    return BALLISTA_DIR / "target" / profile / name


def ballista_sources(binary: str | None = None) -> list[Path]:
    """Sources shared by the Ballista binaries: Cargo.toml, Cargo.lock, the Rust code in src/
    (except src/bin/) and in vendor/ (manifests too) — without documentation, whose change does
    not require a rebuild. With a binary name, its own entry point src/bin/<name>.rs is added:
    cargo relinks only the binaries whose entry points changed, so other binaries' entry points
    do not count."""
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
    """The binaries exist and each is newer than its own sources (shared ones and its entry
    point) — otherwise the measurement would cover code from before the latest change."""
    names = ("ballista_node", "bench_client")
    binaries = {name: ballista_binary(name, profile) for name in names}
    flag = "--release " if profile == "release" else ""
    where = _shown(BALLISTA_DIR)
    build = (
        f"cd {where} && CARGO_BUILD_JOBS=1 cargo build {flag}--bin ballista_node --bin bench_client"
        f" (if cargo has nothing to rebuild, touch {where}/src/lib.rs first)"
    )
    missing = [str(b) for b in binaries.values() if not b.exists()]
    submodule = _ballista_submodule()
    if missing and submodule.is_dir() and not (submodule / "Cargo.toml").is_file():
        sub = _shown(submodule)
        raise EngineError(
            f"missing Ballista binaries: the {sub} submodule is not initialised - run "
            f"git submodule update --init --recursive, then build: cd {sub} && CARGO_BUILD_JOBS=1 "
            f"cargo build {flag}--bin ballista_node --bin bench_client"
        )
    if missing:
        raise EngineError(f"missing binary {', '.join(missing)} - build it: {build}")
    for name, binary in binaries.items():
        newest = max(ballista_sources(name), key=lambda p: p.stat().st_mtime, default=None)
        if newest is not None and binary.stat().st_mtime < newest.stat().st_mtime:
            raise EngineError(f"binary {binary} is older than {newest} - rebuild it: {build}")


def _registered_executors(port: int) -> int:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/executors", timeout=2) as response:
        return len(json.loads(response.read()))


def _require_cpus(parallelism: int, cpus: tuple[int, ...]) -> None:
    """An engine with P processes/workers gets exactly P CPUs (one per Ballista executor)."""
    if len(cpus) != parallelism:
        raise ValueError(f"parallelism {parallelism} needs {parallelism} CPUs, got {list(cpus)}")


class BallistaEngine:
    """Scheduler on the system core (`system`) + P executors with 1 slot each: executor k on a
    single CPU, cpus[k-1]. BIO_TARGET_PARTITIONS = P (at least 2, see
    BALLISTA_MIN_TARGET_PARTITIONS) and BALLISTA_GRPC_MAX_MESSAGE_BYTES in all processes and in
    the client; the client runs on `system`."""

    def __init__(self, parallelism: int, cpus, *, system, profile: str, data_root: Path,
                 log_dir: Path, grpc_max_message_bytes: int = GRPC_MAX_MESSAGE_BYTES):
        self.cpus, self.system = tuple(cpus), tuple(system)
        _require_cpus(parallelism, self.cpus)
        self.parallelism, self.profile = parallelism, profile
        self.data_root, self.log_dir = data_root, log_dir
        # At least 2 partitions: the binaries reject fewer (see BALLISTA_MIN_TARGET_PARTITIONS).
        partitions = max(parallelism, BALLISTA_MIN_TARGET_PARTITIONS)
        self.env = {
            **base_env(),
            "BIO_TARGET_PARTITIONS": str(partitions),
            "BALLISTA_GRPC_MAX_MESSAGE_BYTES": str(grpc_max_message_bytes),
        }
        self.procs: dict[str, subprocess.Popen] = {}
        self.work_dirs: dict[str, Path] = {}
        self.port: int | None = None
        self._tmp: Path | None = None

    def env_record(self) -> dict[str, str]:
        keys = ("BIO_TARGET_PARTITIONS", "BALLISTA_GRPC_MAX_MESSAGE_BYTES")
        return {k: self.env[k] for k in keys}

    def start(self) -> None:
        """Start the cluster; after a failure (e.g. an executor rejected at registration) start
        again, on new ports, up to BALLISTA_START_ATTEMPTS times. Logs of later attempts are
        appended."""
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
            self._spawn("scheduler", self.system, ["scheduler", "--port", str(self.port)])
            # An executor connects to the scheduler only once, at startup — the scheduler goes first.
            self._wait(lambda: _registered_executors(self.port) >= 0, "scheduler does not respond")
            for k, cpu in enumerate(self.cpus, start=1):
                name = f"executor_{k}"
                work = self._tmp / name
                work.mkdir()
                self.work_dirs[name] = work
                self._spawn(name, (cpu,), [
                    "executor", "--scheduler-port", str(self.port),
                    "--port", str(free_port()), "--grpc-port", str(free_port()),
                    "--work-dir", str(work), "--concurrent-tasks", str(BALLISTA_EXECUTOR_SLOTS),
                ])
            self._wait(
                lambda: _registered_executors(self.port) == self.parallelism,
                f"scheduler does not see {self.parallelism} executors",
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
            except (OSError, ValueError, http.client.HTTPException):
                pass  # the scheduler does not respond yet, or responds incompletely (JSON, IncompleteRead, BadStatusLine)
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
            [*taskset(self.system), str(ballista_binary("bench_client", self.profile)),
             *scenario_args(scenario, self.data_root)],
            {**self.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{self.port}"},
            BALLISTA_DIR,
        )

    def explain_command(self, scenario: Scenario, target: Path) -> Command:
        """An extra, unmeasured EXPLAIN ANALYZE run."""
        command = self.command(scenario)
        return Command([*command.argv, "--explain-analyze", str(target)], command.env, command.cwd)

    def log_mark(self) -> dict[Path, int]:
        return logscan.mark(self.log_dir / f"executor_{k}.log" for k in range(1, self.parallelism + 1))

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        """Bytes of broadcast payloads received by the executors during the run
        (bio_phys_codec.rs logs every decoding of a node with a broadcast)."""
        return {"broadcast_bytes": logscan.broadcast_bytes(logscan.text_since(mark))}

    def server_pids(self) -> dict[str, int]:
        return {name: proc.pid for name, proc in self.procs.items()}

    def shuffle_dirs(self) -> list[Path]:
        return list(self.work_dirs.values())


class SailEngine:
    """One Sail server process (local-cluster, P workers with 1 slot each) pinned to the block's
    P CPUs (`cpus`); the client runner runs on the system core (`system`)."""

    def __init__(self, parallelism: int, cpus, *, system, data_root: Path, log_dir: Path):
        self.cpus, self.system = tuple(cpus), tuple(system)
        _require_cpus(parallelism, self.cpus)
        self.parallelism, self.data_root, self.log_dir = parallelism, data_root, log_dir
        self.proc: subprocess.Popen | None = None
        self.port: int | None = None

    def env_record(self) -> dict[str, str]:
        """SAIL_* variables (local-cluster, P workers) and an explicit polars-bio thread count in
        the server process — P, like the block's CPUs (without an explicit POLARS_MAX_THREADS the
        server would use the default thread count of the whole machine)."""
        return {**sail_env(self.parallelism), "POLARS_MAX_THREADS": str(self.parallelism)}

    def start(self) -> None:
        """Start the server; after a failure (e.g. a busy port) start again, on a new port, up to
        SAIL_START_ATTEMPTS times. The log of later attempts is appended."""
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
                [*taskset(self.cpus), sys.executable, "-m", "bench.runners.sail_server",
                 "--port", str(self.port)],
                cwd=REPO, env={**base_env(), **self.env_record()},
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
            self.system,
            [*scenario_args(scenario, self.data_root), "--remote", f"sc://127.0.0.1:{self.port}"],
            {},
        )

    def explain_command(self, scenario: Scenario, target: Path) -> Command | None:
        return None

    def log_mark(self) -> dict[Path, int]:
        return logscan.mark([self.log_dir / "sail_server.log"])

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        """Session workers of the run: Sail creates a driver and P workers for every Spark Connect
        session (at the session's first RPC) — the count from the log shows whether Sail added a
        worker."""
        return {"sail_workers": logscan.sail_workers(logscan.text_since(mark))}

    def server_pids(self) -> dict[str, int]:
        return {"sail_server": self.proc.pid} if self.proc is not None else {}

    def shuffle_dirs(self) -> list[Path]:
        return []


def make_engine(variant: str, parallelism: int, *, layout: CpuLayout, data_root: Path,
                log_dir: Path, profile: str):
    """The engine of a block (variant × P) on CPUs from `layout`. The reference (1 thread, 1 CPU)
    and the control run (CONTROL_THREADS threads on control_cpus) have a fixed configuration —
    `parallelism` does not change it. A P larger than the layout → bench.cpus.NotEnoughCpus (the
    orchestrator skips such blocks earlier)."""
    if variant == REFERENCE_VARIANT:
        return PolarsBioEngine(1, cpus_for(layout, 1), data_root)
    if variant == CONTROL_VARIANT:
        return PolarsBioEngine(CONTROL_THREADS, control_cpus(layout), data_root)
    if variant == "polars_bio":
        return PolarsBioEngine(parallelism, cpus_for(layout, parallelism), data_root)
    if variant == "ballista":
        return BallistaEngine(
            parallelism, cpus_for(layout, parallelism), system=layout.system, profile=profile,
            data_root=data_root, log_dir=log_dir,
        )
    if variant == "sail":
        return SailEngine(
            parallelism, cpus_for(layout, parallelism), system=layout.system, data_root=data_root,
            log_dir=log_dir,
        )
    raise ValueError(f"unknown variant {variant!r}")

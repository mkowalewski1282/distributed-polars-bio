"""Silniki serii (plan 3a, Zadanie 7; etap 4: semantyka P): przypięcie procesów do CPU z układu
(bench/cpus.py), jawna liczba wątków i polecenia runnerów. Testy bez procesów silników używają
układu laptopa z fałszywego sysfs; testy z prawdziwym klastrem — układu tej maszyny.

Testy klastra Ballisty wymagają binarek debug (brak = BŁĄD, jak w P0):
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins"""

from __future__ import annotations

import itertools
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from bench import engines, metrics
from bench.config import Scenario
from bench.cpus import NotEnoughCpus, cpus_for, read_layout
from bench.procutil import EngineError
from bench.runners.sail_server import WORKER_TASK_SLOTS, sail_env
from tests.bench.fake_sysfs import make

#: Laptop pomiarowy: 6 rdzeni, rdzeń k = CPU {2k, 2k+1} (lscpu -e).
LAPTOP_CORES = [[2 * k, 2 * k + 1] for k in range(6)]


@pytest.fixture
def laptop(tmp_path):
    return read_layout(make(tmp_path, LAPTOP_CORES), affinity=set(range(12)))


@pytest.fixture
def machine():
    """Układ CPU tej maszyny — dla testów uruchamiających prawdziwe procesy silników."""
    return read_layout()


def environ(pid: int) -> set[str]:
    return {e.decode() for e in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if e}


def allowed(pid: int) -> set[int]:
    """CPU, na których proces może działać (Cpus_allowed_list, np. '0-1,4')."""
    out: set[int] = set()
    for part in metrics.cpus_allowed(pid).split(","):
        first, _, last = part.partition("-")
        out.update(range(int(first), int(last or first) + 1))
    return out


def cpu_list(cpus) -> str:
    return ",".join(str(c) for c in cpus)


def engine(variant, parallelism, layout, tmp_path, **kwargs):
    kwargs.setdefault("log_dir", tmp_path / "logs")
    return engines.make_engine(
        variant, parallelism, layout=layout, data_root=tmp_path, profile="debug", **kwargs
    )


def test_taskset_lists_cpus():
    assert engines.taskset((2, 3)) == ["taskset", "-c", "2,3"]
    assert engines.taskset((2, 4, 3)) == ["taskset", "-c", "2,4,3"]


@pytest.mark.parametrize(
    "variant, p, threads, cpus",
    [
        ("polars_bio_ref", 1, 1, "2"),
        ("polars_bio_control", 2, 2, "2,3"),
        ("polars_bio", 1, 1, "2"),
        ("polars_bio", 4, 4, "2,4,6,8"),
        ("polars_bio", 8, 8, "2,4,6,8,10,3,5,7"),
    ],
)
def test_polars_bio_runs_pinned_with_explicit_threads(variant, p, threads, cpus, laptop, tmp_path):
    engine_ = engine(variant, p, laptop, tmp_path)
    cmd = engine_.command(Scenario("overlap", "1-2"))
    base = tmp_path / "databio-8p"
    assert cmd.argv == [
        "taskset", "-c", cpus, sys.executable, "-m", "bench.runners.polars_bio_runner",
        "--op", "overlap", "--left", str(base / "fBrain-DS14718"), "--right", str(base / "exons"),
        "--threads", str(threads),
    ]
    assert cmd.env["POLARS_MAX_THREADS"] == str(threads) and cmd.env["MPLBACKEND"] == "Agg"
    assert cmd.cwd == engines.REPO
    assert engine_.server_pids() == {} and engine_.shuffle_dirs() == []
    assert engine_.cpus == tuple(int(c) for c in cpus.split(","))


def test_control_engine_uses_two_threads_on_first_core(laptop, tmp_path):
    """Przebieg kontrolny (specyfikacja 5.1): polars-bio, 2 wątki na obu wątkach pierwszego
    rdzenia poza systemowym — niezależnie od P bloku."""
    control = engine(engines.CONTROL_VARIANT, engines.CONTROL_THREADS, laptop, tmp_path)
    assert engines.CONTROL_VARIANT == "polars_bio_control"
    assert control.threads == 2 and control.cpus == (2, 3)


def test_reference_engine_uses_one_thread_on_one_cpu(laptop, tmp_path):
    reference = engine(engines.REFERENCE_VARIANT, 1, laptop, tmp_path)
    assert reference.threads == 1 and reference.cpus == (2,)


def test_parallelism_beyond_the_layout_is_rejected(laptop, tmp_path):
    for variant in ("polars_bio", "ballista", "sail"):
        with pytest.raises(NotEnoughCpus):
            engine(variant, 11, laptop, tmp_path)


def test_merge_command_has_no_right_side(laptop, tmp_path):
    assert "--right" not in engine("polars_bio", 1, laptop, tmp_path).command(Scenario("merge", "1")).argv


def test_unknown_variant_is_rejected(laptop, tmp_path):
    for variant in ("spark", "polars_bio_a", "polars_bio_b"):
        with pytest.raises(ValueError, match="unknown variant"):
            engine(variant, 1, laptop, tmp_path)


class _FakePopen:
    """Proces bez procesu: zapisuje argv i środowisko, żyje do terminate()."""

    def __init__(self, argv, **kwargs):
        self.argv, self.env, self.returncode, self.pid = argv, kwargs.get("env"), None, 0

    def poll(self):
        return self.returncode

    def terminate(self):
        self.returncode = -15

    kill = terminate

    def wait(self, timeout=None):
        return self.returncode


def test_ballista_pins_one_executor_per_cpu(laptop, tmp_path, monkeypatch):
    """P = 4: cztery executory po 1 slocie, executor k na k-tym CPU układu; scheduler i klient
    na rdzeniu systemowym; BIO_TARGET_PARTITIONS = P w każdym procesie."""
    spawned: list[_FakePopen] = []

    def popen(argv, **kwargs):
        spawned.append(_FakePopen(argv, **kwargs))
        return spawned[-1]

    monkeypatch.setattr(engines, "require_ballista_binaries", lambda profile: None)
    monkeypatch.setattr(engines.subprocess, "Popen", popen)
    monkeypatch.setattr(
        engines, "_registered_executors", lambda port: sum("executor" in p.argv for p in spawned)
    )
    ballista = engine("ballista", 4, laptop, tmp_path)
    ballista.start()
    try:
        scheduler, *executors = spawned
        assert scheduler.argv[:3] == ["taskset", "-c", "0,1"] and "scheduler" in scheduler.argv
        assert [p.argv[:3] for p in executors] == [["taskset", "-c", str(c)] for c in (2, 4, 6, 8)]
        for p in executors:
            flag = p.argv.index("--concurrent-tasks")
            assert p.argv[flag + 1] == "1"
        assert all(p.env["BIO_TARGET_PARTITIONS"] == "4" for p in spawned)
        assert ballista.cpus == (2, 4, 6, 8)
        cmd = ballista.command(Scenario("merge", "1"))
        assert cmd.argv[:3] == ["taskset", "-c", "0,1"] and cmd.env["BIO_TARGET_PARTITIONS"] == "4"
    finally:
        ballista.stop()


def test_ballista_keeps_at_least_two_target_partitions(laptop, tmp_path):
    """P = 1: binarki Ballisty odrzucają BIO_TARGET_PARTITIONS < 2 (runner.rs, target_partitions:
    przy 1 partycji DataFusion nie wstawia hash-repartycji), więc jeden executor z 1 slotem liczy
    2 partycje po kolei."""
    assert engine("ballista", 1, laptop, tmp_path).env["BIO_TARGET_PARTITIONS"] == "2"
    assert engine("ballista", 2, laptop, tmp_path).env["BIO_TARGET_PARTITIONS"] == "2"
    assert engine("ballista", 8, laptop, tmp_path).env["BIO_TARGET_PARTITIONS"] == "8"


def test_sail_env_uses_parallelism():
    env = sail_env(4)
    assert env["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] == "4"
    assert env["SAIL_EXECUTION__DEFAULT_PARALLELISM"] == "4"
    assert env["SAIL_CLUSTER__WORKER_TASK_SLOTS"] == str(WORKER_TASK_SLOTS) == "1"


def test_sail_engine_pins_server_to_the_block_cpus(laptop, tmp_path):
    sail = engine("sail", 4, laptop, tmp_path)
    assert sail.cpus == (2, 4, 6, 8) and sail.system == (0, 1)
    assert sail.command(Scenario("overlap", "1-2")).argv[:3] == ["taskset", "-c", "0,1"]


def test_ballista_dir_prefers_submodule(tmp_path, monkeypatch):
    """Repozytorium pomiarowe po podziale ma Ballistę w submodule engines/ballista-genomics;
    monorepo — w ballista_genomics/. Brak binarek w submodule to błąd z poleceniem budowania
    w tym katalogu, nie FileNotFoundError."""
    monkeypatch.setattr(engines, "REPO", tmp_path)
    assert engines.ballista_dir() == tmp_path / "ballista_genomics"
    submodule = tmp_path / "engines" / "ballista-genomics"
    submodule.mkdir(parents=True)
    assert engines.ballista_dir() == tmp_path / "ballista_genomics"  # pusty submoduł (bez init)
    (submodule / "Cargo.toml").write_text("[package]\n")
    assert engines.ballista_dir() == submodule
    monkeypatch.setattr(engines, "BALLISTA_DIR", submodule)
    with pytest.raises(EngineError, match=r"missing binary .*cd engines/ballista-genomics && "
                                          r"CARGO_BUILD_JOBS=1 cargo build --release"):
        engines.require_ballista_binaries("release")


def test_missing_ballista_binaries_are_reported_with_build_command(tmp_path, monkeypatch):
    monkeypatch.setattr(engines, "BALLISTA_DIR", tmp_path)
    with pytest.raises(EngineError, match=r"missing binary .*ballista_node.*cargo build --release"):
        engines.require_ballista_binaries("release")


@pytest.mark.parallelism(2)
def test_ballista_cluster_is_pinned_and_configured(machine, tmp_path):
    engine = engines.make_engine(
        "ballista", 2, layout=machine, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug",
    )
    first, second = cpus_for(machine, 2)
    engine.start()
    try:
        pids = engine.server_pids()
        assert {name: allowed(pid) for name, pid in pids.items()} == {
            "scheduler": set(machine.system), "executor_1": {first}, "executor_2": {second},
        }
        assert all("BIO_TARGET_PARTITIONS=2" in environ(pid) for pid in pids.values())
        for name in ("executor_1", "executor_2"):
            assert ", slots 1, " in (tmp_path / "logs" / f"{name}.log").read_text()
        cmd = engine.command(Scenario("merge", "1"))
        # Limit gRPC 512 MiB w każdym procesie (specyfikacja 3b-2, 6.2) — i potwierdzenie z węzłów.
        assert all("BALLISTA_GRPC_MAX_MESSAGE_BYTES=536870912" in environ(pid) for pid in pids.values())
        assert cmd.env["BALLISTA_GRPC_MAX_MESSAGE_BYTES"] == "536870912"
        for name in pids:
            assert "grpc max message 536870912 B" in (tmp_path / "logs" / f"{name}.log").read_text()
        assert cmd.argv[:3] == ["taskset", "-c", cpu_list(machine.system)]
        assert cmd.argv[3] == str(engines.BALLISTA_DIR / "target" / "debug" / "bench_client")
        assert cmd.env["BALLISTA_SCHEDULER_URL"] == f"df://localhost:{engine.port}"
        assert cmd.env["BIO_TARGET_PARTITIONS"] == "2" and cmd.cwd == engines.BALLISTA_DIR
        dirs = engine.shuffle_dirs()
        assert len(dirs) == 2 and all(d.is_dir() for d in dirs)
    finally:
        engine.stop()
    assert not any(Path(f"/proc/{pid}").exists() for pid in pids.values())
    assert not any(d.exists() for d in dirs)


@pytest.mark.parallelism(1)
def test_ballista_start_retries_when_executor_registration_fails(machine, tmp_path, monkeypatch):
    """Nieudany start executora (kod 1) ma powtórzyć start klastra na nowych portach.
    Ballista 53 rejestrowała executor, zanim jego serwer gRPC przyjmował połączenia, i przy
    zajętym porcie gRPC rejestracja była odrzucona („Connection refused”; ten sam objaw dawał
    wyścig ze smoke 02.10.2026, blok N = 3). Ballista 55 wiąże port przed rejestracją (#2225),
    więc zajęty port kończy executor już przy wiązaniu („Address already in use”)."""
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))  # zajęty (bez listen()): wiązanie przez executor się nie uda
    taken = blocker.getsockname()[1]
    real_free_port = engines.free_port
    calls = itertools.count(1)
    # Pierwsza próba losuje porty: schedulera, flight executora 1 i gRPC executora 1 (zajęty).
    monkeypatch.setattr(engines, "free_port", lambda: taken if next(calls) == 3 else real_free_port())
    engine = engines.make_engine(
        "ballista", 1, layout=machine, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug",
    )
    try:
        engine.start()
        assert set(engine.server_pids()) == {"scheduler", "executor_1"}
        assert "Address already in use" in (tmp_path / "logs" / "executor_1.log").read_text()
    finally:
        engine.stop()
        blocker.close()


@pytest.mark.parallelism(1)
def test_ballista_restart_replaces_processes(machine, tmp_path):
    engine = engines.make_engine(
        "ballista", 1, layout=machine, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug",
    )
    engine.start()
    try:
        before = set(engine.server_pids().values())
        engine.restart()
        after = set(engine.server_pids().values())
        assert len(after) == 2 and before.isdisjoint(after)
    finally:
        engine.stop()


@pytest.mark.parallelism(2)
def test_sail_server_is_pinned_and_configured(machine, tmp_path):
    engine = engines.make_engine(
        "sail", 2, layout=machine, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug",
    )
    engine.start()
    try:
        pid = engine.server_pids()["sail_server"]
        assert allowed(pid) == set(cpus_for(machine, 2))
        assert {
            "SAIL_MODE=local-cluster",
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT=2",
            "SAIL_CLUSTER__WORKER_TASK_SLOTS=1",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM=2",
            "POLARS_MAX_THREADS=2",
            "MPLBACKEND=Agg",
        } <= environ(pid)
        cmd = engine.command(Scenario("overlap", "1-2"))
        assert cmd.argv[:6] == [
            "taskset", "-c", cpu_list(machine.system), sys.executable, "-m", "bench.runners.sail_runner",
        ]
        assert cmd.argv[-2:] == ["--remote", f"sc://127.0.0.1:{engine.port}"]
        assert cmd.cwd == engines.REPO
    finally:
        engine.stop()
    assert not Path(f"/proc/{pid}").exists()


def test_sail_engine_counts_session_workers_from_its_log(laptop, tmp_path):
    engine = engines.make_engine(
        "sail", 2, layout=laptop, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug",
    )
    log = tmp_path / "logs" / "sail_server.log"
    log.parent.mkdir()
    log.write_text("worker 1 server is ready\nworker 2 server is ready\nworker 3 server is ready\n")
    mark = engine.log_mark()
    with log.open("a") as f:
        f.write("creating session\nworker 1 server is ready\nworker 2 server is ready\n")
    assert engine.log_metrics(mark) == {"sail_workers": 2}


def test_polars_bio_engine_has_no_log_metrics(laptop, tmp_path):
    engine_ = engine("polars_bio", 2, laptop, tmp_path)
    assert engine_.log_metrics(engine_.log_mark()) == {}


def _fake_ballista(tmp_path: Path) -> Path:
    root = tmp_path / "ballista_genomics"
    (root / "src").mkdir(parents=True)
    (root / "src" / "lib.rs").write_text("// source\n")
    (root / "src" / "bin").mkdir()
    for entry in ("ballista_node", "bench_client", "dist_ops"):
        (root / "src" / "bin" / f"{entry}.rs").write_text("// entry point\n")
    (root / "Cargo.toml").write_text("[package]\n")
    (root / "Cargo.lock").write_text("")
    (root / "vendor" / "crate" / "src").mkdir(parents=True)
    (root / "vendor" / "crate" / "PATCH.md").write_text("docs\n")
    (root / "target" / "debug").mkdir(parents=True)
    for name in ("ballista_node", "bench_client"):
        (root / "target" / "debug" / name).write_bytes(b"binary")
    for source in (root / "Cargo.toml", root / "Cargo.lock", *(root / "src" / "bin").iterdir()):
        _set_mtime(source, 500_000)  # manifesty starsze od binarek — o wyniku decyduje lib.rs
    return root


def _set_mtime(path: Path, t: float) -> None:
    os.utime(path, (t, t))


def test_stale_ballista_binary_is_rejected(tmp_path, monkeypatch):
    root = _fake_ballista(tmp_path)
    monkeypatch.setattr(engines, "BALLISTA_DIR", root)
    for binary in (root / "target" / "debug").iterdir():
        _set_mtime(binary, 1_000_000)
    _set_mtime(root / "src" / "lib.rs", 2_000_000)
    with pytest.raises(EngineError, match=r"ballista_node is older than .*lib\.rs.*cargo build"):
        engines.require_ballista_binaries("debug")
    _set_mtime(root / "src" / "lib.rs", 500_000)
    engines.require_ballista_binaries("debug")  # binarki nowsze od źródeł


def test_documentation_in_vendor_does_not_make_binaries_stale(tmp_path, monkeypatch):
    root = _fake_ballista(tmp_path)
    monkeypatch.setattr(engines, "BALLISTA_DIR", root)
    _set_mtime(root / "vendor" / "crate" / "PATCH.md", 9_000_000_000)
    engines.require_ballista_binaries("debug")


def test_other_binaries_entry_points_do_not_make_a_binary_stale(tmp_path, monkeypatch):
    root = _fake_ballista(tmp_path)
    monkeypatch.setattr(engines, "BALLISTA_DIR", root)
    _set_mtime(root / "src" / "lib.rs", 500_000)
    _set_mtime(root / "target" / "debug" / "ballista_node", 1_000_000)
    _set_mtime(root / "target" / "debug" / "bench_client", 3_000_000)
    for entry in ("dist_ops", "bench_client"):
        _set_mtime(root / "src" / "bin" / f"{entry}.rs", 2_000_000)
        engines.require_ballista_binaries("debug")  # ballista_node nie zależy od tych punktów wejścia
    _set_mtime(root / "src" / "bin" / "bench_client.rs", 4_000_000)
    with pytest.raises(EngineError, match=r"bench_client is older than .*bench_client\.rs"):
        engines.require_ballista_binaries("debug")


def test_stop_kills_a_process_that_ignores_sigterm(laptop, tmp_path):
    engine = engines.make_engine("ballista", 1, layout=laptop, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    proc = subprocess.Popen(
        [sys.executable, "-c",
         "import signal, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
         "print('ready', flush=True); time.sleep(60)"],
        stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout.readline().strip() == "ready"
    engine.procs = {"scheduler": proc}
    t0 = time.monotonic()
    engine.stop(grace_s=1)
    assert time.monotonic() - t0 < 10 and proc.poll() is not None


@pytest.mark.parallelism(1)
def test_ballista_reports_a_dead_process(machine, tmp_path):
    engine = engines.make_engine(
        "ballista", 1, layout=machine, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug",
    )
    engine.start()
    try:
        assert engine.dead() == []
        os.kill(engine.server_pids()["executor_1"], signal.SIGKILL)
        deadline = time.monotonic() + 10
        while not engine.dead() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert engine.dead() == ["executor_1"]
    finally:
        engine.stop()


@pytest.mark.parallelism(1)
def test_sail_start_retries_on_a_taken_port(machine, tmp_path, monkeypatch):
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))  # zajęty bez listen(): serwer nie zbinduje portu i zakończy się
    taken = blocker.getsockname()[1]
    real_free_port = engines.free_port
    calls = itertools.count(1)
    monkeypatch.setattr(engines, "free_port", lambda: taken if next(calls) == 1 else real_free_port())
    engine = engines.make_engine(
        "sail", 1, layout=machine, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug",
    )
    try:
        engine.start()
        assert engine.port != taken and engine.dead() == []
    finally:
        engine.stop()
        blocker.close()


def test_polars_bio_engine_never_reports_dead_processes(laptop, tmp_path):
    engine_ = engine("polars_bio", 1, laptop, tmp_path)
    assert engine_.dead() == []
    engine_.restart(grace_s=1)


def test_inherited_engine_variables_are_dropped(laptop, tmp_path, monkeypatch):
    for name, value in {"SAIL_MODE": "local", "POLARS_MAX_THREADS": "12", "RUST_LOG": "warn",
                        "BIO_TARGET_PARTITIONS": "8", "BALLISTA_GRPC_MAX_MESSAGE_BYTES": "1",
                        "TOKIO_WORKER_THREADS": "3"}.items():
        monkeypatch.setenv(name, value)
    env = engines.base_env()
    assert not {"SAIL_MODE", "POLARS_MAX_THREADS", "RUST_LOG", "BIO_TARGET_PARTITIONS",
                "BALLISTA_GRPC_MAX_MESSAGE_BYTES", "TOKIO_WORKER_THREADS"} & set(env)
    assert env["MPLBACKEND"] == "Agg" and env["PATH"] == os.environ["PATH"]
    pb_env = engine("polars_bio", 2, laptop, tmp_path).command(Scenario("overlap", "1-2")).env
    assert pb_env["POLARS_MAX_THREADS"] == "2" and "SAIL_MODE" not in pb_env and "RUST_LOG" not in pb_env
    ballista = engine("ballista", 4, laptop, tmp_path)
    assert ballista.env["BIO_TARGET_PARTITIONS"] == "4" and "RUST_LOG" not in ballista.env


def test_env_records_list_what_the_tool_sets(laptop, tmp_path):
    assert engine("polars_bio", 4, laptop, tmp_path).env_record() == {"POLARS_MAX_THREADS": "4"}
    assert engine("ballista", 4, laptop, tmp_path).env_record() == {
        "BIO_TARGET_PARTITIONS": "4", "BALLISTA_GRPC_MAX_MESSAGE_BYTES": "536870912",
    }
    sail = engine("sail", 4, laptop, tmp_path).env_record()
    assert sail["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] == "4" and sail["POLARS_MAX_THREADS"] == "4"

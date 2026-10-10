"""Silniki serii (plan 3a, Zadanie 7; specyfikacja 3): przypięcie procesów do rdzeni
emulowanych węzłów, jawna liczba wątków i polecenia runnerów.

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
from bench.procutil import EngineError


def environ(pid: int) -> set[str]:
    return {e.decode() for e in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if e}


def test_cpu_layout_follows_spec():
    assert engines.SYSTEM_CPUS == (0, 1)
    assert [engines.node_cpus(k) for k in (1, 2, 3)] == [(2, 3), (4, 5), (6, 7)]
    assert engines.cluster_cpus(3) == (2, 3, 4, 5, 6, 7)
    assert engines.taskset((2, 3)) == ["taskset", "-c", "2,3"]
    with pytest.raises(ValueError):
        engines.node_cpus(4)


@pytest.mark.parametrize(
    "variant, n, threads, cpus",
    [
        ("polars_bio_ref", 1, 1, "2,3"),
        ("polars_bio_a", 1, 2, "2,3"),
        ("polars_bio_b", 2, 4, "2,3,4,5"),
        ("polars_bio_b", 3, 6, "2,3,4,5,6,7"),
    ],
)
def test_polars_bio_runs_pinned_with_explicit_threads(variant, n, threads, cpus, tmp_path):
    engine = engines.make_engine(variant, n, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    cmd = engine.command(Scenario("overlap", "1-2"))
    base = tmp_path / "databio-8p"
    assert cmd.argv == [
        "taskset", "-c", cpus, sys.executable, "-m", "bench.runners.polars_bio_runner",
        "--op", "overlap", "--left", str(base / "fBrain-DS14718"), "--right", str(base / "exons"),
        "--threads", str(threads),
    ]
    assert cmd.env["POLARS_MAX_THREADS"] == str(threads) and cmd.env["MPLBACKEND"] == "Agg"
    assert cmd.cwd == engines.REPO
    assert engine.server_pids() == {} and engine.shuffle_dirs() == []


def test_merge_command_has_no_right_side(tmp_path):
    engine = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert "--right" not in engine.command(Scenario("merge", "1")).argv


def test_unknown_variant_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="unknown variant"):
        engines.make_engine("spark", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")


def test_missing_ballista_binaries_are_reported_with_build_command(tmp_path, monkeypatch):
    monkeypatch.setattr(engines, "BALLISTA_DIR", tmp_path)
    with pytest.raises(EngineError, match=r"missing binary .*ballista_node.*cargo build --release"):
        engines.require_ballista_binaries("release")


@pytest.mark.nodes(2)
def test_ballista_cluster_is_pinned_and_configured(tmp_path):
    engine = engines.make_engine("ballista", 2, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    engine.start()
    try:
        pids = engine.server_pids()
        assert {name: metrics.cpus_allowed(pid) for name, pid in pids.items()} == {
            "scheduler": "0-1", "executor_1": "2-3", "executor_2": "4-5",
        }
        assert all("BIO_TARGET_PARTITIONS=4" in environ(pid) for pid in pids.values())
        cmd = engine.command(Scenario("merge", "1"))
        # Limit gRPC 512 MiB w każdym procesie (specyfikacja 3b-2, 6.2) — i potwierdzenie z węzłów.
        assert all("BALLISTA_GRPC_MAX_MESSAGE_BYTES=536870912" in environ(pid) for pid in pids.values())
        assert cmd.env["BALLISTA_GRPC_MAX_MESSAGE_BYTES"] == "536870912"
        for name in pids:
            assert "grpc max message 536870912 B" in (tmp_path / "logs" / f"{name}.log").read_text()
        assert cmd.argv[:3] == ["taskset", "-c", "0,1"]
        assert cmd.argv[3] == str(engines.BALLISTA_DIR / "target" / "debug" / "bench_client")
        assert cmd.env["BALLISTA_SCHEDULER_URL"] == f"df://localhost:{engine.port}"
        assert cmd.env["BIO_TARGET_PARTITIONS"] == "4" and cmd.cwd == engines.BALLISTA_DIR
        dirs = engine.shuffle_dirs()
        assert len(dirs) == 2 and all(d.is_dir() for d in dirs)
    finally:
        engine.stop()
    assert not any(Path(f"/proc/{pid}").exists() for pid in pids.values())
    assert not any(d.exists() for d in dirs)


@pytest.mark.nodes(1)
def test_ballista_start_retries_when_executor_registration_fails(tmp_path, monkeypatch):
    """Ballista 53 rejestruje executor, zanim jego serwer gRPC przyjmuje połączenia (TODO
    w ballista_executor::executor_server), a scheduler w trybie push od razu łączy się
    z executorem: rejestracja bywa odrzucona („Connection refused”) i executor kończy się
    kodem 1 (smoke 02.10.2026, blok N = 3). Ten sam objaw daje zajęty port gRPC executora —
    start klastra ma się wtedy powtórzyć na nowych portach."""
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))  # zajęty, ale bez listen(): połączenie → Connection refused
    taken = blocker.getsockname()[1]
    real_free_port = engines.free_port
    calls = itertools.count(1)
    # Pierwsza próba losuje porty: schedulera, flight executora 1 i gRPC executora 1 (zajęty).
    monkeypatch.setattr(engines, "free_port", lambda: taken if next(calls) == 3 else real_free_port())
    engine = engines.make_engine("ballista", 1, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    try:
        engine.start()
        assert set(engine.server_pids()) == {"scheduler", "executor_1"}
        assert "Connection refused" in (tmp_path / "logs" / "executor_1.log").read_text()
    finally:
        engine.stop()
        blocker.close()


@pytest.mark.nodes(1)
def test_ballista_restart_replaces_processes(tmp_path):
    engine = engines.make_engine("ballista", 1, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    engine.start()
    try:
        before = set(engine.server_pids().values())
        engine.restart()
        after = set(engine.server_pids().values())
        assert len(after) == 2 and before.isdisjoint(after)
    finally:
        engine.stop()


@pytest.mark.nodes(2)
def test_sail_server_is_pinned_and_configured(tmp_path):
    engine = engines.make_engine("sail", 2, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    engine.start()
    try:
        pid = engine.server_pids()["sail_server"]
        assert metrics.cpus_allowed(pid) == "2-5"
        assert {
            "SAIL_MODE=local-cluster",
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT=2",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM=4",
            "POLARS_MAX_THREADS=4",
            "MPLBACKEND=Agg",
        } <= environ(pid)
        cmd = engine.command(Scenario("overlap", "1-2"))
        assert cmd.argv[:6] == ["taskset", "-c", "0,1", sys.executable, "-m", "bench.runners.sail_runner"]
        assert cmd.argv[-4:] == ["--remote", f"sc://127.0.0.1:{engine.port}", "--path", "arrow"]
        assert cmd.cwd == engines.REPO
    finally:
        engine.stop()
    assert not Path(f"/proc/{pid}").exists()


@pytest.mark.parametrize("variant, path", [("sail", "arrow"), ("sail_lock", "arrow-lock"), ("sail_udtf", "udtf")])
def test_sail_variants_choose_runner_path(variant, path, tmp_path):
    engine = engines.make_engine(variant, 2, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    argv = engine.command(Scenario("overlap", "1-2")).argv
    assert argv[argv.index("--path") + 1] == path


def test_sail_engine_counts_session_workers_from_its_log(tmp_path):
    engine = engines.make_engine("sail", 2, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    log = tmp_path / "logs" / "sail_server.log"
    log.parent.mkdir()
    log.write_text("worker 1 server is ready\nworker 2 server is ready\nworker 3 server is ready\n")
    mark = engine.log_mark()
    with log.open("a") as f:
        f.write("creating session\nworker 1 server is ready\nworker 2 server is ready\n")
    assert engine.log_metrics(mark) == {"sail_workers": 2}


def test_polars_bio_engine_has_no_log_metrics(tmp_path):
    engine = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert engine.log_metrics(engine.log_mark()) == {}


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


def test_stop_kills_a_process_that_ignores_sigterm(tmp_path):
    engine = engines.make_engine("ballista", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
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


@pytest.mark.nodes(1)
def test_ballista_reports_a_dead_process(tmp_path):
    engine = engines.make_engine("ballista", 1, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
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


@pytest.mark.nodes(1)
def test_sail_start_retries_on_a_taken_port(tmp_path, monkeypatch):
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))  # zajęty bez listen(): serwer nie zbinduje portu i zakończy się
    taken = blocker.getsockname()[1]
    real_free_port = engines.free_port
    calls = itertools.count(1)
    monkeypatch.setattr(engines, "free_port", lambda: taken if next(calls) == 1 else real_free_port())
    engine = engines.make_engine("sail", 1, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    try:
        engine.start()
        assert engine.port != taken and engine.dead() == []
    finally:
        engine.stop()
        blocker.close()


def test_polars_bio_engine_never_reports_dead_processes(tmp_path):
    engine = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert engine.dead() == []
    engine.restart(grace_s=1)


def test_inherited_engine_variables_are_dropped(tmp_path, monkeypatch):
    for name, value in {"SAIL_MODE": "local", "POLARS_MAX_THREADS": "12", "RUST_LOG": "warn",
                        "BIO_TARGET_PARTITIONS": "8", "BALLISTA_GRPC_MAX_MESSAGE_BYTES": "1"}.items():
        monkeypatch.setenv(name, value)
    env = engines.base_env()
    assert not {"SAIL_MODE", "POLARS_MAX_THREADS", "RUST_LOG", "BIO_TARGET_PARTITIONS",
                "BALLISTA_GRPC_MAX_MESSAGE_BYTES"} & set(env)
    assert env["MPLBACKEND"] == "Agg" and env["PATH"] == os.environ["PATH"]
    pb_env = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug").command(
        Scenario("overlap", "1-2")).env
    assert pb_env["POLARS_MAX_THREADS"] == "2" and "SAIL_MODE" not in pb_env and "RUST_LOG" not in pb_env
    ballista = engines.make_engine("ballista", 2, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert ballista.env["BIO_TARGET_PARTITIONS"] == "4" and "RUST_LOG" not in ballista.env


def test_env_records_list_what_the_tool_sets(tmp_path):
    make = lambda v, n: engines.make_engine(v, n, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert make("polars_bio_b", 3).env_record() == {"POLARS_MAX_THREADS": "6"}
    assert make("ballista", 2).env_record() == {
        "BIO_TARGET_PARTITIONS": "4", "BALLISTA_GRPC_MAX_MESSAGE_BYTES": "536870912",
    }
    sail = make("sail_lock", 3).env_record()
    assert sail["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] == "3" and sail["POLARS_MAX_THREADS"] == "6"

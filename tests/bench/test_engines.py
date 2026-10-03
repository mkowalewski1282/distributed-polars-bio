"""Silniki serii (plan 3a, Zadanie 7; specyfikacja 3): przypięcie procesów do rdzeni
emulowanych węzłów, jawna liczba wątków i polecenia runnerów.

Testy klastra Ballisty wymagają binarek debug (brak = BŁĄD, jak w P0):
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins"""

from __future__ import annotations

import itertools
import socket
import sys
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
            "MPLBACKEND=Agg",
        } <= environ(pid)
        cmd = engine.command(Scenario("overlap", "1-2"))
        assert cmd.argv[:6] == ["taskset", "-c", "0,1", sys.executable, "-m", "bench.runners.sail_runner"]
        assert cmd.argv[-2:] == ["--remote", f"sc://127.0.0.1:{engine.port}"]
        assert cmd.cwd == engines.REPO
    finally:
        engine.stop()
    assert not Path(f"/proc/{pid}").exists()

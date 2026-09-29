"""
Zadanie P0: dowód, że Ballista wykonuje operacje genomiczne w OSOBNYCH
procesach. Scheduler i dwa executory startują z binarki `ballista_node`,
klient `dist_ops` łączy się z nimi przez zmienną BALLISTA_SCHEDULER_URL.

Cztery dowody (specyfikacja metodyki, sekcja 9.1):
  1. osobne procesy — różne PID-y schedulera, executorów i klienta, a scheduler
     widzi dwa zarejestrowane executory (REST: /api/executors);
  2. wynik zgodny z wyrocznią polars-bio (te same wyrocznie co testy standalone);
  3. praca na OBU executorach — executor zapisuje dane każdego policzonego etapu
     w SWOIM katalogu roboczym (work_dir/job_id/stage_id/...), więc katalogi
     pokazują wprost, kto co liczył;
  4. kontrola negatywna — executor bez koderów (--no-codecs) nie potrafi
     zdekodować planu, więc zapytanie musi zakończyć się błędem.

Bez pomiarów czasu — P0 pyta „czy rozproszone”, nie „jak szybko”.

Brak binarek to BŁĄD, nie pominięcie testu: dowód P0 nie może cicho zniknąć
z wyników. Budowanie: cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build
--bin ballista_node --bin dist_ops

Uruchomienie: pytest tests/test_ballista_multiprocess.py -v
Testy operacji importują wyrocznie polars-bio — import trwa kilka minut.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import pytest

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
TARGET_DIR = BALLISTA_DIR / "target" / "debug"
NODE_BINARY = TARGET_DIR / "ballista_node"
DIST_BINARY = TARGET_DIR / "dist_ops"
OUTPUT_DIR = BALLISTA_DIR / "output"
BUILD_HINT = (
    "cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build "
    "--bin ballista_node --bin dist_ops"
)


def _require(binary: Path) -> None:
    if not binary.exists():
        pytest.fail(f"brak binarki {binary.name} — zbuduj: {BUILD_HINT}")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Cluster:
    scheduler_port: int
    log_dir: Path
    procs: dict[str, subprocess.Popen] = field(default_factory=dict)
    work_dirs: dict[str, Path] = field(default_factory=dict)

    @property
    def url(self) -> str:
        return f"df://localhost:{self.scheduler_port}"

    def pids(self) -> dict[str, int]:
        return {name: p.pid for name, p in self.procs.items()}

    def stop(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.terminate()
        for p in self.procs.values():
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()


def _spawn(args: list[str], log: Path) -> subprocess.Popen:
    with log.open("w") as f:
        return subprocess.Popen(
            [str(NODE_BINARY), *args],
            cwd=BALLISTA_DIR,
            stdout=f,
            stderr=subprocess.STDOUT,
        )


def _registered_executors(port: int) -> list[dict]:
    url = f"http://127.0.0.1:{port}/api/executors"
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.loads(response.read())


def _wait_for_executors(cluster: Cluster, expected: int, timeout: float = 60.0) -> list[dict]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        for name, p in cluster.procs.items():
            if p.poll() is not None:
                pytest.fail(
                    f"proces {name} zakończył się przedwcześnie (kod {p.returncode}); "
                    f"logi w {cluster.log_dir}"
                )
        try:
            executors = _registered_executors(cluster.scheduler_port)
            if len(executors) == expected:
                return executors
        except OSError as e:
            last_error = e
        time.sleep(0.5)
    pytest.fail(
        f"scheduler nie zarejestrował {expected} executorów w {timeout:.0f} s "
        f"(ostatni błąd: {last_error}); logi w {cluster.log_dir}"
    )


def _start_cluster(log_dir: Path, executors: list[tuple[str, list[str]]]) -> Cluster:
    """Scheduler + executory jako osobne procesy; każdy executor ma własny
    katalog roboczy i 2 sloty zadań (specyfikacja, sekcja 9.1)."""
    _require(NODE_BINARY)
    cluster = Cluster(scheduler_port=_free_port(), log_dir=log_dir)
    try:
        cluster.procs["scheduler"] = _spawn(
            ["scheduler", "--port", str(cluster.scheduler_port)],
            log_dir / "scheduler.log",
        )
        for name, extra_args in executors:
            work_dir = log_dir / f"work_{name}"
            work_dir.mkdir()
            cluster.work_dirs[name] = work_dir
            cluster.procs[name] = _spawn(
                [
                    "executor",
                    "--scheduler-port", str(cluster.scheduler_port),
                    "--port", str(_free_port()),
                    "--grpc-port", str(_free_port()),
                    "--work-dir", str(work_dir),
                    "--concurrent-tasks", "2",
                    *extra_args,
                ],
                log_dir / f"{name}.log",
            )
        _wait_for_executors(cluster, expected=len(executors))
    except BaseException:
        cluster.stop()
        raise
    return cluster


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    c = _start_cluster(
        tmp_path_factory.mktemp("p0_klaster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


def test_ballista_node_rejects_bad_arguments():
    """Literówka w nazwie flagi albo brak wymaganej flagi nie może uruchomić
    węzła z wartościami domyślnymi — ma skończyć się kodem 2 i instrukcją użycia."""
    _require(NODE_BINARY)
    for args in (
        [],
        ["executor", "--port", "1"],
        ["scheduler", "--port", "1", "--nieznana-flaga"],
    ):
        r = subprocess.run(
            [str(NODE_BINARY), *args],
            cwd=BALLISTA_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
        assert "użycie" in r.stderr, f"{args}: brak instrukcji użycia: {r.stderr}"


def test_start_cluster_reports_crashed_process(tmp_path):
    """Gdy węzeł padnie przy starcie, pomocnik ma wskazać KTÓRY, zamiast czekać
    w nieskończoność na rejestrację."""
    with pytest.raises(pytest.fail.Exception, match="executor_zly"):
        _start_cluster(tmp_path, [("executor_zly", ["--nieznana-flaga"])])


def test_cluster_stop_terminates_all_processes(tmp_path):
    c = _start_cluster(tmp_path, [("executor_1", [])])
    c.stop()
    still_running = [name for name, p in c.procs.items() if p.poll() is None]
    assert not still_running, f"po stop() nadal działają: {still_running}"


def test_scheduler_sees_two_separate_executors(cluster):
    """Dowód 1 (część klastrowa): trzy osobne procesy, a scheduler widzi dwa
    RÓŻNE executory — różne identyfikatory i porty."""
    pids = cluster.pids()
    assert len(set(pids.values())) == 3, f"PID-y nie są różne: {pids}"
    assert all(p.poll() is None for p in cluster.procs.values())
    executors = _registered_executors(cluster.scheduler_port)
    assert len({e["id"] for e in executors}) == 2, executors
    assert len({e["port"] for e in executors}) == 2, executors

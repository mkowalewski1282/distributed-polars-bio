"""Plan 3a, Zadanie 5: Sail jako osobny proces serwera (local-cluster, N workerów) i runner-
-klient (specyfikacja 8.3): wynik zgodny z polars-bio, protokół jak w bench_client."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent
PROTOCOL_KEYS = {"rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes"}


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    port = free_port()
    log = tmp_path_factory.mktemp("sail_server") / "server.log"
    with log.open("w") as f:
        proc = subprocess.Popen(
            [sys.executable, "-m", "bench.runners.sail_server", "--port", str(port)],
            cwd=REPO, env={**os.environ, **sail_env(2)}, stdout=f, stderr=subprocess.STDOUT,
        )
    try:
        wait_for_port(port, proc, name="sail_server", log=log)
        yield SimpleNamespace(url=f"sc://127.0.0.1:{port}", log=log)
    finally:
        proc.terminate()
        proc.wait(timeout=30)


def run_runner(args: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "bench.runners.sail_runner", *args],
        cwd=REPO, capture_output=True, text=True, timeout=timeout,
    )


def scenario_args(op, a, b) -> list[str]:
    args = ["--op", op, "--left", str(a)]
    return args if op in UNARY_OPS else [*args, "--right", str(b)]


def report(r: subprocess.CompletedProcess) -> dict:
    assert r.returncode == 0, f"exit code {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout must be one JSON line, got:\n{r.stdout}"
    return json.loads(lines[0])


def test_sail_env_for_three_nodes():
    assert sail_env(3) == {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "3",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "8",
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "86400",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "6",
    }


@pytest.mark.parametrize("op", OPS)
def test_runner_matches_polars_bio(op, server, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--remote", server.url]))
    assert set(got) == PROTOCOL_KEYS
    expected = parquet_expected[op]
    assert got["rows"] == sum(expected.values())
    assert got["checksum"] == cs.format_checksum(cs.checksum_rows(op, expected.elements()))
    assert got["t_total_s"] > 0 and got["phases"] == {} and set(got["extra"]) == {"checksum_s"}
    # Czas liczenia sumy kontrolnej — część t_total_s (przegląd końcowy planu 3a).
    assert 0 < got["extra"]["checksum_s"] <= got["t_total_s"]
    assert got["peak_rss_bytes"] > 10 * 2**20


def test_server_keeps_exactly_n_workers(server, parquet_dirs):
    """Przy 2 slotach Sail dokładałby workery ponad N (sonda 01.10.2026); tu N = 2.

    Sail tworzy driver i pulę workerów dla KAŻDEJ sesji Spark Connect (przy pierwszym RPC
    sesji — w runnerze rejestracja UDTF) i zamyka je razem z sesją, więc numeracja workerów
    zaczyna się w każdej sesji od 1. Sprawdzana jest sesja otwarta przez ten test: pierwsza
    sesja na świeżym serwerze dostała raz jednego workera więcej (wyścig przy starcie puli,
    02.10.2026)."""
    report(run_runner([*scenario_args("overlap", *parquet_dirs), "--remote", server.url]))
    log = server.log.read_text()
    session = log[log.rindex("creating session"):]
    workers = set(re.findall(r"worker (\d+) server is ready", session))
    assert workers == {"1", "2"}


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--op", "merge", "--left", "x"],
        ["--op", "overlap", "--left", "x", "--remote", "sc://127.0.0.1:1"],
        ["--op", "merge", "--left", "x", "--right", "y", "--remote", "sc://127.0.0.1:1"],
    ],
)
def test_runner_rejects_bad_arguments(args):
    r = run_runner(args, timeout=60)
    assert r.returncode == 2, f"{args}: exit code {r.returncode}, stderr: {r.stderr}"
    assert r.stdout == ""


def test_missing_data_path_is_reported(server, tmp_path):
    missing = tmp_path / "no_such_dir"
    r = run_runner(["--op", "merge", "--left", str(missing), "--remote", server.url])
    assert r.returncode == 1, r.stderr
    assert str(missing) in r.stderr and r.stdout == ""

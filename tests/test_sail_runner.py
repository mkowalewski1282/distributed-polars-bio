"""Sail as a separate server process (local-cluster, P workers) and a client runner:
the result agrees with polars-bio, and the protocol is the same as in bench_client."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from bench import checksum as cs, logscan
from bench.ops import OPS, UNARY_OPS
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_server import sail_env
from tests.parquet_fixture import write_parts

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


def test_sail_env_for_parallelism_3():
    assert sail_env(3) == {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "3",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "1",
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "86400",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "3",
        "SAIL_EXECUTION__BATCH_SIZE": "65536",
    }


#: Phases and extras of the runner's JSON line.
PHASE_KEYS = {"first_batch_s", "pb_s"}
EXTRA_KEYS = {"checksum_s", "udf_groups"}


@pytest.mark.parametrize("op", OPS)
def test_runner_matches_polars_bio(op, server, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--remote", server.url]))
    assert set(got) == PROTOCOL_KEYS
    expected = parquet_expected[op]
    assert got["rows"] == sum(expected.values())
    assert got["checksum"] == cs.format_checksum(cs.checksum_rows(op, expected.elements()))
    assert set(got["phases"]) == PHASE_KEYS and set(got["extra"]) == EXTRA_KEYS
    assert got["t_total_s"] > 0 and got["extra"]["checksum_s"] > 0
    assert 0 < got["phases"]["first_batch_s"] <= got["t_total_s"]
    # chr1, chr2, chr10, chrA: chromosomes with a non-empty left side
    assert got["extra"]["udf_groups"] == 4 and got["phases"]["pb_s"] > 0
    assert got["peak_rss_bytes"] > 10 * 2**20


def test_empty_result_reports_zero_rows(server, tmp_path):
    left = write_parts([("chrA", 10, 20), ("chrA", 30, 40)], tmp_path / "a")
    right = write_parts([("chrB", 10, 20), ("chrB", 30, 40)], tmp_path / "b")
    got = report(run_runner(["--op", "overlap", "--left", str(left), "--right", str(right), "--remote", server.url]))
    assert (got["rows"], got["checksum"]) == (0, "0x0000000000000000")
    assert 0 <= got["phases"]["first_batch_s"] <= got["t_total_s"]


def test_server_keeps_exactly_n_workers(server, parquet_dirs):
    """With 2 slots Sail would add workers beyond the requested number (probe of 2026-10-01); here P = 2.

    Sail creates a driver and a worker pool for EVERY Spark Connect session (on the session's
    first RPC; in the runner the `warm_up` warm-up before the clock) and closes them together with
    the session, so worker numbering starts from 1 in every session. The session opened by this
    test is the one checked: the first session on a fresh server once got one worker more
    (a race at pool startup, 2026-10-02)."""
    report(run_runner([*scenario_args("overlap", *parquet_dirs), "--remote", server.url]))
    log = server.log.read_text()
    session = log[log.rindex("creating session"):]
    assert logscan.sail_workers(session) == 2


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--op", "merge", "--left", "x"],
        ["--op", "overlap", "--left", "x", "--remote", "sc://127.0.0.1:1"],
        ["--op", "merge", "--left", "x", "--right", "y", "--remote", "sc://127.0.0.1:1"],
        ["--op", "merge", "--left", "x", "--remote", "sc://127.0.0.1:1", "--path", "arrow"],
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

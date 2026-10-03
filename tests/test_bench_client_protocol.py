"""Plan 3a, Zadanie 3: protokół runnera Ballisty (specyfikacja 8.3) — czas, suma kontrolna
zgodna z bench/checksum.py (8.4), szczyt pamięci, liczba partycji z BIO_TARGET_PARTITIONS —
oraz zgodność implementacji sumy w Rust z Pythonem na wspólnych wartościach wzorcowych.

Brak binarki to BŁĄD (jak w P0). Budowanie:
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins
"""

from __future__ import annotations

import json
import os
import subprocess

import pyarrow.parquet as pq
import pytest

from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS
from tests.bench import checksum_vectors as vec
from tests.test_ballista_multiprocess import NODE_BINARY, _require
from tests.test_ballista_parquet import BALLISTA_DIR, run_client

PROTOCOL_KEYS = {"rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes"}


def report(r: subprocess.CompletedProcess) -> dict:
    assert r.returncode == 0, f"kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    return json.loads(lines[0])


def scenario_args(op, a, b) -> list[str]:
    args = ["--op", op, "--left", str(a)]
    return args if op in UNARY_OPS else [*args, "--right", str(b)]


def expected_checksum(op, parquet_expected) -> str:
    return cs.format_checksum(cs.checksum_rows(op, parquet_expected[op].elements()))


@pytest.mark.parametrize("op", OPS)
def test_rust_checksum_matches_golden_values(op, tmp_path):
    path = tmp_path / f"{op}.parquet"
    pq.write_table(vec.table(op), path)
    got = report(run_client(["--op", op, "--checksum", str(path)], timeout=60))
    assert got == {"rows": len(vec.ROWS[op]), "checksum": vec.CHECKSUMS[op]}


@pytest.mark.parametrize(
    "args",
    [
        ["--checksum", "x.parquet"],
        ["--op", "merge", "--checksum", "x.parquet", "--left", "y"],
        ["--op", "merge", "--checksum", "x.parquet", "--output", "y.parquet"],
    ],
)
def test_checksum_mode_rejects_other_arguments(args):
    r = run_client(args, timeout=30)
    assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
    assert "usage" in r.stderr and r.stdout == ""


@pytest.mark.parametrize("op", OPS)
def test_report_follows_protocol_and_matches_oracle(op, parquet_dirs, parquet_expected):
    got = report(run_client(scenario_args(op, *parquet_dirs)))
    assert set(got) == PROTOCOL_KEYS
    assert got["rows"] == sum(parquet_expected[op].values())
    assert got["checksum"] == expected_checksum(op, parquet_expected)
    assert got["t_total_s"] > 0 and got["phases"] == {}
    assert set(got["extra"]) == {"target_partitions", "checksum_s"}
    assert got["extra"]["target_partitions"] == 4
    # Czas liczenia sumy kontrolnej — część t_total_s (przegląd końcowy planu 3a).
    assert 0 < got["extra"]["checksum_s"] <= got["t_total_s"]
    assert got["peak_rss_bytes"] > 10 * 2**20


def test_target_partitions_come_from_environment(parquet_dirs, parquet_expected):
    env = {**os.environ, "BIO_TARGET_PARTITIONS": "3"}
    got = report(run_client(scenario_args("subtract", *parquet_dirs), env=env))
    assert got["extra"]["target_partitions"] == 3
    assert got["checksum"] == expected_checksum("subtract", parquet_expected)


@pytest.mark.parametrize("value", ["1", "zero", "-4"])
def test_invalid_target_partitions_is_a_usage_error(value, parquet_dirs):
    env = {**os.environ, "BIO_TARGET_PARTITIONS": value}
    r = run_client(["--op", "merge", "--left", str(parquet_dirs[0])], env=env, timeout=30)
    assert r.returncode == 2, r.stderr
    assert "BIO_TARGET_PARTITIONS" in r.stderr and r.stdout == ""


def test_ballista_node_rejects_invalid_target_partitions():
    _require(NODE_BINARY)
    r = subprocess.run(
        [str(NODE_BINARY), "scheduler", "--port", "1"],
        cwd=BALLISTA_DIR,
        env={**os.environ, "BIO_TARGET_PARTITIONS": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 2, r.stderr
    assert "BIO_TARGET_PARTITIONS" in r.stderr

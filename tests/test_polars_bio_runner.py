"""Plan 3a, Zadanie 4: runner polars-bio (specyfikacja 8.3) — świeży proces, polars-bio czyta
Parquet sam, wynik konsumowany strumieniowo, protokół jak w bench_client.

Przy target_partitions > 1 polars-bio 0.28 liczy merge i subtract osobno w każdej partycji
(znany błąd polars-bio #372, naprawiony w 0.29+, która wymaga Pythona ≥ 3.11) — te przypadki
z 2 partycjami są oznaczone xfail(strict=True): po migracji do nowszego polars-bio test to zgłosi."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS

REPO = Path(__file__).resolve().parent.parent
PROTOCOL_KEYS = {"rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes"}
_PARTITION_BUG = pytest.mark.xfail(
    raises=AssertionError,
    strict=True,
    reason="polars-bio 0.28 (#372): przy target_partitions > 1 merge i subtract liczone osobno w każdej partycji",
)


def run_runner(args: list[str], timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "bench.runners.polars_bio_runner", *args],
        cwd=REPO, capture_output=True, text=True, timeout=timeout,
    )


def scenario_args(op, a, b) -> list[str]:
    args = ["--op", op, "--left", str(a)]
    return args if op in UNARY_OPS else [*args, "--right", str(b)]


def report(r: subprocess.CompletedProcess) -> dict:
    assert r.returncode == 0, f"kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    return json.loads(lines[0])


def expected_checksum(op, parquet_expected) -> str:
    return cs.format_checksum(cs.checksum_rows(op, parquet_expected[op].elements()))


@pytest.mark.parametrize("op", OPS)
def test_single_partition_matches_polars_bio_oracle(op, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--threads", "1"]))
    assert set(got) == PROTOCOL_KEYS
    assert got["rows"] == sum(parquet_expected[op].values())
    assert got["checksum"] == expected_checksum(op, parquet_expected)
    assert set(got["extra"]) == {"target_partitions", "checksum_s"} and got["phases"] == {}
    assert got["extra"]["target_partitions"] == 1
    assert got["t_total_s"] > 0 and got["peak_rss_bytes"] > 10 * 2**20
    # Czas liczenia sumy kontrolnej — część t_total_s (przegląd końcowy planu 3a).
    assert 0 < got["extra"]["checksum_s"] <= got["t_total_s"]


@pytest.mark.parametrize(
    "op", [pytest.param(op, marks=_PARTITION_BUG) if op in ("merge", "subtract") else op for op in OPS]
)
def test_two_partitions(op, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--threads", "2"]))
    assert got["extra"]["target_partitions"] == 2
    assert got["checksum"] == expected_checksum(op, parquet_expected)


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--op", "merge", "--threads", "1"],
        ["--op", "overlap", "--left", "x", "--threads", "1"],
        ["--op", "merge", "--left", "x", "--right", "y", "--threads", "1"],
        ["--op", "merge", "--left", "x", "--threads", "0"],
        ["--op", "merge", "--left", "x"],
        ["--op", "merge", "--left", "x", "--threads", "1", "--cols", "contig,pos_start"],
        ["--op", "join", "--left", "x", "--threads", "1"],
    ],
)
def test_rejects_bad_arguments(args):
    r = run_runner(args, timeout=60)
    assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
    assert r.stdout == ""


def test_missing_data_path_is_reported(tmp_path):
    missing = tmp_path / "nie_ma_takiego_katalogu"
    r = run_runner(["--op", "merge", "--left", str(missing), "--threads", "1"])
    assert r.returncode == 1, r.stderr
    assert str(missing) in r.stderr and r.stdout == ""

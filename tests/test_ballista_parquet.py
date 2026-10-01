"""Plan 2, Zadanie 4: Ballista czyta dane w układzie databio-8p (katalog plików
Parquet, contig: string, pos_start/pos_end: int32) przez runner `bench_client`.

Wynik każdej z pięciu operacji ma być zgodny z polars-bio na tych samych
plikach (multizbiór wierszy w schemacie znormalizowanym, specyfikacja 8.4) —
w trybie standalone i na klastrze z osobnych procesów (P0). Zbiór testowy
(tests/parquet_fixture.py) zawiera przypadki, na których silniki mogą się
rozjechać: chromosom tylko po jednej stronie, duplikat, przedziały stykające
się, chr10 obok chr2.

Brak binarki to BŁĄD, nie pominięcie (jak w P0). Budowanie:
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin bench_client --bin ballista_node
Uruchomienie: pytest tests/test_ballista_parquet.py -v
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pytest

from bench.ops import OPS, OUTPUT_COLUMNS, UNARY_OPS, describe_diff, row_multiset
from tests.test_ballista_multiprocess import _start_cluster

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
CLIENT = BALLISTA_DIR / "target" / "debug" / "bench_client"
BUILD_HINT = (
    "cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin bench_client --bin ballista_node"
)


def run_client(
    args: list[str], env: dict[str, str] | None = None, timeout: int = 180
) -> subprocess.CompletedProcess:
    if not CLIENT.exists():
        pytest.fail(f"brak binarki bench_client — zbuduj: {BUILD_HINT}")
    return subprocess.run(
        [str(CLIENT), *args], cwd=BALLISTA_DIR, capture_output=True, text=True,
        timeout=timeout, env=env,
    )


def op_args(op: str, left: Path, right: Path | None, out: Path) -> list[str]:
    args = ["--op", op, "--left", str(left), "--output", str(out)]
    return args if op in UNARY_OPS else [*args, "--right", str(right)]


def check_result(op: str, r: subprocess.CompletedProcess, out: Path, expected) -> None:
    assert r.returncode == 0, f"bench_client {op}: kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    df = pl.read_parquet(out)
    assert json.loads(lines[0]) == {"rows": df.height}
    assert tuple(df.columns) == OUTPUT_COLUMNS[op]
    actual = row_multiset(op, df)
    assert actual == expected, f"{op}: rozjazd z polars-bio — {describe_diff(expected, actual)}"


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--op", "merge"],
        ["--op", "join", "--left", "x"],
        ["--op", "overlap", "--left", "x"],
        ["--op", "merge", "--left", "x", "--right", "y"],
        ["--op", "merge", "--left", "x", "--cols", "contig,pos_start"],
        ["--op", "merge", "--left", "x", "--cols", "contig,,pos_end"],
        ["--op", "merge", "--left", "x", "--lef", "y"],
        ["--op", "merge", "--left", "x", "--left", "y"],
        ["--op", "merge", "--left"],
    ],
)
def test_bench_client_rejects_bad_arguments(args):
    r = run_client(args, timeout=30)
    assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
    assert "użycie" in r.stderr
    assert r.stdout == ""


def test_missing_data_path_is_reported(tmp_path):
    missing = tmp_path / "nie_ma_takiego_katalogu"
    r = run_client(["--op", "merge", "--left", str(missing)])
    assert r.returncode == 1, r.stderr
    assert str(missing) in r.stderr
    assert r.stdout == ""


@pytest.mark.parametrize("op", OPS)
def test_bench_client_matches_polars_bio_on_parquet(op, parquet_dirs, parquet_expected, tmp_path):
    a, b = parquet_dirs
    out = tmp_path / f"{op}.parquet"
    check_result(op, run_client(op_args(op, a, b, out)), out, parquet_expected[op])


def test_bench_client_reads_csv_with_reserved_column_names(tmp_path):
    """Dotychczasowe dane CSV — kolumny chrom, start, end (`end` to słowo kluczowe SQL)."""
    from tests.merge_oracle import reference_merge_intervals
    from tests.test_ballista_distributed_ops import INTERVALS_A

    out = tmp_path / "merge.parquet"
    r = run_client(
        ["--op", "merge", "--left", "data/parts_a", "--cols", "chrom,start,end", "--output", str(out)]
    )
    assert r.returncode == 0, r.stderr
    df = pl.read_parquet(out)
    assert tuple(df.columns) == OUTPUT_COLUMNS["merge"]
    actual = {(c, int(s), int(e)) for c, s, e, _ in df.iter_rows()}
    assert actual == reference_merge_intervals(INTERVALS_A)


def test_bench_client_quotes_apostrophe_in_path(parquet_dirs, parquet_expected, tmp_path):
    a = shutil.copytree(parquet_dirs[0], tmp_path / "it's" / "a")
    out = tmp_path / "merge.parquet"
    r = run_client(["--op", "merge", "--left", str(a), "--output", str(out)])
    check_result("merge", r, out, parquet_expected["merge"])


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    c = _start_cluster(
        tmp_path_factory.mktemp("plan2_klaster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


@pytest.mark.parametrize("op", OPS)
def test_bench_client_on_multiprocess_cluster(op, cluster, parquet_dirs, parquet_expected, tmp_path):
    a, b = parquet_dirs
    out = tmp_path / f"{op}.parquet"
    env = {**os.environ, "BALLISTA_SCHEDULER_URL": cluster.url}
    check_result(op, run_client(op_args(op, a, b, out), env=env), out, parquet_expected[op])

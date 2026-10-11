"""Ścieżka Arrow w Sailu (plan 3b-2, Zadanie 9; specyfikacja 5.2): groupBy/cogroup(...).applyInArrow
z polars-bio w trybie „pula kontekstów” i „blokada” — wynik zgodny z polars-bio, plik faz."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

import sail_arrow
from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent
#: Chromosomy lewej strony zbioru testowego (tests/parquet_fixture.py): chr1, chr2, chr10, chrA —
#: tylu wywołań polars-bio oczekujemy (chrB jest tylko po prawej, więc grupa ma pustą lewą stronę).
LEFT_CHROMOSOMES = 4


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    port = free_port()
    log = tmp_path_factory.mktemp("sail_arrow") / "server.log"
    with log.open("w") as f:
        proc = subprocess.Popen(
            [sys.executable, "-m", "bench.runners.sail_server", "--port", str(port)],
            cwd=REPO, env={**os.environ, **sail_env(2)}, stdout=f, stderr=subprocess.STDOUT,
        )
    session = None
    try:
        wait_for_port(port, proc, name="sail_server", log=log)
        from pyspark.sql import SparkSession

        session = SparkSession.builder.remote(f"sc://127.0.0.1:{port}").create()
        yield session
    finally:
        if session is not None:
            session.stop()
        proc.terminate()
        proc.wait(timeout=30)


@pytest.mark.parametrize("mode", sail_arrow.MODES)
@pytest.mark.parametrize("op", OPS)
def test_arrow_path_matches_polars_bio(op, mode, spark, parquet_dirs, parquet_expected, tmp_path):
    left, right = parquet_dirs
    phases = tmp_path / "phases.jsonl"
    df = sail_arrow.build_query(
        spark, op, left, None if op in UNARY_OPS else right, mode=mode, phase_log=str(phases)
    )
    checksum = cs.Checksum(op)
    checksum.update(df.toArrow())
    expected = parquet_expected[op]
    assert checksum.rows == sum(expected.values())
    assert checksum.hex() == cs.format_checksum(cs.checksum_rows(op, expected.elements()))
    pb_s, wait_s, calls = sail_arrow.read_phases(phases)
    assert calls == LEFT_CHROMOSOMES and pb_s > 0
    assert wait_s == 0.0 if mode == "context" else wait_s >= 0.0


def test_empty_left_group_returns_empty_table_without_calling_polars_bio(tmp_path):
    empty = pa.table({"chrom": pa.array([], pa.string()), "start": pa.array([], pa.int64()),
                      "end": pa.array([], pa.int64())})
    phases = tmp_path / "phases.jsonl"
    for mode in sail_arrow.MODES:
        result = sail_arrow.evaluate("overlap", empty, empty, mode, str(phases))
        assert result.num_rows == 0 and result.schema == sail_arrow.arrow_schema("overlap")
    assert not phases.exists()


def test_read_phases_sums_records_and_tolerates_missing_or_empty_file(tmp_path):
    assert sail_arrow.read_phases(None) == (0.0, 0.0, 0)
    assert sail_arrow.read_phases(tmp_path / "missing.jsonl") == (0.0, 0.0, 0)
    path = tmp_path / "phases.jsonl"
    path.write_text("")
    assert sail_arrow.read_phases(path) == (0.0, 0.0, 0)
    path.write_text(json.dumps({"pb_s": 0.5, "wait_s": 0.25}) + "\n" + json.dumps({"pb_s": 1.0, "wait_s": 0.0}) + "\n")
    assert sail_arrow.read_phases(str(path)) == (1.5, 0.25, 2)


def test_schemas_follow_normalized_output_columns():
    assert sail_arrow.ddl_schema("merge") == "chrom string, start long, end long, n_intervals long"
    assert sail_arrow.arrow_schema("subtract") == pa.schema(
        [("chrom", pa.string()), ("start", pa.int64()), ("end", pa.int64())]
    )


@pytest.mark.parametrize(
    "op, right, mode, message",
    [
        ("join", "b", "context", "unknown operation"),
        ("overlap", "b", "fast", "unknown mode"),
        ("merge", "b", "context", "merge requires one table"),
        ("overlap", None, "context", "overlap requires two tables"),
    ],
)
def test_build_query_rejects_bad_arguments(op, right, mode, message):
    with pytest.raises(ValueError, match=message):
        sail_arrow.build_query(None, op, "a", right, mode=mode)

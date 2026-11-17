"""The Arrow path in Sail: groupBy/cogroup(...).applyInArrow with polars-bio in the
"context pool" mode; the result agrees with polars-bio, plus the phase file."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

from sail_genomics import arrow as sail_arrow
from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent
#: Left-side chromosomes of the test dataset (tests/parquet_fixture.py): chr1, chr2, chr10, chrA;
#: this many polars-bio calls are expected (chrB is only on the right, so its group has an empty left side).
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


@pytest.mark.parametrize("op", OPS)
def test_arrow_path_matches_polars_bio(op, spark, parquet_dirs, parquet_expected, tmp_path):
    left, right = parquet_dirs
    phases = tmp_path / "phases.jsonl"
    df = sail_arrow.build_query(
        spark, op, left, None if op in UNARY_OPS else right, phase_log=str(phases)
    )
    checksum = cs.Checksum(op)
    checksum.update(df.toArrow())
    expected = parquet_expected[op]
    assert checksum.rows == sum(expected.values())
    assert checksum.hex() == cs.format_checksum(cs.checksum_rows(op, expected.elements()))
    pb_s, calls = sail_arrow.read_phases(phases)
    assert calls == LEFT_CHROMOSOMES and pb_s > 0


def test_empty_left_group_returns_empty_table_without_calling_polars_bio(tmp_path):
    empty = pa.table({"chrom": pa.array([], pa.string()), "start": pa.array([], pa.int64()),
                      "end": pa.array([], pa.int64())})
    phases = tmp_path / "phases.jsonl"
    result = sail_arrow.evaluate("overlap", empty, empty, str(phases))
    assert result.num_rows == 0 and result.schema == sail_arrow.arrow_schema("overlap")
    assert not phases.exists()


def test_read_phases_sums_records_and_tolerates_missing_or_empty_file(tmp_path):
    assert sail_arrow.read_phases(None) == (0.0, 0)
    assert sail_arrow.read_phases(tmp_path / "missing.jsonl") == (0.0, 0)
    path = tmp_path / "phases.jsonl"
    path.write_text("")
    assert sail_arrow.read_phases(path) == (0.0, 0)
    path.write_text(json.dumps({"pb_s": 0.5}) + "\n" + json.dumps({"pb_s": 1.0}) + "\n")
    assert sail_arrow.read_phases(str(path)) == (1.5, 2)


def test_schemas_follow_normalized_output_columns():
    assert sail_arrow.ddl_schema("merge") == "chrom string, start long, end long, n_intervals long"
    assert sail_arrow.arrow_schema("subtract") == pa.schema(
        [("chrom", pa.string()), ("start", pa.int64()), ("end", pa.int64())]
    )


@pytest.mark.parametrize(
    "op, right, message",
    [
        ("join", "b", "unknown operation"),
        ("merge", "b", "merge requires one table"),
        ("overlap", None, "overlap requires two tables"),
    ],
)
def test_build_query_rejects_bad_arguments(op, right, message):
    with pytest.raises(ValueError, match=message):
        sail_arrow.build_query(None, op, "a", right)


def test_package_schema_matches_bench_schema():
    from bench import ops
    import sail_genomics
    assert sail_genomics.OUTPUT_COLUMNS == ops.OUTPUT_COLUMNS
    assert set(sail_genomics.UNARY_OPS) == set(ops.UNARY_OPS)
    assert tuple(sail_genomics.OPS) == tuple(ops.OPS)


def test_package_does_not_import_bench():
    import ast

    import sail_genomics
    # The installed package, wherever it lives (sail_genomics/ here, the engines/sail-genomics
    # submodule in the benchmark repository), independent of the working directory.
    paths = sorted(Path(sail_genomics.__file__).parent.glob("*.py"))
    assert paths, sail_genomics.__file__
    for path in paths:
        tree = ast.parse(path.read_text())
        names = [n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)] + \
                [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        assert not any((m or "").split(".")[0] == "bench" for m in names), path

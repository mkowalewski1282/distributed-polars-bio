"""The applyInArrow path in Sail: groupBy/cogroup(...).applyInArrow with polars-bio from the
context pool. Results agree with polars-bio (tests/generic_oracle.py), the phase file records
the polars-bio calls, and bad arguments are rejected before any query is built."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from sail_genomics import arrow
from sail_genomics.schema import OPS, OUTPUT_COLUMNS, UNARY_OPS
from tests.rows import row_multiset

#: Left-side chromosomes of the test dataset (tests/parquet_fixture.py): chr1, chr2, chr10, chrA;
#: this many polars-bio calls are expected (chrB is only on the right, so its group has an empty
#: left side and polars-bio is not called).
LEFT_CHROMOSOMES = 4


@pytest.mark.parametrize("op", OPS)
def test_arrow_path_matches_polars_bio(op, spark, parquet_dirs, parquet_expected, tmp_path):
    left, right = parquet_dirs
    phases = tmp_path / "phases.jsonl"
    df = arrow.build_query(
        spark, op, left, None if op in UNARY_OPS else right, phase_log=str(phases)
    )
    result = df.toArrow()
    assert tuple(result.column_names) == OUTPUT_COLUMNS[op]
    assert row_multiset(op, result) == parquet_expected[op]
    pb_s, calls = arrow.read_phases(phases)
    assert calls == LEFT_CHROMOSOMES and pb_s > 0


def test_empty_left_group_returns_empty_table_without_calling_polars_bio(tmp_path):
    empty = pa.table({"chrom": pa.array([], pa.string()), "start": pa.array([], pa.int64()),
                      "end": pa.array([], pa.int64())})
    phases = tmp_path / "phases.jsonl"
    result = arrow.evaluate("overlap", empty, empty, str(phases))
    assert result.num_rows == 0 and result.schema == arrow.arrow_schema("overlap")
    assert not phases.exists()


def test_read_phases_sums_records_and_tolerates_missing_or_empty_file(tmp_path):
    assert arrow.read_phases(None) == (0.0, 0)
    assert arrow.read_phases(tmp_path / "missing.jsonl") == (0.0, 0)
    path = tmp_path / "phases.jsonl"
    path.write_text("")
    assert arrow.read_phases(path) == (0.0, 0)
    path.write_text(json.dumps({"pb_s": 0.5}) + "\n" + json.dumps({"pb_s": 1.0}) + "\n")
    assert arrow.read_phases(str(path)) == (1.5, 2)


def test_schemas_follow_normalized_output_columns():
    assert arrow.ddl_schema("merge") == "chrom string, start long, end long, n_intervals long"
    assert arrow.arrow_schema("subtract") == pa.schema(
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
        arrow.build_query(None, op, "a", right)

"""Wspólny schemat wyników i porównanie niezależne od kolejności (plan 2, Zadanie 3)."""

from __future__ import annotations

from collections import Counter

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from bench import ops

DATABIO = ("contig", "pos_start", "pos_end")


def test_output_columns_follow_spec_8_4():
    assert ops.OUTPUT_COLUMNS == {
        "overlap": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"),
        "nearest": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2", "distance"),
        "coverage": ("chrom", "start", "end", "coverage"),
        "merge": ("chrom", "start", "end", "n_intervals"),
        "subtract": ("chrom", "start", "end"),
    }
    # nearest bez tożsamości sąsiada — przy remisach silniki wybierają różnie.
    assert ops.KEY_COLUMNS["nearest"] == ("chrom_1", "start_1", "end_1", "distance")
    assert set(ops.OPS) == set(ops.OUTPUT_COLUMNS) and ops.UNARY_OPS == {"merge"}


def test_polars_bio_names_for_databio_columns():
    assert ops.polars_bio_names("overlap", DATABIO) == {
        "contig_1": "chrom_1", "pos_start_1": "start_1", "pos_end_1": "end_1",
        "contig_2": "chrom_2", "pos_start_2": "start_2", "pos_end_2": "end_2",
    }
    assert ops.polars_bio_names("coverage", DATABIO) == {
        "contig": "chrom", "pos_start": "start", "pos_end": "end", "coverage": "coverage",
    }


def test_normalize_polars_bio_renames_and_orders():
    df = pl.DataFrame({"n_intervals": [2], "pos_end": [300], "contig": ["chr1"], "pos_start": [100]})
    out = ops.normalize_polars_bio("merge", df, DATABIO)
    assert out.columns == ["chrom", "start", "end", "n_intervals"]
    assert out.row(0) == ("chr1", 100, 300, 2)


def test_normalize_polars_bio_reports_missing_columns():
    df = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [2]})
    with pytest.raises(KeyError, match="pos_start"):
        ops.normalize_polars_bio("subtract", df, DATABIO)


def test_row_multiset_ignores_order_and_integer_width():
    a = pl.DataFrame(
        {"chrom": ["chr1", "chr2"], "start": [1, 5], "end": [3, 9]},
        schema={"chrom": pl.Utf8, "start": pl.Int32, "end": pl.Int32},
    )
    b = pl.DataFrame(
        {"chrom": ["chr2", "chr1"], "start": [5, 1], "end": [9, 3]},
        schema={"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64},
    )
    assert ops.row_multiset("subtract", a) == ops.row_multiset("subtract", b)


def test_row_multiset_counts_duplicates():
    one = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [3]})
    two = pl.concat([one, one])
    assert ops.row_multiset("subtract", two) == Counter({("chr1", 1, 3): 2})
    assert ops.row_multiset("subtract", one) != ops.row_multiset("subtract", two)


def test_row_multiset_treats_pandas_nan_as_null():
    pdf = pd.DataFrame({
        "chrom_1": ["chrA"], "start_1": [10], "end_1": [20],
        "chrom_2": [None], "start_2": [float("nan")], "end_2": [float("nan")],
        "distance": [float("nan")],
    })
    pldf = pl.DataFrame(
        {"chrom_1": ["chrA"], "start_1": [10], "end_1": [20],
         "chrom_2": [None], "start_2": [None], "end_2": [None], "distance": [None]},
        schema={"chrom_1": pl.Utf8, "start_1": pl.Int32, "end_1": pl.Int32,
                "chrom_2": pl.Utf8, "start_2": pl.Int32, "end_2": pl.Int32,
                "distance": pl.Int64},
    )
    expected = Counter({("chrA", 10, 20, None): 1})
    assert ops.row_multiset("nearest", pdf) == expected
    assert ops.row_multiset("nearest", pldf) == expected


def test_nearest_key_ignores_neighbor_identity():
    base = {"chrom_1": ["chr1"], "start_1": [100], "end_1": [200],
            "chrom_2": ["chr1"], "end_2": [260], "distance": [0]}
    a = pl.DataFrame({**base, "start_2": [180]})
    b = pl.DataFrame({**base, "start_2": [150]})
    assert ops.row_multiset("nearest", a) == ops.row_multiset("nearest", b)


def test_row_multiset_reports_missing_key_column():
    with pytest.raises(KeyError, match="coverage"):
        ops.row_multiset("coverage", pl.DataFrame({"chrom": ["c"], "start": [1], "end": [2]}))


def test_describe_diff_shows_counts_and_examples():
    text = ops.describe_diff(Counter({("a",): 2, ("b",): 1}), Counter({("a",): 1, ("c",): 1}))
    assert "missing 2" in text and "extra 1" in text and "('c',)" in text


def test_normalize_arrow_renames_and_orders_batch():
    batch = pa.record_batch({
        "n_intervals": pa.array([2], pa.int64()),
        "pos_end": pa.array([300], pa.int32()),
        "contig": pa.array(["chr1"], pa.string_view()),
        "pos_start": pa.array([100], pa.int32()),
    })
    out = ops.normalize_arrow("merge", batch, DATABIO)
    assert isinstance(out, pa.RecordBatch)
    assert out.schema.names == ["chrom", "start", "end", "n_intervals"]
    assert out.to_pylist() == [{"chrom": "chr1", "start": 100, "end": 300, "n_intervals": 2}]


def test_normalize_arrow_accepts_table_and_reports_missing_columns():
    table = pa.table({"contig": ["chr1"], "pos_start": [1], "pos_end": [2]})
    assert ops.normalize_arrow("subtract", table, DATABIO).column_names == ["chrom", "start", "end"]
    with pytest.raises(KeyError, match="n_intervals"):
        ops.normalize_arrow("merge", table, DATABIO)

"""Suma kontrolna niezależna od kolejności wierszy (plan 3a, Zadanie 1; specyfikacja 8.4)."""

from __future__ import annotations

import random

import polars as pl
import pyarrow as pa
import pytest

from bench import checksum as cs
from bench.ops import OPS
from tests.bench import checksum_vectors as vec


@pytest.mark.parametrize("op", OPS)
def test_definition_gives_golden_values(op):
    assert cs.format_checksum(cs.checksum_rows(op, vec.key_rows(op))) == vec.CHECKSUMS[op]


@pytest.mark.parametrize("op", OPS)
def test_streaming_accumulator_gives_golden_values(op):
    acc = cs.Checksum(op)
    acc.update(vec.table(op))
    assert (acc.hex(), acc.rows) == (vec.CHECKSUMS[op], len(vec.ROWS[op]))


def test_empty_result_has_zero_checksum():
    acc = cs.Checksum("merge")
    acc.update(vec.table("merge").slice(0, 0))
    assert (acc.hex(), acc.rows) == ("0x0000000000000000", 0)


@pytest.mark.parametrize("op", OPS)
def test_order_and_batching_do_not_matter(op):
    t = vec.table(op)
    reversed_rows = t.take(list(reversed(range(t.num_rows))))
    acc = cs.Checksum(op)
    for batch in reversed_rows.to_batches(max_chunksize=1):
        acc.update(batch)
    assert acc.hex() == vec.CHECKSUMS[op]


def test_detects_values_swapped_between_rows():
    """Suma liniowa zależałaby tylko od sum kolumn — zamiana końców między wierszami
    (te same sumy kolumn) musi zmienić wynik."""
    rows = [("chr1", 100, 200, "chr1", 150, 300), ("chr1", 400, 500, "chr1", 450, 600)]
    swapped = [("chr1", 100, 500, "chr1", 150, 300), ("chr1", 400, 200, "chr1", 450, 600)]
    assert cs.checksum_rows("overlap", rows) != cs.checksum_rows("overlap", swapped)


def test_detects_missing_duplicate():
    rows = vec.key_rows("subtract")
    assert cs.checksum_rows("subtract", rows) != cs.checksum_rows("subtract", rows[1:])


def test_nearest_ignores_neighbour_identity_but_not_distance():
    t = vec.table("nearest")
    other_neighbour = t.set_column(
        t.schema.get_field_index("start_2"), "start_2", pa.array([1, 2, 3], pa.int32())
    )
    longer = t.set_column(
        t.schema.get_field_index("distance"), "distance", pa.array([101, None, 60], pa.int64())
    )
    for variant, same in [(other_neighbour, True), (longer, False)]:
        acc = cs.Checksum("nearest")
        acc.update(variant)
        assert (acc.hex() == vec.CHECKSUMS["nearest"]) is same


def test_missing_value_differs_from_zero():
    assert cs.checksum_rows("nearest", [("chrA", 10, 20, None)]) != cs.checksum_rows(
        "nearest", [("chrA", 10, 20, 0)]
    )


def test_accumulator_matches_definition_on_mixed_types():
    """string_view (tak DataFusion 53 czyta napisy z Parquet), int32/int64, braki wartości
    i tabela z kilku kawałków — wynik jak z definicji liczonej wiersz po wierszu."""
    rng = random.Random(7)
    rows = [
        (rng.choice(["chr1", "chr2", "chrX", "chr10", None]), rng.randrange(2**31),
         rng.randrange(2**31), rng.choice([None, rng.randrange(10**6)]))
        for _ in range(5000)
    ]
    t = pa.table({
        "chrom": pa.array([r[0] for r in rows], pa.string_view()),
        "start": pa.array([r[1] for r in rows], pa.int32()),
        "end": pa.array([r[2] for r in rows], pa.int64()),
        "coverage": pa.array([r[3] for r in rows], pa.int64()),
    })
    acc = cs.Checksum("coverage")
    acc.update(pa.concat_tables([t.slice(0, 1234), t.slice(1234)]))
    assert acc.hex() == cs.format_checksum(cs.checksum_rows("coverage", rows))


def test_accepts_polars_dataframe():
    acc = cs.Checksum("merge")
    acc.update(pl.from_arrow(vec.table("merge")))
    assert acc.hex() == vec.CHECKSUMS["merge"]


def test_missing_key_column_is_reported():
    acc = cs.Checksum("merge")
    with pytest.raises(KeyError, match="n_intervals"):
        acc.update(vec.table("subtract"))


def test_unknown_operation_is_rejected():
    with pytest.raises(ValueError, match="unknown operation"):
        cs.Checksum("join")

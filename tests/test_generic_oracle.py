"""Wyrocznia polars-bio na plikach Parquet (plan 2, Zadanie 3): schemat
znormalizowany i semantyka chromosomów obecnych tylko po jednej stronie —
wzorzec, który muszą odtworzyć Ballista i Sail.

Uruchomienie: pytest tests/test_generic_oracle.py -v (import polars-bio trwa kilka minut)
"""

from __future__ import annotations

import pytest

from bench.ops import OPS, OUTPUT_COLUMNS, UNARY_OPS


@pytest.fixture(scope="module")
def oracle_frames(parquet_dirs):
    from tests.generic_oracle import read_intervals, reference

    a, b = (read_intervals(d) for d in parquet_dirs)
    return {op: reference(op, a, None if op in UNARY_OPS else b) for op in OPS}


def test_fixture_is_split_across_files(parquet_dirs):
    for d in parquet_dirs:
        assert len(list(d.glob("part-*.parquet"))) == 2


@pytest.mark.parametrize("op", OPS)
def test_oracle_returns_normalized_schema(op, oracle_frames):
    assert tuple(oracle_frames[op].columns) == OUTPUT_COLUMNS[op]
    assert oracle_frames[op].height > 0


def test_oracle_one_sided_chromosomes(oracle_frames):
    rows = {op: set(oracle_frames[op].iter_rows()) for op in OPS}
    assert ("chrA", 10, 20, None, None, None, None) in rows["nearest"]
    assert ("chrA", 10, 20, 0) in rows["coverage"]
    assert ("chrA", 10, 20) in rows["subtract"]
    assert not any("chrA" in r or "chrB" in r for r in rows["overlap"])
    assert not any(r[0] == "chrB" for op in OPS if op != "overlap" for r in rows[op])

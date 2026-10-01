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


def test_oracle_nearest_has_nonzero_distances(oracle_frames):
    """Zbiór testowy musi zawierać przedziały z odstępem do sąsiada — gdy wszystkie
    odległości wynosiły 0, test nearest sprawdzał tylko orientację i liczbę wierszy
    (tak przypadkiem przechodził test P0). 0-based, półotwarte: odległość = odstęp."""
    rows = {(r[0], r[1], r[2], r[6]) for r in oracle_frames["nearest"].iter_rows()}
    assert ("chr1", 700, 800, 100) in rows  # najbliższy po lewej: [450, 600)
    assert ("chr2", 10, 40, 60) in rows  # najbliższy po prawej: [100, 220)
    assert ("chr1", 260, 280, 10) in rows  # remis: [180, 250) i [290, 420)

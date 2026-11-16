"""polars-bio oracle on Parquet files: the normalized schema and the semantics of
chromosomes present on one side only, a pattern that Ballista and Sail have to reproduce.

Run: pytest tests/test_generic_oracle.py -v
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
    """The test dataset must contain intervals with a gap to the neighbour: when all
    distances were 0, the nearest test checked only the orientation and the number of rows
    (so tests passed by accident). 0-based, half-open: distance = gap."""
    rows = {(r[0], r[1], r[2], r[6]) for r in oracle_frames["nearest"].iter_rows()}
    assert ("chr1", 700, 800, 100) in rows  # nearest on the left: [450, 600)
    assert ("chr2", 10, 40, 60) in rows  # nearest on the right: [100, 220)
    assert ("chr1", 260, 280, 10) in rows  # tie: [180, 250) and [290, 420)

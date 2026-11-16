"""
Shared golden oracle and data generator for the correctness tests of the
overlap operation on Ballista and Sail.

It neither imports nor depends on Ballista/Sail, only on a local polars-bio.
Engine-specific tests should import `generate_intervals`,
`reference_overlap_pairs` and `EDGE_CASES` from here.

NOTE: this is not yet a full property-based harness (the `hypothesis` library
is not installed in the environment); the random generator below is a
hand-written equivalent built on `random`, so as not to add a new dependency
unnecessarily. Many random seeds give a similar effect.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

import pandas as pd
import polars as pl
import polars_bio as pb

SCHEMA = ["chrom", "start", "end", "name"]


@dataclass(frozen=True)
class IntervalSet:
    chrom: str
    start: int
    end: int
    name: str


def _reset_pb_context() -> None:
    """polars-bio registers tables 's1'/'s2' in the global DataFusion context;
    they have to be cleared between successive calls in the same process."""
    from polars_bio.context import ctx as _pb_ctx
    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def reference_overlap_pairs(
    intervals_a: list[tuple[str, int, int, str]],
    intervals_b: list[tuple[str, int, int, str]],
) -> set[tuple[str, str]]:
    """Golden oracle: local pb.overlap(), normalized to a set of (name_a, name_b) pairs.

    Normalized means independent of row order and of types.
    """
    if not intervals_a or not intervals_b:
        return set()

    df_a = pl.DataFrame(intervals_a, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(intervals_b, schema=SCHEMA, orient="row")
    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)

    _reset_pb_context()
    result = pb.overlap(df_a, df_b).collect()

    if result is None or result.height == 0:
        return set()

    return set(zip(result["name_1"].to_list(), result["name_2"].to_list()))


def normalize_engine_pairs(df: pd.DataFrame, name_a_col: str, name_b_col: str) -> set[tuple[str, str]]:
    """Normalizes a distributed-engine result (Sail/Ballista) to the same form as
    reference_overlap_pairs: a set of (name_a, name_b) pairs, independent of order."""
    if df is None or len(df) == 0:
        return set()
    return set(zip(df[name_a_col].astype(str).tolist(), df[name_b_col].astype(str).tolist()))


def generate_random_intervals(
    seed: int,
    n_a: int = 20,
    n_b: int = 20,
    n_chroms: int = 3,
    coord_max: int = 1000,
    max_len: int = 100,
) -> tuple[list[tuple[str, int, int, str]], list[tuple[str, int, int, str]]]:
    """Random intervals for property-based tests (many seeds in a test loop)."""
    rng = random.Random(seed)
    chroms = [f"chr{i + 1}" for i in range(n_chroms)]

    def make(n: int, prefix: str) -> list[tuple[str, int, int, str]]:
        rows = []
        for i in range(n):
            chrom = rng.choice(chroms)
            start = rng.randint(0, coord_max)
            length = rng.randint(0, max_len)  # length=0 -> start==end, an edge case
            end = start + length
            rows.append((chrom, start, end, f"{prefix}{i}"))
        return rows

    return make(n_a, "a"), make(n_b, "b")


# ---------------------------------------------------------------------------
# Edge cases: start==end, touching intervals, duplicates, empty partitions,
# negative coordinates
# ---------------------------------------------------------------------------

EDGE_CASES: dict[str, tuple[list[tuple[str, int, int, str]], list[tuple[str, int, int, str]]]] = {
    "touching_not_overlapping": (
        [("chr1", 100, 200, "a1")],
        [("chr1", 200, 300, "b1")],  # a.end == b.start -> half-open: does NOT overlap
    ),
    "zero_length_interval": (
        [("chr1", 150, 150, "a1")],  # start == end
        [("chr1", 100, 200, "b1")],
    ),
    "duplicate_intervals": (
        [("chr1", 100, 200, "a1"), ("chr1", 100, 200, "a1")],
        [("chr1", 150, 250, "b1")],
    ),
    "empty_a": ([], [("chr1", 100, 200, "b1")]),
    "empty_b": ([("chr1", 100, 200, "a1")], []),
    "no_matching_chrom": (
        [("chr1", 100, 200, "a1")],
        [("chr2", 100, 200, "b1")],
    ),
    "single_base_overlap": (
        [("chr1", 100, 200, "a1")],
        [("chr1", 199, 300, "b1")],  # they overlap on [199,200)
    ),
}

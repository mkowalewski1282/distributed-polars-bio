"""
Correctness tests of operations executed FULLY DISTRIBUTED in Ballista
(the `dist_ops` binary), compared with the oracle, a local polars-bio.

Difference from tests/test_*_correctness.py: those tests check operations
computed LOCALLY (a plain DFSessionContext, the `*_local` binaries). Here every
operation goes through a real Ballista cluster (scheduler + executor), with
serialization of both the logical AND the physical plan.

The oracles are REUSED unchanged from the local tests, so we compare against
exactly the same reference point.

Complement: tests/test_ballista_distribution_evidence.py checks that the result
was actually produced in a distributed way (assertions on EXPLAIN ANALYZE), not
just that it is correct.

Run: pytest tests/test_ballista_distributed_ops.py -v
NOTE: importing polars_bio takes ~4.5 min; do not interrupt it prematurely.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest

from tests.coverage_subtract_oracle import reference_coverage, reference_subtract
from tests.merge_oracle import reference_merge_intervals
from tests.nearest_oracle import reference_nearest_min_distances

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
DIST_BINARY = BALLISTA_DIR / "target" / "debug" / "dist_ops"
OUTPUT_DIR = BALLISTA_DIR / "output"
DATA_DIR = BALLISTA_DIR / "data"

# Canonical dataset, shared by ALL tests and both engines.
INTERVALS_A = [
    ("chr1", 100, 200, "gene_A1"),
    ("chr1", 150, 300, "gene_A2"),
    ("chr1", 400, 500, "gene_A3"),
    ("chr2", 50, 150, "gene_A4"),
    ("chr2", 200, 350, "gene_A5"),
]
INTERVALS_B = [
    ("chr1", 180, 250, "peak_B1"),
    ("chr1", 290, 420, "peak_B2"),
    ("chr1", 450, 600, "peak_B3"),
    ("chr2", 100, 220, "peak_B4"),
    ("chr2", 300, 400, "peak_B5"),
]

pytestmark = pytest.mark.skipif(
    not DIST_BINARY.exists(),
    reason=(
        "dist_ops is not built - run `cd ballista_genomics && "
        "CARGO_BUILD_JOBS=1 cargo build --bin dist_ops` (CARGO_BUILD_JOBS=1 is "
        "required on this machine, see OPIS.md)"
    ),
)


def _run_dist(op: str, timeout: int = 180) -> None:
    result = subprocess.run(
        [str(DIST_BINARY), op],
        cwd=BALLISTA_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert result.returncode == 0, (
        f"dist_ops {op} failed:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def _read_parts(subdir: str) -> set[tuple[str, int, int, str]]:
    rows: set[tuple[str, int, int, str]] = set()
    for csv in sorted((DATA_DIR / subdir).glob("*.csv")):
        df = pd.read_csv(csv)
        rows |= {
            (r.chrom, int(r.start), int(r.end), r["name"])
            for _, r in df.iterrows()
        }
    return rows


def test_part_files_union_equals_canonical_intervals():
    """
    Guard against a silent divergence of the data.

    The distributed variant reads from the data/parts_a and data/parts_b directories
    (splitting into multiple files is NECESSARY so that the source stage has >1
    partitions; without it Ballista may not insert a shuffle at all). The oracle,
    in contrast, computes on the INTERVALS_A/INTERVALS_B lists embedded in Python.

    If these two sets diverged, the correctness tests would compare the distributed
    result with an oracle computed on DIFFERENT data, and could pass or fail for a
    completely misleading reason.
    """
    assert _read_parts("parts_a") == set(INTERVALS_A), "data/parts_a != INTERVALS_A"
    assert _read_parts("parts_b") == set(INTERVALS_B), "data/parts_b != INTERVALS_B"


def test_ballista_distributed_merge_matches_oracle():
    """
    Fully distributed merge: hash shuffle by chromosome + MergeExec on the
    executor, with the result compared with the local pb.merge().

    This test is also a DISTRIBUTION TEST, not only a correctness test:
    the overlapping gene_A1=[100,200) and gene_A2=[150,300) lie in DIFFERENT
    input files (a_part1.csv / a_part2.csv), so they land in different source
    partitions. The correct result [100,300) can arise ONLY if the hash shuffle
    by `chrom` actually moved them into one partition. If distribution stopped
    working, we would get two separate intervals instead of one.
    """
    _run_dist("merge")
    out = OUTPUT_DIR / "dist_merge_result.csv"
    assert out.exists(), f"not found: {out}"

    df = pd.read_csv(out)
    actual = {(r.chrom, int(r.start), int(r.end)) for _, r in df.iterrows()}
    expected = reference_merge_intervals(INTERVALS_A)

    assert actual == expected, (
        f"Difference from the oracle pb.merge().\n"
        f"Only in pb.merge():        {expected - actual}\n"
        f"Only in distributed merge: {actual - expected}"
    )
    assert ("chr1", 100, 300) in actual, (
        "No merged [100,300) - intervals from DIFFERENT input files were not "
        "joined, which means the hash shuffle by chrom did not work."
    )


def test_ballista_distributed_subtract_matches_oracle():
    """
    Fully distributed subtract: a TWO-SIDED hash shuffle by chromosome.

    This is the strongest demonstration of distribution in this suite: SubtractExec
    is a binary node, and `execute(partition)` reaches for the SAME partition on the
    left and on the right. A correct result therefore requires both sides to be
    co-partitioned by `chrom` into the same number of partitions. Both tables are
    split into 2 files each, so both have a parallel source stage.
    """
    _run_dist("subtract")
    out = OUTPUT_DIR / "dist_subtract_result.csv"
    assert out.exists(), f"not found: {out}"

    df = pd.read_csv(out)
    actual = {(r.chrom, int(r.start), int(r.end)) for _, r in df.iterrows()}
    expected = reference_subtract(INTERVALS_A, INTERVALS_B)

    assert actual == expected, (
        f"Difference from the oracle pb.subtract().\n"
        f"Only in pb.subtract():         {expected - actual}\n"
        f"Only in distributed subtract: {actual - expected}"
    )


def test_ballista_distributed_nearest_matches_oracle_distances():
    """
    Fully distributed nearest, BROADCAST pattern: the left (indexed) table
    travels whole in the physical-plan payload (Arrow IPC), and parallelism
    comes from partitioning the right side.

    We compare DISTANCES, not specific pairs. This is a known, documented
    finding: the native nearest() from bio-function-ranges and pb.nearest()
    break ties differently (when several candidates have the same, zero,
    distance). Both answers are correct as far as the distance goes.
    See tests/test_nearest_correctness.py.
    """
    _run_dist("nearest")
    out = OUTPUT_DIR / "dist_nearest_result.csv"
    assert out.exists(), f"not found: {out}"

    df = pd.read_csv(out)
    # Provider convention: the result has a row for each row of the RIGHT (queried) table, with
    # the nearest neighbour from the left (indexed) one. To match pb.nearest(A, B), A is the
    # right table: the names from A are in `right_name`, the chosen neighbours from B in `left_name`.
    assert len(df) == len(INTERVALS_A), "nearest must return one row per interval of A"
    actual = dict(zip(df["right_name"].tolist(), df["distance"].tolist()))
    expected = reference_nearest_min_distances(INTERVALS_A, INTERVALS_B)

    assert set(actual) == set(expected), (
        f"Different set of left-table intervals.\n"
        f"Only in pb.nearest(): {set(expected) - set(actual)}\n"
        f"Only distributed:     {set(actual) - set(expected)}"
    )
    for name, dist in expected.items():
        assert int(actual[name]) == int(dist), (
            f"{name}: pb.nearest() gave distance {dist}, "
            f"distributed nearest {actual[name]}"
        )


def test_distributed_nearest_agrees_with_local_nearest():
    """
    A STRICTER condition than distance agreement: the distributed nearest must pick
    EXACTLY the same partners as the local `nearest_local`: the same engine, the
    same build_nearest_indexes function, the same left_batch (only after a round
    trip through Arrow IPC).

    This is the actual broadcast test: if only PART of the left table reached the
    executor, the distances could come out inflated and the partner choice
    different, and comparing just the distances with pb would not catch that,
    because pb computes on the full data on its side.
    """
    # The local result is computed here rather than taken from another test's file: in CI the
    # test order skipped that test.
    local_binary = BALLISTA_DIR / "target" / "debug" / "nearest_local"
    assert local_binary.exists(), "build it: cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins"
    local = subprocess.run([str(local_binary)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=60)
    assert local.returncode == 0, local.stderr[-3000:]
    local_csv = OUTPUT_DIR / "nearest_local_result.csv"
    _run_dist("nearest")

    def pairs(path: Path) -> set[tuple[str, str]]:
        d = pd.read_csv(path)
        return set(zip(d["right_name"].tolist(), d["left_name"].tolist()))

    dist_pairs = pairs(OUTPUT_DIR / "dist_nearest_result.csv")
    local_pairs = pairs(local_csv)

    assert dist_pairs == local_pairs, (
        f"Distributed and local nearest chose DIFFERENT partners - a sign "
        f"that the left-table broadcast was incomplete or the row order in "
        f"left_batch diverged.\n"
        f"Only local:        {local_pairs - dist_pairs}\n"
        f"Only distributed: {dist_pairs - local_pairs}"
    )


def test_ballista_distributed_coverage_matches_oracle():
    """
    Fully distributed coverage: the only operation that needs its OWN carrier
    node (DistCoverageExec), because CountOverlapsProvider::scan() loses the left
    table's data, its column names and the coverage flag (see coverage_node.rs).

    Mind the REVERSED ARGUMENT CONVENTION: pb.coverage(a, b) reports the coverage
    of intervals `a` by `b`, while the SQL coverage('reads','targets') does the
    opposite, so dist_coverage is called with intervals_b as 'reads' and
    intervals_a as 'targets'.
    """
    _run_dist("coverage")
    out = OUTPUT_DIR / "dist_coverage_result.csv"
    assert out.exists(), f"not found: {out}"

    df = pd.read_csv(out)
    actual = {
        (r.chrom, int(r.start), int(r.end), int(r.coverage)) for _, r in df.iterrows()
    }
    expected = reference_coverage(INTERVALS_A, INTERVALS_B)

    assert actual == expected, (
        f"Difference from the oracle pb.coverage().\n"
        f"Only in pb.coverage():         {expected - actual}\n"
        f"Only in distributed coverage: {actual - expected}"
    )

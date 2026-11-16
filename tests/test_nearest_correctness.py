"""
Formal correctness tests for the nearest operation on Ballista (local).

Run: pytest tests/test_nearest_correctness.py -v
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest

from tests.nearest_oracle import reference_nearest_min_distances, reference_nearest_pairs

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

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
NEAREST_BINARY = BALLISTA_DIR / "target" / "debug" / "nearest_local"
NEAREST_OUTPUT_CSV = BALLISTA_DIR / "output" / "nearest_local_result.csv"


@pytest.mark.skipif(
    not NEAREST_BINARY.exists(),
    reason="nearest_local is not built - cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin nearest_local",
)
def test_ballista_local_nearest_matches_oracle():
    """
    Compares DISTANCES (name_a -> distance), not the chosen partners:
    the tie-breaking rules (several candidates at the same distance)
    are not documented for either engine, so a tie-robust comparison
    is safer. Row count = number of intervals in A, which
    guards the orientation (one row per interval of A, like pb.nearest).
    """
    result = subprocess.run(
        [str(NEAREST_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert NEAREST_OUTPUT_CSV.exists()

    # Provider convention: the result has a row for each row of the RIGHT (queried) table, with
    # the nearest neighbour from the left (indexed) one. To match pb.nearest(A, B), A is the
    # right table: the names from A are in `right_name`, the chosen neighbours from B in `left_name`.
    df = pd.read_csv(NEAREST_OUTPUT_CSV)
    assert len(df) == len(INTERVALS_A), "nearest must return one row per interval of A"
    actual_distances = dict(zip(df["right_name"].tolist(), df["distance"].tolist()))
    expected_distances = reference_nearest_min_distances(INTERVALS_A, INTERVALS_B)

    assert actual_distances == expected_distances, (
        f"Difference in distances.\n"
        f"pb.nearest():  {expected_distances}\n"
        f"Ballista:      {actual_distances}"
    )


def test_ballista_and_pb_nearest_pick_same_neighbours():
    """
    An earlier version recorded the finding "on a tie pb.nearest() picks B1, Ballista
    picks B2", and the test required that difference. That turned out to be an artefact of
    a reversed orientation: nearest() from datafusion-bio-function-ranges returns a row
    for each row of the RIGHT table, so calling nearest(A, B) computed "the nearest A for
    each B", not what pb.nearest(A, B) does. With the correct orientation (A as the right
    table) both engines pick EXACTLY the same neighbours on this data, also on the tie of
    A2 (B1 and B2 at distance 0). If this test starts failing, the engines' tie rules
    have diverged; the comparisons remain correct anyway (distances).
    """
    result = subprocess.run(
        [str(NEAREST_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        pytest.skip("nearest_local binary failed to run")

    df = pd.read_csv(NEAREST_OUTPUT_CSV)
    ballista_pairs = set(zip(df["right_name"].tolist(), df["left_name"].tolist()))
    pb_pairs = reference_nearest_pairs(INTERVALS_A, INTERVALS_B)

    assert ballista_pairs == pb_pairs, (
        "Ballista and pb.nearest() chose different neighbours (different tie rules) - "
        f"Ballista: {sorted(ballista_pairs)}, pb: {sorted(pb_pairs)}"
    )

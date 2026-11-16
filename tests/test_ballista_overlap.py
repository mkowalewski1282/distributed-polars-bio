"""
Formal correctness test for Ballista. It runs the compiled `ballista_genomics`
binary (a real Ballista cluster: scheduler + executor, LogicalExtensionCodec,
a real shuffle; see ballista_genomics/OPIS.md), reads its result from CSV and
compares it with the same oracle as the Sail test (tests/overlap_oracle.py).

Requires a built binary: `cargo build` in ballista_genomics/ (skipped if the
binary is missing, see `pytest.mark.skipif` below, because the build takes long
on this machine; see OPIS.md on the memory limits).

Run: pytest tests/test_ballista_overlap.py -v
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest

from tests.overlap_oracle import normalize_engine_pairs, reference_overlap_pairs

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
BINARY = BALLISTA_DIR / "target" / "debug" / "ballista_genomics"
OUTPUT_CSV = BALLISTA_DIR / "output" / "dist_overlap_result.csv"

# The test data must be identical to the data embedded in main.rs (DATA_LEFT_CSV/
# DATA_RIGHT_CSV point to ballista_genomics/data/intervals_a.csv and intervals_b.csv);
# it is the same set used in all the tests, so that results are directly
# comparable between engines.
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


@pytest.mark.skipif(
    not BINARY.exists(),
    reason=(
        "ballista_genomics is not built - run "
        "`cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build` (see OPIS.md "
        "on the memory limits of this machine: use CARGO_BUILD_JOBS=1)"
    ),
)
def test_ballista_distributed_overlap_matches_oracle():
    """
    Runs a real Ballista cluster (main.rs: scheduler + executor,
    LogicalExtensionCodec, shuffle) on the test data and checks that the result
    of dist_overlap() is identical to the local pb.overlap().
    """
    result = subprocess.run(
        [str(BINARY)],
        cwd=BALLISTA_DIR,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, (
        f"the ballista_genomics binary failed:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )

    assert OUTPUT_CSV.exists(), f"not found: {OUTPUT_CSV} - the binary did not write the result"

    df = pd.read_csv(OUTPUT_CSV)

    expected_pairs = reference_overlap_pairs(INTERVALS_A, INTERVALS_B)
    actual_pairs = normalize_engine_pairs(df, "name_a", "name_b")

    assert actual_pairs == expected_pairs, (
        f"Difference from the oracle.\n"
        f"Only in pb.overlap(): {expected_pairs - actual_pairs}\n"
        f"Only in Ballista:     {actual_pairs - expected_pairs}"
    )
    assert len(actual_pairs) == 8  # sanity check against a known, fixed result

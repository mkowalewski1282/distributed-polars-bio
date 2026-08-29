"""
Formalne testy poprawności dla operacji merge (Faza C) — Ballista (lokalnie,
patrz ballista_genomics/OPIS.md nt. braku furtki do pełnej dystrybucji dla tej
operacji) i Sail (prawdziwy UDTF, ten sam wzorzec co overlap).

Uruchomienie: pytest tests/test_merge_correctness.py -v
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest

from tests.merge_oracle import reference_merge_intervals

INTERVALS_A = [
    ("chr1", 100, 200, "gene_A1"),
    ("chr1", 150, 300, "gene_A2"),
    ("chr1", 400, 500, "gene_A3"),
    ("chr2", 50, 150, "gene_A4"),
    ("chr2", 200, 350, "gene_A5"),
]

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
MERGE_BINARY = BALLISTA_DIR / "target" / "debug" / "merge_local"
MERGE_OUTPUT_CSV = BALLISTA_DIR / "output" / "merge_local_result.csv"


@pytest.mark.skipif(
    not MERGE_BINARY.exists(),
    reason="merge_local nie jest zbudowane — cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin merge_local",
)
def test_ballista_local_merge_matches_oracle():
    result = subprocess.run(
        [str(MERGE_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert MERGE_OUTPUT_CSV.exists()

    df = pd.read_csv(MERGE_OUTPUT_CSV)
    actual = set(zip(df["chrom"].tolist(), df["start"].tolist(), df["end"].tolist()))
    expected = reference_merge_intervals(INTERVALS_A)

    assert actual == expected, (
        f"Tylko w pb.merge(): {expected - actual}\nTylko w Ballistrze: {actual - expected}"
    )


def test_sail_merge_matches_oracle():
    import sail_merge_udtf as mod

    sail_result, _ = mod.run_sail_merge()
    actual = set(
        zip(
            sail_result["chrom"].tolist(),
            sail_result["start"].tolist(),
            sail_result["end"].tolist(),
        )
    )
    expected = reference_merge_intervals(INTERVALS_A)

    assert actual == expected, (
        f"Tylko w pb.merge(): {expected - actual}\nTylko w Sail: {actual - expected}"
    )

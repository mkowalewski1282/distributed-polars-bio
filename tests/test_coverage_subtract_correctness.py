"""
Formalne testy poprawności dla coverage i subtract (Faza C) — Ballista
(lokalnie) i Sail (prawdziwy UDTF).

Uruchomienie: pytest tests/test_coverage_subtract_correctness.py -v
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest

from tests.coverage_subtract_oracle import reference_coverage, reference_subtract

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
COVERAGE_BINARY = BALLISTA_DIR / "target" / "debug" / "coverage_local"
COVERAGE_OUTPUT_CSV = BALLISTA_DIR / "output" / "coverage_local_result.csv"
SUBTRACT_BINARY = BALLISTA_DIR / "target" / "debug" / "subtract_local"
SUBTRACT_OUTPUT_CSV = BALLISTA_DIR / "output" / "subtract_local_result.csv"


@pytest.mark.skipif(not COVERAGE_BINARY.exists(), reason="coverage_local nie jest zbudowane")
def test_ballista_local_coverage_matches_oracle():
    result = subprocess.run(
        [str(COVERAGE_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    df = pd.read_csv(COVERAGE_OUTPUT_CSV)
    actual = set(zip(df["chrom"].tolist(), df["start"].tolist(), df["end"].tolist(), df["coverage"].tolist()))
    expected = reference_coverage(INTERVALS_A, INTERVALS_B)
    assert actual == expected, (
        f"Tylko w pb.coverage(): {expected - actual}\nTylko w Ballistrze: {actual - expected}"
    )


@pytest.mark.skipif(not SUBTRACT_BINARY.exists(), reason="subtract_local nie jest zbudowane")
def test_ballista_local_subtract_matches_oracle():
    result = subprocess.run(
        [str(SUBTRACT_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    df = pd.read_csv(SUBTRACT_OUTPUT_CSV)
    actual = set(zip(df["chrom"].tolist(), df["start"].tolist(), df["end"].tolist()))
    expected = reference_subtract(INTERVALS_A, INTERVALS_B)
    assert actual == expected, (
        f"Tylko w pb.subtract(): {expected - actual}\nTylko w Ballistrze: {actual - expected}"
    )


def test_sail_coverage_and_subtract_match_oracle():
    """
    Jeden test dla obu UDTF-ów, woła je w JEDNEJ sesji Spark Connect przez
    run_udtfs(). UWAGA (znaleziona pułapka): PySpark cache'uje SparkSession
    jako globalny singleton — dwa osobne testy pytest, każdy tworzący WŁASNY
    SparkConnectServer, kończyły się błędem "Connection refused" na drugim
    (getOrCreate() zwracał starą, już zatrzymaną sesję). Stąd jeden test,
    jedna sesja, dwa UDTF-y — patrz sail_coverage_subtract_udtf.run_udtfs().
    """
    import sail_coverage_subtract_udtf as mod

    results = mod.run_udtfs({
        "coverage_udtf": mod._make_coverage_udtf,
        "subtract_udtf": mod._make_subtract_udtf,
    })

    sail_cov = results["coverage_udtf"]
    actual_cov = set(
        zip(
            sail_cov["chrom"].tolist(),
            sail_cov["start"].tolist(),
            sail_cov["end"].tolist(),
            sail_cov["coverage"].tolist(),
        )
    )
    expected_cov = reference_coverage(INTERVALS_A, INTERVALS_B)
    assert actual_cov == expected_cov, (
        f"Tylko w pb.coverage(): {expected_cov - actual_cov}\nTylko w Sail: {actual_cov - expected_cov}"
    )

    sail_sub = results["subtract_udtf"]
    actual_sub = set(
        zip(sail_sub["chrom"].tolist(), sail_sub["start"].tolist(), sail_sub["end"].tolist())
    )
    expected_sub = reference_subtract(INTERVALS_A, INTERVALS_B)
    assert actual_sub == expected_sub, (
        f"Tylko w pb.subtract(): {expected_sub - actual_sub}\nTylko w Sail: {actual_sub - expected_sub}"
    )

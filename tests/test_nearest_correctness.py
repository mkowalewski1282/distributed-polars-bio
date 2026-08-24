"""
Formalne testy poprawności dla operacji nearest (Faza C) — Ballista (lokalnie)
i Sail (prawdziwy UDTF).

Uruchomienie: pytest tests/test_nearest_correctness.py -v
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
    reason="nearest_local nie jest zbudowane — cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin nearest_local",
)
def test_ballista_local_nearest_matches_oracle():
    """
    Porównuje ODLEGŁOŚCI (name_a -> distance), nie dokładnych wybranych
    partnerów. Znalezisko z tej sesji: gdy interwał A ma kilku kandydatów w
    tej samej (zerowej) odległości, natywny nearest() z
    datafusion-bio-function-ranges i pb.nearest() wybierają różnych
    kandydatów (różne, nieudokumentowane reguły tie-breakingu) — np. dla
    A2=[150,300) oba B1=[180,250) i B2=[290,420) overlapują (dystans 0),
    pb.nearest() wybiera B1, Ballista wybiera B2. Obie odpowiedzi są
    poprawne co do odległości — porównanie 1:1 par nie ma tu sensu.
    """
    result = subprocess.run(
        [str(NEAREST_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert NEAREST_OUTPUT_CSV.exists()

    df = pd.read_csv(NEAREST_OUTPUT_CSV)
    actual_distances = dict(zip(df["left_name"].tolist(), df["distance"].tolist()))
    expected_distances = reference_nearest_min_distances(INTERVALS_A, INTERVALS_B)

    assert actual_distances == expected_distances, (
        f"Różnica w odległościach.\n"
        f"pb.nearest():  {expected_distances}\n"
        f"Ballista:      {actual_distances}"
    )


def test_ballista_and_pb_nearest_disagree_on_tie_break():
    """
    Dokumentuje (nie tylko przez komentarz, ale przez asercję) znalezisko:
    dla tego zestawu danych syntetycznych faktycznie istnieje remis, na
    którym oba silniki różnią się wyborem konkretnego partnera — więc
    poprzedni test (test_ballista_local_nearest_matches_oracle) faktycznie
    testuje coś nietrywialnego, nie przechodzi "przez przypadek" bo remisów
    nie ma. Jeśli kiedyś ten test zacznie failować, to znaczy że dane testowe
    się zmieniły i ten fakt (istnienie remisu w danych) już nie zachodzi —
    trzeba by go odtworzyć innymi danymi, żeby test powyżej miał sens.
    """
    result = subprocess.run(
        [str(NEAREST_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        pytest.skip("nearest_local binary failed to run")

    df = pd.read_csv(NEAREST_OUTPUT_CSV)
    ballista_pairs = set(zip(df["left_name"].tolist(), df["right_name"].tolist()))
    pb_pairs = reference_nearest_pairs(INTERVALS_A, INTERVALS_B)

    assert ballista_pairs != pb_pairs, (
        "Oczekiwano remisu (różnych wyborów partnera) między silnikami na tych "
        "danych testowych — jeśli ten test failuje, dane się zmieniły i nie "
        "demonstrują już tie-breakingu; test_ballista_local_nearest_matches_oracle "
        "wciąż powinien przechodzić, ale bez tej asercji nie wiadomo czy testuje "
        "coś nietrywialnego."
    )


def test_sail_nearest_matches_oracle():
    import sail_nearest_udtf as mod

    sail_result, _ = mod.run_sail_nearest()
    actual = set(zip(sail_result["name_a"].tolist(), sail_result["name_b"].tolist()))
    expected = reference_nearest_pairs(INTERVALS_A, INTERVALS_B)

    assert actual == expected, (
        f"Tylko w pb.nearest(): {expected - actual}\nTylko w Sail: {actual - expected}"
    )

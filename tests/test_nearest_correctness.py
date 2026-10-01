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
    Porównuje ODLEGŁOŚCI (name_a -> distance), nie wybranych partnerów:
    reguły rozstrzygania remisów (kilku kandydatów w tej samej odległości)
    nie są udokumentowane w żadnym z silników, więc porównanie odporne na
    remisy jest bezpieczniejsze. Liczba wierszy = liczba przedziałów A
    pilnuje orientacji (wiersz na każdy przedział A, jak pb.nearest).
    """
    result = subprocess.run(
        [str(NEAREST_BINARY)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert NEAREST_OUTPUT_CSV.exists()

    # Konwencja dostawcy: wynik ma wiersz na każdy wiersz PRAWEJ tabeli (odpytywanej), z
    # najbliższym sąsiadem z lewej (indeksowanej). Żeby odpowiadało pb.nearest(A, B),
    # A jest prawą tabelą — nazwy A są w `right_name`, wybrani sąsiedzi z B w `left_name`.
    df = pd.read_csv(NEAREST_OUTPUT_CSV)
    assert len(df) == len(INTERVALS_A), "nearest ma dać jeden wiersz na każdy przedział A"
    actual_distances = dict(zip(df["right_name"].tolist(), df["distance"].tolist()))
    expected_distances = reference_nearest_min_distances(INTERVALS_A, INTERVALS_B)

    assert actual_distances == expected_distances, (
        f"Różnica w odległościach.\n"
        f"pb.nearest():  {expected_distances}\n"
        f"Ballista:      {actual_distances}"
    )


def test_ballista_and_pb_nearest_pick_same_neighbours():
    """
    KOREKTA (plan 2): w Fazie C zapisano znalezisko „przy remisie pb.nearest()
    wybiera B1, Ballista B2” — i test wymagał tej różnicy. Okazało się, że było
    to artefaktem odwróconej orientacji: nearest() z datafusion-bio-function-ranges
    zwraca wiersz na każdy wiersz PRAWEJ tabeli, więc wołanie nearest(A, B)
    liczyło „dla każdego B najbliższy A”, a nie to, co pb.nearest(A, B).
    Przy poprawnej orientacji (A jako prawa) oba silniki wybierają na tych
    danych DOKŁADNIE tych samych sąsiadów, także przy remisie A2 (B1 i B2 w
    odległości 0). Jeśli ten test zacznie failować, reguły remisów silników
    się rozeszły — wtedy porównania i tak pozostają poprawne (odległości).
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
        "Ballista i pb.nearest() wybrały różnych sąsiadów (różne reguły remisów) — "
        f"Ballista: {sorted(ballista_pairs)}, pb: {sorted(pb_pairs)}"
    )


def test_sail_nearest_matches_oracle():
    import sail_nearest_udtf as mod

    sail_result, _ = mod.run_sail_nearest()
    actual = set(zip(sail_result["name_a"].tolist(), sail_result["name_b"].tolist()))
    expected = reference_nearest_pairs(INTERVALS_A, INTERVALS_B)

    assert actual == expected, (
        f"Tylko w pb.nearest(): {expected - actual}\nTylko w Sail: {actual - expected}"
    )

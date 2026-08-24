"""
Formalny test poprawności dla Fazy A (Ballista) — brakujący punkt 6 z planu pracy
("Test poprawności na tym klastrze" — dotąd zrobiony tylko ręcznie, przez odczyt
wydruku). Uruchamia skompilowaną binarkę `ballista_genomics` (prawdziwy klaster
Ballista: scheduler + executor, LogicalExtensionCodec, realny shuffle — patrz
ballista_genomics/OPIS.md, Faza A.4), czyta jej wynik z CSV i porównuje z tą samą
wyrocznią co test Saila (tests/overlap_oracle.py).

Wymaga zbudowanej binarki: `cargo build` w ballista_genomics/ (pomijane, jeśli
binarki nie ma — patrz `pytest.mark.skipif` niżej, bo build trwa długo na tej
maszynie, patrz OPIS.md/plan pracy o ograniczeniach pamięci).

Uruchomienie: pytest tests/test_ballista_overlap.py -v
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

# Dane testowe muszą być identyczne z tymi zaszytymi w main.rs (DATA_LEFT_CSV/
# DATA_RIGHT_CSV wskazują na ballista_genomics/data/intervals_a.csv i intervals_b.csv)
# oraz z tymi w overlap_comparison.py/sail_overlap_udtf.py — to ten sam zestaw
# używany we wszystkich testach w tej pracy, żeby wyniki były bezpośrednio
# porównywalne między silnikami.
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
        "ballista_genomics nie jest zbudowane — uruchom "
        "`cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build` (patrz OPIS.md "
        "odnośnie ograniczeń pamięci na tej maszynie: użyj CARGO_BUILD_JOBS=1)"
    ),
)
def test_ballista_distributed_overlap_matches_oracle():
    """
    Uruchamia prawdziwy klaster Ballista (main.rs: scheduler + executor,
    LogicalExtensionCodec, shuffle) na danych testowych i sprawdza, że wynik
    operacji dist_overlap() jest identyczny z lokalnym pb.overlap().
    """
    result = subprocess.run(
        [str(BINARY)],
        cwd=BALLISTA_DIR,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, (
        f"binarka ballista_genomics zakończyła się błędem:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )

    assert OUTPUT_CSV.exists(), f"nie znaleziono {OUTPUT_CSV} — binarka nie zapisała wyniku"

    df = pd.read_csv(OUTPUT_CSV)

    expected_pairs = reference_overlap_pairs(INTERVALS_A, INTERVALS_B)
    actual_pairs = normalize_engine_pairs(df, "name_a", "name_b")

    assert actual_pairs == expected_pairs, (
        f"Różnica względem wyroczni.\n"
        f"Tylko w pb.overlap(): {expected_pairs - actual_pairs}\n"
        f"Tylko w Ballistrze:   {actual_pairs - expected_pairs}"
    )
    assert len(actual_pairs) == 8  # sanity check na znany, ustalony wynik

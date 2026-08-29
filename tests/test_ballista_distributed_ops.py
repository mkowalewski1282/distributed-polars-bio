"""
Faza H: testy poprawności operacji wykonanych W PEŁNI ROZPROSZONE w Ballistrze
(binarka `dist_ops`), porównane z wyrocznią — lokalnym polars-bio.

Różnica względem tests/test_*_correctness.py: tamte testy sprawdzają operacje
liczone LOKALNIE (zwykły DFSessionContext, binarki `*_local`). Tutaj każda
operacja przechodzi przez prawdziwy klaster Ballista (scheduler + executor),
z serializacją planu logicznego I fizycznego.

Wyrocznie są REUŻYTE bez zmian z testów lokalnych — dzięki temu porównujemy się
z dokładnie tym samym punktem odniesienia.

Uzupełnienie: tests/test_ballista_distribution_evidence.py sprawdza, że wynik
powstał faktycznie rozproszony (asercje na EXPLAIN ANALYZE), a nie tylko że
jest poprawny.

Uruchomienie: pytest tests/test_ballista_distributed_ops.py -v
UWAGA: import polars_bio trwa ~4.5 min — nie przerywać przedwcześnie.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest

from tests.merge_oracle import reference_merge_intervals

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
DIST_BINARY = BALLISTA_DIR / "target" / "debug" / "dist_ops"
OUTPUT_DIR = BALLISTA_DIR / "output"
DATA_DIR = BALLISTA_DIR / "data"

# Kanoniczny zestaw danych, wspólny dla WSZYSTKICH testów i obu silników.
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
        "dist_ops nie jest zbudowane — uruchom `cd ballista_genomics && "
        "CARGO_BUILD_JOBS=1 cargo build --bin dist_ops` (CARGO_BUILD_JOBS=1 jest "
        "obowiązkowe na tej maszynie, patrz OPIS.md)"
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
        f"dist_ops {op} zakończyło się błędem:\n"
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
    Strażnik przeciw cichemu rozjazdowi danych.

    Wariant rozproszony czyta z katalogów data/parts_a i data/parts_b (rozbicie
    na wiele plików jest KONIECZNE, żeby stage źródłowy miał >1 partycji — bez
    tego Ballista może w ogóle nie wstawić shuffle). Wyrocznia liczy natomiast
    na listach INTERVALS_A/INTERVALS_B zaszytych w Pythonie.

    Gdyby te dwa zestawy się rozjechały, testy poprawności porównywałyby wynik
    rozproszony z wyrocznią policzoną na INNYCH danych — i mogłyby przechodzić
    lub padać z zupełnie mylącym powodem.
    """
    assert _read_parts("parts_a") == set(INTERVALS_A), "data/parts_a != INTERVALS_A"
    assert _read_parts("parts_b") == set(INTERVALS_B), "data/parts_b != INTERVALS_B"


def test_ballista_distributed_merge_matches_oracle():
    """
    Merge w pełni rozproszony: hash-shuffle po chromosomie + MergeExec na
    executorze, wynik porównany z lokalnym pb.merge().

    Ten test jest jednocześnie TESTEM DYSTRYBUCJI, nie tylko poprawności:
    nakładające się gene_A1=[100,200) i gene_A2=[150,300) leżą w RÓŻNYCH plikach
    wejściowych (a_part1.csv / a_part2.csv), więc trafiają do różnych partycji
    źródłowych. Poprawny wynik [100,300) może powstać TYLKO wtedy, gdy hash-shuffle
    po `chrom` faktycznie przeniósł je do jednej partycji. Gdyby dystrybucja
    przestała działać, dostalibyśmy dwa osobne interwały zamiast jednego.
    """
    _run_dist("merge")
    out = OUTPUT_DIR / "dist_merge_result.csv"
    assert out.exists(), f"nie znaleziono {out}"

    df = pd.read_csv(out)
    actual = {(r.chrom, int(r.start), int(r.end)) for _, r in df.iterrows()}
    expected = reference_merge_intervals(INTERVALS_A)

    assert actual == expected, (
        f"Różnica względem wyroczni pb.merge().\n"
        f"Tylko w pb.merge():        {expected - actual}\n"
        f"Tylko w rozproszonym merge: {actual - expected}"
    )
    assert ("chr1", 100, 300) in actual, (
        "Brak scalonego [100,300) — interwały z RÓŻNYCH plików wejściowych nie "
        "zostały połączone, co oznacza że hash-shuffle po chrom nie zadziałał."
    )

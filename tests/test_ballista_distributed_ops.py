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

from tests.coverage_subtract_oracle import reference_coverage, reference_subtract
from tests.merge_oracle import reference_merge_intervals
from tests.nearest_oracle import reference_nearest_min_distances

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


def test_ballista_distributed_subtract_matches_oracle():
    """
    Subtract w pełni rozproszony: DWUSTRONNY hash-shuffle po chromosomie.

    To najsilniejszy demonstrator dystrybucji w tym zestawie — SubtractExec jest
    węzłem binarnym, a `execute(partition)` sięga po TĘ SAMĄ partycję z lewej
    i z prawej strony. Poprawny wynik wymaga więc, żeby obie strony zostały
    ko-partycjonowane po `chrom` na tę samą liczbę partycji. Obie tabele są
    rozbite na po 2 pliki, więc obie mają równoległy stage źródłowy.
    """
    _run_dist("subtract")
    out = OUTPUT_DIR / "dist_subtract_result.csv"
    assert out.exists(), f"nie znaleziono {out}"

    df = pd.read_csv(out)
    actual = {(r.chrom, int(r.start), int(r.end)) for _, r in df.iterrows()}
    expected = reference_subtract(INTERVALS_A, INTERVALS_B)

    assert actual == expected, (
        f"Różnica względem wyroczni pb.subtract().\n"
        f"Tylko w pb.subtract():         {expected - actual}\n"
        f"Tylko w rozproszonym subtract: {actual - expected}"
    )


def test_ballista_distributed_nearest_matches_oracle_distances():
    """
    Nearest w pełni rozproszony, wzorzec BROADCAST: lewa (indeksowana) tabela
    jedzie w całości w ładunku planu fizycznego (Arrow IPC), a równoległość
    bierze się z partycjonowania prawej strony.

    Porównujemy ODLEGŁOŚCI, nie konkretne pary — to znane, udokumentowane
    znalezisko z Fazy C: natywny nearest() z bio-function-ranges i pb.nearest()
    inaczej rozstrzygają remisy (gdy kilku kandydatów ma tę samą, zerową
    odległość). Obie odpowiedzi są poprawne co do dystansu.
    Patrz tests/test_nearest_correctness.py.
    """
    _run_dist("nearest")
    out = OUTPUT_DIR / "dist_nearest_result.csv"
    assert out.exists(), f"nie znaleziono {out}"

    df = pd.read_csv(out)
    actual = dict(zip(df["left_name"].tolist(), df["distance"].tolist()))
    expected = reference_nearest_min_distances(INTERVALS_A, INTERVALS_B)

    assert set(actual) == set(expected), (
        f"Inny zbiór interwałów lewej tabeli.\n"
        f"Tylko w pb.nearest(): {set(expected) - set(actual)}\n"
        f"Tylko rozproszony:    {set(actual) - set(expected)}"
    )
    for name, dist in expected.items():
        assert int(actual[name]) == int(dist), (
            f"{name}: pb.nearest() dało odległość {dist}, "
            f"rozproszony nearest {actual[name]}"
        )


def test_distributed_nearest_agrees_with_local_nearest():
    """
    Warunek OSTRZEJSZY niż zgodność odległości: rozproszony nearest musi wybrać
    DOKŁADNIE tych samych partnerów co lokalny `nearest_local` — ten sam silnik,
    ta sama funkcja build_nearest_indexes, ten sam left_batch (tyle że po
    round-tripie przez Arrow IPC).

    To jest właściwy test broadcastu: gdyby na executor trafiła tylko CZĘŚĆ
    lewej tabeli, odległości mogłyby wyjść zawyżone, a wybór partnera inny —
    a porównanie samych odległości z pb tego by nie wyłapało, bo pb liczy na
    pełnych danych po swojej stronie.
    """
    local_csv = OUTPUT_DIR / "nearest_local_result.csv"
    if not local_csv.exists():
        pytest.skip(
            "brak output/nearest_local_result.csv — uruchom najpierw "
            "`cd ballista_genomics && ./target/debug/nearest_local`"
        )
    _run_dist("nearest")

    def pairs(path: Path) -> set[tuple[str, str]]:
        d = pd.read_csv(path)
        return set(zip(d["left_name"].tolist(), d["right_name"].tolist()))

    dist_pairs = pairs(OUTPUT_DIR / "dist_nearest_result.csv")
    local_pairs = pairs(local_csv)

    assert dist_pairs == local_pairs, (
        f"Rozproszony i lokalny nearest wybrały RÓŻNYCH partnerów — to sygnał, "
        f"że broadcast lewej tabeli był niekompletny albo kolejność wierszy w "
        f"left_batch się rozjechała.\n"
        f"Tylko lokalnie:    {local_pairs - dist_pairs}\n"
        f"Tylko rozproszony: {dist_pairs - local_pairs}"
    )


def test_ballista_distributed_coverage_matches_oracle():
    """
    Coverage w pełni rozproszony — jedyna operacja wymagająca WŁASNEGO
    węzła-nośnika (DistCoverageExec), bo CountOverlapsProvider::scan() gubi
    dane lewej tabeli, nazwy jej kolumn i flagę coverage (patrz coverage_node.rs).

    Uwaga na ODWRÓCONĄ KONWENCJĘ ARGUMENTÓW, udokumentowaną w Fazie C:
    pb.coverage(a, b) raportuje pokrycie interwałów `a` przez `b`, a SQL-owe
    coverage('reads','targets') odwrotnie — dlatego dist_coverage jest wołane
    z intervals_b jako 'reads' i intervals_a jako 'targets'.
    """
    _run_dist("coverage")
    out = OUTPUT_DIR / "dist_coverage_result.csv"
    assert out.exists(), f"nie znaleziono {out}"

    df = pd.read_csv(out)
    actual = {
        (r.chrom, int(r.start), int(r.end), int(r.coverage)) for _, r in df.iterrows()
    }
    expected = reference_coverage(INTERVALS_A, INTERVALS_B)

    assert actual == expected, (
        f"Różnica względem wyroczni pb.coverage().\n"
        f"Tylko w pb.coverage():         {expected - actual}\n"
        f"Tylko w rozproszonym coverage: {actual - expected}"
    )

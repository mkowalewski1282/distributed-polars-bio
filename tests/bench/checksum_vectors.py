"""Wspólne wartości wzorcowe sumy kontrolnej (specyfikacja 8.4): te same wiersze sprawdza
implementacja w Pythonie (bench/checksum.py) i w Rust (bench_client --checksum)."""

from __future__ import annotations

import pyarrow as pa

from bench.ops import KEY_COLUMNS, OUTPUT_COLUMNS

#: Wiersze w schemacie znormalizowanym (kolejność OUTPUT_COLUMNS): duplikat, chromosom bez
#: sąsiada (braki wartości), chr10 obok chr2, wartość 2³¹ − 1, zera.
ROWS: dict[str, list[tuple]] = {
    "overlap": [
        ("chr1", 100, 200, "chr1", 150, 300),
        ("chr1", 100, 200, "chr1", 150, 300),
        ("chr10", 0, 2147483647, "chr10", 5, 6),
    ],
    "nearest": [
        ("chr1", 700, 800, "chr1", 450, 600, 100),
        ("chrA", 10, 20, None, None, None, None),
        ("chr2", 10, 40, "chr2", 100, 220, 60),
    ],
    "coverage": [("chr1", 100, 200, 2), ("chrA", 10, 20, 0)],
    "merge": [("chr1", 100, 300, 4), ("chr2", 200, 350, 1)],
    "subtract": [("chr1", 100, 180), ("chr1", 100, 180), ("chr2", 220, 300)],
}

#: Wartości wyliczone z definicji (bench/checksum.py, 01.10.2026). Zmiana definicji sumy
#: kontrolnej unieważnia porównania z wcześniejszymi seriami — wtedy świadomie zaktualizować
#: te stałe i opisać zmianę w specyfikacji (sekcja 8.4).
CHECKSUMS: dict[str, str] = {
    "overlap": "0x696c86b3d336e487",
    "nearest": "0xf471dd63731fa19a",
    "coverage": "0xc895a77648f51a40",
    "merge": "0x126055154e6b5ce1",
    "subtract": "0x34706f6749df24f2",
}


def _arrow_type(column: str) -> pa.DataType:
    """Typy jak w wynikach silników: chromosom string, początek/koniec int32, reszta int64."""
    if column.startswith("chrom"):
        return pa.string()
    if column.startswith(("start", "end")):
        return pa.int32()
    return pa.int64()


def table(op: str) -> pa.Table:
    rows = ROWS[op]
    return pa.table({
        c: pa.array([r[i] for r in rows], _arrow_type(c)) for i, c in enumerate(OUTPUT_COLUMNS[op])
    })


def key_rows(op: str) -> list[tuple]:
    """Wiersze zawężone do KEY_COLUMNS[op] — wejście definicji wzorcowej."""
    idx = [OUTPUT_COLUMNS[op].index(c) for c in KEY_COLUMNS[op]]
    return [tuple(r[i] for i in idx) for r in ROWS[op]]

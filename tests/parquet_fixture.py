"""Mały zbiór testowy w układzie databio-8p: katalog z kilkoma plikami Parquet,
kolumny contig (string), pos_start i pos_end (int32), 0-based półotwarte — te
same nazwy i typy co w prawdziwych plikach (sprawdzone na archiwum).

Przypadki, na których silniki mogą się rozjechać:
- chromosom tylko w A (chrA) i tylko w B (chrB);
- duplikat przedziału w A;
- przedziały stykające się (koniec jednego = początek drugiego), w A i między A a B;
- chr10 obok chr2 (porządek leksykograficzny różny od numerycznego);
- nakładające się przedziały chr1 w RÓŻNYCH plikach (merge wymaga shuffle).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# write_parts rozdziela wiersze na przemian: parzyste -> plik 0, nieparzyste -> plik 1.
FIXTURE_A = [
    ("chr1", 100, 200),    # plik 0
    ("chr1", 150, 300),    # plik 1 — nakłada się na poprzedni
    ("chr1", 400, 500),    # plik 0
    ("chr1", 100, 200),    # plik 1 — duplikat
    ("chr2", 50, 150),     # plik 0
    ("chr2", 200, 350),    # plik 1
    ("chr2", 350, 400),    # plik 0 — styka się z poprzednim
    ("chr10", 1000, 1100), # plik 1
    ("chrA", 10, 20),      # plik 0 — chromosom tylko w A
]

FIXTURE_B = [
    ("chr1", 180, 250),
    ("chr1", 290, 420),
    ("chr1", 450, 600),
    ("chr2", 100, 220),
    ("chr2", 300, 400),
    ("chr10", 1100, 1200), # styka się z chr10 z A
    ("chrB", 5, 50),       # chromosom tylko w B
]


def write_parts(rows: list[tuple[str, int, int]], directory: Path, n_files: int = 2) -> Path:
    """Zapisuje przedziały do `n_files` plików part-*.parquet (wiersze na przemian)."""
    directory.mkdir(parents=True)
    for i in range(n_files):
        chunk = rows[i::n_files]
        table = pa.table({
            "contig": pa.array([r[0] for r in chunk], pa.string()),
            "pos_start": pa.array([r[1] for r in chunk], pa.int32()),
            "pos_end": pa.array([r[2] for r in chunk], pa.int32()),
        })
        pq.write_table(table, directory / f"part-{i:05d}.parquet")
    return directory

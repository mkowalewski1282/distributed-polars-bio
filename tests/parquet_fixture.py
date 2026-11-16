"""Small test dataset in the databio-8p layout: a directory with a few Parquet files,
columns contig (string), pos_start and pos_end (int32), 0-based half-open; the
same names and types as in the real files (checked against the archive).

Cases on which the engines may diverge:
- a chromosome only in A (chrA) and only in B (chrB);
- a duplicate interval in A;
- touching intervals (the end of one = the start of another), within A and between A and B;
- non-zero distances to the nearest neighbour, to the left and to the right, and a tie;
- chr10 next to chr2 (lexicographic order differs from numeric);
- overlapping chr1 intervals in DIFFERENT files (merge needs a shuffle).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# write_parts distributes rows alternately: even -> file 0, odd -> file 1.
FIXTURE_A = [
    ("chr1", 100, 200),    # file 0
    ("chr1", 150, 300),    # file 1 — overlaps the previous one
    ("chr1", 400, 500),    # file 0
    ("chr1", 100, 200),    # file 1 — duplicate
    ("chr2", 50, 150),     # file 0
    ("chr2", 200, 350),    # file 1
    ("chr2", 350, 400),    # file 0 — touches the previous one
    ("chr10", 1000, 1100), # file 1
    ("chrA", 10, 20),      # file 0 — chromosome only in A
    ("chr1", 700, 800),    # file 1 — gap of 100 to the nearest [450, 600) in B
    ("chr2", 10, 40),      # file 0 — gap of 60 to the nearest [100, 220) in B (to the right)
    ("chr1", 260, 280),    # file 1 — tie: gap of 10 to both [180, 250) and [290, 420)
]

FIXTURE_B = [
    ("chr1", 180, 250),
    ("chr1", 290, 420),
    ("chr1", 450, 600),
    ("chr2", 100, 220),
    ("chr2", 300, 400),
    ("chr10", 1100, 1200), # touches chr10 from A
    ("chrB", 5, 50),       # chromosome only in B
]


def write_parts(rows: list[tuple[str, int, int]], directory: Path, n_files: int = 2) -> Path:
    """Writes the intervals to `n_files` part-*.parquet files (rows alternate between files)."""
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

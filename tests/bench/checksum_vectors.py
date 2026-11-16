"""Shared reference values for the checksum: the same rows are checked by the Python
implementation (bench/checksum.py) and the Rust one (bench_client --checksum)."""

from __future__ import annotations

import pyarrow as pa

from bench.ops import KEY_COLUMNS, OUTPUT_COLUMNS

#: Rows in the normalized schema (OUTPUT_COLUMNS order): a duplicate, a chromosome with no
#: neighbour (missing values), chr10 next to chr2, the value 2³¹ − 1, zeros.
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

#: Values computed from the definition (bench/checksum.py, 2026-10-01). Changing the checksum
#: definition invalidates comparisons with earlier series; in that case update these
#: constants deliberately and document the change.
CHECKSUMS: dict[str, str] = {
    "overlap": "0x696c86b3d336e487",
    "nearest": "0xf471dd63731fa19a",
    "coverage": "0xc895a77648f51a40",
    "merge": "0x126055154e6b5ce1",
    "subtract": "0x34706f6749df24f2",
}


def _arrow_type(column: str) -> pa.DataType:
    """Types as in the engines' results: chromosome string, start/end int32, the rest int64."""
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
    """Rows narrowed to KEY_COLUMNS[op], the input of the reference definition."""
    idx = [OUTPUT_COLUMNS[op].index(c) for c in KEY_COLUMNS[op]]
    return [tuple(r[i] for i in idx) for r in ROWS[op]]

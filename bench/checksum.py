"""Row-order-independent checksum of an operation result.

Definition — shared with ballista_genomics/src/checksum.rs (agreement is enforced by a test of
genomics-engines-bench on the reference values in its tests/bench/checksum_vectors.py):

- columns: `KEY_COLUMNS[op]` from bench/ops.py — the same ones `row_multiset` compares, so the
  checksum is a digest of the same multiset (nearest without the neighbour's identity);
- value code: a chromosome column (name starts with `chrom`) — CRC-32 (IEEE, as `zlib.crc32`)
  of the UTF-8 text; a numeric column — the integer value modulo 2⁶⁴ (two's complement); a
  missing value — 2⁶⁴ − 1 (as −1; result columns are non-negative);
- row hash: `mix64(Σ_j M_j · code_j mod 2⁶⁴)`, M_j — the odd constants below, mix64 — the
  splitmix64 finalizer (a bijection on 64-bit numbers);
- checksum: Σ of row hashes mod 2⁶⁴, written as `0x` + 16 hexadecimal digits.

A plain linear sum (without mix64) would depend only on column sums and would not detect, for
example, interval ends swapped between rows — hence the row hash is non-linear.

`row_hash`/`checksum_rows` are the reference definition (pure Python, row by row); `Checksum`
computes the same thing vectorised (numpy) on Arrow batches; runners call `update_all` after
stopping the clock, on batches held in memory.
"""

from __future__ import annotations

import zlib
from typing import Iterable, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from bench.ops import KEY_COLUMNS

MASK = (1 << 64) - 1
#: Code of a missing value (as −1 in two's complement).
NULL_CODE = MASK
#: Key-column multipliers by position: the xxHash64 prime constants and the splitmix64 golden-ratio constant.
MULTIPLIERS = (
    0x9E3779B185EBCA87,
    0xC2B2AE3D27D4EB4F,
    0x165667B19E3779F9,
    0x85EBCA77C2B2AE63,
    0x27D4EB2F165667C5,
    0x9E3779B97F4A7C15,
)
_MIX1 = 0xBF58476D1CE4E5B9
_MIX2 = 0x94D049BB133111EB
#: Result batches are merged before the checksum is computed (Checksum.update_all): numpy has a
#: fixed cost per call, and Sail returns results in batches of ~1.5k rows — a probe measured 1877
#: batches and 1.9 s of checksum time on overlap 7-0 when it was computed per batch.
GROUP_ROWS = 1 << 16


def mix64(z: int) -> int:
    """Finalizator splitmix64."""
    z = ((z ^ (z >> 30)) * _MIX1) & MASK
    z = ((z ^ (z >> 27)) * _MIX2) & MASK
    return z ^ (z >> 31)


def value_code(column: str, value) -> int:
    if value is None:
        return NULL_CODE
    if column.startswith("chrom"):
        return zlib.crc32(str(value).encode("utf-8"))
    return int(value) & MASK


def row_hash(op: str, row: Sequence) -> int:
    """Hash of one row (a tuple in `KEY_COLUMNS[op]` order) — the reference definition."""
    acc = 0
    for j, (column, value) in enumerate(zip(KEY_COLUMNS[op], row, strict=True)):
        acc = (acc + MULTIPLIERS[j] * value_code(column, value)) & MASK
    return mix64(acc)


def checksum_rows(op: str, rows: Iterable[Sequence]) -> int:
    """Checksum of tuples in `KEY_COLUMNS[op]` order (e.g. `row_multiset(...).elements()`)."""
    total = 0
    for row in rows:
        total = (total + row_hash(op, row)) & MASK
    return total


def format_checksum(value: int) -> str:
    return f"0x{value & MASK:016x}"


def _u64(x: int) -> np.uint64:
    return np.uint64(x)


def _mix64_array(z: np.ndarray) -> np.ndarray:
    z = (z ^ (z >> _u64(30))) * _u64(_MIX1)
    z = (z ^ (z >> _u64(27))) * _u64(_MIX2)
    return z ^ (z >> _u64(31))


def _chrom_codes(column) -> np.ndarray:
    encoded = pc.dictionary_encode(pc.cast(column, pa.string()))
    if isinstance(encoded, pa.ChunkedArray):
        encoded = encoded.combine_chunks() if encoded.num_chunks != 1 else encoded.chunk(0)
    words = encoded.dictionary.to_pylist()
    # The last position of the code array (index len(words)) = missing value.
    codes = np.array([zlib.crc32(w.encode("utf-8")) for w in words] + [NULL_CODE], dtype=np.uint64)
    return codes[encoded.indices.fill_null(len(words)).to_numpy(zero_copy_only=False)]


def _int_codes(column) -> np.ndarray:
    values = pc.cast(column, pa.int64()).fill_null(-1).to_numpy(zero_copy_only=False)
    # -1 in two's complement is 2⁶⁴ − 1 = NULL_CODE.
    return np.ascontiguousarray(values, dtype=np.int64).view(np.uint64)


class Checksum:
    """Checksum computed incrementally over successive chunks of the result (a stream of batches)."""

    def __init__(self, op: str):
        if op not in KEY_COLUMNS:
            raise ValueError(f"unknown operation {op!r}")
        self.op = op
        self.rows = 0
        self._sum = np.uint64(0)

    def update(self, data) -> None:
        """Add a chunk: a pyarrow.Table / RecordBatch or a polars.DataFrame in the normalised
        schema (bench/ops.py)."""
        if not isinstance(data, (pa.Table, pa.RecordBatch)):
            data = data.to_arrow()
        names = KEY_COLUMNS[self.op]
        missing = [n for n in names if n not in data.schema.names]
        if missing:
            raise KeyError(f"{self.op}: missing columns {missing}; got {data.schema.names}")
        if data.num_rows == 0:
            return
        acc = np.zeros(data.num_rows, dtype=np.uint64)
        with np.errstate(over="ignore"):
            for m, name in zip(MULTIPLIERS, names):
                column = data.column(name)
                codes = _chrom_codes(column) if name.startswith("chrom") else _int_codes(column)
                acc += _u64(m) * codes
            self._sum += _mix64_array(acc).sum(dtype=np.uint64)
        self.rows += data.num_rows

    def update_all(self, parts: Iterable) -> None:
        """Add many chunks (pyarrow.Table / RecordBatch in the normalised schema), merging
        consecutive batches into tables of at least GROUP_ROWS rows."""
        group: list[pa.RecordBatch] = []
        size = 0
        for part in parts:
            for batch in part.to_batches() if isinstance(part, pa.Table) else [part]:
                group.append(batch)
                size += batch.num_rows
                if size >= GROUP_ROWS:
                    self.update(pa.Table.from_batches(group))
                    group, size = [], 0
        if group:
            self.update(pa.Table.from_batches(group))

    @property
    def value(self) -> int:
        return int(self._sum)

    def hex(self) -> str:
        return format_checksum(self.value)

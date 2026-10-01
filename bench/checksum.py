"""Suma kontrolna wyniku operacji niezależna od kolejności wierszy (specyfikacja, sekcja 8.4).

Definicja — wspólna z ballista_genomics/src/checksum.rs (zgodność pilnuje test na
wartościach wzorcowych z tests/bench/checksum_vectors.py):

- kolumny: `KEY_COLUMNS[op]` z bench/ops.py — te same, które porównuje `row_multiset`,
  więc suma jest skrótem tego samego multizbioru (nearest bez tożsamości sąsiada);
- kod wartości: kolumna chromosomu (nazwa zaczyna się od `chrom`) — CRC-32 (IEEE, jak
  `zlib.crc32`) z UTF-8; kolumna liczbowa — wartość całkowita modulo 2⁶⁴ (U2); brak
  wartości — 2⁶⁴ − 1 (jak −1; kolumny wyników są nieujemne);
- skrót wiersza: `mix64(Σ_j M_j · kod_j mod 2⁶⁴)`, M_j — stałe nieparzyste niżej,
  mix64 — finalizator splitmix64 (bijekcja na liczbach 64-bitowych);
- suma kontrolna: Σ skrótów wierszy mod 2⁶⁴, zapisana jako `0x` + 16 cyfr szesnastkowych.

Sama suma liniowa (bez mix64) zależałaby tylko od sum kolumn i nie wykryłaby np. zamiany
końców przedziałów między wierszami — dlatego skrót wiersza jest nieliniowy.

`row_hash`/`checksum_rows` to definicja wzorcowa (czysty Python, wiersz po wierszu);
`Checksum` liczy to samo wektorowo (numpy) na partiach Arrow — dla runnerów, które
konsumują wynik strumieniowo (5 mln wierszy ≈ 0,5 s).
"""

from __future__ import annotations

import zlib
from typing import Iterable, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from bench.ops import KEY_COLUMNS

MASK = (1 << 64) - 1
#: Kod braku wartości (jak −1 w U2).
NULL_CODE = MASK
#: Mnożniki kolumn klucza według pozycji: stałe pierwsze xxHash64 i złoty podział splitmix64.
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
    """Skrót jednego wiersza (krotka w kolejności `KEY_COLUMNS[op]`) — definicja wzorcowa."""
    acc = 0
    for j, (column, value) in enumerate(zip(KEY_COLUMNS[op], row, strict=True)):
        acc = (acc + MULTIPLIERS[j] * value_code(column, value)) & MASK
    return mix64(acc)


def checksum_rows(op: str, rows: Iterable[Sequence]) -> int:
    """Suma kontrolna krotek w kolejności `KEY_COLUMNS[op]` (np. `row_multiset(...).elements()`)."""
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
    # Ostatnia pozycja tablicy kodów (indeks len(words)) = brak wartości.
    codes = np.array([zlib.crc32(w.encode("utf-8")) for w in words] + [NULL_CODE], dtype=np.uint64)
    return codes[encoded.indices.fill_null(len(words)).to_numpy(zero_copy_only=False)]


def _int_codes(column) -> np.ndarray:
    values = pc.cast(column, pa.int64()).fill_null(-1).to_numpy(zero_copy_only=False)
    # -1 w U2 to 2⁶⁴ − 1 = NULL_CODE.
    return np.ascontiguousarray(values, dtype=np.int64).view(np.uint64)


class Checksum:
    """Suma kontrolna liczona przyrostowo na kolejnych porcjach wyniku (strumień partii)."""

    def __init__(self, op: str):
        if op not in KEY_COLUMNS:
            raise ValueError(f"nieznana operacja {op!r}")
        self.op = op
        self.rows = 0
        self._sum = np.uint64(0)

    def update(self, data) -> None:
        """Dokłada porcję: pyarrow.Table / RecordBatch albo polars.DataFrame w schemacie
        znormalizowanym (bench/ops.py)."""
        if not isinstance(data, (pa.Table, pa.RecordBatch)):
            data = data.to_arrow()
        names = KEY_COLUMNS[self.op]
        missing = [n for n in names if n not in data.schema.names]
        if missing:
            raise KeyError(f"{self.op}: brak kolumn {missing}; są {data.schema.names}")
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

    @property
    def value(self) -> int:
        return int(self._sum)

    def hex(self) -> str:
        return format_checksum(self.value)

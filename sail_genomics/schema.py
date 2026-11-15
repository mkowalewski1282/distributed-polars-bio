"""Schemat operacji i wyników pakietu — niezależny od reszty repozytorium.

Nazwy kolumn wyniku w schemacie znormalizowanym (identyczne z bench/ops.py; zgodność pilnuje
test tests/test_sail_arrow.py) oraz zamiana nazw kolumn polars-bio na ten schemat."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

OPS = ("overlap", "nearest", "coverage", "merge", "subtract")
UNARY_OPS = frozenset({"merge"})

#: Domyślne nazwy kolumn przedziału w plikach wejściowych.
DEFAULT_INPUT_COLUMNS = ("contig", "pos_start", "pos_end")

#: Kolumny wyniku w schemacie znormalizowanym, w tej kolejności.
OUTPUT_COLUMNS: dict[str, tuple[str, ...]] = {
    "overlap": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"),
    "nearest": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2", "distance"),
    "coverage": ("chrom", "start", "end", "coverage"),
    "merge": ("chrom", "start", "end", "n_intervals"),
    "subtract": ("chrom", "start", "end"),
}


def ddl_schema(op: str) -> str:
    """Schemat wyniku dla applyInArrow (DDL Sparka) w schemacie znormalizowanym."""
    return ", ".join(f"{c} {'string' if c.startswith('chrom') else 'long'}" for c in OUTPUT_COLUMNS[op])


def arrow_schema(op: str) -> pa.Schema:
    return pa.schema([(c, pa.string() if c.startswith("chrom") else pa.int64()) for c in OUTPUT_COLUMNS[op]])


def _names(op: str, cols: tuple[str, str, str]) -> dict[str, str]:
    """Nazwy kolumn wyniku polars-bio -> schemat znormalizowany."""
    c, s, e = cols
    if op in ("overlap", "nearest"):
        names = {
            f"{c}_1": "chrom_1", f"{s}_1": "start_1", f"{e}_1": "end_1",
            f"{c}_2": "chrom_2", f"{s}_2": "start_2", f"{e}_2": "end_2",
        }
        if op == "nearest":
            names["distance"] = "distance"
        return names
    names = {c: "chrom", s: "start", e: "end"}
    if op == "coverage":
        names["coverage"] = "coverage"
    elif op == "merge":
        names["n_intervals"] = "n_intervals"
    return names


def rename_polars_bio(op: str, df: pl.DataFrame, pb_cols: tuple[str, str, str]) -> pl.DataFrame:
    """Wynik polars-bio -> kolumny schematu znormalizowanego, w kolejności OUTPUT_COLUMNS."""
    names = _names(op, pb_cols)
    missing = [src for src in names if src not in df.columns]
    if missing:
        raise KeyError(f"{op}: polars-bio result lacks columns {missing}; has {df.columns}")
    return df.select([pl.col(src).alias(dst) for src, dst in names.items()])

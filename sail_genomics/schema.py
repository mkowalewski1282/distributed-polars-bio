"""Schema of the operations and results of the package — independent of the rest of the repository.

Result column names in the normalised schema (the same as in the genomics-engines-bench harness,
whose test `tests/test_sail_arrow.py` enforces the agreement with its `bench/ops.py`) and the
renaming of polars-bio columns to this schema."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

OPS = ("overlap", "nearest", "coverage", "merge", "subtract")
UNARY_OPS = frozenset({"merge"})

#: Default names of the interval columns in the input files.
DEFAULT_INPUT_COLUMNS = ("contig", "pos_start", "pos_end")

#: Result columns in the normalised schema, in this order.
OUTPUT_COLUMNS: dict[str, tuple[str, ...]] = {
    "overlap": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"),
    "nearest": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2", "distance"),
    "coverage": ("chrom", "start", "end", "coverage"),
    "merge": ("chrom", "start", "end", "n_intervals"),
    "subtract": ("chrom", "start", "end"),
}


def ddl_schema(op: str) -> str:
    """Result schema for applyInArrow (Spark DDL) in the normalised schema."""
    return ", ".join(f"{c} {'string' if c.startswith('chrom') else 'long'}" for c in OUTPUT_COLUMNS[op])


def arrow_schema(op: str) -> pa.Schema:
    return pa.schema([(c, pa.string() if c.startswith("chrom") else pa.int64()) for c in OUTPUT_COLUMNS[op]])


def _names(op: str, cols: tuple[str, str, str]) -> dict[str, str]:
    """Column names of a polars-bio result -> the normalised schema."""
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
    """A polars-bio result -> the columns of the normalised schema, in OUTPUT_COLUMNS order."""
    names = _names(op, pb_cols)
    missing = [src for src in names if src not in df.columns]
    if missing:
        raise KeyError(f"{op}: polars-bio result lacks columns {missing}; has {df.columns}")
    return df.select([pl.col(src).alias(dst) for src, dst in names.items()])

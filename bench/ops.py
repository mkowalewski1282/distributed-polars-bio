"""Benchmark operations and the common schema of their results.

Every engine returns its result under different column names. Comparisons (and the
checksum) work on the normalised schema below, on a multiset of rows — independent of row
order and type widths.
"""

from __future__ import annotations

from collections import Counter

import polars as pl

OPS = ("overlap", "nearest", "coverage", "merge", "subtract")
UNARY_OPS = frozenset({"merge"})

#: Result columns in the normalised schema, in this order.
OUTPUT_COLUMNS: dict[str, tuple[str, ...]] = {
    "overlap": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"),
    "nearest": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2", "distance"),
    "coverage": ("chrom", "start", "end", "coverage"),
    "merge": ("chrom", "start", "end", "n_intervals"),
    "subtract": ("chrom", "start", "end"),
}

#: Columns that enter the comparison. `nearest` without the neighbour's identity: on ties
#: (several candidates at the same distance) the engines choose differently.
KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    **OUTPUT_COLUMNS,
    "nearest": ("chrom_1", "start_1", "end_1", "distance"),
}


def polars_bio_names(op: str, cols: tuple[str, str, str]) -> dict[str, str]:
    """Column names of a polars-bio 0.28 result -> normalised schema."""
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


def normalize_polars_bio(op: str, df: pl.DataFrame, cols: tuple[str, str, str]) -> pl.DataFrame:
    names = polars_bio_names(op, cols)
    missing = [src for src in names if src not in df.columns]
    if missing:
        raise KeyError(f"{op}: polars-bio result lacks columns {missing}; has {df.columns}")
    return df.select([pl.col(src).alias(dst) for src, dst in names.items()])


def normalize_arrow(op: str, data, cols: tuple[str, str, str]):
    """An Arrow batch (RecordBatch or Table) with a polars-bio result -> the columns of the
    normalised schema, in this order — the counterpart of `normalize_polars_bio` for a stream
    of batches."""
    names = polars_bio_names(op, cols)
    missing = [src for src in names if src not in data.schema.names]
    if missing:
        raise KeyError(f"{op}: polars-bio result lacks columns {missing}; has {data.schema.names}")
    return type(data).from_arrays([data.column(src) for src in names], names=list(names.values()))


def row_multiset(op: str, df) -> Counter:
    """Multiset of rows over the key columns (polars or pandas; NaN = missing)."""
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df, nan_to_null=True)
    key = KEY_COLUMNS[op]
    missing = [k for k in key if k not in df.columns]
    if missing:
        raise KeyError(f"{op}: missing columns {missing}; got {df.columns}")
    exprs = [
        pl.col(k).cast(pl.Utf8) if k.startswith("chrom") else pl.col(k).cast(pl.Int64)
        for k in key
    ]
    return Counter(df.select(exprs).iter_rows())


def describe_diff(expected: Counter, actual: Counter, limit: int = 5) -> str:
    """Short description of the difference of two multisets — counts and a few examples."""
    missing = expected - actual
    extra = actual - expected
    return (
        f"missing {sum(missing.values())} rows, e.g. {list(missing)[:limit]}; "
        f"extra {sum(extra.values())}, e.g. {list(extra)[:limit]}"
    )

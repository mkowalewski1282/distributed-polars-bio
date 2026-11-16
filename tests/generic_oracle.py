"""Oracle for data in the databio-8p layout: local polars-bio, result in the
normalized schema (bench/ops.py).

Complements the oracles in tests/*_oracle.py, which work on a set of five
intervals with a `name` column; this one works on arbitrary Parquet files.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import polars_bio as pb

from bench.data.datasets import COLUMNS
from bench.ops import normalize_polars_bio


def _reset_pb_context() -> None:
    """polars-bio keeps a global context with tables s1/s2, as in the other oracles."""
    from polars_bio.context import ctx as _pb_ctx

    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def read_intervals(path: Path) -> pl.DataFrame:
    """Parquet file or directory of Parquet files -> DataFrame marked as 0-based."""
    path = Path(path)
    df = pl.read_parquet(str(path / "*.parquet") if path.is_dir() else str(path))
    df.config_meta.set(coordinate_system_zero_based=True)
    return df


def reference(
    op: str,
    left: pl.DataFrame,
    right: pl.DataFrame | None,
    cols: tuple[str, str, str] = COLUMNS,
) -> pl.DataFrame:
    """polars-bio result for an operation, in the normalized schema."""
    c = list(cols)
    _reset_pb_context()
    if op == "merge":
        result = pb.merge(left, cols=c)
    elif op in ("overlap", "nearest", "coverage", "subtract"):
        result = getattr(pb, op)(left, right, cols1=c, cols2=c)
    else:
        raise ValueError(f"unknown operation {op!r}")
    return normalize_polars_bio(op, result.collect(), tuple(cols))

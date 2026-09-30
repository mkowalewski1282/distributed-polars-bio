"""Wyrocznia dla danych w układzie databio-8p: polars-bio lokalnie, wynik
w schemacie znormalizowanym (bench/ops.py, specyfikacja 8.4).

Uzupełnia wyrocznie z tests/*_oracle.py, które działają na zbiorze pięciu
przedziałów z kolumną `name`; ta działa na dowolnych plikach Parquet.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import polars_bio as pb

from bench.data.datasets import COLUMNS
from bench.ops import normalize_polars_bio


def _reset_pb_context() -> None:
    """polars-bio trzyma globalny kontekst z tabelami s1/s2 — jak w pozostałych wyroczniach."""
    from polars_bio.context import ctx as _pb_ctx

    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def read_intervals(path: Path) -> pl.DataFrame:
    """Plik Parquet albo katalog plików Parquet -> DataFrame oznaczony jako 0-based."""
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
    """Wynik polars-bio dla operacji, w schemacie znormalizowanym."""
    c = list(cols)
    _reset_pb_context()
    if op == "merge":
        result = pb.merge(left, cols=c)
    elif op in ("overlap", "nearest", "coverage", "subtract"):
        result = getattr(pb, op)(left, right, cols1=c, cols2=c)
    else:
        raise ValueError(f"nieznana operacja {op!r}")
    return normalize_polars_bio(op, result.collect(), tuple(cols))

"""Golden oracle dla operacji nearest — analogicznie do overlap_oracle.py."""

from __future__ import annotations

import polars as pl
import polars_bio as pb

SCHEMA = ["chrom", "start", "end", "name"]


def _reset_pb_context() -> None:
    from polars_bio.context import ctx as _pb_ctx
    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def reference_nearest_pairs(
    intervals_a: list[tuple[str, int, int, str]],
    intervals_b: list[tuple[str, int, int, str]],
) -> set[tuple[str, str]]:
    """Golden oracle: lokalny pb.nearest(), znormalizowany do zbioru par (name_a, name_b)."""
    if not intervals_a or not intervals_b:
        return set()

    df_a = pl.DataFrame(intervals_a, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(intervals_b, schema=SCHEMA, orient="row")
    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)

    _reset_pb_context()
    result = pb.nearest(df_a, df_b).collect()

    if result is None or result.height == 0:
        return set()

    return set(zip(result["name_1"].to_list(), result["name_2"].to_list()))

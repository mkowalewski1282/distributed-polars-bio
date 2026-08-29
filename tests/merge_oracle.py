"""Golden oracle dla operacji merge — analogicznie do overlap_oracle.py."""

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


def reference_merge_intervals(
    intervals: list[tuple[str, int, int, str]],
) -> set[tuple[str, int, int]]:
    """Golden oracle: lokalny pb.merge(), znormalizowany do zbioru (chrom, start, end)."""
    if not intervals:
        return set()

    df = pl.DataFrame(intervals, schema=SCHEMA, orient="row")
    df.config_meta.set(coordinate_system_zero_based=True)

    _reset_pb_context()
    result = pb.merge(df).collect()

    if result is None or result.height == 0:
        return set()

    cols = result.columns
    chrom_col = "chrom" if "chrom" in cols else cols[0]
    start_col = "start" if "start" in cols else cols[1]
    end_col = "end" if "end" in cols else cols[2]

    return set(
        zip(
            result[chrom_col].to_list(),
            result[start_col].to_list(),
            result[end_col].to_list(),
        )
    )

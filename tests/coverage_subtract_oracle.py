"""Golden oracle dla operacji coverage i subtract."""

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


def _make_dfs(a, b):
    df_a = pl.DataFrame(a, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(b, schema=SCHEMA, orient="row")
    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)
    return df_a, df_b


def reference_coverage(
    reads: list[tuple[str, int, int, str]],
    targets: list[tuple[str, int, int, str]],
) -> set[tuple[str, int, int, int]]:
    """(chrom, start, end, coverage) dla każdego targetu."""
    if not reads or not targets:
        return set()

    df_reads, df_targets = _make_dfs(reads, targets)
    _reset_pb_context()
    result = pb.coverage(df_reads, df_targets).collect()
    if result is None or result.height == 0:
        return set()

    cov_col = next(c for c in result.columns if "coverage" in c.lower())
    return set(
        zip(
            result["chrom"].to_list(),
            result["start"].to_list(),
            result["end"].to_list(),
            result[cov_col].to_list(),
        )
    )


def reference_subtract(
    left: list[tuple[str, int, int, str]],
    right: list[tuple[str, int, int, str]],
) -> set[tuple[str, int, int]]:
    """(chrom, start, end) dla fragmentów left minus right."""
    if not left:
        return set()

    df_left, df_right = _make_dfs(left, right)
    _reset_pb_context()
    result = pb.subtract(df_left, df_right).collect()
    if result is None or result.height == 0:
        return set()

    return set(
        zip(result["chrom"].to_list(), result["start"].to_list(), result["end"].to_list())
    )

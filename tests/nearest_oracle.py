"""Golden oracle for the nearest operation, analogous to overlap_oracle.py."""

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
    """Golden oracle: local pb.nearest(), normalized to a set of (name_a, name_b) pairs.

    NOTE: when interval A has several candidates at the same distance (e.g. zero),
    pb.nearest() and the native nearest() from datafusion-bio-function-ranges
    MAY pick different candidates (the tie-breaking rules are not documented; on
    the test data, with the correct orientation, they pick the same ones, see
    test_ballista_and_pb_nearest_pick_same_neighbours). Both answers would then
    be correct as far as the distance goes, but this set of pairs is NOT suitable
    for a 1:1 comparison between engines when there are ties. Use
    reference_nearest_min_distances() below for that: compare DISTANCES
    (name_a -> distance), not the specific partners, whenever the data may
    contain ties (e.g. an interval overlapping more than one candidate).
    """
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


def reference_nearest_min_distances(
    intervals_a: list[tuple[str, int, int, str]],
    intervals_b: list[tuple[str, int, int, str]],
) -> dict[str, int]:
    """Tie-robust golden oracle: name_a -> distance to the nearest candidate
    according to pb.nearest() (not the specific candidate that was CHOSEN, whose
    tie-breaking may differ between engines)."""
    if not intervals_a or not intervals_b:
        return {}

    df_a = pl.DataFrame(intervals_a, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(intervals_b, schema=SCHEMA, orient="row")
    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)

    _reset_pb_context()
    result = pb.nearest(df_a, df_b).collect()

    if result is None or result.height == 0:
        return {}

    dist_col = next((c for c in result.columns if "distance" in c.lower()), None)
    if dist_col is None:
        raise RuntimeError(f"pb.nearest() has no distance column: {result.columns}")

    return dict(zip(result["name_1"].to_list(), result[dist_col].to_list()))

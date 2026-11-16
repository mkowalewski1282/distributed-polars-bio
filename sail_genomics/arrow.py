"""polars-bio operations in Sail through applyInArrow — the measurement path.

The data is grouped by chromosome: groupBy (merge) or a cogroup of two tables (the other
operations), and the Python function receives an Arrow table of one chromosome and returns an
Arrow table with the result. collect_list, LATERAL and explode are gone, and the result passes
through Arrow without being converted to Python rows — a probe measured it 10–20× faster than a
UDTF with LATERAL on the pair 1-2, with a result matching polars-bio for all five operations.

Access to polars-bio in the Sail server process: a polars-bio context from a pool, one per
concurrent call (sail_genomics.pb_context), without a lock — chromosomes are computed in
parallel.

A chromosome present only on the right side (empty left) gives an empty result without calling
polars-bio; present only on the left — polars-bio gets an empty right side and decides the
semantics itself.

Phases: when a phase file is given, every polars-bio call appends a JSON line {"pb_s": …} to it
— the duration of the call; the runner sums them after the query (read_phases). The file lives
on the same machine as the server (a local cluster); appending short lines in append mode is
safe between threads.

The package must be importable by name in the server process (working directory = the
repository): cloudpickle serializes the function passed to applyInArrow, and that function
imports sail_genomics.arrow."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pyarrow as pa

from sail_genomics.schema import (
    DEFAULT_INPUT_COLUMNS,
    OUTPUT_COLUMNS,
    UNARY_OPS,
    arrow_schema,
    ddl_schema,
    rename_polars_bio,
)

#: Names of the interval columns under which the data reaches polars-bio.
_PB_COLS = ("chrom", "start", "end")


def _frame(table: pa.Table):
    """The group's Arrow table → polars (zero-copy) with 0-based coordinate metadata for polars-bio."""
    import polars as pl
    from polars_bio._metadata import set_coordinate_system

    df = pl.from_arrow(table).select(
        pl.col("chrom").cast(pl.Utf8), pl.col("start").cast(pl.Int64), pl.col("end").cast(pl.Int64)
    )
    set_coordinate_system(df, True)
    return df


def _call_polars_bio(op: str, left, right):
    import polars_bio as pb

    cols = list(_PB_COLS)
    if op in UNARY_OPS:
        return pb.merge(left, cols=cols, output_type="polars.DataFrame")
    return getattr(pb, op)(left, right, cols1=cols, cols2=cols, output_type="polars.DataFrame")


def evaluate(op: str, left: pa.Table, right: pa.Table | None, phase_log: str | None) -> pa.Table:
    """Body of the applyInArrow function: one polars-bio operation on one chromosome."""
    schema = arrow_schema(op)
    if left.num_rows == 0:
        return schema.empty_table()  # every operation returns rows derived from the left table
    left_df = _frame(left)
    right_df = None if right is None else _frame(right)
    from sail_genomics import pb_context  # polars-bio is imported only in the server process

    pb_context.install()  # idempotent: a pool of contexts in the server process

    t = time.perf_counter()
    result = _call_polars_bio(op, left_df, right_df)
    pb_s = time.perf_counter() - t
    if phase_log:
        with open(phase_log, "a", encoding="utf-8") as f:
            f.write(json.dumps({"pb_s": pb_s}) + "\n")
    if result.height == 0:
        return schema.empty_table()
    return rename_polars_bio(op, result, _PB_COLS).to_arrow().cast(schema)


def build_query(spark, op: str, left, right=None, cols: tuple[str, str, str] = DEFAULT_INPUT_COLUMNS, *,
                phase_log: str | None = None):
    """A query (a lazy DataFrame) on Parquet files (a file or a directory) with the result in the
    normalised schema (schema.OUTPUT_COLUMNS)."""
    if op not in OUTPUT_COLUMNS:
        raise ValueError(f"unknown operation {op!r}")
    if (op in UNARY_OPS) != (right is None):
        raise ValueError(f"{op} requires {'one table' if op in UNARY_OPS else 'two tables'}")
    from pyspark.sql import functions as F

    c, s, e = cols

    def load(path):
        return spark.read.parquet(str(path)).select(
            F.col(c).cast("string").alias("chrom"),
            F.col(s).cast("long").alias("start"),
            F.col(e).cast("long").alias("end"),
        )

    schema = ddl_schema(op)
    if op in UNARY_OPS:
        def unary(table):
            from sail_genomics import arrow

            return arrow.evaluate(op, table, None, phase_log)

        return load(left).groupBy("chrom").applyInArrow(unary, schema)

    def binary(left_table, right_table):
        from sail_genomics import arrow

        return arrow.evaluate(op, left_table, right_table, phase_log)

    return load(left).groupBy("chrom").cogroup(load(right).groupBy("chrom")).applyInArrow(binary, schema)


def read_phases(path: str | Path | None) -> tuple[float, int]:
    """The sum of polars-bio times and the number of calls from the phase file.
    A missing or empty file (no group called polars-bio) → zeros."""
    if path is None:
        return 0.0, 0
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return 0.0, 0
    records = [json.loads(line) for line in lines if line.strip()]
    return sum(r["pb_s"] for r in records), len(records)

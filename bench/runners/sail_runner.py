"""Sail runner: a Spark Connect client connecting to the block's server
(bench/runners/sail_server.py); one scenario in a fresh process.

Always applyInArrow with pooled polars-bio contexts (sail_genomics.arrow, the `sail` variant).
The function records phases from the inside (the time of the polars-bio calls) to a temporary
file, which the runner sums after the query: `phases.pb_s`, `extra.udf_groups`.

Time: from building the query (reading Parquet, grouping, applyInArrow) to the last result batch;
excluding session startup and warm-up. The first RPC of a session is always `warm_up` (before
the clock) — that is when Sail starts the session's driver and workers, so the startup of the
session's "cluster" is also before the measurement.

The result is received as Arrow tables through `client.to_table_as_iterator` — the same thing
the public toLocalIterator uses, but without converting to Row objects — and kept in memory;
the clock stops after the last batch and the checksum is computed afterwards (its time:
`extra.checksum_s`). `phases.first_batch_s`: time to the first batch. Peak memory: the client
process (with the held result), read before the checksum; the server's peak is measured by the
orchestrator."""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

from bench.checksum import Checksum
from bench.metrics import peak_rss, reset_peak_rss
from bench.runners.common import main_guard, parse_scenario, report_line, require_paths, scenario_parser

PROG = "sail_runner"


def iter_arrow(spark, df):
    """The query result as consecutive Arrow tables (one per batch from the server)."""
    import pyarrow as pa

    client = spark.client
    for item in client.to_table_as_iterator(df._plan.to_proto(client), df._plan.observations):
        if isinstance(item, pa.Table):
            yield item


def warm_up(spark) -> None:
    """The session's first query, before the clock: Sail starts the session's driver and workers
    at the first RPC (probe of 2026-10-02)."""
    spark.range(1).count()


def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], remote: str) -> str:
    require_paths(left, right)
    from pyspark.sql import SparkSession

    from sail_genomics import arrow as sail_arrow

    fd, phase_log = tempfile.mkstemp(prefix="sail_phases_", suffix=".jsonl")
    os.close(fd)
    try:
        spark = SparkSession.builder.remote(remote).create()
        try:
            warm_up(spark)
            reset_peak_rss()
            t0 = time.perf_counter()
            df = sail_arrow.build_query(spark, op, left, right, cols, phase_log=phase_log)
            tables = []
            first_batch_s = None
            for table in iter_arrow(spark, df):
                tables.append(table)
                if first_batch_s is None:
                    first_batch_s = time.perf_counter() - t0
            t_total_s = time.perf_counter() - t0
            peak_rss_bytes = peak_rss()  # before the checksum: its buffers do not inflate the peak
        finally:
            spark.stop()
        t = time.perf_counter()
        checksum = Checksum(op)
        checksum.update_all(tables)
        checksum_s = time.perf_counter() - t
        phases = {"first_batch_s": t_total_s if first_batch_s is None else first_batch_s}
        extra = {"checksum_s": checksum_s}
        pb_s, calls = sail_arrow.read_phases(phase_log)
        phases["pb_s"] = pb_s
        extra["udf_groups"] = calls
    finally:
        os.unlink(phase_log)
    return report_line(
        rows=checksum.rows, checksum=checksum.hex(), t_total_s=t_total_s, peak_rss_bytes=peak_rss_bytes,
        phases=phases, extra=extra,
    )


def main(argv: list[str] | None = None) -> int:
    parser = scenario_parser(PROG, "One scenario in Sail (external server); result: one JSON line.")
    parser.add_argument("--remote", required=True, help="server address, e.g. sc://127.0.0.1:50051")
    args = parse_scenario(parser, argv)
    return main_guard(PROG, lambda: run(args.op, args.left, args.right, args.cols, args.remote))


if __name__ == "__main__":
    sys.exit(main())

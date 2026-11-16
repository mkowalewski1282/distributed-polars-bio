"""polars-bio runner: one scenario in a fresh process.

polars-bio reads the Parquet files itself — the path is given as a `directory/*.parquet`
pattern (a bare directory is not accepted) — and returns the result as a
`datafusion.DataFrame`, which is consumed as a stream (`execute_stream`).

Time: from calling the operation to receiving the last batch; excluding process startup,
imports and settings. The batches are kept in memory and the checksum is computed AFTER the
clock stops — its time is reported for information in `extra.checksum_s`. Peak memory is read
right after the clock stops, before the checksum (it includes the held result, without the
checksum buffers).
`phases.first_batch_s`: time to the first batch (no batches → equal to t_total_s).
Threads: `--threads T` → `target_partitions = T` (pb.POLARS_BIO_MAX_THREADS; by default
polars-bio computes on 1 partition). The orchestrator also sets POLARS_MAX_THREADS = T and
pins the process (taskset) to the CPUs chosen for parallelism P.

NOTE (probe of 2026-10-01): with target_partitions > 1, polars-bio 0.28 computes merge and
subtract separately in each partition — the result is wrong when a chromosome's intervals lie
in different files (the known polars-bio bug #372, fixed in 0.29+, which requires Python
≥ 3.11). The series' reference result is therefore computed on 1 partition, and the checksum
detects the invalidity of such runs.
"""

from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

from bench.checksum import Checksum
from bench.metrics import peak_rss, reset_peak_rss
from bench.ops import UNARY_OPS, normalize_arrow
from bench.runners.common import main_guard, parse_scenario, report_line, require_paths, scenario_parser

PROG = "polars_bio_runner"


def source(path: Path) -> str:
    """A directory of Parquet files → the pattern `directory/*.parquet`; a file → unchanged."""
    return str(path / "*.parquet") if path.is_dir() else str(path)


def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], threads: int) -> str:
    require_paths(left, right)
    import polars_bio as pb

    # Paths carry no coordinate-system metadata (in 0.37 only scan_*/read_* accept zero_based, and
    # those do not return a datafusion.DataFrame), so the warning is silenced; the databio-8p data is 0-based.
    warnings.filterwarnings("ignore", message="Coordinate system metadata is missing")
    pb.set_option("datafusion.bio.coordinate_system_zero_based", True)
    pb.set_option(pb.POLARS_BIO_MAX_THREADS, threads)
    reset_peak_rss()
    t0 = time.perf_counter()
    if op in UNARY_OPS:
        df = pb.merge(source(left), cols=list(cols), output_type="datafusion.DataFrame")
    else:
        df = getattr(pb, op)(
            source(left), source(right), cols1=list(cols), cols2=list(cols),
            output_type="datafusion.DataFrame",
        )
    batches = []
    first_batch_s = None
    for batch in df.execute_stream():
        batches.append(batch.to_pyarrow())
        if first_batch_s is None:
            first_batch_s = time.perf_counter() - t0
    t_total_s = time.perf_counter() - t0
    peak_rss_bytes = peak_rss()  # before the checksum: its buffers do not inflate the peak
    t = time.perf_counter()
    checksum = Checksum(op)
    checksum.update_all(normalize_arrow(op, b, cols) for b in batches)
    checksum_s = time.perf_counter() - t
    return report_line(
        rows=checksum.rows,
        checksum=checksum.hex(),
        t_total_s=t_total_s,
        peak_rss_bytes=peak_rss_bytes,
        phases={"first_batch_s": t_total_s if first_batch_s is None else first_batch_s},
        extra={
            "target_partitions": int(pb.get_option(pb.POLARS_BIO_MAX_THREADS)),
            "checksum_s": checksum_s,
        },
    )


def main(argv: list[str] | None = None) -> int:
    parser = scenario_parser(PROG, "One scenario in polars-bio; result: one JSON line.")
    parser.add_argument("--threads", type=int, required=True, help="target_partitions polars-bio (≥ 1)")
    args = parse_scenario(parser, argv)
    if args.threads < 1:
        parser.error("--threads: expected an integer ≥ 1")
    return main_guard(PROG, lambda: run(args.op, args.left, args.right, args.cols, args.threads))


if __name__ == "__main__":
    sys.exit(main())

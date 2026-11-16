"""Sail server for a measurement series: a separate process for the duration of a block; the
client runners (bench/runners/sail_runner.py) connect through Spark Connect.

The orchestrator sets the resources: pinning to P CPUs from the layout (taskset; bench/cpus.py)
and the SAIL_* variables from `sail_env`. polars-bio is imported before startup so that the
first function call does not pay for the import. The process ends on SIGTERM. The working
directory must be the repository root — the applyInArrow functions from sail_genomics import
modules by name in this process."""

from __future__ import annotations

import argparse
import sys
import time

#: Task slots per worker: P workers × 1 slot, with the parallelism equal to the worker count P.
#: Probe of 2026-11-16 (Sail 0.7.3, overlap 7-0, P = 1, 2, 4 workers × 1 slot with parallelism P):
#: a correct checksum, a worker count equal to P and no hang in every cell, so 1 slot per worker
#: is honoured. An earlier probe of 2026-10-01 (1 worker, 2 slots) made Sail add workers, and
#: with cluster.worker_max_count the query hung.
WORKER_TASK_SLOTS = 1


def sail_env(parallelism: int) -> dict[str, str]:
    """SAIL_* variables of the server for parallelism P: local-cluster, P workers of 1 slot each
    without idle shutdown, execution parallelism P.

    Sail creates a driver and P workers for EVERY Spark Connect session — at the session's first
    RPC (in the runner: the session warm-up, before the time measurement) — and closes them with
    the session (probe of 2026-10-02). Every run therefore has its own fresh pool of workers."""
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": str(parallelism),
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": str(WORKER_TASK_SLOTS),
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "86400",
        # With 1 slot per worker the parallelism must equal the worker count, otherwise Sail
        # adds workers (probe on Sail 0.7.3, 2026-11-16).
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(parallelism),
        # Execution batch size (probe: overlap 7-0, 3 workers): 273 result batches instead of
        # 1877, the client receiving the result in 1.59 s instead of 2.79 s, the same checksum.
        # The Spark Connect options (arrow.maxRecordsPerBatch, maxRecordsPerOutputBatch,
        # connect.grpc.arrow.maxBatchSize) did not change the number of batches.
        "SAIL_EXECUTION__BATCH_SIZE": "65536",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sail_server", description="Spark Connect server (Sail) on 127.0.0.1."
    )
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args(argv)
    import polars_bio  # noqa: F401 — before startup; MPLBACKEND is set by bench/__init__.py
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer("127.0.0.1", args.port)
    server.start(background=True)
    print(f"sail_server: listening on 127.0.0.1:{args.port}", flush=True)
    while server.running:
        time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())

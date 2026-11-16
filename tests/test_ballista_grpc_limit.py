"""Broadcast guard: when payloads carrying the broadcast table do not fit in the gRPC message
limit, the query ends with a readable error instead of hanging until the timeout, as Ballista
does in push mode (53 and, as verified, 55: it treats the executor as lost and retries).

The broadcast table in coverage is the right side (df2): 1.2 million intervals are about 23 MiB in
Arrow IPC (string + int32; about 37 MiB after reading in Ballista as Utf8View/Int64), i.e. above
Ballista's default 16 MiB even without a safety margin of 2. The positive test therefore shows that
the raised 512 MiB limit reaches the transport. Requires the debug binaries (missing = ERROR)."""

from __future__ import annotations

import subprocess
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from bench import engines
from bench.cpus import cpus_for, read_layout
from tests.parquet_fixture import FIXTURE_A, write_parts

BIG_ROWS = 1_200_000


@pytest.fixture(scope="module")
def pair(tmp_path_factory):
    root = tmp_path_factory.mktemp("big_broadcast")
    left = write_parts(FIXTURE_A, root / "a")
    right = root / "b"
    right.mkdir()
    starts = pa.array(range(0, 10 * BIG_ROWS, 10), pa.int32())
    pq.write_table(
        pa.table({"contig": pa.array(["chr1"] * BIG_ROWS, pa.string()), "pos_start": starts,
                  "pos_end": pc.add(starts, 5)}),
        right / "part-00000.parquet",
    )
    return left, right


def run_coverage(engine, left, right, timeout: int = 180):
    argv = [str(engines.ballista_binary("bench_client", "debug")), "--op", "coverage",
            "--left", str(left), "--right", str(right)]
    env = {**engine.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{engine.port}"}
    t0 = time.monotonic()
    r = subprocess.run(argv, cwd=engines.BALLISTA_DIR, env=env, capture_output=True, text=True, timeout=timeout)
    return r, time.monotonic() - t0


def cluster(tmp_path, limit: int):
    layout = read_layout()
    return engines.BallistaEngine(
        1, cpus_for(layout, 1), system=layout.system, profile="debug", data_root=tmp_path,
        log_dir=tmp_path / "logs", grpc_max_message_bytes=limit,
    )


@pytest.mark.parallelism(1)
def test_broadcast_over_the_limit_fails_fast_with_a_clear_error(pair, tmp_path):
    engine = cluster(tmp_path, 16 * 2**20)
    engine.start()
    try:
        r, elapsed = run_coverage(engine, *pair)
    finally:
        engine.stop()
    assert r.returncode == 1, f"stdout: {r.stdout}\nstderr: {r.stderr[-3000:]}"
    assert "exceeds the gRPC message limit" in r.stderr and "BALLISTA_GRPC_MAX_MESSAGE_BYTES" in r.stderr
    assert elapsed < 60


@pytest.mark.parallelism(1)
def test_same_broadcast_fits_under_the_default_limit(pair, tmp_path):
    engine = cluster(tmp_path, engines.GRPC_MAX_MESSAGE_BYTES)
    engine.start()
    try:
        r, _ = run_coverage(engine, *pair)
    finally:
        engine.stop()
    assert r.returncode == 0, r.stderr[-3000:]
    assert '"rows": 12' in r.stdout  # coverage: one row per interval of the left table (FIXTURE_A)

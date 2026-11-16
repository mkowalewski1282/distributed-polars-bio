"""broadcast_bytes: executors log the size of every received broadcast payload, and the engine
sums those entries over the run, giving the bytes that actually arrived: payload x number of
stage tasks with a broadcast (Ballista 55 encodes the plan separately for each task; Ballista 53
sent the stage plan once per executor). Requires the debug binaries."""

from __future__ import annotations

import subprocess

import pytest

from bench import engines
from bench.cpus import read_layout
from bench.ops import OPS, UNARY_OPS


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    """Two executors with 1 slot each (P = 2): the broadcast must reach tasks on both."""
    tmp = tmp_path_factory.mktemp("broadcast_bytes")
    engine = engines.make_engine(
        "ballista", 2, layout=read_layout(), data_root=tmp, log_dir=tmp / "logs", profile="debug",
    )
    engine.start()
    yield engine
    engine.stop()


@pytest.mark.parallelism(2)
@pytest.mark.parametrize("op", OPS)
def test_broadcast_bytes_only_for_broadcast_operations(op, cluster, parquet_dirs):
    left, right = parquet_dirs
    argv = [str(engines.ballista_binary("bench_client", "debug")), "--op", op, "--left", str(left)]
    if op not in UNARY_OPS:
        argv += ["--right", str(right)]
    env = {**cluster.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{cluster.port}"}
    mark = cluster.log_mark()
    r = subprocess.run(argv, cwd=engines.BALLISTA_DIR, env=env, capture_output=True, text=True, timeout=180)
    assert r.returncode == 0, r.stderr[-3000:]
    got = cluster.log_metrics(mark)["broadcast_bytes"]
    if op in ("nearest", "coverage"):
        assert got > 0
    else:
        assert got == 0

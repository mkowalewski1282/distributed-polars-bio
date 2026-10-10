"""broadcast_bytes (plan 3b-2, Zadanie 15; specyfikacja 6.3): executory logują rozmiar każdego
odebranego ładunku broadcastu, a silnik sumuje te wpisy z czasu przebiegu — bajty, które
faktycznie dotarły (ładunek × liczba zadań). Wymaga binarek debug."""

from __future__ import annotations

import subprocess

import pytest

from bench import engines
from bench.config import Scenario
from bench.ops import OPS, UNARY_OPS


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("broadcast_bytes")
    engine = engines.BallistaEngine(2, profile="debug", data_root=tmp, log_dir=tmp / "logs")
    engine.start()
    yield engine
    engine.stop()


@pytest.mark.nodes(2)
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

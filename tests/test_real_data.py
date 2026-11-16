"""Acceptance test on the real databio-8p data.

Pair 1-2 (fBrain-DS14718 x exons; merge: dataset 1), the smallest pair of the local
matrix, in three execution variants: Ballista standalone, Ballista on a cluster of
separate processes, and Sail. Each result is compared with polars-bio on the same
files. No timing measurements.

This is an acceptance test of paths that are already implemented, so it is expected
to pass right away; the tests of the individual paths on the same `row_multiset` show
that this comparison can fail.

Requires: python -m bench.data.download (the data lives outside the repository) and the
bench_client and ballista_node binaries. Missing data = skip, missing binaries = error.
Run: pytest tests/test_real_data.py -v
"""

from __future__ import annotations

import os
import subprocess

import pytest

from bench.data.datasets import DATASETS, data_dir, resolve
from bench.data.download import expected_rows, verify
from bench.ops import OPS, UNARY_OPS, describe_diff, row_multiset
from tests.test_ballista_multiprocess import _start_cluster
from tests.test_ballista_parquet import check_result, op_args, run_client

PAIR = "1-2"

#: Ballista 53 standalone mode (scheduler and executor in one process, pull mode) cannot
#: handle operations with a broadcast on real data: the executor fetches tasks with a gRPC
#: client with tonic's default 4 MiB limit (not changeable through standalone_with_state), and a
#: response with several tasks, each carrying the whole broadcast table, is larger (9 808 588 B
#: measured already at 100 thousand broadcast rows). The executor retries the fetch forever,
#: so the query hangs instead of ending with an error. The measurements use a cluster of
#: separate processes (push mode, 16 MiB limit): test_ballista_cluster_on_1_2. `strict`: when
#: standalone starts working, the test reports it.
STANDALONE_HANG_TIMEOUT_S = 90
_STANDALONE_BROADCAST_HANGS = pytest.mark.xfail(
    raises=subprocess.TimeoutExpired,
    strict=True,
    reason="standalone: 4 MiB gRPC client limit of the executor with a broadcast",
)
STANDALONE_CASES = [
    pytest.param(op, marks=_STANDALONE_BROADCAST_HANGS) if op in ("nearest", "coverage") else op
    for op in OPS
]
_NEEDED = {DATASETS[1], DATASETS[2]}
_PROBLEMS = verify(data_dir(), {n: r for n, r in expected_rows().items() if n in _NEEDED})

pytestmark = [
    pytest.mark.real_data,
    pytest.mark.skipif(
        bool(_PROBLEMS), reason=f"incomplete data for pair {PAIR} in {data_dir()}: {_PROBLEMS}"
    ),
]


@pytest.fixture(scope="module")
def real_dirs():
    return resolve(PAIR)


@pytest.fixture(scope="module")
def real_expected(real_dirs):
    from tests.generic_oracle import read_intervals, reference

    left, right = real_dirs
    a, b = read_intervals(left), read_intervals(right)
    return {
        op: row_multiset(op, reference(op, a, None if op in UNARY_OPS else b)) for op in OPS
    }


@pytest.mark.parametrize("op", STANDALONE_CASES)
def test_ballista_standalone_on_1_2(op, real_dirs, real_expected, tmp_path):
    out = tmp_path / f"{op}.parquet"
    timeout = STANDALONE_HANG_TIMEOUT_S if op in ("nearest", "coverage") else 900
    r = run_client(op_args(op, *real_dirs, out), timeout=timeout)
    check_result(op, r, out, real_expected[op])


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    c = _start_cluster(
        tmp_path_factory.mktemp("real_data_cluster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


@pytest.mark.parametrize("op", OPS)
def test_ballista_cluster_on_1_2(op, cluster, real_dirs, real_expected, tmp_path):
    out = tmp_path / f"{op}.parquet"
    env = {**os.environ, "BALLISTA_SCHEDULER_URL": cluster.url}
    r = run_client(op_args(op, *real_dirs, out), env=env, timeout=900)
    check_result(op, r, out, real_expected[op])


@pytest.fixture(scope="module")
def spark():
    from sail_genomics import session as sail_session

    with sail_session.local_session() as session:
        yield session


@pytest.mark.parametrize("op", OPS)
def test_sail_on_1_2(op, spark, real_dirs, real_expected):
    import polars as pl

    from sail_genomics import arrow as sail_arrow

    left, right = real_dirs
    df = sail_arrow.build_query(spark, op, left, None if op in UNARY_OPS else right)
    actual = row_multiset(op, pl.from_arrow(df.toArrow()))
    assert actual == real_expected[op], f"{op}: {describe_diff(real_expected[op], actual)}"

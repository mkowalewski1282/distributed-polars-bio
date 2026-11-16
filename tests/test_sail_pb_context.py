"""A pool of polars-bio contexts: a bridging solution.

Replacing polars_bio.range_op.range_operation (sail_genomics.pb_context) must give results identical
to the public pb.*(), also for concurrent calls from threads without a lock. The test also guards
polars-bio internals: when a new version changes how range_operation is called, the results diverge.

Without the replacement, concurrent calls fail with "The table s1 already exists" (seen in a probe);
we do not test that directly, because a race on small data can be invisible."""

from __future__ import annotations

import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import polars_bio as pb
import polars_bio.range_op as range_op
import pytest
from polars_bio._metadata import set_coordinate_system

from bench.ops import OPS, UNARY_OPS
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B

REPO = Path(__file__).resolve().parent.parent
COLS = ["chrom", "start", "end"]
SCHEMA = {"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64}


def _zero_based(df: pl.DataFrame) -> pl.DataFrame:
    set_coordinate_system(df, True)
    return df


def _by_chrom(rows) -> dict[str, pl.DataFrame]:
    df = pl.DataFrame(rows, schema=SCHEMA, orient="row")
    return {c: _zero_based(df.filter(pl.col("chrom") == c)) for c in sorted(set(df["chrom"]))}


LEFT, RIGHT = _by_chrom(FIXTURE_A), _by_chrom(FIXTURE_B)


def _call(op: str, chrom: str) -> pl.DataFrame:
    """One polars-bio operation on one chromosome, like a Sail function on one group."""
    left = LEFT[chrom]
    right = RIGHT[chrom] if chrom in RIGHT else _zero_based(pl.DataFrame(schema=SCHEMA))
    if op in UNARY_OPS:
        result = pb.merge(left, cols=COLS, output_type="polars.DataFrame")
    else:
        result = getattr(pb, op)(left, right, cols1=COLS, cols2=COLS, output_type="polars.DataFrame")
    return result.sort(result.columns)


@pytest.fixture
def bridge():
    """The bridge module with the replacement DISABLED; a test enables it explicitly. After the test the
    original is restored, so that the remaining tests in this process compute on the global polars-bio context."""
    from sail_genomics import pb_context as sail_pb_context

    sail_pb_context.uninstall()  # just in case: start without the replacement
    yield sail_pb_context
    sail_pb_context.uninstall()


@pytest.mark.parametrize("op", OPS)
def test_threads_without_lock_match_plain_polars_bio(op, bridge):
    expected = {chrom: _call(op, chrom) for chrom in LEFT}
    bridge.install()
    jobs = list(LEFT) * 6
    before = bridge.contexts_created()
    for _ in range(3):
        with ThreadPoolExecutor(6) as pool:
            results = list(pool.map(lambda chrom: (chrom, _call(op, chrom)), jobs))
        mismatched = sorted({chrom for chrom, got in results if not got.equals(expected[chrom])})
        assert not mismatched, f"{op}: results differ for {mismatched}"
    # Never more contexts than concurrent calls (6 worker threads).
    assert bridge.contexts_created() - before <= 6


def test_install_is_idempotent_and_uninstall_restores_polars_bio(bridge):
    original = range_op.range_operation
    bridge.install()
    bridge.install()
    assert bridge.installed() and range_op.range_operation is bridge._range_operation
    bridge.uninstall()
    assert not bridge.installed() and range_op.range_operation is original is bridge._original


def test_sequential_calls_from_new_threads_reuse_one_context(bridge):
    """As in Sail: successive calls, each from a new, short-lived thread; the pool must create
    at most one context (a per-thread implementation would create 10)."""
    chrom = next(iter(LEFT))
    expected = _call("overlap", chrom)
    bridge.install()
    before = bridge.contexts_created()
    for _ in range(10):
        out = []
        worker = threading.Thread(target=lambda: out.append(_call("overlap", chrom)))
        worker.start()
        worker.join()
        assert out[0].equals(expected)
    assert bridge.contexts_created() - before <= 1


def test_context_after_failed_call_does_not_return_to_pool(bridge, monkeypatch):
    """A failed call may leave s1/s2 registered; its context must be discarded."""
    chrom = next(iter(LEFT))
    expected = _call("overlap", chrom)
    bridge.install()
    while True:  # empty pool: the failed call's context is then the only candidate
        try:
            bridge._pool.get_nowait()
        except Exception:
            break

    def failing(*args, **kwargs):
        raise RuntimeError("simulated polars-bio failure")

    good = bridge._original
    monkeypatch.setattr(bridge, "_original", failing)
    before = bridge.contexts_created()
    with pytest.raises(RuntimeError):
        _call("overlap", chrom)
    assert bridge.contexts_created() - before == 1
    assert bridge._pool.qsize() == 0
    monkeypatch.setattr(bridge, "_original", good)
    assert _call("overlap", chrom).equals(expected)
    assert bridge.contexts_created() - before == 2


def test_install_is_explicit_and_idempotent():
    """Importing does not enable the replacement; install() enables it in a fresh process, and a second call changes nothing."""
    code = (
        "import polars_bio.range_op as r; from sail_genomics import pb_context as s; "
        "before = s.installed(); s.install(); s.install(); "
        "print(before, s.installed(), r.range_operation is s._range_operation)"
    )
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    assert r.stdout.strip() == "False True True"


def test_evaluate_installs_the_bridge():
    """Server path: evaluate() must enable the pool itself in a fresh process (without an explicit install())."""
    code = (
        "import pyarrow as pa; "
        "from sail_genomics import arrow, pb_context as s; "
        "before = s.installed(); "
        "t = pa.table({'chrom': ['chr1', 'chr1'], 'start': [1, 5], 'end': [10, 20]}); "
        "arrow.evaluate('merge', t, None, None); "
        "print(before, s.installed())"
    )
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    assert r.stdout.strip() == "False True"

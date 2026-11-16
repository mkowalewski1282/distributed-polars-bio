"""A pool of polars-bio contexts — a bridging solution.

polars-bio 0.36 keeps one global context (BioSessionContext) and registers the tables of
operations on frames in it under the fixed names s1/s2: concurrent pb.*() calls in threads of
one process fail with "The table s1 already exists" (in 0.28 — with a silent overwrite of the
tables). Sail runs Python functions in threads of the server process, so without a pool they
would have to be protected by a lock — at the cost of computing one chromosome at a time.

All five operations (overlap, nearest, coverage, merge, subtract) build RangeOptions and call
polars_bio.range_op.range_operation(df1, df2, options, output_type, ctx, …). This module
replaces that function with a version that substitutes a context taken from the pool for ctx
(created when the pool is empty, with the same options as the global one) and returns it after
a successful call (after an exception the context is discarded). Every concurrent call thus has
its own context: there are as many contexts as the peak number of simultaneous calls. The pool
assumes eager results (polars.DataFrame, as in sail_genomics.arrow): with lazy ones
(polars.LazyFrame, datafusion.DataFrame) polars-bio uses the context only at collect, after it
has returned to the pool, so a concurrent call could share it. The public pb.*() stay unchanged
— with their own default settings — and the calls stop sharing tables (a probe found 0
discrepancies, 5 operations × 6 threads × 3 rounds; overlap 7-0 2.4× faster on 6 threads).

Why not threading.local: Sail calls Python from Rust threads and recreates the Python thread
state on every call, so a "per thread" context became a per-call context (probe of 2026-10-11:
54 contexts per overlap 7-0 query, i.e. one per chromosome group). The Sail server's memory
grew by ~150 MB per query versus ~75 MB in the lock variant; with the pool the growth is the
same as in the lock variant.

This depends on polars-bio internals, not on its public API: the version is pinned in
pyproject.toml, and tests/test_sail_pb_context.py compares results with and without the
replacement — a change of the internals in a new version shows up at once. The polars-bio code
itself is not modified.

The replacement is switched on explicitly by install() (idempotent; sail_genomics.arrow.evaluate
calls it in the server process). It works only in the process that imported it — the polars-bio
runner (the reference) does not import it."""

from __future__ import annotations

import queue
import threading

import polars_bio.range_op as _range_op
from polars_bio.polars_bio import BioSessionContext

#: Options of a pooled context: as in the global polars-bio 0.36 context (polars_bio/context.py,
#: Context.__init__) plus a fallback 0-based coordinate system (the databio-8p data is 0-based,
#: half-open); the frame's coordinate metadata decides the system, the context option is only a
#: fallback.
OPTIONS = {
    "datafusion.execution.target_partitions": "1",
    "datafusion.execution.parquet.schema_force_view_types": "true",
    "datafusion.execution.skip_physical_aggregate_schema_check": "true",
    "datafusion.bio.coordinate_system_zero_based": "true",
    "datafusion.bio.coordinate_system_check": "false",
    "bio.interval_join_algorithm": "coitrees",
}

_pool: queue.SimpleQueue = queue.SimpleQueue()
_created = 0
_created_lock = threading.Lock()
_original = _range_op.range_operation


def _new_context() -> BioSessionContext:
    """A new polars-bio context with OPTIONS and a unique seed."""
    global _created
    with _created_lock:
        number = _created
        _created += 1
    ctx = BioSessionContext(seed=f"pool-{number}")
    for key, value in OPTIONS.items():
        ctx.set_option(key, value)
    return ctx


def contexts_created() -> int:
    """Number of contexts created so far in this process (diagnostics for tests)."""
    return _created


def _range_operation(*args, **kwargs):
    """polars-bio's range_operation with a pooled context instead of the global one (5th argument)."""
    try:
        ctx = _pool.get_nowait()
    except queue.Empty:
        ctx = _new_context()
    if "ctx" in kwargs:
        kwargs["ctx"] = ctx
    else:
        args = (*args[:4], ctx, *args[5:])
    result = _original(*args, **kwargs)
    # The context returns to the pool only after a successful call: a failed one may have left
    # s1/s2 registered, and reusing such a context would break later calls ("table s1 already
    # exists") until the server restarts. On an exception the context is discarded.
    _pool.put(ctx)
    return result


def install() -> None:
    _range_op.range_operation = _range_operation


def uninstall() -> None:
    _range_op.range_operation = _original


def installed() -> bool:
    return _range_op.range_operation is _range_operation


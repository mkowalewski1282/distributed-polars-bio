"""A local Sail server and a Spark Connect session — for tests and one-off scripts."""

from __future__ import annotations

from contextlib import contextmanager


@contextmanager
def local_session():
    """A Sail server (local) and a Spark Connect session; both are stopped on exit.

    `.create()`, not `.getOrCreate()` — PySpark caches the session as a process-wide singleton
    and would return a dead session for the next server.
    """
    from pyspark.sql import SparkSession
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://{ip}:{port}").create()
    try:
        yield spark
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        server.stop()

"""Lokalny serwer Sail i sesja Spark Connect — dla testów i skryptów jednorazowych."""

from __future__ import annotations

from contextlib import contextmanager


@contextmanager
def local_session():
    """Serwer Sail (local) i sesja Spark Connect; zatrzymywane przy wyjściu.

    `.create()`, nie `.getOrCreate()` — PySpark cachuje sesję jako singleton
    procesu i przy kolejnym serwerze zwróciłby martwą sesję.
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

"""Runner Saila (specyfikacja 8.1, 8.3): klient Spark Connect łączący się z serwerem bloku
(bench/runners/sail_server.py); jeden scenariusz w świeżym procesie.

Czas: od budowy zapytania (wczytanie Parquet, grupowanie, widok, SQL) do ostatniej partii
wyniku; bez startu sesji i rejestracji UDTF (odpowiednik rejestracji funkcji dist_*
w bench_client). Rejestracja to pierwsze RPC sesji — wtedy Sail uruchamia driver i workery
sesji, więc start „klastra” sesji też jest przed pomiarem.

Wynik jest odbierany jako tabele Arrow przez `client.to_table_as_iterator` — to samo, czego
używa publiczne toLocalIterator, ale bez zamiany na obiekty Row — i trzymany w pamięci; zegar
staje po ostatniej partii, a suma kontrolna jest liczona potem (specyfikacja 3b-2, 4.1; jej czas:
`extra.checksum_s`). `phases.first_batch_s`: czas do pierwszej partii. Szczyt pamięci: proces
klienta (z trzymanym wynikiem); szczyt serwera mierzy orkiestrator."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from bench.checksum import Checksum
from bench.metrics import peak_rss, reset_peak_rss
from bench.runners.common import main_guard, parse_scenario, report_line, require_paths, scenario_parser

PROG = "sail_runner"


def iter_arrow(spark, df):
    """Wynik zapytania jako kolejne tabele Arrow (po jednej na partię z serwera)."""
    import pyarrow as pa

    client = spark.client
    for item in client.to_table_as_iterator(df._plan.to_proto(client), df._plan.observations):
        if isinstance(item, pa.Table):
            yield item


def warm_up(spark) -> None:
    """Pierwsze zapytanie sesji, przed zegarem: Sail uruchamia driver i workery sesji przy
    pierwszym RPC (sonda 02.10.2026). Ścieżka Arrow nie rejestruje funkcji, a to rejestracja UDTF
    była dotąd tym RPC — rozgrzanie wyrównuje obie ścieżki."""
    spark.range(1).count()


def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], remote: str) -> str:
    require_paths(left, right)
    from pyspark.sql import SparkSession

    import sail_bio

    spark = SparkSession.builder.remote(remote).create()
    try:
        sail_bio.register(spark, op)
        warm_up(spark)
        reset_peak_rss()
        t0 = time.perf_counter()
        df = sail_bio.build_query(spark, op, left, right, cols)
        tables = []
        first_batch_s = None
        for table in iter_arrow(spark, df):
            tables.append(table)
            if first_batch_s is None:
                first_batch_s = time.perf_counter() - t0
        t_total_s = time.perf_counter() - t0
    finally:
        spark.stop()
    t = time.perf_counter()
    checksum = Checksum(op)
    checksum.update_all(tables)
    checksum_s = time.perf_counter() - t
    return report_line(
        rows=checksum.rows, checksum=checksum.hex(), t_total_s=t_total_s, peak_rss_bytes=peak_rss(),
        phases={"first_batch_s": t_total_s if first_batch_s is None else first_batch_s},
        extra={"checksum_s": checksum_s},
    )


def main(argv: list[str] | None = None) -> int:
    parser = scenario_parser(PROG, "One scenario in Sail (external server); result: one JSON line.")
    parser.add_argument("--remote", required=True, help="server address, e.g. sc://127.0.0.1:50051")
    args = parse_scenario(parser, argv)
    return main_guard(PROG, lambda: run(args.op, args.left, args.right, args.cols, args.remote))


if __name__ == "__main__":
    sys.exit(main())

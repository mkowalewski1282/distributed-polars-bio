"""Serwer Sail dla serii pomiarowej (specyfikacja 3 i 6.2): osobny proces na czas bloku;
runnery-klienci (bench/runners/sail_runner.py) łączą się przez Spark Connect.

Zasoby ustawia orkiestrator: przypięcie do P CPU z układu (taskset; bench/cpus.py) i zmienne
SAIL_* z `sail_env`. polars-bio jest importowany przed startem, żeby pierwsze wywołanie funkcji nie
płaciło za import. Proces kończy się sygnałem SIGTERM. Katalog roboczy musi być katalogiem
głównym repozytorium — funkcje applyInArrow z sail_genomics importują moduły po nazwie w tym procesie."""

from __future__ import annotations

import argparse
import sys
import time

#: Sloty zadań na workera. Sonda 2026-11-16 (Sail 0.7.3, overlap 7-0, P = 1, 2, 4 workerów × 1 slot
#: z równoległością P): poprawna suma kontrolna, liczba workerów równa P i brak zawieszenia
#: w każdej komórce, więc 1 slot na workera jest honorowany. Wcześniejsza sonda 01.10.2026
#: (1 worker, 2 sloty) dokładała workery, a z cluster.worker_max_count zapytanie wisiało.
WORKER_TASK_SLOTS = 1


def sail_env(parallelism: int) -> dict[str, str]:
    """Zmienne SAIL_* serwera dla równoległości P: local-cluster, P workerów po 1 slocie bez
    wygaszania bezczynnych, równoległość wykonania P.

    Sail tworzy driver i P workerów dla KAŻDEJ sesji Spark Connect — przy pierwszym RPC
    sesji (w runnerze: rozgrzanie sesji, przed pomiarem czasu) — i zamyka je razem z sesją
    (sonda 02.10.2026). Każdy przebieg ma więc własną, świeżą pulę workerów."""
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": str(parallelism),
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": str(WORKER_TASK_SLOTS),
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "86400",
        # Przy 1 slocie na workera równoległość musi równać się liczbie workerów, inaczej Sail
        # dokłada workery (sonda 2026-11-16).
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(parallelism),
        # Rozmiar partii wykonania (sonda 3b-2, Zadanie 12): overlap 7-0, 3 workery — 273 partii
        # wyniku zamiast 1877, odbiór wyniku przez klienta 1,59 s zamiast 2,79 s, ta sama suma
        # kontrolna. Opcje Spark Connect (arrow.maxRecordsPerBatch, maxRecordsPerOutputBatch,
        # connect.grpc.arrow.maxBatchSize) nie zmieniły liczby partii.
        "SAIL_EXECUTION__BATCH_SIZE": "65536",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sail_server", description="Spark Connect server (Sail) on 127.0.0.1."
    )
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args(argv)
    import polars_bio  # noqa: F401 — przed startem; MPLBACKEND ustawia bench/__init__.py
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer("127.0.0.1", args.port)
    server.start(background=True)
    print(f"sail_server: listening on 127.0.0.1:{args.port}", flush=True)
    while server.running:
        time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())

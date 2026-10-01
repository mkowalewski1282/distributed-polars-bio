"""Serwer Sail dla serii pomiarowej (specyfikacja 3 i 6.2): osobny proces na czas bloku;
runnery-klienci (bench/runners/sail_runner.py) łączą się przez Spark Connect.

Zasoby ustawia orkiestrator: przypięcie do rdzeni węzłów 1..N (taskset) i zmienne SAIL_*
z `sail_env`. polars-bio jest importowany przed startem, żeby pierwsze wywołanie UDTF nie
płaciło za import. Proces kończy się sygnałem SIGTERM. Katalog roboczy musi być katalogiem
głównym repozytorium — UDTF z sail_bio importuje moduły po nazwie w tym procesie."""

from __future__ import annotations

import argparse
import sys
import time

#: Sloty zadań na workera. Przy 2 slotach (jak w Ballistcie) Sail w trybie local-cluster sam
#: uruchamia DODATKOWE workery, gdy etap ma więcej zadań niż slotów (2 workery przy N = 1 na
#: danych w 8 plikach), a z limitem cluster.worker_max_count zapytanie wisi (sonda 01.10.2026).
#: 8 slotów (wartość domyślna Saila) utrzymuje dokładnie N workerów; o równoległości i tak
#: decyduje przypięcie procesu do 2N rdzeni.
WORKER_TASK_SLOTS = 8


def sail_env(n_nodes: int) -> dict[str, str]:
    """Zmienne SAIL_* serwera dla N węzłów: local-cluster, N workerów bez wygaszania
    bezczynnych, równoległość 2N.

    Sail tworzy driver i N workerów dla KAŻDEJ sesji Spark Connect — przy pierwszym RPC
    sesji (w runnerze: rejestracja UDTF, przed pomiarem czasu) — i zamyka je razem z sesją
    (sonda 02.10.2026). Każdy przebieg ma więc własną, świeżą pulę workerów."""
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": str(n_nodes),
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": str(WORKER_TASK_SLOTS),
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "86400",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(2 * n_nodes),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sail_server", description="Serwer Spark Connect (Sail) na 127.0.0.1."
    )
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args(argv)
    import polars_bio  # noqa: F401 — przed startem; MPLBACKEND ustawia bench/__init__.py
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer("127.0.0.1", args.port)
    server.start(background=True)
    print(f"sail_server: nasłuchuje na 127.0.0.1:{args.port}", flush=True)
    while server.running:
        time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())

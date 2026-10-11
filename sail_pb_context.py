"""Pula kontekstów polars-bio — rozwiązanie pomostowe (specyfikacja 3b-2, 5.1).

polars-bio 0.36 trzyma jeden globalny kontekst (BioSessionContext) i rejestruje w nim tabele
operacji na ramkach pod stałymi nazwami s1/s2: równoczesne wywołania pb.*() w wątkach jednego
procesu kończą się błędem „The table s1 already exists” (w 0.28 — cichym nadpisaniem tabel).
Sail wykonuje funkcje Pythona w wątkach procesu serwera, więc dotąd chroniła je blokada
(sail_pb_guard) — kosztem liczenia jednego chromosomu naraz.

Wszystkie pięć operacji (overlap, nearest, coverage, merge, subtract) budują RangeOptions
i wołają polars_bio.range_op.range_operation(df1, df2, options, output_type, ctx, …). Moduł
podmienia tę funkcję na wersję, która w miejsce ctx wstawia kontekst pobrany z puli (tworzony,
gdy pula jest pusta, z tymi samymi opcjami co globalny) i po wywołaniu go zwraca. Każde
równoczesne wywołanie ma więc własny kontekst: kontekstów jest tyle, ile wywołań naraz. Publiczne
pb.*() zostają bez zmian — z własnymi ustawieniami domyślnymi — a wywołania przestają dzielić
tabele (sonda 3b-2: 0 rozbieżności, 5 operacji × 6 wątków × 3 rundy; overlap 7-0 2,4× szybciej
na 6 wątkach).

Dlaczego nie threading.local: Sail woła Python z wątków Rusta i przy każdym wywołaniu odtwarza
stan wątku Pythona, więc kontekst „na wątek” stawał się kontekstem na wywołanie (sonda
11.10.2026: 54 konteksty na zapytanie overlap 7-0, czyli po jednym na grupę chromosomową).
Pamięć serwera Saila rosła o ok. 150 MB na zapytanie wobec ok. 75 MB w wariancie z blokadą;
z pulą wzrost jest taki sam jak w wariancie z blokadą.

Zależność od wnętrza polars-bio, nie od publicznego API: wersja przypięta w pyproject.toml
(polars-bio==0.36.*), a tests/test_sail_pb_context.py porównuje wyniki z podmianą i bez — zmiana
wnętrza w nowej wersji wyjdzie od razu. Kodu polars-bio nie zmieniamy (decyzja 3b-2).

Import instaluje podmianę: moduł importuje po nazwie funkcja Saila (sail_arrow) w procesie
serwera, jak sail_pb_guard. Działa tylko w procesie, który go zaimportował — runner polars-bio
(wzorzec, warianty A i B) go nie importuje."""

from __future__ import annotations

import queue
import threading

import polars_bio.range_op as _range_op
from polars_bio.polars_bio import BioSessionContext

#: Opcje kontekstu z puli: jak w globalnym kontekście polars-bio 0.36 (polars_bio/context.py,
#: Context.__init__) oraz układ 0-based — dane databio-8p są 0-based, półotwarte.
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
    """Nowy kontekst polars-bio z OPTIONS i unikalnym ziarnem."""
    global _created
    with _created_lock:
        number = _created
        _created += 1
    ctx = BioSessionContext(seed=f"pool-{number}")
    for key, value in OPTIONS.items():
        ctx.set_option(key, value)
    return ctx


def contexts_created() -> int:
    """Liczba kontekstów utworzonych dotąd w tym procesie (diagnostyka dla testów)."""
    return _created


def _range_operation(*args, **kwargs):
    """range_operation polars-bio z kontekstem z puli zamiast globalnego (5. argument)."""
    try:
        ctx = _pool.get_nowait()
    except queue.Empty:
        ctx = _new_context()
    try:
        if "ctx" in kwargs:
            kwargs["ctx"] = ctx
        else:
            args = (*args[:4], ctx, *args[5:])
        return _original(*args, **kwargs)
    finally:
        _pool.put(ctx)


def install() -> None:
    _range_op.range_operation = _range_operation


def uninstall() -> None:
    _range_op.range_operation = _original


def installed() -> bool:
    return _range_op.range_operation is _range_operation


install()

"""Osobny kontekst polars-bio dla każdego wątku — rozwiązanie pomostowe (specyfikacja 3b-2, 5.1).

polars-bio 0.36 trzyma jeden globalny kontekst (BioSessionContext) i rejestruje w nim tabele
operacji na ramkach pod stałymi nazwami s1/s2: równoczesne wywołania pb.*() w wątkach jednego
procesu kończą się błędem „The table s1 already exists” (w 0.28 — cichym nadpisaniem tabel).
Sail wykonuje funkcje Pythona w wątkach procesu serwera, więc dotąd chroniła je blokada
(sail_pb_guard) — kosztem liczenia jednego chromosomu naraz.

Wszystkie pięć operacji (overlap, nearest, coverage, merge, subtract) budują RangeOptions
i wołają polars_bio.range_op.range_operation(df1, df2, options, output_type, ctx, …). Moduł
podmienia tę funkcję na wersję, która w miejsce ctx wstawia kontekst bieżącego wątku, tworzony
przy pierwszym użyciu z tymi samymi opcjami co globalny. Publiczne pb.*() zostają bez zmian —
z własnymi ustawieniami domyślnymi — a wątki przestają dzielić tabele (sonda 3b-2: 0 rozbieżności,
5 operacji × 6 wątków × 3 rundy; overlap 7-0 2,4× szybciej na 6 wątkach).

Zależność od wnętrza polars-bio, nie od publicznego API: wersja przypięta w pyproject.toml
(polars-bio==0.36.*), a tests/test_sail_pb_context.py porównuje wyniki z podmianą i bez — zmiana
wnętrza w nowej wersji wyjdzie od razu. Kodu polars-bio nie zmieniamy (decyzja 3b-2).

Import instaluje podmianę: moduł importuje po nazwie funkcja Saila (sail_arrow) w procesie
serwera, jak sail_pb_guard. Działa tylko w procesie, który go zaimportował — runner polars-bio
(wzorzec, warianty A i B) go nie importuje."""

from __future__ import annotations

import threading

import polars_bio.range_op as _range_op
from polars_bio.polars_bio import BioSessionContext

#: Opcje kontekstu wątku: jak w globalnym kontekście polars-bio 0.36 (polars_bio/context.py,
#: Context.__init__) oraz układ 0-based — dane databio-8p są 0-based, półotwarte.
OPTIONS = {
    "datafusion.execution.target_partitions": "1",
    "datafusion.execution.parquet.schema_force_view_types": "true",
    "datafusion.execution.skip_physical_aggregate_schema_check": "true",
    "datafusion.bio.coordinate_system_zero_based": "true",
    "datafusion.bio.coordinate_system_check": "false",
    "bio.interval_join_algorithm": "coitrees",
}

_local = threading.local()
_original = _range_op.range_operation


def thread_context() -> BioSessionContext:
    """Kontekst polars-bio bieżącego wątku (tworzony przy pierwszym użyciu)."""
    ctx = getattr(_local, "ctx", None)
    if ctx is None:
        ctx = BioSessionContext(seed=f"thread-{threading.get_ident()}")
        for key, value in OPTIONS.items():
            ctx.set_option(key, value)
        _local.ctx = ctx
    return ctx


def _range_operation(*args, **kwargs):
    """range_operation polars-bio z kontekstem bieżącego wątku zamiast globalnego (5. argument)."""
    if "ctx" in kwargs:
        kwargs["ctx"] = thread_context()
    else:
        args = (*args[:4], thread_context(), *args[5:])
    return _original(*args, **kwargs)


def install() -> None:
    _range_op.range_operation = _range_operation


def uninstall() -> None:
    _range_op.range_operation = _original


def installed() -> bool:
    return _range_op.range_operation is _range_operation


install()

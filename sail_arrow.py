"""Operacje polars-bio w Sailu przez applyInArrow — ścieżka pomiarowa (specyfikacja 3b-2, 5.2).

Zamiast UDTF z LATERAL (sail_bio.py) dane są grupowane po chromosomie: groupBy (merge) albo
cogroup dwóch tabel (pozostałe operacje), a funkcja Pythona dostaje tabelę Arrow jednego
chromosomu i zwraca tabelę Arrow wyniku. Znikają collect_list, LATERAL i explode, a wynik
przechodzi przez Arrow bez zamiany na wiersze Pythona — sonda 3b-2: 10–20× szybciej niż UDTF
na parze 1-2, wynik zgodny z polars-bio dla pięciu operacji.

Tryby dostępu do polars-bio w procesie serwera Saila:
- "context" — kontekst polars-bio z puli, jeden na równoczesne wywołanie (sail_pb_context), bez
  blokady: chromosomy liczą się równolegle; wariant główny `sail`;
- "lock" — globalny kontekst i blokada sail_pb_guard.PB_LOCK (jeden chromosom naraz); wariant
  porównawczy `sail_lock`.

Chromosom tylko po prawej stronie (pusta lewa) daje pusty wynik bez wołania polars-bio; tylko po
lewej — polars-bio dostaje pustą prawą stronę i sam rozstrzyga semantykę (jak w sail_bio.py).

Fazy: gdy podano plik faz, każde wywołanie polars-bio dopisuje do niego linię JSON
{"pb_s": …, "wait_s": …} — czas wywołania i czekania na blokadę; runner sumuje je po zapytaniu
(read_phases). Plik leży na tej samej maszynie co serwer (lokalny klaster); dopisywanie krótkich
linii w trybie append jest bezpieczne między wątkami.

Moduł musi być importowalny po nazwie w procesie serwera (katalog roboczy = repozytorium):
cloudpickle serializuje funkcję przekazaną do applyInArrow, a ta importuje sail_arrow."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pyarrow as pa

from bench.data.datasets import COLUMNS
from bench.ops import OUTPUT_COLUMNS, UNARY_OPS, normalize_polars_bio

MODES = ("context", "lock")
#: Nazwy kolumn przedziału, pod którymi dane trafiają do polars-bio.
_PB_COLS = ("chrom", "start", "end")


def ddl_schema(op: str) -> str:
    """Schemat wyniku dla applyInArrow (DDL Sparka) w schemacie znormalizowanym."""
    return ", ".join(f"{c} {'string' if c.startswith('chrom') else 'long'}" for c in OUTPUT_COLUMNS[op])


def arrow_schema(op: str) -> pa.Schema:
    return pa.schema([(c, pa.string() if c.startswith("chrom") else pa.int64()) for c in OUTPUT_COLUMNS[op]])


def _frame(table: pa.Table):
    """Tabela Arrow grupy → polars (bez kopii) z metadanymi układu 0-based dla polars-bio."""
    import polars as pl
    from polars_bio._metadata import set_coordinate_system

    df = pl.from_arrow(table).select(
        pl.col("chrom").cast(pl.Utf8), pl.col("start").cast(pl.Int64), pl.col("end").cast(pl.Int64)
    )
    set_coordinate_system(df, True)
    return df


def _call_polars_bio(op: str, left, right):
    import polars_bio as pb

    cols = list(_PB_COLS)
    if op in UNARY_OPS:
        return pb.merge(left, cols=cols, output_type="polars.DataFrame")
    return getattr(pb, op)(left, right, cols1=cols, cols2=cols, output_type="polars.DataFrame")


def evaluate(op: str, left: pa.Table, right: pa.Table | None, mode: str, phase_log: str | None) -> pa.Table:
    """Treść funkcji applyInArrow: jedna operacja polars-bio na jednym chromosomie."""
    schema = arrow_schema(op)
    if left.num_rows == 0:
        return schema.empty_table()  # każda z operacji zwraca wiersze wynikające z lewej tabeli
    left_df = _frame(left)
    right_df = None if right is None else _frame(right)
    wait_s = 0.0
    if mode == "context":
        import sail_pb_context  # noqa: F401 — import instaluje pulę kontekstów

        t = time.perf_counter()
        result = _call_polars_bio(op, left_df, right_df)
        pb_s = time.perf_counter() - t
    elif mode == "lock":
        from sail_pb_guard import PB_LOCK

        t = time.perf_counter()
        with PB_LOCK:
            wait_s = time.perf_counter() - t
            t = time.perf_counter()
            result = _call_polars_bio(op, left_df, right_df)
            pb_s = time.perf_counter() - t
    else:
        raise ValueError(f"unknown mode {mode!r}; allowed: {', '.join(MODES)}")
    if phase_log:
        with open(phase_log, "a", encoding="utf-8") as f:
            f.write(json.dumps({"pb_s": pb_s, "wait_s": wait_s}) + "\n")
    if result.height == 0:
        return schema.empty_table()
    return normalize_polars_bio(op, result, _PB_COLS).to_arrow().cast(schema)


def build_query(spark, op: str, left, right=None, cols: tuple[str, str, str] = COLUMNS, *,
                mode: str = "context", phase_log: str | None = None):
    """Zapytanie (leniwy DataFrame) na plikach Parquet (plik albo katalog) z wynikiem w schemacie
    znormalizowanym (bench/ops.py)."""
    if op not in OUTPUT_COLUMNS:
        raise ValueError(f"unknown operation {op!r}")
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; allowed: {', '.join(MODES)}")
    if (op in UNARY_OPS) != (right is None):
        raise ValueError(f"{op} requires {'one table' if op in UNARY_OPS else 'two tables'}")
    from pyspark.sql import functions as F

    c, s, e = cols

    def load(path):
        return spark.read.parquet(str(path)).select(
            F.col(c).cast("string").alias("chrom"),
            F.col(s).cast("long").alias("start"),
            F.col(e).cast("long").alias("end"),
        )

    schema = ddl_schema(op)
    if op in UNARY_OPS:
        def unary(table):
            import sail_arrow

            return sail_arrow.evaluate(op, table, None, mode, phase_log)

        return load(left).groupBy("chrom").applyInArrow(unary, schema)

    def binary(left_table, right_table):
        import sail_arrow

        return sail_arrow.evaluate(op, left_table, right_table, mode, phase_log)

    return load(left).groupBy("chrom").cogroup(load(right).groupBy("chrom")).applyInArrow(binary, schema)


def read_phases(path: str | Path | None) -> tuple[float, float, int]:
    """Suma czasów polars-bio, suma czasów czekania na blokadę i liczba wywołań z pliku faz.
    Brak pliku albo pusty plik (żadna grupa nie wywołała polars-bio) → zera."""
    if path is None:
        return 0.0, 0.0, 0
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return 0.0, 0.0, 0
    records = [json.loads(line) for line in lines if line.strip()]
    return sum(r["pb_s"] for r in records), sum(r["wait_s"] for r in records), len(records)

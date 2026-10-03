"""Operacje polars-bio w Sailu na danych z plików Parquet (plan 2, specyfikacja 9.3).

Wzorzec z sail_*_udtf.py — skalarny UDTF wołany przez LATERAL, dane zgrupowane
po chromosomie przez collect_list, dostęp do polars-bio przez sail_pb_guard —
uogólniony na potrzeby pomiarów:

- dane czytane przez spark.read.parquet, dowolne nazwy kolumn przedziału,
  bez kolumny z nazwą (zbiory databio-8p mają tylko contig, pos_start, pos_end);
- wynik w schemacie znormalizowanym (bench/ops.py, specyfikacja 8.4);
- UDTF zwraca JEDEN wiersz na chromosom z tablicą wyników, rozwijaną (`explode`) dopiero
  poza złączeniem LATERAL. Sail dokleja do każdego wiersza wyniku UDTF-a cały wiersz
  zewnętrzny — razem z listą wszystkich przedziałów chromosomu — więc przy wierszu na
  wynik pamięć rosła jak (wiersze wyniku) × (rozmiar grupy): +3,8 GB przy 5000
  przedziałach w jednej grupie, a na parze 1-2 z databio-8p proces był zabijany
  (tests/test_sail_parquet.py::test_sail_memory_does_not_scale_with_output_times_group);
- chromosom obecny tylko w lewej tabeli trafia do polars-bio z PUSTĄ prawą
  stroną, więc semantykę rozstrzyga polars-bio, nie ten moduł: nearest daje
  wiersz bez sąsiada, coverage — pokrycie 0, subtract — przedział bez zmian.
  (sail_coverage_subtract_udtf.py pomija taki chromosom w coverage; dane
  demonstracyjne mają te same chromosomy po obu stronach, więc tego nie widać.)

Moduł musi być importowalny po nazwie (jak sail_pb_guard): cloudpickle
serializuje klasy UDTF, a kod w eval() sięga po funkcje modułu przez import.
Dekorator @udtf trzeba wywołać PO utworzeniu sesji Spark Connect (patrz
sail_overlap_udtf.py, _make_overlap_udtf) — stąd fabryka make_udtf().
"""

from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from bench.data.datasets import COLUMNS
from bench.ops import OUTPUT_COLUMNS, UNARY_OPS, polars_bio_names

#: Nazwy kolumn, pod którymi przedziały trafiają do polars-bio wewnątrz UDTF.
_PB_COLS = ("chrom", "start", "end")

RETURN_TYPES = {
    "overlap": "chrom_1: string, start_1: long, end_1: long, "
               "chrom_2: string, start_2: long, end_2: long",
    "nearest": "chrom_1: string, start_1: long, end_1: long, "
               "chrom_2: string, start_2: long, end_2: long, distance: long",
    "coverage": "chrom: string, start: long, end: long, coverage: long",
    "merge": "chrom: string, start: long, end: long, n_intervals: long",
    "subtract": "chrom: string, start: long, end: long",
}


def _frame(chrom: str, rows) -> pd.DataFrame:
    """Przedziały jednego chromosomu -> pandas dla polars-bio (0-based, półotwarte)."""
    rows = rows or []
    df = pd.DataFrame({
        "chrom": pd.Series([chrom] * len(rows), dtype=object),
        "start": pd.Series([int(r["start"]) for r in rows], dtype="int64"),
        "end": pd.Series([int(r["end"]) for r in rows], dtype="int64"),
    })
    df.attrs["coordinate_system_zero_based"] = True
    return df


def _value(v):
    """Wartość z pandas -> typ Pythona akceptowany przez UDTF (brak -> None)."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    return v if isinstance(v, str) else int(v)


def _rows(op: str, result: pd.DataFrame):
    if result is None or len(result) == 0:
        return
    source = {dst: src for src, dst in polars_bio_names(op, _PB_COLS).items()}
    cols = [source[c] for c in OUTPUT_COLUMNS[op]]
    for rec in result[cols].itertuples(index=False, name=None):
        yield tuple(_value(v) for v in rec)


def evaluate(op: str, chrom: str, rows_a, rows_b=None):
    """Treść eval() UDTF-a: jedna operacja polars-bio na jednym chromosomie."""
    import sail_pb_guard

    if not rows_a:
        return  # każda z operacji zwraca wiersze wynikające z lewej tabeli
    left = _frame(chrom, rows_a)
    if op in UNARY_OPS:
        result = sail_pb_guard.merge(left, cols=list(_PB_COLS), output_type="pandas.DataFrame")
    else:
        result = getattr(sail_pb_guard, op)(
            left,
            _frame(chrom, rows_b),
            cols1=list(_PB_COLS),
            cols2=list(_PB_COLS),
            output_type="pandas.DataFrame",
        )
    yield from _rows(op, result)


def make_udtf(op: str):
    """Klasa UDTF dla operacji — wołać PO utworzeniu sesji Spark Connect.

    Zwraca jeden wiersz na chromosom: `res` — tablica wierszy wyniku (patrz docstring
    modułu, dlaczego nie wiersz na wynik)."""
    from pyspark.sql.functions import udtf

    packed = f"res: array<struct<{RETURN_TYPES[op]}>>"

    if op in UNARY_OPS:

        @udtf(returnType=packed)
        class UnaryUDTF:
            def eval(self, chrom, rows_a):
                import sail_bio

                yield (list(sail_bio.evaluate(op, chrom, rows_a)),)

        return UnaryUDTF

    @udtf(returnType=packed)
    class BinaryUDTF:
        def eval(self, chrom, rows_a, rows_b):
            import sail_bio

            yield (list(sail_bio.evaluate(op, chrom, rows_a, rows_b)),)

    return BinaryUDTF


def udtf_name(op: str) -> str:
    return f"bio_{op}"


def register(spark, op: str) -> None:
    """Rejestruje UDTF operacji w sesji — przed `build_query`. Runner pomiarowy robi to przed
    pomiarem czasu, tak jak bench_client rejestruje funkcje dist_* przy połączeniu."""
    if op not in RETURN_TYPES:
        raise ValueError(f"unknown operation {op!r}")
    spark.udtf.register(udtf_name(op), make_udtf(op))


def build_query(spark, op: str, left, right=None, cols: tuple[str, str, str] = COLUMNS):
    """Zapytanie (leniwy DataFrame) na plikach Parquet (plik albo katalog) z wynikiem
    w schemacie znormalizowanym; UDTF operacji musi być zarejestrowany (`register`)."""
    from pyspark.sql import functions as F

    if op not in RETURN_TYPES:
        raise ValueError(f"unknown operation {op!r}")
    if (op in UNARY_OPS) != (right is None):
        raise ValueError(
            f"{op} requires {'one table' if op in UNARY_OPS else 'two tables'}"
        )
    c, s, e = cols

    def load(path, source: str):
        return spark.read.parquet(str(path)).select(
            F.col(c).cast("string").alias("chrom"),
            F.col(s).cast("long").alias("start"),
            F.col(e).cast("long").alias("end"),
            F.lit(source).alias("source"),
        )

    interval = F.struct("start", "end")
    name = udtf_name(op)
    if op in UNARY_OPS:
        grouped = load(left, "a").groupBy("chrom").agg(
            F.collect_list(interval).alias("rows_a")
        )
        call = f"{name}(g.chrom, g.rows_a)"
    else:
        grouped = load(left, "a").union(load(right, "b")).groupBy("chrom").agg(
            F.collect_list(F.when(F.col("source") == "a", interval)).alias("rows_a"),
            F.collect_list(F.when(F.col("source") == "b", interval)).alias("rows_b"),
        )
        call = f"{name}(g.chrom, g.rows_a, g.rows_b)"
    view = f"bio_grouped_{op}"
    grouped.createOrReplaceTempView(view)
    # explode POZA złączeniem LATERAL: rozwijana tablica nie niesie już listy przedziałów grupy.
    return spark.sql(
        f"SELECT r.* FROM (SELECT explode(o.res) AS r FROM {view} g, LATERAL {call} o)"
    )


def run_op(spark, op: str, left, right=None, cols: tuple[str, str, str] = COLUMNS) -> pd.DataFrame:
    """Operacja na plikach Parquet (plik albo katalog) -> wynik w schemacie znormalizowanym."""
    register(spark, op)
    return build_query(spark, op, left, right, cols).toPandas()


@contextmanager
def sail_session():
    """Serwer Sail (local) i sesja Spark Connect; zatrzymywane przy wyjściu.

    `.create()`, nie `.getOrCreate()` — PySpark cachuje sesję jako singleton
    procesu i przy kolejnym serwerze zwróciłby martwą sesję (sail_overlap_udtf.py).
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

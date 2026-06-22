"""
Genomic overlap przez Sail z użyciem polars-bio lokalnie na każdej partycji.

Architektura:
  1. Oba datasety tagowane (source=a/b) i łączone
  2. Repartition po chromosomie → każdy chrom trafia na jeden węzeł
  3. applyInPandas: na każdej partycji pb.overlap() z COITrees
  4. Wyniki zbierane przez Sail

Uruchomienie:
  python sail_overlap_udtf.py
"""

import time
import pandas as pd
import polars_bio as pb
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# ---------------------------------------------------------------------------
# Dane testowe — identyczne jak w overlap_comparison.py
# ---------------------------------------------------------------------------

INTERVALS_A = [
    ("chr1", 100, 200, "gene_A1"),
    ("chr1", 150, 300, "gene_A2"),
    ("chr1", 400, 500, "gene_A3"),
    ("chr2",  50, 150, "gene_A4"),
    ("chr2", 200, 350, "gene_A5"),
]

INTERVALS_B = [
    ("chr1", 180, 250, "peak_B1"),
    ("chr1", 290, 420, "peak_B2"),
    ("chr1", 450, 600, "peak_B3"),
    ("chr2", 100, 220, "peak_B4"),
    ("chr2", 300, 400, "peak_B5"),
]

SCHEMA = ["chrom", "start", "end", "name"]

# Schema wyniku
RESULT_SCHEMA = StructType([
    StructField("chrom",   StringType(), True),
    StructField("start_a", LongType(),   True),
    StructField("end_a",   LongType(),   True),
    StructField("name_a",  StringType(), True),
    StructField("start_b", LongType(),   True),
    StructField("end_b",   LongType(),   True),
    StructField("name_b",  StringType(), True),
])

# ---------------------------------------------------------------------------
# Funkcja wykonywana lokalnie na każdej partycji (jeden chromosom)
# ---------------------------------------------------------------------------

def _reset_pb_context():
    """
    polars-bio używa globalnego kontekstu DataFusion i rejestruje tabele jako
    's1' i 's2'. Przy kolejnym wywołaniu w tym samym procesie (kolejna partycja)
    Rust panikuje z 'table already exists'. Czyścimy kontekst przed każdym wywołaniem.
    """
    from polars_bio.context import ctx as _pb_ctx
    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def overlap_partition(key, pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Wykonuje pb.overlap() na partycji jednego chromosomu.
    key = (chrom,) — klucz grupowania
    pdf = pandas DataFrame z kolumnami: chrom, start, end, name, source
    """
    df_a = pdf[pdf["source"] == "a"][["chrom", "start", "end", "name"]].reset_index(drop=True)
    df_b = pdf[pdf["source"] == "b"][["chrom", "start", "end", "name"]].reset_index(drop=True)

    empty = pd.DataFrame(columns=["chrom", "start_a", "end_a", "name_a",
                                   "start_b", "end_b", "name_b"])

    if len(df_a) == 0 or len(df_b) == 0:
        return empty

    # polars-bio wymaga metadanych o układzie współrzędnych
    df_a.attrs["coordinate_system_zero_based"] = True
    df_b.attrs["coordinate_system_zero_based"] = True

    # Wyczyść globalny kontekst polars-bio przed wywołaniem
    _reset_pb_context()

    result = pb.overlap(
        df_a, df_b,
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
        output_type="pandas.DataFrame",
    )

    if result is None or len(result) == 0:
        return empty

    # polars-bio zwraca kolumny z sufixami _1 i _2
    result = result.rename(columns={
        "chrom_1": "chrom",
        "start_1": "start_a", "end_1": "end_a", "name_1": "name_a",
        "start_2": "start_b", "end_2": "end_b", "name_2": "name_b",
    })

    return result[["chrom", "start_a", "end_a", "name_a",
                   "start_b", "end_b", "name_b"]]


# ---------------------------------------------------------------------------
# Referencja: polars-bio lokalnie
# ---------------------------------------------------------------------------

def run_polars_bio():
    import polars as pl

    df_a = pl.DataFrame(INTERVALS_A, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(INTERVALS_B, schema=SCHEMA, orient="row")

    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)

    t0 = time.perf_counter()
    result = pb.overlap(df_a, df_b).collect()
    elapsed = time.perf_counter() - t0

    return result, elapsed


# ---------------------------------------------------------------------------
# Sail: applyInPandas grouped by chromosome
# ---------------------------------------------------------------------------

def run_sail_overlap():
    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address

    spark = (
        SparkSession.builder
        .remote(f"sc://{ip}:{port}")
        .getOrCreate()
    )

    # Tworzenie DataFrames
    df_a = spark.createDataFrame(INTERVALS_A, schema=SCHEMA).withColumn("source", F.lit("a"))
    df_b = spark.createDataFrame(INTERVALS_B, schema=SCHEMA).withColumn("source", F.lit("b"))

    # Łączymy oba zbiory i partycjonujemy po chromosomie
    df_combined = df_a.union(df_b).repartition("chrom")

    t0 = time.perf_counter()
    # Na każdej partycji (jeden chrom) uruchamiamy pb.overlap()
    result = (
        df_combined
        .groupBy("chrom")
        .applyInPandas(overlap_partition, schema=RESULT_SCHEMA)
        .toPandas()
    )
    elapsed = time.perf_counter() - t0

    spark.stop()
    server.stop()

    return result, elapsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  Genomic Overlap: polars-bio vs Sail+polars-bio")
    print("=" * 60)

    print("\n[1/2] polars-bio (lokalnie)...")
    pb_result, pb_time = run_polars_bio()
    print(f"  Czas: {pb_time:.4f}s  |  Wiersze: {len(pb_result)}")
    print(pb_result)

    print("\n[2/2] Sail + polars-bio (applyInPandas po chromosomie)...")
    sail_result, sail_time = run_sail_overlap()
    print(f"  Czas: {sail_time:.4f}s  |  Wiersze: {len(sail_result)}")
    print(sail_result.sort_values(["chrom", "start_a", "start_b"]).to_string(index=False))

    print("\n" + "=" * 60)
    print(f"  polars-bio lokalnie : {pb_time:.4f}s")
    print(f"  Sail + polars-bio   : {sail_time:.4f}s")

    # Weryfikacja poprawności
    pb_pairs = set(
        zip(pb_result["name_1"].to_list(), pb_result["name_2"].to_list())
    )
    sail_pairs = set(
        zip(sail_result["name_a"].to_list(), sail_result["name_b"].to_list())
    )

    if pb_pairs == sail_pairs:
        print("\n  WYNIK: identyczne pary ✓")
    else:
        print(f"\n  WYNIK: RÓŻNICA!")
        print(f"  Tylko w polars-bio: {pb_pairs - sail_pairs}")
        print(f"  Tylko w Sail:       {sail_pairs - pb_pairs}")
    print("=" * 60)


if __name__ == "__main__":
    main()

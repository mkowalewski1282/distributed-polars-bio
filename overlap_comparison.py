"""
Porównanie operacji overlap na danych genomicznych:
  - polars-bio (lokalny silnik DataFusion/Polars)
  - Sail (Spark Connect → Rust/DataFusion, bez JVM)

Uruchomienie:
  python overlap_comparison.py

Sail uruchamia SparkConnectServer automatycznie w tle.
"""

import time
import polars as pl
import polars_bio as pb
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Dane testowe
# ---------------------------------------------------------------------------

INTERVALS_A = [
    ("chr1", 100, 200, "gene_A1"),
    ("chr1", 150, 300, "gene_A2"),
    ("chr1", 400, 500, "gene_A3"),
    ("chr2", 50,  150, "gene_A4"),
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


# ---------------------------------------------------------------------------
# 1. polars-bio overlap
# ---------------------------------------------------------------------------

def run_polars_bio():
    df_a = pl.DataFrame(INTERVALS_A, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(INTERVALS_B, schema=SCHEMA, orient="row")

    # polars-bio wymaga ustawienia układu współrzędnych na DataFramie
    # config_meta jest dostępne po zaimportowaniu polars_bio (rejestruje rozszerzenie)
    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)

    t0 = time.perf_counter()
    # domyślnie zwraca LazyFrame — collect() materializuje wynik
    result = pb.overlap(df_a, df_b).collect()
    elapsed = time.perf_counter() - t0

    return result, elapsed


# ---------------------------------------------------------------------------
# 2. Sail overlap (przez PySpark Connect API)
# ---------------------------------------------------------------------------

def run_sail():
    # Uruchom SparkConnectServer (Sail) — port losowy, czytamy z listening_address
    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address

    spark = (
        SparkSession.builder
        .remote(f"sc://{ip}:{port}")
        .create()  # nie .getOrCreate() — patrz sail_overlap_udtf.py
    )

    df_a = spark.createDataFrame(INTERVALS_A, schema=SCHEMA)
    df_b = spark.createDataFrame(INTERVALS_B, schema=SCHEMA)

    t0 = time.perf_counter()
    result = (
        df_a.alias("a")
        .join(
            df_b.alias("b"),
            on=(
                (F.col("a.chrom") == F.col("b.chrom")) &
                (F.col("a.start") < F.col("b.end")) &
                (F.col("b.start") < F.col("a.end"))
            ),
            how="inner",
        )
        .select(
            F.col("a.chrom").alias("chrom_a"),
            F.col("a.start").alias("start_a"),
            F.col("a.end").alias("end_a"),
            F.col("a.name").alias("name_a"),
            F.col("b.start").alias("start_b"),
            F.col("b.end").alias("end_b"),
            F.col("b.name").alias("name_b"),
        )
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
    print("  Overlap: polars-bio vs Sail")
    print("=" * 60)

    print("\n[1/2] polars-bio...")
    pb_result, pb_time = run_polars_bio()
    print(f"  Czas: {pb_time:.4f}s  |  Wiersze: {len(pb_result)}")
    print(pb_result)

    print("\n[2/2] Sail (PySpark Connect)...")
    sail_result, sail_time = run_sail()
    print(f"  Czas: {sail_time:.4f}s  |  Wiersze: {len(sail_result)}")
    print(sail_result)

    print("\n" + "=" * 60)
    print(f"  polars-bio : {pb_time:.4f}s")
    print(f"  Sail       : {sail_time:.4f}s")
    speedup = sail_time / pb_time if pb_time > 0 else float("inf")
    print(f"  Stosunek   : {speedup:.1f}x (na małych danych nie jest miarodajny)")
    print("=" * 60)


if __name__ == "__main__":
    main()

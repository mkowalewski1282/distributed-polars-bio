"""
Genomic nearest przez Sail z użyciem polars-bio — ten sam sprawdzony wzorzec co
sail_overlap_udtf.py (skalarny UDTF, groupBy/collect_list, LATERAL,
.repartition(1) jako obejście buga Saila — patrz komentarze tam).

Uruchomienie:
  python sail_nearest_udtf.py
"""

import time
import pandas as pd
import polars_bio as pb
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udtf

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

NEAREST_UDTF_RETURN_TYPE = (
    "chrom: string, start_a: long, end_a: long, name_a: string, "
    "start_b: long, end_b: long, name_b: string, distance: long"
)


def _reset_pb_context():
    from polars_bio.context import ctx as _pb_ctx
    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def _make_nearest_udtf():
    @udtf(returnType=NEAREST_UDTF_RETURN_TYPE)
    class NearestUDTF:
        def eval(self, chrom, rows_a, rows_b):
            if not rows_a or not rows_b:
                return

            df_a = pd.DataFrame(
                [(chrom, r["start"], r["end"], r["name"]) for r in rows_a], columns=SCHEMA
            )
            df_b = pd.DataFrame(
                [(chrom, r["start"], r["end"], r["name"]) for r in rows_b], columns=SCHEMA
            )
            df_a.attrs["coordinate_system_zero_based"] = True
            df_b.attrs["coordinate_system_zero_based"] = True

            _reset_pb_context()

            result = pb.nearest(
                df_a, df_b,
                cols1=["chrom", "start", "end"],
                cols2=["chrom", "start", "end"],
                output_type="pandas.DataFrame",
            )

            if result is None or len(result) == 0:
                return

            cols = list(result.columns)
            dist_col = next((c for c in cols if "distance" in c.lower()), None)

            for _, r in result.iterrows():
                dist = int(r[dist_col]) if dist_col is not None else 0
                yield (
                    r["chrom_1"],
                    int(r["start_1"]), int(r["end_1"]), r["name_1"],
                    int(r["start_2"]), int(r["end_2"]), r["name_2"],
                    dist,
                )

    return NearestUDTF


def run_polars_bio():
    import polars as pl

    df_a = pl.DataFrame(INTERVALS_A, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(INTERVALS_B, schema=SCHEMA, orient="row")
    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)

    t0 = time.perf_counter()
    result = pb.nearest(df_a, df_b).collect()
    elapsed = time.perf_counter() - t0
    return result, elapsed


def run_sail_nearest():
    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address

    # .create() zamiast .getOrCreate() — patrz sail_merge_udtf.py (unika
    # "Connection refused" przy wielu wywołaniach w jednym procesie).
    spark = SparkSession.builder.remote(f"sc://{ip}:{port}").create()
    spark.udtf.register("nearest_udtf", _make_nearest_udtf())

    df_a = spark.createDataFrame(INTERVALS_A, schema=SCHEMA).withColumn("source", F.lit("a"))
    df_b = spark.createDataFrame(INTERVALS_B, schema=SCHEMA).withColumn("source", F.lit("b"))
    df_combined = df_a.union(df_b)

    interval_struct = F.struct(F.col("start"), F.col("end"), F.col("name"))
    grouped = (
        df_combined.groupBy("chrom")
        .agg(
            F.collect_list(F.when(F.col("source") == F.lit("a"), interval_struct)).alias("rows_a"),
            F.collect_list(F.when(F.col("source") == F.lit("b"), interval_struct)).alias("rows_b"),
        )
        .repartition(1)
    )
    grouped.createOrReplaceTempView("grouped_by_chrom")

    t0 = time.perf_counter()
    result = spark.sql(
        "SELECT o.* FROM grouped_by_chrom g, LATERAL nearest_udtf(g.chrom, g.rows_a, g.rows_b) o"
    ).toPandas()
    elapsed = time.perf_counter() - t0

    spark.stop()
    server.stop()
    return result, elapsed


def main():
    print("=" * 60)
    print("  Genomic nearest: polars-bio vs Sail+polars-bio (UDTF)")
    print("=" * 60)

    print("\n[1/2] polars-bio (lokalnie)...")
    pb_result, pb_time = run_polars_bio()
    print(f"  Czas: {pb_time:.4f}s  |  Wiersze: {len(pb_result)}")
    print(pb_result)

    print("\n[2/2] Sail + polars-bio (UDTF)...")
    sail_result, sail_time = run_sail_nearest()
    print(f"  Czas: {sail_time:.4f}s  |  Wiersze: {len(sail_result)}")
    print(sail_result.sort_values(["chrom", "start_a"]).to_string(index=False))

    pb_pairs = set(zip(pb_result["name_1"].to_list(), pb_result["name_2"].to_list()))
    sail_pairs = set(zip(sail_result["name_a"].tolist(), sail_result["name_b"].tolist()))

    print("\n" + "=" * 60)
    if pb_pairs == sail_pairs:
        print("  WYNIK: identyczne pary nearest ✓")
    else:
        print("  WYNIK: RÓŻNICA!")
        print(f"  Tylko w polars-bio: {pb_pairs - sail_pairs}")
        print(f"  Tylko w Sail:       {sail_pairs - pb_pairs}")
    print("=" * 60)


if __name__ == "__main__":
    main()

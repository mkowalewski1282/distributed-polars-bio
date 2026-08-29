"""
Genomic merge przez Sail z użyciem polars-bio, wzorowane na sail_overlap_udtf.py
(Faza C, ten sam sprawdzony wzorzec: groupBy/collect_list + LATERAL skalarny UDTF,
partycjami — patrz komentarze w sail_overlap_udtf.py po pełne wyjaśnienie).

Architektura:
  1. Dane grupowane wg chrom, interwały zbierane w listę (collect_list)
  2. Prawdziwy zarejestrowany UDTF (PR #1519) wołany przez LATERAL — raz na
     chromosom, z całą listą interwałów jako argumentem
  3. eval() woła pb.merge() (przez sail_pb_guard) na interwałach danego chromosomu

Uruchomienie:
  python sail_merge_udtf.py
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
SCHEMA = ["chrom", "start", "end", "name"]

MERGE_UDTF_RETURN_TYPE = "chrom: string, start: long, end: long, n_intervals: long"

def _reset_pb_context():
    from polars_bio.context import ctx as _pb_ctx
    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass

def _make_merge_udtf():
    @udtf(returnType=MERGE_UDTF_RETURN_TYPE)
    class MergeUDTF:
        def eval(self, chrom, rows):
            if not rows:
                return

            df = pd.DataFrame(
                [(chrom, r["start"], r["end"], r["name"]) for r in rows],
                columns=SCHEMA,
            )
            df.attrs["coordinate_system_zero_based"] = True

            # Import WEWNATRZ eval(): to tutaj wykonuje sie worker. Modul jest
            # importowalny po nazwie, wiec cloudpickle serializuje REFERENCJE,
            # a nie obiekt locka (ten nie jest picklowalny). Dzieki temu wszystkie
            # partycje w danym procesie dziela ten sam lock. Patrz sail_pb_guard.py.
            import sail_pb_guard

            result = sail_pb_guard.merge(df, cols=["chrom", "start", "end"], output_type="pandas.DataFrame")

            if result is None or len(result) == 0:
                return

            cols = list(result.columns)
            chrom_col = "chrom" if "chrom" in cols else cols[0]
            start_col = "start" if "start" in cols else cols[1]
            end_col = "end" if "end" in cols else cols[2]
            n_col = "n_intervals" if "n_intervals" in cols else (cols[3] if len(cols) > 3 else None)

            for _, r in result.iterrows():
                n = int(r[n_col]) if n_col else 0
                yield (r[chrom_col], int(r[start_col]), int(r[end_col]), n)

    return MergeUDTF

def run_polars_bio():
    import polars as pl

    df = pl.DataFrame(INTERVALS_A, schema=SCHEMA, orient="row")
    df.config_meta.set(coordinate_system_zero_based=True)

    t0 = time.perf_counter()
    result = pb.merge(df).collect()
    elapsed = time.perf_counter() - t0
    return result, elapsed

def run_sail_merge():
    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address

    # .create() zamiast .getOrCreate(): PySpark cache'uje sesję jako globalny
    # singleton procesu — .getOrCreate() w drugim (i kolejnych) wywołaniu w tym
    # samym procesie Pythona (np. kilka plików testów pytest w jednym przebiegu)
    # zwracałoby STARĄ sesję wskazującą na już zatrzymany serwer ("Connection
    # refused"). .create() zawsze tworzy nową, poprawnie podłączoną sesję.
    spark = SparkSession.builder.remote(f"sc://{ip}:{port}").create()
    spark.udtf.register("merge_udtf", _make_merge_udtf())

    df = spark.createDataFrame(INTERVALS_A, schema=SCHEMA)
    interval_struct = F.struct(F.col("start"), F.col("end"), F.col("name"))
    grouped = (
        df.groupBy("chrom")
        .agg(F.collect_list(interval_struct).alias("rows"))
    )
    grouped.createOrReplaceTempView("grouped_by_chrom")

    t0 = time.perf_counter()
    result = spark.sql(
        "SELECT o.* FROM grouped_by_chrom g, LATERAL merge_udtf(g.chrom, g.rows) o"
    ).toPandas()
    elapsed = time.perf_counter() - t0

    spark.stop()
    server.stop()
    return result, elapsed

def main():
    print("=" * 60)
    print("  Genomic merge: polars-bio vs Sail+polars-bio (UDTF)")
    print("=" * 60)

    print("\n[1/2] polars-bio (lokalnie)...")
    pb_result, pb_time = run_polars_bio()
    print(f"  Czas: {pb_time:.4f}s  |  Wiersze: {len(pb_result)}")
    print(pb_result)

    print("\n[2/2] Sail + polars-bio (UDTF)...")
    sail_result, sail_time = run_sail_merge()
    print(f"  Czas: {sail_time:.4f}s  |  Wiersze: {len(sail_result)}")
    print(sail_result.sort_values(["chrom", "start"]).to_string(index=False))

    pb_cols = pb_result.columns
    chrom_c = "chrom" if "chrom" in pb_cols else pb_cols[0]
    start_c = "start" if "start" in pb_cols else pb_cols[1]
    end_c = "end" if "end" in pb_cols else pb_cols[2]

    pb_set = set(
        zip(pb_result[chrom_c].to_list(), pb_result[start_c].to_list(), pb_result[end_c].to_list())
    )
    sail_set = set(
        zip(sail_result["chrom"].tolist(), sail_result["start"].tolist(), sail_result["end"].tolist())
    )

    print("\n" + "=" * 60)
    if pb_set == sail_set:
        print("  WYNIK: identyczne zmergowane interwały ✓")
    else:
        print("  WYNIK: RÓŻNICA!")
        print(f"  Tylko w polars-bio: {pb_set - sail_set}")
        print(f"  Tylko w Sail:       {sail_set - pb_set}")
    print("=" * 60)

if __name__ == "__main__":
    main()

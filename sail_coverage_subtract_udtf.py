"""
Genomic coverage i subtract przez Sail z użyciem polars-bio — ten sam sprawdzony
wzorzec co poprzednie skrypty (skalarny UDTF, groupBy/collect_list, LATERAL,
.repartition(1) — patrz komentarze w sail_overlap_udtf.py).

Jeden plik dla obu operacji (coverage i subtract), bo obie biorą dwie tabele
(reads/targets, left/right) i mają identyczny kształt integracji co overlap/nearest
— różni je tylko wywoływana funkcja polars-bio wewnątrz eval().

Uruchomienie:
  python sail_coverage_subtract_udtf.py
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

COVERAGE_RETURN_TYPE = "chrom: string, start: long, end: long, name: string, coverage: long"
SUBTRACT_RETURN_TYPE = "chrom: string, start: long, end: long, name: string"


def _reset_pb_context():
    from polars_bio.context import ctx as _pb_ctx
    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def _to_df(rows, chrom):
    return pd.DataFrame(
        [(chrom, r["start"], r["end"], r["name"]) for r in rows], columns=SCHEMA
    )


def _make_coverage_udtf():
    @udtf(returnType=COVERAGE_RETURN_TYPE)
    class CoverageUDTF:
        def eval(self, chrom, rows_reads, rows_targets):
            if not rows_reads or not rows_targets:
                return
            df_reads = _to_df(rows_reads, chrom)
            df_targets = _to_df(rows_targets, chrom)
            df_reads.attrs["coordinate_system_zero_based"] = True
            df_targets.attrs["coordinate_system_zero_based"] = True

            _reset_pb_context()
            result = pb.coverage(
                df_reads, df_targets,
                cols1=["chrom", "start", "end"],
                cols2=["chrom", "start", "end"],
                output_type="pandas.DataFrame",
            )
            if result is None or len(result) == 0:
                return
            cols = list(result.columns)
            cov_col = next(c for c in cols if "coverage" in c.lower() or "cov" == c.lower())
            for _, r in result.iterrows():
                yield (r["chrom"], int(r["start"]), int(r["end"]), r["name"], int(r[cov_col]))

    return CoverageUDTF


def _make_subtract_udtf():
    @udtf(returnType=SUBTRACT_RETURN_TYPE)
    class SubtractUDTF:
        def eval(self, chrom, rows_left, rows_right):
            if not rows_left:
                return
            df_left = _to_df(rows_left, chrom)
            df_right = _to_df(rows_right, chrom) if rows_right else pd.DataFrame(columns=SCHEMA)
            df_left.attrs["coordinate_system_zero_based"] = True
            df_right.attrs["coordinate_system_zero_based"] = True

            _reset_pb_context()
            result = pb.subtract(
                df_left, df_right,
                cols1=["chrom", "start", "end"],
                cols2=["chrom", "start", "end"],
                output_type="pandas.DataFrame",
            )
            if result is None or len(result) == 0:
                return
            for _, r in result.iterrows():
                yield (r["chrom"], int(r["start"]), int(r["end"]), r["name"])

    return SubtractUDTF


def run_udtfs(udtf_factories: dict):
    """
    Uruchamia JEDNĄ sesję Spark Connect i woła w niej WIELE UDTF-ów po kolei.

    UWAGA #1 (nieoczywista pułapka, znaleziona empirycznie w tej sesji):
    PySpark cache'uje sesję jako globalny singleton (SparkSession.getOrCreate()).
    Wywołanie tej funkcji WIĘCEJ NIŻ RAZ w tym samym procesie Pythona (np. dwa
    osobne testy pytest w jednym pliku, każdy tworzący WŁASNY SparkConnectServer
    na innym porcie) kończyło się błędem "Connection refused" — druga sesja
    próbowała połączyć się przez URL PIERWSZEGO (już zatrzymanego) serwera,
    bo getOrCreate() zwrócił cache'owaną sesję zamiast nowej. Dlatego ta
    funkcja przyjmuje SŁOWNIK {nazwa_udtf: FUNKCJA_FABRYKUJĄCA} i woła
    wszystkie w jednej sesji/jednym serwerze.

    UWAGA #2 (ta sama pułapka co w sail_overlap_udtf.py, druga odsłona):
    argumenty MUSZĄ być FUNKCJAMI budującymi klasę UDTF (np. `_make_coverage_udtf`,
    NIE `_make_coverage_udtf()`), bo dekorator @udtf wewnątrz sprawdza tryb
    "remote" w momencie DEFINICJI klasy — jeśli klasa zostanie zbudowana
    wcześniej (np. jako argument tej funkcji, obliczony PRZED stworzeniem
    sesji), zostanie zbudowana jako klasyczny (nie connect) UDTF i rejestracja
    zawiedzie błędem "expected a 'UserDefinedTableFunction'". Fabryki są
    wołane DOPIERO wewnątrz tej funkcji, po utworzeniu sesji.

    Zwraca: {nazwa_udtf: pandas.DataFrame}
    """
    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address

    # .create() zamiast .getOrCreate() — patrz sail_merge_udtf.py (unika
    # "Connection refused" przy wielu wywołaniach w jednym procesie). Dzięki
    # temu ta funkcja mogłaby teraz w zasadzie być wołana wielokrotnie z
    # osobna zamiast wymagać jednej wspólnej sesji — zostawione jak jest
    # (jedna sesja na wiele UDTF-ów), bo to i tak szybsze (jeden start serwera).
    spark = SparkSession.builder.remote(f"sc://{ip}:{port}").create()

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

    results = {}
    for udtf_name, make_udtf_class in udtf_factories.items():
        spark.udtf.register(udtf_name, make_udtf_class())
        results[udtf_name] = spark.sql(
            f"SELECT o.* FROM grouped_by_chrom g, LATERAL {udtf_name}(g.chrom, g.rows_a, g.rows_b) o"
        ).toPandas()

    spark.stop()
    server.stop()
    return results


def main():
    print("=" * 60)
    print("  Genomic coverage/subtract: polars-bio vs Sail+polars-bio (UDTF)")
    print("=" * 60)

    import polars as pl

    df_a = pl.DataFrame(INTERVALS_A, schema=SCHEMA, orient="row")
    df_b = pl.DataFrame(INTERVALS_B, schema=SCHEMA, orient="row")
    df_a.config_meta.set(coordinate_system_zero_based=True)
    df_b.config_meta.set(coordinate_system_zero_based=True)

    print("\n[coverage] polars-bio (lokalnie)...")
    t0 = time.perf_counter()
    pb_cov = pb.coverage(df_a, df_b).collect()
    print(f"  Czas: {time.perf_counter() - t0:.4f}s")
    print(pb_cov)

    print("\n[coverage+subtract] Sail + polars-bio (UDTF, jedna sesja)...")
    sail_results = run_udtfs({
        "coverage_udtf": _make_coverage_udtf,
        "subtract_udtf": _make_subtract_udtf,
    })
    sail_cov = sail_results["coverage_udtf"]
    sail_sub = sail_results["subtract_udtf"]
    print(sail_cov.sort_values(["chrom", "start"]).to_string(index=False))

    cov_cols = pb_cov.columns
    cov_col = next(c for c in cov_cols if "coverage" in c.lower())
    pb_cov_set = set(zip(pb_cov["chrom"].to_list(), pb_cov["start"].to_list(),
                          pb_cov["end"].to_list(), pb_cov[cov_col].to_list()))
    sail_cov_set = set(zip(sail_cov["chrom"].tolist(), sail_cov["start"].tolist(),
                            sail_cov["end"].tolist(), sail_cov["coverage"].tolist()))
    print("\n  WYNIK coverage:", "identyczne ✓" if pb_cov_set == sail_cov_set else "RÓŻNICA!")
    if pb_cov_set != sail_cov_set:
        print(f"    Tylko w polars-bio: {pb_cov_set - sail_cov_set}")
        print(f"    Tylko w Sail:       {sail_cov_set - pb_cov_set}")

    print("\n[subtract] polars-bio (lokalnie)...")
    t0 = time.perf_counter()
    pb_sub = pb.subtract(df_a, df_b).collect()
    print(f"  Czas: {time.perf_counter() - t0:.4f}s")
    print(pb_sub)

    print("\n[subtract] Sail + polars-bio (UDTF, wynik z tej samej sesji co coverage)...")
    print(sail_sub.sort_values(["chrom", "start"]).to_string(index=False))

    pb_sub_set = set(zip(pb_sub["chrom"].to_list(), pb_sub["start"].to_list(), pb_sub["end"].to_list()))
    sail_sub_set = set(zip(sail_sub["chrom"].tolist(), sail_sub["start"].tolist(), sail_sub["end"].tolist()))
    print("\n  WYNIK subtract:", "identyczne ✓" if pb_sub_set == sail_sub_set else "RÓŻNICA!")
    if pb_sub_set != sail_sub_set:
        print(f"    Tylko w polars-bio: {pb_sub_set - sail_sub_set}")
        print(f"    Tylko w Sail:       {sail_sub_set - pb_sub_set}")

    print("=" * 60)


if __name__ == "__main__":
    main()

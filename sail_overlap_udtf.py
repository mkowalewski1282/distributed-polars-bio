"""
Genomic overlap przez Sail z użyciem polars-bio lokalnie na każdą grupę chromosomu.

Faza B (plan pracy magisterskiej) — historia trzech podejść wypróbowanych w tej sesji,
wszystkie empirycznie przetestowane na żywym Sail (pysail 0.5.3):

  1. `TABLE(combined) PARTITION BY chrom` w SQL
     -> błąd parsera: "found PARTITION at ... expected ..." (Sail nie rozpoznaje
        tej składni SQL).
  2. To samo przez DataFrame API (`TableArg.partitionBy()`, z pominięciem parsera SQL)
     -> błąd warstwy wykonania: "UnsupportedOperationException: table argument
        options in subquery expression".
  3. Argument TABLE bez żadnych opcji partycjonowania (`asTable()`, po własnej
     `.repartition("chrom")`)
     -> błąd warstwy wykonania: "UnsupportedOperationException: unsupported
        subquery type" — argumenty TABLE dla UDTF nie są w Sailu (na dziś,
        pysail 0.5.3) wspierane W OGÓLE, niezależnie od API czy opcji.

Wniosek #1 (udokumentowane ograniczenie silnika, nie nasz bug): PR #1519 dodał
w Sailu rejestrację UDTF i argumenty SKALARNE (Arrow-native), ale NIE argumenty
TABLE. To realny, wart odnotowania w pracy magisterskiej stan dojrzałości Saila.

Rozwiązanie — UDTF czysto skalarny, wołany przez LATERAL JOIN:
  1. Oba datasety tagowane (source=a/b) i łączone (UNION ALL)
  2. `groupBy("chrom").agg(collect_list(struct(...)))` — STANDARDOWA, na pewno
     wspierana operacja Sail/DataFusion — grupuje dane wg chromosomu i pakuje
     interwały source=a i source=b w dwie kolumny array<struct<...>>, po
     jednym wierszu na chromosom.
  3. Prawdziwy, zarejestrowany UDTF (spark.udtf.register, mechanizm PR #1519)
     wołany jako zwykła funkcja skalarna przez `LATERAL overlap_udtf(chrom,
     rows_a, rows_b)` — eval() wywoływane raz na wiersz wejściowy = raz na
     chromosom, z całą zawartością dwóch list interwałów jako argumentami.
     Wewnątrz eval() wołane pb.overlap().

Wniosek #2 (KOLEJNY udokumentowany bug Sail/pysail 0.5.3, znaleziony empirycznie
w tej sesji): `LATERAL <skalarny UDTF>` nad tabelą zewnętrzną rozłożoną na >1
partycję fizyczną GUBI wiersze z jednych partycji i DUBLUJE inne (obserwowane:
2 grupy/2 partycje -> 1 grupa zgubiona całkowicie, druga zdublowana 2x). Po
wymuszeniu 1 partycji (`grouped.repartition(1)`) wynik jest w 100% poprawny.
To NIE jest workaround "bo tak wygodniej" — to jedyny sposób na poprawność przy
obecnym stanie Saila. WAŻNA KONSEKWENCJA dla Fazy D (benchmarki): dopóki ten bug
nie zostanie naprawiony w Sailu, ta ścieżka implementacji nie może pokazać
realnego przyspieszenia z równoległości (wymusza pojedynczą partycję) — do
uwzględnienia przy projektowaniu benchmarków i w dyskusji ograniczeń w pracy.

Uruchomienie:
  python sail_overlap_udtf.py
"""

import time
import pandas as pd
import polars_bio as pb
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udtf

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

OVERLAP_UDTF_RETURN_TYPE = (
    "chrom: string, start_a: long, end_a: long, name_a: string, "
    "start_b: long, end_b: long, name_b: string"
)


def _reset_pb_context():
    """
    polars-bio używa globalnego kontekstu DataFusion i rejestruje tabele jako
    's1' i 's2'. Przy kolejnym wywołaniu w tym samym procesie (kolejna grupa)
    Rust panikuje z 'table already exists'. Czyścimy kontekst przed każdym wywołaniem.
    """
    from polars_bio.context import ctx as _pb_ctx
    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def _make_overlap_udtf():
    """
    Buduje i dekoruje klasę UDTF DOPIERO PO utworzeniu sesji Spark Connect.

    WAŻNE (nieoczywista pułapka): dekorator @udtf sprawdza tryb "remote" (Connect
    vs klasyczny PySpark) w MOMENCIE DEFINICJI klasy (pyspark.sql.utils.is_remote()),
    nie w momencie rejestracji/wywołania. Zdefiniowanie klasy na poziomie modułu
    (przed spark.builder.remote(...).getOrCreate()) tworzy klasyczny
    UserDefinedTableFunction, którego connect-owy spark.udtf.register() odrzuca
    błędem "expected a 'UserDefinedTableFunction'" — mylącym, bo obiekt faktycznie
    JEST instancją tej klasy, tylko złej odmiany (classic vs connect).
    """

    @udtf(returnType=OVERLAP_UDTF_RETURN_TYPE)
    class OverlapUDTF:
        """
        Skalarny UDTF: overlap_udtf(rows_a, rows_b), gdzie rows_a/rows_b to
        array<struct<start,end,name>> — cała zawartość jednego chromosomu,
        zebrana wcześniej przez groupBy("chrom").agg(collect_list(...)).
        eval() wywoływane raz na wiersz wejściowy = raz na chromosom.
        """

        def eval(self, chrom, rows_a, rows_b):
            if not rows_a or not rows_b:
                return

            df_a = pd.DataFrame(
                [(chrom, r["start"], r["end"], r["name"]) for r in rows_a],
                columns=SCHEMA,
            )
            df_b = pd.DataFrame(
                [(chrom, r["start"], r["end"], r["name"]) for r in rows_b],
                columns=SCHEMA,
            )
            df_a.attrs["coordinate_system_zero_based"] = True
            df_b.attrs["coordinate_system_zero_based"] = True

            _reset_pb_context()

            result = pb.overlap(
                df_a, df_b,
                cols1=["chrom", "start", "end"],
                cols2=["chrom", "start", "end"],
                output_type="pandas.DataFrame",
            )

            if result is None or len(result) == 0:
                return

            for _, r in result.iterrows():
                yield (
                    r["chrom_1"],
                    int(r["start_1"]), int(r["end_1"]), r["name_1"],
                    int(r["start_2"]), int(r["end_2"]), r["name_2"],
                )

    return OverlapUDTF


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
# Sail: prawdziwy UDTF (PR #1519), skalarny, dane zgrupowane wcześniej wg chrom
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

    spark.udtf.register("overlap_udtf", _make_overlap_udtf())

    df_a = spark.createDataFrame(INTERVALS_A, schema=SCHEMA).withColumn("source", F.lit("a"))
    df_b = spark.createDataFrame(INTERVALS_B, schema=SCHEMA).withColumn("source", F.lit("b"))
    df_combined = df_a.union(df_b)

    interval_struct = F.struct(F.col("start"), F.col("end"), F.col("name"))
    grouped = df_combined.groupBy("chrom").agg(
        F.collect_list(F.when(F.col("source") == F.lit("a"), interval_struct)).alias("rows_a"),
        F.collect_list(F.when(F.col("source") == F.lit("b"), interval_struct)).alias("rows_b"),
    )
    # DIAGNOSTYKA: hipoteza -- LATERAL + UDTF w Sailu (pysail 0.5.3) źle obsługuje
    # >1 partycję fizyczną tabeli zewnętrznej (obserwowano: brak wierszy z jednej
    # grupy, duplikacja innej). Wymuszamy 1 partycję, żeby to zweryfikować.
    grouped = grouped.repartition(1)

    t0 = time.perf_counter()
    # UDTF wołany dla każdego wiersza źródła (jeden wiersz = jeden chromosom) —
    # wymaga LATERAL JOIN żeby powiązać argumenty UDTF z kolumnami zewnętrznego
    # wiersza (standardowy wzorzec z bundled przykładu CountUDTF w PySpark).
    grouped.createOrReplaceTempView("grouped_by_chrom")
    result = spark.sql(
        """
        SELECT o.* FROM grouped_by_chrom g, LATERAL overlap_udtf(g.chrom, g.rows_a, g.rows_b) o
        """
    ).toPandas()
    elapsed = time.perf_counter() - t0

    spark.stop()
    server.stop()

    return result, elapsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  Genomic Overlap: polars-bio vs Sail+polars-bio (natywny UDTF)")
    print("=" * 60)

    print("\n[1/2] polars-bio (lokalnie)...")
    pb_result, pb_time = run_polars_bio()
    print(f"  Czas: {pb_time:.4f}s  |  Wiersze: {len(pb_result)}")
    print(pb_result)

    print("\n[2/2] Sail + polars-bio (UDTF skalarny + groupBy/collect_list)...")
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

"""
Faza H krok 5: Sail liczy UDTF na WIELU partycjach — bez obejścia .repartition(1).

Tło i korekta wcześniejszej diagnozy
------------------------------------
W Fazie B zapisano znalezisko: „bug Saila — LATERAL nad tabelą na >1 partycji
fizycznej gubi/dubluje wiersze", a jako obejście wprowadzono `.repartition(1)`
przed każdym wywołaniem UDTF. Obejście działało, ale kosztowało CAŁĄ
równoległość — Sail liczył wszystko w jednej partycji.

Eksperymenty w Fazie H pokazały, że **ta atrybucja była błędna**:

| wariant (wszystkie BEZ repartition(1), wiele partycji)      | wynik |
|-------------------------------------------------------------|-------|
| UDTF czysto pythonowy (merge napisany ręcznie, zero pb)      | 3/3   |
| UDTF -> pb.merge() bez resetu kontekstu                      | 1/3   |
| UDTF -> pb.merge() z _reset_pb_context()                     | 0/3   |
| applyInPandas -> pb.merge() (inna ścieżka kodowa Saila)      | 0/3   |
| UDTF -> pb.merge() przez lock z importowalnego modułu        | 5/5   |

Ten sam kształt zapytania z funkcją czysto pythonową jest w 100% poprawny,
więc LATERAL i partycjonowanie w Sailu działają prawidłowo. Gubienie wierszy
pochodzi z **globalnego, mutowalnego kontekstu DataFusion w polars-bio**:
przy współbieżnym wykonaniu wielu partycji w jednym procesie wywołania `pb.*()`
nadpisują sobie zarejestrowane tabele (`s1`/`s2`). Pomocnik
`_reset_pb_context()`, który sam wprowadziliśmy, dodatkowo to pogarszał —
zamieniał wyścig w błąd deterministyczny, bo jawnie derejestrował tabele.

Rozwiązanie: `sail_pb_guard` — lock w OSOBNYM, importowalnym module (obiektu
locka nie da się wpiąć w domknięcie UDTF-a, bo cloudpickle go nie zserializuje;
moduł importowany po nazwie serializuje się przez referencję).

Znaczenie dla pracy: to nie jest ograniczenie Saila, tylko polars-bio — i jest
usuwalne bez modyfikowania którejkolwiek z tych bibliotek.

Uruchomienie: pytest tests/test_sail_parallel_udtf.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.merge_oracle import reference_merge_intervals

INTERVALS_A = [
    ("chr1", 100, 200, "gene_A1"),
    ("chr1", 150, 300, "gene_A2"),
    ("chr1", 400, 500, "gene_A3"),
    ("chr2", 50, 150, "gene_A4"),
    ("chr2", 200, 350, "gene_A5"),
]
SCHEMA = ["chrom", "start", "end", "name"]
MERGE_RETURN = "chrom: string, start: long, end: long"

# Wyścig jest niedeterministyczny, więc pojedyncze przejście niczego nie dowodzi.
# Bez locka wariant „pb.merge() bez resetu" dawał 1/3 — przy 3 powtórzeniach
# szansa, że regresja przejdzie niezauważona, jest już mała.
REPEATS = 3


def _make_merge_udtf():
    from pyspark.sql.functions import udtf

    @udtf(returnType=MERGE_RETURN)
    class MergeUDTF:
        def eval(self, chrom, rows):
            if not rows:
                return
            import sail_pb_guard

            df = pd.DataFrame(
                [(chrom, r["start"], r["end"], r["name"]) for r in rows], columns=SCHEMA
            )
            df.attrs["coordinate_system_zero_based"] = True
            res = sail_pb_guard.merge(df, output_type="pandas.DataFrame")
            if res is None or len(res) == 0:
                return
            for _, r in res.iterrows():
                yield (r["chrom"], int(r["start"]), int(r["end"]))

    return MergeUDTF


def _make_pure_python_udtf():
    """Ten sam kształt zapytania, ale bez ŻADNEGO wywołania polars-bio."""
    from pyspark.sql.functions import udtf

    @udtf(returnType=MERGE_RETURN)
    class PureMergeUDTF:
        def eval(self, chrom, rows):
            if not rows:
                return
            iv = sorted((int(r["start"]), int(r["end"])) for r in rows)
            cur_s, cur_e = iv[0]
            for s, e in iv[1:]:
                if s <= cur_e:
                    cur_e = max(cur_e, e)
                else:
                    yield (chrom, cur_s, cur_e)
                    cur_s, cur_e = s, e
            yield (chrom, cur_s, cur_e)

    return PureMergeUDTF


def _run_merge_udtf(factory) -> set[tuple[str, int, int]]:
    """Jedno pełne przejście: serwer -> sesja -> LATERAL UDTF -> wynik.

    Świadomie BEZ `.repartition(1)` — o to w tym teście chodzi.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address
    # .create(), nie .getOrCreate() — PySpark cachuje sesję jako singleton
    # procesu, a przy wielu serwerach w jednym procesie zwracałby martwą sesję.
    spark = SparkSession.builder.remote(f"sc://{ip}:{port}").create()
    try:
        spark.udtf.register("merge_udtf", factory())
        grouped = spark.createDataFrame(INTERVALS_A, schema=SCHEMA).groupBy("chrom").agg(
            F.collect_list(F.struct("start", "end", "name")).alias("rows")
        )
        grouped.createOrReplaceTempView("grouped_by_chrom")
        out = spark.sql(
            "SELECT o.* FROM grouped_by_chrom g, LATERAL merge_udtf(g.chrom, g.rows) o"
        ).toPandas()
        return {(r.chrom, int(r.start), int(r.end)) for _, r in out.iterrows()}
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        server.stop()


def test_sail_udtf_correct_without_repartition_workaround():
    """
    REGRESJA na znalezisko Fazy H: UDTF wołający polars-bio przez
    `sail_pb_guard` daje poprawny wynik na wielu partycjach, bez
    `.repartition(1)` — czyli z zachowaną równoległością.

    Gdyby ten test zaczął padać niedeterministycznie, znaczy to, że lock
    przestał być współdzielony między partycjami (np. bo UDTF zaczął go
    kopiować zamiast importować modułu po nazwie).
    """
    expected = reference_merge_intervals(INTERVALS_A)
    for attempt in range(REPEATS):
        actual = _run_merge_udtf(_make_merge_udtf)
        assert actual == expected, (
            f"Próba {attempt + 1}/{REPEATS}: rozjazd z wyrocznią pb.merge().\n"
            f"Brakuje: {expected - actual}\nNadmiar: {actual - expected}\n"
            f"To sygnał, że współbieżny dostęp do globalnego kontekstu polars-bio "
            f"nie jest już serializowany przez sail_pb_guard."
        )


def test_lateral_over_many_partitions_is_not_a_sail_bug():
    """
    ASERCJA DOKUMENTUJĄCA KOREKTĘ DIAGNOZY.

    Ten sam kształt zapytania (LATERAL + UDTF nad tabelą na wielu partycjach),
    ale z funkcją czysto pythonową — bez ani jednego wywołania polars-bio.
    Wynik jest poprawny, co dowodzi, że LATERAL i partycjonowanie w Sailu
    działają prawidłowo, a przyczyna gubienia wierszy leżała po stronie
    współdzielonego, mutowalnego stanu polars-bio.

    Test celowo NIE używa `sail_pb_guard` — ma pokazywać zachowanie Saila
    w izolacji od naszego obejścia.
    """
    expected = reference_merge_intervals(INTERVALS_A)
    actual = _run_merge_udtf(_make_pure_python_udtf)
    assert actual == expected, (
        f"Czysto pythonowy UDTF nad wieloma partycjami dał zły wynik — to "
        f"oznaczałoby, że w Sailu JEST jednak błąd w LATERAL/partycjonowaniu, "
        f"wbrew ustaleniom Fazy H.\n"
        f"Brakuje: {expected - actual}\nNadmiar: {actual - expected}"
    )

"""Plan 2, Zadanie 5: Sail czyta dane Parquet (spark.read.parquet, specyfikacja
9.3) i wykonuje pięć operacji przez uogólnione UDTF-y z `sail_bio.py`; wynik
zgodny z polars-bio na tych samych plikach (schemat znormalizowany, 8.4).

Zbiór testowy zawiera chromosom tylko w A (chrA) — wcześniejsze UDTF-y
(sail_coverage_subtract_udtf.py) pomijały taki chromosom w coverage.

Uruchomienie: pytest tests/test_sail_parquet.py -v
"""

from __future__ import annotations

import pytest

from bench.ops import OPS, OUTPUT_COLUMNS, UNARY_OPS, describe_diff, row_multiset


@pytest.fixture(scope="module")
def spark():
    import sail_bio

    with sail_bio.sail_session() as session:
        yield session


@pytest.mark.parametrize("op", OPS)
def test_sail_matches_polars_bio_on_parquet(op, spark, parquet_dirs, parquet_expected):
    import sail_bio

    a, b = parquet_dirs
    df = sail_bio.run_op(spark, op, a, None if op in UNARY_OPS else b)
    assert tuple(df.columns) == OUTPUT_COLUMNS[op]
    actual = row_multiset(op, df)
    assert actual == parquet_expected[op], f"{op}: {describe_diff(parquet_expected[op], actual)}"


def test_run_op_twice_in_one_session(spark, parquet_dirs, parquet_expected):
    """Pomiary (plan 3) wołają tę samą operację wiele razy w jednej sesji —
    ponowna rejestracja UDTF-a pod tą samą nazwą nie może zmienić wyniku."""
    import sail_bio

    for _ in range(2):
        df = sail_bio.run_op(spark, "merge", parquet_dirs[0])
        assert row_multiset("merge", df) == parquet_expected["merge"]


@pytest.mark.parametrize("op, with_right", [("overlap", False), ("merge", True)])
def test_run_op_rejects_wrong_number_of_tables(op, with_right, spark, parquet_dirs):
    import sail_bio

    a, b = parquet_dirs
    with pytest.raises(ValueError, match="requires"):
        sail_bio.run_op(spark, op, a, b if with_right else None)


def _hwm_mb() -> int:
    for line in open("/proc/self/status"):
        if line.startswith("VmHWM"):
            return int(line.split()[1]) // 1024
    raise RuntimeError("no VmHWM in /proc/self/status")


def test_sail_memory_does_not_scale_with_output_times_group(spark, tmp_path):
    """LATERAL w Sailu dokleja do KAŻDEGO wiersza wyniku UDTF-a cały wiersz zewnętrzny —
    razem z listą wszystkich przedziałów chromosomu. Gdy UDTF zwracał wiersz na przedział,
    pamięć rosła jak (wiersze wyniku) × (rozmiar grupy): na prawdziwych danych (para 1-2)
    proces przekraczał 2,5 GB i był zabijany, a czysto pythonowy UDTF bez polars-bio
    zachowywał się tak samo (5000 przedziałów w grupie: +3,8 GB). 2000 rozłącznych
    przedziałów na jednym chromosomie wystarczy, by wykryć powrót powielania (wzrost
    kwadratowy), a przy regresji nie grozi wywróceniem WSL."""
    import sail_bio
    from tests.parquet_fixture import write_parts

    n = 2000
    d = write_parts([("chr1", i * 10, i * 10 + 5) for i in range(n)], tmp_path / "chr1", n_files=1)
    with open("/proc/self/clear_refs", "w") as f:
        f.write("5")  # zeruje licznik szczytu: VmHWM = bieżący RSS
    start = _hwm_mb()
    df = sail_bio.run_op(spark, "merge", d)
    growth = _hwm_mb() - start
    assert len(df) == n
    assert growth < 300, f"peak memory grew by {growth} MB with {n} intervals in one group"

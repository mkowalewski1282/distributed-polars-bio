"""Plan 2, Zadanie 5: Sail czyta dane Parquet (spark.read.parquet, specyfikacja
9.3) i wykonuje pięć operacji przez uogólnione UDTF-y z `sail_bio.py`; wynik
zgodny z polars-bio na tych samych plikach (schemat znormalizowany, 8.4).

Zbiór testowy zawiera chromosom tylko w A (chrA) — wcześniejsze UDTF-y
(sail_coverage_subtract_udtf.py) pomijały taki chromosom w coverage.

Uruchomienie: pytest tests/test_sail_parquet.py -v (import polars-bio trwa kilka minut)
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
    with pytest.raises(ValueError, match="wymaga"):
        sail_bio.run_op(spark, op, a, b if with_right else None)

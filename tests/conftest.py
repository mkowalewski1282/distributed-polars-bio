"""Wspólna konfiguracja testów."""

import os

# Jak w bench/__init__.py: bez tego każdy import polars-bio w testach czeka ~270 s na serwer X
# z DISPLAY. Ustawiane przed importem czegokolwiek, co ładuje polars-bio; procesy potomne
# testów dziedziczą tę zmienną.
os.environ.setdefault("MPLBACKEND", "Agg")

import pytest

#: Zmienne sterujące klientem dist_ops: tryb zdalny i katalog wyników. Testy
#: trybu standalone muszą działać niezależnie od tego, co jest wyeksportowane
#: w powłoce — inaczej mogłyby cicho liczyć zdalnie albo sprawdzać stare wyniki
#: z ballista_genomics/output/. Testy klastra z osobnych procesów ustawiają te
#: zmienne jawnie przy każdym wywołaniu klienta.
#: BIO_TARGET_PARTITIONS — liczba partycji sesji Ballisty; testy zakładają domyślne 4.
CLIENT_ENV_VARS = ("BALLISTA_SCHEDULER_URL", "DIST_OUTPUT_DIR", "BIO_TARGET_PARTITIONS")


@pytest.fixture(autouse=True)
def _isolate_client_env(monkeypatch):
    for name in CLIENT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(scope="session")
def parquet_dirs(tmp_path_factory):
    """Zbiór testowy w układzie databio-8p (tests/parquet_fixture.py): katalogi A i B."""
    from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts

    root = tmp_path_factory.mktemp("parquet_fixture")
    return write_parts(FIXTURE_A, root / "a"), write_parts(FIXTURE_B, root / "b")


@pytest.fixture(scope="session")
def parquet_expected(parquet_dirs):
    """Wynik polars-bio na zbiorze testowym: operacja -> multizbiór wierszy.
    Raz na sesję — wspólny dla testów wszystkich silników."""
    from bench.ops import OPS, UNARY_OPS, row_multiset
    from tests.generic_oracle import read_intervals, reference

    a, b = (read_intervals(d) for d in parquet_dirs)
    return {
        op: row_multiset(op, reference(op, a, None if op in UNARY_OPS else b)) for op in OPS
    }


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "dane: wymaga pobranego zbioru databio-8p (python -m bench.data.download)",
    )

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
        "real_data: needs the downloaded databio-8p dataset (uv run python -m bench.data.download)",
    )
    config.addinivalue_line(
        "markers",
        "parallelism(p): needs p CPUs outside the system core (bench/cpus.py); "
        "skipped when the machine has fewer or its CPU topology is unreadable",
    )


def pytest_runtest_setup(item):
    """Znacznik parallelism(p): test uruchamiający silnik z P CPU (specyfikacja etapu 4, 5.2) jest
    pomijany, gdy układ CPU tej maszyny nie ma P CPU poza rdzeniem systemowym (CI: 4 vCPU → P ≤ 2)
    albo topologii nie da się odczytać."""
    marker = item.get_closest_marker("parallelism")
    if marker is None:
        return
    from bench import cpus

    p = marker.args[0]
    try:
        available = cpus.max_parallelism(cpus.read_layout())
    except cpus.TopologyError as e:
        pytest.skip(f"needs P = {p} CPUs; CPU topology unreadable: {e}")
    if available < p:
        pytest.skip(f"needs P = {p} CPUs; this machine has {available} CPUs outside the system core")

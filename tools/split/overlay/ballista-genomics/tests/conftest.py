"""Shared test configuration."""

import os

# Without this, every polars-bio import in the tests waits ~270 s for an X server from DISPLAY
# (matplotlib backend selection). Set before importing anything that loads polars-bio; child
# processes started by the tests inherit the variable.
os.environ.setdefault("MPLBACKEND", "Agg")

import pytest

#: Variables that control the dist_ops client: remote mode and the results directory. Standalone-mode
#: tests must work regardless of what is exported in the shell, otherwise they could silently
#: compute remotely or check stale results from output/. Cluster tests that run in separate
#: processes set these variables explicitly on every client call.
#: BIO_TARGET_PARTITIONS is the number of Ballista session partitions; the tests assume the default of 4.
CLIENT_ENV_VARS = ("BALLISTA_SCHEDULER_URL", "DIST_OUTPUT_DIR", "BIO_TARGET_PARTITIONS")


@pytest.fixture(autouse=True)
def _isolate_client_env(monkeypatch):
    for name in CLIENT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(scope="session")
def parquet_dirs(tmp_path_factory):
    """Test dataset in the databio-8p layout (tests/parquet_fixture.py): directories A and B."""
    from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts

    root = tmp_path_factory.mktemp("parquet_fixture")
    return write_parts(FIXTURE_A, root / "a"), write_parts(FIXTURE_B, root / "b")


@pytest.fixture(scope="session")
def parquet_expected(parquet_dirs):
    """polars-bio result on the test dataset: operation -> row multiset.
    Computed once per session and shared by the tests."""
    from tests.generic_oracle import read_intervals, reference
    from tests.support.ops import OPS, UNARY_OPS, row_multiset

    a, b = (read_intervals(d) for d in parquet_dirs)
    return {
        op: row_multiset(op, reference(op, a, None if op in UNARY_OPS else b)) for op in OPS
    }


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_data: needs the full databio-8p dataset (excluded in CI with -m 'not real_data')",
    )
    config.addinivalue_line(
        "markers",
        "parallelism(p): needs p CPUs outside the system core (tests/support/cpus.py); "
        "skipped when the machine has fewer or its CPU topology is unreadable",
    )


def pytest_runtest_setup(item):
    """Marker parallelism(p): a test that runs an engine with P CPUs is skipped when this machine
    does not have P CPUs outside the system core (CI: 4 vCPUs -> P <= 2) or when the topology
    cannot be read."""
    marker = item.get_closest_marker("parallelism")
    if marker is None:
        return
    from tests.support import cpus

    p = marker.args[0]
    try:
        available = cpus.max_parallelism(cpus.read_layout())
    except cpus.TopologyError as e:
        pytest.skip(f"needs P = {p} CPUs; CPU topology unreadable: {e}")
    if available < p:
        pytest.skip(f"needs P = {p} CPUs; this machine has {available} CPUs outside the system core")

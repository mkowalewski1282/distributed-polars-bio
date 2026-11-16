"""Shared fixtures: a local Sail session, the test dataset and the polars-bio reference result."""

import os

# Without this, every polars-bio import waits ~270 s for an X server from DISPLAY (matplotlib
# backend selection). Set before importing anything that loads polars-bio; the Sail server runs
# in this process and child processes inherit the variable.
os.environ.setdefault("MPLBACKEND", "Agg")

import pytest


@pytest.fixture(scope="session")
def spark():
    """A local Sail server with a Spark Connect session, shared by the tests of a session."""
    from sail_genomics import local_session

    with local_session() as session:
        yield session


@pytest.fixture(scope="session")
def parquet_dirs(tmp_path_factory):
    """Test dataset in the databio-8p layout (tests/parquet_fixture.py): directories A and B."""
    from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts

    root = tmp_path_factory.mktemp("parquet_fixture")
    return write_parts(FIXTURE_A, root / "a"), write_parts(FIXTURE_B, root / "b")


@pytest.fixture(scope="session")
def parquet_expected(parquet_dirs):
    """polars-bio result on the test dataset (tests/generic_oracle.py, columns of the normalised
    schema from sail_genomics.schema): operation -> row multiset."""
    from sail_genomics.schema import OPS, UNARY_OPS
    from tests.generic_oracle import read_intervals, reference
    from tests.rows import row_multiset

    a, b = (read_intervals(d) for d in parquet_dirs)
    return {
        op: row_multiset(op, reference(op, a, None if op in UNARY_OPS else b)) for op in OPS
    }

"""Pula kontekstów polars-bio — rozwiązanie pomostowe (plan 3b-2, Zadanie 8; specyfikacja 5.1).

Podmiana polars_bio.range_op.range_operation (sail_pb_context) ma dawać wyniki identyczne
z publicznym pb.*(), także przy równoczesnych wywołaniach w wątkach bez blokady. Test pilnuje też
wnętrza polars-bio: gdy nowa wersja zmieni sposób wywołania range_operation, wyniki się rozjadą.

Bez podmiany równoczesne wywołania kończą się błędem „The table s1 already exists” (sonda 3b-2) —
tego nie testujemy wprost, bo wyścig na małych danych bywa niewidoczny."""

from __future__ import annotations

import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import polars_bio as pb
import polars_bio.range_op as range_op
import pytest
from polars_bio._metadata import set_coordinate_system

from bench.ops import OPS, UNARY_OPS
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B

REPO = Path(__file__).resolve().parent.parent
COLS = ["chrom", "start", "end"]
SCHEMA = {"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64}


def _zero_based(df: pl.DataFrame) -> pl.DataFrame:
    set_coordinate_system(df, True)
    return df


def _by_chrom(rows) -> dict[str, pl.DataFrame]:
    df = pl.DataFrame(rows, schema=SCHEMA, orient="row")
    return {c: _zero_based(df.filter(pl.col("chrom") == c)) for c in sorted(set(df["chrom"]))}


LEFT, RIGHT = _by_chrom(FIXTURE_A), _by_chrom(FIXTURE_B)


def _call(op: str, chrom: str) -> pl.DataFrame:
    """Jedna operacja polars-bio na jednym chromosomie — jak funkcja Saila na jednej grupie."""
    left = LEFT[chrom]
    right = RIGHT[chrom] if chrom in RIGHT else _zero_based(pl.DataFrame(schema=SCHEMA))
    if op in UNARY_OPS:
        result = pb.merge(left, cols=COLS, output_type="polars.DataFrame")
    else:
        result = getattr(pb, op)(left, right, cols1=COLS, cols2=COLS, output_type="polars.DataFrame")
    return result.sort(result.columns)


@pytest.fixture
def bridge():
    """Moduł pomostu z WYŁĄCZONĄ podmianą — test włącza ją jawnie; po teście przywracany oryginał,
    żeby pozostałe testy w tym procesie liczyły na globalnym kontekście polars-bio."""
    import sail_pb_context

    sail_pb_context.uninstall()  # import instaluje podmianę
    yield sail_pb_context
    sail_pb_context.uninstall()


@pytest.mark.parametrize("op", OPS)
def test_threads_without_lock_match_plain_polars_bio(op, bridge):
    expected = {chrom: _call(op, chrom) for chrom in LEFT}
    bridge.install()
    jobs = list(LEFT) * 6
    before = bridge.contexts_created()
    for _ in range(3):
        with ThreadPoolExecutor(6) as pool:
            results = list(pool.map(lambda chrom: (chrom, _call(op, chrom)), jobs))
        mismatched = sorted({chrom for chrom, got in results if not got.equals(expected[chrom])})
        assert not mismatched, f"{op}: results differ for {mismatched}"
    # Nigdy więcej kontekstów niż równoczesnych wywołań (6 wątków roboczych).
    assert 0 <= bridge.contexts_created() - before <= 6


def test_install_is_idempotent_and_uninstall_restores_polars_bio(bridge):
    original = range_op.range_operation
    bridge.install()
    bridge.install()
    assert bridge.installed() and range_op.range_operation is bridge._range_operation
    bridge.uninstall()
    assert not bridge.installed() and range_op.range_operation is original is bridge._original


def test_sequential_calls_from_new_threads_reuse_one_context(bridge):
    """Jak w Sailu: kolejne wywołania, każde z nowego, krótkotrwałego wątku — pula ma utworzyć
    najwyżej jeden kontekst (implementacja na wątek utworzyłaby 10)."""
    chrom = next(iter(LEFT))
    expected = _call("overlap", chrom)
    bridge.install()
    before = bridge.contexts_created()
    for _ in range(10):
        out = []
        worker = threading.Thread(target=lambda: out.append(_call("overlap", chrom)))
        worker.start()
        worker.join()
        assert out[0].equals(expected)
    assert bridge.contexts_created() - before <= 1


def test_import_installs_the_bridge():
    """Proces serwera Saila importuje moduł po nazwie — sam import ma włączyć podmianę."""
    code = (
        "import polars_bio.range_op as r, sail_pb_context as s; "
        "print(r.range_operation is s._range_operation)"
    )
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    assert r.stdout.strip() == "True"

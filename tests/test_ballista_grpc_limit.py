"""Strażnik broadcastu (plan 3b-2, Zadanie 14; specyfikacja 6.2): gdy ładunki z tabelą rozsyłaną
nie mieszczą się w limicie wiadomości gRPC, zapytanie kończy się czytelnym błędem — zamiast
wisieć do timeoutu, jak robi to Ballista 53 w trybie push (uznaje executor za utraconego i ponawia).

Tabela rozsyłana w coverage to prawa strona (df2): 1,2 mln przedziałów ≈ 23 MiB w Arrow IPC
(string + int32; po odczycie w Ballisście Utf8View/Int64 ≈ 37 MiB), czyli ponad domyślne 16 MiB
Ballisty także bez marginesu 2 — test dodatni dowodzi więc, że podniesiony limit 512 MiB dociera do transportu. Wymaga binarek debug (brak = BŁĄD, jak w P0)."""

from __future__ import annotations

import subprocess
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from bench import engines
from tests.parquet_fixture import FIXTURE_A, write_parts

BIG_ROWS = 1_200_000


@pytest.fixture(scope="module")
def pair(tmp_path_factory):
    root = tmp_path_factory.mktemp("big_broadcast")
    left = write_parts(FIXTURE_A, root / "a")
    right = root / "b"
    right.mkdir()
    starts = pa.array(range(0, 10 * BIG_ROWS, 10), pa.int32())
    pq.write_table(
        pa.table({"contig": pa.array(["chr1"] * BIG_ROWS, pa.string()), "pos_start": starts,
                  "pos_end": pc.add(starts, 5)}),
        right / "part-00000.parquet",
    )
    return left, right


def run_coverage(engine, left, right, timeout: int = 180):
    argv = [str(engines.ballista_binary("bench_client", "debug")), "--op", "coverage",
            "--left", str(left), "--right", str(right)]
    env = {**engine.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{engine.port}"}
    t0 = time.monotonic()
    r = subprocess.run(argv, cwd=engines.BALLISTA_DIR, env=env, capture_output=True, text=True, timeout=timeout)
    return r, time.monotonic() - t0


def cluster(tmp_path, limit: int):
    return engines.BallistaEngine(
        1, profile="debug", data_root=tmp_path, log_dir=tmp_path / "logs", grpc_max_message_bytes=limit,
    )


@pytest.mark.nodes(1)
def test_broadcast_over_the_limit_fails_fast_with_a_clear_error(pair, tmp_path):
    engine = cluster(tmp_path, 16 * 2**20)
    engine.start()
    try:
        r, elapsed = run_coverage(engine, *pair)
    finally:
        engine.stop()
    assert r.returncode == 1, f"stdout: {r.stdout}\nstderr: {r.stderr[-3000:]}"
    assert "exceeds the gRPC message limit" in r.stderr and "BALLISTA_GRPC_MAX_MESSAGE_BYTES" in r.stderr
    assert elapsed < 60


@pytest.mark.nodes(1)
def test_same_broadcast_fits_under_the_default_limit(pair, tmp_path):
    engine = cluster(tmp_path, engines.GRPC_MAX_MESSAGE_BYTES)
    engine.start()
    try:
        r, _ = run_coverage(engine, *pair)
    finally:
        engine.stop()
    assert r.returncode == 0, r.stderr[-3000:]
    assert '"rows": 12' in r.stdout  # coverage: wiersz na każdy przedział lewej tabeli (FIXTURE_A)

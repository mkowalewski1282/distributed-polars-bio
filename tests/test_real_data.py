"""Plan 2, Zadanie 6: test akceptacyjny na prawdziwych danych databio-8p.

Para 1-2 (fBrain-DS14718 × exons; merge: zbiór 1) — najmniejsza para macierzy
lokalnej (specyfikacja, sekcja 7.1) — w trzech wariantach wykonania: Ballista
standalone, Ballista z osobnych procesów (P0) i Sail. Każdy wynik porównywany
z polars-bio na tych samych plikach. Bez pomiarów czasu.

To test akceptacyjny już zaimplementowanych ścieżek (Zadania 4–5), więc ma
przejść od razu; to, że to porównanie potrafi się nie udać, pokazują testy
Zadań 3–5 na tym samym `row_multiset`.

Wymaga: python -m bench.data.download (dane poza repozytorium) oraz binarek
bench_client i ballista_node. Brak danych = pominięcie, brak binarek = błąd.
Uruchomienie: pytest tests/test_real_data.py -v
"""

from __future__ import annotations

import os
import subprocess

import pytest

from bench.data.datasets import DATASETS, data_dir, resolve
from bench.data.download import expected_rows, verify
from bench.ops import OPS, UNARY_OPS, describe_diff, row_multiset
from tests.test_ballista_multiprocess import _start_cluster
from tests.test_ballista_parquet import check_result, op_args, run_client

PAIR = "1-2"

#: Tryb standalone Ballisty 53 (scheduler i executor w jednym procesie, tryb pull) nie
#: udźwiga operacji z broadcastem na prawdziwych danych: executor pobiera zadania klientem
#: gRPC z domyślnym limitem tonic 4 MiB (niezmienialnym przez standalone_with_state), a odpowiedź
#: z kilkoma zadaniami — każde niesie całą tabelę broadcastowaną — jest większa (zmierzono
#: 9 808 588 B już przy 100 tys. wierszy broadcastu). Executor ponawia pobranie w nieskończoność,
#: więc zapytanie wisi, zamiast zakończyć się błędem. Pomiary używają klastra z osobnych
#: procesów (tryb push, limit 16 MiB) — test_ballista_cluster_on_1_2. `strict`: gdy standalone
#: zacznie działać, test to zgłosi.
STANDALONE_HANG_TIMEOUT_S = 90
_STANDALONE_BROADCAST_HANGS = pytest.mark.xfail(
    raises=subprocess.TimeoutExpired,
    strict=True,
    reason="standalone: limit 4 MiB klienta gRPC executora przy broadcaście (specyfikacja 9.2)",
)
STANDALONE_CASES = [
    pytest.param(op, marks=_STANDALONE_BROADCAST_HANGS) if op in ("nearest", "coverage") else op
    for op in OPS
]
_NEEDED = {DATASETS[1], DATASETS[2]}
_PROBLEMS = verify(data_dir(), {n: r for n, r in expected_rows().items() if n in _NEEDED})

pytestmark = [
    pytest.mark.real_data,
    pytest.mark.skipif(
        bool(_PROBLEMS), reason=f"brak kompletnych danych pary {PAIR} w {data_dir()}: {_PROBLEMS}"
    ),
]


@pytest.fixture(scope="module")
def real_dirs():
    return resolve(PAIR)


@pytest.fixture(scope="module")
def real_expected(real_dirs):
    from tests.generic_oracle import read_intervals, reference

    left, right = real_dirs
    a, b = read_intervals(left), read_intervals(right)
    return {
        op: row_multiset(op, reference(op, a, None if op in UNARY_OPS else b)) for op in OPS
    }


@pytest.mark.parametrize("op", STANDALONE_CASES)
def test_ballista_standalone_on_1_2(op, real_dirs, real_expected, tmp_path):
    out = tmp_path / f"{op}.parquet"
    timeout = STANDALONE_HANG_TIMEOUT_S if op in ("nearest", "coverage") else 900
    r = run_client(op_args(op, *real_dirs, out), timeout=timeout)
    check_result(op, r, out, real_expected[op])


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    c = _start_cluster(
        tmp_path_factory.mktemp("plan2_dane_klaster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


@pytest.mark.parametrize("op", OPS)
def test_ballista_cluster_on_1_2(op, cluster, real_dirs, real_expected, tmp_path):
    out = tmp_path / f"{op}.parquet"
    env = {**os.environ, "BALLISTA_SCHEDULER_URL": cluster.url}
    r = run_client(op_args(op, *real_dirs, out), env=env, timeout=900)
    check_result(op, r, out, real_expected[op])


@pytest.fixture(scope="module")
def spark():
    import sail_bio

    with sail_bio.sail_session() as session:
        yield session


@pytest.mark.parametrize("op", OPS)
def test_sail_on_1_2(op, spark, real_dirs, real_expected):
    import sail_bio

    left, right = real_dirs
    df = sail_bio.run_op(spark, op, left, None if op in UNARY_OPS else right)
    actual = row_multiset(op, df)
    assert actual == real_expected[op], f"{op}: {describe_diff(real_expected[op], actual)}"

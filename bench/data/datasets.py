"""Rejestr zbiorów databio-8p (polars-bio-bench) i rozwiązywanie identyfikatorów.

Identyfikatory jak w polars-bio-bench (`conf/common.yaml`): zbiór to cyfra 0–8,
para `a-b` to df1 = zbiór a, df2 = zbiór b. Dane leżą poza repozytorium, w tym
samym układzie co w polars-bio-bench:
`$BENCH_DATA_ROOT/databio-8p/<katalog zbioru>/part-*.parquet`.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

DATA_ROOT_ENV = "BENCH_DATA_ROOT"
DEFAULT_DATA_ROOT = Path.home() / "bench_data"
DATASET_NAME = "databio-8p"

#: Kolumny przedziału w plikach databio-8p (0-based, półotwarte).
COLUMNS = ("contig", "pos_start", "pos_end")

#: Identyfikator -> katalog zbioru w archiwum databio-8p.zip.
DATASETS: dict[int, str] = {
    0: "chainRn4",
    1: "fBrain-DS14718",
    2: "exons",
    3: "chainOrnAna1",
    4: "chainVicPac2",
    5: "chainXenTro3Link",
    6: "chainMonDom5Link",
    7: "ex-anno",
    8: "ex-rna",
}

#: Liczba wierszy w tysiącach (specyfikacja, sekcja 4) — do weryfikacji pobrania.
EXPECTED_ROWS_K: dict[int, int] = {
    0: 2351,
    1: 199,
    2: 439,
    3: 1957,
    4: 7684,
    5: 50981,
    6: 128187,
    7: 1194,
    8: 9945,
}

_SCENARIO = re.compile(r"([0-8])(?:-([0-8]))?")


def data_dir(root: Path | None = None) -> Path:
    """Katalog zbioru: `root` albo `$BENCH_DATA_ROOT` (pusta = brak) albo domyślny.
    `~` jest rozwijane — procesy silników działają w innych katalogach niż powłoka."""
    if root is None:
        env = os.environ.get(DATA_ROOT_ENV, "")
        root = Path(env) if env else DEFAULT_DATA_ROOT
    return Path(root).expanduser() / DATASET_NAME


def dataset_dir(idx: int, root: Path | None = None) -> Path:
    if idx not in DATASETS:
        raise ValueError(f"nieznany zbiór {idx!r}; dozwolone: 0–8")
    return data_dir(root) / DATASETS[idx]


def resolve(scenario: str, root: Path | None = None) -> tuple[Path, Path | None]:
    """`"a-b"` -> (katalog a, katalog b); `"a"` -> (katalog a, None)."""
    m = _SCENARIO.fullmatch(scenario)
    if m is None:
        raise ValueError(
            f"niepoprawny identyfikator danych {scenario!r}; oczekiwano 'a' albo 'a-b', a, b ∈ 0–8"
        )
    left = dataset_dir(int(m.group(1)), root)
    right = dataset_dir(int(m.group(2)), root) if m.group(2) is not None else None
    return left, right

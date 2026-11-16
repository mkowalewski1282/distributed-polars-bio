"""Registry of the databio-8p datasets (polars-bio-bench) and identifier resolution.

Identifiers as in polars-bio-bench (`conf/common.yaml`): a dataset is a digit 0–8, a pair
`a-b` means df1 = dataset a, df2 = dataset b. The data lives outside the repository, in the
same layout as in polars-bio-bench:
`$BENCH_DATA_ROOT/databio-8p/<dataset directory>/part-*.parquet`.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

DATA_ROOT_ENV = "BENCH_DATA_ROOT"
DEFAULT_DATA_ROOT = Path.home() / "bench_data"
DATASET_NAME = "databio-8p"

#: Interval columns in the databio-8p files (0-based, half-open).
COLUMNS = ("contig", "pos_start", "pos_end")

#: Identifier -> dataset directory in the databio-8p.zip archive.
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

#: Row counts in thousands — used to verify the download.
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
    """Dataset directory: `root`, or `$BENCH_DATA_ROOT` (empty = unset), or the default.
    `~` is expanded — engine processes run in different directories than the shell."""
    if root is None:
        env = os.environ.get(DATA_ROOT_ENV, "")
        root = Path(env) if env else DEFAULT_DATA_ROOT
    return Path(root).expanduser() / DATASET_NAME


def dataset_dir(idx: int, root: Path | None = None) -> Path:
    if idx not in DATASETS:
        raise ValueError(f"unknown dataset {idx!r}; allowed: 0–8")
    return data_dir(root) / DATASETS[idx]


def resolve(scenario: str, root: Path | None = None) -> tuple[Path, Path | None]:
    """`"a-b"` -> (directory a, directory b); `"a"` -> (directory a, None)."""
    m = _SCENARIO.fullmatch(scenario)
    if m is None:
        raise ValueError(
            f"invalid data identifier {scenario!r}; expected 'a' or 'a-b', a, b ∈ 0–8"
        )
    left = dataset_dir(int(m.group(1)), root)
    right = dataset_dir(int(m.group(2)), root) if m.group(2) is not None else None
    return left, right

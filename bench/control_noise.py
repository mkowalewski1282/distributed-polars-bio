"""Badanie szumu przebiegu kontrolnego (specyfikacja 3b-2, sekcja 4.2).

Seria bench/conf/control_noise.yaml mierzy zadanie kontrolne (overlap 7-0, polars-bio A) 20 razy
pod rząd. Ten moduł liczy z jej wyników, w jakim odsetku losowych podziałów czasów na dwie
rozłączne grupy po `group` (jak przebiegi kontrolne z początku i końca bloku) względna różnica
median przekracza próg — czyli odsetek fałszywych dryfów przy niezmienionej maszynie.
Kryterium specyfikacji: ≤ 5% przy progu 10% i grupach po 10.

Uruchomienie:
    uv run python -m bench.control_noise bench/results/control-noise/<znacznik czasu>"""

from __future__ import annotations

import argparse
import random
import statistics
import sys
from pathlib import Path
from typing import Sequence

import polars as pl

from bench.validity import drift


def false_drift_rate(times: Sequence[float], *, group: int, tolerance: float,
                     samples: int = 10000, seed: int = 0) -> float:
    """Odsetek losowych podziałów (ziarno `seed`) z dryfem median powyżej `tolerance`."""
    times = list(times)
    if len(times) < 2 * group:
        raise ValueError(f"the noise study needs at least {2 * group} times, got {len(times)}")
    rng = random.Random(seed)
    hits = 0
    for _ in range(samples):
        picked = rng.sample(times, 2 * group)
        if drift(statistics.median(picked[:group]), statistics.median(picked[group:])) > tolerance:
            hits += 1
    return hits / samples


def load_times(directory: Path) -> list[float]:
    """Czasy ważnych rund mierzonych polars-bio A z runs.parquet serii (bez wzorca, rozgrzewki
    i przebiegów kontrolnych bloku)."""
    df = pl.read_parquet(Path(directory) / "runs.parquet")
    measured = df.filter(
        pl.col("valid") & ~pl.col("is_reference") & ~pl.col("is_warmup") & ~pl.col("is_control")
        & (pl.col("variant") == "polars_bio_a")
    )
    return measured["t_total_s"].to_list()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bench.control_noise",
        description="False drift rate of the control run from a noise-study series.",
    )
    parser.add_argument("directory", type=Path, help="series results directory (with runs.parquet)")
    parser.add_argument("--group", type=int, default=10, help="control runs per block end (default 10)")
    parser.add_argument("--tolerance", type=float, default=0.10, help="drift threshold (default 0.10)")
    args = parser.parse_args(argv)
    times = load_times(args.directory)
    try:
        rate = false_drift_rate(times, group=args.group, tolerance=args.tolerance)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print(
        f"runs: {len(times)}, median {statistics.median(times):.3f} s, min {min(times):.3f} s, "
        f"max {max(times):.3f} s, spread {max(times) / min(times) - 1:.1%}"
    )
    print(
        f"false drift rate (two groups of {args.group}, tolerance {args.tolerance:.0%}): "
        f"{rate:.1%} (10000 random splits)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

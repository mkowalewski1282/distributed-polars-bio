"""Control-run noise study.

The series bench/conf/control_noise.yaml measures the control run (overlap 7-0, polars-bio with
2 threads on both hardware threads of the first core outside the system core) 20 times in a row
at the start of a block. From its results this module computes the share of random splits of the
times into two disjoint groups of `group` (like the control runs from the start and the end of
a block) in which the relative difference of the medians exceeds the threshold — that is, the
rate of false drifts on an unchanged machine.
Acceptance criterion: ≤ 5% at a 10% threshold and groups of 10.

Usage:
    uv run python -m bench.control_noise bench/results/control-noise/<timestamp>"""

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
    """Share of random splits (seeded with `seed`) whose median drift exceeds `tolerance`."""
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
    """Times of the valid control runs from the start of a block (the series' runs.parquet) —
    the control variant differs from the series variants, so the sample is the control runs
    alone; those from the end of the block are skipped so that the sample is a consecutive
    sequence."""
    df = pl.read_parquet(Path(directory) / "runs.parquet")
    controls = df.filter(
        pl.col("valid") & pl.col("is_control") & (pl.col("control_position") == "start")
    )
    return controls["t_total_s"].to_list()


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

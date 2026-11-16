"""Shared parts of the Python runners: scenario arguments, one JSON line on stdout and exit
codes as in bench_client — 0 success, 1 execution error (message on stderr), 2 bad arguments
(argparse)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

from bench.data.datasets import COLUMNS
from bench.ops import OPS, UNARY_OPS


def scenario_parser(prog: str, description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("--op", required=True, choices=OPS)
    parser.add_argument("--left", required=True, type=Path, help="Parquet file or directory of Parquet files")
    parser.add_argument("--right", type=Path, help="second table (operations other than merge)")
    parser.add_argument(
        "--cols", default=",".join(COLUMNS), help="contig,start,end (default: databio-8p columns)"
    )
    return parser


def parse_scenario(parser: argparse.ArgumentParser, argv: list[str] | None) -> argparse.Namespace:
    """Parse the arguments; --right inconsistent with the operation or a bad column list → code 2.
    Paths become absolute (the runner may run in a different directory than its caller)."""
    args = parser.parse_args(argv)
    if args.op in UNARY_OPS and args.right is not None:
        parser.error(f"{args.op} takes a single table (no --right)")
    if args.op not in UNARY_OPS and args.right is None:
        parser.error(f"{args.op} requires --right")
    cols = tuple(args.cols.split(","))
    if len(cols) != 3 or not all(cols):
        parser.error(f"--cols: expected three non-empty comma-separated names, got {args.cols!r}")
    args.cols = cols
    args.left = args.left.expanduser().resolve()
    if args.right is not None:
        args.right = args.right.expanduser().resolve()
    return args


def require_paths(*paths: Path | None) -> None:
    """Missing data → an execution error (code 1) naming the path, as in bench_client."""
    for path in paths:
        if path is not None and not path.exists():
            raise FileNotFoundError(f"missing data: {path}")


def report_line(*, rows: int, checksum: str, t_total_s: float, peak_rss_bytes: int,
                phases: dict | None = None, extra: dict | None = None) -> str:
    return json.dumps({
        "rows": rows,
        "checksum": checksum,
        "t_total_s": t_total_s,
        "phases": phases or {},
        "extra": extra or {},
        "peak_rss_bytes": peak_rss_bytes,
    })


def main_guard(prog: str, body: Callable[[], str]) -> int:
    """Execute the runner: a JSON line on stdout (code 0) or a message on stderr (code 1)."""
    try:
        line = body()
    except Exception as e:  # noqa: BLE001 — any engine error is an invalid run with a reason
        print(f"{prog}: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    print(line, flush=True)
    return 0

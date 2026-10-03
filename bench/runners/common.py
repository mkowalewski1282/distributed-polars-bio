"""Wspólne elementy runnerów w Pythonie (specyfikacja 8.3): argumenty scenariusza, jedna
linia JSON na stdout i kody wyjścia jak w bench_client — 0 sukces, 1 błąd wykonania
(komunikat na stderr), 2 błędne argumenty (argparse)."""

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
    """Parsuje argumenty; niezgodność --right z operacją albo zła lista kolumn → kod 2.
    Ścieżki stają się bezwzględne (runner może działać w innym katalogu niż wywołujący)."""
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
    """Brak danych → błąd wykonania (kod 1) z nazwą ścieżki, jak w bench_client."""
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
    """Wykonuje runner: linia JSON na stdout (kod 0) albo komunikat na stderr (kod 1)."""
    try:
        line = body()
    except Exception as e:  # noqa: BLE001 — każdy błąd silnika to nieważny przebieg z przyczyną
        print(f"{prog}: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    print(line, flush=True)
    return 0

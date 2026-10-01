"""Fałszywy runner do testów orkiestratora (tests/bench/test_orchestrator.py): zachowanie
sterowane argumentami, wynik w protokole 8.3."""

import argparse
import json
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=3)
parser.add_argument("--checksum", default="0x00000000000000aa")
parser.add_argument("--t", type=float, default=0.25)
parser.add_argument("--t-file", type=Path, help="kolejne czasy, po jednym na wywołanie")
parser.add_argument("--sleep", type=float, default=0.0)
parser.add_argument("--touch", type=Path, help="plik tworzony na starcie (sygnał dla testu)")
parser.add_argument("--exit", type=int, default=0)
parser.add_argument("--garbage", action="store_true")
parser.add_argument("--stderr-mb", type=int, default=0)
args = parser.parse_args()

if args.touch:
    args.touch.touch()
if args.stderr_mb:
    line = "x" * 1023 + "\n"
    for _ in range(args.stderr_mb * 1024):
        sys.stderr.write(line)
time.sleep(args.sleep)
if args.exit:
    print("silnik padł: błąd testowy", file=sys.stderr)
    sys.exit(args.exit)
if args.garbage:
    print("to nie jest JSON")
    sys.exit(0)
t = args.t
if args.t_file:
    first, *rest = args.t_file.read_text().split()
    args.t_file.write_text("\n".join(rest))
    t = float(first)
print(json.dumps({
    "rows": args.rows, "checksum": args.checksum, "t_total_s": t, "phases": {}, "extra": {},
    "peak_rss_bytes": 1234,
}))

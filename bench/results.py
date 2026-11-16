"""Writing series results: one row per run, appended to JSON Lines (robust to an interrupted
series) and converted to Parquet at the end."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

#: A result row. Besides the basic fields it has is_reference, attempt, wall_s, extra,
#: pswpin_delta, control_position and base_rss; parallelism (formerly n_nodes), cpus and smt
#: describe the CPU layout taken from the topology.
SCHEMA = {
    "timestamp": pl.Utf8,
    "git_commit": pl.Utf8,
    "engine_versions": pl.Utf8,  # JSON: component -> version
    "series": pl.Utf8,
    "seed": pl.Int64,
    "scenario_id": pl.Utf8,
    "op": pl.Utf8,
    "pair": pl.Utf8,  # a pair "a-b" or a dataset "a"
    "algorithm": pl.Utf8,  # reserved (the algorithm parameter is not supported yet; always null)
    "variant": pl.Utf8,
    "parallelism": pl.Int64,  # P of the block; reference: 1; control run: 2 (polars-bio threads)
    "cpus": pl.Utf8,  # JSON: the engine's CPU list (Ballista: the executors' CPUs); "[]" — skipped block
    "smt": pl.Boolean,  # whether cpus includes SMT siblings; null — skipped block
    "rep": pl.Int64,  # round 1..R; 0 — warm-up, reference and skipped block; control runs: 1..control_repeats
    "attempt": pl.Int64,  # 1; 2 — the block repeated after a drift
    "is_warmup": pl.Boolean,
    "is_control": pl.Boolean,
    "is_reference": pl.Boolean,
    "control_position": pl.Utf8,  # "start" / "end" — a control run at the start / end of the block
    "valid": pl.Boolean,
    "invalid_reason": pl.Utf8,
    "rows": pl.Int64,
    "checksum": pl.Utf8,
    "t_total_s": pl.Float64,  # time measured by the runner
    "wall_s": pl.Float64,  # runner process time including startup (the series' duration)
    "phases": pl.Utf8,  # JSON
    "extra": pl.Utf8,  # JSON from the runner (e.g. target_partitions)
    "peak_rss": pl.Utf8,  # JSON: process -> bytes
    "peak_rss_sum": pl.Int64,  # an upper estimate of memory
    "base_rss": pl.Utf8,  # JSON: long-lived process -> RSS right after the peak was reset
    "shuffle_bytes": pl.Int64,
    "broadcast_bytes": pl.Int64,  # bytes of broadcast payloads (Ballista)
    "pswpout_delta": pl.Int64,
    "pswpin_delta": pl.Int64,  # recorded, and any swap-in also invalidates the run
}


class ResultsWriter:
    """Append rows to a JSON Lines file — each one straight to disk."""

    def __init__(self, path: Path):
        self.path = path

    def write(self, row: dict) -> None:
        unknown, missing = set(row) - set(SCHEMA), set(SCHEMA) - set(row)
        if unknown or missing:
            raise ValueError(
                f"result row: unknown fields {sorted(unknown)}, missing {sorted(missing)}"
            )
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def to_parquet(jsonl: Path, parquet: Path) -> int:
    """Convert JSON Lines to Parquet; a missing file → an empty table with the schema."""
    rows: list[dict] = []
    if jsonl.exists():
        rows = [
            json.loads(line)
            for line in jsonl.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    pl.DataFrame(rows, schema=SCHEMA).write_parquet(parquet)
    return len(rows)

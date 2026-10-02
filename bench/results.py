"""Zapis wyników serii (specyfikacja 8.5): wiersz na przebieg, dopisywany do JSON Lines
(odporny na przerwanie serii) i przepisywany na końcu do Parquet."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

#: Wiersz wyniku: pola specyfikacji 8.5 oraz is_reference, attempt, wall_s, extra i pswpin_delta
#: (plan 3a).
SCHEMA = {
    "timestamp": pl.Utf8,
    "git_commit": pl.Utf8,
    "engine_versions": pl.Utf8,  # JSON: komponent -> wersja
    "series": pl.Utf8,
    "seed": pl.Int64,
    "scenario_id": pl.Utf8,
    "op": pl.Utf8,
    "pair": pl.Utf8,  # para "a-b" albo zbiór "a"
    "algorithm": pl.Utf8,  # plan 3b
    "variant": pl.Utf8,
    "n_nodes": pl.Int64,
    "rep": pl.Int64,  # runda 1..R; 0 — rozgrzewka, wzorzec, kontrola na początku bloku; 1 — kontrola na końcu
    "attempt": pl.Int64,  # 1; 2 — powtórzenie bloku po dryfie
    "is_warmup": pl.Boolean,
    "is_control": pl.Boolean,
    "is_reference": pl.Boolean,
    "valid": pl.Boolean,
    "invalid_reason": pl.Utf8,
    "rows": pl.Int64,
    "checksum": pl.Utf8,
    "t_total_s": pl.Float64,  # czas zmierzony przez runner
    "wall_s": pl.Float64,  # czas procesu runnera razem ze startem (długość serii)
    "phases": pl.Utf8,  # JSON
    "extra": pl.Utf8,  # JSON z runnera (np. target_partitions)
    "peak_rss": pl.Utf8,  # JSON: proces -> bajty
    "peak_rss_sum": pl.Int64,  # górne oszacowanie pamięci (specyfikacja 5)
    "shuffle_bytes": pl.Int64,
    "broadcast_bytes": pl.Int64,  # plan 3b
    "pswpout_delta": pl.Int64,
    "pswpin_delta": pl.Int64,  # zapisywany; czy unieważnia przebieg — plan 3b
}


class ResultsWriter:
    """Dopisuje wiersze do pliku JSON Lines — każdy od razu na dysk."""

    def __init__(self, path: Path):
        self.path = path

    def write(self, row: dict) -> None:
        unknown, missing = set(row) - set(SCHEMA), set(SCHEMA) - set(row)
        if unknown or missing:
            raise ValueError(
                f"wiersz wyniku: nieznane pola {sorted(unknown)}, brakujące {sorted(missing)}"
            )
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def to_parquet(jsonl: Path, parquet: Path) -> int:
    """Przepisuje JSON Lines do Parquet; brak pliku → pusta tabela ze schematem."""
    rows: list[dict] = []
    if jsonl.exists():
        rows = [
            json.loads(line)
            for line in jsonl.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    pl.DataFrame(rows, schema=SCHEMA).write_parquet(parquet)
    return len(rows)

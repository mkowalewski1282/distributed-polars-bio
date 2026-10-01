"""Zapis wyników serii (plan 3a, Zadanie 8; specyfikacja 8.5)."""

from __future__ import annotations

import json

import polars as pl
import pytest

from bench.results import SCHEMA, ResultsWriter, to_parquet


def full_row(**overrides) -> dict:
    row = dict.fromkeys(SCHEMA)
    row.update(
        timestamp="2026-10-01T12:00:00", git_commit="abc", engine_versions=json.dumps({"ballista": "53.0.0"}),
        series="test", seed=7, scenario_id="overlap/1-2", op="overlap", pair="1-2", variant="ballista",
        n_nodes=2, rep=1, attempt=1, is_warmup=False, is_control=False, is_reference=False, valid=True,
        rows=3, checksum="0x00000000000000aa", t_total_s=0.5, wall_s=1.5, phases="{}",
        extra=json.dumps({"target_partitions": 4}), peak_rss=json.dumps({"runner": 10, "scheduler": 20}),
        peak_rss_sum=30, shuffle_bytes=100, pswpout_delta=0,
    )
    row.update(overrides)
    return row


def test_rows_round_trip_through_jsonl_and_parquet(tmp_path):
    first = full_row()
    second = full_row(rep=2, valid=False, invalid_reason="timeout", rows=None, checksum=None, t_total_s=None)
    ResultsWriter(tmp_path / "runs.jsonl").write(first)
    ResultsWriter(tmp_path / "runs.jsonl").write(second)  # dopisywanie (kolejny blok)
    assert to_parquet(tmp_path / "runs.jsonl", tmp_path / "runs.parquet") == 2
    df = pl.read_parquet(tmp_path / "runs.parquet")
    assert df.schema == pl.Schema(SCHEMA)
    assert df.to_dicts() == [first, second]
    assert json.loads(df["peak_rss"][0]) == {"runner": 10, "scheduler": 20}


def test_missing_jsonl_gives_empty_table_with_schema(tmp_path):
    assert to_parquet(tmp_path / "brak.jsonl", tmp_path / "runs.parquet") == 0
    df = pl.read_parquet(tmp_path / "runs.parquet")
    assert df.height == 0 and df.schema == pl.Schema(SCHEMA)


@pytest.mark.parametrize("unknown, removed", [("nieznane", None), (None, "valid")])
def test_row_with_unknown_or_missing_field_is_rejected(tmp_path, unknown, removed):
    row = full_row()
    if unknown:
        row[unknown] = 1
    if removed:
        row.pop(removed)
    with pytest.raises(ValueError, match="wiersz wyniku"):
        ResultsWriter(tmp_path / "runs.jsonl").write(row)
    assert not (tmp_path / "runs.jsonl").exists()

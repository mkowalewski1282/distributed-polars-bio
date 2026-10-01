"""Rejestr zbiorów databio-8p i rozwiązywanie identyfikatorów (plan 2, Zadanie 1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.data import datasets as ds


def test_dataset_ids_match_polars_bio_bench_directories():
    # Nazwy katalogów z archiwum databio-8p.zip; identyfikatory jak w
    # conf/common.yaml polars-bio-bench (specyfikacja, sekcja 4).
    assert ds.DATASETS == {
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
    assert set(ds.EXPECTED_ROWS_K) == set(ds.DATASETS)
    assert ds.COLUMNS == ("contig", "pos_start", "pos_end")


def test_data_dir_uses_env_root(monkeypatch, tmp_path):
    monkeypatch.setenv("BENCH_DATA_ROOT", str(tmp_path))
    assert ds.data_dir() == tmp_path / "databio-8p"


def test_data_dir_empty_env_means_default(monkeypatch):
    monkeypatch.setenv("BENCH_DATA_ROOT", "")
    assert ds.data_dir() == ds.DEFAULT_DATA_ROOT / "databio-8p"


def test_explicit_root_wins_over_env(monkeypatch, tmp_path):
    monkeypatch.setenv("BENCH_DATA_ROOT", "/nie/tutaj")
    assert ds.data_dir(tmp_path) == tmp_path / "databio-8p"


def test_resolve_pair(tmp_path):
    base = tmp_path / "databio-8p"
    assert ds.resolve("1-2", tmp_path) == (base / "fBrain-DS14718", base / "exons")


def test_resolve_single_dataset(tmp_path):
    assert ds.resolve("0", tmp_path) == (tmp_path / "databio-8p" / "chainRn4", None)


@pytest.mark.parametrize(
    "bad", ["", "9", "1-9", "1-2-3", "a-b", "-1", "1-", " 1-2", "1-2\n", "01-2"]
)
def test_resolve_rejects_malformed_ids(bad, tmp_path):
    with pytest.raises(ValueError, match="niepoprawny identyfikator"):
        ds.resolve(bad, tmp_path)


def test_data_dir_expands_home(monkeypatch):
    monkeypatch.setenv("BENCH_DATA_ROOT", "~/dane_testowe")
    assert ds.data_dir() == Path.home() / "dane_testowe" / "databio-8p"

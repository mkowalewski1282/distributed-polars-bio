"""Plan 3a, Zadanie 9: orkiestrator na prawdziwych silnikach i zbiorze testowym w układzie
databio-8p (para 1-2, zbiór 1) — wszystkie warianty, N = 1 i 2, po jednym przebiegu.

Wynik wzorcowy: polars-bio na 1 partycji. polars-bio A i B (2 i 4 partycje) liczą merge
i subtract osobno w każdej partycji (błąd polars-bio 0.28) — te przebiegi MUSZĄ wyjść
nieważne przez sumę kontrolną; wszystkie pozostałe — ważne.

Wymaga binarek ballista_node i bench_client (debug). Swap jest wyłączony z kryteriów
(pswpout zastąpiony stałą): jego wykrywanie sprawdzają testy jednostkowe, a tu przypadkowy
swap innej aplikacji nie może dawać fałszywych porażek."""

from __future__ import annotations

import functools
import json
import os

import polars as pl
import pytest
import yaml

from bench import orchestrator as orch
from bench.config import VARIANTS, parse
from bench.data.datasets import DATASETS
from bench.ops import OPS, UNARY_OPS
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts

SCENARIOS = [{"op": op, "dataset": 1} if op in UNARY_OPS else {"op": op, "pair": "1-2"} for op in OPS]
KNOWN_POLARS_BIO_BUG = {
    ("polars_bio_a", 1, "merge"), ("polars_bio_a", 1, "subtract"),
    ("polars_bio_b", 2, "merge"), ("polars_bio_b", 2, "subtract"),
}


@pytest.fixture(scope="module")
def data_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("dane")
    write_parts(FIXTURE_A, root / "databio-8p" / DATASETS[1])
    write_parts(FIXTURE_B, root / "databio-8p" / DATASETS[2])
    return root


@pytest.fixture(scope="module")
def series(data_root, tmp_path_factory):
    cfg = parse({
        "series": "integracja", "scenarios": SCENARIOS, "variants": list(VARIANTS),
        "nodes": [1, 2], "repeats": 1, "warmup": 0, "seed": 1, "control_tolerance": 100.0,
    })
    summary = orch.run_series(
        cfg, data_root=data_root, results_dir=tmp_path_factory.mktemp("wyniki"),
        profile="debug", probe=orch.Probe(pswpout=lambda: 0),
    )
    return summary, pl.read_parquet(summary.parquet)


def test_every_planned_run_is_recorded(series):
    summary, df = series
    # wzorzec: 5 scenariuszy (kontrola overlap/1-2 jest wśród nich); bloki: A1, B2,
    # Ballista 1 i 2, Sail 1 i 2 — każdy 2 przebiegi kontrolne + 5 scenariuszy
    assert df.height == 5 + 6 * (2 + 5)
    assert summary.rows == df.height and not summary.failures


def test_only_known_polars_bio_bug_is_invalid(series):
    _, df = series
    bad = df.filter(~pl.col("valid"))
    assert set(zip(bad["variant"], bad["n_nodes"], bad["op"])) == KNOWN_POLARS_BIO_BUG
    assert all("suma kontrolna" in r for r in bad["invalid_reason"]), bad["invalid_reason"].to_list()


def test_threads_follow_variant_and_n(series):
    _, df = series
    partitions = {
        key: {json.loads(e)["target_partitions"] for e in group["extra"]}
        for key, group in df.filter(pl.col("variant") != "sail").group_by(["variant", "n_nodes"])
    }
    assert partitions == {
        ("polars_bio_ref", 1): {1}, ("polars_bio_a", 1): {2}, ("polars_bio_b", 2): {4},
        ("ballista", 1): {2}, ("ballista", 2): {4},
    }


def test_memory_is_measured_per_process(series):
    _, df = series

    def processes(variant: str, n: int) -> set[frozenset]:
        rows = df.filter((pl.col("variant") == variant) & (pl.col("n_nodes") == n) & ~pl.col("is_control"))
        return {frozenset(json.loads(p)) for p in rows["peak_rss"]}

    assert processes("ballista", 2) == {frozenset({"runner", "scheduler", "executor_1", "executor_2"})}
    assert processes("sail", 1) == {frozenset({"runner", "sail_server"})}
    assert processes("polars_bio_b", 2) == {frozenset({"runner"})}
    assert (df["peak_rss_sum"] > 10 * 2**20).all()


def test_ballista_shuffle_volume_is_measured(series):
    _, df = series
    ballista = df.filter(pl.col("variant") == "ballista")
    assert (ballista.filter(pl.col("op").is_in(["merge", "subtract"]))["shuffle_bytes"] > 0).all()
    assert df.filter(pl.col("variant") != "ballista")["shuffle_bytes"].is_null().all()


def test_main_resolves_relative_data_root(data_root, tmp_path, monkeypatch):
    """Runnery Pythona działają w katalogu repozytorium, a Ballisty w ballista_genomics/ —
    względna ścieżka danych musi zostać zamieniona na bezwzględną przed startem serii."""
    monkeypatch.chdir(data_root.parent)
    monkeypatch.setattr(orch, "Probe", functools.partial(orch.Probe, pswpout=lambda: 0))
    cfg = tmp_path / "seria.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "sciezki", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["polars_bio_a"], "nodes": [1], "repeats": 1, "warmup": 0, "seed": 1,
        "control_tolerance": 100.0,
    }))
    affinity = os.sched_getaffinity(0)
    try:
        code = orch.main([str(cfg), "--data-root", data_root.name, "--results-dir", str(tmp_path / "wyniki")])
    finally:
        os.sched_setaffinity(0, affinity)
    assert code == 0

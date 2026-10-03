"""Konfiguracja serii (plan 3a, Zadanie 6; specyfikacja 8.2): walidacja przed pierwszym
pomiarem, rozwinięcie w bloki (kolumny macierzy 7.1) i odtwarzalna kolejność scenariuszy."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.config import Block, ConfigError, Scenario, blocks, check_data, load, parse, round_order
from bench.data.datasets import DATASETS

REPO = Path(__file__).resolve().parents[2]
_DELETE = object()
FIVE = [
    {"op": "overlap", "pair": "1-2"},
    {"op": "nearest", "pair": "1-2"},
    {"op": "coverage", "pair": "1-2"},
    {"op": "merge", "dataset": 1},
    {"op": "subtract", "pair": "1-2"},
]


def raw(**overrides) -> dict:
    base = {
        "series": "test",
        "scenarios": [{"op": "overlap", "pair": "1-2"}, {"op": "merge", "dataset": 1}],
        "variants": ["polars_bio_a", "ballista"],
        "nodes": [1, 3],
        "seed": 7,
    }
    for key, value in overrides.items():
        if value is _DELETE:
            base.pop(key)
        else:
            base[key] = value
    return base


def test_minimal_config_gets_spec_defaults():
    cfg = parse(raw())
    assert cfg.scenarios == (Scenario("overlap", "1-2"), Scenario("merge", "1"))
    assert [s.id for s in cfg.scenarios] == ["overlap/1-2", "merge/1"]
    assert (cfg.repeats, cfg.warmup, cfg.timeout_s, cfg.control_tolerance) == (5, 1, 1200.0, 0.10)
    assert (cfg.nodes, cfg.seed) == ((1, 3), 7)


def test_smoke_config_is_the_full_matrix_on_pair_1_2():
    cfg = load(REPO / "bench" / "conf" / "smoke.yaml")
    assert cfg.series == "smoke" and (cfg.repeats, cfg.warmup) == (1, 0)
    assert {s.id for s in cfg.scenarios} == {
        "overlap/1-2", "nearest/1-2", "coverage/1-2", "merge/1", "subtract/1-2",
    }
    assert len(blocks(cfg)) == 9


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"extra_key": 1}, "unknown keys"),
        ({"seed": _DELETE}, "missing required keys"),
        ({"seed": "x"}, "seed"),
        ({"seed": True}, "seed"),
        ({"series": "Smoke!"}, "series"),
        ({"scenarios": []}, "scenarios: expected a non-empty list"),
        ({"scenarios": ["overlap"]}, "expected a mapping"),
        ({"scenarios": [{"op": "overlap", "pair": "1-2", "algorithm": "Lapper"}]}, "plan 3b"),
        ({"scenarios": [{"op": "overlap", "pair": "1-2", "x": 1}]}, "unknown keys"),
        ({"scenarios": [{"op": "join", "pair": "1-2"}]}, "unknown operation"),
        ({"scenarios": [{"op": "merge", "pair": "1-2"}]}, "single dataset"),
        ({"scenarios": [{"op": "overlap", "dataset": 1}]}, "pair of datasets"),
        ({"scenarios": [{"op": "overlap", "pair": "1-9"}]}, "pair"),
        ({"scenarios": [{"op": "overlap", "pair": 12}]}, "pair"),
        ({"scenarios": [{"op": "merge", "dataset": 9}]}, "dataset"),
        ({"scenarios": [{"op": "merge", "dataset": True}]}, "dataset"),
        (
            {"scenarios": [{"op": "merge", "dataset": 1}, {"dataset": 1, "op": "merge"}]},
            "repeated scenario",
        ),
        ({"variants": ["spark"]}, "variants: unknown"),
        ({"variants": "ballista"}, "variants: expected a non-empty list"),
        ({"nodes": [4]}, "nodes"),
        ({"nodes": [True]}, "nodes"),
        ({"nodes": [1.0]}, "nodes"),
        ({"nodes": [1, 1]}, "repeated items"),
        ({"repeats": 0}, "repeats"),
        ({"warmup": -1}, "warmup"),
        ({"timeout_s": 0}, "timeout_s"),
        ({"control_tolerance": "10%"}, "control_tolerance"),
        ({"variants": ["polars_bio_b"], "nodes": [1]}, "no blocks"),
    ],
)
def test_invalid_config_is_rejected_before_any_run(overrides, message):
    with pytest.raises(ConfigError, match=message):
        parse(raw(**overrides))


def test_blocks_follow_matrix_columns():
    """polars-bio A raz (N = 1, niezależny od N), B tylko dla N ≥ 2 (dla N = 1 tożsamy z A),
    Ballista i Sail dla każdego N; kolejność wariantów stała, N rosnąco."""
    cfg = parse(raw(variants=["sail", "polars_bio_b", "ballista", "polars_bio_a"], nodes=[3, 1]))
    assert blocks(cfg) == [
        Block("polars_bio_a", 1), Block("polars_bio_b", 3), Block("ballista", 1),
        Block("ballista", 3), Block("sail", 1), Block("sail", 3),
    ]
    assert Block("ballista", 3).label == "ballista N=3"


def test_round_order_is_a_reproducible_permutation():
    cfg = parse(raw(scenarios=FIVE))
    block = Block("ballista", 3)
    orders = [round_order(cfg, block, str(r)) for r in range(1, 6)]
    for order in orders:
        assert sorted(order, key=lambda s: s.id) == sorted(cfg.scenarios, key=lambda s: s.id)
    assert orders == [round_order(cfg, block, str(r)) for r in range(1, 6)]
    assert len({tuple(o) for o in orders}) > 1


def test_round_order_depends_on_seed_and_block():
    a, b = parse(raw(scenarios=FIVE, seed=7)), parse(raw(scenarios=FIVE, seed=8))
    rounds = [str(r) for r in range(1, 6)]
    block = Block("ballista", 3)
    assert [round_order(a, block, r) for r in rounds] != [round_order(b, block, r) for r in rounds]
    assert [round_order(a, block, r) for r in rounds] != [
        round_order(a, Block("sail", 3), r) for r in rounds
    ]


def test_check_data_requires_parts_for_scenarios_and_control_pair(tmp_path):
    cfg = parse(raw(scenarios=[{"op": "merge", "dataset": 1}]))
    with pytest.raises(ConfigError, match="exons"):  # zbiór 2 — z pary kontrolnej 1-2
        check_data(cfg, tmp_path)
    for idx in (1, 2):
        d = tmp_path / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "part-00000.parquet").write_bytes(b"")
    check_data(cfg, tmp_path)


def test_check_data_ignores_files_that_are_not_parts(tmp_path):
    for idx in (1, 2):
        d = tmp_path / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "_SUCCESS").write_bytes(b"")
    with pytest.raises(ConfigError, match="python -m bench.data.download"):
        check_data(parse(raw()), tmp_path)


def test_load_reports_invalid_yaml(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text("series: [unclosed\n")
    with pytest.raises(ConfigError, match="invalid YAML"):
        load(path)


def test_load_rejects_non_mapping(tmp_path):
    path = tmp_path / "list.yaml"
    path.write_text("- 1\n- 2\n")
    with pytest.raises(ConfigError, match="expected a mapping"):
        load(path)

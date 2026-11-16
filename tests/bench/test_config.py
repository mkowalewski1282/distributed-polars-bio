"""Series configuration (P semantics): validation before the first measurement, expansion
into blocks (variant × P) and a reproducible scenario order."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.config import (
    DEFAULT_PARALLELISM, VARIANTS, Block, ConfigError, Scenario, blocks, check_data, load, parse, round_order,
)
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
        "variants": ["polars_bio", "ballista"],
        "parallelism": [4, 1],
        "seed": 7,
    }
    for key, value in overrides.items():
        if value is _DELETE:
            base.pop(key)
        else:
            base[key] = value
    return base


def test_minimal_config_gets_default_values():
    cfg = parse(raw())
    assert cfg.scenarios == (Scenario("overlap", "1-2"), Scenario("merge", "1"))
    assert [s.id for s in cfg.scenarios] == ["overlap/1-2", "merge/1"]
    assert (cfg.repeats, cfg.warmup, cfg.timeout_s, cfg.control_tolerance) == (5, 1, 1200.0, 0.10)
    assert (cfg.parallelism, cfg.seed) == ((1, 4), 7)
    # Control run: overlap 7-0, 10 runs at each end of the block.
    assert (cfg.control, cfg.control_repeats) == (Scenario("overlap", "7-0"), 10)
    assert cfg.explain is False
    assert cfg.settle_s == 0.0
    assert cfg.expected_invalid == ()


def test_smoke_config_is_the_full_matrix_on_pair_1_2():
    cfg = load(REPO / "bench" / "conf" / "smoke.yaml")
    assert cfg.series == "smoke" and (cfg.repeats, cfg.warmup) == (1, 0)
    assert {s.id for s in cfg.scenarios} == {
        "overlap/1-2", "nearest/1-2", "coverage/1-2", "merge/1", "subtract/1-2",
    }
    assert cfg.variants == ("polars_bio", "ballista", "sail") and cfg.parallelism == (1, 2)
    assert len(blocks(cfg)) == 6
    assert cfg.explain is True
    assert cfg.settle_s == 3.0


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
        ({"scenarios": [{"op": "overlap", "pair": "1-2", "algorithm": "Lapper"}]}, "algorithm parameter is not supported"),
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
        ({"parallelism": [0]}, "parallelism"),
        ({"parallelism": [True]}, "parallelism"),
        ({"parallelism": [1.0]}, "parallelism"),
        ({"parallelism": ["2"]}, "parallelism"),
        ({"parallelism": []}, "parallelism: expected a non-empty list"),
        ({"parallelism": 2}, "parallelism: expected a non-empty list"),
        ({"parallelism": [1, 1]}, "repeated items"),
        ({"repeats": 0}, "repeats"),
        ({"warmup": -1}, "warmup"),
        ({"timeout_s": 0}, "timeout_s"),
        ({"control_tolerance": "10%"}, "control_tolerance"),
        ({"control": "overlap"}, "control: expected a mapping"),
        ({"control": {"op": "merge", "pair": "1-2"}}, "control: merge works on a single dataset"),
        ({"control": {"op": "overlap", "pair": "1-9"}}, "control: pair"),
        ({"control_repeats": 0}, "control_repeats"),
        ({"control_repeats": True}, "control_repeats"),
        ({"explain": "yes"}, "explain: expected true or false"),
        ({"expected_invalid": "all"}, "expected_invalid: expected a list"),
        ({"expected_invalid": [{"scenario": "overlap/1-2"}]}, r"expected_invalid\[0\]: missing keys"),
        ({"expected_invalid": [{"scenario": "nearest/1-2", "variant": "sail"}]}, "not a scenario of this series"),
        ({"expected_invalid": [{"scenario": "merge/1", "variant": "spark"}]}, "unknown variant"),
        ({"expected_invalid": [{"scenario": "merge/1", "variant": "sail", "x": 1}]}, "unknown keys"),
        ({"expected_invalid": [{"scenario": "merge/1", "variant": "sail", "parallelism": 0}]}, "parallelism"),
        ({"expected_invalid": [{"scenario": "merge/1", "variant": "sail", "parallelism": True}]}, "parallelism"),
        (
            {"expected_invalid": [{"scenario": "merge/1", "variant": "sail", "nodes": 2}]},
            r"expected_invalid\[0\]: nodes: renamed to parallelism",
        ),
    ],
)
def test_invalid_config_is_rejected_before_any_run(overrides, message):
    with pytest.raises(ConfigError, match=message):
        parse(raw(**overrides))


def test_parallelism_defaults_to_1_2_4_8():
    cfg = parse(raw(parallelism=_DELETE))
    assert cfg.parallelism == (1, 2, 4, 8) == DEFAULT_PARALLELISM


def test_nodes_key_reports_rename():
    with pytest.raises(ConfigError, match="nodes: renamed to parallelism"):
        parse(raw(nodes=[1, 2]))


def test_blocks_are_variants_times_parallelism():
    """Every variant for every P; variant order fixed (VARIANTS), P ascending."""
    cfg = parse(raw(variants=["sail", "polars_bio", "ballista"], parallelism=_DELETE))
    assert VARIANTS == ("polars_bio", "ballista", "sail")
    assert blocks(cfg) == [Block(v, p) for v in VARIANTS for p in (1, 2, 4, 8)]
    assert len(blocks(cfg)) == 12
    assert Block("ballista", 4).label == "ballista P=4"


@pytest.mark.parametrize("variant", ["polars_bio_a", "polars_bio_b", "sail_lock", "sail_udtf"])
def test_removed_variants_are_rejected(variant):
    with pytest.raises(ConfigError, match="variants: unknown"):
        parse(raw(variants=[variant]))


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


def test_control_can_be_overridden():
    cfg = parse(raw(control={"op": "merge", "dataset": 1}, control_repeats=3))
    assert (cfg.control, cfg.control_repeats) == (Scenario("merge", "1"), 3)


def test_check_data_requires_parts_for_scenarios_and_control_pair(tmp_path):
    cfg = parse(raw(scenarios=[{"op": "merge", "dataset": 1}]))
    with pytest.raises(ConfigError, match="chainRn4"):  # dataset 0, from the 7-0 control pair
        check_data(cfg, tmp_path)
    for idx in (1, 7, 0):
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


def test_expected_invalid_entries_match_rows():
    cfg = parse(raw(expected_invalid=[
        {"scenario": "merge/1", "variant": "ballista", "parallelism": 4, "reason": "timeout"},
    ]))
    (entry,) = cfg.expected_invalid
    row = {"scenario_id": "merge/1", "variant": "ballista", "parallelism": 4, "invalid_reason": "timeout"}
    assert entry.matches(row)
    assert not entry.matches({**row, "parallelism": 1})
    assert not entry.matches({**row, "invalid_reason": "skipped: timeout"})
    assert not entry.matches({**row, "variant": "sail"})


def test_broadcast_check_config_covers_the_broadcast_cells():
    cfg = load(REPO / "bench" / "conf" / "broadcast_check.yaml")
    assert cfg.variants == ("ballista",) and cfg.parallelism == (1, 2, 4, 8)
    assert {s.id for s in cfg.scenarios} == {"nearest/2-7", "coverage/2-7", "nearest/7-0", "coverage/7-0"}
    assert cfg.explain is True
    assert cfg.control_tolerance == 100.0 and cfg.timeout_s == 600
    assert cfg.repeats == 1 and cfg.warmup == 0
    assert cfg.settle_s == 3.0


@pytest.mark.parametrize("value", [3, 3.0, 0, 0.5])
def test_settle_s_accepts_non_negative_numbers(value):
    assert parse(raw(settle_s=value)).settle_s == float(value)


@pytest.mark.parametrize("value", [-1, True, "3"])
def test_settle_s_rejects_negative_boolean_and_text(value):
    with pytest.raises(ConfigError, match="settle_s"):
        parse(raw(settle_s=value))

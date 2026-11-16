"""Configuration of a measurement series and its expansion into blocks (variant × P).

A YAML file describes the series: scenarios (operation and data), variants, values of P
(parallelism — the number of CPUs of a block), number of rounds, warm-up, time limit, drift
tolerance of the control run and the ordering seed.
The configuration is validated in full before the first measurement — an unknown key or missing
data is an error, not a silent default."""

from __future__ import annotations

import random
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from bench.data.datasets import resolve
from bench.ops import OPS, UNARY_OPS

#: Series variants — compared at equal resources P: polars_bio — one process with P threads on
#: P CPUs; ballista — P executors with 1 slot each, every one on a single CPU; sail — one server
#: process with P workers of 1 slot each on P CPUs (applyInArrow with a pool of polars-bio
#: contexts).
VARIANTS = ("polars_bio", "ballista", "sail")
#: Values of P of the default series; a P larger than the number of CPUs outside the system core
#: is skipped by the orchestrator with a stated reason (bench/cpus.py).
DEFAULT_PARALLELISM = (1, 2, 4, 8)
#: Defaults: control-run drift tolerance of 10%; the control task is overlap 7-0 (~1–2 s as a
#: polars-bio control run with 2 threads; the pair 1-2 took ~0.13 s and its noise exceeded the
#: threshold), executed 10 times at the start and at the end of a block.
#: explain: an extra EXPLAIN ANALYZE run of every scenario in Ballista blocks.
#: settle_s: pause before every run, in seconds (0 = none; the reason is in orchestrator.measure).
DEFAULTS = {
    "repeats": 5, "warmup": 1, "timeout_s": 1200, "control_tolerance": 0.10,
    "control": {"op": "overlap", "pair": "7-0"}, "control_repeats": 10, "explain": False,
    "expected_invalid": [], "settle_s": 0.0, "parallelism": list(DEFAULT_PARALLELISM),
}
_REQUIRED = ("series", "scenarios", "variants", "seed")
#: Keys that were renamed — an error naming the new key instead of a generic "unknown keys".
_RENAMED = {"nodes": "parallelism"}
_KEYS = set(_REQUIRED) | set(DEFAULTS)
_SCENARIO_KEYS = {"op", "pair", "dataset"}
_SERIES_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*")
_PAIR = re.compile(r"[0-8]-[0-8]")


class ConfigError(ValueError):
    """Series configuration error — raised before the first measurement."""


@dataclass(frozen=True)
class Scenario:
    op: str
    #: A pair "a-b" or a dataset "a" (identifiers as in polars-bio-bench).
    data: str

    @property
    def id(self) -> str:
        return f"{self.op}/{self.data}"


@dataclass(frozen=True)
class Block:
    variant: str
    parallelism: int

    @property
    def label(self) -> str:
        return f"{self.variant} P={self.parallelism}"


@dataclass(frozen=True)
class ExpectedInvalid:
    """An expected invalid run: a series containing such a run still counts as passed (e.g. a
    smoke series with a documented engine limitation)."""

    scenario: str
    variant: str
    #: P of the block; None — any.
    parallelism: int | None = None
    #: Beginning of the invalidity reason; None — any.
    reason: str | None = None

    def matches(self, row: dict) -> bool:
        return (
            row["scenario_id"] == self.scenario
            and row["variant"] == self.variant
            and (self.parallelism is None or row["parallelism"] == self.parallelism)
            and (self.reason is None or (row["invalid_reason"] or "").startswith(self.reason))
        )


@dataclass(frozen=True)
class SeriesConfig:
    series: str
    scenarios: tuple[Scenario, ...]
    variants: tuple[str, ...]
    parallelism: tuple[int, ...]
    repeats: int
    warmup: int
    timeout_s: float
    settle_s: float
    seed: int
    control_tolerance: float
    control: Scenario
    control_repeats: int
    explain: bool
    expected_invalid: tuple[ExpectedInvalid, ...]


def _int(raw: dict, key: str, minimum: int) -> int:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{key}: expected an integer ≥ {minimum}, got {value!r}")
    return value


def _positive(raw: dict, key: str) -> float:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ConfigError(f"{key}: expected a number > 0, got {value!r}")
    return float(value)


def _non_negative(raw: dict, key: str) -> float:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ConfigError(f"{key}: expected a number ≥ 0, got {value!r}")
    return float(value)


def _renamed(raw: dict, where: str) -> None:
    for old, new in _RENAMED.items():
        if old in raw:
            raise ConfigError(f"{where}{old}: renamed to {new}")


def _is_parallelism(value) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 1


def _list(raw: dict, key: str) -> list:
    value = raw[key]
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{key}: expected a non-empty list, got {value!r}")
    if len({repr(v) for v in value}) != len(value):
        raise ConfigError(f"{key}: repeated items in {value!r}")
    return value


def _scenario(item, where: str) -> Scenario:
    if not isinstance(item, dict):
        raise ConfigError(f"{where}: expected a mapping, got {item!r}")
    if "algorithm" in item:
        raise ConfigError(f"{where}: the algorithm parameter is not supported yet")
    unknown = set(item) - _SCENARIO_KEYS
    if unknown:
        raise ConfigError(f"{where}: unknown keys {sorted(unknown)}")
    op = item.get("op")
    if op not in OPS:
        raise ConfigError(f"{where}: unknown operation {op!r}; allowed: {', '.join(OPS)}")
    if op in UNARY_OPS:
        if "pair" in item or "dataset" not in item:
            raise ConfigError(f"{where}: {op} works on a single dataset - give dataset, not pair")
        dataset = item["dataset"]
        if isinstance(dataset, bool) or not isinstance(dataset, int) or not 0 <= dataset <= 8:
            raise ConfigError(f"{where}: dataset must be a number 0–8, got {dataset!r}")
        return Scenario(op, str(dataset))
    if "dataset" in item or "pair" not in item:
        raise ConfigError(
            f'{where}: {op} works on a pair of datasets - give pair (e.g. "1-2"), not dataset'
        )
    pair = item["pair"]
    if not isinstance(pair, str) or not _PAIR.fullmatch(pair):
        raise ConfigError(f'{where}: pair must look like "a-b", a, b ∈ 0–8, got {pair!r}')
    return Scenario(op, pair)


_EXPECTED_KEYS = {"scenario", "variant", "parallelism", "reason"}


def _expected_invalid(raw_list, scenario_ids: set[str]) -> tuple[ExpectedInvalid, ...]:
    if not isinstance(raw_list, list):
        raise ConfigError(f"expected_invalid: expected a list, got {raw_list!r}")
    out = []
    for i, item in enumerate(raw_list):
        where = f"expected_invalid[{i}]"
        if not isinstance(item, dict):
            raise ConfigError(f"{where}: expected a mapping, got {item!r}")
        _renamed(item, f"{where}: ")
        unknown = set(item) - _EXPECTED_KEYS
        if unknown:
            raise ConfigError(f"{where}: unknown keys {sorted(unknown)}")
        missing = [k for k in ("scenario", "variant") if k not in item]
        if missing:
            raise ConfigError(f"{where}: missing keys {missing}")
        if item["scenario"] not in scenario_ids:
            raise ConfigError(f"{where}: {item['scenario']!r} is not a scenario of this series")
        if item["variant"] not in VARIANTS:
            raise ConfigError(f"{where}: unknown variant {item['variant']!r}")
        parallelism = item.get("parallelism")
        if parallelism is not None and not _is_parallelism(parallelism):
            raise ConfigError(f"{where}: parallelism must be an integer ≥ 1, got {parallelism!r}")
        reason = item.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise ConfigError(f"{where}: reason must be text, got {reason!r}")
        out.append(ExpectedInvalid(item["scenario"], item["variant"], parallelism, reason))
    return tuple(out)


def parse(raw) -> SeriesConfig:
    """YAML mapping → SeriesConfig; every error → ConfigError with the location and the reason."""
    if not isinstance(raw, dict):
        raise ConfigError("configuration: expected a mapping at the top level")
    _renamed(raw, "")
    unknown = set(raw) - _KEYS
    if unknown:
        raise ConfigError(f"unknown keys: {sorted(unknown)}")
    missing = [k for k in _REQUIRED if k not in raw]
    if missing:
        raise ConfigError(f"missing required keys: {missing}")
    raw = {**DEFAULTS, **raw}
    series = raw["series"]
    if not isinstance(series, str) or not _SERIES_NAME.fullmatch(series):
        raise ConfigError(f"series: a name of lowercase letters, digits, - and _, got {series!r}")
    scenarios = tuple(_scenario(item, f"scenarios[{i}]") for i, item in enumerate(_list(raw, "scenarios")))
    ids = [s.id for s in scenarios]
    duplicated = sorted({i for i in ids if ids.count(i) > 1})
    if duplicated:
        raise ConfigError(f"scenarios: repeated scenario {', '.join(duplicated)}")
    variants = tuple(_list(raw, "variants"))
    unknown_variants = [v for v in variants if v not in VARIANTS]
    if unknown_variants:
        raise ConfigError(f"variants: unknown {unknown_variants}; allowed: {', '.join(VARIANTS)}")
    parallelism = _list(raw, "parallelism")
    if not all(_is_parallelism(p) for p in parallelism):
        raise ConfigError(f"parallelism: expected integers ≥ 1, got {parallelism!r}")
    seed = raw["seed"]
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ConfigError(f"seed: expected an integer, got {seed!r}")
    explain = raw["explain"]
    if not isinstance(explain, bool):
        raise ConfigError(f"explain: expected true or false, got {explain!r}")
    control = _scenario(raw["control"], "control")
    expected_invalid = _expected_invalid(raw["expected_invalid"], {*ids, control.id})
    return SeriesConfig(
        series=series,
        scenarios=scenarios,
        variants=variants,
        parallelism=tuple(sorted(parallelism)),
        repeats=_int(raw, "repeats", 1),
        warmup=_int(raw, "warmup", 0),
        timeout_s=_positive(raw, "timeout_s"),
        settle_s=_non_negative(raw, "settle_s"),
        seed=seed,
        control_tolerance=_positive(raw, "control_tolerance"),
        control=control,
        control_repeats=_int(raw, "control_repeats", 1),
        explain=explain,
        expected_invalid=expected_invalid,
    )


def load(path: Path) -> SeriesConfig:
    try:
        raw = yaml.safe_load(Path(path).read_text())
    except yaml.YAMLError as e:
        raise ConfigError(f"{path}: invalid YAML: {e}") from e
    return parse(raw)


def blocks(cfg: SeriesConfig) -> list[Block]:
    """Blocks: every variant of the series for every P — variants in the fixed VARIANTS order, P
    ascending. Blocks with a P larger than the machine's CPU layout stay in the list; the
    orchestrator skips them."""
    return [Block(v, p) for v in VARIANTS if v in cfg.variants for p in cfg.parallelism]


def round_order(cfg: SeriesConfig, block: Block, label: str) -> list[Scenario]:
    """Order of scenarios in a round: random but reproducible from the series seed — the same for
    the same block and round, independent of the rest of the series (a repeated block gets the
    same order)."""
    rng = random.Random(f"{cfg.seed}/{block.variant}/{block.parallelism}/{label}")
    return rng.sample(list(cfg.scenarios), len(cfg.scenarios))


def check_data(cfg: SeriesConfig, root: Path) -> None:
    """Every dataset of the series — and of the control task — must have part-*.parquet files."""
    missing: set[str] = set()
    for scenario in (*cfg.scenarios, cfg.control):
        for path in resolve(scenario.data, root):
            if path is not None and not any(path.glob("part-*.parquet")):
                missing.add(str(path))
    if missing:
        raise ConfigError(
            "missing data (uv run python -m bench.data.download): " + ", ".join(sorted(missing))
        )

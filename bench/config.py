"""Konfiguracja serii pomiarowej (specyfikacja, sekcja 8.2) i jej rozwinięcie w bloki.

Plik YAML opisuje serię: scenariusze (operacja i dane), warianty, wartości N, liczbę rund,
rozgrzewkę, limit czasu, tolerancję dryfu przebiegu kontrolnego i ziarno kolejności.
Konfiguracja jest sprawdzana w całości przed pierwszym pomiarem — nieznany klucz albo brak
danych to błąd, nie cicha wartość domyślna."""

from __future__ import annotations

import random
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from bench.data.datasets import resolve
from bench.ops import OPS, UNARY_OPS

VARIANTS = ("polars_bio_a", "polars_bio_b", "ballista", "sail")
NODES = (1, 2, 3)
#: Wartości domyślne ze specyfikacji 8.2 i 6.3 (tolerancja dryfu przebiegu kontrolnego: 10%).
DEFAULTS = {"repeats": 5, "warmup": 1, "timeout_s": 1200, "control_tolerance": 0.10}
_REQUIRED = ("series", "scenarios", "variants", "nodes", "seed")
_KEYS = set(_REQUIRED) | set(DEFAULTS)
_SCENARIO_KEYS = {"op", "pair", "dataset"}
_SERIES_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*")
_PAIR = re.compile(r"[0-8]-[0-8]")


class ConfigError(ValueError):
    """Błąd konfiguracji serii — zgłaszany przed pierwszym pomiarem."""


@dataclass(frozen=True)
class Scenario:
    op: str
    #: Para "a-b" albo zbiór "a" (identyfikatory jak w polars-bio-bench).
    data: str

    @property
    def id(self) -> str:
        return f"{self.op}/{self.data}"


#: Przebieg kontrolny (specyfikacja 6.3): overlap na parze 1-2, polars-bio A.
CONTROL = Scenario("overlap", "1-2")


@dataclass(frozen=True)
class Block:
    variant: str
    n_nodes: int

    @property
    def label(self) -> str:
        return f"{self.variant} N={self.n_nodes}"


@dataclass(frozen=True)
class SeriesConfig:
    series: str
    scenarios: tuple[Scenario, ...]
    variants: tuple[str, ...]
    nodes: tuple[int, ...]
    repeats: int
    warmup: int
    timeout_s: float
    seed: int
    control_tolerance: float


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


def _list(raw: dict, key: str) -> list:
    value = raw[key]
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{key}: expected a non-empty list, got {value!r}")
    if len({repr(v) for v in value}) != len(value):
        raise ConfigError(f"{key}: repeated items in {value!r}")
    return value


def _scenario(item, i: int) -> Scenario:
    where = f"scenarios[{i}]"
    if not isinstance(item, dict):
        raise ConfigError(f"{where}: expected a mapping, got {item!r}")
    if "algorithm" in item:
        raise ConfigError(f"{where}: the algorithm parameter is not supported yet (plan 3b-3)")
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


def parse(raw) -> SeriesConfig:
    """Słownik z YAML → SeriesConfig; każdy błąd → ConfigError z miejscem i przyczyną."""
    if not isinstance(raw, dict):
        raise ConfigError("configuration: expected a mapping at the top level")
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
    scenarios = tuple(_scenario(item, i) for i, item in enumerate(_list(raw, "scenarios")))
    ids = [s.id for s in scenarios]
    duplicated = sorted({i for i in ids if ids.count(i) > 1})
    if duplicated:
        raise ConfigError(f"scenarios: repeated scenario {', '.join(duplicated)}")
    variants = tuple(_list(raw, "variants"))
    unknown_variants = [v for v in variants if v not in VARIANTS]
    if unknown_variants:
        raise ConfigError(f"variants: unknown {unknown_variants}; allowed: {', '.join(VARIANTS)}")
    nodes = _list(raw, "nodes")
    if any(isinstance(n, bool) or not isinstance(n, int) or n not in NODES for n in nodes):
        raise ConfigError(f"nodes: values from {NODES}, got {nodes!r}")
    seed = raw["seed"]
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ConfigError(f"seed: expected an integer, got {seed!r}")
    cfg = SeriesConfig(
        series=series,
        scenarios=scenarios,
        variants=variants,
        nodes=tuple(sorted(nodes)),
        repeats=_int(raw, "repeats", 1),
        warmup=_int(raw, "warmup", 0),
        timeout_s=_positive(raw, "timeout_s"),
        seed=seed,
        control_tolerance=_positive(raw, "control_tolerance"),
    )
    if not blocks(cfg):
        raise ConfigError("the configuration gives no blocks (polars_bio_b needs N ≥ 2)")
    return cfg


def load(path: Path) -> SeriesConfig:
    try:
        raw = yaml.safe_load(Path(path).read_text())
    except yaml.YAMLError as e:
        raise ConfigError(f"{path}: invalid YAML: {e}") from e
    return parse(raw)


def blocks(cfg: SeriesConfig) -> list[Block]:
    """Bloki (wariant × N) jak kolumny macierzy 7.1: polars-bio A raz (N = 1, niezależny
    od N), polars-bio B dla N ≥ 2 (dla N = 1 tożsamy z A), Ballista i Sail dla każdego N."""
    out: list[Block] = []
    for variant in VARIANTS:
        if variant not in cfg.variants:
            continue
        if variant == "polars_bio_a":
            out.append(Block(variant, 1))
        elif variant == "polars_bio_b":
            out.extend(Block(variant, n) for n in cfg.nodes if n >= 2)
        else:
            out.extend(Block(variant, n) for n in cfg.nodes)
    return out


def round_order(cfg: SeriesConfig, block: Block, label: str) -> list[Scenario]:
    """Kolejność scenariuszy w rundzie (specyfikacja 6.2): losowa, ale odtwarzalna z ziarna
    serii — ta sama dla tego samego bloku i rundy, niezależnie od reszty serii (powtórzony
    blok dostaje tę samą kolejność)."""
    rng = random.Random(f"{cfg.seed}/{block.variant}/{block.n_nodes}/{label}")
    return rng.sample(list(cfg.scenarios), len(cfg.scenarios))


def check_data(cfg: SeriesConfig, root: Path) -> None:
    """Każdy zbiór serii — i pary kontrolnej — ma mieć pliki part-*.parquet."""
    missing: set[str] = set()
    for scenario in (*cfg.scenarios, CONTROL):
        for path in resolve(scenario.data, root):
            if path is not None and not any(path.glob("part-*.parquet")):
                missing.add(str(path))
    if missing:
        raise ConfigError(
            "missing data (python -m bench.data.download): " + ", ".join(sorted(missing))
        )

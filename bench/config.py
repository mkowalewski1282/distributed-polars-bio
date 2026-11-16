"""Konfiguracja serii pomiarowej (specyfikacja, sekcja 8.2; etap 4: semantyka P) i jej rozwinięcie
w bloki (wariant × P).

Plik YAML opisuje serię: scenariusze (operacja i dane), warianty, wartości P (równoległość —
liczba CPU bloku), liczbę rund, rozgrzewkę, limit czasu, tolerancję dryfu przebiegu kontrolnego
i ziarno kolejności.
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

#: Warianty serii (etap 4, specyfikacja 5.1) — przy równych zasobach P: polars_bio — jeden proces
#: z P wątkami na P CPU; ballista — P executorów po 1 slocie, każdy na jednym CPU; sail — jeden
#: proces serwera z P workerami po 1 slocie na P CPU (applyInArrow z pulą kontekstów polars-bio).
VARIANTS = ("polars_bio", "ballista", "sail")
#: Wartości P domyślnej serii; P większe niż liczba CPU poza rdzeniem systemowym orkiestrator
#: pomija z powodem (bench/cpus.py).
DEFAULT_PARALLELISM = (1, 2, 4, 8)
#: Wartości domyślne ze specyfikacji 8.2 i 6.3 (tolerancja dryfu przebiegu kontrolnego: 10%)
#: oraz 3b-2, 4.2: zadanie kontrolne overlap 7-0 (~1–2 s w przebiegu kontrolnym polars-bio
#: z 2 wątkami; para 1-2 trwała ~0,13 s i szum przekraczał próg) wykonywane po 10 razy na początku
#: i na końcu bloku.
#: explain: dodatkowy przebieg EXPLAIN ANALYZE każdego scenariusza w blokach Ballisty (3b-2, 6.4).
#: settle_s: przerwa przed każdym przebiegiem w sekundach (0 = brak; powód w orchestrator.measure).
DEFAULTS = {
    "repeats": 5, "warmup": 1, "timeout_s": 1200, "control_tolerance": 0.10,
    "control": {"op": "overlap", "pair": "7-0"}, "control_repeats": 10, "explain": False,
    "expected_invalid": [], "settle_s": 0.0, "parallelism": list(DEFAULT_PARALLELISM),
}
_REQUIRED = ("series", "scenarios", "variants", "seed")
#: Klucze zastąpione w etapie 4 — błąd ze wskazaniem nowej nazwy zamiast „unknown keys”.
_RENAMED = {"nodes": "parallelism"}
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


@dataclass(frozen=True)
class Block:
    variant: str
    parallelism: int

    @property
    def label(self) -> str:
        return f"{self.variant} P={self.parallelism}"


@dataclass(frozen=True)
class ExpectedInvalid:
    """Oczekiwany nieważny przebieg (specyfikacja 3b-2, pozycja 17): seria z takim przebiegiem
    nadal jest zaliczona (np. smoke z udokumentowanym ograniczeniem silnika)."""

    scenario: str
    variant: str
    #: P bloku; None — dowolne.
    parallelism: int | None = None
    #: Początek przyczyny nieważności; None — dowolna.
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
    """Słownik z YAML → SeriesConfig; każdy błąd → ConfigError z miejscem i przyczyną."""
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
    """Bloki: każdy wariant serii dla każdego P — warianty w stałej kolejności VARIANTS, P rosnąco.
    Bloki z P większym niż układ CPU maszyny zostają na liście; pomija je orkiestrator."""
    return [Block(v, p) for v in VARIANTS if v in cfg.variants for p in cfg.parallelism]


def round_order(cfg: SeriesConfig, block: Block, label: str) -> list[Scenario]:
    """Kolejność scenariuszy w rundzie (specyfikacja 6.2): losowa, ale odtwarzalna z ziarna
    serii — ta sama dla tego samego bloku i rundy, niezależnie od reszty serii (powtórzony
    blok dostaje tę samą kolejność)."""
    rng = random.Random(f"{cfg.seed}/{block.variant}/{block.parallelism}/{label}")
    return rng.sample(list(cfg.scenarios), len(cfg.scenarios))


def check_data(cfg: SeriesConfig, root: Path) -> None:
    """Każdy zbiór serii — i zadania kontrolnego — ma mieć pliki part-*.parquet."""
    missing: set[str] = set()
    for scenario in (*cfg.scenarios, cfg.control):
        for path in resolve(scenario.data, root):
            if path is not None and not any(path.glob("part-*.parquet")):
                missing.add(str(path))
    if missing:
        raise ConfigError(
            "missing data (uv run python -m bench.data.download): " + ", ".join(sorted(missing))
        )

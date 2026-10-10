"""Sprawdzenia przed pierwszym pomiarem serii (specyfikacja 3; 3b-2, pozycja 4 z przeglądu 3a):
procesowi dostępne są CPU rdzenia systemowego i węzłów, te CPU tworzą pary SMT zgodne z tabelą
węzłów (rdzeń k = wątki {2k, 2k+1}), są taskset i pysail, swap jest pusty (3b-2, 4.3), a binarki
Ballisty istnieją i są świeże. Każdy błąd przerywa serię przed pierwszym pomiarem (kod 2)."""

from __future__ import annotations

import importlib.util
import os
import shutil
from pathlib import Path

from bench import metrics
from bench.config import ConfigError, SeriesConfig
from bench.engines import SYSTEM_CPUS, cluster_cpus, require_ballista_binaries

SYSFS_CPU = Path("/sys/devices/system/cpu")


def _cpu_list(text: str) -> set[int]:
    """Lista CPU w zapisie jądra, np. '0-1' albo '2,8' → zbiór numerów."""
    cpus: set[int] = set()
    for part in text.strip().split(","):
        first, _, last = part.partition("-")
        cpus.update(range(int(first), int(last or first) + 1))
    return cpus


def check_cpus(n_nodes: int, *, affinity: set[int] | None = None, sysfs: Path | None = None) -> None:
    needed = {*SYSTEM_CPUS, *cluster_cpus(n_nodes)}
    affinity = os.sched_getaffinity(0) if affinity is None else affinity
    missing = sorted(needed - set(affinity))
    if missing:
        raise ConfigError(
            f"CPUs {missing} are not available to this process (N = {n_nodes} needs CPUs "
            f"0-{max(needed)}); check the CPU affinity of the shell"
        )
    sysfs = SYSFS_CPU if sysfs is None else sysfs
    for core in range(n_nodes + 1):  # rdzeń 0 — system, 1..N — węzły
        first = 2 * core
        path = sysfs / f"cpu{first}" / "topology" / "thread_siblings_list"
        try:
            siblings = _cpu_list(path.read_text())
        except (OSError, ValueError) as e:
            raise ConfigError(f"cannot read SMT topology of CPU {first}: {e}") from e
        if siblings != {first, first + 1}:
            raise ConfigError(
                f"CPU {first} and {first + 1} are not SMT siblings (siblings of CPU {first}: "
                f"{sorted(siblings)}) - the node layout of the methodology spec (section 3) does "
                "not hold on this machine"
            )


def check_tools(variants) -> None:
    if shutil.which("taskset") is None:
        raise ConfigError("taskset not found (package util-linux) - it pins engines to emulated nodes")
    if any(v.startswith("sail") for v in variants) and importlib.util.find_spec("pysail") is None:
        raise ConfigError("pysail is not importable - run 'uv sync'")


def check_swap(used: int) -> None:
    if used > 0:
        raise ConfigError(
            f"swap is not empty ({used // 2**20} MiB used) - a run touching swap is invalid, so "
            "a series starts from empty swap: run 'wsl --shutdown' in Windows and reopen the terminal"
        )


def run(cfg: SeriesConfig, profile: str) -> None:
    check_cpus(max(cfg.nodes))
    check_swap(metrics.swap_used())
    check_tools(cfg.variants)
    if "ballista" in cfg.variants:
        require_ballista_binaries(profile)

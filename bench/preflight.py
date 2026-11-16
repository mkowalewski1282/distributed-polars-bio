"""Sprawdzenia przed pierwszym pomiarem serii (specyfikacja 3; 3b-2, pozycja 4 z przeglądu 3a;
etap 4, 5.2): topologia CPU daje się odczytać, poza rdzeniem systemowym mieści się co najmniej
P = 1 i przebieg kontrolny (2 CPU), są taskset i pysail, swap jest pusty (3b-2, 4.3), a binarki
Ballisty istnieją i są świeże. Każdy błąd przerywa serię przed pierwszym pomiarem (kod 2).
P większe niż układ CPU nie przerywa serii: preflight ostrzega, a orkiestrator pomija te bloki
z powodem (domyślna seria ma działać na mniejszej maszynie i pokazać luki w raporcie)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from bench import metrics
from bench.config import ConfigError, SeriesConfig
from bench.cpus import SYSFS_CPU, CpuLayout, TopologyError, control_cpus, max_parallelism, read_layout
from bench.engines import CONTROL_THREADS, require_ballista_binaries


def cpu_layout(sysfs: Path = SYSFS_CPU) -> CpuLayout:
    """Układ CPU tego procesu (maska affinity); nieczytelna topologia → ConfigError z przyczyną."""
    try:
        return read_layout(sysfs)
    except TopologyError as e:
        raise ConfigError(f"cannot read the CPU topology: {e}") from e


def check_cpus(cfg: SeriesConfig, layout: CpuLayout) -> list[int]:
    """P serii, które nie mieszczą się w układzie (zostaną pominięte). Błąd, gdy poza rdzeniem
    systemowym nie ma ani jednego CPU (P = 1) albo dwóch CPU dla przebiegu kontrolnego."""
    available = max_parallelism(layout)
    if available < 1:
        raise ConfigError(
            f"no CPU outside the system core {list(layout.system)} - a series needs at least P = 1"
        )
    control = control_cpus(layout)
    if len(control) < CONTROL_THREADS:
        raise ConfigError(
            f"the control run needs {CONTROL_THREADS} CPUs outside the system core "
            f"{list(layout.system)}, available: {list(layout.order)}"
        )
    return [p for p in cfg.parallelism if p > available]


def _importable(module: str) -> bool:
    """Czy `import module` się udaje. Import idzie w osobnym procesie, żeby orkiestrator nie
    ładował natywnej biblioteki pysail (pamięć na maszynie 5 GB), a uszkodzone rozszerzenie
    i tak zostało wykryte (specyfikacja 3b-2, 7.4)."""
    try:
        done = subprocess.run(
            [sys.executable, "-c", f"import {module}"], capture_output=True, timeout=120, env=os.environ.copy()
        )
    except subprocess.TimeoutExpired:
        return False
    return done.returncode == 0


def check_tools(variants) -> None:
    if shutil.which("taskset") is None:
        raise ConfigError("taskset not found (package util-linux) - it pins engines to CPUs")
    if "sail" in variants and not _importable("pysail"):
        raise ConfigError("pysail is not importable - run 'uv sync'")


def check_swap(used: int) -> None:
    if used > 0:
        raise ConfigError(
            f"swap is not empty ({used // 2**20} MiB used) - a run touching swap is invalid, so "
            "a series starts from empty swap: run 'wsl --shutdown' in Windows and reopen the terminal"
        )


def run(cfg: SeriesConfig, profile: str, layout: CpuLayout) -> list[int]:
    """Wszystkie sprawdzenia; zwraca P, które orkiestrator pominie (ostrzeżenie na stderr)."""
    skipped = check_cpus(cfg, layout)
    check_swap(metrics.swap_used())
    check_tools(cfg.variants)
    if "ballista" in cfg.variants:
        require_ballista_binaries(profile)
    if skipped:
        print(
            f"warning: P = {', '.join(map(str, skipped))} exceed the {max_parallelism(layout)} CPUs "
            "outside the system core - those blocks will be skipped (reason recorded in the results)",
            file=sys.stderr,
        )
    return skipped

"""Checks before the first measurement of a series: the CPU topology can be read, at least
P = 1 and the control run (2 CPUs) fit outside the system core, taskset and pysail are present,
swap is empty, and the Ballista binaries exist and are fresh. Every error aborts the series
before the first measurement (exit code 2).
A P larger than the CPU layout does not abort the series: preflight warns and the orchestrator
skips those blocks with a stated reason (the default series is meant to run on a smaller
machine and show the gaps in the report)."""

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
    """CPU layout of this process (the affinity mask); an unreadable topology → ConfigError with the reason."""
    try:
        return read_layout(sysfs)
    except TopologyError as e:
        raise ConfigError(f"cannot read the CPU topology: {e}") from e


def check_cpus(cfg: SeriesConfig, layout: CpuLayout) -> list[int]:
    """The P values of the series that do not fit the layout (they will be skipped). An error when
    there is not a single CPU outside the system core (P = 1) or not two CPUs for the control run."""
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
    """Whether `import module` succeeds. The import runs in a separate process so that the
    orchestrator does not load the native pysail library (memory on a 5 GB machine) while a
    broken extension is still detected."""
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
            "a series starts from empty swap: on WSL2 run 'wsl --shutdown' in Windows and reopen "
            "the terminal; on native Linux run 'sudo swapoff -a && sudo swapon -a'"
        )


def run(cfg: SeriesConfig, profile: str, layout: CpuLayout) -> list[int]:
    """All the checks; returns the P values the orchestrator will skip (a warning on stderr)."""
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

"""CPU layout derived from the machine topology: the system core and the CPU allocation order for P."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

SYSFS_CPU = Path("/sys/devices/system/cpu")


class TopologyError(RuntimeError):
    """The CPU topology cannot be read from sysfs."""


class NotEnoughCpus(ValueError):
    """The requested P exceeds the number of CPUs available outside the system core."""


@dataclass(frozen=True)
class CpuLayout:
    system: tuple[int, ...]  # all allowed threads of the first core
    order: tuple[int, ...]  # CPU allocation order for P (without the system core)
    siblings: tuple[frozenset[int], ...]  # cores other than the system core: sets of allowed threads


def _read(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError as exc:
        raise TopologyError(f"cannot read {path}: {exc}") from exc


def _parse_list(text: str) -> set[int]:
    """Kernel list format: '0-1', '0,4', '0-2,8'."""
    out: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return out


def read_layout(sysfs: Path = SYSFS_CPU, affinity: set[int] | None = None) -> CpuLayout:
    if affinity is None:
        affinity = set(os.sched_getaffinity(0))
    sysfs = Path(sysfs)
    if not sysfs.is_dir():
        raise TopologyError(f"sysfs directory not found: {sysfs}")
    cores: dict[tuple[int, int], set[int]] = {}
    for entry in sysfs.iterdir():
        m = re.fullmatch(r"cpu(\d+)", entry.name)
        if not m or int(m.group(1)) not in affinity:
            continue
        cpu = int(m.group(1))
        topo = entry / "topology"
        try:
            key = (int(_read(topo / "physical_package_id")), int(_read(topo / "core_id")))
            # sibling threads are read only to validate the format; grouping is by (package, core)
            _parse_list(_read(topo / "thread_siblings_list"))
        except ValueError as exc:
            raise TopologyError(f"malformed topology of cpu{cpu}: {exc}") from exc
        cores.setdefault(key, set()).add(cpu)
    if not cores:
        raise TopologyError(f"no allowed CPU with topology under {sysfs}")
    groups = sorted((sorted(threads) for threads in cores.values()), key=lambda t: t[0])
    system = tuple(groups[0])
    rest = groups[1:]
    order = tuple(
        threads[rank] for rank in range(max((len(t) for t in rest), default=0)) for threads in rest if rank < len(threads)
    )
    return CpuLayout(system=system, order=order, siblings=tuple(frozenset(t) for t in rest))


def cpus_for(layout: CpuLayout, p: int) -> tuple[int, ...]:
    if p < 1:
        raise ValueError(f"parallelism must be >= 1, got {p}")
    if p > len(layout.order):
        raise NotEnoughCpus(f"parallelism {p} exceeds {len(layout.order)} CPUs available outside the system core")
    return layout.order[:p]


def uses_smt(layout: CpuLayout, cpus) -> bool:
    """Whether the chosen CPUs include two threads of the same core."""
    chosen = set(cpus)
    return any(len(core & chosen) > 1 for core in layout.siblings)


def control_cpus(layout: CpuLayout) -> tuple[int, ...]:
    """Threads of the first core in `order`; without SMT, the first two CPUs of `order`."""
    if not layout.order:
        return ()
    first = layout.order[0]
    core = next(c for c in layout.siblings if first in c)
    if len(core) > 1:
        return tuple(sorted(core))
    return layout.order[:2]


def max_parallelism(layout: CpuLayout) -> int:
    return len(layout.order)

"""System metrics from /proc: a process's peak memory (VmHWM) and resetting it (clear_refs),
CPU pinning, available memory (MemAvailable), used swap, swap-outs and swap-ins (pswpout,
pswpin), and a directory's size (Ballista's shuffle volume).

Every function takes the `proc` directory — tests pass fixture files."""

from __future__ import annotations

import os
import stat
from pathlib import Path

PROC = Path("/proc")


def _field(path: Path, key: str) -> str:
    for line in path.read_text().splitlines():
        name, sep, value = line.partition(":")
        if sep and name == key:
            return value.strip()
    raise KeyError(f"no field {key} in {path}")


def _kib_field(path: Path, key: str) -> int:
    value = _field(path, key)
    number, _, unit = value.partition(" ")
    if unit.strip() != "kB":
        raise ValueError(f"{path}: field {key} in an unexpected unit: {value!r}")
    return int(number) * 1024


def peak_rss(pid: int | str = "self", proc: Path = PROC) -> int:
    """Peak resident memory of a process (VmHWM) in bytes."""
    return _kib_field(proc / str(pid) / "status", "VmHWM")


def rss(pid: int | str = "self", proc: Path = PROC) -> int:
    """Current resident memory of a process (VmRSS) in bytes."""
    return _kib_field(proc / str(pid) / "status", "VmRSS")


def reset_peak_rss(pid: int | str = "self", proc: Path = PROC) -> None:
    """Reset the peak counter: VmHWM := current RSS (`echo 5 > clear_refs`)."""
    (proc / str(pid) / "clear_refs").write_text("5")


def cpus_allowed(pid: int | str = "self", proc: Path = PROC) -> str:
    """CPUs the process is allowed to run on (e.g. '2-3')."""
    return _field(proc / str(pid) / "status", "Cpus_allowed_list")


def mem_available(proc: Path = PROC) -> int:
    return _kib_field(proc / "meminfo", "MemAvailable")


def swap_used(proc: Path = PROC) -> int:
    """Used swap in bytes: SwapTotal − SwapFree from /proc/meminfo."""
    meminfo = proc / "meminfo"
    return _kib_field(meminfo, "SwapTotal") - _kib_field(meminfo, "SwapFree")


def _vmstat(key: str, proc: Path) -> int:
    for line in (proc / "vmstat").read_text().splitlines():
        name, _, value = line.partition(" ")
        if name == key:
            return int(value)
    raise KeyError(f"no {key} in {proc / 'vmstat'}")


def pswpout(proc: Path = PROC) -> int:
    """Counter of pages swapped out since system boot."""
    return _vmstat("pswpout", proc)


def pswpin(proc: Path = PROC) -> int:
    """Counter of pages swapped in since system boot."""
    return _vmstat("pswpin", proc)


def dir_size(path: Path) -> int:
    """Sum of the sizes of regular files in the tree (symlinks excluded); a missing directory → 0."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                st = os.lstat(os.path.join(root, name))
            except FileNotFoundError:
                continue  # file removed during the walk
            if stat.S_ISREG(st.st_mode):
                total += st.st_size
    return total

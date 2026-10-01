"""Metryki systemowe z /proc (specyfikacja, sekcje 5 i 6): szczyt pamięci procesu (VmHWM)
i jego zerowanie (clear_refs), przypięcie do rdzeni, dostępna pamięć (MemAvailable),
wypchnięcia do swapu (pswpout) oraz rozmiar katalogu (wolumen shuffle Ballisty).

Każda funkcja przyjmuje katalog `proc` — testy podają pliki wzorcowe."""

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
    raise KeyError(f"brak pola {key} w {path}")


def _kib_field(path: Path, key: str) -> int:
    value = _field(path, key)
    number, _, unit = value.partition(" ")
    if unit.strip() != "kB":
        raise ValueError(f"{path}: pole {key} w nieoczekiwanej jednostce: {value!r}")
    return int(number) * 1024


def peak_rss(pid: int | str = "self", proc: Path = PROC) -> int:
    """Szczyt pamięci rezydentnej procesu (VmHWM) w bajtach."""
    return _kib_field(proc / str(pid) / "status", "VmHWM")


def reset_peak_rss(pid: int | str = "self", proc: Path = PROC) -> None:
    """Zeruje licznik szczytu: VmHWM := bieżący RSS (`echo 5 > clear_refs`)."""
    (proc / str(pid) / "clear_refs").write_text("5")


def cpus_allowed(pid: int | str = "self", proc: Path = PROC) -> str:
    """Rdzenie, na których proces może działać (np. '2-3')."""
    return _field(proc / str(pid) / "status", "Cpus_allowed_list")


def mem_available(proc: Path = PROC) -> int:
    return _kib_field(proc / "meminfo", "MemAvailable")


def pswpout(proc: Path = PROC) -> int:
    """Licznik stron wypchniętych do swapu od startu systemu."""
    for line in (proc / "vmstat").read_text().splitlines():
        name, _, value = line.partition(" ")
        if name == "pswpout":
            return int(value)
    raise KeyError(f"brak pswpout w {proc / 'vmstat'}")


def dir_size(path: Path) -> int:
    """Suma rozmiarów plików regularnych w drzewie (bez dowiązań); brak katalogu → 0."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                st = os.lstat(os.path.join(root, name))
            except FileNotFoundError:
                continue  # plik usunięty w trakcie przeglądania
            if stat.S_ISREG(st.st_mode):
                total += st.st_size
    return total

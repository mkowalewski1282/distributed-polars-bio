"""Liczniki z logów procesów silników z czasu jednego przebiegu (specyfikacja 3b-2, 5.6 i 6.3).

Orkiestrator zapamiętuje rozmiary logów przed przebiegiem (`mark`) i po przebiegu czyta tylko
dopisany tekst (`text_since`): liczbę workerów sesji Saila i rozmiar ładunków broadcastu
odebranych przez executory Ballisty."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

#: Start workera sesji w logu serwera Saila 0.7.2 (tests/test_sail_runner.py).
WORKER_READY = re.compile(r"worker (\d+) server is ready")
#: Linia executora Ballisty przy dekodowaniu węzła z broadcastem (bio_phys_codec.rs, plan 3b-2).
BROADCAST_DECODED = re.compile(r"broadcast payload decoded: (\d+) bytes")


def mark(paths: Iterable[Path]) -> dict[Path, int]:
    """Bieżące rozmiary plików (brak pliku → 0)."""
    return {Path(p): (Path(p).stat().st_size if Path(p).exists() else 0) for p in paths}


def text_since(mark: dict[Path, int]) -> str:
    """Tekst dopisany do plików od `mark`; plik krótszy niż zapamiętany offset (zastąpiony albo
    skrócony) jest czytany od początku."""
    parts = []
    for path, offset in mark.items():
        if not path.exists():
            continue
        if path.stat().st_size < offset:
            offset = 0
        with path.open("rb") as f:
            f.seek(offset)
            parts.append(f.read().decode("utf-8", errors="replace"))
    return "".join(parts)


def sail_workers(text: str) -> int:
    return len(set(WORKER_READY.findall(text)))


def broadcast_bytes(text: str) -> int:
    return sum(int(n) for n in BROADCAST_DECODED.findall(text))

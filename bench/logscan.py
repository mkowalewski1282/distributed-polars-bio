"""Counters taken from the engine process logs for the duration of one run.

The orchestrator records the log sizes before a run (`mark`) and after the run reads only the
appended text (`text_since`): the number of Sail session workers and the size of the broadcast
payloads received by Ballista executors."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

#: Session worker start in the Sail 0.7.2 server log (tests/test_sail_runner.py).
WORKER_READY = re.compile(r"worker (\d+) server is ready")
#: Ballista executor line logged when decoding a node with a broadcast (bio_phys_codec.rs).
BROADCAST_DECODED = re.compile(r"broadcast payload decoded: (\d+) bytes")


def mark(paths: Iterable[Path]) -> dict[Path, int]:
    """Current file sizes (a missing file → 0)."""
    return {Path(p): (Path(p).stat().st_size if Path(p).exists() else 0) for p in paths}


def text_since(mark: dict[Path, int]) -> str:
    """Text appended to the files since `mark`; a file shorter than the recorded offset (replaced
    or truncated) is read from the start."""
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

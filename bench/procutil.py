"""Procesy silników: wolne porty, sprawdzanie, czy proces żyje, i oczekiwanie na port."""

from __future__ import annotations

import socket
import subprocess
import time
from pathlib import Path


class EngineError(RuntimeError):
    """Silnik nie wystartował albo padł — komunikat wskazuje proces i jego log."""


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def check_alive(name: str, proc: subprocess.Popen, log: Path) -> None:
    if proc.poll() is not None:
        raise EngineError(f"process {name} exited (code {proc.returncode}); log: {log}")


def wait_for_port(
    port: int, proc: subprocess.Popen, *, name: str, log: Path, timeout: float = 60.0
) -> None:
    """Czeka, aż proces zacznie przyjmować połączenia TCP na porcie (albo padnie)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        check_alive(name, proc, log)
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise EngineError(f"{name} not listening on port {port} after {timeout:.0f} s; log: {log}")

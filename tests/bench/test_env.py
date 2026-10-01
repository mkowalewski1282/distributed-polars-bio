"""Środowisko narzędzia (plan 3a, Zadanie 1): import polars-bio nie może czekać na
serwer X ze zmiennej DISPLAY.

Matplotlib (importowany przez polars-bio) przy domyślnym backendzie sprawdza ekran
z DISPLAY. Gdy adres jest nieosiągalny, połączenie TCP czeka na timeout — na tej
maszynie import polars-bio trwał ~270 s zamiast ~1 s."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _python(code: str, env: dict[str, str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, env=env, capture_output=True, text=True,
        timeout=timeout,
    )


def _env_without_backend(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k != "MPLBACKEND"}
    env.update(extra)
    return env


def test_bench_import_selects_headless_matplotlib_backend():
    r = _python("import bench, os; print(os.environ['MPLBACKEND'])", _env_without_backend())
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "Agg"


def test_explicit_matplotlib_backend_wins():
    r = _python("import bench, os; print(os.environ['MPLBACKEND'])", {**os.environ, "MPLBACKEND": "pdf"})
    assert r.stdout.strip() == "pdf"


def test_polars_bio_import_does_not_wait_for_x_server():
    """DISPLAY na adresie z TEST-NET-1 (RFC 5737, nieosiągalny): bez backendu bez okien
    import czeka na timeout TCP i przekracza limit procesu."""
    r = _python("import bench, polars_bio", _env_without_backend(DISPLAY="192.0.2.1:0"))
    assert r.returncode == 0, r.stderr[-2000:]

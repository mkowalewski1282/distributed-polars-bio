"""Tool environment: importing polars-bio must not wait for the X server named in DISPLAY.

Matplotlib (imported by polars-bio) checks the display from DISPLAY with the default
backend. When the address is unreachable, the TCP connection waits for a timeout; on this
machine the polars-bio import took ~270 s instead of ~1 s."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import bench

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
    """DISPLAY on a TEST-NET-1 address (RFC 5737, unreachable): without a windowless backend
    the import waits for the TCP timeout and exceeds the process limit."""
    r = _python("import bench, polars_bio", _env_without_backend(DISPLAY="192.0.2.1:0"))
    assert r.returncode == 0, r.stderr[-2000:]


def test_bench_rejects_a_foreign_python():
    """The system python3 (3.10) sees older libraries from ~/.local: polars-bio 0.28 with bug
    #372, Sail 0.5.3. A series run with it would silently mix engine versions in the results."""
    with pytest.raises(ImportError, match=r"Python 3\.12\.15.*found 3\.10\.12.*uv run"):
        bench.require_project_python((3, 10, 12, "final", 0))


def test_bench_rejects_another_patch_release():
    with pytest.raises(ImportError, match=r"found 3\.12\.7"):
        bench.require_project_python((3, 12, 7, "final", 0))


def test_bench_accepts_the_project_python():
    assert bench.PROJECT_PYTHON == (3, 12, 15)
    bench.require_project_python((3, 12, 15, "final", 0))

"""Benchmark harness for genomic interval operations."""

import os
import sys
from pathlib import Path

#: Python of the project environment — the full version from .python-version (uv downloads exactly
#: that one). The system python3 (3.10) sees older libraries from ~/.local (polars-bio 0.28 with
#: bug #372, Sail 0.5.3), so a series run with it would silently mix engine versions in the
#: results; a different 3.12.x is a different environment than the one the other results were
#: measured in.
PROJECT_PYTHON = tuple(
    int(part)
    for part in (Path(__file__).resolve().parent.parent / ".python-version").read_text().split()[0].split(".")
)


def require_project_python(version_info) -> None:
    """Raise ImportError when the interpreter is not the project environment's Python (uv)."""
    if tuple(version_info[: len(PROJECT_PYTHON)]) != PROJECT_PYTHON:
        found = ".".join(str(part) for part in version_info[:3])
        wanted = ".".join(str(part) for part in PROJECT_PYTHON)
        raise ImportError(
            f"bench needs Python {wanted} from the project environment, found {found}; run it "
            f"through uv, e.g. 'uv run python -m bench.orchestrator ...' or 'uv run pytest'"
        )


require_project_python(sys.version_info)

# With the default backend, Matplotlib (imported indirectly by polars-bio) probes the X server
# named in DISPLAY. On the measurement machine DISPLAY points at the Windows host, which has no
# X server (/etc/bash.bashrc), and the TCP connection waits for a timeout: importing polars-bio
# took ~270 s instead of ~1 s. A windowless backend skips the probe; an explicitly set
# MPLBACKEND wins.
os.environ.setdefault("MPLBACKEND", "Agg")

"""Narzędzie pomiarowe (specyfikacja metodyki, sekcja 8)."""

import os
import sys

#: Python środowiska projektu (pyproject.toml, .python-version; plan 3b-1). Systemowy python3
#: (3.10) widzi starsze biblioteki z ~/.local — polars-bio 0.28 z błędem #372, Sail 0.5.3 —
#: więc seria uruchomiona nim po cichu mieszałaby wersje silników w wynikach.
PROJECT_PYTHON = (3, 12)


def require_project_python(version_info) -> None:
    """ImportError, gdy interpreter nie jest Pythonem środowiska projektu (uv)."""
    if tuple(version_info[:2]) != PROJECT_PYTHON:
        found = ".".join(str(part) for part in version_info[:3])
        raise ImportError(
            f"bench needs Python {PROJECT_PYTHON[0]}.{PROJECT_PYTHON[1]} from the project "
            f"environment, found {found}; run it through uv, e.g. "
            f"'uv run python -m bench.orchestrator ...' or 'uv run pytest'"
        )


require_project_python(sys.version_info)

# Matplotlib (importowany pośrednio przez polars-bio) przy domyślnym backendzie sprawdza
# serwer X ze zmiennej DISPLAY. Na tej maszynie DISPLAY wskazuje host Windows bez serwera X
# (/etc/bash.bashrc), a połączenie TCP czeka na timeout: import polars-bio trwał ~270 s
# zamiast ~1 s. Backend bez okien omija to sprawdzenie; jawnie ustawiony MPLBACKEND wygrywa.
os.environ.setdefault("MPLBACKEND", "Agg")

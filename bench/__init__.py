"""Narzędzie pomiarowe (specyfikacja metodyki, sekcja 8)."""

import os
import sys
from pathlib import Path

#: Python środowiska projektu — pełna wersja z .python-version (plan 3b-2; uv pobiera dokładnie
#: tę). Systemowy python3 (3.10) widzi starsze biblioteki z ~/.local — polars-bio 0.28 z błędem
#: #372, Sail 0.5.3 — więc seria uruchomiona nim po cichu mieszałaby wersje silników w wynikach;
#: inna wersja 3.12.x to inne środowisko niż to, w którym zmierzono pozostałe wyniki.
PROJECT_PYTHON = tuple(
    int(part)
    for part in (Path(__file__).resolve().parent.parent / ".python-version").read_text().split()[0].split(".")
)


def require_project_python(version_info) -> None:
    """ImportError, gdy interpreter nie jest Pythonem środowiska projektu (uv)."""
    if tuple(version_info[: len(PROJECT_PYTHON)]) != PROJECT_PYTHON:
        found = ".".join(str(part) for part in version_info[:3])
        wanted = ".".join(str(part) for part in PROJECT_PYTHON)
        raise ImportError(
            f"bench needs Python {wanted} from the project environment, found {found}; run it "
            f"through uv, e.g. 'uv run python -m bench.orchestrator ...' or 'uv run pytest'"
        )


require_project_python(sys.version_info)

# Matplotlib (importowany pośrednio przez polars-bio) przy domyślnym backendzie sprawdza
# serwer X ze zmiennej DISPLAY. Na tej maszynie DISPLAY wskazuje host Windows bez serwera X
# (/etc/bash.bashrc), a połączenie TCP czeka na timeout: import polars-bio trwał ~270 s
# zamiast ~1 s. Backend bez okien omija to sprawdzenie; jawnie ustawiony MPLBACKEND wygrywa.
os.environ.setdefault("MPLBACKEND", "Agg")

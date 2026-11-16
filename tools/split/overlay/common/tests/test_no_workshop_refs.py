"""Content guard: every text file of the repository is in English and free of references to
internal working notes.

Vendored code (vendor/) is scanned too: its local patches and notes must be English as well.
Not scanned: the engine submodules (engines/, each has its own guard), measurement results
(bench/results/), version control, virtual environments, build output, the directories listed in
.gitignore, binary files and this file. The patterns are ASCII only (Polish letters as \\u
escapes)."""

from __future__ import annotations

import os
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
THIS_FILE = Path(__file__).resolve().relative_to(REPO).as_posix()

_POLISH_ESCAPES = r"\u0105\u0107\u0119\u0142\u0144\u00f3\u015b\u017a\u017c\u0104\u0106\u0118\u0141\u0143\u00d3\u015a\u0179\u017b"

#: References to working documents (specifications, plans, tasks, phases, review rounds).
DOC_REFERENCE_PATTERNS: tuple[str, ...] = (
    r"specyfikacj",
    r"\bspec(?:ification)?\b",
    r"\bplan(?:u|ie)? (?:2|3|3a|3b|P0)\b",
    r"zadani[ea] \d",
    r"\btask \d",
    r"\bfaz[aye] [A-H]\b",
    r"\bphase [A-H]\b",
    r"przegl[a\u0105]d(?:u)? (?:3a|3b|ko\u0144cow)",
    r"\breview (?:3a|3b)\b",
)
POLISH_PATTERNS: tuple[str, ...] = (
    "[" + _POLISH_ESCAPES + "]",
    r"\b(?:jest|oraz|przez|ktory|ktora|gdy|tylko|wiec|czyli|albo|zeby|dlatego)\b",
)

_PATTERNS = [re.compile(p, re.IGNORECASE) for p in DOC_REFERENCE_PATTERNS + POLISH_PATTERNS]

#: Directory names never scanned, at any depth.
SKIPPED_DIR_NAMES = frozenset({".git", ".venv", "target", "__pycache__", ".pytest_cache"})
#: Paths relative to the repository root that are never scanned: the engine submodules (each
#: has its own guard) and measurement results.
SKIPPED_PREFIXES = ("engines/", "bench/results/")


def _gitignored_dirs() -> tuple[set[str], tuple[str, ...]]:
    """Directory entries of .gitignore without wildcards: `name/` (any depth) and `a/b/`
    (from the root)."""
    names: set[str] = set()
    prefixes: list[str] = []
    gitignore = REPO / ".gitignore"
    lines = gitignore.read_text(encoding="utf-8").splitlines() if gitignore.is_file() else []
    for line in (raw.strip() for raw in lines):
        if not line.endswith("/") or line.startswith(("#", "!")) or any(c in line for c in "*?["):
            continue
        entry = line.strip("/")
        if "/" in entry:
            prefixes.append(entry + "/")
        else:
            names.add(entry)
    return names, tuple(prefixes)


def scanned_files() -> list[str]:
    """Paths (relative to the repository root) of the files the guard reads."""
    ignored_names, ignored_prefixes = _gitignored_dirs()
    names = SKIPPED_DIR_NAMES | ignored_names
    prefixes = SKIPPED_PREFIXES + ignored_prefixes
    files = []
    for dirpath, dirnames, filenames in os.walk(REPO):
        base = Path(dirpath).relative_to(REPO).as_posix()
        base = "" if base == "." else base + "/"
        dirnames[:] = sorted(
            d for d in dirnames if d not in names and not f"{base}{d}/".startswith(prefixes)
        )
        files += [
            f"{base}{name}"
            for name in sorted(filenames)
            if not f"{base}{name}".startswith(prefixes) and f"{base}{name}" != THIS_FILE
        ]
    return files


def line_hits(text: str) -> list[tuple[int, str]]:
    return [
        (n, line.strip())
        for n, line in enumerate(text.splitlines(), 1)
        if any(p.search(line) for p in _PATTERNS)
    ]


def test_patterns_catch_polish_and_working_note_references():
    assert line_hits("# liczba w\u0105tk\u00f3w")
    assert line_hits("# to jest test")
    assert line_hits("# see the " + "spec" + " for details")
    assert not line_hits("# the number of threads; see TECHNICAL.md")


def test_repository_has_no_polish_or_working_note_references():
    hits = []
    for rel in scanned_files():
        raw = (REPO / rel).read_bytes()
        if b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        hits += [f"{rel}:{n}: {line}" for n, line in line_hits(text)]
    assert not hits, f"{len(hits)} lines with Polish text or working-note references:\n" + "\n".join(
        hits[:50]
    )

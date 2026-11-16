"""Skrypt podzialu monorepo na trzy repozytoria (ballista-genomics, sail-genomics,
genomics-engines-bench). Powtarzalny: `git archive <zrodlo>` -> mapowanie sciezek -> nakladki
(tools/split/overlay/<repo>/, kopiowane bez zmian, na koncu) -> przepisanie importow ->
kontrola braku importow `bench` -> straznik tresci. Zmiany niezatwierdzone nie trafiaja do
eksportu. Warsztat: ten plik i cale tools/ nie sa eksportowane.

Uzycie: uv run python tools/split/split.py --source <commit> --out <katalog>
Kod wyjscia != 0, gdy straznik tresci cos znajdzie."""

from __future__ import annotations

import argparse
import io
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
REPOS = ("ballista-genomics", "sail-genomics", "genomics-engines-bench")

# Wszystkie repozytoria: nigdy nie eksportowane (warsztat, wyniki pomiarow, smieci Pythona).
EXCLUDES_ALL = (
    "tools/**",
    "tests/test_split.py",
    "tests/test_code_language.py",
    "bench/results/**",
    "**/__pycache__/**",
    "**/*.pyc",
)

# Ballista wspolna z repo pomiarowym tylko przez nakladke — testy tylko dla Ballisty nie
# trafiaja do genomics-engines-bench.
_BALLISTA_ONLY_TESTS = (
    "tests/test_ballista_overlap.py",
    "tests/test_ballista_distributed_ops.py",
    "tests/test_*_correctness.py",
    "tests/test_oracle_sanity.py",
)

EXCLUDES: dict[str, tuple[str, ...]] = {
    "ballista-genomics": (),
    "sail-genomics": (),
    "genomics-engines-bench": _BALLISTA_ONLY_TESTS,
}

_BALLISTA_FILES = ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml")
_BALLISTA_TESTS = (
    "__init__.py conftest.py parquet_fixture.py generic_oracle.py overlap_oracle.py "
    "merge_oracle.py nearest_oracle.py coverage_subtract_oracle.py test_ballista_overlap.py "
    "test_ballista_distributed_ops.py test_ballista_distribution_evidence.py "
    "test_ballista_multiprocess.py test_ballista_parquet.py test_merge_correctness.py "
    "test_nearest_correctness.py test_coverage_subtract_correctness.py test_oracle_sanity.py "
    "test_generic_oracle.py"
).split()
_SAIL_TESTS = (
    "__init__.py parquet_fixture.py generic_oracle.py test_sail_pb_context.py"
).split()

#: repo -> [(sciezka w monorepo albo glob, cel)]. Bez globa cel to sciezka pliku; z globem cel to
#: katalog, a podsciezka (wzgledem statycznego prefiksu globa) jest zachowana. Pierwsza pasujaca
#: regula wygrywa.
MAPPING: dict[str, list[tuple[str, str]]] = {
    "ballista-genomics": (
        [(f"ballista_genomics/{name}", name) for name in _BALLISTA_FILES]
        + [
            ("ballista_genomics/src/**", "src"),
            ("ballista_genomics/vendor/**", "vendor"),
            ("ballista_genomics/data/**", "data"),
        ]
        + [(f"tests/{name}", f"tests/{name}") for name in _BALLISTA_TESTS]
        + [
            ("bench/ops.py", "tests/support/ops.py"),
            ("bench/checksum.py", "tests/support/checksum.py"),
            ("bench/cpus.py", "tests/support/cpus.py"),
        ]
    ),
    "sail-genomics": (
        [("sail_genomics/**", "src/sail_genomics")]
        + [(f"tests/{name}", f"tests/{name}") for name in _SAIL_TESTS]
    ),
    "genomics-engines-bench": [("bench/**", "bench"), ("tests/**", "tests")],
}

_POLISH_ESCAPES = r"\u0105\u0107\u0119\u0142\u0144\u00f3\u015b\u017a\u017c\u0104\u0106\u0118\u0141\u0143\u00d3\u015a\u0179\u017b"

# Zrodlo wzorcow jest czystym ASCII (polskie litery jako \u), zeby nakladka Task 12 mogla je
# skopiowac slowo w slowo, a straznik nie trafial sam w siebie.
IMPORT_REWRITES: dict[str, list[tuple[str, str]]] = {
    "ballista-genomics": [
        (r'^from bench\.data\.datasets import COLUMNS$', 'COLUMNS = ("contig", "pos_start", "pos_end")'),
        (r"\bfrom bench\.(ops|checksum|cpus)\b", r"from tests.support.\1"),
        (r"\bfrom bench import (ops|checksum|cpus)\b", r"from tests.support import \1"),
        (
            r'Path\(__file__\)\.resolve\(\)\.parent\.parent / "ballista_genomics"',
            r"Path(__file__).resolve().parent.parent",
        ),
    ],
    "sail-genomics": [
        (
            r"^from bench\.ops import normalize_polars_bio$",
            "from sail_genomics.schema import rename_polars_bio as normalize_polars_bio",
        ),
        (
            r"^from bench\.data\.datasets import COLUMNS$",
            "from sail_genomics.schema import DEFAULT_INPUT_COLUMNS as COLUMNS",
        ),
        (r"^from bench\.ops import ", "from sail_genomics.schema import "),
    ],
    "genomics-engines-bench": [
        (
            r'Path\(__file__\)\.resolve\(\)\.parent\.parent / "ballista_genomics"',
            r'Path(__file__).resolve().parent.parent / "engines" / "ballista-genomics"',
        ),
    ],
}

#: Import modulu `bench` — w ballista-genomics i sail-genomics ma ich nie byc wcale.
BENCH_IMPORT = re.compile(r"^\s*(?:from\s+bench\b|import\s+bench\b)", re.MULTILINE)
REPOS_WITHOUT_BENCH = ("ballista-genomics", "sail-genomics")

WORKSHOP_PATTERNS: tuple[str, ...] = (
    r"specyfikacj",
    r"\bspec(?:ification)?\b",
    r"\bplan(?:u|ie)? (?:2|3|3a|3b|P0)\b",
    r"zadani[ea] \d",
    r"\btask \d",
    r"docs/superpowers",
    r"wnioski_claude",
    r"raporty/",
    r"ustalenia_zalozen",
    r"promotor",
    r"supervisor",
    r"claude",
    r"\bfaz[aye] [A-H]\b",
    r"\bphase [A-H]\b",
    r"przegl[a\u0105]d(?:u)? (?:3a|3b|ko\u0144cow)",
    r"\breview (?:3a|3b)\b",
)
POLISH_PATTERNS: tuple[str, ...] = (
    "[" + _POLISH_ESCAPES + "]",
    r"\b(?:jest|oraz|przez|ktory|ktora|gdy|tylko|wiec|czyli|albo|zeby|dlatego)\b",
)

#: Pliki definiujace sam straznik (nakladka Task 12) — zawieraja wzorce, wiec nie sa skanowane.
GUARD_DEFINITION_FILES = ("tests/test_no_workshop_refs.py",)
#: Kod upstreamu — poza zakresem straznika.
SCAN_EXCLUDED_PREFIXES = ("vendor/", ".git/")

_WORKSHOP_RE = [re.compile(p, re.IGNORECASE) for p in WORKSHOP_PATTERNS]
_POLISH_RE = [re.compile(p, re.IGNORECASE) for p in POLISH_PATTERNS]


def _glob_regex(pattern: str) -> re.Pattern[str]:
    out, i = [], 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


def _matches(path: str, patterns) -> bool:
    return any(_glob_regex(p).match(path) for p in patterns)


def map_path(repo: str, path: str) -> str | None:
    """Sciezka docelowa pliku `path` (wzgledem korzenia monorepo) w repozytorium `repo`
    albo None, gdy plik tam nie trafia."""
    if _matches(path, EXCLUDES_ALL) or _matches(path, EXCLUDES[repo]):
        return None
    for src, dest in MAPPING[repo]:
        if not any(c in src for c in "*?"):
            if path == src:
                return dest
            continue
        if _glob_regex(src).match(path):
            prefix = src[: re.search(r"[*?]", src).start()]
            prefix = prefix[: prefix.rfind("/") + 1]
            return f"{dest}/{path[len(prefix):]}"
    return None


def rewrite_text(repo: str, text: str) -> str:
    """Przepisania importow repozytorium `repo` zastosowane do tresci pliku .py."""
    for pattern, replacement in IMPORT_REWRITES[repo]:
        text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
    return text


def _archive(source: str, dest: Path) -> None:
    data = subprocess.run(
        ["git", "archive", "--format=tar", source], cwd=REPO, capture_output=True, check=True
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        tar.extractall(dest, filter="data")


def _relative_files(root: Path):
    for path in sorted(root.rglob("*")):
        if path.is_file() or path.is_symlink():
            yield path.relative_to(root).as_posix()


def export(source: str, out: Path) -> None:
    """Eksport `source` (commit/drzewo) do `out/<repo>/` (bez .git). Odmawia, gdy `out` istnieje
    i nie jest pusty — dzieki temu ponowne uruchomienie nie nadpisze repozytoriow z historia."""
    out = Path(out)
    if out.exists() and (not out.is_dir() or any(out.iterdir())):
        raise SystemExit(f"refusing to export into non-empty path: {out}")
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        tree = Path(tmp)
        _archive(source, tree)
        files = list(_relative_files(tree))
        for repo in REPOS:
            target = out / repo
            for rel in files:
                if rel.startswith("tools/split/overlay/"):
                    continue
                dest = map_path(repo, rel)
                if dest is not None:
                    (target / dest).parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(tree / rel, target / dest)
            overlay = tree / "tools" / "split" / "overlay" / repo
            if overlay.is_dir():
                shutil.copytree(overlay, target, dirs_exist_ok=True)
            target.mkdir(parents=True, exist_ok=True)
            for path in target.rglob("*.py"):
                text = path.read_text(encoding="utf-8")
                rewritten = rewrite_text(repo, text)
                if rewritten != text:
                    path.write_text(rewritten, encoding="utf-8")
    leftovers = [
        f"{repo}/{path.relative_to(out / repo).as_posix()}:{n}: {line.strip()}"
        for repo in REPOS_WITHOUT_BENCH
        for path in sorted((out / repo).rglob("*.py"))
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if BENCH_IMPORT.match(line)
    ]
    if leftovers:
        raise SystemExit("bench imports remain after rewriting:\n" + "\n".join(leftovers))


def line_families(line: str) -> set[str]:
    """Rodziny wzorcow trafiajace w linie: "workshop" i/lub "language"."""
    found = set()
    if any(r.search(line) for r in _WORKSHOP_RE):
        found.add("workshop")
    if any(r.search(line) for r in _POLISH_RE):
        found.add("language")
    return found


def scan_workshop_refs(root: Path) -> list[str]:
    """Trafienia straznika tresci i jezyka w plikach tekstowych pod `root`: "plik:linia: tekst"
    (jedno trafienie na linie). Pomija vendor/, .git/, pliki binarne i plik definiujacy straznik."""
    root = Path(root)
    hits = []
    for rel in _relative_files(root):
        if rel.startswith(SCAN_EXCLUDED_PREFIXES) or rel in GUARD_DEFINITION_FILES:
            continue
        raw = (root / rel).read_bytes()
        if b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for n, line in enumerate(text.splitlines(), 1):
            if line_families(line):
                hits.append(f"{rel}:{n}: {line.strip()}")
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--source", required=True, help="commit albo drzewo do wyeksportowania")
    parser.add_argument("--out", required=True, type=Path, help="pusty albo nieistniejacy katalog")
    args = parser.parse_args(argv)
    export(args.source, args.out)
    total = 0
    for repo in REPOS:
        hits = scan_workshop_refs(args.out / repo)
        by_family = {"workshop": 0, "language": 0}
        for hit in hits:
            _, _, text = hit.split(":", 2)
            for family in line_families(text):
                by_family[family] += 1
            print(f"{repo}/{hit}")
        print(f"== {repo}: {len(hits)} hits {by_family}", file=sys.stderr)
        total += len(hits)
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())

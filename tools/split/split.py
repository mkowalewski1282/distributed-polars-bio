"""Skrypt podzialu monorepo na trzy repozytoria (ballista-genomics, sail-genomics,
genomics-engines-bench). Powtarzalny: `git archive <zrodlo>` -> mapowanie sciezek z
przepisaniem importow (.py) i sciezek (pliki tekstowe) -> nakladki (tools/split/overlay/common/,
potem tools/split/overlay/<repo>/, kopiowane bez zmian) -> `uv lock` (wymaga sieci) -> kontrola
braku importow `bench` -> straznik tresci. Zmiany niezatwierdzone nie trafiaja do eksportu.
Warsztat: ten plik i cale tools/ nie sa eksportowane.

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
# trafiaja do genomics-engines-bench. Testy dowodow rozproszenia czytaja plany z output/, ktore
# pisza testy dist_ops (tylko w ballista-genomics) — w repo pomiarowym bylyby zawsze pomijane.
_BALLISTA_ONLY_TESTS = (
    "tests/test_ballista_overlap.py",
    "tests/test_ballista_distributed_ops.py",
    "tests/test_ballista_distribution_evidence.py",
    "tests/test_*_correctness.py",
    "tests/test_oracle_sanity.py",
)

EXCLUDES: dict[str, tuple[str, ...]] = {
    "ballista-genomics": (),
    "sail-genomics": (),
    "genomics-engines-bench": _BALLISTA_ONLY_TESTS,
}

_BALLISTA_FILES = ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml")
# conftest.py Ballisty pochodzi z nakladki (bez importow `bench`, znaczniki dla tego repo).
_BALLISTA_TESTS = (
    "__init__.py parquet_fixture.py generic_oracle.py overlap_oracle.py "
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
        # Stala za importami nastepujacymi po zastepowanym (inaczej E402 w tests/generic_oracle.py).
        (
            r"^from bench\.data\.datasets import COLUMNS\n((?:(?:from|import) [^\n(]*\n)*)",
            "\\1\n#: Interval columns of the input files (databio-8p layout).\n"
            'COLUMNS = ("contig", "pos_start", "pos_end")\n',
        ),
        (r"\bfrom bench\.(ops|checksum)\b", r"from tests.support.\1"),
        (r"\bfrom bench import (ops|checksum)\b", r"from tests.support import \1"),
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

#: Przepisania sciezek we wszystkich eksportowanych plikach tekstowych (poza kodem upstreamu
#: w vendor/<crate>/): katalog crate'a `ballista_genomics/` monorepo to korzen ballista-genomics
#: i engines/ballista-genomics w repo pomiarowym; opis techniczny OPIS.md (po polsku, tylko
#: w monorepo) to tam TECHNICAL.md (nakladka ballista-genomics). Kopie modulow `bench` w Ballistcie
#: leza w tests/support/.
PATH_REWRITES: dict[str, list[tuple[str, str]]] = {
    "ballista-genomics": [
        (r"\bcd ballista_genomics && ", ""),
        (r"\bthe `ballista_genomics/` directory", "the repository root"),
        (r"\bin ballista_genomics/(?=\s)", "in the repository root"),
        (r"\bballista_genomics/(?=[\w.])", ""),
        (r"\bbench/(ops|checksum)\.py\b", r"tests/support/\1.py"),
        (r"\bOPIS\.md\b", "TECHNICAL.md"),
        (r"\bOPIS\b", "TECHNICAL_DOC"),
    ],
    "sail-genomics": [(r"\(bench/ops\.py\)", "(src/sail_genomics/schema.py)")],
    "genomics-engines-bench": [
        (r"\bcd ballista_genomics && ", "cd engines/ballista-genomics && "),
        (r'"OPIS\.md"', '"TECHNICAL.md"'),
        (r"\b(?:ballista_genomics/)?OPIS\.md\b", "engines/ballista-genomics/TECHNICAL.md"),
        (r"\bOPIS\b", "TECHNICAL_DOC"),
        # Sciezki plikow crate'a (np. ballista_genomics/src/runner.rs) — nie `/ "ballista_genomics"`
        # ani `ballista_genomics::` (nazwa crate'a w logach i kodzie Rusta).
        (r"\bballista_genomics/(?=[\w.])", "engines/ballista-genomics/"),
    ],
}

#: Nakladka wspolna (LICENSE, straznik tresci) — kopiowana do kazdego repo przed nakladka repo.
COMMON_OVERLAY = "common"

#: Repozytoria, w ktorych export tworzy uv.lock. genomics-engines-bench zalezy od
#: engines/sail-genomics (submodul dodawany po eksporcie), wiec blokuje sie je dopiero z submodulami.
LOCKED_REPOS = ("ballista-genomics", "sail-genomics")

#: Import modulu `bench` — w ballista-genomics i sail-genomics ma ich nie byc wcale.
BENCH_IMPORT = re.compile(r"^\s*(?:from\s+bench\b|import\s+bench\b)", re.MULTILINE)
REPOS_WITHOUT_BENCH = ("ballista-genomics", "sail-genomics")

#: Odwolania do dokumentow roboczych opisane ogolnie (specyfikacja, plan, zadanie, faza, przeglad)
#: — te wzorce kopiuje publiczny straznik nakladki (tests/test_no_workshop_refs.py).
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
#: Nazwy warsztatu — tylko tutaj (tools/ nie jest eksportowane), nigdy w nowych repozytoriach,
#: takze nie w pliku straznika (skan eksportu sprawdza go tymi wzorcami).
SENSITIVE_PATTERNS: tuple[str, ...] = (
    r"docs/superpowers",
    r"wnioski_claude",
    r"raporty/",
    r"ustalenia_zalozen",
    r"promotor",
    r"supervisor",
    r"claude",
)
WORKSHOP_PATTERNS: tuple[str, ...] = DOC_REFERENCE_PATTERNS + SENSITIVE_PATTERNS
POLISH_PATTERNS: tuple[str, ...] = (
    "[" + _POLISH_ESCAPES + "]",
    r"\b(?:jest|oraz|przez|ktory|ktora|gdy|tylko|wiec|czyli|albo|zeby|dlatego)\b",
)

#: Pliki definiujace straznik nakladki — zawieraja wzorce ogolne i jezyka, wiec skan sprawdza je
#: tylko wzorcami SENSITIVE_PATTERNS.
GUARD_DEFINITION_FILES = ("tests/test_no_workshop_refs.py",)
#: Poza zakresem straznika. vendor/ jest skanowany w calosci: skan kopii v0.25.0 nie ma trafien
#: w kodzie upstreamu, a nasze komentarze latek i vendor/PATCH.md musza byc po angielsku.
SCAN_EXCLUDED_PREFIXES = (".git/",)

_WORKSHOP_RE = [re.compile(p, re.IGNORECASE) for p in WORKSHOP_PATTERNS]
_POLISH_RE = [re.compile(p, re.IGNORECASE) for p in POLISH_PATTERNS]
_SENSITIVE_RE = [re.compile(p, re.IGNORECASE) for p in SENSITIVE_PATTERNS]


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


def rewrite_text(repo: str, text: str, python: bool = True) -> str:
    """Przepisania repozytorium `repo` zastosowane do tresci pliku tekstowego: importy (tylko
    pliki .py, `python`) i sciezki."""
    rules = (IMPORT_REWRITES[repo] if python else []) + PATH_REWRITES[repo]
    for pattern, replacement in rules:
        text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
    return text


def _is_upstream(dest: str) -> bool:
    """Kod upstreamu w vendor/<crate>/ — kopiowany bajt w bajt (nasze notatki w vendor/ nie)."""
    return dest.startswith("vendor/") and dest.count("/") > 1


def _copy_rewritten(repo: str, src: Path, dest: Path, rel: str) -> None:
    """Kopia pliku z przepisaniami importow/sciezek (pliki tekstowe UTF-8 poza kodem upstreamu)."""
    shutil.copy2(src, dest)
    if _is_upstream(rel):
        return
    raw = src.read_bytes()
    if b"\0" in raw:
        return
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return
    rewritten = rewrite_text(repo, text, python=rel.endswith(".py"))
    if rewritten != text:
        dest.write_text(rewritten, encoding="utf-8")


def _uv_lock(target: Path) -> None:
    subprocess.run(["uv", "lock"], cwd=target, check=True)


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
            target.mkdir(parents=True, exist_ok=True)
            for rel in files:
                if rel.startswith("tools/split/overlay/"):
                    continue
                dest = map_path(repo, rel)
                if dest is not None:
                    (target / dest).parent.mkdir(parents=True, exist_ok=True)
                    _copy_rewritten(repo, tree / rel, target / dest, dest)
            for name in (COMMON_OVERLAY, repo):
                overlay = tree / "tools" / "split" / "overlay" / name
                if overlay.is_dir():
                    shutil.copytree(overlay, target, dirs_exist_ok=True)
    leftovers = [
        f"{repo}/{path.relative_to(out / repo).as_posix()}:{n}: {line.strip()}"
        for repo in REPOS_WITHOUT_BENCH
        for path in sorted((out / repo).rglob("*.py"))
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if BENCH_IMPORT.match(line)
    ]
    if leftovers:
        raise SystemExit("bench imports remain after rewriting:\n" + "\n".join(leftovers))
    for repo in LOCKED_REPOS:
        _uv_lock(out / repo)
    print(
        "note: genomics-engines-bench is not locked - its sail-genomics dependency lives in the "
        "engines/ submodules; run `uv lock` there after adding them",
        file=sys.stderr,
    )


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
    (jedno trafienie na linie). Pomija .git/ i pliki binarne; plik definiujacy straznik sprawdza
    tylko nazwami warsztatu (SENSITIVE_PATTERNS)."""
    root = Path(root)
    hits = []
    for rel in _relative_files(root):
        if rel.startswith(SCAN_EXCLUDED_PREFIXES):
            continue
        guard = rel in GUARD_DEFINITION_FILES
        raw = (root / rel).read_bytes()
        if b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for n, line in enumerate(text.splitlines(), 1):
            hit = any(r.search(line) for r in _SENSITIVE_RE) if guard else line_families(line)
            if hit:
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

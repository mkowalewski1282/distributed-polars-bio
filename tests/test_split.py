"""Testy skryptu podzialu na trzy repozytoria (tools/split/split.py): odmowa nadpisania,
pokrycie testow mapowaniem, straznik tresci i przepisywanie importow. Plik nalezy tylko do
warsztatu — nie trafia do zadnego z nowych repozytoriow (patrz split.EXCLUDES_ALL)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tools.split import split

REPO = Path(__file__).resolve().parent.parent

#: Testy, ktore zostaja w warsztacie (nie sa eksportowane do zadnego repozytorium).
STAYS_IN_WORKSHOP = {"tests/test_split.py", "tests/test_code_language.py"}

#: Testy trafiajace jawnie do kilku repozytoriow (reszta: dokladnie do jednego).
IN_SEVERAL_REPOS = {
    "tests/test_ballista_distribution_evidence.py",
    "tests/test_ballista_multiprocess.py",
    "tests/test_ballista_parquet.py",
    "tests/test_generic_oracle.py",
    "tests/test_sail_pb_context.py",
}


def test_split_refuses_existing_output(tmp_path):
    (tmp_path / "cos.txt").write_text("x")
    with pytest.raises(SystemExit) as info:
        split.export("HEAD", tmp_path)
    assert str(tmp_path) in str(info.value)
    assert (tmp_path / "cos.txt").read_text() == "x"  # nic nie ruszone


def test_mapping_covers_every_moved_test():
    tracked = subprocess.run(
        ["git", "ls-files", "tests"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.split()
    tests = [p for p in tracked if p.startswith("tests/test_") and p.endswith(".py") and "/" not in p[6:]]
    assert tests
    for path in tests:
        repos = {repo for repo in split.REPOS if split.map_path(repo, path) is not None}
        if path in STAYS_IN_WORKSHOP:
            assert not repos, f"{path} powinien zostac w warsztacie, a trafia do {repos}"
        elif path in IN_SEVERAL_REPOS:
            assert len(repos) > 1, f"{path} ma byc w kilku repozytoriach, jest w {repos}"
        else:
            assert len(repos) == 1, f"{path} -> {repos}"


def test_moved_files_are_english():
    """Kazdy plik eksportowany wg MAPPING (zrodlo w monorepo, bez vendor/ i plikow binarnych) nie
    ma trafien wzorcow jezyka polskiego ani odwolan do dokumentow roboczych."""
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.splitlines()
    hits = []
    for path in tracked:
        dests = [split.map_path(repo, path) for repo in split.REPOS]
        dests = [d for d in dests if d is not None]
        if not dests or all(d.startswith(split.SCAN_EXCLUDED_PREFIXES) for d in dests):
            continue
        raw = (REPO / path).read_bytes()
        if b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        hits += [
            f"{path}:{n}: {line.strip()}"
            for n, line in enumerate(text.splitlines(), 1)
            if split.line_families(line)
        ]
    assert not hits, f"{len(hits)} trafien w przenoszonych plikach:\n" + "\n".join(hits[:50])


def test_workshop_only_files_are_never_mapped():
    for repo in split.REPOS:
        assert split.map_path(repo, "tools/split/split.py") is None
        assert split.map_path(repo, "bench/results/x/runs.parquet") is None
        assert split.map_path(repo, "tests/test_split.py") is None
        assert split.map_path(repo, "tests/test_code_language.py") is None


def test_scan_finds_workshop_refs(tmp_path):
    (tmp_path / "a.py").write_text("# specyfikacja 3b-2\nx = 1  # Zadanie 27\n")
    (tmp_path / "b.py").write_text("# liczba w\u0105tk\u00f3w\n")
    (tmp_path / "vendor").mkdir()
    (tmp_path / "vendor" / "x.rs").write_text("// spec says so\n")
    hits = split.scan_workshop_refs(tmp_path)
    assert len(hits) == 3, hits
    assert [h for h in hits if h.startswith("a.py:")].__len__() == 2
    assert [h for h in hits if h.startswith("b.py:1:")]
    assert not [h for h in hits if h.startswith("vendor/")]


def test_scan_skips_guard_definition_file(tmp_path):
    guard = tmp_path / "tests" / "test_no_workshop_refs.py"
    guard.parent.mkdir()
    guard.write_text("PATTERN = 'promotor'\n")
    assert split.scan_workshop_refs(tmp_path) == []


def test_pattern_sources_are_ascii():
    # Wzorce kopiuje do swojego testu nakladka Task 12 — nie moga zawierac polskich liter.
    for pattern in split.WORKSHOP_PATTERNS + split.POLISH_PATTERNS:
        assert pattern.isascii(), pattern


def test_rewrites_remove_bench_imports():
    sample = (
        "from bench import checksum as cs\n"
        "from bench.ops import OPS, UNARY_OPS\n"
        "from bench.cpus import read_layout\n"
        "from bench.checksum import checksum_rows\n"
        "from bench.data.datasets import COLUMNS\n"
        'BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"\n'
    )
    out = split.rewrite_text("ballista-genomics", sample)
    assert not split.BENCH_IMPORT.search(out)
    assert "from tests.support import checksum as cs" in out
    assert "from tests.support.ops import OPS, UNARY_OPS" in out
    assert "BALLISTA_DIR = Path(__file__).resolve().parent.parent\n" in out
    sail_sample = (
        "from bench.ops import OPS, UNARY_OPS\n"
        "from bench.ops import normalize_polars_bio\n"
        "from bench.data.datasets import COLUMNS\n"
    )
    out = split.rewrite_text("sail-genomics", sail_sample)
    assert not split.BENCH_IMPORT.search(out)
    assert "from sail_genomics.schema import OPS, UNARY_OPS" in out


def test_rewrites_on_real_ballista_test():
    text = (REPO / "tests" / "test_ballista_parquet.py").read_text(encoding="utf-8")
    out = split.rewrite_text("ballista-genomics", text)
    assert not split.BENCH_IMPORT.search(out)
    assert '"ballista_genomics"' not in out.split("BALLISTA_DIR =")[1].split("\n")[0]
    bench_out = split.rewrite_text("genomics-engines-bench", text)
    assert '/ "engines" / "ballista-genomics"' in bench_out

"""Testy skryptu podzialu na trzy repozytoria (tools/split/split.py): odmowa nadpisania,
pokrycie testow mapowaniem, straznik tresci i przepisywanie importow. Plik nalezy tylko do
warsztatu — nie trafia do zadnego z nowych repozytoriow (patrz split.EXCLUDES_ALL)."""

from __future__ import annotations

import re
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
    """Kazdy plik eksportowany wg MAPPING (zrodlo w monorepo, takze vendor/, bez plikow binarnych)
    nie ma trafien wzorcow jezyka polskiego ani odwolan do dokumentow roboczych."""
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
    (tmp_path / "vendor" / "x.rs").write_text("// PATCH (praca magisterska) - to jest latka\n")
    hits = split.scan_workshop_refs(tmp_path)
    assert len(hits) == 4, hits
    assert [h for h in hits if h.startswith("a.py:")].__len__() == 2
    assert [h for h in hits if h.startswith("b.py:1:")]
    assert [h for h in hits if h.startswith("vendor/x.rs:1:")]  # vendor/ tez jest skanowany


def test_scan_checks_guard_definition_file_only_for_workshop_names(tmp_path):
    guard = tmp_path / "tests" / "test_no_workshop_refs.py"
    guard.parent.mkdir()
    guard.write_text("PATTERNS = (r'specyfikacj', r'\\b(?:jest|oraz)\\b')\n")
    assert split.scan_workshop_refs(tmp_path) == []
    guard.write_text("PATTERN = 'promotor'\n")
    assert split.scan_workshop_refs(tmp_path) == ["tests/test_no_workshop_refs.py:1: PATTERN = 'promotor'"]


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


def test_columns_literal_follows_the_imports():
    """Stala COLUMNS w Ballistcie zastepuje import, ale stoi za wszystkimi importami (bez E402)."""
    import ast

    text = (REPO / "tests" / "generic_oracle.py").read_text(encoding="utf-8")
    tree = ast.parse(split.rewrite_text("ballista-genomics", text))
    kinds = [
        "import" if isinstance(node, (ast.Import, ast.ImportFrom)) else "columns"
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        or (isinstance(node, ast.Assign) and [t.id for t in node.targets] == ["COLUMNS"])
    ]
    assert kinds[-1] == "columns" and kinds.count("columns") == 1, kinds


def test_technical_doc_rewrites_resolve_per_repo():
    text = (REPO / "tests" / "test_ballista_distribution_evidence.py").read_text(encoding="utf-8")
    ballista = split.rewrite_text("ballista-genomics", text)
    assert 'BALLISTA_DIR = Path(__file__).resolve().parent.parent\n' in ballista
    assert 'TECHNICAL_DOC = BALLISTA_DIR / "TECHNICAL.md"' in ballista
    assert "cd ballista_genomics" not in ballista
    bench = split.rewrite_text("genomics-engines-bench", text)
    assert '/ "engines" / "ballista-genomics"' in bench
    assert 'TECHNICAL_DOC = BALLISTA_DIR / "TECHNICAL.md"' in bench
    assert "cd engines/ballista-genomics && " in bench
    for out in (ballista, bench):
        assert "OPIS" not in out
    assert (REPO / "tools" / "split" / "overlay" / "ballista-genomics" / "TECHNICAL.md").is_file()


def test_technical_doc_declares_each_checked_setting_once():
    doc = (REPO / "tools" / "split" / "overlay" / "ballista-genomics" / "TECHNICAL.md").read_text()
    for key, value in [
        ("ballista.planner.adaptive.enabled", "true"),
        ("ballista.scheduler.max_partitions_per_task", "1"),
        ("ballista.scheduler.share_file_statistics_cache", "false"),
    ]:
        assert re.findall(rf"`{re.escape(key)}` = `([^`]+)`", doc) == [value], key


def test_ballista_conftest_comes_from_the_overlay():
    assert split.map_path("ballista-genomics", "tests/conftest.py") is None
    conftest = REPO / "tools" / "split" / "overlay" / "ballista-genomics" / "tests" / "conftest.py"
    assert not split.BENCH_IMPORT.search(conftest.read_text(encoding="utf-8"))


def test_overlay_guard_uses_the_split_patterns():
    import importlib.util

    path = REPO / "tools" / "split" / "overlay" / split.COMMON_OVERLAY / split.GUARD_DEFINITION_FILES[0]
    spec = importlib.util.spec_from_file_location("overlay_guard", path)
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    assert guard.DOC_REFERENCE_PATTERNS == split.DOC_REFERENCE_PATTERNS
    assert set(guard.DOC_REFERENCE_PATTERNS) < set(split.WORKSHOP_PATTERNS)
    assert not hasattr(guard, "WORKSHOP_PATTERNS") and not hasattr(guard, "SENSITIVE_PATTERNS")
    assert guard.POLISH_PATTERNS == split.POLISH_PATTERNS
    text = path.read_text(encoding="utf-8")
    assert text.isascii()
    assert not [p for p in split.SENSITIVE_PATTERNS if re.search(p, text, re.IGNORECASE)]


def test_overlays_have_no_workshop_refs():
    overlays = REPO / "tools" / "split" / "overlay"
    assert {p.name for p in overlays.iterdir()} == {split.COMMON_OVERLAY, *split.REPOS}
    hits = [h for name in sorted(p.name for p in overlays.iterdir())
            for h in split.scan_workshop_refs(overlays / name)]
    assert not hits, hits


def test_export_locks_the_integration_repos(tmp_path, monkeypatch):
    locked = []
    monkeypatch.setattr(split, "_uv_lock", lambda target: locked.append(target.name))
    split.export("HEAD", tmp_path / "out")
    assert locked == list(split.LOCKED_REPOS) == ["ballista-genomics", "sail-genomics"]
    # Nazwy warsztatu nigdzie w eksporcie (takze w pliku straznika i w vendor/).
    sensitive = [re.compile(p, re.IGNORECASE) for p in split.SENSITIVE_PATTERNS]
    found = [
        f"{path.relative_to(tmp_path)}:{n}"
        for path in sorted((tmp_path / "out").rglob("*"))
        if path.is_file()
        for n, line in enumerate(path.read_bytes().decode("utf-8", "replace").splitlines(), 1)
        if any(r.search(line) for r in sensitive)
    ]
    assert not found, found

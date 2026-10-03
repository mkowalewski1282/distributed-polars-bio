"""Strażnik konwencji językowej (plan 3b-1, etap 6): w plikach źródłowych po polsku są tylko
komentarze i docstringi. Test szuka polskich znaków diakrytycznych w pozostałym kodzie (nazwy,
napisy, komunikaty, f-stringi). Polskich słów bez znaków diakrytycznych nie wykryje — chroni przed
nawrotem, nie zastępuje przeglądu.

Zakres: pliki .py śledzone przez git i pliki .rs w ballista_genomics/src. Poza zakresem: kod
zwendorowany (należy do upstreamu), źródła raportów w raporty/ (podpisy rysunków do polskich
raportów to treść dokumentu, nie komunikaty programu) i ten plik (dane testowe skanera)."""

from __future__ import annotations

import ast
import io
import re
import subprocess
import tokenize
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
POLISH_LETTERS = re.compile("[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]")
EXCLUDED = ("ballista_genomics/vendor/", "raporty/")
_RAW_STRING = re.compile(r'b?r(#*)"')
_CHAR_LITERAL = re.compile(r"'(?:\\u\{[0-9a-fA-F]+\}|\\.|[^\\'\n])'")


def _docstring_starts(tree: ast.AST) -> set[tuple[int, int]]:
    """Pozycje (linia, kolumna) docstringów modułu, klas i funkcji."""
    starts = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                starts.add((first.lineno, first.col_offset))
    return starts


def python_code(source: str) -> list[tuple[int, str]]:
    """Tokeny kodu Pythona z numerem linii, bez komentarzy i docstringów. W Pythonie 3.12 tekst
    f-stringa to osobne tokeny FSTRING_MIDDLE — też trafiają do wyniku."""
    docstrings = _docstring_starts(ast.parse(source))
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            continue
        if tok.type == tokenize.STRING and tok.start in docstrings:
            continue
        out.append((tok.start[0], tok.string))
    return out


def rust_code(source: str) -> list[tuple[int, str]]:
    """Linie kodu Rusta z komentarzami (//, ///, //!, /* */ także zagnieżdżonymi) zamienionymi na
    spacje. Napisy (także surowe r#"…"#) i literały znakowe są przepisywane w całości, więc `//`
    wewnątrz napisu nie jest komentarzem."""
    out: list[str] = []
    i, n = 0, len(source)
    while i < n:
        if source.startswith("//", i):
            j = source.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif source.startswith("/*", i):
            depth, j = 0, i
            while j < n:
                if source.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif source.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                    if depth == 0:
                        break
                else:
                    j += 1
            out.append(re.sub(r"[^\n]", " ", source[i:j]))
            i = j
        elif (raw := _RAW_STRING.match(source, i)) and not (
            i and (source[i - 1].isalnum() or source[i - 1] == "_")
        ):
            end = '"' + raw.group(1)
            j = source.find(end, raw.end())
            j = n if j < 0 else j + len(end)
            out.append(source[i:j])
            i = j
        elif source[i] == '"':
            j = i + 1
            while j < n and source[j] != '"':
                j += 2 if source[j] == "\\" else 1
            out.append(source[i : j + 1])
            i = j + 1
        elif source[i] == "'" and (char := _CHAR_LITERAL.match(source, i)):
            out.append(char.group(0))
            i = char.end()
        else:
            out.append(source[i])
            i += 1
    return list(enumerate("".join(out).split("\n"), start=1))


def violations(path: Path) -> list[tuple[int, str]]:
    """Fragmenty kodu (bez komentarzy i docstringów) z polskimi znakami diakrytycznymi."""
    source = path.read_text(encoding="utf-8")
    code = python_code(source) if path.suffix == ".py" else rust_code(source)
    return [(line, text.strip()) for line, text in code if POLISH_LETTERS.search(text)]


def source_files() -> list[Path]:
    """Pliki objęte konwencją: .py śledzone przez git i .rs w ballista_genomics/src."""
    listed = subprocess.run(
        ["git", "ls-files", "--", "*.py", "ballista_genomics/src/*.rs"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    # Bez tego pliku: jego przypadki testowe celowo zawierają polskie znaki w napisach.
    own = Path(__file__).resolve()
    return [REPO / name for name in listed if not name.startswith(EXCLUDED) and REPO / name != own]



def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_python_comments_and_docstrings_are_ignored(tmp_path):
    path = _write(
        tmp_path, "ok.py",
        '"""Moduł: żółw."""\n\n\ndef f():\n    """Zwraca ślad."""\n    return 1  # żaden problem\n',
    )
    assert violations(path) == []


def test_python_names_strings_and_fstrings_are_checked(tmp_path):
    path = _write(tmp_path, "bad.py", 'x = "błąd"\nzażółć = 1\ny = f"{x} gęś"\n')
    assert [line for line, _ in violations(path)] == [1, 2, 3]


def test_rust_comments_are_ignored_and_strings_checked(tmp_path):
    path = _write(
        tmp_path, "a.rs",
        '//! moduł ż\n/// funkcja ś\nfn f() {\n    println!("błąd"); // komentarz ą\n}\n'
        "/* blok ę /* zagnieżdżony ź */ ó */\n",
    )
    assert [line for line, _ in violations(path)] == [4]


def test_rust_comment_markers_inside_strings_are_code(tmp_path):
    path = _write(tmp_path, "b.rs", 'let url = "df://localhost"; // adres ł\nlet s = "// ć";\n')
    assert [line for line, _ in violations(path)] == [2]


def test_rust_char_and_raw_string_literals(tmp_path):
    path = _write(
        tmp_path, "c.rs",
        "let q = '\"'; let s = \"ok\";\nlet r = r#\"a \" ń\"#;\nfn g<'a>(x: &'a str) {}\n",
    )
    assert [line for line, _ in violations(path)] == [2]


def test_source_files_cover_python_and_rust_without_excluded_trees():
    names = {str(path.relative_to(REPO)) for path in source_files()}
    assert {"bench/orchestrator.py", "ballista_genomics/src/bin/bench_client.rs"} <= names
    assert not any(name.startswith(EXCLUDED) for name in names)
    assert "tests/test_code_language.py" not in names


def test_source_code_outside_comments_is_english():
    found = [
        f"{path.relative_to(REPO)}:{line}: {text[:100]}"
        for path in source_files()
        for line, text in violations(path)
    ]
    assert not found, f"{len(found)} code fragments with Polish letters:\n" + "\n".join(found[:60])

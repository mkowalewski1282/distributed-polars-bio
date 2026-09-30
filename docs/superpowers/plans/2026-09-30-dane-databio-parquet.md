# Plan 2 — dane databio-8p i ścieżka Parquet: plan implementacji

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pobrać i zweryfikować zbiór `databio-8p` z polars-bio-bench, a następnie uruchomić wszystkie pięć operacji na tych plikach Parquet w Ballistcie (standalone i z osobnych procesów) oraz w Sailu, z wynikami zgodnymi z polars-bio. Bez pomiarów czasu.

**Architecture:** Nowy pakiet `bench/` (zalążek narzędzia pomiarowego ze specyfikacji, sekcja 8.1) zawiera rejestr zbiorów, pobieranie z weryfikacją oraz wspólny, znormalizowany schemat wyników (sekcja 8.4). Po stronie Ballisty:
- `dist_provider.rs` rozpoznaje Parquet po ścieżce;
- nowa binarka `bench_client` (runner z sekcji 8.1, na razie tylko liczba wierszy i zapis wyniku) wykonuje jeden scenariusz na dowolnych plikach.

Po stronie Saila nowy moduł `sail_bio.py` czyta dane przez `spark.read.parquet` i uogólnia sprawdzone UDTF-y na dane bez kolumny z nazwą. Poprawność sprawdzają dwa zestawy testów, w obu względem polars-bio:
- szybkie testy na małym zbiorze o typach i nazwach kolumn prawdziwych danych;
- test akceptacyjny na parze 1-2.

**Tech Stack:**
- Python 3.10: pytest 6.2.5, pyarrow 22, polars 1.39, polars-bio 0.28 (tylko wyrocznie), requests 2.31, pysail 0.5.3, pyspark 4.1.
- Rust 2024: Ballista 53.0.0, DataFusion `=53.0.0`, `futures` 0.3 (wersja z `Cargo.lock`).

**Spec:** `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`. Plan realizuje sekcje 4 (dane) i 9.3 (CSV → Parquet). Opiera się na sekcji 8.1 (struktura `bench/`, runner `bench_client`) i sekcji 8.4 (schemat wyników). Stan wyjściowy opisuje `ballista_genomics/OPIS.md`, sekcja P0.

## Global Constraints

- Budowanie ZAWSZE z `CARGO_BUILD_JOBS=1`. Maszyna ma 3,5 GB RAM, a bez tego linkowanie wywraca WSL.
- Nie uruchamiać Pythona z polars-bio równolegle z budowaniem Rusta.
- Ballista `53.0.0`, DataFusion `=53.0.0`. Bez forkowania i modyfikowania Ballisty i Saila. Zvendorowana `datafusion-bio-function-ranges` pozostaje bez zmian.
- Nowe zależności Rusta tylko w wersjach już obecnych w `Cargo.lock` (`futures` 0.3.32). Nowych zależności Pythona nie dodajemy (w szczególności nie `gdown`).
- Dane leżą **poza repozytorium**, w `$BENCH_DATA_ROOT/databio-8p/<zbiór>/part-*.parquet` (domyślnie `~/bench_data`). Ten układ jest identyczny jak w polars-bio-bench. Archiwum zajmuje ok. 1,2 GB, a rozpakowane dane ok. 1,6 GB. Danych nigdy nie commitujemy.
- Układ współrzędnych jest 0-based i półotwarty. Kolumny danych to `contig`, `pos_start` i `pos_end`.
- Wyniki wszystkich silników porównujemy w schemacie z sekcji 8.4 specyfikacji (`bench/ops.py`, `OUTPUT_COLUMNS`). Porównanie jest niezależne od kolejności wierszy i od szerokości typów całkowitych. Dla `nearest` nie obejmuje tożsamości sąsiada, bo przy remisach silniki mogą wybrać różnych sąsiadów.
- **Bez pomiarów czasu.** Czas, pamięć, suma kontrolna i orkiestrator to plan 3.
- Istniejące testy (57) muszą przechodzić. Zachowanie `dist_ops` i skryptów demonstracyjnych `sail_*_udtf.py` pozostaje bez zmian (specyfikacja 8.1: `dist_ops` bez zmian).
- Brak binarki w testach Ballisty to **błąd**, nie pominięcie (zasada z P0). Brak danych w teście akceptacyjnym to pominięcie z podaniem powodu (marker `dane`).
- **Przed Zadaniem 6** (pobranie i testy na prawdziwych danych) przypomnij użytkownikowi checklistę RAM z pamięci `feedback_ram_przed_eksperymentem` i poczekaj na potwierdzenie.
- Komentarze w kodzie i dokumentacja po polsku. Komunikaty commitów po polsku, bez znaków diakrytycznych, zakończone linią `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- W repozytorium nic o komunikacji z promotorem.

## Review Focus

Wejścia i warunki, których specyfikacja nie wymienia wprost, a które najłatwiej ugryzą osobę używającą tych narzędzi (najbardziej prawdopodobne na początku):

1. **Chromosom obecny tylko po jednej stronie.** Przykłady: kontigi `SIRV*` w `ex-anno`, `chrM` albo `chrY` w części zbiorów. Oczekiwana semantyka polars-bio (sprawdzona sondą na polars-bio 0.28):
   - `nearest` zwraca wiersz z pustym sąsiadem i pustą odległością;
   - `coverage` zwraca pokrycie 0;
   - `subtract` zostawia przedział bez zmian;
   - `overlap` nic nie zwraca.

   Dotychczasowy UDTF coverage w Sailu (`sail_coverage_subtract_udtf.py`) pomija taki chromosom. Pokrycie: chromosomy `chrA`/`chrB` w zbiorze testowym, testy w Zadaniach 3 (wyrocznia), 4 (Ballista) i 5 (Sail).
2. **Śmieci w archiwum.** `__MACOSX/…/._part-*.parquet` ma rozszerzenie `.parquet`, ale nie jest Parquetem. Obok leżą `.part-*.crc` i `_SUCCESS`. Żaden z tych plików nie może trafić do katalogu zbioru. Test: `test_extract_keeps_only_data_parts` (Zadanie 2).
3. **Przerwane pobranie, strona HTML z Google Drive albo uszkodzone archiwum.** Oczekiwany jest błąd z jasnym komunikatem. Nie może powstać katalog, który wygląda na kompletny, ani zginąć poprzednia poprawna kopia. Testy: `test_download_rejects_html_page`, `test_download_rejects_truncated_file`, `test_failed_extraction_keeps_previous_dataset` (Zadanie 2).
4. **Nazwa kolumny będąca słowem kluczowym SQL i apostrof w ścieżce.** Stare dane testowe mają kolumnę `end`, a ścieżki bywają dowolne. Testy: `test_bench_client_reads_csv_with_reserved_column_names`, `test_bench_client_quotes_apostrophe_in_path` (Zadanie 4).
5. **Ścieżka danych, której nie ma.** Oczekiwany jest błąd zawierający tę ścieżkę, a nie mylące „table not found” gdzieś w planowaniu. Test: `test_missing_data_path_is_reported` (Zadanie 4).

Dodatkowo zbiór testowy ma **dokładnie typy prawdziwych danych**: `string` oraz `int32`, które DataFusion 53 czyta jako `Utf8View` i `Int32`. Rozjazd typów wyjdzie więc na małych danych, zanim trafi na prawdziwe.

## Miejsce w całości prac

To drugi z planów realizujących specyfikację:

1. P0 — Ballista w osobnych procesach (zrobione, scalone).
2. **Dane: pobranie `databio-8p`, przejście z CSV na Parquet w obu ścieżkach** (ten plan).
3. Narzędzie pomiarowe:
   - metryki `/proc` i suma kontrolna (Python i Rust, na kolumnach `KEY_COLUMNS` z tego planu);
   - czas i fazy w `bench_client`, runnery polars-bio i Saila;
   - orkiestrator, konfiguracje YAML i smoke;
   - limity gRPC, instrumentacja Saila, parametr `algorithm`.
4. Próba Saila na Kubernetesie (ograniczona czasowo; wymaga zmiany `.wslconfig`).

## Struktura plików

| Plik | Rola |
|---|---|
| `bench/__init__.py`, `bench/data/__init__.py` (nowe) | pakiet narzędzia pomiarowego (specyfikacja 8.1) |
| `bench/data/datasets.py` (nowy) | rejestr zbiorów (id → katalog), oczekiwane liczby wierszy, katalog danych z `BENCH_DATA_ROOT`, rozwiązywanie identyfikatorów `a` / `a-b` |
| `bench/data/download.py` (nowy) | pobranie archiwum, rozpakowanie samych plików danych, weryfikacja, CLI `python -m bench.data.download` |
| `bench/ops.py` (nowy) | operacje, schemat znormalizowany (8.4), mapowanie nazw kolumn polars-bio, multizbiór wierszy do porównań |
| `tests/bench/__init__.py`, `tests/bench/test_datasets.py`, `tests/bench/test_download.py`, `tests/bench/test_ops.py` (nowe) | testy jednostkowe `bench/` (bez polars-bio) |
| `tests/parquet_fixture.py` (nowy) | mały zbiór w układzie databio-8p (katalog Parquet, `int32`) z przypadkami brzegowymi |
| `tests/generic_oracle.py` (nowy) | wyrocznia polars-bio na dowolnych plikach Parquet, wynik w schemacie znormalizowanym |
| `tests/conftest.py` | fikstury sesyjne zbioru testowego i wyniku wyroczni; marker `dane` |
| `tests/test_generic_oracle.py` (nowy) | schemat wyroczni i semantyka chromosomów jednostronnych |
| `ballista_genomics/src/cli.rs` (nowy) | ścisłe parsowanie flag (przeniesione z `ballista_node.rs`, plus zakaz powtórzeń) |
| `ballista_genomics/src/scenario.rs` (nowy) | scenariusz → SQL z wynikiem w schemacie znormalizowanym |
| `ballista_genomics/src/runner.rs` | `scheduler_url()`, `connect_from_env()` wspólne dla `dist_ops` i `bench_client` |
| `ballista_genomics/src/dist_provider.rs` | rejestracja źródła: Parquet albo CSV po ścieżce, błąd z nazwą brakującej ścieżki |
| `ballista_genomics/src/bin/bench_client.rs` (nowy) | runner Ballisty: jeden scenariusz, strumieniowa konsumpcja, jedna linia JSON na stdout |
| `ballista_genomics/src/bin/ballista_node.rs` | korzysta z `cli.rs` |
| `ballista_genomics/src/lib.rs`, `ballista_genomics/Cargo.toml` | moduły `cli`, `scenario`; zależność `futures` |
| `tests/test_ballista_parquet.py` (nowy) | `bench_client`: argumenty, błędy, pięć operacji na zbiorze testowym (standalone i klaster), CSV |
| `sail_bio.py` (nowy, w katalogu głównym obok `sail_pb_guard.py`) | Sail: `spark.read.parquet`, uogólnione UDTF-y, wynik znormalizowany |
| `tests/test_sail_parquet.py` (nowy) | Sail na zbiorze testowym |
| `tests/test_real_data.py` (nowy) | test akceptacyjny: para 1-2 w Ballistcie standalone, na klastrze i w Sailu |
| `ballista_genomics/OPIS.md`, specyfikacja (sekcje 4, 8.1, 9.3) | dokumentacja wyniku planu |

Testy Ballisty są czarnoskrzynkowe w Pythonie, zgodnie z konwencją repozytorium. Osobny harness testów Rusta oznaczałby dodatkowe, kosztowne linkowanie na tej maszynie.

---

### Task 1: Rejestr zbiorów databio-8p

**Files:**
- Create: `bench/__init__.py`, `bench/data/__init__.py`, `bench/data/datasets.py`
- Test: `tests/bench/__init__.py`, `tests/bench/test_datasets.py`

**Interfaces:**
- Consumes: nic.
- Produces (`bench.data.datasets`):
  - `DATA_ROOT_ENV = "BENCH_DATA_ROOT"`, `DEFAULT_DATA_ROOT: Path`, `DATASET_NAME = "databio-8p"`;
  - `COLUMNS: tuple[str, str, str] = ("contig", "pos_start", "pos_end")`;
  - `DATASETS: dict[int, str]` (id → nazwa katalogu), `EXPECTED_ROWS_K: dict[int, int]`;
  - `data_dir(root: Path | None = None) -> Path`;
  - `dataset_dir(idx: int, root: Path | None = None) -> Path`;
  - `resolve(scenario: str, root: Path | None = None) -> tuple[Path, Path | None]`.

- [ ] **Step 1: Napisz testy**

`tests/bench/__init__.py` — pusty plik.

`tests/bench/test_datasets.py`:

```python
"""Rejestr zbiorów databio-8p i rozwiązywanie identyfikatorów (plan 2, Zadanie 1)."""

from __future__ import annotations

import pytest

from bench.data import datasets as ds


def test_dataset_ids_match_polars_bio_bench_directories():
    # Nazwy katalogów z archiwum databio-8p.zip; identyfikatory jak w
    # conf/common.yaml polars-bio-bench (specyfikacja, sekcja 4).
    assert ds.DATASETS == {
        0: "chainRn4",
        1: "fBrain-DS14718",
        2: "exons",
        3: "chainOrnAna1",
        4: "chainVicPac2",
        5: "chainXenTro3Link",
        6: "chainMonDom5Link",
        7: "ex-anno",
        8: "ex-rna",
    }
    assert set(ds.EXPECTED_ROWS_K) == set(ds.DATASETS)
    assert ds.COLUMNS == ("contig", "pos_start", "pos_end")


def test_data_dir_uses_env_root(monkeypatch, tmp_path):
    monkeypatch.setenv("BENCH_DATA_ROOT", str(tmp_path))
    assert ds.data_dir() == tmp_path / "databio-8p"


def test_data_dir_empty_env_means_default(monkeypatch):
    monkeypatch.setenv("BENCH_DATA_ROOT", "")
    assert ds.data_dir() == ds.DEFAULT_DATA_ROOT / "databio-8p"


def test_explicit_root_wins_over_env(monkeypatch, tmp_path):
    monkeypatch.setenv("BENCH_DATA_ROOT", "/nie/tutaj")
    assert ds.data_dir(tmp_path) == tmp_path / "databio-8p"


def test_resolve_pair(tmp_path):
    base = tmp_path / "databio-8p"
    assert ds.resolve("1-2", tmp_path) == (base / "fBrain-DS14718", base / "exons")


def test_resolve_single_dataset(tmp_path):
    assert ds.resolve("0", tmp_path) == (tmp_path / "databio-8p" / "chainRn4", None)


@pytest.mark.parametrize(
    "bad", ["", "9", "1-9", "1-2-3", "a-b", "-1", "1-", " 1-2", "1-2\n", "01-2"]
)
def test_resolve_rejects_malformed_ids(bad, tmp_path):
    with pytest.raises(ValueError, match="niepoprawny identyfikator"):
        ds.resolve(bad, tmp_path)
```

- [ ] **Step 2: Uruchom — ma się nie udać**

Run: `pytest tests/bench/test_datasets.py -v`
Expected: błąd zbierania testów, `ModuleNotFoundError: No module named 'bench'`.

- [ ] **Step 3: Implementacja**

`bench/__init__.py`:

```python
"""Narzędzie pomiarowe (specyfikacja metodyki, sekcja 8)."""
```

`bench/data/__init__.py`:

```python
"""Dane benchmarku: rejestr zbiorów databio-8p i ich pobieranie."""
```

`bench/data/datasets.py`:

```python
"""Rejestr zbiorów databio-8p (polars-bio-bench) i rozwiązywanie identyfikatorów.

Identyfikatory jak w polars-bio-bench (`conf/common.yaml`): zbiór to cyfra 0–8,
para `a-b` to df1 = zbiór a, df2 = zbiór b. Dane leżą poza repozytorium, w tym
samym układzie co w polars-bio-bench:
`$BENCH_DATA_ROOT/databio-8p/<katalog zbioru>/part-*.parquet`.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

DATA_ROOT_ENV = "BENCH_DATA_ROOT"
DEFAULT_DATA_ROOT = Path.home() / "bench_data"
DATASET_NAME = "databio-8p"

#: Kolumny przedziału w plikach databio-8p (0-based, półotwarte).
COLUMNS = ("contig", "pos_start", "pos_end")

#: Identyfikator -> katalog zbioru w archiwum databio-8p.zip.
DATASETS: dict[int, str] = {
    0: "chainRn4",
    1: "fBrain-DS14718",
    2: "exons",
    3: "chainOrnAna1",
    4: "chainVicPac2",
    5: "chainXenTro3Link",
    6: "chainMonDom5Link",
    7: "ex-anno",
    8: "ex-rna",
}

#: Liczba wierszy w tysiącach (specyfikacja, sekcja 4) — do weryfikacji pobrania.
EXPECTED_ROWS_K: dict[int, int] = {
    0: 2351,
    1: 199,
    2: 439,
    3: 1957,
    4: 7684,
    5: 50981,
    6: 128187,
    7: 1194,
    8: 9945,
}

_SCENARIO = re.compile(r"([0-8])(?:-([0-8]))?")


def data_dir(root: Path | None = None) -> Path:
    """Katalog zbioru: `root` albo `$BENCH_DATA_ROOT` (pusta = brak) albo domyślny."""
    if root is None:
        env = os.environ.get(DATA_ROOT_ENV, "")
        root = Path(env) if env else DEFAULT_DATA_ROOT
    return Path(root) / DATASET_NAME


def dataset_dir(idx: int, root: Path | None = None) -> Path:
    if idx not in DATASETS:
        raise ValueError(f"nieznany zbiór {idx!r}; dozwolone: 0–8")
    return data_dir(root) / DATASETS[idx]


def resolve(scenario: str, root: Path | None = None) -> tuple[Path, Path | None]:
    """`"a-b"` -> (katalog a, katalog b); `"a"` -> (katalog a, None)."""
    m = _SCENARIO.fullmatch(scenario)
    if m is None:
        raise ValueError(
            f"niepoprawny identyfikator danych {scenario!r}; oczekiwano 'a' albo 'a-b', a, b ∈ 0–8"
        )
    left = dataset_dir(int(m.group(1)), root)
    right = dataset_dir(int(m.group(2)), root) if m.group(2) is not None else None
    return left, right
```

- [ ] **Step 4: Uruchom — ma przejść**

Run: `pytest tests/bench/test_datasets.py -v`
Expected: `16 passed`.

- [ ] **Step 5: Commit**

```bash
git add bench/__init__.py bench/data/__init__.py bench/data/datasets.py tests/bench/__init__.py tests/bench/test_datasets.py
git commit -m "Plan 2: rejestr zbiorow databio-8p i identyfikatory scenariuszy

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Pobieranie i weryfikacja zbioru

**Files:**
- Create: `bench/data/download.py`
- Test: `tests/bench/test_download.py`

**Interfaces:**
- Consumes (Task 1): `DATASETS`, `EXPECTED_ROWS_K`, `data_dir`.
- Produces (`bench.data.download`):
  - `GDRIVE_ID`, `ZIP_URL`, `ZIP_BYTES = 1_208_716_701`, `PARTS_PER_DATASET = 8`, `EXPECTED_SCHEMA: pa.Schema`, `ROW_TOLERANCE = 1000`;
  - `expected_rows() -> dict[str, int]`;
  - `extract_parts(zip_path: Path, dest: Path) -> None`;
  - `verify(dest: Path, expected: Mapping[str, int], tolerance: int = ROW_TOLERANCE) -> list[str]`;
  - `summarize(dest: Path) -> list[tuple[int, str, int, int]]`;
  - `download(url: str, target: Path, expected_bytes: int, timeout: float = 60) -> None`;
  - `ensure_dataset(root: Path | None = None, *, url=ZIP_URL, expected_bytes=ZIP_BYTES, expected: Mapping[str, int] | None = None) -> Path`;
  - `main(argv: list[str] | None = None) -> int`.

Fakty sprawdzone przy pisaniu planu (odczyt spisu archiwum przez zapytania HTTP z zakresem bajtów):
- Rozmiar archiwum to 1 208 716 701 B.
- Główne katalogi to `databio-8p/` i `__MACOSX/`. W każdym zbiorze jest 8 plików `part-*.snappy.parquet` oraz `.part-*.crc`, `_SUCCESS` i `._SUCCESS.crc`.
- Schemat każdego pliku to `contig: string, pos_start: int32, pos_end: int32`.

- [ ] **Step 1: Napisz testy**

`tests/bench/test_download.py`:

```python
"""Pobieranie i weryfikacja databio-8p (plan 2, Zadanie 2) — na syntetycznym
archiwum o układzie databio-8p.zip i lokalnym serwerze HTTP, bez sieci."""

from __future__ import annotations

import threading
import zipfile
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bench.data import download as dl

NAMES = ("exons", "fBrain-DS14718")
ROWS_PER_PART = 2
EXPECTED = {name: ROWS_PER_PART * 8 for name in NAMES}


def _table(n: int, start: int, pos_type=pa.int32()) -> pa.Table:
    return pa.table(
        {
            "contig": pa.array([f"chr{i % 3 + 1}" for i in range(n)], pa.string()),
            "pos_start": pa.array(list(range(start, start + n)), pos_type),
            "pos_end": pa.array(list(range(start + 10, start + n + 10)), pos_type),
        }
    )


def _parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def _make_zip(path: Path, pos_type=pa.int32()) -> None:
    """Archiwum o układzie databio-8p.zip: dane i śmieci, które trzeba pominąć."""
    with zipfile.ZipFile(path, "w") as zf:
        for name in NAMES:
            for i in range(8):
                fname = f"part-{i:05d}-abc-c000.snappy.parquet"
                data = _parquet_bytes(_table(ROWS_PER_PART, i * 100, pos_type))
                zf.writestr(f"databio-8p/{name}/{fname}", data)
                zf.writestr(f"databio-8p/{name}/.{fname}.crc", b"crc")
                zf.writestr(f"__MACOSX/databio-8p/{name}/._{fname}", b"\x00\x05\x16\x07Mac")
            zf.writestr(f"databio-8p/{name}/_SUCCESS", b"")
            zf.writestr(f"databio-8p/{name}/._SUCCESS.crc", b"crc")


def _no_download(*args, **kwargs):
    raise AssertionError("pobieranie nie powinno być potrzebne")


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@pytest.fixture
def http_dir(tmp_path):
    www = tmp_path / "www"
    www.mkdir()
    server = HTTPServer(("127.0.0.1", 0), partial(_QuietHandler, directory=str(www)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield www, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


@pytest.fixture
def extracted(tmp_path) -> Path:
    zip_path = tmp_path / "databio-8p.zip"
    _make_zip(zip_path)
    dest = tmp_path / "databio-8p"
    dl.extract_parts(zip_path, dest)
    return dest


def test_extract_keeps_only_data_parts(extracted, tmp_path):
    assert sorted(p.name for p in extracted.iterdir()) == sorted(NAMES)
    for name in NAMES:
        files = sorted(p.name for p in (extracted / name).iterdir())
        assert files == [f"part-{i:05d}-abc-c000.snappy.parquet" for i in range(8)]
        for f in files:
            pq.ParquetFile(extracted / name / f)  # każdy plik to prawdziwy Parquet
    assert not (tmp_path / "databio-8p.tmp").exists()


def test_verify_accepts_complete_dataset(extracted):
    assert dl.verify(extracted, EXPECTED, tolerance=0) == []


def test_verify_reports_missing_part(extracted):
    next((extracted / "exons").glob("part-*.parquet")).unlink()
    problems = dl.verify(extracted, EXPECTED, tolerance=0)
    assert len(problems) == 1 and "exons" in problems[0] and "7 plików" in problems[0]


def test_verify_reports_missing_dataset(extracted):
    problems = dl.verify(extracted, {**EXPECTED, "ex-anno": 16}, tolerance=0)
    assert len(problems) == 1 and "ex-anno" in problems[0] and "0 plików" in problems[0]


def test_verify_reports_wrong_schema(tmp_path):
    zip_path = tmp_path / "databio-8p.zip"
    _make_zip(zip_path, pos_type=pa.int64())
    dest = tmp_path / "databio-8p"
    dl.extract_parts(zip_path, dest)
    problems = dl.verify(dest, EXPECTED, tolerance=0)
    assert len(problems) == 2 and all("schemat" in p for p in problems)


def test_verify_reports_wrong_row_count(extracted):
    problems = dl.verify(extracted, {**EXPECTED, "exons": 17}, tolerance=0)
    assert len(problems) == 1 and "exons" in problems[0] and "wierszy" in problems[0]


def test_failed_extraction_keeps_previous_dataset(tmp_path):
    zip_path = tmp_path / "databio-8p.zip"
    _make_zip(zip_path)
    data = bytearray(zip_path.read_bytes())
    data[data.index(b"PAR1") + 16] ^= 0xFF  # uszkodzenie wnętrza pierwszego pliku danych
    zip_path.write_bytes(bytes(data))
    dest = tmp_path / "databio-8p"
    dest.mkdir()
    (dest / "znacznik").write_text("poprzednia wersja")
    with pytest.raises(zipfile.BadZipFile):
        dl.extract_parts(zip_path, dest)
    assert (dest / "znacznik").read_text() == "poprzednia wersja"
    assert not (tmp_path / "databio-8p.tmp").exists()


def test_download_rejects_html_page(http_dir, tmp_path):
    www, base = http_dir
    (www / "strona.html").write_text("<html>Quota exceeded</html>")
    target = tmp_path / "x.zip"
    with pytest.raises(RuntimeError, match="HTML"):
        dl.download(f"{base}/strona.html", target, expected_bytes=10)
    assert not target.exists()
    assert not (tmp_path / "x.zip.part").exists()


def test_download_rejects_truncated_file(http_dir, tmp_path):
    www, base = http_dir
    (www / "dane.bin").write_bytes(b"x" * 100)
    target = tmp_path / "x.zip"
    with pytest.raises(RuntimeError, match="niekompletne"):
        dl.download(f"{base}/dane.bin", target, expected_bytes=200)
    assert not target.exists()
    assert not (tmp_path / "x.zip.part").exists()


def test_download_saves_complete_file(http_dir, tmp_path):
    www, base = http_dir
    (www / "dane.bin").write_bytes(b"x" * 100)
    target = tmp_path / "x.zip"
    dl.download(f"{base}/dane.bin", target, expected_bytes=100)
    assert target.read_bytes() == b"x" * 100


def test_ensure_dataset_downloads_extracts_and_removes_zip(http_dir, tmp_path):
    www, base = http_dir
    _make_zip(www / "databio-8p.zip")
    size = (www / "databio-8p.zip").stat().st_size
    root = tmp_path / "root"
    dest = dl.ensure_dataset(
        root, url=f"{base}/databio-8p.zip", expected_bytes=size, expected=EXPECTED
    )
    assert dest == root / "databio-8p"
    assert dl.verify(dest, EXPECTED, tolerance=0) == []
    assert not (root / "databio-8p.zip").exists()


def test_ensure_dataset_skips_download_when_complete(tmp_path, monkeypatch):
    zip_path = tmp_path / "src.zip"
    _make_zip(zip_path)
    root = tmp_path / "root"
    dl.extract_parts(zip_path, root / "databio-8p")
    monkeypatch.setattr(dl, "download", _no_download)
    assert dl.ensure_dataset(root, expected=EXPECTED) == root / "databio-8p"


def test_ensure_dataset_reuses_downloaded_zip(tmp_path, monkeypatch):
    """Archiwum pobrane ręcznie (np. przez przeglądarkę) wystarcza."""
    root = tmp_path / "root"
    root.mkdir()
    zip_path = root / "databio-8p.zip"
    _make_zip(zip_path)
    monkeypatch.setattr(dl, "download", _no_download)
    dest = dl.ensure_dataset(root, expected_bytes=zip_path.stat().st_size, expected=EXPECTED)
    assert dl.verify(dest, EXPECTED, tolerance=0) == []


def test_main_reports_failure_with_exit_code_1(monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise RuntimeError("symulowany błąd")

    monkeypatch.setattr(dl, "ensure_dataset", fail)
    assert dl.main(["--root", "/tmp/nieistotne"]) == 1
    assert "symulowany błąd" in capsys.readouterr().err
```

- [ ] **Step 2: Uruchom — ma się nie udać**

Run: `pytest tests/bench/test_download.py -v`
Expected: błąd zbierania testów, `ImportError: cannot import name 'download' from 'bench.data'`.

- [ ] **Step 3: Implementacja**

`bench/data/download.py`:

```python
"""Pobranie i weryfikacja zbioru databio-8p (polars-bio-bench).

Źródło i układ katalogów jak w polars-bio-bench (`conf/common.yaml`,
`src/utils.py::prepare_datatests`): archiwum `databio-8p.zip` z Google Drive,
rozpakowane do `$BENCH_DATA_ROOT/databio-8p/<zbiór>/part-*.parquet`.
Świadome różnice względem tamtego mechanizmu:

- `requests` z bezpośrednim adresem pobierania zamiast `gdown` — bez nowej
  zależności. Gdy Google zwróci stronę HTML (limit pobrań), błąd z instrukcją
  pobrania ręcznego; archiwum położone ręcznie jako
  `$BENCH_DATA_ROOT/databio-8p.zip` jest używane bez ponownego pobierania.
- Rozpakowywane są TYLKO pliki danych. Archiwum zawiera też `__MACOSX/`
  z plikami `._part-*.parquet` (rozszerzenie .parquet, ale to nie Parquet),
  sumy `.crc` i znaczniki `_SUCCESS` Sparka.
- Rozpakowanie do katalogu tymczasowego i podmiana dopiero po sukcesie —
  przerwane rozpakowanie nie zostawia katalogu, który wygląda na kompletny,
  i nie niszczy poprzedniej kopii.
- Weryfikacja: 8 plików na zbiór, schemat, liczba wierszy zgodna ze
  specyfikacją (sekcja 4).

Użycie: python -m bench.data.download [--root KATALOG]
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import zipfile
from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from bench.data.datasets import DATASETS, EXPECTED_ROWS_K, data_dir

GDRIVE_ID = "1Sj7nTB5gCUq9nbeQOg4zzS4tKO37M5Nd"
ZIP_URL = (
    f"https://drive.usercontent.google.com/download?id={GDRIVE_ID}&export=download&confirm=t"
)
ZIP_BYTES = 1_208_716_701
PARTS_PER_DATASET = 8
EXPECTED_SCHEMA = pa.schema(
    [("contig", pa.string()), ("pos_start", pa.int32()), ("pos_end", pa.int32())]
)
ROW_TOLERANCE = 1000

_PART = re.compile(r"databio-8p/([^/]+)/(part-[^/]+\.parquet)")


def expected_rows() -> dict[str, int]:
    """Oczekiwane liczby wierszy (nazwa katalogu -> wiersze), ze specyfikacji."""
    return {DATASETS[i]: k * 1000 for i, k in EXPECTED_ROWS_K.items()}


def extract_parts(zip_path: Path, dest: Path) -> None:
    """Rozpakowuje wyłącznie `databio-8p/<zbiór>/part-*.parquet` do `dest/<zbiór>/`."""
    names = set(DATASETS.values())
    tmp = dest.with_name(dest.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for info in zf.infolist():
                m = _PART.fullmatch(info.filename)
                if m is None or m.group(1) not in names:
                    continue
                out = tmp / m.group(1) / m.group(2)
                out.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, open(out, "wb") as dst:
                    shutil.copyfileobj(src, dst, 1 << 20)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    shutil.rmtree(dest, ignore_errors=True)
    tmp.rename(dest)


def _schema_text(schema: pa.Schema) -> str:
    return ", ".join(f"{f.name}: {f.type}" for f in schema)


def verify(
    dest: Path, expected: Mapping[str, int], tolerance: int = ROW_TOLERANCE
) -> list[str]:
    """Lista problemów (pusta = zbiór kompletny). Czyta tylko stopki plików."""
    problems: list[str] = []
    for name, rows_expected in expected.items():
        d = dest / name
        parts = sorted(d.glob("part-*.parquet")) if d.is_dir() else []
        if len(parts) != PARTS_PER_DATASET:
            problems.append(
                f"{name}: {len(parts)} plików part-*.parquet zamiast {PARTS_PER_DATASET}"
            )
            continue
        rows = 0
        for p in parts:
            try:
                pf = pq.ParquetFile(p)
            except Exception as e:  # uszkodzony albo nie-Parquet
                problems.append(f"{name}/{p.name}: nieczytelny Parquet ({e})")
                break
            if not pf.schema_arrow.equals(EXPECTED_SCHEMA):
                problems.append(
                    f"{name}/{p.name}: schemat {_schema_text(pf.schema_arrow)} "
                    f"zamiast {_schema_text(EXPECTED_SCHEMA)}"
                )
                break
            rows += pf.metadata.num_rows
        else:
            if abs(rows - rows_expected) > tolerance:
                problems.append(
                    f"{name}: {rows} wierszy, oczekiwano {rows_expected} ± {tolerance}"
                )
    return problems


def summarize(dest: Path) -> list[tuple[int, str, int, int]]:
    """(id, zbiór, liczba plików, liczba wierszy) dla każdego zbioru."""
    out = []
    for idx, name in DATASETS.items():
        files = sorted((dest / name).glob("part-*.parquet"))
        rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
        out.append((idx, name, len(files), rows))
    return out


def download(url: str, target: Path, expected_bytes: int, timeout: float = 60) -> None:
    """Pobiera strumieniowo do `target.part`; nazwa docelowa dopiero po sprawdzeniu rozmiaru."""
    part = target.with_name(target.name + ".part")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        if r.headers.get("Content-Type", "").startswith("text/html"):
            raise RuntimeError(
                "Google Drive zwrócił stronę HTML zamiast archiwum (limit pobrań albo zmiana "
                f"adresu). Pobierz ręcznie https://drive.google.com/uc?id={GDRIVE_ID} "
                f"i zapisz jako {target}"
            )
        with open(part, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    size = part.stat().st_size
    if size != expected_bytes:
        part.unlink()
        raise RuntimeError(
            f"pobrano {size} B zamiast {expected_bytes} B — archiwum niekompletne"
        )
    part.rename(target)


def ensure_dataset(
    root: Path | None = None,
    *,
    url: str = ZIP_URL,
    expected_bytes: int = ZIP_BYTES,
    expected: Mapping[str, int] | None = None,
) -> Path:
    """Zapewnia kompletny zbiór w `data_dir(root)`; zwraca jego katalog."""
    expected = expected_rows() if expected is None else expected
    dest = data_dir(root)
    if dest.is_dir() and not verify(dest, expected):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    zip_path = dest.parent / f"{dest.name}.zip"
    if not (zip_path.is_file() and zip_path.stat().st_size == expected_bytes):
        download(url, zip_path, expected_bytes)
    extract_parts(zip_path, dest)
    problems = verify(dest, expected)
    if problems:
        raise RuntimeError(
            "zbiór po rozpakowaniu jest niepoprawny (archiwum zostaje do wglądu):\n"
            + "\n".join(problems)
        )
    zip_path.unlink()
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pobiera i weryfikuje zbiór databio-8p.")
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="katalog danych (domyślnie $BENCH_DATA_ROOT albo ~/bench_data)",
    )
    args = parser.parse_args(argv)
    try:
        dest = ensure_dataset(args.root)
    except (RuntimeError, OSError, requests.RequestException, zipfile.BadZipFile) as e:
        print(f"BŁĄD: {e}", file=sys.stderr)
        return 1
    print(f"{'id':>2}  {'zbiór':<18} {'pliki':>5} {'wiersze':>12}")
    for idx, name, files, rows in summarize(dest):
        print(f"{idx:>2}  {name:<18} {files:>5} {rows:>12}")
    print(f"Dane: {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Uruchom — ma przejść**

Run: `pytest tests/bench/ -v`
Expected: `30 passed` (16 z Zadania 1 + 14 z tego zadania).

- [ ] **Step 5: Commit**

```bash
git add bench/data/download.py tests/bench/test_download.py
git commit -m "Plan 2: pobieranie i weryfikacja zbioru databio-8p

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Wspólny schemat wyników, zbiór testowy i wyrocznia

**Files:**
- Create: `bench/ops.py`, `tests/parquet_fixture.py`, `tests/generic_oracle.py`
- Modify: `tests/conftest.py`
- Test: `tests/bench/test_ops.py`, `tests/test_generic_oracle.py`

**Interfaces:**
- Consumes (Task 1): `bench.data.datasets.COLUMNS`.
- Produces:
  - `bench.ops`:
    - `OPS = ("overlap", "nearest", "coverage", "merge", "subtract")`, `UNARY_OPS = frozenset({"merge"})`;
    - `OUTPUT_COLUMNS: dict[str, tuple[str, ...]]`, `KEY_COLUMNS: dict[str, tuple[str, ...]]`;
    - `polars_bio_names(op: str, cols: tuple[str, str, str]) -> dict[str, str]`;
    - `normalize_polars_bio(op: str, df: pl.DataFrame, cols: tuple[str, str, str]) -> pl.DataFrame`;
    - `row_multiset(op: str, df: pl.DataFrame | pd.DataFrame) -> Counter`;
    - `describe_diff(expected: Counter, actual: Counter, limit: int = 5) -> str`.
  - `tests.parquet_fixture`: `FIXTURE_A`, `FIXTURE_B: list[tuple[str, int, int]]`, `write_parts(rows, directory: Path, n_files: int = 2) -> Path`.
  - `tests.generic_oracle`: `read_intervals(path: Path) -> pl.DataFrame`, `reference(op: str, left: pl.DataFrame, right: pl.DataFrame | None, cols=COLUMNS) -> pl.DataFrame` (wynik znormalizowany).
  - Fikstury sesyjne w `tests/conftest.py`: `parquet_dirs -> tuple[Path, Path]` (katalogi A i B, po 2 pliki), `parquet_expected -> dict[str, Counter]` (operacja → multizbiór wierszy wyroczni).

Nazwy kolumn wyników polars-bio 0.28 dla kolumn `contig/pos_start/pos_end` zostały sprawdzone sondą przy pisaniu planu:
- `overlap`: `contig_1, pos_start_1, pos_end_1, contig_2, pos_start_2, pos_end_2`;
- `nearest`: jak `overlap` plus `distance`;
- `coverage`: `contig, pos_start, pos_end, coverage`;
- `merge`: `contig, pos_start, pos_end, n_intervals`;
- `subtract`: `contig, pos_start, pos_end`.

Chromosom obecny tylko w lewej tabeli daje w `nearest` wiersz z pustym sąsiadem i pustą odległością, w `coverage` pokrycie 0, a w `subtract` przedział bez zmian.

- [ ] **Step 1: Napisz testy `bench/ops.py`**

`tests/bench/test_ops.py`:

```python
"""Wspólny schemat wyników i porównanie niezależne od kolejności (plan 2, Zadanie 3)."""

from __future__ import annotations

from collections import Counter

import pandas as pd
import polars as pl
import pytest

from bench import ops

DATABIO = ("contig", "pos_start", "pos_end")


def test_output_columns_follow_spec_8_4():
    assert ops.OUTPUT_COLUMNS == {
        "overlap": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"),
        "nearest": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2", "distance"),
        "coverage": ("chrom", "start", "end", "coverage"),
        "merge": ("chrom", "start", "end", "n_intervals"),
        "subtract": ("chrom", "start", "end"),
    }
    # nearest bez tożsamości sąsiada — przy remisach silniki wybierają różnie.
    assert ops.KEY_COLUMNS["nearest"] == ("chrom_1", "start_1", "end_1", "distance")
    assert set(ops.OPS) == set(ops.OUTPUT_COLUMNS) and ops.UNARY_OPS == {"merge"}


def test_polars_bio_names_for_databio_columns():
    assert ops.polars_bio_names("overlap", DATABIO) == {
        "contig_1": "chrom_1", "pos_start_1": "start_1", "pos_end_1": "end_1",
        "contig_2": "chrom_2", "pos_start_2": "start_2", "pos_end_2": "end_2",
    }
    assert ops.polars_bio_names("coverage", DATABIO) == {
        "contig": "chrom", "pos_start": "start", "pos_end": "end", "coverage": "coverage",
    }


def test_normalize_polars_bio_renames_and_orders():
    df = pl.DataFrame({"n_intervals": [2], "pos_end": [300], "contig": ["chr1"], "pos_start": [100]})
    out = ops.normalize_polars_bio("merge", df, DATABIO)
    assert out.columns == ["chrom", "start", "end", "n_intervals"]
    assert out.row(0) == ("chr1", 100, 300, 2)


def test_normalize_polars_bio_reports_missing_columns():
    df = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [2]})
    with pytest.raises(KeyError, match="pos_start"):
        ops.normalize_polars_bio("subtract", df, DATABIO)


def test_row_multiset_ignores_order_and_integer_width():
    a = pl.DataFrame(
        {"chrom": ["chr1", "chr2"], "start": [1, 5], "end": [3, 9]},
        schema={"chrom": pl.Utf8, "start": pl.Int32, "end": pl.Int32},
    )
    b = pl.DataFrame(
        {"chrom": ["chr2", "chr1"], "start": [5, 1], "end": [9, 3]},
        schema={"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64},
    )
    assert ops.row_multiset("subtract", a) == ops.row_multiset("subtract", b)


def test_row_multiset_counts_duplicates():
    one = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [3]})
    two = pl.concat([one, one])
    assert ops.row_multiset("subtract", two) == Counter({("chr1", 1, 3): 2})
    assert ops.row_multiset("subtract", one) != ops.row_multiset("subtract", two)


def test_row_multiset_treats_pandas_nan_as_null():
    pdf = pd.DataFrame({
        "chrom_1": ["chrA"], "start_1": [10], "end_1": [20],
        "chrom_2": [None], "start_2": [float("nan")], "end_2": [float("nan")],
        "distance": [float("nan")],
    })
    pldf = pl.DataFrame(
        {"chrom_1": ["chrA"], "start_1": [10], "end_1": [20],
         "chrom_2": [None], "start_2": [None], "end_2": [None], "distance": [None]},
        schema={"chrom_1": pl.Utf8, "start_1": pl.Int32, "end_1": pl.Int32,
                "chrom_2": pl.Utf8, "start_2": pl.Int32, "end_2": pl.Int32,
                "distance": pl.Int64},
    )
    expected = Counter({("chrA", 10, 20, None): 1})
    assert ops.row_multiset("nearest", pdf) == expected
    assert ops.row_multiset("nearest", pldf) == expected


def test_nearest_key_ignores_neighbor_identity():
    base = {"chrom_1": ["chr1"], "start_1": [100], "end_1": [200],
            "chrom_2": ["chr1"], "end_2": [260], "distance": [0]}
    a = pl.DataFrame({**base, "start_2": [180]})
    b = pl.DataFrame({**base, "start_2": [150]})
    assert ops.row_multiset("nearest", a) == ops.row_multiset("nearest", b)


def test_row_multiset_reports_missing_key_column():
    with pytest.raises(KeyError, match="coverage"):
        ops.row_multiset("coverage", pl.DataFrame({"chrom": ["c"], "start": [1], "end": [2]}))


def test_describe_diff_shows_counts_and_examples():
    text = ops.describe_diff(Counter({("a",): 2, ("b",): 1}), Counter({("a",): 1, ("c",): 1}))
    assert "brakuje 2" in text and "nadmiarowych 1" in text and "('c',)" in text
```

- [ ] **Step 2: Uruchom — ma się nie udać**

Run: `pytest tests/bench/test_ops.py -v`
Expected: błąd zbierania testów, `ImportError: cannot import name 'ops' from 'bench'`.

- [ ] **Step 3: Implementacja `bench/ops.py`**

```python
"""Operacje benchmarku i wspólny schemat ich wyników (specyfikacja, sekcja 8.4).

Każdy silnik zwraca wynik pod innymi nazwami kolumn. Porównania (a w kolejnym
etapie suma kontrolna) działają na schemacie znormalizowanym poniżej, na
multizbiorze wierszy — niezależnie od kolejności wierszy i szerokości typów.
"""

from __future__ import annotations

from collections import Counter

import polars as pl

OPS = ("overlap", "nearest", "coverage", "merge", "subtract")
UNARY_OPS = frozenset({"merge"})

#: Kolumny wyniku w schemacie znormalizowanym, w tej kolejności.
OUTPUT_COLUMNS: dict[str, tuple[str, ...]] = {
    "overlap": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"),
    "nearest": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2", "distance"),
    "coverage": ("chrom", "start", "end", "coverage"),
    "merge": ("chrom", "start", "end", "n_intervals"),
    "subtract": ("chrom", "start", "end"),
}

#: Kolumny wchodzące do porównania. `nearest` bez identyfikacji sąsiada: przy
#: remisach (kilku kandydatów w tej samej odległości) silniki wybierają różnie.
KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    **OUTPUT_COLUMNS,
    "nearest": ("chrom_1", "start_1", "end_1", "distance"),
}


def polars_bio_names(op: str, cols: tuple[str, str, str]) -> dict[str, str]:
    """Nazwy kolumn wyniku polars-bio 0.28 -> schemat znormalizowany."""
    c, s, e = cols
    if op in ("overlap", "nearest"):
        names = {
            f"{c}_1": "chrom_1", f"{s}_1": "start_1", f"{e}_1": "end_1",
            f"{c}_2": "chrom_2", f"{s}_2": "start_2", f"{e}_2": "end_2",
        }
        if op == "nearest":
            names["distance"] = "distance"
        return names
    names = {c: "chrom", s: "start", e: "end"}
    if op == "coverage":
        names["coverage"] = "coverage"
    elif op == "merge":
        names["n_intervals"] = "n_intervals"
    return names


def normalize_polars_bio(op: str, df: pl.DataFrame, cols: tuple[str, str, str]) -> pl.DataFrame:
    names = polars_bio_names(op, cols)
    missing = [src for src in names if src not in df.columns]
    if missing:
        raise KeyError(f"{op}: wynik polars-bio nie ma kolumn {missing}; ma {df.columns}")
    return df.select([pl.col(src).alias(dst) for src, dst in names.items()])


def row_multiset(op: str, df) -> Counter:
    """Multizbiór wierszy po kolumnach klucza (polars albo pandas; NaN = brak)."""
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df, nan_to_null=True)
    key = KEY_COLUMNS[op]
    missing = [k for k in key if k not in df.columns]
    if missing:
        raise KeyError(f"{op}: brak kolumn {missing}; są {df.columns}")
    exprs = [
        pl.col(k).cast(pl.Utf8) if k.startswith("chrom") else pl.col(k).cast(pl.Int64)
        for k in key
    ]
    return Counter(df.select(exprs).iter_rows())


def describe_diff(expected: Counter, actual: Counter, limit: int = 5) -> str:
    """Krótki opis różnicy dwóch multizbiorów — liczności i kilka przykładów."""
    missing = expected - actual
    extra = actual - expected
    return (
        f"brakuje {sum(missing.values())} wierszy, np. {list(missing)[:limit]}; "
        f"nadmiarowych {sum(extra.values())}, np. {list(extra)[:limit]}"
    )
```

- [ ] **Step 4: Uruchom — ma przejść**

Run: `pytest tests/bench/test_ops.py -v`
Expected: `10 passed`.

- [ ] **Step 5: Napisz zbiór testowy, fikstury i testy wyroczni**

`tests/parquet_fixture.py`:

```python
"""Mały zbiór testowy w układzie databio-8p: katalog z kilkoma plikami Parquet,
kolumny contig (string), pos_start i pos_end (int32), 0-based półotwarte — te
same nazwy i typy co w prawdziwych plikach (sprawdzone na archiwum).

Przypadki, na których silniki mogą się rozjechać:
- chromosom tylko w A (chrA) i tylko w B (chrB);
- duplikat przedziału w A;
- przedziały stykające się (koniec jednego = początek drugiego), w A i między A a B;
- chr10 obok chr2 (porządek leksykograficzny różny od numerycznego);
- nakładające się przedziały chr1 w RÓŻNYCH plikach (merge wymaga shuffle).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# write_parts rozdziela wiersze na przemian: parzyste -> plik 0, nieparzyste -> plik 1.
FIXTURE_A = [
    ("chr1", 100, 200),    # plik 0
    ("chr1", 150, 300),    # plik 1 — nakłada się na poprzedni
    ("chr1", 400, 500),    # plik 0
    ("chr1", 100, 200),    # plik 1 — duplikat
    ("chr2", 50, 150),     # plik 0
    ("chr2", 200, 350),    # plik 1
    ("chr2", 350, 400),    # plik 0 — styka się z poprzednim
    ("chr10", 1000, 1100), # plik 1
    ("chrA", 10, 20),      # plik 0 — chromosom tylko w A
]

FIXTURE_B = [
    ("chr1", 180, 250),
    ("chr1", 290, 420),
    ("chr1", 450, 600),
    ("chr2", 100, 220),
    ("chr2", 300, 400),
    ("chr10", 1100, 1200), # styka się z chr10 z A
    ("chrB", 5, 50),       # chromosom tylko w B
]


def write_parts(rows: list[tuple[str, int, int]], directory: Path, n_files: int = 2) -> Path:
    """Zapisuje przedziały do `n_files` plików part-*.parquet (wiersze na przemian)."""
    directory.mkdir(parents=True)
    for i in range(n_files):
        chunk = rows[i::n_files]
        table = pa.table({
            "contig": pa.array([r[0] for r in chunk], pa.string()),
            "pos_start": pa.array([r[1] for r in chunk], pa.int32()),
            "pos_end": pa.array([r[2] for r in chunk], pa.int32()),
        })
        pq.write_table(table, directory / f"part-{i:05d}.parquet")
    return directory
```

Dopisz na końcu `tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def parquet_dirs(tmp_path_factory):
    """Zbiór testowy w układzie databio-8p (tests/parquet_fixture.py): katalogi A i B."""
    from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts

    root = tmp_path_factory.mktemp("parquet_fixture")
    return write_parts(FIXTURE_A, root / "a"), write_parts(FIXTURE_B, root / "b")


@pytest.fixture(scope="session")
def parquet_expected(parquet_dirs):
    """Wynik polars-bio na zbiorze testowym: operacja -> multizbiór wierszy.
    Raz na sesję, bo import polars-bio trwa kilka minut."""
    from bench.ops import OPS, UNARY_OPS, row_multiset
    from tests.generic_oracle import read_intervals, reference

    a, b = (read_intervals(d) for d in parquet_dirs)
    return {
        op: row_multiset(op, reference(op, a, None if op in UNARY_OPS else b)) for op in OPS
    }
```

`tests/test_generic_oracle.py`:

```python
"""Wyrocznia polars-bio na plikach Parquet (plan 2, Zadanie 3): schemat
znormalizowany i semantyka chromosomów obecnych tylko po jednej stronie —
wzorzec, który muszą odtworzyć Ballista i Sail.

Uruchomienie: pytest tests/test_generic_oracle.py -v (import polars-bio trwa kilka minut)
"""

from __future__ import annotations

import pytest

from bench.ops import OPS, OUTPUT_COLUMNS, UNARY_OPS


@pytest.fixture(scope="module")
def oracle_frames(parquet_dirs):
    from tests.generic_oracle import read_intervals, reference

    a, b = (read_intervals(d) for d in parquet_dirs)
    return {op: reference(op, a, None if op in UNARY_OPS else b) for op in OPS}


def test_fixture_is_split_across_files(parquet_dirs):
    for d in parquet_dirs:
        assert len(list(d.glob("part-*.parquet"))) == 2


@pytest.mark.parametrize("op", OPS)
def test_oracle_returns_normalized_schema(op, oracle_frames):
    assert tuple(oracle_frames[op].columns) == OUTPUT_COLUMNS[op]
    assert oracle_frames[op].height > 0


def test_oracle_one_sided_chromosomes(oracle_frames):
    rows = {op: set(oracle_frames[op].iter_rows()) for op in OPS}
    assert ("chrA", 10, 20, None, None, None, None) in rows["nearest"]
    assert ("chrA", 10, 20, 0) in rows["coverage"]
    assert ("chrA", 10, 20) in rows["subtract"]
    assert not any("chrA" in r or "chrB" in r for r in rows["overlap"])
    assert not any(r[0] == "chrB" for op in OPS if op != "overlap" for r in rows[op])
```

- [ ] **Step 6: Uruchom — ma się nie udać**

Run: `pytest tests/test_generic_oracle.py -v`
Expected: `test_fixture_is_split_across_files` przechodzi. Pozostałe mają błąd (ERROR) z `ModuleNotFoundError: No module named 'tests.generic_oracle'`.

- [ ] **Step 7: Implementacja wyroczni**

`tests/generic_oracle.py`:

```python
"""Wyrocznia dla danych w układzie databio-8p: polars-bio lokalnie, wynik
w schemacie znormalizowanym (bench/ops.py, specyfikacja 8.4).

Uzupełnia wyrocznie z tests/*_oracle.py, które działają na zbiorze pięciu
przedziałów z kolumną `name`; ta działa na dowolnych plikach Parquet.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import polars_bio as pb

from bench.data.datasets import COLUMNS
from bench.ops import normalize_polars_bio


def _reset_pb_context() -> None:
    """polars-bio trzyma globalny kontekst z tabelami s1/s2 — jak w pozostałych wyroczniach."""
    from polars_bio.context import ctx as _pb_ctx

    for table in ["s1", "s2"]:
        try:
            _pb_ctx.deregister_table(table)
        except Exception:
            pass


def read_intervals(path: Path) -> pl.DataFrame:
    """Plik Parquet albo katalog plików Parquet -> DataFrame oznaczony jako 0-based."""
    path = Path(path)
    df = pl.read_parquet(str(path / "*.parquet") if path.is_dir() else str(path))
    df.config_meta.set(coordinate_system_zero_based=True)
    return df


def reference(
    op: str,
    left: pl.DataFrame,
    right: pl.DataFrame | None,
    cols: tuple[str, str, str] = COLUMNS,
) -> pl.DataFrame:
    """Wynik polars-bio dla operacji, w schemacie znormalizowanym."""
    c = list(cols)
    _reset_pb_context()
    if op == "merge":
        result = pb.merge(left, cols=c)
    elif op in ("overlap", "nearest", "coverage", "subtract"):
        result = getattr(pb, op)(left, right, cols1=c, cols2=c)
    else:
        raise ValueError(f"nieznana operacja {op!r}")
    return normalize_polars_bio(op, result.collect(), tuple(cols))
```

- [ ] **Step 8: Uruchom — ma przejść**

Run: `pytest tests/test_generic_oracle.py tests/bench/ -v`
Expected: `47 passed` (7 wyroczni + 40 jednostkowych). Import polars-bio trwa kilka minut.

- [ ] **Step 9: Commit**

```bash
git add bench/ops.py tests/bench/test_ops.py tests/parquet_fixture.py tests/generic_oracle.py tests/conftest.py tests/test_generic_oracle.py
git commit -m "Plan 2: wspolny schemat wynikow, zbior testowy Parquet i wyrocznia

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Ballista — Parquet w `dist_provider.rs` i runner `bench_client`

**Files:**
- Create: `ballista_genomics/src/cli.rs`, `ballista_genomics/src/scenario.rs`, `ballista_genomics/src/bin/bench_client.rs`, `tests/test_ballista_parquet.py`
- Modify: `ballista_genomics/src/runner.rs`, `ballista_genomics/src/dist_provider.rs`, `ballista_genomics/src/bin/ballista_node.rs`, `ballista_genomics/src/lib.rs`, `ballista_genomics/Cargo.toml`

**Interfaces:**
- Consumes:
  - Task 3: `bench.ops.OPS`, `UNARY_OPS`, `OUTPUT_COLUMNS`, `row_multiset`, `describe_diff`; fikstury `parquet_dirs` i `parquet_expected`.
  - P0: `tests.test_ballista_multiprocess._start_cluster(log_dir: Path, executors: list[tuple[str, list[str]]]) -> Cluster` (`Cluster.url` → `df://localhost:<port>`, `Cluster.stop()`).
- Produces:
  - CLI: `bench_client --op <op> --left <ścieżka> [--right <ścieżka>] [--cols c,s,e] [--output plik.parquet]`.
    - stdout: dokładnie jedna linia `{"rows": N}`;
    - kody wyjścia: 0 sukces, 1 błąd wykonania, 2 błędne argumenty (stderr zawiera „użycie”);
    - tryb klastra przez `BALLISTA_SCHEDULER_URL`;
    - `--output` zapisuje wynik w schemacie `OUTPUT_COLUMNS[op]`.
  - Rust:
    - `ballista_genomics::cli::{Flags, parse_flags, required, optional}`;
    - `ballista_genomics::scenario::Scenario { op, left, right, cols }` z `validate() -> Result<(), String>` i `sql() -> String`;
    - `ballista_genomics::runner::{scheduler_url, connect_from_env}`.
  - Python (dla Zadania 6): `tests.test_ballista_parquet.run_client(args, env=None, timeout=180)`, `op_args(op, left, right, out)`, `check_result(op, completed, out, expected)`.

Fakty sprawdzone przy pisaniu planu:
- Dostawca (`vendor/datafusion-bio-function-ranges`) obsługuje pozycje `Int32` (`array_utils.rs`, `PosArray::Int32`) i tekst `StringViewArray`.
- `merge` i `subtract` budują wyjście jako `Utf8`/`Int64`. Pozostałe operacje przejmują typy wejścia.
- DataFusion 53 czyta tekst z Parqueta jako `Utf8View` (`schema_force_view_types`, domyślnie `true`).
- Nazwy kolumn wyjścia to `left_<kolumna>` i `right_<kolumna>` (overlap, nearest), kolumny tabeli `targets` plus `coverage` (coverage), `<kolumny>` plus `n_intervals` (merge) oraz `<kolumny>` (subtract).

- [ ] **Step 1: Napisz testy**

`tests/test_ballista_parquet.py`:

```python
"""Plan 2, Zadanie 4: Ballista czyta dane w układzie databio-8p (katalog plików
Parquet, contig: string, pos_start/pos_end: int32) przez runner `bench_client`.

Wynik każdej z pięciu operacji ma być zgodny z polars-bio na tych samych
plikach (multizbiór wierszy w schemacie znormalizowanym, specyfikacja 8.4) —
w trybie standalone i na klastrze z osobnych procesów (P0). Zbiór testowy
(tests/parquet_fixture.py) zawiera przypadki, na których silniki mogą się
rozjechać: chromosom tylko po jednej stronie, duplikat, przedziały stykające
się, chr10 obok chr2.

Brak binarki to BŁĄD, nie pominięcie (jak w P0). Budowanie:
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin bench_client --bin ballista_node
Uruchomienie: pytest tests/test_ballista_parquet.py -v (import polars-bio trwa kilka minut)
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pytest

from bench.ops import OPS, OUTPUT_COLUMNS, UNARY_OPS, describe_diff, row_multiset
from tests.test_ballista_multiprocess import _start_cluster

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
CLIENT = BALLISTA_DIR / "target" / "debug" / "bench_client"
BUILD_HINT = (
    "cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin bench_client --bin ballista_node"
)


def run_client(
    args: list[str], env: dict[str, str] | None = None, timeout: int = 180
) -> subprocess.CompletedProcess:
    if not CLIENT.exists():
        pytest.fail(f"brak binarki bench_client — zbuduj: {BUILD_HINT}")
    return subprocess.run(
        [str(CLIENT), *args], cwd=BALLISTA_DIR, capture_output=True, text=True,
        timeout=timeout, env=env,
    )


def op_args(op: str, left: Path, right: Path | None, out: Path) -> list[str]:
    args = ["--op", op, "--left", str(left), "--output", str(out)]
    return args if op in UNARY_OPS else [*args, "--right", str(right)]


def check_result(op: str, r: subprocess.CompletedProcess, out: Path, expected) -> None:
    assert r.returncode == 0, f"bench_client {op}: kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    df = pl.read_parquet(out)
    assert json.loads(lines[0]) == {"rows": df.height}
    assert tuple(df.columns) == OUTPUT_COLUMNS[op]
    actual = row_multiset(op, df)
    assert actual == expected, f"{op}: rozjazd z polars-bio — {describe_diff(expected, actual)}"


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--op", "merge"],
        ["--op", "join", "--left", "x"],
        ["--op", "overlap", "--left", "x"],
        ["--op", "merge", "--left", "x", "--right", "y"],
        ["--op", "merge", "--left", "x", "--cols", "contig,pos_start"],
        ["--op", "merge", "--left", "x", "--cols", "contig,,pos_end"],
        ["--op", "merge", "--left", "x", "--lef", "y"],
        ["--op", "merge", "--left", "x", "--left", "y"],
        ["--op", "merge", "--left"],
    ],
)
def test_bench_client_rejects_bad_arguments(args):
    r = run_client(args, timeout=30)
    assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
    assert "użycie" in r.stderr
    assert r.stdout == ""


def test_missing_data_path_is_reported(tmp_path):
    missing = tmp_path / "nie_ma_takiego_katalogu"
    r = run_client(["--op", "merge", "--left", str(missing)])
    assert r.returncode == 1, r.stderr
    assert str(missing) in r.stderr
    assert r.stdout == ""


@pytest.mark.parametrize("op", OPS)
def test_bench_client_matches_polars_bio_on_parquet(op, parquet_dirs, parquet_expected, tmp_path):
    a, b = parquet_dirs
    out = tmp_path / f"{op}.parquet"
    check_result(op, run_client(op_args(op, a, b, out)), out, parquet_expected[op])


def test_bench_client_reads_csv_with_reserved_column_names(tmp_path):
    """Dotychczasowe dane CSV — kolumny chrom, start, end (`end` to słowo kluczowe SQL)."""
    from tests.merge_oracle import reference_merge_intervals
    from tests.test_ballista_distributed_ops import INTERVALS_A

    out = tmp_path / "merge.parquet"
    r = run_client(
        ["--op", "merge", "--left", "data/parts_a", "--cols", "chrom,start,end", "--output", str(out)]
    )
    assert r.returncode == 0, r.stderr
    df = pl.read_parquet(out)
    assert tuple(df.columns) == OUTPUT_COLUMNS["merge"]
    actual = {(c, int(s), int(e)) for c, s, e, _ in df.iter_rows()}
    assert actual == reference_merge_intervals(INTERVALS_A)


def test_bench_client_quotes_apostrophe_in_path(parquet_dirs, parquet_expected, tmp_path):
    a = shutil.copytree(parquet_dirs[0], tmp_path / "it's" / "a")
    out = tmp_path / "merge.parquet"
    r = run_client(["--op", "merge", "--left", str(a), "--output", str(out)])
    check_result("merge", r, out, parquet_expected["merge"])


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    c = _start_cluster(
        tmp_path_factory.mktemp("plan2_klaster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


@pytest.mark.parametrize("op", OPS)
def test_bench_client_on_multiprocess_cluster(op, cluster, parquet_dirs, parquet_expected, tmp_path):
    a, b = parquet_dirs
    out = tmp_path / f"{op}.parquet"
    env = {**os.environ, "BALLISTA_SCHEDULER_URL": cluster.url}
    check_result(op, run_client(op_args(op, a, b, out), env=env), out, parquet_expected[op])
```

- [ ] **Step 2: Uruchom — ma się nie udać**

Run: `pytest tests/test_ballista_parquet.py -v -k "rejects_bad_arguments"`
Expected: `10 failed`, każdy z komunikatem `brak binarki bench_client`.

- [ ] **Step 3: Wspólne parsowanie flag (`cli.rs`)**

`ballista_genomics/src/cli.rs`:

```rust
//! Ścisłe parsowanie flag wiersza poleceń, wspólne dla binarek `ballista_node`
//! i `bench_client`. Nieznany albo powtórzony argument to błąd: literówka w nazwie
//! flagi nie może cicho uruchomić programu z wartością domyślną, a powtórzenie —
//! z inną wartością niż zamierzona.

use std::collections::HashMap;
use std::str::FromStr;

pub type Flags = HashMap<String, Option<String>>;

/// Parsuje `--klucz wartość` i flagi bez wartości.
pub fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    bool_flags: &[&str],
) -> Result<Flags, String> {
    let mut flags = Flags::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let value = if value_flags.contains(&arg.as_str()) {
            Some(it.next().ok_or_else(|| format!("brak wartości dla {arg}"))?.clone())
        } else if bool_flags.contains(&arg.as_str()) {
            None
        } else {
            return Err(format!("nieznany argument: {arg}"));
        };
        if flags.insert(arg.clone(), value).is_some() {
            return Err(format!("powtórzony argument: {arg}"));
        }
    }
    Ok(flags)
}

/// Wymagana flaga z wartością danego typu.
pub fn required<T: FromStr>(flags: &Flags, name: &str) -> Result<T, String> {
    optional(flags, name)?.ok_or_else(|| format!("brak wymaganej flagi {name}"))
}

/// Opcjonalna flaga z wartością danego typu; brak flagi → `None`.
pub fn optional<T: FromStr>(flags: &Flags, name: &str) -> Result<Option<T>, String> {
    match flags.get(name).and_then(|v| v.as_deref()) {
        None => Ok(None),
        Some(v) => v
            .parse::<T>()
            .map(Some)
            .map_err(|_| format!("niepoprawna wartość flagi {name}")),
    }
}
```

W `ballista_genomics/src/bin/ballista_node.rs`:
- usuń `use std::collections::HashMap;` i `use std::str::FromStr;`;
- usuń `type Flags = …`, funkcję `parse_flags` (razem z jej komentarzem dokumentującym) i funkcję `required`;
- dodaj `use ballista_genomics::cli::{parse_flags, required};` obok pozostałych importów `ballista_genomics::…`.

- [ ] **Step 4: Scenariusz → SQL (`scenario.rs`)**

`ballista_genomics/src/scenario.rs`:

```rust
//! Scenariusz narzędzia pomiarowego → zapytanie SQL z wynikiem w schemacie
//! znormalizowanym (specyfikacja, sekcja 8.4; odpowiednik `bench/ops.py`).
//!
//! Argumenty funkcji `dist_*` odwzorowują SQL-e z `runner::spec()` 1:1 — te
//! same flagi strict/weak i ta sama odwrócona konwencja coverage — bo tamte są
//! zweryfikowane testami względem polars-bio. Różnice: dowolne ścieżki i nazwy
//! kolumn, jawne nazwy wyjściowe, brak ORDER BY (wynik konsumowany
//! strumieniowo, porównania nie zależą od kolejności wierszy).

use crate::dist_payload::DistOp;

/// Jedna operacja na wskazanych danych; `right` tylko dla operacji binarnych.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub op: DistOp,
    pub left: String,
    pub right: Option<String>,
    /// Nazwy kolumn przedziału: kontig, początek, koniec.
    pub cols: [String; 3],
}

/// Literał tekstowy SQL (apostrof podwojony — ścieżki bywają dowolne).
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Identyfikator SQL w cudzysłowie: nazwy kolumn bywają słowami kluczowymi
/// (`end`), a bez cudzysłowu DataFusion zamieniłby wielkie litery na małe.
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn col(source: &str, alias: &str) -> String {
    format!("{} AS {}", ident(source), ident(alias))
}

impl Scenario {
    pub fn validate(&self) -> Result<(), String> {
        match (self.op, self.right.is_some()) {
            (DistOp::Merge, true) => Err("merge działa na jednej tabeli — bez --right".to_string()),
            (DistOp::Merge, false) | (_, true) => Ok(()),
            (op, false) => Err(format!("{} wymaga --right", op.as_str())),
        }
    }

    pub fn sql(&self) -> String {
        let [c, s, e] = &self.cols;
        let cols = format!("{}, {}, {}", lit(c), lit(s), lit(e));
        let left = lit(&self.left);
        let right = lit(self.right.as_deref().unwrap_or_default());
        let side = |prefix: &str, n: u8| {
            [
                col(&format!("{prefix}_{c}"), &format!("chrom_{n}")),
                col(&format!("{prefix}_{s}"), &format!("start_{n}")),
                col(&format!("{prefix}_{e}"), &format!("end_{n}")),
            ]
            .join(", ")
        };
        let own = [col(c, "chrom"), col(s, "start"), col(e, "end")].join(", ");
        match self.op {
            DistOp::Overlap => format!(
                "SELECT {}, {} FROM dist_overlap('df1', {left}, 'df2', {right}, {cols}, 'strict')",
                side("left", 1),
                side("right", 2)
            ),
            // Bez 'strict' — jak runner::spec() i nearest_local.rs.
            DistOp::Nearest => format!(
                "SELECT {}, {}, {} FROM dist_nearest('df1', {left}, 'df2', {right}, 1, true, true, {cols})",
                side("left", 1),
                side("right", 2),
                col("distance", "distance")
            ),
            // Odwrócona konwencja z runner::spec(): 'reads' = prawa, 'targets' = lewa,
            // więc wynik to wiersze lewej tabeli z pokryciem przez prawą — jak
            // pb.coverage(lewa, prawa).
            DistOp::Coverage => format!(
                "SELECT {own}, {} FROM dist_coverage('df2', {right}, 'df1', {left}, {cols}, 'strict')",
                col("coverage", "coverage")
            ),
            DistOp::Merge => format!(
                "SELECT {own}, {} FROM dist_merge('df1', {left}, {cols}, 0, 'strict')",
                col("n_intervals", "n_intervals")
            ),
            DistOp::Subtract => format!(
                "SELECT {own} FROM dist_subtract('df1', {left}, 'df2', {right}, {cols}, 'strict')"
            ),
        }
    }
}
```

- [ ] **Step 5: Połączenie z klastrem wspólne dla klientów (`runner.rs`)**

W `ballista_genomics/src/runner.rs` dodaj pod `fn output_dir()`:

```rust
/// Adres zewnętrznego schedulera z `BALLISTA_SCHEDULER_URL`; pusta wartość = brak.
pub fn scheduler_url() -> Option<String> {
    std::env::var(SCHEDULER_URL_ENV).ok().filter(|url| !url.is_empty())
}

/// Bio-owa sesja klienta Ballisty z zarejestrowanymi funkcjami `dist_*`:
/// zewnętrzny scheduler, gdy podano adres, inaczej standalone in-proc.
/// Nic nie wypisuje na stdout — stdout `bench_client` to protokół.
pub async fn connect_from_env() -> Result<DFSessionContext> {
    // Sesja klienta MUSI być bio-owa (new_with_bio), a konfiguracja mieć oba
    // kodery — patrz cluster.rs. Ten sam stan dostaje scheduler standalone.
    let state = bio_session_state(bio_ballista_config())?;
    let ctx = match scheduler_url() {
        Some(url) => DFSessionContext::remote_with_state(&url, state).await?,
        None => DFSessionContext::standalone_with_state(state).await?,
    };
    for o in DistOp::ALL {
        ctx.register_udtf(o.udtf_name(), Arc::new(DistTableFunction::new(o)));
    }
    Ok(ctx)
}
```

W `pub async fn run(op: DistOp)` zamień fragment od komentarza `// Sesja klienta MUSI być bio-owa` do końca pętli `for o in DistOp::ALL { … }` (włącznie z komentarzem `// Rejestrujemy wszystkie UDTF-y…`) na:

```rust
    match scheduler_url() {
        Some(url) => println!("Łączenie z zewnętrznym schedulerem Ballisty: {url}"),
        None => println!("Łączenie z Ballista standalone (scheduler + executor in-proc)..."),
    }
    let ctx = connect_from_env().await?;
    println!("Klaster Ballista gotowy.\n");
```

Komunikaty na stdout `dist_ops` zostają identyczne.

- [ ] **Step 6: Binarka `bench_client`**

W `ballista_genomics/Cargo.toml`, po linii `prost = "0.14"`, dodaj:

```toml
# bench_client: strumieniowa konsumpcja wyniku (StreamExt); wersja z Cargo.lock.
futures = "0.3"
```

W `ballista_genomics/src/lib.rs` dodaj do mapy modułów (komentarz na górze pliku), w porządku alfabetycznym:

```rust
//! - `cli`             — ścisłe parsowanie flag wspólne dla binarek (plan 2)
//! - `scenario`        — scenariusz narzędzia pomiarowego → SQL w schemacie znormalizowanym (plan 2)
```

Dodaj też deklaracje `pub mod cli;` i `pub mod scenario;` w porządku alfabetycznym.

`ballista_genomics/src/bin/bench_client.rs`:

```rust
//! Runner Ballisty dla narzędzia pomiarowego (specyfikacja, sekcje 8.1 i 8.3).
//!
//! Wykonuje JEDEN scenariusz na wskazanych danych — plik albo katalog plików
//! Parquet (zbiory databio-8p) lub CSV — i wypisuje na stdout JEDNĄ linię JSON.
//! Na tym etapie: liczba wierszy i opcjonalny zapis wyniku do Parquet (testy
//! poprawności). Czas, fazy i suma kontrolna dochodzą w etapie narzędzia
//! pomiarowego.
//!
//! Wynik jest konsumowany strumieniowo (bez zbierania w pamięci klienta),
//! w schemacie znormalizowanym (`scenario.rs`). Tryb klastra jak w `dist_ops`:
//! `BALLISTA_SCHEDULER_URL` → zewnętrzny scheduler, brak → standalone in-proc.
//!
//! Kody wyjścia: 0 — sukces, 1 — błąd wykonania, 2 — błędne argumenty.

use std::fs::File;

use ballista_genomics::cli::{optional, parse_flags, required};
use ballista_genomics::runner::connect_from_env;
use ballista_genomics::scenario::Scenario;
use ballista_genomics::DistOp;
use datafusion::error::Result;
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;

const USAGE: &str = "użycie:
  bench_client --op <overlap|nearest|coverage|merge|subtract> --left <ŚCIEŻKA>
               [--right <ŚCIEŻKA>] [--cols <kontig,start,koniec>] [--output <PLIK.parquet>]
  ŚCIEŻKA: plik albo katalog plików Parquet (lub CSV); --right dla operacji innych niż merge.
  Domyślne --cols: contig,pos_start,pos_end (zbiory databio-8p).";

const DEFAULT_COLS: &str = "contig,pos_start,pos_end";

struct Opts {
    scenario: Scenario,
    output: Option<String>,
}

fn parse(args: &[String]) -> std::result::Result<Opts, String> {
    let flags = parse_flags(args, &["--op", "--left", "--right", "--cols", "--output"], &[])?;
    let op_name: String = required(&flags, "--op")?;
    let op = DistOp::from_cli(&op_name).ok_or_else(|| format!("nieznana operacja: {op_name}"))?;
    let cols_raw: String =
        optional(&flags, "--cols")?.unwrap_or_else(|| DEFAULT_COLS.to_string());
    let cols: [String; 3] = cols_raw
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| format!("--cols: trzy nazwy oddzielone przecinkami, dostałem: {cols_raw}"))?;
    if cols.iter().any(String::is_empty) {
        return Err(format!("--cols: pusta nazwa kolumny w {cols_raw}"));
    }
    let scenario = Scenario {
        op,
        left: required(&flags, "--left")?,
        right: optional(&flags, "--right")?,
        cols,
    };
    scenario.validate()?;
    Ok(Opts {
        scenario,
        output: optional(&flags, "--output")?,
    })
}

async fn run(opts: &Opts) -> Result<u64> {
    let ctx = connect_from_env().await?;
    let mut stream = ctx.sql(&opts.scenario.sql()).await?.execute_stream().await?;
    let mut writer = match &opts.output {
        Some(path) => Some(ArrowWriter::try_new(File::create(path)?, stream.schema(), None)?),
        None => None,
    };
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows() as u64;
        if let Some(w) = writer.as_mut() {
            w.write(&batch)?;
        }
    }
    if let Some(w) = writer {
        w.close()?;
    }
    Ok(rows)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Najpierw WYŁĄCZNIE parsowanie (kod 2), potem działanie (kod 1) — jak w ballista_node.
    let opts = parse(&args).unwrap_or_else(|msg| {
        eprintln!("błąd: {msg}\n{USAGE}");
        std::process::exit(2);
    });
    match run(&opts).await {
        Ok(rows) => println!("{{\"rows\": {rows}}}"),
        Err(e) => {
            eprintln!("bench_client: {e}");
            std::process::exit(1);
        }
    }
}
```

Jeśli kompilator zgłosi brak metody `schema()` na strumieniu, dodaj `use datafusion::physical_plan::RecordBatchStream;`.

- [ ] **Step 7: Zbuduj**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin bench_client --bin ballista_node --bin dist_ops 2>&1 | tail -3`
Expected: `Finished` bez błędów i bez ostrzeżeń w nowych plikach.

- [ ] **Step 8: Uruchom — Parquet i brak ścieżki mają się nie udać**

Run: `pytest tests/test_ballista_parquet.py -v 2>&1 | tail -30`
Expected:
- przechodzą: 10× `test_bench_client_rejects_bad_arguments`, `test_bench_client_reads_csv_with_reserved_column_names`;
- nie przechodzą:
  - 5× `test_bench_client_matches_polars_bio_on_parquet`, 5× `test_bench_client_on_multiprocess_cluster` i `test_bench_client_quotes_apostrophe_in_path` — kod 1, bo katalog Parquet jest rejestrowany jako CSV;
  - `test_missing_data_path_is_reported` — stderr bez ścieżki, bo błąd rejestracji jest dziś połykany.

- [ ] **Step 9: Rejestracja źródła w `dist_provider.rs`**

W `ballista_genomics/src/dist_provider.rs`:

Importy — zamień `use datafusion::error::Result;` na `use datafusion::error::{DataFusionError, Result};`. Zamień `use datafusion::prelude::{CsvReadOptions, SessionContext as DFSessionContext};` na:

```rust
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext as DFSessionContext};
```

Zamień `use crate::dist_payload::DistPayload;` na `use crate::dist_payload::{DistPayload, TableRef};`. Dodaj `use std::path::Path;` do grupy importów `std`.

W komentarzu dokumentującym `build()` zamień „rejestruje pliki CSV i konstruuje właściwy provider vendora” na „rejestruje źródła danych (Parquet albo CSV, patrz `register_source`) i konstruuje właściwy provider vendora”.

Zamień pętlę rejestracji razem z komentarzem nad nią („Rejestracja może się powtórzyć…”):

```rust
        for t in payload.tables() {
            let _ = session
                .register_csv(&t.name, &t.path, CsvReadOptions::new())
                .await;
        }
```

na:

```rust
        for t in payload.tables() {
            register_source(&session, t).await?;
        }
```

Dodaj pod `impl DistBioProvider { … }`:

```rust
/// Rejestruje źródło danych tabeli w sesji odbiorcy. Format rozpoznawany po
/// ścieżce: plik `*.parquet` albo katalog zawierający pliki `*.parquet` →
/// Parquet (zbiory databio-8p), wszystko inne → CSV (dotychczasowe dane
/// testowe). Ścieżka, której nie ma, to błąd z jej nazwą — wcześniej błąd
/// rejestracji był połykany, a zapytanie padało później na mylącym „table not
/// found”. Nazwa już zarejestrowana (ta sama tabela dwa razy w ładunku) jest
/// pomijana.
async fn register_source(session: &DFSessionContext, t: &TableRef) -> Result<()> {
    if session.table_exist(t.name.as_str())? {
        return Ok(());
    }
    let path = Path::new(&t.path);
    if !path.exists() {
        return Err(DataFusionError::Plan(format!(
            "brak danych tabeli {}: {}",
            t.name, t.path
        )));
    }
    if is_parquet_source(path) {
        session
            .register_parquet(&t.name, &t.path, ParquetReadOptions::default())
            .await
    } else {
        session
            .register_csv(&t.name, &t.path, CsvReadOptions::new())
            .await
    }
}

fn is_parquet_source(path: &Path) -> bool {
    let is_parquet = |p: &Path| p.extension().is_some_and(|ext| ext == "parquet");
    if path.is_dir() {
        std::fs::read_dir(path)
            .map(|entries| entries.flatten().any(|entry| is_parquet(&entry.path())))
            .unwrap_or(false)
    } else {
        is_parquet(path)
    }
}
```

- [ ] **Step 10: Zbuduj i uruchom — ma przejść**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin bench_client --bin ballista_node --bin dist_ops 2>&1 | tail -3 && cd .. && pytest tests/test_ballista_parquet.py -v 2>&1 | tail -30`
Expected: `23 passed`.

Jeśli któraś operacja rozjeżdża się z polars-bio, użyj superpowers:systematic-debugging i sprawdź najpierw trzy znane podejrzenia:
- **Chromosom jednostronny (`chrA`, `chrB`).** `describe_diff` pokaże wiersze `chrA`.
- **Przedziały stykające się.** Dotyczy tylko `nearest`, które w `runner::spec()` jest wołane bez `'strict'`.
- **Typy `Utf8View` albo `Int32`.** Objaw to błąd wykonania, nie rozjazd. Możliwa naprawa: `schema_force_view_types = false` w `bio_session_config()`.

Każdą decyzję zapisz w ledgerze jako `Ruling:` i w `OPIS.md` (Zadanie 6).

- [ ] **Step 11: Regresja testów Ballisty**

Run: `pytest tests/test_ballista_overlap.py tests/test_ballista_distributed_ops.py tests/test_ballista_distribution_evidence.py tests/test_ballista_multiprocess.py tests/test_ballista_parquet.py -v 2>&1 | tail -15`
Expected: wszystkie przechodzą, 0 failed. Kolejność plików ma znaczenie, bo test dowodów czyta pliki zapisane przez wcześniejsze przebiegi `dist_ops` (znana zależność z P0).

- [ ] **Step 12: Commit**

```bash
git add ballista_genomics/Cargo.toml ballista_genomics/Cargo.lock ballista_genomics/src/cli.rs ballista_genomics/src/scenario.rs ballista_genomics/src/runner.rs ballista_genomics/src/dist_provider.rs ballista_genomics/src/lib.rs ballista_genomics/src/bin/bench_client.rs ballista_genomics/src/bin/ballista_node.rs tests/test_ballista_parquet.py
git commit -m "Plan 2: Ballista czyta Parquet, runner bench_client ze schematem wynikow 8.4

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Sail — `spark.read.parquet` i uogólnione UDTF-y

**Files:**
- Create: `sail_bio.py` (katalog główny repozytorium, obok `sail_pb_guard.py`), `tests/test_sail_parquet.py`

**Interfaces:**
- Consumes:
  - Task 1: `bench.data.datasets.COLUMNS`.
  - Task 3: `bench.ops.OUTPUT_COLUMNS`, `UNARY_OPS`, `polars_bio_names`, `row_multiset`, `describe_diff`; fikstury `parquet_dirs` i `parquet_expected`.
  - Istniejący `sail_pb_guard.{overlap, nearest, coverage, merge, subtract}`.
- Produces (`sail_bio`):
  - `RETURN_TYPES: dict[str, str]`;
  - `evaluate(op, chrom, rows_a, rows_b=None)` (generator);
  - `make_udtf(op)`;
  - `run_op(spark, op, left, right=None, cols=COLUMNS) -> pandas.DataFrame` (kolumny `OUTPUT_COLUMNS[op]`);
  - `sail_session()` (menedżer kontekstu: serwer Sail i sesja Spark Connect).

- [ ] **Step 1: Napisz testy**

`tests/test_sail_parquet.py`:

```python
"""Plan 2, Zadanie 5: Sail czyta dane Parquet (spark.read.parquet, specyfikacja
9.3) i wykonuje pięć operacji przez uogólnione UDTF-y z `sail_bio.py`; wynik
zgodny z polars-bio na tych samych plikach (schemat znormalizowany, 8.4).

Zbiór testowy zawiera chromosom tylko w A (chrA) — wcześniejsze UDTF-y
(sail_coverage_subtract_udtf.py) pomijały taki chromosom w coverage.

Uruchomienie: pytest tests/test_sail_parquet.py -v (import polars-bio trwa kilka minut)
"""

from __future__ import annotations

import pytest

from bench.ops import OPS, OUTPUT_COLUMNS, UNARY_OPS, describe_diff, row_multiset


@pytest.fixture(scope="module")
def spark():
    import sail_bio

    with sail_bio.sail_session() as session:
        yield session


@pytest.mark.parametrize("op", OPS)
def test_sail_matches_polars_bio_on_parquet(op, spark, parquet_dirs, parquet_expected):
    import sail_bio

    a, b = parquet_dirs
    df = sail_bio.run_op(spark, op, a, None if op in UNARY_OPS else b)
    assert tuple(df.columns) == OUTPUT_COLUMNS[op]
    actual = row_multiset(op, df)
    assert actual == parquet_expected[op], f"{op}: {describe_diff(parquet_expected[op], actual)}"


def test_run_op_twice_in_one_session(spark, parquet_dirs, parquet_expected):
    """Pomiary (plan 3) wołają tę samą operację wiele razy w jednej sesji —
    ponowna rejestracja UDTF-a pod tą samą nazwą nie może zmienić wyniku."""
    import sail_bio

    for _ in range(2):
        df = sail_bio.run_op(spark, "merge", parquet_dirs[0])
        assert row_multiset("merge", df) == parquet_expected["merge"]


@pytest.mark.parametrize("op, with_right", [("overlap", False), ("merge", True)])
def test_run_op_rejects_wrong_number_of_tables(op, with_right, spark, parquet_dirs):
    import sail_bio

    a, b = parquet_dirs
    with pytest.raises(ValueError, match="wymaga"):
        sail_bio.run_op(spark, op, a, b if with_right else None)
```

- [ ] **Step 2: Uruchom — ma się nie udać**

Run: `pytest tests/test_sail_parquet.py -v`
Expected: 8 błędów (ERROR) przy przygotowaniu fikstury `spark`, `ModuleNotFoundError: No module named 'sail_bio'`.

- [ ] **Step 3: Implementacja**

`sail_bio.py`:

```python
"""Operacje polars-bio w Sailu na danych z plików Parquet (plan 2, specyfikacja 9.3).

Wzorzec z sail_*_udtf.py — skalarny UDTF wołany przez LATERAL, dane zgrupowane
po chromosomie przez collect_list, dostęp do polars-bio przez sail_pb_guard —
uogólniony na potrzeby pomiarów:

- dane czytane przez spark.read.parquet, dowolne nazwy kolumn przedziału,
  bez kolumny z nazwą (zbiory databio-8p mają tylko contig, pos_start, pos_end);
- wynik w schemacie znormalizowanym (bench/ops.py, specyfikacja 8.4);
- chromosom obecny tylko w lewej tabeli trafia do polars-bio z PUSTĄ prawą
  stroną, więc semantykę rozstrzyga polars-bio, nie ten moduł: nearest daje
  wiersz bez sąsiada, coverage — pokrycie 0, subtract — przedział bez zmian.
  (sail_coverage_subtract_udtf.py pomija taki chromosom w coverage; dane
  demonstracyjne mają te same chromosomy po obu stronach, więc tego nie widać.)

Moduł musi być importowalny po nazwie (jak sail_pb_guard): cloudpickle
serializuje klasy UDTF, a kod w eval() sięga po funkcje modułu przez import.
Dekorator @udtf trzeba wywołać PO utworzeniu sesji Spark Connect (patrz
sail_overlap_udtf.py, _make_overlap_udtf) — stąd fabryka make_udtf().
"""

from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from bench.data.datasets import COLUMNS
from bench.ops import OUTPUT_COLUMNS, UNARY_OPS, polars_bio_names

#: Nazwy kolumn, pod którymi przedziały trafiają do polars-bio wewnątrz UDTF.
_PB_COLS = ("chrom", "start", "end")

RETURN_TYPES = {
    "overlap": "chrom_1: string, start_1: long, end_1: long, "
               "chrom_2: string, start_2: long, end_2: long",
    "nearest": "chrom_1: string, start_1: long, end_1: long, "
               "chrom_2: string, start_2: long, end_2: long, distance: long",
    "coverage": "chrom: string, start: long, end: long, coverage: long",
    "merge": "chrom: string, start: long, end: long, n_intervals: long",
    "subtract": "chrom: string, start: long, end: long",
}


def _frame(chrom: str, rows) -> pd.DataFrame:
    """Przedziały jednego chromosomu -> pandas dla polars-bio (0-based, półotwarte)."""
    rows = rows or []
    df = pd.DataFrame({
        "chrom": pd.Series([chrom] * len(rows), dtype=object),
        "start": pd.Series([int(r["start"]) for r in rows], dtype="int64"),
        "end": pd.Series([int(r["end"]) for r in rows], dtype="int64"),
    })
    df.attrs["coordinate_system_zero_based"] = True
    return df


def _value(v):
    """Wartość z pandas -> typ Pythona akceptowany przez UDTF (brak -> None)."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    return v if isinstance(v, str) else int(v)


def _rows(op: str, result: pd.DataFrame):
    if result is None or len(result) == 0:
        return
    source = {dst: src for src, dst in polars_bio_names(op, _PB_COLS).items()}
    cols = [source[c] for c in OUTPUT_COLUMNS[op]]
    for rec in result[cols].itertuples(index=False, name=None):
        yield tuple(_value(v) for v in rec)


def evaluate(op: str, chrom: str, rows_a, rows_b=None):
    """Treść eval() UDTF-a: jedna operacja polars-bio na jednym chromosomie."""
    import sail_pb_guard

    if not rows_a:
        return  # każda z operacji zwraca wiersze wynikające z lewej tabeli
    left = _frame(chrom, rows_a)
    if op in UNARY_OPS:
        result = sail_pb_guard.merge(left, cols=list(_PB_COLS), output_type="pandas.DataFrame")
    else:
        result = getattr(sail_pb_guard, op)(
            left,
            _frame(chrom, rows_b),
            cols1=list(_PB_COLS),
            cols2=list(_PB_COLS),
            output_type="pandas.DataFrame",
        )
    yield from _rows(op, result)


def make_udtf(op: str):
    """Klasa UDTF dla operacji — wołać PO utworzeniu sesji Spark Connect."""
    from pyspark.sql.functions import udtf

    if op in UNARY_OPS:

        @udtf(returnType=RETURN_TYPES[op])
        class UnaryUDTF:
            def eval(self, chrom, rows_a):
                import sail_bio

                yield from sail_bio.evaluate(op, chrom, rows_a)

        return UnaryUDTF

    @udtf(returnType=RETURN_TYPES[op])
    class BinaryUDTF:
        def eval(self, chrom, rows_a, rows_b):
            import sail_bio

            yield from sail_bio.evaluate(op, chrom, rows_a, rows_b)

    return BinaryUDTF


def run_op(spark, op: str, left, right=None, cols: tuple[str, str, str] = COLUMNS) -> pd.DataFrame:
    """Operacja na plikach Parquet (plik albo katalog) -> wynik w schemacie znormalizowanym."""
    from pyspark.sql import functions as F

    if op not in RETURN_TYPES:
        raise ValueError(f"nieznana operacja {op!r}")
    if (op in UNARY_OPS) != (right is None):
        raise ValueError(
            f"{op} wymaga {'jednej tabeli' if op in UNARY_OPS else 'dwóch tabel'}"
        )
    c, s, e = cols

    def load(path, source: str):
        return spark.read.parquet(str(path)).select(
            F.col(c).cast("string").alias("chrom"),
            F.col(s).cast("long").alias("start"),
            F.col(e).cast("long").alias("end"),
            F.lit(source).alias("source"),
        )

    interval = F.struct("start", "end")
    name = f"bio_{op}"
    spark.udtf.register(name, make_udtf(op))
    if op in UNARY_OPS:
        grouped = load(left, "a").groupBy("chrom").agg(
            F.collect_list(interval).alias("rows_a")
        )
        call = f"{name}(g.chrom, g.rows_a)"
    else:
        grouped = load(left, "a").union(load(right, "b")).groupBy("chrom").agg(
            F.collect_list(F.when(F.col("source") == "a", interval)).alias("rows_a"),
            F.collect_list(F.when(F.col("source") == "b", interval)).alias("rows_b"),
        )
        call = f"{name}(g.chrom, g.rows_a, g.rows_b)"
    view = f"bio_grouped_{op}"
    grouped.createOrReplaceTempView(view)
    return spark.sql(f"SELECT o.* FROM {view} g, LATERAL {call} o").toPandas()


@contextmanager
def sail_session():
    """Serwer Sail (local) i sesja Spark Connect; zatrzymywane przy wyjściu.

    `.create()`, nie `.getOrCreate()` — PySpark cachuje sesję jako singleton
    procesu i przy kolejnym serwerze zwróciłby martwą sesję (sail_overlap_udtf.py).
    """
    from pyspark.sql import SparkSession
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://{ip}:{port}").create()
    try:
        yield spark
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        server.stop()
```

- [ ] **Step 4: Uruchom — ma przejść**

Run: `pytest tests/test_sail_parquet.py -v 2>&1 | tail -15`
Expected: `8 passed`.

- [ ] **Step 5: Regresja testów Saila**

Run: `pytest tests/test_sail_parallel_udtf.py -v 2>&1 | tail -5`
Expected: `2 passed`.

- [ ] **Step 6: Commit**

```bash
git add sail_bio.py tests/test_sail_parquet.py
git commit -m "Plan 2: Sail czyta Parquet, uogolnione UDTF-y ze schematem wynikow 8.4

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Dane rzeczywiste — pobranie, test akceptacyjny na parze 1-2, dokumentacja

**Files:**
- Create: `tests/test_real_data.py`
- Modify: `tests/conftest.py`, `ballista_genomics/OPIS.md`, `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`

**Interfaces:**
- Consumes:
  - Task 1: `DATASETS`, `data_dir`, `resolve`.
  - Task 2: `expected_rows`, `verify`, `python -m bench.data.download`.
  - Task 3: `OPS`, `UNARY_OPS`, `row_multiset`, `describe_diff`, `tests.generic_oracle`.
  - Task 4: `tests.test_ballista_parquet.run_client`, `op_args`, `check_result`.
  - Task 5: `sail_bio.run_op`, `sail_bio.sail_session`.
  - P0: `_start_cluster`.
- Produces: pobrany zbiór w `$BENCH_DATA_ROOT/databio-8p` (poza repozytorium), marker `dane`, dokumentacja.

- [ ] **Step 1: Checklista RAM — stop i potwierdzenie**

Przypomnij użytkownikowi checklistę z pamięci `feedback_ram_przed_eksperymentem`:
- zamknąć VS Code i aplikacje w tle;
- najlepiej `.wslconfig` z 5 GB;
- to nie jest pomiar, ale Ballista (3 procesy), Sail i polars-bio na prawdziwych danych działają naraz.

Dodaj, że pobranie to ok. 1,2 GB, a dane zajmą ok. 1,6 GB w `~/bench_data`. **Poczekaj na potwierdzenie.**

- [ ] **Step 2: Pobierz dane**

Run: `python -m bench.data.download`
Expected: tabela z 9 zbiorami, każdy ma `8` plików, a liczby wierszy są zgodne z `EXPECTED_ROWS_K` z dokładnością do tysiąca. Na końcu linia `Dane: /home/milosz/bench_data/databio-8p`. Kod wyjścia 0.

Jeśli Google zwróci stronę HTML, komunikat poda adres do pobrania ręcznego. Po zapisaniu archiwum jako `~/bench_data/databio-8p.zip` uruchom komendę ponownie; test `test_ensure_dataset_reuses_downloaded_zip` gwarantuje, że archiwum nie będzie pobierane drugi raz.

- [ ] **Step 3: Zapisz dokładne liczby wierszy w specyfikacji**

W sekcji 4 specyfikacji, po punkcie „Identyfikatory zbiorów”, dodaj punkt z liczbami z tabeli z kroku 2:

```markdown
- **Dokładne liczby wierszy** (po pobraniu, `python -m bench.data.download`): 0 — <liczba>,
  1 — <liczba>, …, 8 — <liczba>. Schemat każdego pliku: `contig` (string), `pos_start`,
  `pos_end` (int32); wiersze nieposortowane, w każdym pliku wszystkie chromosomy. Archiwum
  zawiera też `__MACOSX/` (pliki `._*.parquet`, które nie są Parquetem), sumy `.crc`
  i znaczniki `_SUCCESS` — pomijane przy rozpakowaniu.
```

`<liczba>` to wartości z kolumny `wiersze` wydruku z kroku 2, wpisane z odstępami tysięcy („2 350 968”).

- [ ] **Step 4: Napisz test akceptacyjny**

Dopisz na końcu `tests/conftest.py`:

```python
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "dane: wymaga pobranego zbioru databio-8p (python -m bench.data.download)",
    )
```

`tests/test_real_data.py`:

```python
"""Plan 2, Zadanie 6: test akceptacyjny na prawdziwych danych databio-8p.

Para 1-2 (fBrain-DS14718 × exons; merge: zbiór 1) — najmniejsza para macierzy
lokalnej (specyfikacja, sekcja 7.1) — w trzech wariantach wykonania: Ballista
standalone, Ballista z osobnych procesów (P0) i Sail. Każdy wynik porównywany
z polars-bio na tych samych plikach. Bez pomiarów czasu.

To test akceptacyjny już zaimplementowanych ścieżek (Zadania 4–5), więc ma
przejść od razu; to, że to porównanie potrafi się nie udać, pokazują testy
Zadań 3–5 na tym samym `row_multiset`.

Wymaga: python -m bench.data.download (dane poza repozytorium) oraz binarek
bench_client i ballista_node. Brak danych = pominięcie, brak binarek = błąd.
Uruchomienie: pytest tests/test_real_data.py -v
"""

from __future__ import annotations

import os

import pytest

from bench.data.datasets import DATASETS, data_dir, resolve
from bench.data.download import expected_rows, verify
from bench.ops import OPS, UNARY_OPS, describe_diff, row_multiset
from tests.test_ballista_multiprocess import _start_cluster
from tests.test_ballista_parquet import check_result, op_args, run_client

PAIR = "1-2"
_NEEDED = {DATASETS[1], DATASETS[2]}
_PROBLEMS = verify(data_dir(), {n: r for n, r in expected_rows().items() if n in _NEEDED})

pytestmark = [
    pytest.mark.dane,
    pytest.mark.skipif(
        bool(_PROBLEMS), reason=f"brak kompletnych danych pary {PAIR} w {data_dir()}: {_PROBLEMS}"
    ),
]


@pytest.fixture(scope="module")
def real_dirs():
    return resolve(PAIR)


@pytest.fixture(scope="module")
def real_expected(real_dirs):
    from tests.generic_oracle import read_intervals, reference

    left, right = real_dirs
    a, b = read_intervals(left), read_intervals(right)
    return {
        op: row_multiset(op, reference(op, a, None if op in UNARY_OPS else b)) for op in OPS
    }


@pytest.mark.parametrize("op", OPS)
def test_ballista_standalone_on_1_2(op, real_dirs, real_expected, tmp_path):
    out = tmp_path / f"{op}.parquet"
    r = run_client(op_args(op, *real_dirs, out), timeout=900)
    check_result(op, r, out, real_expected[op])


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    c = _start_cluster(
        tmp_path_factory.mktemp("plan2_dane_klaster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


@pytest.mark.parametrize("op", OPS)
def test_ballista_cluster_on_1_2(op, cluster, real_dirs, real_expected, tmp_path):
    out = tmp_path / f"{op}.parquet"
    env = {**os.environ, "BALLISTA_SCHEDULER_URL": cluster.url}
    r = run_client(op_args(op, *real_dirs, out), env=env, timeout=900)
    check_result(op, r, out, real_expected[op])


@pytest.fixture(scope="module")
def spark():
    import sail_bio

    with sail_bio.sail_session() as session:
        yield session


@pytest.mark.parametrize("op", OPS)
def test_sail_on_1_2(op, spark, real_dirs, real_expected):
    import sail_bio

    left, right = real_dirs
    df = sail_bio.run_op(spark, op, left, None if op in UNARY_OPS else right)
    actual = row_multiset(op, df)
    assert actual == real_expected[op], f"{op}: {describe_diff(real_expected[op], actual)}"
```

- [ ] **Step 5: Uruchom test akceptacyjny**

Run: `pytest tests/test_real_data.py -v 2>&1 | tail -25`
Expected: `15 passed`.

Jeśli `nearest` albo `coverage` w Ballistcie padają na rozmiarze wiadomości gRPC („message length too large” / „decoded message length”), to przypadek z sekcji 9.2 specyfikacji, która dla pary 1-2 zakładała brak problemu. Użyj superpowers:systematic-debugging, zmierz rozmiar ładunku broadcast i zapisz `Ruling:`. Podniesienie limitów to zakres planu 3. Dopuszczalne rozstrzygnięcie to oznaczenie tych dwóch przypadków `pytest.mark.xfail(strict=True, reason=…)` z odwołaniem do 9.2 i adnotacja w `OPIS.md`.

Inne rozjazdy traktuj jak w Zadaniu 4, krok 10.

- [ ] **Step 6: Dokumentacja**

W `ballista_genomics/OPIS.md` dopisz na końcu sekcję:

```markdown
## Plan 2 — dane databio-8p i Parquet (wrzesień 2026)

**Dane.** `python -m bench.data.download` pobiera `databio-8p.zip` z polars-bio-bench
(Google Drive, 1,2 GB) do `$BENCH_DATA_ROOT` (domyślnie `~/bench_data`), rozpakowuje wyłącznie
pliki `databio-8p/<zbiór>/part-*.parquet` i weryfikuje: 8 plików na zbiór, schemat
`contig: string, pos_start: int32, pos_end: int32`, liczby wierszy ze specyfikacji (sekcja 4).
Identyfikatory zbiorów i par: `bench/data/datasets.py` (`resolve("1-2")`).

**Ballista.** `DistBioProvider` rozpoznaje format po ścieżce: plik `*.parquet` albo katalog
z plikami `*.parquet` → Parquet, inaczej CSV. Brak ścieżki to błąd z jej nazwą (wcześniej błąd
rejestracji był połykany). Runner `bench_client` wykonuje jeden scenariusz:

    ./target/debug/bench_client --op overlap \
        --left ~/bench_data/databio-8p/fBrain-DS14718 --right ~/bench_data/databio-8p/exons
    {"rows": <liczba>}

Wynik w schemacie znormalizowanym (specyfikacja 8.4), konsumowany strumieniowo; `--output`
zapisuje go do Parquet. Klaster z osobnych procesów — jak w P0, przez `BALLISTA_SCHEDULER_URL`.

**Sail.** `sail_bio.py`: `spark.read.parquet`, UDTF-y dla danych bez kolumny z nazwą, wynik
w schemacie 8.4.

**Testy.** `tests/test_ballista_parquet.py` i `tests/test_sail_parquet.py` — zbiór testowy
o typach prawdziwych danych, z przypadkami brzegowymi; `tests/test_real_data.py` (marker
`dane`) — para 1-2 w Ballistcie standalone, na klastrze i w Sailu, zgodność z polars-bio.

### Znaleziska

- Archiwum zawiera `__MACOSX/` z plikami `._part-*.parquet` — rozszerzenie `.parquet`, ale
  to nie Parquet; wczytanie ich wzorcem `**/*.parquet` wywróciłoby czytanie.
- Chromosom obecny tylko w lewej tabeli (np. kontigi `SIRV*` w `ex-anno`): polars-bio daje
  w `nearest` wiersz bez sąsiada, w `coverage` pokrycie 0, w `subtract` przedział bez zmian.
  Demonstracyjny UDTF coverage w Sailu (`sail_coverage_subtract_udtf.py`) pomija taki
  chromosom — w `sail_bio.py` polars-bio dostaje pustą prawą stronę.
- DataFusion 53 czyta tekst z Parqueta jako `Utf8View`, a pozycje databio-8p mają typ `int32`.
```

Pod listą znalezisk dopisz po jednym punkcie dla każdej linii `Ruling:` z ledgera tego planu, która zmienia zachowanie albo opisuje własność silnika. Jeśli takich linii nie ma, nic nie dopisuj.

W specyfikacji:
- **Sekcja 8.1.** Po zdaniu „Ballista nie wymaga wrappera pythonowego: …” dodaj: „`bench_client` powstał w planie 2 z częścią protokołu (liczba wierszy, zapis wyniku do Parquet, wynik w schemacie 8.4); czas, fazy i suma kontrolna — plan 3.”
- **Sekcja 9.3.** Pierwszy punkt zamień na:

```markdown
- CSV → Parquet — **zrobione (plan 2)**: `dist_provider.rs` rozpoznaje Parquet po ścieżce
  (plik `*.parquet` albo katalog z takimi plikami); Sail — `sail_bio.py`
  (`spark.read.parquet`, UDTF-y dla danych bez kolumny z nazwą). Pobieranie danych:
  `bench/data/download.py` (zamiast `gdown` — `requests`, bez nowej zależności).
```

- [ ] **Step 7: Pełna regresja**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -2 && cd .. && pytest tests -v 2>&1 | tail -20`
Expected: `0 failed`. Wszystkie testy przechodzą: 57 dotychczasowych, nowe z Zadań 1–5 i 15 testów `dane`.

- [ ] **Step 8: Commit**

```bash
git add tests/test_real_data.py tests/conftest.py ballista_genomics/OPIS.md docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md
git commit -m "Plan 2: test akceptacyjny na parze 1-2 z databio-8p i dokumentacja

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

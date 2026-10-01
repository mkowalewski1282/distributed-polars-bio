# Plan 3a — narzędzie pomiarowe, część 1: plan implementacji

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Zbudować narzędzie, które dla serii opisanej w YAML uruchamia scenariusze w polars-bio, Ballistcie i Sailu na emulowanych węzłach i mierzy:
- czas;
- szczyt pamięci;
- swap;
- wolumen shuffle.

Narzędzie sprawdza każdy wynik sumą kontrolną względem wzorca i zapisuje do Parquet jeden wiersz na przebieg. Działanie potwierdza przebieg smoke na parze 1-2.

**Architecture:** Każdy przebieg to osobny proces runnera, który wypisuje jedną linię JSON (specyfikacja 8.3). Są trzy runnery:
- `bench_client` (Rust) dla Ballisty;
- `polars_bio_runner`;
- `sail_runner` (klient Spark Connect łączący się z serwerem Saila).

Orkiestrator (`bench/orchestrator.py`) prowadzi serię. Dla każdego bloku (wariant × N):
1. startuje silnik przypięty do rdzeni, `bench/engines.py`;
2. wykonuje przebiegi kontrolne, rozgrzewkę i rundy w losowej kolejności z ziarna;
3. mierzy procesy przez `/proc` (`bench/metrics.py`);
4. ocenia ważność przebiegu (`bench/validity.py`);
5. zapisuje wiersze (`bench/results.py`).

Wynik wzorcowy liczy polars-bio na 1 partycji. Suma kontrolna ma tę samą definicję w Pythonie (`bench/checksum.py`) i w Rust (`checksum.rs`).

**Tech Stack:**
- Python 3.10.12: pytest 6.2.5, numpy 1.24, pyarrow 22, polars 1.39.3, polars-bio 0.28.0, PyYAML 6.0.2, pyspark 4.1.1, pysail 0.5.3.
- Rust (edycja 2024): Ballista 53.0.0, DataFusion `=53.0.0` (arrow 58.4), `futures` 0.3, `crc32fast` 1.5.0 (wersja z `Cargo.lock`).

**Spec:** `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`. Plan realizuje:
- sekcje 5 (metryki bez faz i `broadcast_bytes`), 6 (protokół), 7.3 etap 0 (smoke);
- sekcje 8.1–8.6 (narzędzie, bez `analyze.py` i parametru `algorithm`);
- emulację węzłów z sekcji 3.

Stan wyjściowy: plan 2 (`docs/superpowers/plans/2026-09-30-dane-databio-parquet.md`), czyli dane `databio-8p`, ścieżka Parquet, `bench_client` z liczbą wierszy i `sail_bio.py`.

## Global Constraints

- Budowanie Rusta ZAWSZE z `CARGO_BUILD_JOBS=1`. Nie uruchamiać Pythona z polars-bio równolegle z budowaniem.
- Ballista `53.0.0`, DataFusion `=53.0.0`. Bez forkowania i modyfikowania Ballisty, Saila i polars-bio (polars-bio to zewnętrzne repozytorium). Zvendorowana `datafusion-bio-function-ranges` pozostaje bez zmian.
- Nowe zależności Rusta tylko w wersjach z `Cargo.lock` (`crc32fast` 1.5.0, używany już przez flate2 z cechą `default`). Bez nowych zależności Pythona (PyYAML 6.0.2 jest zainstalowany).
- Dane leżą poza repozytorium, w `$BENCH_DATA_ROOT/databio-8p/<zbiór>/part-*.parquet` (domyślnie `~/bench_data`), i nigdy nie trafiają do commitów. Układ współrzędnych 0-based, półotwarty; kolumny `contig`, `pos_start`, `pos_end`.
- Wyniki silników porównujemy w schemacie znormalizowanym (`bench/ops.py`, `OUTPUT_COLUMNS`). Suma kontrolna obejmuje kolumny `KEY_COLUMNS`.
- Emulacja węzłów (specyfikacja 3):
  - rdzeń 0 (CPU 0–1) obsługuje system, scheduler Ballisty, klientów i orkiestrator;
  - węzeł k to CPU {2k, 2k+1};
  - 2 sloty, czyli 2 wątki, na węzeł;
  - przypięcie przez `taskset`.
- Istniejące testy muszą przechodzić. `dist_ops` i skrypty `sail_*_udtf.py` bez zmian. API `sail_bio.run_op` bez zmian.
- Brak binarki w testach Ballisty to **błąd**, nie pominięcie. Brak danych w testach na prawdziwych danych to pominięcie (marker `dane`).
- **Przed Zadaniem 10** (dane rzeczywiste i smoke) przypomnij użytkownikowi checklistę RAM z pamięci `feedback_ram_przed_eksperymentem` i poczekaj na potwierdzenie.
- Ten plan **nie wykonuje serii pomiarowych**: smoke sprawdza narzędzie (Ballista w wersji debug), a jego liczby nie są wynikami pracy.
- Komentarze i dokumentacja po polsku. Komunikaty commitów po polsku, bez znaków diakrytycznych, zakończone linią `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- W repozytorium nic o komunikacji z promotorem.

## Review Focus

Wejścia i warunki, których specyfikacja nie wymienia wprost, a które najłatwiej ugryzą osobę prowadzącą serię (najbardziej prawdopodobne na początku):

1. **Przerwanie serii** (Ctrl-C, padnięcie terminala) w środku bloku. Oczekiwane zachowanie:
   - wiersze ukończonych bloków zostają w `runs.parquet`;
   - przebiegi przerwanego bloku zapisują się z przyczyną;
   - silnik zostaje zatrzymany (żadnych osieroconych executorów na rdzeniach).

   Test: `test_interrupt_keeps_finished_rows_and_stops_engine` (Zadanie 9).
2. **Runner, który zalewa stderr logami albo wisi bez końca.** Orkiestrator nie może się zakleszczyć na potoku, a przebieg kończy się po limicie czasu. Scenariusz jest wtedy pomijany do końca bloku, bo zaczęty job mógłby obciążać silnik. Testy: `test_runner_flooding_stderr_does_not_block`, `test_timeout_kills_runner_and_skips_scenario_for_rest_of_block` (Zadanie 9).
3. **Proces silnika ginie w trakcie bloku** (np. executor zabity przez OOM-killera). Oczekiwane zachowanie:
   - przebieg jest nieważny z nazwą procesu;
   - silnik zostaje uruchomiony od nowa;
   - seria trwa dalej.

   Test: `test_dead_server_process_invalidates_run_and_restarts_engine` (Zadanie 9).
4. **Brak binarek Ballisty dla wybranego profilu** (release jeszcze niezbudowany). Oczekiwany jest błąd przed pierwszym pomiarem (kod 2) z poleceniem budowania, a nie godzina pomiarów zakończona porażką bloku. Test: `test_main_fails_fast_without_ballista_binaries` (Zadanie 9).
5. **Względna albo zaczynająca się od `~` ścieżka danych** (`BENCH_DATA_ROOT`, `--data-root`). Procesy Ballisty działają w `ballista_genomics/`, a runnery Pythona w katalogu repozytorium, więc ścieżki do danych muszą być bezwzględne. Testy: `test_data_dir_expands_home` (Zadanie 6), `test_main_resolves_relative_data_root` (Zadanie 9).

## Miejsce w całości prac

Narzędzie pomiarowe ze specyfikacji jest podzielone na dwa plany.

- **3a (ten plan):**
  - pomiar czasu i pamięci;
  - wykrywanie swapu;
  - wolumen shuffle;
  - suma kontrolna w Pythonie i Rust;
  - runnery wszystkich silników;
  - orkiestrator, konfiguracja YAML i smoke.
- **3b:**
  - Ballista: limity gRPC i `broadcast_bytes` (specyfikacja 9.2), build release i smoke na release;
  - fazy: `EXPLAIN ANALYZE` dla Ballisty oraz instrumentacja UDTF Saila (specyfikacja 9.3);
  - szybsza konwersja wyniku UDTF w Sailu (dziś `itertuples`);
  - parametr `algorithm` (P3);
  - kalibracja i konfiguracje `p1`, `p2`, `p3`;
  - wariant polars-bio na 1 partycji jako punkt odniesienia dla `merge` i `subtract` (decyzja (c), niżej);
  - na samym początku, przed resztą 3b: nowe środowisko z Pythonem ≥ 3.11 i aktualnym polars-bio — przez uv albo globalnie (wariant do wyboru; sekcja „Decyzje”).

`analyze.py` (tabele i wykresy) powstaje po pomiarach.

## Ustalenia z sond (01.10.2026)

Plan opiera się na faktach sprawdzonych sondami na tej maszynie:

1. **Import polars-bio trwał ~270 s przez serwer X.** Polars-bio importuje matplotlib, a ten przy domyślnym backendzie sprawdza ekran z `DISPLAY`. `/etc/bash.bashrc:72` ustawia `DISPLAY` na adres hosta Windows (`…:0`), gdzie nie działa serwer X, więc połączenie TCP czeka na timeout (proces w stanie `SYN-SENT` do portu 6000). Z `MPLBACKEND=Agg` import trwa 1,0 s. Wszystkie wcześniejsze uwagi „import polars-bio trwa kilka minut” wynikały z tego.
2. **polars-bio 0.28 psuje `merge` i `subtract` przy `target_partitions > 1`.** Liczy je wtedy osobno w każdej partycji, więc przedziały chromosomu z różnych plików nie są scalane ani odejmowane. Sonda na zbiorze testowym:
   - wejście jako ścieżki: błędny wynik przy 2 i 4 partycjach;
   - wejście jako ramki: błędny wynik przy 2 partycjach;
   - przy 1 partycji (domyślnej w polars-bio) wynik jest poprawny i zgodny z wyrocznią;
   - `overlap`, `nearest` i `coverage` są poprawne przy każdej liczbie partycji.

   `pb.POLARS_BIO_MAX_THREADS` to właśnie `target_partitions`. To znany błąd upstream ([#372](https://github.com/biodatageeks/polars-bio/issues/372)), naprawiony w 0.29.0. Ta wersja wymaga jednak Pythona ≥ 3.11, a system ma 3.10, stąd w projekcie 0.28.0. Opis: `raporty/polars_bio_blad_partycji.md`.
3. **polars-bio czyta Parquet sam.** Ścieżka musi być wzorcem `katalog/*.parquet`, bo goły katalog daje błąd „No table named”. Bez metadanych trzeba ustawić globalnie `datafusion.bio.coordinate_system_zero_based = true` (polars-bio domyślnie przyjmuje 1-based). Wynik `output_type="datafusion.DataFrame"` daje się czytać strumieniowo przez `execute_stream()`, a partie Arrow są dostępne przez `to_pyarrow()`.
4. **Sail w trybie `local-cluster` jest konfigurowany zmiennymi `SAIL_*`:**
   - `SAIL_MODE`;
   - `SAIL_CLUSTER__WORKER_INITIAL_COUNT`;
   - `SAIL_CLUSTER__WORKER_TASK_SLOTS`;
   - `SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS`;
   - `SAIL_EXECUTION__DEFAULT_PARALLELISM`.

   Co wyszło w sondach:
   - Przy 2 slotach Sail sam uruchamia workery ponad liczbę początkową: N = 1 dawało 2 workery na danych w 8 plikach, a 12 partycji dawało 6 workerów.
   - Z `SAIL_CLUSTER__WORKER_MAX_COUNT` zapytanie wisi.
   - Przy 8 slotach Sail trzyma dokładnie N workerów (sprawdzone dla N = 1 i 3).
   - Domyślnie Sail usuwa workera po 60 s bezczynności.
5. **Sail z serwerem w osobnym procesie działa.** Runner-klient łączy się przez `sc://127.0.0.1:PORT`, a UDTF z `sail_bio` wykonuje się w procesie serwera. Serwer musi więc mieć repozytorium w `sys.path`, czyli działać z katalogiem roboczym = katalog główny repozytorium. Wynik da się czytać strumieniowo jako tabele Arrow przez `spark.client.to_table_as_iterator(df._plan.to_proto(spark.client), df._plan.observations)`. Robi to publiczne `toLocalIterator`, tylko bez zamiany na obiekty `Row`.
6. **Suma kontrolna w wersji wektorowej (numpy) liczy 5 mln wierszy `overlap` w partiach po 8192 w 0,48 s.** Wartości wzorcowe (niżej, `tests/bench/checksum_vectors.py`) policzono z definicji. Wersja wektorowa daje te same wartości, także dla `string_view`, tablic z wielu kawałków i braków wartości.

## Decyzje (stan na 01.10.2026)

- **Punkt odniesienia polars-bio dla `merge` i `subtract` w P1/P2 — podjęta, opcja (c): mierzymy oba warianty.**
  - polars-bio na 1 partycji jako dodatkowy, poprawny punkt odniesienia (DataFusion liczy wtedy na jednym wątku);
  - przebiegi A/B z 2 i 2N partycjami zostają w wynikach jako nieważne i dokumentują błąd.

  Wariant 1-partycyjny wdraża plan 3b. W tym planie wzorzec liczy się na 1 partycji, a nieważność przebiegów A/B wykrywa suma kontrolna.
- **Błąd jest znany upstream, więc go nie zgłaszamy.** Zgłoszenie polars-bio [#372](https://github.com/biodatageeks/polars-bio/issues/372) zamknięto jako naprawione 23.04.2026, a dzień później wyszła wersja 0.29.0. Opis i sprawdzony minimalny przykład: `raporty/polars_bio_blad_partycji.md`.
- **Ustalone 02.10.2026: PRZED planem 3b trzeba zbudować nowe środowisko z Pythonem ≥ 3.11 i aktualnymi bibliotekami** (polars-bio 0.36.0 z 21.09.2026: poprawka #372, DataFusion 53 jak w Ballistcie).
  - Projekt ma polars-bio 0.28.0, bo od 0.29.0 polars-bio wymaga Pythona ≥ 3.11, a system ma 3.10.12. Na maszynie nie ma Pythona 3.11+, `uv` ani condy.
  - **Wariant — do rozważenia z użytkownikiem przed 3b:**
    - **uv**: projektowe `.venv` z Pythonem pobranym przez uv, bez `sudo`. Wersje bibliotek są zapisane w `pyproject.toml` i `uv.lock`, więc środowisko odtwarza się jednym poleceniem (`uv sync`), a systemowy Python 3.10 zostaje nietknięty. Polecenia uruchamia się przez `uv run …` albo po aktywacji `.venv`; w VS Code trzeba wybrać interpreter `.venv/bin/python`. `.venv` dopisać do `.gitignore`.
    - **globalnie**: drugi Python systemowy (np. 3.12 z PPA deadsnakes, wymaga `sudo`) obok 3.10, z bibliotekami w `~/.local` tej wersji. Prostsze w codziennym użyciu, ale bez pliku blokady wersji i z ryzykiem pomylenia interpreterów. `python3` nadal oznacza 3.10, a systemowego domyślnego Pythona nie wolno zmieniać, bo korzystają z niego narzędzia Ubuntu.
    - Wstępna rekomendacja: uv — izolacja, odtwarzalność środowiska (ważna dla pracy) i brak `sudo`.
  - **Zakres prac:**
    - instalacja bibliotek: polars-bio 0.36 z zależnościami (pyarrow 23–24, datafusion 53, polars ≥ 1.37.1), pysail, pyspark, pandas, PyYAML, requests, pytest, numpy (pytest i numpy w wersjach zgodnych z wybranym Pythonem);
    - sprawdzenie zmian API polars-bio 0.28 → 0.36: nazwy kolumn wyniku, opcje `datafusion.bio.*`, `output_type="datafusion.DataFrame"`, wyrocznie w `tests/`;
    - pełny pakiet testów;
    - aktualizacja wersji w specyfikacji i opcji (c) — może okazać się zbędna.
  - Narzędzie z tego planu uruchamia runnery przez `sys.executable`, więc działa w każdym wariancie bez zmian. Jeśli #372 rzeczywiście zniknął, zgłoszą to testy `xfail(strict=True)` w `tests/test_polars_bio_runner.py` i zbiór `KNOWN_POLARS_BIO_BUG` w teście integracyjnym; trzeba je wtedy zaktualizować.
- **Usunięcie linii `DISPLAY` z `/etc/bash.bashrc`.** Plik systemowy, decyzja użytkownika. Narzędzie i testy i tak ustawiają `MPLBACKEND=Agg`.

## Struktura plików

| Plik | Rola |
|---|---|
| `bench/__init__.py` | `MPLBACKEND=Agg` przed importem polars-bio |
| `bench/checksum.py` (nowy) | suma kontrolna: definicja wzorcowa i akumulator wektorowy na partiach Arrow |
| `bench/metrics.py` (nowy) | `/proc`: VmHWM, `clear_refs`, `Cpus_allowed_list`, MemAvailable, pswpout; rozmiar katalogu |
| `ballista_genomics/src/checksum.rs` (nowy) | suma kontrolna w Rust (ta sama definicja) |
| `ballista_genomics/src/runner.rs` | `BIO_TARGET_PARTITIONS` → `target_partitions` wszystkich sesji |
| `ballista_genomics/src/bin/bench_client.rs` | czas, suma kontrolna, szczyt pamięci, `extra.target_partitions`; tryb `--checksum` |
| `ballista_genomics/src/bin/ballista_node.rs`, `lib.rs`, `Cargo.toml` | walidacja zmiennej, moduł `checksum`, `crc32fast` |
| `bench/ops.py` | `normalize_arrow` — partia Arrow z polars-bio → schemat znormalizowany |
| `bench/runners/__init__.py`, `bench/runners/common.py` (nowe) | wspólne argumenty runnerów, linia JSON, kody wyjścia |
| `bench/runners/polars_bio_runner.py` (nowy) | runner polars-bio |
| `bench/procutil.py` (nowy) | `EngineError`, wolne porty, oczekiwanie na port |
| `bench/runners/sail_server.py` (nowy) | serwer Saila na czas bloku, `sail_env(N)` |
| `bench/runners/sail_runner.py` (nowy) | runner-klient Saila |
| `sail_bio.py` | `register`, `build_query` (wydzielone z `run_op`) |
| `bench/config.py`, `bench/conf/smoke.yaml` (nowe) | konfiguracja serii, bloki, kolejność z ziarna, sprawdzenie danych |
| `bench/data/datasets.py` | `expanduser` dla `BENCH_DATA_ROOT` |
| `bench/engines.py` (nowy) | przypięcie do rdzeni, silniki: polars-bio, klaster Ballisty, serwer Saila |
| `bench/validity.py`, `bench/results.py` (nowe) | reguły ważności i dryfu; zapis JSONL → Parquet |
| `bench/orchestrator.py` (nowy) | seria: wzorzec, bloki, pomiary, CLI `python -m bench.orchestrator` |
| `tests/bench/test_*.py`, `tests/bench/checksum_vectors.py`, `tests/bench/fake_runner.py` (nowe) | testy jednostkowe narzędzia |
| `tests/test_bench_client_protocol.py`, `tests/test_polars_bio_runner.py`, `tests/test_sail_runner.py`, `tests/test_orchestrator_integration.py` (nowe) | testy runnerów i orkiestratora na prawdziwych silnikach (zbiór testowy) |
| `tests/conftest.py`, `tests/test_env_isolation.py`, `tests/test_ballista_parquet.py` | `MPLBACKEND`; izolacja `BIO_TARGET_PARTITIONS`; `check_result` w nowym protokole |
| `.gitignore` | surowe pliki serii (`runs.jsonl`, `logs/`, `tmp/`) |
| specyfikacja, `ballista_genomics/OPIS.md`, `wnioski_claude.md` | dokumentacja wyniku planu |

Testy Ballisty są czarnoskrzynkowe w Pythonie, zgodnie z konwencją repozytorium. Zgodność implementacji sumy kontrolnej w Rust z Pythonem sprawdza tryb `bench_client --checksum`, bez osobnego (kosztownego w linkowaniu) harnessu testów Rusta.

---

### Task 1: Szybki import polars-bio i suma kontrolna w Pythonie

**Files:**
- Modify: `bench/__init__.py`
- Modify: `tests/conftest.py` (początek pliku, docstring `parquet_expected`)
- Modify (tylko docstringi): `tests/test_ballista_parquet.py:13`, `tests/test_generic_oracle.py:5`, `tests/test_sail_parquet.py:8`, `tests/test_ballista_multiprocess.py:23`, `tests/test_ballista_multiprocess.py:335-336`
- Create: `bench/checksum.py`
- Create: `tests/bench/checksum_vectors.py`
- Test: `tests/bench/test_env.py`, `tests/bench/test_checksum.py`

**Interfaces:**
- Consumes: `bench.ops.KEY_COLUMNS`, `bench.ops.OUTPUT_COLUMNS`, `bench.ops.OPS`.
- Produces:
  - `bench.checksum`:
    - `MASK: int`, `NULL_CODE: int`, `MULTIPLIERS: tuple[int, ...]`;
    - `mix64(z: int) -> int`, `value_code(column: str, value) -> int`;
    - `row_hash(op: str, row: Sequence) -> int`, `checksum_rows(op: str, rows: Iterable[Sequence]) -> int`, `format_checksum(value: int) -> str`;
    - `class Checksum(op)` z metodą `update(data)` (`pa.Table | pa.RecordBatch | pl.DataFrame`), atrybutem `rows: int`, właściwością `value: int` i metodą `hex() -> str`.
  - `tests.bench.checksum_vectors`: `ROWS: dict[str, list[tuple]]`, `CHECKSUMS: dict[str, str]`, `table(op) -> pa.Table`, `key_rows(op) -> list[tuple]`.

- [ ] **Step 1: Napisz testy środowiska (nieudane)**

`tests/bench/test_env.py`:

```python
"""Środowisko narzędzia (plan 3a, Zadanie 1): import polars-bio nie może czekać na
serwer X ze zmiennej DISPLAY.

Matplotlib (importowany przez polars-bio) przy domyślnym backendzie sprawdza ekran
z DISPLAY. Gdy adres jest nieosiągalny, połączenie TCP czeka na timeout — na tej
maszynie import polars-bio trwał ~270 s zamiast ~1 s."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _python(code: str, env: dict[str, str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, env=env, capture_output=True, text=True,
        timeout=timeout,
    )


def _env_without_backend(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k != "MPLBACKEND"}
    env.update(extra)
    return env


def test_bench_import_selects_headless_matplotlib_backend():
    r = _python("import bench, os; print(os.environ['MPLBACKEND'])", _env_without_backend())
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "Agg"


def test_explicit_matplotlib_backend_wins():
    r = _python("import bench, os; print(os.environ['MPLBACKEND'])", {**os.environ, "MPLBACKEND": "pdf"})
    assert r.stdout.strip() == "pdf"


def test_polars_bio_import_does_not_wait_for_x_server():
    """DISPLAY na adresie z TEST-NET-1 (RFC 5737, nieosiągalny): bez backendu bez okien
    import czeka na timeout TCP i przekracza limit procesu."""
    r = _python("import bench, polars_bio", _env_without_backend(DISPLAY="192.0.2.1:0"))
    assert r.returncode == 0, r.stderr[-2000:]
```

- [ ] **Step 2: Uruchom testy środowiska, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_env.py -v`

Expected:
- `test_bench_import_selects_headless_matplotlib_backend`: FAIL (`KeyError: 'MPLBACKEND'`, kod 1);
- `test_explicit_matplotlib_backend_wins`: PASS (pilnuje, że jawna wartość wygrywa);
- `test_polars_bio_import_does_not_wait_for_x_server`: ERROR `TimeoutExpired` po 60 s.

Jeśli sieć odrzuca połączenie natychmiast, ten trzeci test może przejść już teraz. Wtedy zapisz Ruling: test zostaje jako zabezpieczenie, a dowodem RED jest pierwszy test.

- [ ] **Step 3: Ustaw backend matplotlib w pakiecie i w testach**

`bench/__init__.py` (cały plik):

```python
"""Narzędzie pomiarowe (specyfikacja metodyki, sekcja 8)."""

import os

# Matplotlib (importowany pośrednio przez polars-bio) przy domyślnym backendzie sprawdza
# serwer X ze zmiennej DISPLAY. Na tej maszynie DISPLAY wskazuje host Windows bez serwera X
# (/etc/bash.bashrc), a połączenie TCP czeka na timeout: import polars-bio trwał ~270 s
# zamiast ~1 s. Backend bez okien omija to sprawdzenie; jawnie ustawiony MPLBACKEND wygrywa.
os.environ.setdefault("MPLBACKEND", "Agg")
```

`tests/conftest.py`: dopisz zaraz pod docstringiem modułu, przed `import pytest`:

```python
import os

# Jak w bench/__init__.py: bez tego każdy import polars-bio w testach czeka ~270 s na serwer X
# z DISPLAY. Ustawiane przed importem czegokolwiek, co ładuje polars-bio; procesy potomne
# testów dziedziczą tę zmienną.
os.environ.setdefault("MPLBACKEND", "Agg")
```

W tym samym pliku zmień docstring fikstury `parquet_expected` na:

```python
    """Wynik polars-bio na zbiorze testowym: operacja -> multizbiór wierszy.
    Raz na sesję — wspólny dla testów wszystkich silników."""
```

- [ ] **Step 4: Uruchom testy środowiska, mają przejść**

Run: `python3 -m pytest tests/bench/test_env.py -v`
Expected: 3 passed, w kilka sekund.

- [ ] **Step 5: Napisz wartości wzorcowe i testy sumy kontrolnej (nieudane)**

`tests/bench/checksum_vectors.py`:

```python
"""Wspólne wartości wzorcowe sumy kontrolnej (specyfikacja 8.4): te same wiersze sprawdza
implementacja w Pythonie (bench/checksum.py) i w Rust (bench_client --checksum)."""

from __future__ import annotations

import pyarrow as pa

from bench.ops import KEY_COLUMNS, OUTPUT_COLUMNS

#: Wiersze w schemacie znormalizowanym (kolejność OUTPUT_COLUMNS): duplikat, chromosom bez
#: sąsiada (braki wartości), chr10 obok chr2, wartość 2³¹ − 1, zera.
ROWS: dict[str, list[tuple]] = {
    "overlap": [
        ("chr1", 100, 200, "chr1", 150, 300),
        ("chr1", 100, 200, "chr1", 150, 300),
        ("chr10", 0, 2147483647, "chr10", 5, 6),
    ],
    "nearest": [
        ("chr1", 700, 800, "chr1", 450, 600, 100),
        ("chrA", 10, 20, None, None, None, None),
        ("chr2", 10, 40, "chr2", 100, 220, 60),
    ],
    "coverage": [("chr1", 100, 200, 2), ("chrA", 10, 20, 0)],
    "merge": [("chr1", 100, 300, 4), ("chr2", 200, 350, 1)],
    "subtract": [("chr1", 100, 180), ("chr1", 100, 180), ("chr2", 220, 300)],
}

#: Wartości wyliczone z definicji (bench/checksum.py, 01.10.2026). Zmiana definicji sumy
#: kontrolnej unieważnia porównania z wcześniejszymi seriami — wtedy świadomie zaktualizować
#: te stałe i opisać zmianę w specyfikacji (sekcja 8.4).
CHECKSUMS: dict[str, str] = {
    "overlap": "0x696c86b3d336e487",
    "nearest": "0xf471dd63731fa19a",
    "coverage": "0xc895a77648f51a40",
    "merge": "0x126055154e6b5ce1",
    "subtract": "0x34706f6749df24f2",
}


def _arrow_type(column: str) -> pa.DataType:
    """Typy jak w wynikach silników: chromosom string, początek/koniec int32, reszta int64."""
    if column.startswith("chrom"):
        return pa.string()
    if column.startswith(("start", "end")):
        return pa.int32()
    return pa.int64()


def table(op: str) -> pa.Table:
    rows = ROWS[op]
    return pa.table({
        c: pa.array([r[i] for r in rows], _arrow_type(c)) for i, c in enumerate(OUTPUT_COLUMNS[op])
    })


def key_rows(op: str) -> list[tuple]:
    """Wiersze zawężone do KEY_COLUMNS[op] — wejście definicji wzorcowej."""
    idx = [OUTPUT_COLUMNS[op].index(c) for c in KEY_COLUMNS[op]]
    return [tuple(r[i] for i in idx) for r in ROWS[op]]
```

`tests/bench/test_checksum.py`:

```python
"""Suma kontrolna niezależna od kolejności wierszy (plan 3a, Zadanie 1; specyfikacja 8.4)."""

from __future__ import annotations

import random

import polars as pl
import pyarrow as pa
import pytest

from bench import checksum as cs
from bench.ops import OPS
from tests.bench import checksum_vectors as vec


@pytest.mark.parametrize("op", OPS)
def test_definition_gives_golden_values(op):
    assert cs.format_checksum(cs.checksum_rows(op, vec.key_rows(op))) == vec.CHECKSUMS[op]


@pytest.mark.parametrize("op", OPS)
def test_streaming_accumulator_gives_golden_values(op):
    acc = cs.Checksum(op)
    acc.update(vec.table(op))
    assert (acc.hex(), acc.rows) == (vec.CHECKSUMS[op], len(vec.ROWS[op]))


def test_empty_result_has_zero_checksum():
    acc = cs.Checksum("merge")
    acc.update(vec.table("merge").slice(0, 0))
    assert (acc.hex(), acc.rows) == ("0x0000000000000000", 0)


@pytest.mark.parametrize("op", OPS)
def test_order_and_batching_do_not_matter(op):
    t = vec.table(op)
    reversed_rows = t.take(list(reversed(range(t.num_rows))))
    acc = cs.Checksum(op)
    for batch in reversed_rows.to_batches(max_chunksize=1):
        acc.update(batch)
    assert acc.hex() == vec.CHECKSUMS[op]


def test_detects_values_swapped_between_rows():
    """Suma liniowa zależałaby tylko od sum kolumn — zamiana końców między wierszami
    (te same sumy kolumn) musi zmienić wynik."""
    rows = [("chr1", 100, 200, "chr1", 150, 300), ("chr1", 400, 500, "chr1", 450, 600)]
    swapped = [("chr1", 100, 500, "chr1", 150, 300), ("chr1", 400, 200, "chr1", 450, 600)]
    assert cs.checksum_rows("overlap", rows) != cs.checksum_rows("overlap", swapped)


def test_detects_missing_duplicate():
    rows = vec.key_rows("subtract")
    assert cs.checksum_rows("subtract", rows) != cs.checksum_rows("subtract", rows[1:])


def test_nearest_ignores_neighbour_identity_but_not_distance():
    t = vec.table("nearest")
    other_neighbour = t.set_column(
        t.schema.get_field_index("start_2"), "start_2", pa.array([1, 2, 3], pa.int32())
    )
    longer = t.set_column(
        t.schema.get_field_index("distance"), "distance", pa.array([101, None, 60], pa.int64())
    )
    for variant, same in [(other_neighbour, True), (longer, False)]:
        acc = cs.Checksum("nearest")
        acc.update(variant)
        assert (acc.hex() == vec.CHECKSUMS["nearest"]) is same


def test_missing_value_differs_from_zero():
    assert cs.checksum_rows("nearest", [("chrA", 10, 20, None)]) != cs.checksum_rows(
        "nearest", [("chrA", 10, 20, 0)]
    )


def test_accumulator_matches_definition_on_mixed_types():
    """string_view (tak DataFusion 53 czyta napisy z Parquet), int32/int64, braki wartości
    i tabela z kilku kawałków — wynik jak z definicji liczonej wiersz po wierszu."""
    rng = random.Random(7)
    rows = [
        (rng.choice(["chr1", "chr2", "chrX", "chr10", None]), rng.randrange(2**31),
         rng.randrange(2**31), rng.choice([None, rng.randrange(10**6)]))
        for _ in range(5000)
    ]
    t = pa.table({
        "chrom": pa.array([r[0] for r in rows], pa.string_view()),
        "start": pa.array([r[1] for r in rows], pa.int32()),
        "end": pa.array([r[2] for r in rows], pa.int64()),
        "coverage": pa.array([r[3] for r in rows], pa.int64()),
    })
    acc = cs.Checksum("coverage")
    acc.update(pa.concat_tables([t.slice(0, 1234), t.slice(1234)]))
    assert acc.hex() == cs.format_checksum(cs.checksum_rows("coverage", rows))


def test_accepts_polars_dataframe():
    acc = cs.Checksum("merge")
    acc.update(pl.from_arrow(vec.table("merge")))
    assert acc.hex() == vec.CHECKSUMS["merge"]


def test_missing_key_column_is_reported():
    acc = cs.Checksum("merge")
    with pytest.raises(KeyError, match="n_intervals"):
        acc.update(vec.table("subtract"))


def test_unknown_operation_is_rejected():
    with pytest.raises(ValueError, match="nieznana operacja"):
        cs.Checksum("join")
```

- [ ] **Step 6: Uruchom testy sumy kontrolnej, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_checksum.py -q`
Expected: błąd zbierania testów: `ImportError: cannot import name 'checksum' from 'bench'`.

- [ ] **Step 7: Zaimplementuj sumę kontrolną**

`bench/checksum.py`:

```python
"""Suma kontrolna wyniku operacji niezależna od kolejności wierszy (specyfikacja, sekcja 8.4).

Definicja — wspólna z ballista_genomics/src/checksum.rs (zgodność pilnuje test na
wartościach wzorcowych z tests/bench/checksum_vectors.py):

- kolumny: `KEY_COLUMNS[op]` z bench/ops.py — te same, które porównuje `row_multiset`,
  więc suma jest skrótem tego samego multizbioru (nearest bez tożsamości sąsiada);
- kod wartości: kolumna chromosomu (nazwa zaczyna się od `chrom`) — CRC-32 (IEEE, jak
  `zlib.crc32`) z UTF-8; kolumna liczbowa — wartość całkowita modulo 2⁶⁴ (U2); brak
  wartości — 2⁶⁴ − 1 (jak −1; kolumny wyników są nieujemne);
- skrót wiersza: `mix64(Σ_j M_j · kod_j mod 2⁶⁴)`, M_j — stałe nieparzyste niżej,
  mix64 — finalizator splitmix64 (bijekcja na liczbach 64-bitowych);
- suma kontrolna: Σ skrótów wierszy mod 2⁶⁴, zapisana jako `0x` + 16 cyfr szesnastkowych.

Sama suma liniowa (bez mix64) zależałaby tylko od sum kolumn i nie wykryłaby np. zamiany
końców przedziałów między wierszami — dlatego skrót wiersza jest nieliniowy.

`row_hash`/`checksum_rows` to definicja wzorcowa (czysty Python, wiersz po wierszu);
`Checksum` liczy to samo wektorowo (numpy) na partiach Arrow — dla runnerów, które
konsumują wynik strumieniowo (5 mln wierszy ≈ 0,5 s).
"""

from __future__ import annotations

import zlib
from typing import Iterable, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from bench.ops import KEY_COLUMNS

MASK = (1 << 64) - 1
#: Kod braku wartości (jak −1 w U2).
NULL_CODE = MASK
#: Mnożniki kolumn klucza według pozycji: stałe pierwsze xxHash64 i złoty podział splitmix64.
MULTIPLIERS = (
    0x9E3779B185EBCA87,
    0xC2B2AE3D27D4EB4F,
    0x165667B19E3779F9,
    0x85EBCA77C2B2AE63,
    0x27D4EB2F165667C5,
    0x9E3779B97F4A7C15,
)
_MIX1 = 0xBF58476D1CE4E5B9
_MIX2 = 0x94D049BB133111EB


def mix64(z: int) -> int:
    """Finalizator splitmix64."""
    z = ((z ^ (z >> 30)) * _MIX1) & MASK
    z = ((z ^ (z >> 27)) * _MIX2) & MASK
    return z ^ (z >> 31)


def value_code(column: str, value) -> int:
    if value is None:
        return NULL_CODE
    if column.startswith("chrom"):
        return zlib.crc32(str(value).encode("utf-8"))
    return int(value) & MASK


def row_hash(op: str, row: Sequence) -> int:
    """Skrót jednego wiersza (krotka w kolejności `KEY_COLUMNS[op]`) — definicja wzorcowa."""
    acc = 0
    for j, (column, value) in enumerate(zip(KEY_COLUMNS[op], row, strict=True)):
        acc = (acc + MULTIPLIERS[j] * value_code(column, value)) & MASK
    return mix64(acc)


def checksum_rows(op: str, rows: Iterable[Sequence]) -> int:
    """Suma kontrolna krotek w kolejności `KEY_COLUMNS[op]` (np. `row_multiset(...).elements()`)."""
    total = 0
    for row in rows:
        total = (total + row_hash(op, row)) & MASK
    return total


def format_checksum(value: int) -> str:
    return f"0x{value & MASK:016x}"


def _u64(x: int) -> np.uint64:
    return np.uint64(x)


def _mix64_array(z: np.ndarray) -> np.ndarray:
    z = (z ^ (z >> _u64(30))) * _u64(_MIX1)
    z = (z ^ (z >> _u64(27))) * _u64(_MIX2)
    return z ^ (z >> _u64(31))


def _chrom_codes(column) -> np.ndarray:
    encoded = pc.dictionary_encode(pc.cast(column, pa.string()))
    if isinstance(encoded, pa.ChunkedArray):
        encoded = encoded.combine_chunks() if encoded.num_chunks != 1 else encoded.chunk(0)
    words = encoded.dictionary.to_pylist()
    # Ostatnia pozycja tablicy kodów (indeks len(words)) = brak wartości.
    codes = np.array([zlib.crc32(w.encode("utf-8")) for w in words] + [NULL_CODE], dtype=np.uint64)
    return codes[encoded.indices.fill_null(len(words)).to_numpy(zero_copy_only=False)]


def _int_codes(column) -> np.ndarray:
    values = pc.cast(column, pa.int64()).fill_null(-1).to_numpy(zero_copy_only=False)
    # -1 w U2 to 2⁶⁴ − 1 = NULL_CODE.
    return np.ascontiguousarray(values, dtype=np.int64).view(np.uint64)


class Checksum:
    """Suma kontrolna liczona przyrostowo na kolejnych porcjach wyniku (strumień partii)."""

    def __init__(self, op: str):
        if op not in KEY_COLUMNS:
            raise ValueError(f"nieznana operacja {op!r}")
        self.op = op
        self.rows = 0
        self._sum = np.uint64(0)

    def update(self, data) -> None:
        """Dokłada porcję: pyarrow.Table / RecordBatch albo polars.DataFrame w schemacie
        znormalizowanym (bench/ops.py)."""
        if not isinstance(data, (pa.Table, pa.RecordBatch)):
            data = data.to_arrow()
        names = KEY_COLUMNS[self.op]
        missing = [n for n in names if n not in data.schema.names]
        if missing:
            raise KeyError(f"{self.op}: brak kolumn {missing}; są {data.schema.names}")
        if data.num_rows == 0:
            return
        acc = np.zeros(data.num_rows, dtype=np.uint64)
        with np.errstate(over="ignore"):
            for m, name in zip(MULTIPLIERS, names):
                column = data.column(name)
                codes = _chrom_codes(column) if name.startswith("chrom") else _int_codes(column)
                acc += _u64(m) * codes
            self._sum += _mix64_array(acc).sum(dtype=np.uint64)
        self.rows += data.num_rows

    @property
    def value(self) -> int:
        return int(self._sum)

    def hex(self) -> str:
        return format_checksum(self.value)
```

- [ ] **Step 8: Uruchom testy sumy kontrolnej, mają przejść**

Run: `python3 -m pytest tests/bench/test_checksum.py -v`
Expected: 24 passed.

- [ ] **Step 9: Popraw docstringi o „kilku minutach” importu**

Zastąp dokładne teksty:

| Plik | Było | Ma być |
|---|---|---|
| `tests/test_ballista_parquet.py:13` | `Uruchomienie: pytest tests/test_ballista_parquet.py -v (import polars-bio trwa kilka minut)` | `Uruchomienie: pytest tests/test_ballista_parquet.py -v` |
| `tests/test_generic_oracle.py:5` | `Uruchomienie: pytest tests/test_generic_oracle.py -v (import polars-bio trwa kilka minut)` | `Uruchomienie: pytest tests/test_generic_oracle.py -v` |
| `tests/test_sail_parquet.py:8` | `Uruchomienie: pytest tests/test_sail_parquet.py -v (import polars-bio trwa kilka minut)` | `Uruchomienie: pytest tests/test_sail_parquet.py -v` |
| `tests/test_ballista_multiprocess.py:23` | `Testy operacji importują wyrocznie polars-bio — import trwa kilka minut.` | `Testy operacji importują wyrocznie polars-bio.` |
| `tests/test_ballista_multiprocess.py:335-336` | `tests/test_ballista_distributed_ops.py). Importy leniwe — polars-bio ładuje`<br>`    się kilka minut, a testy samego klastra go nie potrzebują."""` | `tests/test_ballista_distributed_ops.py). Importy leniwe — testy samego klastra`<br>`    nie potrzebują polars-bio."""` |

Run: `grep -rn "kilka minut" tests/ bench/`
Expected: brak wyników.

- [ ] **Step 10: Commit**

```bash
git add bench/__init__.py bench/checksum.py tests/conftest.py tests/bench/test_env.py \
  tests/bench/checksum_vectors.py tests/bench/test_checksum.py tests/test_ballista_parquet.py \
  tests/test_generic_oracle.py tests/test_sail_parquet.py tests/test_ballista_multiprocess.py
git commit -m "$(cat <<'EOF'
Plan 3a: szybki import polars-bio (MPLBACKEND=Agg) i suma kontrolna w Pythonie

Import polars-bio czekal ~270 s na serwer X z DISPLAY (matplotlib); z backendem Agg
trwa ~1 s. Suma kontrolna niezalezna od kolejnosci wierszy (specyfikacja 8.4):
definicja wzorcowa i akumulator wektorowy na partiach Arrow, wartosci wzorcowe.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Metryki z /proc

**Files:**
- Create: `bench/metrics.py`
- Test: `tests/bench/test_metrics.py`

**Interfaces:**
- Produces: `bench.metrics` z funkcjami:
  - `PROC: Path`;
  - `peak_rss(pid: int | str = "self", proc: Path = PROC) -> int` (bajty);
  - `reset_peak_rss(pid: int | str = "self", proc: Path = PROC) -> None`;
  - `cpus_allowed(pid: int | str = "self", proc: Path = PROC) -> str`;
  - `mem_available(proc: Path = PROC) -> int` (bajty);
  - `pswpout(proc: Path = PROC) -> int` (strony);
  - `dir_size(path: Path) -> int` (bajty).

  Błędy: `KeyError` (brak pola), `ValueError` (nieoczekiwana jednostka), `OSError` (brak procesu lub pliku).

- [ ] **Step 1: Napisz testy (nieudane)**

`tests/bench/test_metrics.py`:

```python
"""Metryki z /proc (plan 3a, Zadanie 2; specyfikacja 5 i 6): parsowanie na plikach
wzorcowych oraz działanie na prawdziwym /proc."""

from __future__ import annotations

import os

import pytest

from bench import metrics

STATUS = """Name:\tpython3
Umask:\t0022
State:\tS (sleeping)
VmPeak:\t  812344 kB
VmSize:\t  812344 kB
VmHWM:\t  123456 kB
VmRSS:\t  100000 kB
Threads:\t14
Cpus_allowed:\t00c
Cpus_allowed_list:\t2-3
"""

MEMINFO = """MemTotal:        5000000 kB
MemFree:         3000000 kB
MemAvailable:    3900000 kB
Buffers:           10000 kB
"""

VMSTAT = """nr_free_pages 750000
pswpin 92346
pswpout 206037
pgfault 123
"""


@pytest.fixture
def fake_proc(tmp_path):
    (tmp_path / "4242").mkdir()
    (tmp_path / "4242" / "status").write_text(STATUS)
    (tmp_path / "4242" / "clear_refs").write_text("")
    (tmp_path / "meminfo").write_text(MEMINFO)
    (tmp_path / "vmstat").write_text(VMSTAT)
    return tmp_path


def test_peak_rss_reads_vmhwm_in_bytes(fake_proc):
    assert metrics.peak_rss(4242, fake_proc) == 123456 * 1024


def test_reset_peak_rss_writes_5_to_clear_refs(fake_proc):
    metrics.reset_peak_rss(4242, fake_proc)
    assert (fake_proc / "4242" / "clear_refs").read_text() == "5"


def test_cpus_allowed(fake_proc):
    assert metrics.cpus_allowed(4242, fake_proc) == "2-3"


def test_mem_available_in_bytes(fake_proc):
    assert metrics.mem_available(fake_proc) == 3900000 * 1024


def test_pswpout_not_pswpin(fake_proc):
    assert metrics.pswpout(fake_proc) == 206037


def test_missing_field_is_reported(tmp_path):
    (tmp_path / "1").mkdir()
    (tmp_path / "1" / "status").write_text("Name:\tzombie\nState:\tZ (zombie)\n")
    with pytest.raises(KeyError, match="VmHWM"):
        metrics.peak_rss(1, tmp_path)


def test_unexpected_unit_is_rejected(tmp_path):
    (tmp_path / "1").mkdir()
    (tmp_path / "1" / "status").write_text("VmHWM:\t  12 MB\n")
    with pytest.raises(ValueError, match="jednostce"):
        metrics.peak_rss(1, tmp_path)


def test_missing_pswpout_is_reported(tmp_path):
    (tmp_path / "vmstat").write_text("pswpin 1\n")
    with pytest.raises(KeyError, match="pswpout"):
        metrics.pswpout(tmp_path)


def test_reset_peak_rss_on_real_process():
    """200 MB zapisanych stron podnosi VmHWM; po zwolnieniu i wyzerowaniu licznik
    wraca do bieżącego RSS."""
    blob = b"x" * (200 << 20)
    del blob
    before = metrics.peak_rss()
    metrics.reset_peak_rss()
    assert metrics.peak_rss() < before - (100 << 20)


def test_real_proc_counters_are_readable():
    assert metrics.mem_available() > 0
    assert metrics.pswpout() >= 0
    assert metrics.cpus_allowed() != ""


def test_dir_size_counts_regular_files_recursively(tmp_path):
    (tmp_path / "a.arrow").write_bytes(b"x" * 10)
    (tmp_path / "etap" / "1").mkdir(parents=True)
    (tmp_path / "etap" / "1" / "b.arrow").write_bytes(b"y" * 20)
    big = tmp_path.parent / f"{tmp_path.name}_poza.bin"
    big.write_bytes(b"z" * 1000)
    os.symlink(big, tmp_path / "dowiazanie")
    assert metrics.dir_size(tmp_path) == 30
    assert metrics.dir_size(tmp_path / "nie_ma") == 0
```

- [ ] **Step 2: Uruchom, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_metrics.py -q`
Expected: błąd zbierania `ImportError: cannot import name 'metrics' from 'bench'`.

- [ ] **Step 3: Zaimplementuj metryki**

`bench/metrics.py`:

```python
"""Metryki systemowe z /proc (specyfikacja, sekcje 5 i 6): szczyt pamięci procesu (VmHWM)
i jego zerowanie (clear_refs), przypięcie do rdzeni, dostępna pamięć (MemAvailable),
wypchnięcia do swapu (pswpout) oraz rozmiar katalogu (wolumen shuffle Ballisty).

Każda funkcja przyjmuje katalog `proc` — testy podają pliki wzorcowe."""

from __future__ import annotations

import os
import stat
from pathlib import Path

PROC = Path("/proc")


def _field(path: Path, key: str) -> str:
    for line in path.read_text().splitlines():
        name, sep, value = line.partition(":")
        if sep and name == key:
            return value.strip()
    raise KeyError(f"brak pola {key} w {path}")


def _kib_field(path: Path, key: str) -> int:
    value = _field(path, key)
    number, _, unit = value.partition(" ")
    if unit.strip() != "kB":
        raise ValueError(f"{path}: pole {key} w nieoczekiwanej jednostce: {value!r}")
    return int(number) * 1024


def peak_rss(pid: int | str = "self", proc: Path = PROC) -> int:
    """Szczyt pamięci rezydentnej procesu (VmHWM) w bajtach."""
    return _kib_field(proc / str(pid) / "status", "VmHWM")


def reset_peak_rss(pid: int | str = "self", proc: Path = PROC) -> None:
    """Zeruje licznik szczytu: VmHWM := bieżący RSS (`echo 5 > clear_refs`)."""
    (proc / str(pid) / "clear_refs").write_text("5")


def cpus_allowed(pid: int | str = "self", proc: Path = PROC) -> str:
    """Rdzenie, na których proces może działać (np. '2-3')."""
    return _field(proc / str(pid) / "status", "Cpus_allowed_list")


def mem_available(proc: Path = PROC) -> int:
    return _kib_field(proc / "meminfo", "MemAvailable")


def pswpout(proc: Path = PROC) -> int:
    """Licznik stron wypchniętych do swapu od startu systemu."""
    for line in (proc / "vmstat").read_text().splitlines():
        name, _, value = line.partition(" ")
        if name == "pswpout":
            return int(value)
    raise KeyError(f"brak pswpout w {proc / 'vmstat'}")


def dir_size(path: Path) -> int:
    """Suma rozmiarów plików regularnych w drzewie (bez dowiązań); brak katalogu → 0."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                st = os.lstat(os.path.join(root, name))
            except FileNotFoundError:
                continue  # plik usunięty w trakcie przeglądania
            if stat.S_ISREG(st.st_mode):
                total += st.st_size
    return total
```

- [ ] **Step 4: Uruchom, mają przejść**

Run: `python3 -m pytest tests/bench/test_metrics.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add bench/metrics.py tests/bench/test_metrics.py
git commit -m "$(cat <<'EOF'
Plan 3a: metryki z /proc (VmHWM i clear_refs, MemAvailable, pswpout, rozmiar katalogu)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Ballista — czas, suma kontrolna, szczyt pamięci i liczba partycji w `bench_client`

**Files:**
- Create: `ballista_genomics/src/checksum.rs`
- Modify: `ballista_genomics/src/lib.rs` (mapa modułów, `pub mod checksum;`)
- Modify: `ballista_genomics/Cargo.toml` (`crc32fast`)
- Modify: `ballista_genomics/src/runner.rs:58-85` (`target_partitions`, `bio_session_config`)
- Modify: `ballista_genomics/src/bin/bench_client.rs` (cały plik)
- Modify: `ballista_genomics/src/bin/ballista_node.rs:22,138-141`
- Modify: `tests/conftest.py:9`, `tests/test_env_isolation.py:10` (lista `CLIENT_ENV_VARS`)
- Modify: `tests/test_ballista_parquet.py:51-59` (`check_result`)
- Test: `tests/test_bench_client_protocol.py`

**Interfaces:**
- Consumes:
  - `tests.bench.checksum_vectors` (Zadanie 1);
  - `bench.checksum.checksum_rows`, `format_checksum` (Zadanie 1);
  - `tests.test_ballista_parquet.run_client(args, env=None, timeout=180)`, `BALLISTA_DIR` (plan 2);
  - `tests.test_ballista_multiprocess.NODE_BINARY`, `_require` (P0).
- Produces:
  - Protokół `bench_client` (specyfikacja 8.3), jedna linia na stdout:

    ```json
    {"rows": N, "checksum": "0x…", "t_total_s": T, "phases": {}, "extra": {"target_partitions": P}, "peak_rss_bytes": M}
    ```

  - `bench_client --op OP --checksum PLIK` wypisuje `{"rows": N, "checksum": "0x…"}`.
  - Zmienna `BIO_TARGET_PARTITIONS`:
    - liczba całkowita ≥ 2, domyślnie 4, czytana przez wszystkie sesje (klient, scheduler, executory);
    - niepoprawna wartość daje kod 2 w `bench_client` i `ballista_node`.
  - Rust: `runner::target_partitions() -> Result<usize, String>`, `runner::TARGET_PARTITIONS_ENV`, `checksum::Checksum`, `checksum::key_columns`.

- [ ] **Step 1: Napisz testy protokołu i zaktualizuj `check_result` (nieudane)**

`tests/test_bench_client_protocol.py`:

```python
"""Plan 3a, Zadanie 3: protokół runnera Ballisty (specyfikacja 8.3) — czas, suma kontrolna
zgodna z bench/checksum.py (8.4), szczyt pamięci, liczba partycji z BIO_TARGET_PARTITIONS —
oraz zgodność implementacji sumy w Rust z Pythonem na wspólnych wartościach wzorcowych.

Brak binarki to BŁĄD (jak w P0). Budowanie:
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins
"""

from __future__ import annotations

import json
import os
import subprocess

import pyarrow.parquet as pq
import pytest

from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS
from tests.bench import checksum_vectors as vec
from tests.test_ballista_multiprocess import NODE_BINARY, _require
from tests.test_ballista_parquet import BALLISTA_DIR, run_client

PROTOCOL_KEYS = {"rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes"}


def report(r: subprocess.CompletedProcess) -> dict:
    assert r.returncode == 0, f"kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    return json.loads(lines[0])


def scenario_args(op, a, b) -> list[str]:
    args = ["--op", op, "--left", str(a)]
    return args if op in UNARY_OPS else [*args, "--right", str(b)]


def expected_checksum(op, parquet_expected) -> str:
    return cs.format_checksum(cs.checksum_rows(op, parquet_expected[op].elements()))


@pytest.mark.parametrize("op", OPS)
def test_rust_checksum_matches_golden_values(op, tmp_path):
    path = tmp_path / f"{op}.parquet"
    pq.write_table(vec.table(op), path)
    got = report(run_client(["--op", op, "--checksum", str(path)], timeout=60))
    assert got == {"rows": len(vec.ROWS[op]), "checksum": vec.CHECKSUMS[op]}


@pytest.mark.parametrize(
    "args",
    [
        ["--checksum", "x.parquet"],
        ["--op", "merge", "--checksum", "x.parquet", "--left", "y"],
        ["--op", "merge", "--checksum", "x.parquet", "--output", "y.parquet"],
    ],
)
def test_checksum_mode_rejects_other_arguments(args):
    r = run_client(args, timeout=30)
    assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
    assert "użycie" in r.stderr and r.stdout == ""


@pytest.mark.parametrize("op", OPS)
def test_report_follows_protocol_and_matches_oracle(op, parquet_dirs, parquet_expected):
    got = report(run_client(scenario_args(op, *parquet_dirs)))
    assert set(got) == PROTOCOL_KEYS
    assert got["rows"] == sum(parquet_expected[op].values())
    assert got["checksum"] == expected_checksum(op, parquet_expected)
    assert got["t_total_s"] > 0 and got["phases"] == {}
    assert got["extra"] == {"target_partitions": 4}
    assert got["peak_rss_bytes"] > 10 * 2**20


def test_target_partitions_come_from_environment(parquet_dirs, parquet_expected):
    env = {**os.environ, "BIO_TARGET_PARTITIONS": "3"}
    got = report(run_client(scenario_args("subtract", *parquet_dirs), env=env))
    assert got["extra"] == {"target_partitions": 3}
    assert got["checksum"] == expected_checksum("subtract", parquet_expected)


@pytest.mark.parametrize("value", ["1", "zero", "-4"])
def test_invalid_target_partitions_is_a_usage_error(value, parquet_dirs):
    env = {**os.environ, "BIO_TARGET_PARTITIONS": value}
    r = run_client(["--op", "merge", "--left", str(parquet_dirs[0])], env=env, timeout=30)
    assert r.returncode == 2, r.stderr
    assert "BIO_TARGET_PARTITIONS" in r.stderr and r.stdout == ""


def test_ballista_node_rejects_invalid_target_partitions():
    _require(NODE_BINARY)
    r = subprocess.run(
        [str(NODE_BINARY), "scheduler", "--port", "1"],
        cwd=BALLISTA_DIR,
        env={**os.environ, "BIO_TARGET_PARTITIONS": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 2, r.stderr
    assert "BIO_TARGET_PARTITIONS" in r.stderr
```

W `tests/test_ballista_parquet.py` dopisz import `from bench.checksum import checksum_rows, format_checksum` i zastąp funkcję `check_result`:

```python
def check_result(op: str, r: subprocess.CompletedProcess, out: Path, expected) -> None:
    assert r.returncode == 0, f"bench_client {op}: kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    report = json.loads(lines[0])
    df = pl.read_parquet(out)
    assert report["rows"] == df.height
    # Suma kontrolna z runnera = suma kontrolna wyniku wyroczni (specyfikacja 8.4).
    assert report["checksum"] == format_checksum(checksum_rows(op, expected.elements()))
    assert tuple(df.columns) == OUTPUT_COLUMNS[op]
    actual = row_multiset(op, df)
    assert actual == expected, f"{op}: rozjazd z polars-bio — {describe_diff(expected, actual)}"
```

W `tests/conftest.py` i `tests/test_env_isolation.py` zmień listę na:

```python
CLIENT_ENV_VARS = ("BALLISTA_SCHEDULER_URL", "DIST_OUTPUT_DIR", "BIO_TARGET_PARTITIONS")
```

W `tests/conftest.py` dopisz też do komentarza nad listą: `BIO_TARGET_PARTITIONS — liczba partycji sesji Ballisty; testy zakładają domyślne 4.`

- [ ] **Step 2: Uruchom, mają nie przejść (stara binarka)**

Run: `python3 -m pytest tests/test_bench_client_protocol.py tests/test_ballista_parquet.py -q 2>&1 | tail -15`

Expected: porażki z trzech powodów:
- `--checksum` jest nieznanym argumentem (kod 2);
- w raporcie brakuje pól (`KeyError: 'checksum'` w `check_result`);
- stara binarka ignoruje `BIO_TARGET_PARTITIONS`.

`test_checksum_mode_rejects_other_arguments` przechodzi już teraz: stara binarka odrzuca `--checksum`. To test-strażnik, nie dowód RED. `test_ballista_node_rejects_invalid_target_partitions`: kod ≠ 2 (port 1 wymaga roota → kod 1).

- [ ] **Step 3: Suma kontrolna w Rust**

`ballista_genomics/Cargo.toml`: pod linią `futures = "0.3"` dopisz:

```toml
# bench_client: suma kontrolna (specyfikacja 8.4) — CRC-32 kontigu; wersja z Cargo.lock
# (używana już przez flate2 z tymi samymi cechami, więc bez przebudowy zależności).
crc32fast = "1.5"
```

`ballista_genomics/src/checksum.rs`:

```rust
//! Suma kontrolna wyniku niezależna od kolejności wierszy (specyfikacja, sekcja 8.4) —
//! odpowiednik `bench/checksum.py`, z tą samą definicją:
//!
//! - kolumny: jak `KEY_COLUMNS` w `bench/ops.py` (nearest bez tożsamości sąsiada);
//! - kod wartości: kolumna chromosomu — CRC-32 (IEEE) z UTF-8, liczbowa — wartość
//!   modulo 2⁶⁴ (U2), brak wartości — 2⁶⁴ − 1;
//! - skrót wiersza: `mix64(Σ M_j · kod_j)` w arytmetyce modulo 2⁶⁴ (finalizator splitmix64);
//! - suma kontrolna: suma skrótów modulo 2⁶⁴, `0x` + 16 cyfr szesnastkowych.
//!
//! Zgodność z Pythonem: `tests/test_bench_client_protocol.py` (wartości wzorcowe
//! z `tests/bench/checksum_vectors.py`, liczone przez `bench_client --checksum`).

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use crate::dist_payload::DistOp;

/// Kod braku wartości (jak −1 w U2; kolumny wyników są nieujemne).
pub const NULL_CODE: u64 = u64::MAX;

/// Mnożniki kolumn klucza według pozycji — jak `MULTIPLIERS` w `bench/checksum.py`.
pub const MULTIPLIERS: [u64; 6] = [
    0x9E37_79B1_85EB_CA87,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
    0x85EB_CA77_C2B2_AE63,
    0x27D4_EB2F_1656_67C5,
    0x9E37_79B9_7F4A_7C15,
];

/// Kolumny klucza w schemacie znormalizowanym — jak `KEY_COLUMNS` w `bench/ops.py`.
pub fn key_columns(op: DistOp) -> &'static [&'static str] {
    match op {
        DistOp::Overlap => &["chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"],
        DistOp::Nearest => &["chrom_1", "start_1", "end_1", "distance"],
        DistOp::Coverage => &["chrom", "start", "end", "coverage"],
        DistOp::Merge => &["chrom", "start", "end", "n_intervals"],
        DistOp::Subtract => &["chrom", "start", "end"],
    }
}

/// Finalizator splitmix64 (bijekcja na liczbach 64-bitowych).
pub fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Suma kontrolna liczona przyrostowo na kolejnych partiach wyniku.
pub struct Checksum {
    op: DistOp,
    sum: u64,
    rows: u64,
}

impl Checksum {
    pub fn new(op: DistOp) -> Self {
        Self { op, sum: 0, rows: 0 }
    }

    pub fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut codes = Vec::new();
        for name in key_columns(self.op) {
            let column = batch.column_by_name(name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "suma kontrolna {}: brak kolumny {name} w wyniku",
                    self.op.as_str()
                ))
            })?;
            codes.push(if name.starts_with("chrom") {
                chrom_codes(column.as_ref())?
            } else {
                int_codes(column.as_ref())?
            });
        }
        for row in 0..batch.num_rows() {
            let mut acc = 0u64;
            for (m, column) in MULTIPLIERS.iter().zip(&codes) {
                acc = acc.wrapping_add(m.wrapping_mul(column[row]));
            }
            self.sum = self.sum.wrapping_add(mix64(acc));
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn hex(&self) -> String {
        format!("0x{:016x}", self.sum)
    }
}

fn chrom_codes(column: &dyn Array) -> Result<Vec<u64>> {
    let utf8 = cast(column, &DataType::Utf8)?;
    let strings = utf8.as_string::<i32>();
    Ok((0..strings.len())
        .map(|i| {
            if strings.is_null(i) {
                NULL_CODE
            } else {
                u64::from(crc32fast::hash(strings.value(i).as_bytes()))
            }
        })
        .collect())
}

fn int_codes(column: &dyn Array) -> Result<Vec<u64>> {
    let ints = cast(column, &DataType::Int64)?;
    let values = ints.as_primitive::<Int64Type>();
    Ok((0..values.len())
        .map(|i| if values.is_null(i) { NULL_CODE } else { values.value(i) as u64 })
        .collect())
}
```

`ballista_genomics/src/lib.rs`: w mapie modułów, po linii `bio_phys_codec`, dopisz:

```rust
//! - `checksum`        — suma kontrolna wyniku niezależna od kolejności wierszy (plan 3a)
```

Na liście modułów, po `pub mod bio_phys_codec;`, dopisz `pub mod checksum;`.

- [ ] **Step 4: Liczba partycji z `BIO_TARGET_PARTITIONS` (`runner.rs`)**

W `ballista_genomics/src/runner.rs`, po funkcji `scheduler_url()`, dopisz:

```rust
/// Liczba partycji docelowych (`target_partitions`) dla WSZYSTKICH sesji — klienta,
/// schedulera, executorów i wewnętrznych sesji providera. Orkiestrator pomiarów ustawia
/// ją na 2N (N węzłów po 2 sloty; specyfikacja, sekcja 3) w środowisku każdego procesu
/// klastra i klienta — rozjazd między procesami cicho psułby dystrybucję (patrz
/// `bio_session_config`). Brak albo pusta → 4 (wartość sprzed planu 3a).
pub const TARGET_PARTITIONS_ENV: &str = "BIO_TARGET_PARTITIONS";
pub const DEFAULT_TARGET_PARTITIONS: usize = 4;

/// Wartość z `BIO_TARGET_PARTITIONS`: liczba całkowita ≥ 2 (przy 1 DataFusion nie wstawia
/// hash-repartycji — patrz `bio_session_config`). Binarki sprawdzają ją przy starcie.
pub fn target_partitions() -> std::result::Result<usize, String> {
    match std::env::var(TARGET_PARTITIONS_ENV) {
        Ok(v) if !v.is_empty() => v
            .parse::<usize>()
            .ok()
            .filter(|n| *n >= 2)
            .ok_or_else(|| format!("{TARGET_PARTITIONS_ENV}: liczba całkowita ≥ 2, jest {v:?}")),
        _ => Ok(DEFAULT_TARGET_PARTITIONS),
    }
}
```

W komentarzu nad `bio_session_config()` zastąp akapit zaczynający się od `` /// `with_target_partitions(4)` jest OBOWIĄZKOWE, nie kosmetyczne: `` (aż do `/// przez `EXPLAIN ANALYZE`.`) tekstem:

```rust
/// `target_partitions ≥ 2` jest OBOWIĄZKOWE, nie kosmetyczne: przy
/// `target_partitions == 1` DataFusion w ogóle nie wstawia hash-repartycji
/// (`enforce_distribution.rs`, `add_hash_on_top`: `if n_target == 1 && count == 1
/// { return input }`), więc zapytanie policzyłoby się poprawnie, ale w JEDNYM
/// stage'u — a teza o dystrybucji byłaby pusta. Objawu brak; wykrywalne tylko
/// przez `EXPLAIN ANALYZE`. Wartość: `BIO_TARGET_PARTITIONS` (domyślnie 4).
```

Treść funkcji:

```rust
pub fn bio_session_config() -> SessionConfig {
    // Binarki sprawdzają zmienną przy starcie (błąd użycia, kod 2), więc niepoprawna
    // wartość w tym miejscu to błąd programisty.
    let partitions = target_partitions().unwrap_or_else(|e| panic!("{e}"));
    SessionConfig::from(ConfigOptions::new())
        .with_option_extension(BioConfig::default())
        .with_target_partitions(partitions)
}
```

- [ ] **Step 5: Nowy `bench_client.rs`**

`ballista_genomics/src/bin/bench_client.rs` (cały plik):

```rust
//! Runner Ballisty dla narzędzia pomiarowego (specyfikacja, sekcje 8.1 i 8.3).
//!
//! Wykonuje JEDEN scenariusz na wskazanych danych — plik albo katalog plików
//! Parquet (zbiory databio-8p) lub CSV — i wypisuje na stdout JEDNĄ linię JSON:
//! `{"rows": N, "checksum": "0x…", "t_total_s": T, "phases": {}, "extra":
//! {"target_partitions": P}, "peak_rss_bytes": M}`.
//!
//! - czas: od wysłania zapytania (`ctx.sql`) do skonsumowania ostatniej partii — bez
//!   startu procesu i połączenia z klastrem;
//! - suma kontrolna: `checksum.rs` (specyfikacja 8.4), liczona na bieżąco ze strumienia;
//! - szczyt pamięci: VmHWM tego procesu, licznik zerowany tuż przed zapytaniem;
//! - `extra.target_partitions`: liczba partycji sesji (`BIO_TARGET_PARTITIONS`).
//!
//! Wynik jest konsumowany strumieniowo (bez zbierania w pamięci klienta), w schemacie
//! znormalizowanym (`scenario.rs`). Tryb klastra jak w `dist_ops`:
//! `BALLISTA_SCHEDULER_URL` → zewnętrzny scheduler, brak → standalone in-proc.
//! `--output` dodatkowo zapisuje wynik do Parquet (testy poprawności).
//!
//! `--checksum PLIK --op OP` liczy samą sumę kontrolną pliku Parquet w schemacie
//! znormalizowanym, bez silnika — test zgodności z `bench/checksum.py`.
//!
//! Kody wyjścia: 0 — sukces, 1 — błąd wykonania, 2 — błędne argumenty lub środowisko.

use std::fs::File;
use std::time::Instant;

use ballista_genomics::checksum::Checksum;
use ballista_genomics::cli::{optional, parse_flags, required};
use ballista_genomics::runner::{connect_from_env, target_partitions};
use ballista_genomics::scenario::Scenario;
use ballista_genomics::DistOp;
use datafusion::error::Result;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::StreamExt;

const USAGE: &str = "użycie:
  bench_client --op <overlap|nearest|coverage|merge|subtract> --left <ŚCIEŻKA>
               [--right <ŚCIEŻKA>] [--cols <kontig,start,koniec>] [--output <PLIK.parquet>]
  bench_client --op <OPERACJA> --checksum <PLIK.parquet>
  ŚCIEŻKA: plik albo katalog plików Parquet (lub CSV); --right dla operacji innych niż merge.
  Domyślne --cols: contig,pos_start,pos_end (zbiory databio-8p).
  --checksum: suma kontrolna pliku w schemacie znormalizowanym, bez uruchamiania operacji.
  Środowisko: BALLISTA_SCHEDULER_URL (klaster), BIO_TARGET_PARTITIONS (≥ 2, domyślnie 4).";

const DEFAULT_COLS: &str = "contig,pos_start,pos_end";

enum Mode {
    /// Jeden scenariusz na silniku; opcjonalnie zapis wyniku do Parquet.
    Run { scenario: Scenario, output: Option<String> },
    /// Sama suma kontrolna pliku w schemacie znormalizowanym.
    Checksum { op: DistOp, file: String },
}

fn parse(args: &[String]) -> std::result::Result<Mode, String> {
    let flags = parse_flags(
        args,
        &["--op", "--left", "--right", "--cols", "--output", "--checksum"],
        &[],
    )?;
    let op_name: String = required(&flags, "--op")?;
    let op = DistOp::from_cli(&op_name).ok_or_else(|| format!("nieznana operacja: {op_name}"))?;
    if let Some(file) = optional::<String>(&flags, "--checksum")? {
        if let Some(other) = ["--left", "--right", "--cols", "--output"]
            .into_iter()
            .find(|f| flags.contains_key(*f))
        {
            return Err(format!("--checksum nie łączy się z {other}"));
        }
        return Ok(Mode::Checksum { op, file });
    }
    // Zmienna środowiskowa to też wejście: błąd = kod 2, zanim cokolwiek się uruchomi.
    target_partitions()?;
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
    Ok(Mode::Run {
        scenario,
        output: optional(&flags, "--output")?,
    })
}

/// Logi Ballisty na stderr — tylko gdy ustawiono `RUST_LOG` (diagnostyka; stdout
/// to protokół, a domyślnie stderr ma zawierać wyłącznie komunikat błędu).
fn init_logging() {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .init();
    }
}

/// Zeruje licznik szczytu pamięci tego procesu (VmHWM := bieżący RSS).
fn reset_peak_rss() -> std::io::Result<()> {
    std::fs::write("/proc/self/clear_refs", "5")
}

/// VmHWM tego procesu w bajtach.
fn peak_rss_bytes() -> std::io::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().strip_suffix(" kB"))
        .and_then(|kb| kb.trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .ok_or_else(|| std::io::Error::other("brak VmHWM w /proc/self/status"))
}

struct Report {
    rows: u64,
    checksum: String,
    t_total_s: f64,
    target_partitions: usize,
    peak_rss_bytes: u64,
}

async fn run(scenario: &Scenario, output: Option<&str>) -> Result<Report> {
    let ctx = connect_from_env().await?;
    let target_partitions = ctx.state().config().target_partitions();
    let mut checksum = Checksum::new(scenario.op);
    reset_peak_rss()?;
    let t0 = Instant::now();
    let mut stream = ctx.sql(&scenario.sql()).await?.execute_stream().await?;
    let mut writer = match output {
        Some(path) => Some(ArrowWriter::try_new(File::create(path)?, stream.schema(), None)?),
        None => None,
    };
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        checksum.update(&batch)?;
        if let Some(w) = writer.as_mut() {
            w.write(&batch)?;
        }
    }
    if let Some(w) = writer {
        w.close()?;
    }
    let t_total_s = t0.elapsed().as_secs_f64();
    Ok(Report {
        rows: checksum.rows(),
        checksum: checksum.hex(),
        t_total_s,
        target_partitions,
        peak_rss_bytes: peak_rss_bytes()?,
    })
}

async fn checksum_file(op: DistOp, file: &str) -> Result<Checksum> {
    let ctx = SessionContext::new();
    let mut stream = ctx
        .read_parquet(file, ParquetReadOptions::default())
        .await?
        .execute_stream()
        .await?;
    let mut checksum = Checksum::new(op);
    while let Some(batch) = stream.next().await {
        checksum.update(&batch?)?;
    }
    Ok(checksum)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Najpierw WYŁĄCZNIE parsowanie (kod 2), potem działanie (kod 1) — jak w ballista_node.
    let mode = parse(&args).unwrap_or_else(|msg| {
        eprintln!("błąd: {msg}\n{USAGE}");
        std::process::exit(2);
    });
    init_logging();
    let line = match mode {
        Mode::Checksum { op, file } => checksum_file(op, &file).await.map(|c| {
            format!("{{\"rows\": {}, \"checksum\": \"{}\"}}", c.rows(), c.hex())
        }),
        Mode::Run { scenario, output } => run(&scenario, output.as_deref()).await.map(|r| {
            format!(
                "{{\"rows\": {}, \"checksum\": \"{}\", \"t_total_s\": {:.6}, \"phases\": {{}}, \
                 \"extra\": {{\"target_partitions\": {}}}, \"peak_rss_bytes\": {}}}",
                r.rows, r.checksum, r.t_total_s, r.target_partitions, r.peak_rss_bytes
            )
        }),
    };
    match line {
        Ok(line) => println!("{line}"),
        Err(e) => {
            eprintln!("bench_client: {e}");
            std::process::exit(1);
        }
    }
}
```

- [ ] **Step 6: `ballista_node` sprawdza zmienną przy starcie**

W `ballista_genomics/src/bin/ballista_node.rs` zmień import:

```rust
use ballista_genomics::runner::{bio_session_config, target_partitions};
```

Na początku `main()`, zaraz po `let args: Vec<String> = ...`, wstaw:

```rust
    // Zmienna środowiskowa to też wejście: niepoprawna = błąd użycia (kod 2), zanim
    // węzeł cokolwiek uruchomi (inaczej panika w bio_session_config).
    if let Err(e) = target_partitions() {
        usage_error(&e);
    }
```

- [ ] **Step 7: Zbuduj wszystkie binarki**

Run (w tle, timeout 3600 s; w tym czasie nie uruchamiaj Pythona z polars-bio):
`cd /home/milosz/praca_magisterska/ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -5`

Expected: `Finished` bez błędów i ostrzeżeń w `ballista_genomics`. Przebudowa obejmuje wszystkie binarki używane przez testy, bo zmienił się `runner.rs`.

- [ ] **Step 8: Uruchom testy Ballisty, mają przejść**

Run: `cd /home/milosz/praca_magisterska && python3 -m pytest tests/test_bench_client_protocol.py tests/test_ballista_parquet.py tests/test_ballista_multiprocess.py tests/test_ballista_distributed_ops.py tests/test_env_isolation.py -q 2>&1 | tail -5`
Expected: wszystkie zielone (protokół: 18 testów; parquet: 23; P0 i operacje rozproszone jak przed zmianą).

- [ ] **Step 9: Commit**

```bash
git add ballista_genomics/Cargo.toml ballista_genomics/Cargo.lock ballista_genomics/src/checksum.rs \
  ballista_genomics/src/lib.rs ballista_genomics/src/runner.rs ballista_genomics/src/bin/bench_client.rs \
  ballista_genomics/src/bin/ballista_node.rs tests/test_bench_client_protocol.py \
  tests/test_ballista_parquet.py tests/conftest.py tests/test_env_isolation.py
git commit -m "$(cat <<'EOF'
Plan 3a: bench_client mierzy czas, sume kontrolna i szczyt pamieci; BIO_TARGET_PARTITIONS

Protokol runnera (specyfikacja 8.3): rows, checksum, t_total_s, phases, extra,
peak_rss_bytes. Suma kontrolna w Rust zgodna z bench/checksum.py (tryb --checksum,
wartosci wzorcowe). Liczba partycji wszystkich sesji z BIO_TARGET_PARTITIONS
(domyslnie 4; orkiestrator ustawi 2N), niepoprawna wartosc = kod 2.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Runner polars-bio

**Files:**
- Modify: `bench/ops.py` (dopisać `normalize_arrow`)
- Create: `bench/runners/__init__.py`, `bench/runners/common.py`, `bench/runners/polars_bio_runner.py`
- Test: `tests/bench/test_ops.py` (dopisać testy `normalize_arrow`), `tests/test_polars_bio_runner.py`

**Interfaces:**
- Consumes:
  - `bench.checksum.Checksum` (Zadanie 1);
  - `bench.metrics.peak_rss`, `reset_peak_rss` (Zadanie 2);
  - `bench.ops.polars_bio_names` (plan 2).
- Produces:
  - `bench.ops.normalize_arrow(op, data, cols) -> pa.RecordBatch | pa.Table`.
  - `bench.runners.common`:
    - `scenario_parser(prog, description) -> argparse.ArgumentParser` (flagi `--op`, `--left`, `--right`, `--cols`);
    - `parse_scenario(parser, argv) -> argparse.Namespace` (sprawdza `--right` względem operacji; `cols` jako krotka; ścieżki bezwzględne);
    - `require_paths(*paths)` (`FileNotFoundError` z nazwą ścieżki);
    - `report_line(*, rows, checksum, t_total_s, peak_rss_bytes, phases=None, extra=None) -> str`;
    - `main_guard(prog, body) -> int` (kod 0 z linią JSON albo 1 z komunikatem na stderr).
  - CLI: `python -m bench.runners.polars_bio_runner --op OP --left ŚCIEŻKA [--right ŚCIEŻKA] [--cols c,s,e] --threads T`.
    - Wynik: protokół 8.3 z `extra = {"target_partitions": T}`.
    - Kody wyjścia: 0 / 1 (błąd wykonania) / 2 (argumenty).

- [ ] **Step 1: Napisz testy (nieudane)**

Na końcu `tests/bench/test_ops.py` dopisz (oraz `import pyarrow as pa` na górze pliku):

```python
def test_normalize_arrow_renames_and_orders_batch():
    batch = pa.record_batch({
        "n_intervals": pa.array([2], pa.int64()),
        "pos_end": pa.array([300], pa.int32()),
        "contig": pa.array(["chr1"], pa.string_view()),
        "pos_start": pa.array([100], pa.int32()),
    })
    out = ops.normalize_arrow("merge", batch, DATABIO)
    assert isinstance(out, pa.RecordBatch)
    assert out.schema.names == ["chrom", "start", "end", "n_intervals"]
    assert out.to_pylist() == [{"chrom": "chr1", "start": 100, "end": 300, "n_intervals": 2}]


def test_normalize_arrow_accepts_table_and_reports_missing_columns():
    table = pa.table({"contig": ["chr1"], "pos_start": [1], "pos_end": [2]})
    assert ops.normalize_arrow("subtract", table, DATABIO).column_names == ["chrom", "start", "end"]
    with pytest.raises(KeyError, match="n_intervals"):
        ops.normalize_arrow("merge", table, DATABIO)
```

`tests/test_polars_bio_runner.py`:

```python
"""Plan 3a, Zadanie 4: runner polars-bio (specyfikacja 8.3) — świeży proces, polars-bio czyta
Parquet sam, wynik konsumowany strumieniowo, protokół jak w bench_client.

Przy target_partitions > 1 polars-bio 0.28 liczy merge i subtract osobno w każdej partycji
(znany błąd polars-bio #372, naprawiony w 0.29+, która wymaga Pythona ≥ 3.11) — te przypadki
z 2 partycjami są oznaczone xfail(strict=True): po migracji do nowszego polars-bio test to zgłosi."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS

REPO = Path(__file__).resolve().parent.parent
PROTOCOL_KEYS = {"rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes"}
_PARTITION_BUG = pytest.mark.xfail(
    raises=AssertionError,
    strict=True,
    reason="polars-bio 0.28 (#372): przy target_partitions > 1 merge i subtract liczone osobno w każdej partycji",
)


def run_runner(args: list[str], timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "bench.runners.polars_bio_runner", *args],
        cwd=REPO, capture_output=True, text=True, timeout=timeout,
    )


def scenario_args(op, a, b) -> list[str]:
    args = ["--op", op, "--left", str(a)]
    return args if op in UNARY_OPS else [*args, "--right", str(b)]


def report(r: subprocess.CompletedProcess) -> dict:
    assert r.returncode == 0, f"kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    return json.loads(lines[0])


def expected_checksum(op, parquet_expected) -> str:
    return cs.format_checksum(cs.checksum_rows(op, parquet_expected[op].elements()))


@pytest.mark.parametrize("op", OPS)
def test_single_partition_matches_polars_bio_oracle(op, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--threads", "1"]))
    assert set(got) == PROTOCOL_KEYS
    assert got["rows"] == sum(parquet_expected[op].values())
    assert got["checksum"] == expected_checksum(op, parquet_expected)
    assert got["extra"] == {"target_partitions": 1} and got["phases"] == {}
    assert got["t_total_s"] > 0 and got["peak_rss_bytes"] > 10 * 2**20


@pytest.mark.parametrize(
    "op", [pytest.param(op, marks=_PARTITION_BUG) if op in ("merge", "subtract") else op for op in OPS]
)
def test_two_partitions(op, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--threads", "2"]))
    assert got["extra"] == {"target_partitions": 2}
    assert got["checksum"] == expected_checksum(op, parquet_expected)


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--op", "merge", "--threads", "1"],
        ["--op", "overlap", "--left", "x", "--threads", "1"],
        ["--op", "merge", "--left", "x", "--right", "y", "--threads", "1"],
        ["--op", "merge", "--left", "x", "--threads", "0"],
        ["--op", "merge", "--left", "x"],
        ["--op", "merge", "--left", "x", "--threads", "1", "--cols", "contig,pos_start"],
        ["--op", "join", "--left", "x", "--threads", "1"],
    ],
)
def test_rejects_bad_arguments(args):
    r = run_runner(args, timeout=60)
    assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
    assert r.stdout == ""


def test_missing_data_path_is_reported(tmp_path):
    missing = tmp_path / "nie_ma_takiego_katalogu"
    r = run_runner(["--op", "merge", "--left", str(missing), "--threads", "1"])
    assert r.returncode == 1, r.stderr
    assert str(missing) in r.stderr and r.stdout == ""
```

- [ ] **Step 2: Uruchom, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_ops.py tests/test_polars_bio_runner.py -q 2>&1 | tail -8`

Expected:
- `AttributeError: module 'bench.ops' has no attribute 'normalize_arrow'`;
- testy runnera nie przechodzą, bo moduł nie istnieje (`No module named bench.runners`, kod 1 zamiast 0 lub 2);
- `test_two_partitions[merge]` i `[subtract]` pokazują XFAIL.

- [ ] **Step 3: `normalize_arrow` w `bench/ops.py`**

Pod funkcją `normalize_polars_bio` dopisz:

```python
def normalize_arrow(op: str, data, cols: tuple[str, str, str]):
    """Partia Arrow (RecordBatch albo Table) z wynikiem polars-bio -> kolumny schematu
    znormalizowanego, w tej kolejności — odpowiednik `normalize_polars_bio` dla
    strumienia partii."""
    names = polars_bio_names(op, cols)
    missing = [src for src in names if src not in data.schema.names]
    if missing:
        raise KeyError(f"{op}: wynik polars-bio nie ma kolumn {missing}; ma {data.schema.names}")
    return type(data).from_arrays([data.column(src) for src in names], names=list(names.values()))
```

- [ ] **Step 4: Wspólne elementy runnerów**

`bench/runners/__init__.py`:

```python
"""Runnery narzędzia pomiarowego (specyfikacja 8.1, 8.3): jeden scenariusz w świeżym
procesie, wynik — jedna linia JSON na stdout."""
```

`bench/runners/common.py`:

```python
"""Wspólne elementy runnerów w Pythonie (specyfikacja 8.3): argumenty scenariusza, jedna
linia JSON na stdout i kody wyjścia jak w bench_client — 0 sukces, 1 błąd wykonania
(komunikat na stderr), 2 błędne argumenty (argparse)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

from bench.data.datasets import COLUMNS
from bench.ops import OPS, UNARY_OPS


def scenario_parser(prog: str, description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("--op", required=True, choices=OPS)
    parser.add_argument("--left", required=True, type=Path, help="plik albo katalog plików Parquet")
    parser.add_argument("--right", type=Path, help="druga tabela (operacje inne niż merge)")
    parser.add_argument(
        "--cols", default=",".join(COLUMNS), help="kontig,start,koniec (domyślnie kolumny databio-8p)"
    )
    return parser


def parse_scenario(parser: argparse.ArgumentParser, argv: list[str] | None) -> argparse.Namespace:
    """Parsuje argumenty; niezgodność --right z operacją albo zła lista kolumn → kod 2.
    Ścieżki stają się bezwzględne (runner może działać w innym katalogu niż wywołujący)."""
    args = parser.parse_args(argv)
    if args.op in UNARY_OPS and args.right is not None:
        parser.error(f"{args.op} działa na jednej tabeli — bez --right")
    if args.op not in UNARY_OPS and args.right is None:
        parser.error(f"{args.op} wymaga --right")
    cols = tuple(args.cols.split(","))
    if len(cols) != 3 or not all(cols):
        parser.error(f"--cols: trzy niepuste nazwy oddzielone przecinkami, jest {args.cols!r}")
    args.cols = cols
    args.left = args.left.expanduser().resolve()
    if args.right is not None:
        args.right = args.right.expanduser().resolve()
    return args


def require_paths(*paths: Path | None) -> None:
    """Brak danych → błąd wykonania (kod 1) z nazwą ścieżki, jak w bench_client."""
    for path in paths:
        if path is not None and not path.exists():
            raise FileNotFoundError(f"brak danych: {path}")


def report_line(*, rows: int, checksum: str, t_total_s: float, peak_rss_bytes: int,
                phases: dict | None = None, extra: dict | None = None) -> str:
    return json.dumps({
        "rows": rows,
        "checksum": checksum,
        "t_total_s": t_total_s,
        "phases": phases or {},
        "extra": extra or {},
        "peak_rss_bytes": peak_rss_bytes,
    })


def main_guard(prog: str, body: Callable[[], str]) -> int:
    """Wykonuje runner: linia JSON na stdout (kod 0) albo komunikat na stderr (kod 1)."""
    try:
        line = body()
    except Exception as e:  # noqa: BLE001 — każdy błąd silnika to nieważny przebieg z przyczyną
        print(f"{prog}: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    print(line, flush=True)
    return 0
```

- [ ] **Step 5: Runner polars-bio**

`bench/runners/polars_bio_runner.py`:

```python
"""Runner polars-bio (specyfikacja 8.1, 8.3): jeden scenariusz w świeżym procesie.

polars-bio czyta pliki Parquet sam — ścieżka jako wzorzec `katalog/*.parquet` (gołego
katalogu nie przyjmuje) — a wynik dostaje jako `datafusion.DataFrame` i konsumuje
strumieniowo (`execute_stream`), licząc sumę kontrolną partia po partii.

Czas: od wywołania operacji do ostatniej partii; bez startu procesu, importów i ustawień.
Wątki: `--threads T` → `target_partitions = T` (pb.POLARS_BIO_MAX_THREADS; domyślnie
polars-bio liczy na 1 partycji). Orkiestrator ustawia też POLARS_MAX_THREADS = T
i przypina proces do rdzeni węzłów (taskset).

UWAGA (sonda 01.10.2026): przy target_partitions > 1 polars-bio 0.28 liczy merge
i subtract osobno w każdej partycji — wynik jest błędny, gdy przedziały chromosomu leżą
w różnych plikach (znany błąd polars-bio #372, naprawiony w 0.29+, która wymaga Pythona
≥ 3.11). Wynik wzorcowy serii liczy się dlatego na 1 partycji, a nieważność takich przebiegów
wykrywa suma kontrolna.
"""

from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

from bench.checksum import Checksum
from bench.metrics import peak_rss, reset_peak_rss
from bench.ops import UNARY_OPS, normalize_arrow
from bench.runners.common import main_guard, parse_scenario, report_line, require_paths, scenario_parser

PROG = "polars_bio_runner"


def source(path: Path) -> str:
    """Katalog plików Parquet → wzorzec `katalog/*.parquet`; plik → bez zmian."""
    return str(path / "*.parquet") if path.is_dir() else str(path)


def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], threads: int) -> str:
    require_paths(left, right)
    import polars_bio as pb

    # Ścieżki nie niosą metadanych układu współrzędnych; dane databio-8p są 0-based.
    warnings.filterwarnings("ignore", message="Coordinate system metadata is missing")
    pb.set_option("datafusion.bio.coordinate_system_zero_based", True)
    pb.set_option(pb.POLARS_BIO_MAX_THREADS, threads)
    checksum = Checksum(op)
    reset_peak_rss()
    t0 = time.perf_counter()
    if op in UNARY_OPS:
        df = pb.merge(source(left), cols=list(cols), output_type="datafusion.DataFrame")
    else:
        df = getattr(pb, op)(
            source(left), source(right), cols1=list(cols), cols2=list(cols),
            output_type="datafusion.DataFrame",
        )
    for batch in df.execute_stream():
        checksum.update(normalize_arrow(op, batch.to_pyarrow(), cols))
    t_total_s = time.perf_counter() - t0
    return report_line(
        rows=checksum.rows,
        checksum=checksum.hex(),
        t_total_s=t_total_s,
        peak_rss_bytes=peak_rss(),
        extra={"target_partitions": int(pb.get_option(pb.POLARS_BIO_MAX_THREADS))},
    )


def main(argv: list[str] | None = None) -> int:
    parser = scenario_parser(PROG, "Jeden scenariusz w polars-bio; wynik: jedna linia JSON.")
    parser.add_argument("--threads", type=int, required=True, help="target_partitions polars-bio (≥ 1)")
    args = parse_scenario(parser, argv)
    if args.threads < 1:
        parser.error("--threads: liczba całkowita ≥ 1")
    return main_guard(PROG, lambda: run(args.op, args.left, args.right, args.cols, args.threads))


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Uruchom, mają przejść**

Run: `python3 -m pytest tests/bench/test_ops.py tests/test_polars_bio_runner.py -v 2>&1 | tail -25`
Expected: 33 passed, 2 xfailed. Rozkład:
- `test_ops`: 10 dotychczasowych + 2 nowe;
- runner: 5 + 3 + 8 + 1;
- `test_two_partitions[merge]` i `[subtract]` dają XFAIL na asercji sumy kontrolnej.

- [ ] **Step 7: Commit**

```bash
git add bench/ops.py bench/runners/__init__.py bench/runners/common.py \
  bench/runners/polars_bio_runner.py tests/bench/test_ops.py tests/test_polars_bio_runner.py
git commit -m "$(cat <<'EOF'
Plan 3a: runner polars-bio (strumien partii Arrow, suma kontrolna, target_partitions)

polars-bio czyta Parquet sam (wzorzec katalog/*.parquet), wynik jako
datafusion.DataFrame konsumowany przez execute_stream. Test dokumentuje blad
polars-bio 0.28: merge i subtract przy target_partitions > 1 (xfail strict).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Sail — serwer na czas bloku i runner-klient

**Files:**
- Create: `bench/procutil.py`
- Create: `bench/runners/sail_server.py`, `bench/runners/sail_runner.py`
- Modify: `sail_bio.py` (`udtf_name`, `register`, `build_query`; `run_op` korzysta z nich)
- Test: `tests/bench/test_procutil.py`, `tests/test_sail_runner.py`

**Interfaces:**
- Consumes:
  - `bench.runners.common` (Zadanie 4), `bench.checksum.Checksum`, `bench.metrics`;
  - `sail_bio.make_udtf`, `RETURN_TYPES` (plan 2).
- Produces:
  - `bench.procutil`:
    - `EngineError(RuntimeError)`;
    - `free_port() -> int`;
    - `check_alive(name, proc, log) -> None`;
    - `wait_for_port(port, proc, *, name, log, timeout=60.0) -> None`.
  - `bench.runners.sail_server`: `WORKER_TASK_SLOTS = 8`, `sail_env(n_nodes: int) -> dict[str, str]`, CLI `python -m bench.runners.sail_server --port P`.
  - `bench.runners.sail_runner`: CLI `python -m bench.runners.sail_runner --op … --left … [--right …] [--cols …] --remote sc://127.0.0.1:P`, wynik w protokole 8.3 z `extra = {}`.
  - `sail_bio`:
    - `udtf_name(op) -> str`;
    - `register(spark, op) -> None`;
    - `build_query(spark, op, left, right=None, cols=COLUMNS)` → leniwy DataFrame.

- [ ] **Step 1: Napisz testy (nieudane)**

`tests/bench/test_procutil.py`:

```python
"""Pomocnicze funkcje procesów silników (plan 3a, Zadanie 5)."""

from __future__ import annotations

import socket
import subprocess
import sys

import pytest

from bench.procutil import EngineError, free_port, wait_for_port


def test_free_port_is_bindable():
    port = free_port()
    with socket.socket() as s:
        s.bind(("127.0.0.1", port))


def test_wait_for_port_returns_when_process_listens(tmp_path):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_port(port, proc, name="http", log=tmp_path / "log", timeout=30)
    finally:
        proc.terminate()
        proc.wait()


def test_wait_for_port_reports_dead_process(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "raise SystemExit(3)"])
    proc.wait()
    with pytest.raises(EngineError, match=r"proces serwer_testowy zakończył się \(kod 3\)"):
        wait_for_port(free_port(), proc, name="serwer_testowy", log=tmp_path / "log", timeout=5)


def test_wait_for_port_times_out(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        with pytest.raises(EngineError, match="nie nasłuchuje"):
            wait_for_port(free_port(), proc, name="cichy", log=tmp_path / "log", timeout=1)
    finally:
        proc.kill()
        proc.wait()
```

`tests/test_sail_runner.py`:

```python
"""Plan 3a, Zadanie 5: Sail jako osobny proces serwera (local-cluster, N workerów) i runner-
-klient (specyfikacja 8.3): wynik zgodny z polars-bio, protokół jak w bench_client."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent
PROTOCOL_KEYS = {"rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes"}


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    port = free_port()
    log = tmp_path_factory.mktemp("sail_serwer") / "server.log"
    with log.open("w") as f:
        proc = subprocess.Popen(
            [sys.executable, "-m", "bench.runners.sail_server", "--port", str(port)],
            cwd=REPO, env={**os.environ, **sail_env(2)}, stdout=f, stderr=subprocess.STDOUT,
        )
    try:
        wait_for_port(port, proc, name="sail_server", log=log)
        yield SimpleNamespace(url=f"sc://127.0.0.1:{port}", log=log)
    finally:
        proc.terminate()
        proc.wait(timeout=30)


def run_runner(args: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "bench.runners.sail_runner", *args],
        cwd=REPO, capture_output=True, text=True, timeout=timeout,
    )


def scenario_args(op, a, b) -> list[str]:
    args = ["--op", op, "--left", str(a)]
    return args if op in UNARY_OPS else [*args, "--right", str(b)]


def report(r: subprocess.CompletedProcess) -> dict:
    assert r.returncode == 0, f"kod {r.returncode}\nstderr:\n{r.stderr[-3000:]}"
    lines = r.stdout.strip().splitlines()
    assert len(lines) == 1, f"stdout ma być jedną linią JSON, jest:\n{r.stdout}"
    return json.loads(lines[0])


def test_sail_env_for_three_nodes():
    assert sail_env(3) == {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "3",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "8",
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "86400",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "6",
    }


@pytest.mark.parametrize("op", OPS)
def test_runner_matches_polars_bio(op, server, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--remote", server.url]))
    assert set(got) == PROTOCOL_KEYS
    expected = parquet_expected[op]
    assert got["rows"] == sum(expected.values())
    assert got["checksum"] == cs.format_checksum(cs.checksum_rows(op, expected.elements()))
    assert got["t_total_s"] > 0 and got["phases"] == {} and got["extra"] == {}
    assert got["peak_rss_bytes"] > 10 * 2**20


def test_server_keeps_exactly_n_workers(server, parquet_dirs):
    """Przy 2 slotach Sail dokładałby workery ponad N (sonda 01.10.2026); tu N = 2."""
    report(run_runner([*scenario_args("overlap", *parquet_dirs), "--remote", server.url]))
    workers = set(re.findall(r"worker (\d+) server is ready", server.log.read_text()))
    assert workers == {"1", "2"}


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--op", "merge", "--left", "x"],
        ["--op", "overlap", "--left", "x", "--remote", "sc://127.0.0.1:1"],
        ["--op", "merge", "--left", "x", "--right", "y", "--remote", "sc://127.0.0.1:1"],
    ],
)
def test_runner_rejects_bad_arguments(args):
    r = run_runner(args, timeout=60)
    assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
    assert r.stdout == ""


def test_missing_data_path_is_reported(server, tmp_path):
    missing = tmp_path / "nie_ma_takiego_katalogu"
    r = run_runner(["--op", "merge", "--left", str(missing), "--remote", server.url])
    assert r.returncode == 1, r.stderr
    assert str(missing) in r.stderr and r.stdout == ""
```

- [ ] **Step 2: Uruchom, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_procutil.py tests/test_sail_runner.py -q 2>&1 | tail -6`
Expected: błędy zbierania, `ModuleNotFoundError: No module named 'bench.procutil'`.

- [ ] **Step 3: `bench/procutil.py`**

```python
"""Procesy silników: wolne porty, sprawdzanie, czy proces żyje, i oczekiwanie na port."""

from __future__ import annotations

import socket
import subprocess
import time
from pathlib import Path


class EngineError(RuntimeError):
    """Silnik nie wystartował albo padł — komunikat wskazuje proces i jego log."""


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def check_alive(name: str, proc: subprocess.Popen, log: Path) -> None:
    if proc.poll() is not None:
        raise EngineError(f"proces {name} zakończył się (kod {proc.returncode}); log: {log}")


def wait_for_port(
    port: int, proc: subprocess.Popen, *, name: str, log: Path, timeout: float = 60.0
) -> None:
    """Czeka, aż proces zacznie przyjmować połączenia TCP na porcie (albo padnie)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        check_alive(name, proc, log)
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise EngineError(f"{name} nie nasłuchuje na porcie {port} po {timeout:.0f} s; log: {log}")
```

- [ ] **Step 4: Wydziel `register` i `build_query` w `sail_bio.py`**

Funkcję `run_op` w `sail_bio.py` zastąp trzema funkcjami i nową wersją `run_op`. Treść budowy zapytania pozostaje bez zmian:

```python
def udtf_name(op: str) -> str:
    return f"bio_{op}"


def register(spark, op: str) -> None:
    """Rejestruje UDTF operacji w sesji — przed `build_query`. Runner pomiarowy robi to przed
    pomiarem czasu, tak jak bench_client rejestruje funkcje dist_* przy połączeniu."""
    if op not in RETURN_TYPES:
        raise ValueError(f"nieznana operacja {op!r}")
    spark.udtf.register(udtf_name(op), make_udtf(op))


def build_query(spark, op: str, left, right=None, cols: tuple[str, str, str] = COLUMNS):
    """Zapytanie (leniwy DataFrame) na plikach Parquet (plik albo katalog) z wynikiem
    w schemacie znormalizowanym; UDTF operacji musi być zarejestrowany (`register`)."""
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
    name = udtf_name(op)
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
    # explode POZA złączeniem LATERAL: rozwijana tablica nie niesie już listy przedziałów grupy.
    return spark.sql(
        f"SELECT r.* FROM (SELECT explode(o.res) AS r FROM {view} g, LATERAL {call} o)"
    )


def run_op(spark, op: str, left, right=None, cols: tuple[str, str, str] = COLUMNS) -> pd.DataFrame:
    """Operacja na plikach Parquet (plik albo katalog) -> wynik w schemacie znormalizowanym."""
    register(spark, op)
    return build_query(spark, op, left, right, cols).toPandas()
```

- [ ] **Step 5: Serwer i runner Saila**

`bench/runners/sail_server.py`:

```python
"""Serwer Sail dla serii pomiarowej (specyfikacja 3 i 6.2): osobny proces na czas bloku;
runnery-klienci (bench/runners/sail_runner.py) łączą się przez Spark Connect.

Zasoby ustawia orkiestrator: przypięcie do rdzeni węzłów 1..N (taskset) i zmienne SAIL_*
z `sail_env`. polars-bio jest importowany przed startem, żeby pierwsze wywołanie UDTF nie
płaciło za import. Proces kończy się sygnałem SIGTERM. Katalog roboczy musi być katalogiem
głównym repozytorium — UDTF z sail_bio importuje moduły po nazwie w tym procesie."""

from __future__ import annotations

import argparse
import sys
import time

#: Sloty zadań na workera. Przy 2 slotach (jak w Ballistcie) Sail w trybie local-cluster sam
#: uruchamia DODATKOWE workery, gdy etap ma więcej zadań niż slotów (2 workery przy N = 1 na
#: danych w 8 plikach), a z limitem cluster.worker_max_count zapytanie wisi (sonda 01.10.2026).
#: 8 slotów (wartość domyślna Saila) utrzymuje dokładnie N workerów; o równoległości i tak
#: decyduje przypięcie procesu do 2N rdzeni.
WORKER_TASK_SLOTS = 8


def sail_env(n_nodes: int) -> dict[str, str]:
    """Zmienne SAIL_* serwera dla N węzłów: local-cluster, N workerów bez wygaszania
    bezczynnych (start workera nie trafia do pomiarów), równoległość 2N."""
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": str(n_nodes),
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": str(WORKER_TASK_SLOTS),
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "86400",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(2 * n_nodes),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sail_server", description="Serwer Spark Connect (Sail) na 127.0.0.1."
    )
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args(argv)
    import polars_bio  # noqa: F401 — przed startem; MPLBACKEND ustawia bench/__init__.py
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer("127.0.0.1", args.port)
    server.start(background=True)
    print(f"sail_server: nasłuchuje na 127.0.0.1:{args.port}", flush=True)
    while server.running:
        time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

`bench/runners/sail_runner.py`:

```python
"""Runner Saila (specyfikacja 8.1, 8.3): klient Spark Connect łączący się z serwerem bloku
(bench/runners/sail_server.py); jeden scenariusz w świeżym procesie.

Czas: od budowy zapytania (wczytanie Parquet, grupowanie, widok, SQL) do ostatniej partii
wyniku; bez startu sesji i rejestracji UDTF (odpowiednik rejestracji funkcji dist_*
w bench_client). Wynik jest konsumowany strumieniowo jako tabele Arrow przez
`client.to_table_as_iterator` — to samo, czego używa publiczne toLocalIterator, ale bez
zamiany na obiekty Row — a suma kontrolna liczona na bieżąco. Szczyt pamięci: proces
klienta; szczyt serwera mierzy orkiestrator."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from bench.checksum import Checksum
from bench.metrics import peak_rss, reset_peak_rss
from bench.runners.common import main_guard, parse_scenario, report_line, require_paths, scenario_parser

PROG = "sail_runner"


def iter_arrow(spark, df):
    """Wynik zapytania jako kolejne tabele Arrow (po jednej na partię z serwera)."""
    import pyarrow as pa

    client = spark.client
    for item in client.to_table_as_iterator(df._plan.to_proto(client), df._plan.observations):
        if isinstance(item, pa.Table):
            yield item


def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], remote: str) -> str:
    require_paths(left, right)
    from pyspark.sql import SparkSession

    import sail_bio

    spark = SparkSession.builder.remote(remote).create()
    try:
        sail_bio.register(spark, op)
        checksum = Checksum(op)
        reset_peak_rss()
        t0 = time.perf_counter()
        df = sail_bio.build_query(spark, op, left, right, cols)
        for table in iter_arrow(spark, df):
            checksum.update(table)
        t_total_s = time.perf_counter() - t0
    finally:
        spark.stop()
    return report_line(
        rows=checksum.rows, checksum=checksum.hex(), t_total_s=t_total_s, peak_rss_bytes=peak_rss()
    )


def main(argv: list[str] | None = None) -> int:
    parser = scenario_parser(PROG, "Jeden scenariusz w Sailu (serwer zewnętrzny); wynik: jedna linia JSON.")
    parser.add_argument("--remote", required=True, help="adres serwera, np. sc://127.0.0.1:50051")
    args = parse_scenario(parser, argv)
    return main_guard(PROG, lambda: run(args.op, args.left, args.right, args.cols, args.remote))


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Uruchom nowe i dotychczasowe testy Saila, mają przejść**

Run: `python3 -m pytest tests/bench/test_procutil.py tests/test_sail_runner.py tests/test_sail_parquet.py -v 2>&1 | tail -25`

Expected: 31 passed:
- `procutil`: 4;
- runner: 1 + 5 + 1 + 4 + 1 = 12;
- `test_sail_parquet`: 10, bez zmian API `run_op`.

- [ ] **Step 7: Commit**

```bash
git add bench/procutil.py bench/runners/sail_server.py bench/runners/sail_runner.py sail_bio.py \
  tests/bench/test_procutil.py tests/test_sail_runner.py
git commit -m "$(cat <<'EOF'
Plan 3a: Sail jako serwer bloku (local-cluster, N workerow) i runner-klient

Serwer w osobnym procesie ze zmiennymi SAIL_* (8 slotow: przy 2 Sail dokladal
workery ponad N, z worker_max_count zapytanie wisialo). Klient czyta wynik
strumieniowo jako tabele Arrow (to_table_as_iterator). sail_bio: register i
build_query wydzielone z run_op (API run_op bez zmian).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Konfiguracja serii (YAML)

**Files:**
- Create: `bench/config.py`, `bench/conf/smoke.yaml`
- Modify: `bench/data/datasets.py:51-56` (`data_dir`: rozwijanie `~`)
- Test: `tests/bench/test_config.py`, `tests/bench/test_datasets.py` (dopisać jeden test)

**Interfaces:**
- Consumes: `bench.data.datasets.resolve`, `bench.ops.OPS`, `UNARY_OPS`.
- Produces: `bench.config`:
  - stałe: `VARIANTS = ("polars_bio_a", "polars_bio_b", "ballista", "sail")`, `NODES = (1, 2, 3)`, `DEFAULTS`;
  - `ConfigError(ValueError)`;
  - `Scenario(op: str, data: str)` (frozen; `.id` → `"overlap/1-2"`, `"merge/1"`);
  - `CONTROL = Scenario("overlap", "1-2")`;
  - `Block(variant: str, n_nodes: int)` (frozen; `.label` → `"ballista N=2"`);
  - `SeriesConfig` (frozen): `series`, `scenarios`, `variants`, `nodes`, `repeats`, `warmup`, `timeout_s`, `seed`, `control_tolerance`;
  - funkcje: `parse(raw) -> SeriesConfig`, `load(path) -> SeriesConfig`, `blocks(cfg) -> list[Block]`, `round_order(cfg, block, label: str) -> list[Scenario]`, `check_data(cfg, root: Path) -> None`.

- [ ] **Step 1: Napisz testy (nieudane)**

`tests/bench/test_config.py`:

```python
"""Konfiguracja serii (plan 3a, Zadanie 6; specyfikacja 8.2): walidacja przed pierwszym
pomiarem, rozwinięcie w bloki (kolumny macierzy 7.1) i odtwarzalna kolejność scenariuszy."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.config import Block, ConfigError, Scenario, blocks, check_data, load, parse, round_order
from bench.data.datasets import DATASETS

REPO = Path(__file__).resolve().parents[2]
_DELETE = object()
FIVE = [
    {"op": "overlap", "pair": "1-2"},
    {"op": "nearest", "pair": "1-2"},
    {"op": "coverage", "pair": "1-2"},
    {"op": "merge", "dataset": 1},
    {"op": "subtract", "pair": "1-2"},
]


def raw(**overrides) -> dict:
    base = {
        "series": "test",
        "scenarios": [{"op": "overlap", "pair": "1-2"}, {"op": "merge", "dataset": 1}],
        "variants": ["polars_bio_a", "ballista"],
        "nodes": [1, 3],
        "seed": 7,
    }
    for key, value in overrides.items():
        if value is _DELETE:
            base.pop(key)
        else:
            base[key] = value
    return base


def test_minimal_config_gets_spec_defaults():
    cfg = parse(raw())
    assert cfg.scenarios == (Scenario("overlap", "1-2"), Scenario("merge", "1"))
    assert [s.id for s in cfg.scenarios] == ["overlap/1-2", "merge/1"]
    assert (cfg.repeats, cfg.warmup, cfg.timeout_s, cfg.control_tolerance) == (5, 1, 1200.0, 0.10)
    assert (cfg.nodes, cfg.seed) == ((1, 3), 7)


def test_smoke_config_is_the_full_matrix_on_pair_1_2():
    cfg = load(REPO / "bench" / "conf" / "smoke.yaml")
    assert cfg.series == "smoke" and (cfg.repeats, cfg.warmup) == (1, 0)
    assert {s.id for s in cfg.scenarios} == {
        "overlap/1-2", "nearest/1-2", "coverage/1-2", "merge/1", "subtract/1-2",
    }
    assert len(blocks(cfg)) == 9


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"extra_key": 1}, "nieznane klucze"),
        ({"seed": _DELETE}, "brak wymaganych kluczy"),
        ({"seed": "x"}, "seed"),
        ({"seed": True}, "seed"),
        ({"series": "Smoke!"}, "series"),
        ({"scenarios": []}, "scenarios: niepusta lista"),
        ({"scenarios": ["overlap"]}, "słownika"),
        ({"scenarios": [{"op": "overlap", "pair": "1-2", "algorithm": "Lapper"}]}, "plan 3b"),
        ({"scenarios": [{"op": "overlap", "pair": "1-2", "x": 1}]}, "nieznane klucze"),
        ({"scenarios": [{"op": "join", "pair": "1-2"}]}, "nieznana operacja"),
        ({"scenarios": [{"op": "merge", "pair": "1-2"}]}, "jednym zbiorze"),
        ({"scenarios": [{"op": "overlap", "dataset": 1}]}, "parze zbiorów"),
        ({"scenarios": [{"op": "overlap", "pair": "1-9"}]}, "pair"),
        ({"scenarios": [{"op": "overlap", "pair": 12}]}, "pair"),
        ({"scenarios": [{"op": "merge", "dataset": 9}]}, "dataset"),
        ({"scenarios": [{"op": "merge", "dataset": True}]}, "dataset"),
        (
            {"scenarios": [{"op": "merge", "dataset": 1}, {"dataset": 1, "op": "merge"}]},
            "powtórzony scenariusz",
        ),
        ({"variants": ["spark"]}, "variants: nieznane"),
        ({"variants": "ballista"}, "variants: niepusta lista"),
        ({"nodes": [4]}, "nodes"),
        ({"nodes": [True]}, "nodes"),
        ({"nodes": [1.0]}, "nodes"),
        ({"nodes": [1, 1]}, "powtórzone"),
        ({"repeats": 0}, "repeats"),
        ({"warmup": -1}, "warmup"),
        ({"timeout_s": 0}, "timeout_s"),
        ({"control_tolerance": "10%"}, "control_tolerance"),
        ({"variants": ["polars_bio_b"], "nodes": [1]}, "żadnego bloku"),
    ],
)
def test_invalid_config_is_rejected_before_any_run(overrides, message):
    with pytest.raises(ConfigError, match=message):
        parse(raw(**overrides))


def test_blocks_follow_matrix_columns():
    """polars-bio A raz (N = 1, niezależny od N), B tylko dla N ≥ 2 (dla N = 1 tożsamy z A),
    Ballista i Sail dla każdego N; kolejność wariantów stała, N rosnąco."""
    cfg = parse(raw(variants=["sail", "polars_bio_b", "ballista", "polars_bio_a"], nodes=[3, 1]))
    assert blocks(cfg) == [
        Block("polars_bio_a", 1), Block("polars_bio_b", 3), Block("ballista", 1),
        Block("ballista", 3), Block("sail", 1), Block("sail", 3),
    ]
    assert Block("ballista", 3).label == "ballista N=3"


def test_round_order_is_a_reproducible_permutation():
    cfg = parse(raw(scenarios=FIVE))
    block = Block("ballista", 3)
    orders = [round_order(cfg, block, str(r)) for r in range(1, 6)]
    for order in orders:
        assert sorted(order, key=lambda s: s.id) == sorted(cfg.scenarios, key=lambda s: s.id)
    assert orders == [round_order(cfg, block, str(r)) for r in range(1, 6)]
    assert len({tuple(o) for o in orders}) > 1


def test_round_order_depends_on_seed_and_block():
    a, b = parse(raw(scenarios=FIVE, seed=7)), parse(raw(scenarios=FIVE, seed=8))
    rounds = [str(r) for r in range(1, 6)]
    block = Block("ballista", 3)
    assert [round_order(a, block, r) for r in rounds] != [round_order(b, block, r) for r in rounds]
    assert [round_order(a, block, r) for r in rounds] != [
        round_order(a, Block("sail", 3), r) for r in rounds
    ]


def test_check_data_requires_parts_for_scenarios_and_control_pair(tmp_path):
    cfg = parse(raw(scenarios=[{"op": "merge", "dataset": 1}]))
    with pytest.raises(ConfigError, match="exons"):  # zbiór 2 — z pary kontrolnej 1-2
        check_data(cfg, tmp_path)
    for idx in (1, 2):
        d = tmp_path / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "part-00000.parquet").write_bytes(b"")
    check_data(cfg, tmp_path)


def test_check_data_ignores_files_that_are_not_parts(tmp_path):
    for idx in (1, 2):
        d = tmp_path / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "_SUCCESS").write_bytes(b"")
    with pytest.raises(ConfigError, match="python -m bench.data.download"):
        check_data(parse(raw()), tmp_path)


def test_load_reports_invalid_yaml(tmp_path):
    path = tmp_path / "zly.yaml"
    path.write_text("series: [niedomknięta\n")
    with pytest.raises(ConfigError, match="niepoprawny YAML"):
        load(path)


def test_load_rejects_non_mapping(tmp_path):
    path = tmp_path / "lista.yaml"
    path.write_text("- 1\n- 2\n")
    with pytest.raises(ConfigError, match="słownika"):
        load(path)
```

W `tests/bench/test_datasets.py` dopisz `from pathlib import Path` oraz test:

```python
def test_data_dir_expands_home(monkeypatch):
    monkeypatch.setenv("BENCH_DATA_ROOT", "~/dane_testowe")
    assert ds.data_dir() == Path.home() / "dane_testowe" / "databio-8p"
```

- [ ] **Step 2: Uruchom, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_config.py tests/bench/test_datasets.py -q 2>&1 | tail -6`

Expected:
- `ModuleNotFoundError: No module named 'bench.config'`;
- `test_data_dir_expands_home` FAIL (`PosixPath('~/dane_testowe/databio-8p')`).

- [ ] **Step 3: Rozwijanie `~` w `data_dir`**

W `bench/data/datasets.py` zastąp funkcję `data_dir`:

```python
def data_dir(root: Path | None = None) -> Path:
    """Katalog zbioru: `root` albo `$BENCH_DATA_ROOT` (pusta = brak) albo domyślny.
    `~` jest rozwijane — procesy silników działają w innych katalogach niż powłoka."""
    if root is None:
        env = os.environ.get(DATA_ROOT_ENV, "")
        root = Path(env) if env else DEFAULT_DATA_ROOT
    return Path(root).expanduser() / DATASET_NAME
```

- [ ] **Step 4: `bench/config.py`**

```python
"""Konfiguracja serii pomiarowej (specyfikacja, sekcja 8.2) i jej rozwinięcie w bloki.

Plik YAML opisuje serię: scenariusze (operacja i dane), warianty, wartości N, liczbę rund,
rozgrzewkę, limit czasu, tolerancję dryfu przebiegu kontrolnego i ziarno kolejności.
Konfiguracja jest sprawdzana w całości przed pierwszym pomiarem — nieznany klucz albo brak
danych to błąd, nie cicha wartość domyślna."""

from __future__ import annotations

import random
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from bench.data.datasets import resolve
from bench.ops import OPS, UNARY_OPS

VARIANTS = ("polars_bio_a", "polars_bio_b", "ballista", "sail")
NODES = (1, 2, 3)
#: Wartości domyślne ze specyfikacji 8.2 i 6.3 (tolerancja dryfu przebiegu kontrolnego: 10%).
DEFAULTS = {"repeats": 5, "warmup": 1, "timeout_s": 1200, "control_tolerance": 0.10}
_REQUIRED = ("series", "scenarios", "variants", "nodes", "seed")
_KEYS = set(_REQUIRED) | set(DEFAULTS)
_SCENARIO_KEYS = {"op", "pair", "dataset"}
_SERIES_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*")
_PAIR = re.compile(r"[0-8]-[0-8]")


class ConfigError(ValueError):
    """Błąd konfiguracji serii — zgłaszany przed pierwszym pomiarem."""


@dataclass(frozen=True)
class Scenario:
    op: str
    #: Para "a-b" albo zbiór "a" (identyfikatory jak w polars-bio-bench).
    data: str

    @property
    def id(self) -> str:
        return f"{self.op}/{self.data}"


#: Przebieg kontrolny (specyfikacja 6.3): overlap na parze 1-2, polars-bio A.
CONTROL = Scenario("overlap", "1-2")


@dataclass(frozen=True)
class Block:
    variant: str
    n_nodes: int

    @property
    def label(self) -> str:
        return f"{self.variant} N={self.n_nodes}"


@dataclass(frozen=True)
class SeriesConfig:
    series: str
    scenarios: tuple[Scenario, ...]
    variants: tuple[str, ...]
    nodes: tuple[int, ...]
    repeats: int
    warmup: int
    timeout_s: float
    seed: int
    control_tolerance: float


def _int(raw: dict, key: str, minimum: int) -> int:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{key}: liczba całkowita ≥ {minimum}, jest {value!r}")
    return value


def _positive(raw: dict, key: str) -> float:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ConfigError(f"{key}: liczba > 0, jest {value!r}")
    return float(value)


def _list(raw: dict, key: str) -> list:
    value = raw[key]
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{key}: niepusta lista, jest {value!r}")
    if len({repr(v) for v in value}) != len(value):
        raise ConfigError(f"{key}: powtórzone elementy w {value!r}")
    return value


def _scenario(item, i: int) -> Scenario:
    where = f"scenarios[{i}]"
    if not isinstance(item, dict):
        raise ConfigError(f"{where}: oczekiwano słownika, jest {item!r}")
    if "algorithm" in item:
        raise ConfigError(f"{where}: parametr algorithm nie jest jeszcze obsługiwany (plan 3b)")
    unknown = set(item) - _SCENARIO_KEYS
    if unknown:
        raise ConfigError(f"{where}: nieznane klucze {sorted(unknown)}")
    op = item.get("op")
    if op not in OPS:
        raise ConfigError(f"{where}: nieznana operacja {op!r}; dozwolone: {', '.join(OPS)}")
    if op in UNARY_OPS:
        if "pair" in item or "dataset" not in item:
            raise ConfigError(f"{where}: {op} działa na jednym zbiorze — podaj dataset, bez pair")
        dataset = item["dataset"]
        if isinstance(dataset, bool) or not isinstance(dataset, int) or not 0 <= dataset <= 8:
            raise ConfigError(f"{where}: dataset to liczba 0–8, jest {dataset!r}")
        return Scenario(op, str(dataset))
    if "dataset" in item or "pair" not in item:
        raise ConfigError(
            f'{where}: {op} działa na parze zbiorów — podaj pair (np. "1-2"), bez dataset'
        )
    pair = item["pair"]
    if not isinstance(pair, str) or not _PAIR.fullmatch(pair):
        raise ConfigError(f'{where}: pair w postaci "a-b", a, b ∈ 0–8, jest {pair!r}')
    return Scenario(op, pair)


def parse(raw) -> SeriesConfig:
    """Słownik z YAML → SeriesConfig; każdy błąd → ConfigError z miejscem i przyczyną."""
    if not isinstance(raw, dict):
        raise ConfigError("konfiguracja: oczekiwano słownika na najwyższym poziomie")
    unknown = set(raw) - _KEYS
    if unknown:
        raise ConfigError(f"nieznane klucze: {sorted(unknown)}")
    missing = [k for k in _REQUIRED if k not in raw]
    if missing:
        raise ConfigError(f"brak wymaganych kluczy: {missing}")
    raw = {**DEFAULTS, **raw}
    series = raw["series"]
    if not isinstance(series, str) or not _SERIES_NAME.fullmatch(series):
        raise ConfigError(f"series: nazwa z małych liter, cyfr, - i _, jest {series!r}")
    scenarios = tuple(_scenario(item, i) for i, item in enumerate(_list(raw, "scenarios")))
    ids = [s.id for s in scenarios]
    duplicated = sorted({i for i in ids if ids.count(i) > 1})
    if duplicated:
        raise ConfigError(f"scenarios: powtórzony scenariusz {', '.join(duplicated)}")
    variants = tuple(_list(raw, "variants"))
    unknown_variants = [v for v in variants if v not in VARIANTS]
    if unknown_variants:
        raise ConfigError(f"variants: nieznane {unknown_variants}; dozwolone: {', '.join(VARIANTS)}")
    nodes = _list(raw, "nodes")
    if any(isinstance(n, bool) or not isinstance(n, int) or n not in NODES for n in nodes):
        raise ConfigError(f"nodes: wartości z {NODES}, jest {nodes!r}")
    seed = raw["seed"]
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ConfigError(f"seed: liczba całkowita, jest {seed!r}")
    cfg = SeriesConfig(
        series=series,
        scenarios=scenarios,
        variants=variants,
        nodes=tuple(sorted(nodes)),
        repeats=_int(raw, "repeats", 1),
        warmup=_int(raw, "warmup", 0),
        timeout_s=_positive(raw, "timeout_s"),
        seed=seed,
        control_tolerance=_positive(raw, "control_tolerance"),
    )
    if not blocks(cfg):
        raise ConfigError("konfiguracja nie daje żadnego bloku (polars_bio_b wymaga N ≥ 2)")
    return cfg


def load(path: Path) -> SeriesConfig:
    try:
        raw = yaml.safe_load(Path(path).read_text())
    except yaml.YAMLError as e:
        raise ConfigError(f"{path}: niepoprawny YAML: {e}") from e
    return parse(raw)


def blocks(cfg: SeriesConfig) -> list[Block]:
    """Bloki (wariant × N) jak kolumny macierzy 7.1: polars-bio A raz (N = 1, niezależny
    od N), polars-bio B dla N ≥ 2 (dla N = 1 tożsamy z A), Ballista i Sail dla każdego N."""
    out: list[Block] = []
    for variant in VARIANTS:
        if variant not in cfg.variants:
            continue
        if variant == "polars_bio_a":
            out.append(Block(variant, 1))
        elif variant == "polars_bio_b":
            out.extend(Block(variant, n) for n in cfg.nodes if n >= 2)
        else:
            out.extend(Block(variant, n) for n in cfg.nodes)
    return out


def round_order(cfg: SeriesConfig, block: Block, label: str) -> list[Scenario]:
    """Kolejność scenariuszy w rundzie (specyfikacja 6.2): losowa, ale odtwarzalna z ziarna
    serii — ta sama dla tego samego bloku i rundy, niezależnie od reszty serii (powtórzony
    blok dostaje tę samą kolejność)."""
    rng = random.Random(f"{cfg.seed}/{block.variant}/{block.n_nodes}/{label}")
    return rng.sample(list(cfg.scenarios), len(cfg.scenarios))


def check_data(cfg: SeriesConfig, root: Path) -> None:
    """Każdy zbiór serii — i pary kontrolnej — ma mieć pliki part-*.parquet."""
    missing: set[str] = set()
    for scenario in (*cfg.scenarios, CONTROL):
        for path in resolve(scenario.data, root):
            if path is not None and not any(path.glob("part-*.parquet")):
                missing.add(str(path))
    if missing:
        raise ConfigError(
            "brak danych (python -m bench.data.download): " + ", ".join(sorted(missing))
        )
```

`bench/conf/smoke.yaml`:

```yaml
# Smoke (specyfikacja 7.3, etap 0): pełna macierz na parze 1-2 (merge: zbiór 1),
# 1 przebieg bez rozgrzewki — poprawność narzędzia; uruchamiany przed każdą dużą serią:
#   python -m bench.orchestrator bench/conf/smoke.yaml
series: smoke
scenarios:
  - {op: overlap, pair: "1-2"}
  - {op: nearest, pair: "1-2"}
  - {op: coverage, pair: "1-2"}
  - {op: merge, dataset: 1}
  - {op: subtract, pair: "1-2"}
variants: [polars_bio_a, polars_bio_b, ballista, sail]
nodes: [1, 2, 3]
repeats: 1
warmup: 0
seed: 20261001
```

- [ ] **Step 5: Uruchom, mają przejść**

Run: `python3 -m pytest tests/bench/test_config.py tests/bench/test_datasets.py -q`
Expected: 54 passed (konfiguracja 37, zbiory 16 + 1).

- [ ] **Step 6: Commit**

```bash
git add bench/config.py bench/conf/smoke.yaml bench/data/datasets.py \
  tests/bench/test_config.py tests/bench/test_datasets.py
git commit -m "$(cat <<'EOF'
Plan 3a: konfiguracja serii w YAML (walidacja, bloki, kolejnosc z ziarna) i smoke.yaml

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Silniki bloku — przypięcie do rdzeni, klaster Ballisty, serwer Saila

**Files:**
- Create: `bench/engines.py`
- Test: `tests/bench/test_engines.py`

**Interfaces:**
- Consumes:
  - `bench.config.Scenario` (Zadanie 6), `bench.data.datasets.resolve`;
  - `bench.procutil` (Zadanie 5), `bench.runners.sail_server.sail_env` (Zadanie 5);
  - binarki `ballista_node` i `bench_client` (Zadanie 3).
- Produces: `bench.engines`:
  - stałe: `REPO`, `BALLISTA_DIR`, `PROFILES = ("release", "debug")`, `SYSTEM_CPUS = (0, 1)`, `SLOTS_PER_NODE = 2`, `REFERENCE_VARIANT = "polars_bio_ref"`, `CONTROL_VARIANT = "polars_bio_a"`;
  - funkcje: `node_cpus(k) -> tuple[int, int]`, `cluster_cpus(n) -> tuple[int, ...]`, `taskset(cpus) -> list[str]`, `ballista_binary(name, profile) -> Path`, `require_ballista_binaries(profile) -> None` (`EngineError`);
  - `Command(argv: list[str], env: dict[str, str], cwd: Path)` (frozen);
  - silniki z metodami `start()`, `stop()`, `restart()`, `command(scenario) -> Command`, `server_pids() -> dict[str, int]`, `shuffle_dirs() -> list[Path]`; atrybut `port` w Ballistcie i Sailu;
  - fabryka `make_engine(variant, n_nodes, *, data_root, log_dir, profile)`:
    - `PolarsBioEngine` dla `polars_bio_ref` (1 wątek, węzeł 1), `polars_bio_a` (2 wątki, węzeł 1) i `polars_bio_b` (2N wątków, węzły 1..N);
    - `BallistaEngine` (scheduler na CPU 0–1, executor k na węźle k, `BIO_TARGET_PARTITIONS = 2N`);
    - `SailEngine` (serwer na węzłach 1..N, `sail_env(N)`).

- [ ] **Step 1: Napisz testy (nieudane)**

`tests/bench/test_engines.py`:

```python
"""Silniki serii (plan 3a, Zadanie 7; specyfikacja 3): przypięcie procesów do rdzeni
emulowanych węzłów, jawna liczba wątków i polecenia runnerów.

Testy klastra Ballisty wymagają binarek debug (brak = BŁĄD, jak w P0):
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from bench import engines, metrics
from bench.config import Scenario
from bench.procutil import EngineError


def environ(pid: int) -> set[str]:
    return {e.decode() for e in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if e}


def test_cpu_layout_follows_spec():
    assert engines.SYSTEM_CPUS == (0, 1)
    assert [engines.node_cpus(k) for k in (1, 2, 3)] == [(2, 3), (4, 5), (6, 7)]
    assert engines.cluster_cpus(3) == (2, 3, 4, 5, 6, 7)
    assert engines.taskset((2, 3)) == ["taskset", "-c", "2,3"]
    with pytest.raises(ValueError):
        engines.node_cpus(4)


@pytest.mark.parametrize(
    "variant, n, threads, cpus",
    [
        ("polars_bio_ref", 1, 1, "2,3"),
        ("polars_bio_a", 1, 2, "2,3"),
        ("polars_bio_b", 2, 4, "2,3,4,5"),
        ("polars_bio_b", 3, 6, "2,3,4,5,6,7"),
    ],
)
def test_polars_bio_runs_pinned_with_explicit_threads(variant, n, threads, cpus, tmp_path):
    engine = engines.make_engine(variant, n, data_root=tmp_path, log_dir=tmp_path / "logi", profile="debug")
    cmd = engine.command(Scenario("overlap", "1-2"))
    base = tmp_path / "databio-8p"
    assert cmd.argv == [
        "taskset", "-c", cpus, sys.executable, "-m", "bench.runners.polars_bio_runner",
        "--op", "overlap", "--left", str(base / "fBrain-DS14718"), "--right", str(base / "exons"),
        "--threads", str(threads),
    ]
    assert cmd.env["POLARS_MAX_THREADS"] == str(threads) and cmd.env["MPLBACKEND"] == "Agg"
    assert cmd.cwd == engines.REPO
    assert engine.server_pids() == {} and engine.shuffle_dirs() == []


def test_merge_command_has_no_right_side(tmp_path):
    engine = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert "--right" not in engine.command(Scenario("merge", "1")).argv


def test_unknown_variant_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="nieznany wariant"):
        engines.make_engine("spark", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")


def test_missing_ballista_binaries_are_reported_with_build_command(tmp_path, monkeypatch):
    monkeypatch.setattr(engines, "BALLISTA_DIR", tmp_path)
    with pytest.raises(EngineError, match=r"brak binarki .*ballista_node.*cargo build --release"):
        engines.require_ballista_binaries("release")


def test_ballista_cluster_is_pinned_and_configured(tmp_path):
    engine = engines.make_engine("ballista", 2, data_root=tmp_path, log_dir=tmp_path / "logi", profile="debug")
    engine.start()
    try:
        pids = engine.server_pids()
        assert {name: metrics.cpus_allowed(pid) for name, pid in pids.items()} == {
            "scheduler": "0-1", "executor_1": "2-3", "executor_2": "4-5",
        }
        assert all("BIO_TARGET_PARTITIONS=4" in environ(pid) for pid in pids.values())
        cmd = engine.command(Scenario("merge", "1"))
        assert cmd.argv[:3] == ["taskset", "-c", "0,1"]
        assert cmd.argv[3] == str(engines.BALLISTA_DIR / "target" / "debug" / "bench_client")
        assert cmd.env["BALLISTA_SCHEDULER_URL"] == f"df://localhost:{engine.port}"
        assert cmd.env["BIO_TARGET_PARTITIONS"] == "4" and cmd.cwd == engines.BALLISTA_DIR
        dirs = engine.shuffle_dirs()
        assert len(dirs) == 2 and all(d.is_dir() for d in dirs)
    finally:
        engine.stop()
    assert not any(Path(f"/proc/{pid}").exists() for pid in pids.values())
    assert not any(d.exists() for d in dirs)


def test_ballista_restart_replaces_processes(tmp_path):
    engine = engines.make_engine("ballista", 1, data_root=tmp_path, log_dir=tmp_path / "logi", profile="debug")
    engine.start()
    try:
        before = set(engine.server_pids().values())
        engine.restart()
        after = set(engine.server_pids().values())
        assert len(after) == 2 and before.isdisjoint(after)
    finally:
        engine.stop()


def test_sail_server_is_pinned_and_configured(tmp_path):
    engine = engines.make_engine("sail", 2, data_root=tmp_path, log_dir=tmp_path / "logi", profile="debug")
    engine.start()
    try:
        pid = engine.server_pids()["sail_server"]
        assert metrics.cpus_allowed(pid) == "2-5"
        assert {
            "SAIL_MODE=local-cluster",
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT=2",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM=4",
            "MPLBACKEND=Agg",
        } <= environ(pid)
        cmd = engine.command(Scenario("overlap", "1-2"))
        assert cmd.argv[:6] == ["taskset", "-c", "0,1", sys.executable, "-m", "bench.runners.sail_runner"]
        assert cmd.argv[-2:] == ["--remote", f"sc://127.0.0.1:{engine.port}"]
        assert cmd.cwd == engines.REPO
    finally:
        engine.stop()
    assert not Path(f"/proc/{pid}").exists()
```

- [ ] **Step 2: Uruchom, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_engines.py -q 2>&1 | tail -4`
Expected: `ModuleNotFoundError: No module named 'bench.engines'`.

- [ ] **Step 3: `bench/engines.py`**

```python
"""Silniki bloku pomiarowego (specyfikacja, sekcje 3 i 6): przypięcie do rdzeni (emulacja
węzłów), klaster Ballisty, serwer Saila i polecenia runnerów.

Silnik ma interfejs: start(), stop(), restart(), command(scenariusz) → Command,
server_pids() → {nazwa: PID} (procesy długożyjące, których szczyt pamięci mierzy
orkiestrator) i shuffle_dirs() → katalogi robocze executorów (wolumen shuffle)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from bench.config import Scenario
from bench.data.datasets import resolve
from bench.procutil import EngineError, check_alive, free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent
BALLISTA_DIR = REPO / "ballista_genomics"
PROFILES = ("release", "debug")
#: Rdzeń 0 (wątki 0–1): system, scheduler Ballisty, klienci, orkiestrator (specyfikacja 3).
SYSTEM_CPUS = (0, 1)
#: Sloty zadań i wątki na węzeł: 1 rdzeń fizyczny = 2 wątki SMT.
SLOTS_PER_NODE = 2
#: Wynik wzorcowy: polars-bio na 1 partycji — przy większej liczbie partycji polars-bio 0.28
#: psuje merge i subtract (bench/runners/polars_bio_runner.py).
REFERENCE_VARIANT = "polars_bio_ref"
#: Przebieg kontrolny (specyfikacja 6.3): polars-bio A.
CONTROL_VARIANT = "polars_bio_a"


def node_cpus(k: int) -> tuple[int, int]:
    """Węzeł k (1..3) = rdzeń fizyczny k = wątki {2k, 2k+1} (lscpu -e)."""
    if k not in (1, 2, 3):
        raise ValueError(f"węzeł {k!r}: dozwolone 1–3")
    return (2 * k, 2 * k + 1)


def cluster_cpus(n_nodes: int) -> tuple[int, ...]:
    return tuple(cpu for k in range(1, n_nodes + 1) for cpu in node_cpus(k))


def taskset(cpus) -> list[str]:
    return ["taskset", "-c", ",".join(str(c) for c in cpus)]


@dataclass(frozen=True)
class Command:
    argv: list[str]
    env: dict[str, str]
    cwd: Path


def scenario_args(scenario: Scenario, data_root: Path) -> list[str]:
    left, right = resolve(scenario.data, data_root)
    args = ["--op", scenario.op, "--left", str(left)]
    return args if right is None else [*args, "--right", str(right)]


def _python(module: str, cpus, args: list[str], env: dict[str, str]) -> Command:
    return Command(
        [*taskset(cpus), sys.executable, "-m", module, *args],
        {**os.environ, "MPLBACKEND": "Agg", **env},
        REPO,
    )


class PolarsBioEngine:
    """polars-bio: świeży proces na przebieg, bez procesów długożyjących."""

    def __init__(self, threads: int, cpus, data_root: Path):
        self.threads, self.cpus, self.data_root = threads, tuple(cpus), data_root

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def restart(self) -> None:
        pass

    def command(self, scenario: Scenario) -> Command:
        return _python(
            "bench.runners.polars_bio_runner",
            self.cpus,
            [*scenario_args(scenario, self.data_root), "--threads", str(self.threads)],
            {"POLARS_MAX_THREADS": str(self.threads)},
        )

    def server_pids(self) -> dict[str, int]:
        return {}

    def shuffle_dirs(self) -> list[Path]:
        return []


def ballista_binary(name: str, profile: str) -> Path:
    return BALLISTA_DIR / "target" / profile / name


def require_ballista_binaries(profile: str) -> None:
    missing = [
        str(b)
        for b in (ballista_binary("ballista_node", profile), ballista_binary("bench_client", profile))
        if not b.exists()
    ]
    if missing:
        flag = "--release " if profile == "release" else ""
        raise EngineError(
            f"brak binarki {', '.join(missing)} — zbuduj: cd ballista_genomics && "
            f"CARGO_BUILD_JOBS=1 cargo build {flag}--bin ballista_node --bin bench_client"
        )


def _registered_executors(port: int) -> int:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/executors", timeout=2) as response:
        return len(json.loads(response.read()))


class BallistaEngine:
    """Scheduler na rdzeniu systemowym + N executorów (executor k na rdzeniu węzła k,
    po 2 sloty); BIO_TARGET_PARTITIONS = 2N we wszystkich procesach i w kliencie."""

    def __init__(self, n_nodes: int, *, profile: str, data_root: Path, log_dir: Path):
        self.n, self.profile, self.data_root, self.log_dir = n_nodes, profile, data_root, log_dir
        self.env = {**os.environ, "BIO_TARGET_PARTITIONS": str(SLOTS_PER_NODE * n_nodes)}
        self.procs: dict[str, subprocess.Popen] = {}
        self.work_dirs: dict[str, Path] = {}
        self.port: int | None = None
        self._tmp: Path | None = None

    def start(self) -> None:
        require_ballista_binaries(self.profile)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._tmp = Path(tempfile.mkdtemp(prefix="bench_ballista_"))
        self.port = free_port()
        try:
            self._spawn("scheduler", SYSTEM_CPUS, ["scheduler", "--port", str(self.port)])
            # Executor łączy się ze schedulerem tylko raz, przy starcie (P0) — najpierw scheduler.
            self._wait(lambda: _registered_executors(self.port) >= 0, "scheduler nie odpowiada")
            for k in range(1, self.n + 1):
                name = f"executor_{k}"
                work = self._tmp / name
                work.mkdir()
                self.work_dirs[name] = work
                self._spawn(name, node_cpus(k), [
                    "executor", "--scheduler-port", str(self.port),
                    "--port", str(free_port()), "--grpc-port", str(free_port()),
                    "--work-dir", str(work), "--concurrent-tasks", str(SLOTS_PER_NODE),
                ])
            self._wait(
                lambda: _registered_executors(self.port) == self.n,
                f"scheduler nie widzi {self.n} executorów",
            )
        except BaseException:
            self.stop()
            raise

    def _spawn(self, name: str, cpus, args: list[str]) -> None:
        with (self.log_dir / f"{name}.log").open("a") as log:
            self.procs[name] = subprocess.Popen(
                [*taskset(cpus), str(ballista_binary("ballista_node", self.profile)), *args],
                cwd=BALLISTA_DIR, env=self.env, stdout=log, stderr=subprocess.STDOUT,
            )

    def _wait(self, ready, message: str, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for name, proc in self.procs.items():
                check_alive(name, proc, self.log_dir / f"{name}.log")
            try:
                if ready():
                    return
            except OSError:
                pass
            time.sleep(0.3)
        raise EngineError(f"{message} po {timeout:.0f} s; logi: {self.log_dir}")

    def stop(self) -> None:
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        self.procs.clear()
        self.work_dirs.clear()
        if self._tmp is not None:
            shutil.rmtree(self._tmp, ignore_errors=True)
            self._tmp = None

    def restart(self) -> None:
        self.stop()
        self.start()

    def command(self, scenario: Scenario) -> Command:
        return Command(
            [*taskset(SYSTEM_CPUS), str(ballista_binary("bench_client", self.profile)),
             *scenario_args(scenario, self.data_root)],
            {**self.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{self.port}"},
            BALLISTA_DIR,
        )

    def server_pids(self) -> dict[str, int]:
        return {name: proc.pid for name, proc in self.procs.items()}

    def shuffle_dirs(self) -> list[Path]:
        return list(self.work_dirs.values())


class SailEngine:
    """Jeden proces serwera Saila (local-cluster, N workerów) na rdzeniach węzłów 1..N;
    runner-klient na rdzeniu systemowym."""

    def __init__(self, n_nodes: int, *, data_root: Path, log_dir: Path):
        self.n, self.data_root, self.log_dir = n_nodes, data_root, log_dir
        self.proc: subprocess.Popen | None = None
        self.port: int | None = None

    def start(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.port = free_port()
        log = self.log_dir / "sail_server.log"
        with log.open("a") as f:
            self.proc = subprocess.Popen(
                [*taskset(cluster_cpus(self.n)), sys.executable, "-m", "bench.runners.sail_server",
                 "--port", str(self.port)],
                cwd=REPO, env={**os.environ, "MPLBACKEND": "Agg", **sail_env(self.n)},
                stdout=f, stderr=subprocess.STDOUT,
            )
        try:
            wait_for_port(self.port, self.proc, name="sail_server", log=log)
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None

    def restart(self) -> None:
        self.stop()
        self.start()

    def command(self, scenario: Scenario) -> Command:
        return _python(
            "bench.runners.sail_runner",
            SYSTEM_CPUS,
            [*scenario_args(scenario, self.data_root), "--remote", f"sc://127.0.0.1:{self.port}"],
            {},
        )

    def server_pids(self) -> dict[str, int]:
        return {"sail_server": self.proc.pid} if self.proc is not None else {}

    def shuffle_dirs(self) -> list[Path]:
        return []


def make_engine(variant: str, n_nodes: int, *, data_root: Path, log_dir: Path, profile: str):
    """Silnik bloku (wariant × N) według specyfikacji 3."""
    if variant == REFERENCE_VARIANT:
        return PolarsBioEngine(1, node_cpus(1), data_root)
    if variant == "polars_bio_a":
        return PolarsBioEngine(SLOTS_PER_NODE, node_cpus(1), data_root)
    if variant == "polars_bio_b":
        return PolarsBioEngine(SLOTS_PER_NODE * n_nodes, cluster_cpus(n_nodes), data_root)
    if variant == "ballista":
        return BallistaEngine(n_nodes, profile=profile, data_root=data_root, log_dir=log_dir)
    if variant == "sail":
        return SailEngine(n_nodes, data_root=data_root, log_dir=log_dir)
    raise ValueError(f"nieznany wariant {variant!r}")
```

- [ ] **Step 4: Uruchom, mają przejść**

Run: `python3 -m pytest tests/bench/test_engines.py -v 2>&1 | tail -15`
Expected: 11 passed (klaster Ballisty i serwer Saila wstają i znikają w kilkanaście sekund).

- [ ] **Step 5: Commit**

```bash
git add bench/engines.py tests/bench/test_engines.py
git commit -m "$(cat <<'EOF'
Plan 3a: silniki bloku - przypiecie do rdzeni, klaster Ballisty i serwer Saila

Emulacja wezlow (specyfikacja 3): CPU 0-1 system i klienci, wezel k = CPU 2k, 2k+1.
polars-bio: 1 / 2 / 2N watkow (wzorzec / A / B); Ballista: executor k na wezle k,
2 sloty, BIO_TARGET_PARTITIONS = 2N; Sail: serwer local-cluster na wezlach 1..N.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Reguły ważności przebiegu i zapis wyników

**Files:**
- Create: `bench/validity.py`, `bench/results.py`
- Modify: `.gitignore`
- Test: `tests/bench/test_validity.py`, `tests/bench/test_results.py`

**Interfaces:**
- Produces:
  - `bench.validity`:
    - `REPORT_KEYS`;
    - `Expected(rows: int, checksum: str)` (frozen);
    - `parse_report(stdout: str) -> dict` (`ValueError` z opisem);
    - `stderr_tail(stderr: str, limit=300) -> str`;
    - `invalid_reasons(*, returncode, killed, report, report_error, expected, pswpout_delta, stderr) -> list[str]`;
    - `drift(t_start, t_end) -> float`;
    - `block_drifted(t_start | None, t_end | None, tolerance) -> bool`.
  - `bench.results`:
    - `SCHEMA: dict[str, polars dtype]` (29 pól);
    - `ResultsWriter(path)` z metodą `.write(row: dict)` (`ValueError` przy nieznanych lub brakujących polach) i atrybutem `.path`;
    - `to_parquet(jsonl: Path, parquet: Path) -> int`.

- [ ] **Step 1: Napisz testy (nieudane)**

`tests/bench/test_validity.py`:

```python
"""Reguły ważności przebiegu i dryfu (plan 3a, Zadanie 8; specyfikacja 6.3–6.5 i 8.3)."""

from __future__ import annotations

import json

import pytest

from bench.validity import Expected, block_drifted, drift, invalid_reasons, parse_report, stderr_tail

GOOD = {
    "rows": 3, "checksum": "0x00000000000000aa", "t_total_s": 0.5, "phases": {},
    "extra": {"target_partitions": 2}, "peak_rss_bytes": 1234,
}
EXPECTED = Expected(3, "0x00000000000000aa")


def reasons(**overrides) -> list[str]:
    args = dict(
        returncode=0, killed=None, report=dict(GOOD), report_error=None, expected=EXPECTED,
        pswpout_delta=0, stderr="",
    )
    args.update(overrides)
    return invalid_reasons(**args)


def test_parse_report_accepts_protocol_line():
    assert parse_report("\n" + json.dumps(GOOD) + "\n") == GOOD


@pytest.mark.parametrize(
    "stdout, message",
    [
        ("", "jednej linii"),
        (json.dumps(GOOD) + "\n" + json.dumps(GOOD), "jednej linii"),
        ("to nie JSON", "niepoprawny JSON"),
        ("[1, 2]", "obiektu"),
        (json.dumps({k: v for k, v in GOOD.items() if k != "extra"}), "brak pól"),
        (json.dumps({**GOOD, "rows": -1}), "rows"),
        (json.dumps({**GOOD, "rows": True}), "rows"),
        (json.dumps({**GOOD, "checksum": "0xAA"}), "checksum"),
        (json.dumps({**GOOD, "t_total_s": "1"}), "t_total_s"),
        (json.dumps({**GOOD, "phases": []}), "phases"),
        (json.dumps({**GOOD, "peak_rss_bytes": 0}), "peak_rss_bytes"),
    ],
)
def test_parse_report_rejects_broken_lines(stdout, message):
    with pytest.raises(ValueError, match=message):
        parse_report(stdout)


def test_valid_run_has_no_reasons():
    assert reasons() == []


def test_killed_run_reports_why_it_was_killed_and_swap():
    assert reasons(killed="timeout", returncode=-9, report=None, pswpout_delta=5) == [
        "timeout", "swap: pswpout +5",
    ]


def test_engine_error_quotes_last_stderr_line():
    got = reasons(returncode=1, report=None, stderr="log\nbench_client: brak danych: /x\n\n")
    assert got == ["kod wyjścia 1: bench_client: brak danych: /x"]


def test_missing_report_uses_parse_error():
    got = reasons(report=None, report_error="stdout runnera: niepoprawny JSON")
    assert got == ["stdout runnera: niepoprawny JSON"]


def test_wrong_rows_and_checksum_are_both_reported():
    got = reasons(report={**GOOD, "rows": 4, "checksum": "0x00000000000000bb"})
    assert got == [
        "liczba wierszy 4 ≠ wzorzec 3",
        "suma kontrolna 0x00000000000000bb ≠ wzorzec 0x00000000000000aa",
    ]


def test_swap_invalidates_otherwise_good_run():
    assert reasons(pswpout_delta=12) == ["swap: pswpout +12"]


def test_without_expected_value_only_execution_is_checked():
    assert reasons(expected=None, report={**GOOD, "rows": 99}) == []


def test_stderr_tail_is_shortened():
    assert stderr_tail("x" * 1000, limit=10) == "x" * 10
    assert stderr_tail("") == "(pusty stderr)"


def test_drift_rule():
    assert drift(1.0, 1.2) == pytest.approx(0.2)
    assert not block_drifted(1.0, 1.05, 0.10)
    assert block_drifted(1.0, 1.2, 0.10)
    assert block_drifted(1.0, 0.8, 0.10)
    assert block_drifted(None, 1.0, 0.10) and block_drifted(1.0, None, 0.10)
```

`tests/bench/test_results.py`:

```python
"""Zapis wyników serii (plan 3a, Zadanie 8; specyfikacja 8.5)."""

from __future__ import annotations

import json

import polars as pl
import pytest

from bench.results import SCHEMA, ResultsWriter, to_parquet


def full_row(**overrides) -> dict:
    row = dict.fromkeys(SCHEMA)
    row.update(
        timestamp="2026-10-01T12:00:00", git_commit="abc", engine_versions=json.dumps({"ballista": "53.0.0"}),
        series="test", seed=7, scenario_id="overlap/1-2", op="overlap", pair="1-2", variant="ballista",
        n_nodes=2, rep=1, attempt=1, is_warmup=False, is_control=False, is_reference=False, valid=True,
        rows=3, checksum="0x00000000000000aa", t_total_s=0.5, wall_s=1.5, phases="{}",
        extra=json.dumps({"target_partitions": 4}), peak_rss=json.dumps({"runner": 10, "scheduler": 20}),
        peak_rss_sum=30, shuffle_bytes=100, pswpout_delta=0,
    )
    row.update(overrides)
    return row


def test_rows_round_trip_through_jsonl_and_parquet(tmp_path):
    first = full_row()
    second = full_row(rep=2, valid=False, invalid_reason="timeout", rows=None, checksum=None, t_total_s=None)
    ResultsWriter(tmp_path / "runs.jsonl").write(first)
    ResultsWriter(tmp_path / "runs.jsonl").write(second)  # dopisywanie (kolejny blok)
    assert to_parquet(tmp_path / "runs.jsonl", tmp_path / "runs.parquet") == 2
    df = pl.read_parquet(tmp_path / "runs.parquet")
    assert df.schema == pl.Schema(SCHEMA)
    assert df.to_dicts() == [first, second]
    assert json.loads(df["peak_rss"][0]) == {"runner": 10, "scheduler": 20}


def test_missing_jsonl_gives_empty_table_with_schema(tmp_path):
    assert to_parquet(tmp_path / "brak.jsonl", tmp_path / "runs.parquet") == 0
    df = pl.read_parquet(tmp_path / "runs.parquet")
    assert df.height == 0 and df.schema == pl.Schema(SCHEMA)


@pytest.mark.parametrize("unknown, removed", [("nieznane", None), (None, "valid")])
def test_row_with_unknown_or_missing_field_is_rejected(tmp_path, unknown, removed):
    row = full_row()
    if unknown:
        row[unknown] = 1
    if removed:
        row.pop(removed)
    with pytest.raises(ValueError, match="wiersz wyniku"):
        ResultsWriter(tmp_path / "runs.jsonl").write(row)
    assert not (tmp_path / "runs.jsonl").exists()
```

- [ ] **Step 2: Uruchom, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_validity.py tests/bench/test_results.py -q 2>&1 | tail -4`
Expected: `ModuleNotFoundError: No module named 'bench.validity'` (i `bench.results`).

- [ ] **Step 3: `bench/validity.py`**

```python
"""Reguły ważności przebiegu (specyfikacja 6.4–6.5) i dryfu przebiegu kontrolnego (6.3)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

#: Pola linii JSON runnera (specyfikacja 8.3).
REPORT_KEYS = ("rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes")
_CHECKSUM = re.compile(r"0x[0-9a-f]{16}")


@dataclass(frozen=True)
class Expected:
    """Wynik wzorcowy scenariusza: liczba wierszy i suma kontrolna."""

    rows: int
    checksum: str


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_report(stdout: str) -> dict:
    """Jedna niepusta linia JSON zgodna z protokołem 8.3; inaczej ValueError z opisem."""
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise ValueError(f"stdout runnera: oczekiwano jednej linii JSON, jest {len(lines)}")
    try:
        report = json.loads(lines[0])
    except json.JSONDecodeError as e:
        raise ValueError(f"stdout runnera: niepoprawny JSON ({e})") from e
    if not isinstance(report, dict):
        raise ValueError("stdout runnera: oczekiwano obiektu JSON")
    missing = [k for k in REPORT_KEYS if k not in report]
    if missing:
        raise ValueError(f"stdout runnera: brak pól {missing}")
    if not (_is_int(report["rows"]) and report["rows"] >= 0):
        raise ValueError(f"stdout runnera: rows = {report['rows']!r}")
    if not (isinstance(report["checksum"], str) and _CHECKSUM.fullmatch(report["checksum"])):
        raise ValueError(f"stdout runnera: checksum = {report['checksum']!r}")
    t = report["t_total_s"]
    if isinstance(t, bool) or not isinstance(t, (int, float)) or t < 0:
        raise ValueError(f"stdout runnera: t_total_s = {t!r}")
    if not isinstance(report["phases"], dict) or not isinstance(report["extra"], dict):
        raise ValueError("stdout runnera: phases i extra muszą być obiektami")
    if not (_is_int(report["peak_rss_bytes"]) and report["peak_rss_bytes"] > 0):
        raise ValueError(f"stdout runnera: peak_rss_bytes = {report['peak_rss_bytes']!r}")
    return report


def stderr_tail(stderr: str, limit: int = 300) -> str:
    """Ostatnia niepusta linia stderr (komunikat błędu runnera), skrócona do `limit` znaków."""
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    return lines[-1][-limit:] if lines else "(pusty stderr)"


def invalid_reasons(
    *, returncode: int, killed: str | None, report: dict | None, report_error: str | None,
    expected: Expected | None, pswpout_delta: int | None, stderr: str,
) -> list[str]:
    """Przyczyny nieważności przebiegu (pusta lista = ważny), w kolejności:

    - zabicie przez orkiestrator (timeout, strażnik pamięci) — przyczyna wprost;
    - niezerowy kod wyjścia — z ostatnią linią stderr;
    - brak poprawnej linii JSON;
    - liczba wierszy albo suma kontrolna różna od wzorca;
    - przyrost pswpout (specyfikacja 6.5: swap unieważnia czas i pamięć)."""
    reasons: list[str] = []
    if killed is not None:
        reasons.append(killed)
    elif returncode != 0:
        reasons.append(f"kod wyjścia {returncode}: {stderr_tail(stderr)}")
    elif report is None:
        reasons.append(report_error or "brak raportu runnera")
    elif expected is not None:
        if report["rows"] != expected.rows:
            reasons.append(f"liczba wierszy {report['rows']} ≠ wzorzec {expected.rows}")
        if report["checksum"] != expected.checksum:
            reasons.append(f"suma kontrolna {report['checksum']} ≠ wzorzec {expected.checksum}")
    if pswpout_delta:
        reasons.append(f"swap: pswpout +{pswpout_delta}")
    return reasons


def drift(t_start: float, t_end: float) -> float:
    """Względna zmiana czasu przebiegu kontrolnego między początkiem a końcem bloku."""
    return abs(t_end - t_start) / max(t_start, 1e-9)


def block_drifted(t_start: float | None, t_end: float | None, tolerance: float) -> bool:
    """Blok do powtórzenia (specyfikacja 6.3): dryf ponad tolerancję albo nieważny przebieg
    kontrolny (brak czasu)."""
    if t_start is None or t_end is None:
        return True
    return drift(t_start, t_end) > tolerance
```

- [ ] **Step 4: `bench/results.py`**

```python
"""Zapis wyników serii (specyfikacja 8.5): wiersz na przebieg, dopisywany do JSON Lines
(odporny na przerwanie serii) i przepisywany na końcu do Parquet."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

#: Wiersz wyniku: pola specyfikacji 8.5 oraz is_reference, attempt, wall_s i extra (plan 3a).
SCHEMA = {
    "timestamp": pl.Utf8,
    "git_commit": pl.Utf8,
    "engine_versions": pl.Utf8,  # JSON: komponent -> wersja
    "series": pl.Utf8,
    "seed": pl.Int64,
    "scenario_id": pl.Utf8,
    "op": pl.Utf8,
    "pair": pl.Utf8,  # para "a-b" albo zbiór "a"
    "algorithm": pl.Utf8,  # plan 3b
    "variant": pl.Utf8,
    "n_nodes": pl.Int64,
    "rep": pl.Int64,  # runda 1..R; 0 — rozgrzewka, wzorzec, kontrola na początku bloku; 1 — kontrola na końcu
    "attempt": pl.Int64,  # 1; 2 — powtórzenie bloku po dryfie
    "is_warmup": pl.Boolean,
    "is_control": pl.Boolean,
    "is_reference": pl.Boolean,
    "valid": pl.Boolean,
    "invalid_reason": pl.Utf8,
    "rows": pl.Int64,
    "checksum": pl.Utf8,
    "t_total_s": pl.Float64,  # czas zmierzony przez runner
    "wall_s": pl.Float64,  # czas procesu runnera razem ze startem (długość serii)
    "phases": pl.Utf8,  # JSON
    "extra": pl.Utf8,  # JSON z runnera (np. target_partitions)
    "peak_rss": pl.Utf8,  # JSON: proces -> bajty
    "peak_rss_sum": pl.Int64,  # górne oszacowanie pamięci (specyfikacja 5)
    "shuffle_bytes": pl.Int64,
    "broadcast_bytes": pl.Int64,  # plan 3b
    "pswpout_delta": pl.Int64,
}


class ResultsWriter:
    """Dopisuje wiersze do pliku JSON Lines — każdy od razu na dysk."""

    def __init__(self, path: Path):
        self.path = path

    def write(self, row: dict) -> None:
        unknown, missing = set(row) - set(SCHEMA), set(SCHEMA) - set(row)
        if unknown or missing:
            raise ValueError(
                f"wiersz wyniku: nieznane pola {sorted(unknown)}, brakujące {sorted(missing)}"
            )
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def to_parquet(jsonl: Path, parquet: Path) -> int:
    """Przepisuje JSON Lines do Parquet; brak pliku → pusta tabela ze schematem."""
    rows: list[dict] = []
    if jsonl.exists():
        rows = [
            json.loads(line)
            for line in jsonl.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    pl.DataFrame(rows, schema=SCHEMA).write_parquet(parquet)
    return len(rows)
```

`.gitignore`, dopisz na końcu:

```
# Serie pomiarowe: surowe JSON Lines, logi silników i pliki tymczasowe (wynik: runs.parquet)
bench/results/**/runs.jsonl
bench/results/**/logs/
bench/results/**/tmp/
```

- [ ] **Step 5: Uruchom, mają przejść**

Run: `python3 -m pytest tests/bench/test_validity.py tests/bench/test_results.py -v 2>&1 | tail -6`
Expected: 25 passed (ważność 21, wyniki 4).

Run: `git check-ignore -v bench/results/smoke/20261001-120000/runs.jsonl bench/results/smoke/20261001-120000/logs/a.log`
Expected: obie ścieżki dopasowane do nowych reguł `.gitignore`.

- [ ] **Step 6: Commit**

```bash
git add bench/validity.py bench/results.py .gitignore tests/bench/test_validity.py tests/bench/test_results.py
git commit -m "$(cat <<'EOF'
Plan 3a: reguly waznosci przebiegu i dryfu, zapis wynikow (JSON Lines -> Parquet)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Orkiestrator serii

**Files:**
- Create: `bench/orchestrator.py`
- Create: `tests/bench/fake_runner.py`
- Test: `tests/bench/test_orchestrator.py`, `tests/test_orchestrator_integration.py`

**Interfaces:**
- Consumes:
  - `bench.config` (Zadanie 6), `bench.engines` (Zadanie 7), `bench.procutil.EngineError` (Zadanie 5);
  - `bench.validity`, `bench.results` (Zadanie 8), `bench.metrics` (Zadanie 2).
- Produces: `bench.orchestrator`:
  - stałe: `POLL_S = 0.2`, `MEM_FLOOR_BYTES = 300 MiB`, `MAX_BLOCK_ATTEMPTS = 2`;
  - `Probe` (dataclass funkcji metryk: `mem_available`, `pswpout`, `peak_rss`, `reset_peak_rss`, `dir_size`);
  - `Outcome`;
  - `SeriesSummary(directory, parquet, rows, invalid, failures)` z właściwością `.ok`;
  - `SeriesError`;
  - `execute(command, *, timeout_s, mem_floor, probe, scratch) -> Outcome`;
  - `git_commit() -> str`, `engine_versions(profile) -> dict`;
  - `run_series(cfg, *, data_root, results_dir, profile="release", factory=None, probe=None, mem_floor=MEM_FLOOR_BYTES, out=print) -> SeriesSummary`;
  - `preflight(cfg, profile) -> None`;
  - `main(argv=None) -> int`, uruchamiany jako `python -m bench.orchestrator KONFIGURACJA [--data-root] [--results-dir] [--ballista-profile release|debug]`.

  Wyniki trafiają do `results_dir/<seria>/<znacznik czasu>/runs.jsonl`, `runs.parquet` i `logs/<wariant>-n<N>/`.

- [ ] **Step 1: Fałszywy runner i testy jednostkowe (nieudane)**

`tests/bench/fake_runner.py`:

```python
"""Fałszywy runner do testów orkiestratora (tests/bench/test_orchestrator.py): zachowanie
sterowane argumentami, wynik w protokole 8.3."""

import argparse
import json
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=3)
parser.add_argument("--checksum", default="0x00000000000000aa")
parser.add_argument("--t", type=float, default=0.25)
parser.add_argument("--t-file", type=Path, help="kolejne czasy, po jednym na wywołanie")
parser.add_argument("--sleep", type=float, default=0.0)
parser.add_argument("--touch", type=Path, help="plik tworzony na starcie (sygnał dla testu)")
parser.add_argument("--exit", type=int, default=0)
parser.add_argument("--garbage", action="store_true")
parser.add_argument("--stderr-mb", type=int, default=0)
args = parser.parse_args()

if args.touch:
    args.touch.touch()
if args.stderr_mb:
    line = "x" * 1023 + "\n"
    for _ in range(args.stderr_mb * 1024):
        sys.stderr.write(line)
time.sleep(args.sleep)
if args.exit:
    print("silnik padł: błąd testowy", file=sys.stderr)
    sys.exit(args.exit)
if args.garbage:
    print("to nie jest JSON")
    sys.exit(0)
t = args.t
if args.t_file:
    first, *rest = args.t_file.read_text().split()
    args.t_file.write_text("\n".join(rest))
    t = float(first)
print(json.dumps({
    "rows": args.rows, "checksum": args.checksum, "t_total_s": t, "phases": {}, "extra": {},
    "peak_rss_bytes": 1234,
}))
```

`tests/bench/test_orchestrator.py`:

```python
"""Orkiestrator serii (plan 3a, Zadanie 9) na fałszywych silnikach i runnerze: logika bloków,
ważności, limitu czasu, strażnika pamięci, swapu, dryfu i przerwań — bez uruchamiania silników."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest
import yaml

from bench import engines, metrics
from bench import orchestrator as orch
from bench.config import parse
from bench.data.datasets import DATASETS
from bench.engines import Command
from bench.procutil import EngineError

REPO = Path(__file__).resolve().parents[2]
FAKE_RUNNER = Path(__file__).resolve().parent / "fake_runner.py"


class FakeEngine:
    """Silnik bez procesów: runnerem jest fake_runner.py z argumentami zależnymi od wariantu
    i scenariusza; start, stop i restart trafiają do wspólnego dziennika."""

    def __init__(self, variant, n_nodes, args_for, log):
        self.variant, self.n, self.args_for, self.log = variant, n_nodes, args_for, log
        self.pids: dict[str, int] = {}

    def start(self):
        self.log.append(("start", self.variant, self.n))

    def stop(self):
        self.log.append(("stop", self.variant, self.n))

    def restart(self):
        self.log.append(("restart", self.variant, self.n))

    def command(self, scenario):
        args = self.args_for(self.variant, self.n, scenario)
        return Command([sys.executable, str(FAKE_RUNNER), *args], dict(os.environ), REPO)

    def server_pids(self):
        return dict(self.pids)

    def shuffle_dirs(self):
        return []


def make_factory(args_for=lambda variant, n, scenario: [], customize=None):
    log: list[tuple] = []

    def factory(variant, n_nodes):
        engine = FakeEngine(variant, n_nodes, args_for, log)
        if customize:
            customize(engine)
        return engine

    factory.log = log
    return factory


def probe(**overrides) -> orch.Probe:
    functions = dict(
        mem_available=lambda: 4 << 30,
        pswpout=lambda: 0,
        peak_rss=lambda pid: 2048,
        reset_peak_rss=lambda pid: None,
        dir_size=lambda path: 0,
    )
    functions.update(overrides)
    return orch.Probe(**functions)


def config(**overrides):
    raw = {
        "series": "test",
        "scenarios": [{"op": "overlap", "pair": "1-2"}, {"op": "merge", "dataset": 1}],
        "variants": ["polars_bio_a", "ballista"],
        "nodes": [1],
        "repeats": 2,
        "warmup": 1,
        "timeout_s": 30,
        "seed": 7,
        "control_tolerance": 0.5,
    }
    raw.update(overrides)
    return parse(raw)


def run(tmp_path, cfg=None, **kwargs):
    kwargs.setdefault("factory", make_factory())
    kwargs.setdefault("probe", probe())
    return orch.run_series(
        cfg or config(), data_root=tmp_path, results_dir=tmp_path / "wyniki",
        out=lambda line: None, **kwargs,
    )


def rows(summary) -> pl.DataFrame:
    return pl.read_parquet(summary.parquet)


def block(df: pl.DataFrame, variant: str) -> pl.DataFrame:
    """Przebiegi bloku wariantu bez wzorca i przebiegów kontrolnych."""
    return df.filter(
        (pl.col("variant") == variant) & ~pl.col("is_control") & ~pl.col("is_reference")
    )


def test_series_runs_reference_then_blocks_with_control_warmup_and_rounds(tmp_path):
    factory = make_factory()
    summary = run(tmp_path, factory=factory)
    df = rows(summary)
    reference = df.filter(pl.col("is_reference"))
    assert reference["scenario_id"].to_list() == ["overlap/1-2", "merge/1"]
    assert set(reference["variant"]) == {"polars_bio_ref"}
    for variant in ("polars_bio_a", "ballista"):
        b = block(df, variant)
        assert b.filter(pl.col("is_warmup"))["rep"].to_list() == [0, 0]
        assert sorted(b.filter(~pl.col("is_warmup"))["rep"].to_list()) == [1, 1, 2, 2]
    control = df.filter(pl.col("is_control"))
    assert control["rep"].to_list() == [0, 1, 0, 1]
    assert set(control["scenario_id"]) == {"overlap/1-2"} and set(control["variant"]) == {"polars_bio_a"}
    assert df.height == 2 + 2 * (2 + 2 + 4)
    assert df["valid"].all() and summary.ok and (summary.rows, summary.invalid) == (df.height, 0)
    assert set(df["attempt"]) == {1}
    assert json.loads(df["peak_rss"][0]) == {"runner": 1234}
    assert factory.log == [
        ("start", "polars_bio_ref", 1), ("stop", "polars_bio_ref", 1),
        ("start", "polars_bio_a", 1), ("stop", "polars_bio_a", 1),
        ("start", "ballista", 1), ("stop", "ballista", 1),
    ]
    assert not (summary.directory / "tmp").exists()


def test_wrong_result_invalidates_only_that_scenario(tmp_path):
    def args_for(variant, n, scenario):
        return ["--checksum", "0x00000000000000bb"] if (variant, scenario.op) == ("ballista", "merge") else []

    summary = run(tmp_path, factory=make_factory(args_for))
    bad = rows(summary).filter(~pl.col("valid"))
    assert set(zip(bad["variant"], bad["op"])) == {("ballista", "merge")}
    assert bad.height == 3  # rozgrzewka i dwie rundy
    assert set(bad["invalid_reason"]) == {
        "suma kontrolna 0x00000000000000bb ≠ wzorzec 0x00000000000000aa"
    }
    assert not summary.ok and summary.invalid == 3


def test_engine_error_is_recorded_with_its_message(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "1"] if (variant, scenario.op) == ("ballista", "overlap") else []

    bad = rows(run(tmp_path, factory=make_factory(args_for))).filter(~pl.col("valid"))
    assert set(bad["scenario_id"]) == {"overlap/1-2"} and set(bad["variant"]) == {"ballista"}
    assert set(bad["invalid_reason"]) == {"kod wyjścia 1: silnik padł: błąd testowy"}


def test_garbage_on_stdout_invalidates_run(tmp_path):
    def args_for(variant, n, scenario):
        return ["--garbage"] if variant == "ballista" else []

    bad = rows(run(tmp_path, factory=make_factory(args_for))).filter(~pl.col("valid"))
    assert bad.height == 6
    assert all(r.startswith("stdout runnera: niepoprawny JSON") for r in bad["invalid_reason"])


def test_timeout_kills_runner_and_skips_scenario_for_rest_of_block(tmp_path):
    def args_for(variant, n, scenario):
        return ["--sleep", "60"] if (variant, scenario.op) == ("ballista", "merge") else []

    factory = make_factory(args_for)
    summary = run(tmp_path, cfg=config(timeout_s=1), factory=factory)
    merge = block(rows(summary), "ballista").filter(pl.col("op") == "merge").sort("rep")
    assert merge["invalid_reason"].to_list() == ["timeout", "pominięty: timeout", "pominięty: timeout"]
    assert merge["wall_s"][0] < 10 and merge["wall_s"][1:].is_null().all()
    assert ("restart", "ballista", 1) in factory.log


def test_memory_watchdog_kills_runner(tmp_path):
    marker = tmp_path / "pamiec_sie_konczy"

    def mem_available():
        if marker.exists():
            marker.unlink()
            return 100 * 2**20
        return 4 << 30

    def args_for(variant, n, scenario):
        if (variant, scenario.op) == ("ballista", "merge"):
            return ["--touch", str(marker), "--sleep", "60"]
        return []

    summary = run(tmp_path, factory=make_factory(args_for), probe=probe(mem_available=mem_available))
    merge = block(rows(summary), "ballista").filter(pl.col("op") == "merge").sort("rep")
    reason = "strażnik pamięci: MemAvailable 100 MiB < 300 MiB"
    assert merge["invalid_reason"].to_list() == [reason, f"pominięty: {reason}", f"pominięty: {reason}"]


def test_swap_during_run_invalidates_it(tmp_path):
    marker = tmp_path / "swap"

    def args_for(variant, n, scenario):
        return ["--touch", str(marker)] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(
        tmp_path, factory=make_factory(args_for),
        probe=probe(pswpout=lambda: 10 if marker.exists() else 0),
    )
    bad = rows(summary).filter(~pl.col("valid"))
    assert bad.height == 1
    assert bad["invalid_reason"][0] == "swap: pswpout +10" and bad["pswpout_delta"][0] == 10


def _control_times(tmp_path, times: list[float]):
    path = tmp_path / "czasy_kontroli.txt"
    path.write_text("\n".join(map(str, times)))

    def args_for(variant, n, scenario):
        # Bez wariantu polars_bio_a w serii jedynymi przebiegami polars_bio_a są kontrolne.
        return ["--t-file", str(path)] if variant == "polars_bio_a" else []

    return make_factory(args_for)


def test_control_drift_repeats_block_once(tmp_path):
    factory = _control_times(tmp_path, [1.0, 2.0, 1.0, 1.0])
    summary = run(tmp_path, cfg=config(variants=["ballista"], control_tolerance=0.1), factory=factory)
    df = rows(summary).filter(~pl.col("is_reference"))
    first, second = df.filter(pl.col("attempt") == 1), df.filter(pl.col("attempt") == 2)
    assert first.height == second.height == 8
    assert set(first["invalid_reason"]) == {"dryf kontrolny 100%"} and not first["valid"].any()
    assert second["valid"].all()
    assert summary.invalid == 8 and not summary.ok


def test_drift_twice_leaves_block_invalid_without_third_attempt(tmp_path):
    factory = _control_times(tmp_path, [1.0, 2.0, 1.0, 2.0])
    summary = run(tmp_path, cfg=config(variants=["ballista"], control_tolerance=0.1), factory=factory)
    df = rows(summary).filter(~pl.col("is_reference"))
    assert set(df["attempt"]) == {1, 2} and not df["valid"].any()


def test_reference_failure_aborts_series_but_keeps_written_rows(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "3"] if (variant, scenario.op) == ("polars_bio_ref", "merge") else []

    with pytest.raises(orch.SeriesError, match="merge/1"):
        run(tmp_path, factory=make_factory(args_for))
    df = pl.read_parquet(next((tmp_path / "wyniki").rglob("runs.parquet")))
    assert df["scenario_id"].to_list() == ["overlap/1-2", "merge/1"] and df["is_reference"].all()


def test_engine_start_failure_is_reported_and_other_blocks_run(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            def start():
                raise EngineError("executor nie wstał")
            engine.start = start

    summary = run(tmp_path, factory=make_factory(customize=customize))
    assert summary.failures == ["ballista N=1: executor nie wstał"] and not summary.ok
    df = rows(summary)
    assert "ballista" not in set(df["variant"]) and block(df, "polars_bio_a").height == 6


def test_interrupt_keeps_finished_rows_and_stops_engine(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            def command(scenario):
                raise KeyboardInterrupt
            engine.command = command

    factory = make_factory(customize=customize)
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, factory=factory)
    df = pl.read_parquet(next((tmp_path / "wyniki").rglob("runs.parquet")))
    assert block(df, "polars_bio_a")["valid"].all()
    assert df["invalid_reason"][-1] == "seria przerwana"  # kontrola przerwanego bloku Ballisty
    assert factory.log[-1] == ("stop", "ballista", 1)


def test_runner_flooding_stderr_does_not_block(tmp_path):
    def args_for(variant, n, scenario):
        return ["--stderr-mb", "20"] if variant == "ballista" else []

    assert run(tmp_path, factory=make_factory(args_for)).ok


def test_dead_server_process_invalidates_run_and_restarts_engine(tmp_path):
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()

    def customize(engine):
        if engine.variant == "ballista":
            engine.pids = {"executor_1": dead.pid}

    factory = make_factory(customize=customize)
    summary = run(
        tmp_path, factory=factory,
        probe=probe(peak_rss=metrics.peak_rss, reset_peak_rss=metrics.reset_peak_rss),
    )
    reasons = block(rows(summary), "ballista")["invalid_reason"].to_list()
    died = "proces executor_1 zakończył się w trakcie przebiegu"
    assert reasons.count(died) == 2  # pierwszy przebieg każdego scenariusza
    assert all(r in (died, f"pominięty: {died}") for r in reasons)
    assert factory.log.count(("restart", "ballista", 1)) == 2


def _fixture_data(root: Path) -> Path:
    for idx in (1, 2):
        d = root / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "part-00000.parquet").write_bytes(b"")
    return root


def test_main_fails_fast_without_ballista_binaries(tmp_path, monkeypatch, capsys):
    root = _fixture_data(tmp_path / "dane")
    cfg = tmp_path / "seria.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "b", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["ballista"], "nodes": [1], "seed": 1,
    }))
    monkeypatch.setattr(engines, "BALLISTA_DIR", tmp_path / "brak")
    code = orch.main([str(cfg), "--data-root", str(root), "--results-dir", str(tmp_path / "wyniki")])
    assert code == 2
    assert "brak binarki" in capsys.readouterr().err
    assert not (tmp_path / "wyniki").exists()


def test_main_reports_config_error_with_code_2(tmp_path, capsys):
    cfg = tmp_path / "zla.yaml"
    cfg.write_text("series: x\n")
    assert orch.main([str(cfg), "--results-dir", str(tmp_path / "wyniki")]) == 2
    assert "brak wymaganych kluczy" in capsys.readouterr().err
    assert not (tmp_path / "wyniki").exists()
```

- [ ] **Step 2: Uruchom, mają nie przejść**

Run: `python3 -m pytest tests/bench/test_orchestrator.py -q 2>&1 | tail -4`
Expected: `ImportError: cannot import name 'orchestrator' from 'bench'`.

- [ ] **Step 3: `bench/orchestrator.py`**

```python
"""Orkiestrator serii pomiarowej (specyfikacja 6 i 8): przebieg wzorcowy, potem bloki
(wariant × N) — przebieg kontrolny, rozgrzewka, rundy w losowej kolejności z ziarna,
przebieg kontrolny — z walidacją każdego przebiegu i zapisem wiersza na przebieg.

Uruchomienie (seria pomiarowa: z Windows Terminal, VS Code zamknięty — checklista RAM):
    python -m bench.orchestrator bench/conf/smoke.yaml [--ballista-profile debug]
Kody wyjścia: 0 — wszystkie przebiegi ważne; 1 — są przebiegi nieważne albo przerwane
bloki; 2 — błąd konfiguracji, brak danych albo binarek (przed pierwszym pomiarem)."""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from typing import Callable

from bench import metrics
from bench.config import (
    CONTROL, Block, ConfigError, Scenario, SeriesConfig, blocks, check_data, load, round_order,
)
from bench.data.datasets import data_dir
from bench.engines import (
    BALLISTA_DIR, CONTROL_VARIANT, PROFILES, REFERENCE_VARIANT, REPO, SYSTEM_CPUS, Command,
    cluster_cpus, make_engine, require_ballista_binaries,
)
from bench.procutil import EngineError
from bench.results import ResultsWriter, to_parquet
from bench.validity import Expected, block_drifted, drift, invalid_reasons, parse_report

#: Co ile sekund orkiestrator sprawdza runner (limit czasu, strażnik pamięci).
POLL_S = 0.2
#: Strażnik pamięci: przy MemAvailable poniżej tej wartości przebieg jest przerywany, zanim
#: system zacznie intensywnie swapować albo wywróci WSL.
MEM_FLOOR_BYTES = 300 * 2**20
#: Blok z dryfem przebiegu kontrolnego powtarzany jest raz (specyfikacja 6.3).
MAX_BLOCK_ATTEMPTS = 2

_METRIC_KEYS = (
    "valid", "invalid_reason", "rows", "checksum", "t_total_s", "wall_s", "phases", "extra",
    "peak_rss", "peak_rss_sum", "shuffle_bytes", "broadcast_bytes", "pswpout_delta",
)


@dataclass
class Probe:
    """Źródła metryk systemowych (bench/metrics.py) — w testach podmieniane."""

    mem_available: Callable[[], int] = metrics.mem_available
    pswpout: Callable[[], int] = metrics.pswpout
    peak_rss: Callable[[int], int] = metrics.peak_rss
    reset_peak_rss: Callable[[int], None] = metrics.reset_peak_rss
    dir_size: Callable[[Path], int] = metrics.dir_size


@dataclass
class Outcome:
    """Wynik jednego wykonania runnera."""

    returncode: int
    stdout: str
    stderr: str
    killed: str | None
    wall_s: float


@dataclass
class SeriesSummary:
    directory: Path
    parquet: Path
    rows: int = 0
    invalid: int = 0
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.invalid == 0 and not self.failures


class SeriesError(RuntimeError):
    """Seria przerwana: scenariusz bez wyniku wzorcowego."""


def execute(command: Command, *, timeout_s: float, mem_floor: int, probe: Probe, scratch: Path) -> Outcome:
    """Uruchamia runner z wyjściem do plików — potoki mogłyby zakleszczyć runner zalewający
    stderr — pilnując limitu czasu i strażnika pamięci; zabija całą grupę procesów runnera."""
    out_path, err_path = scratch / "stdout.txt", scratch / "stderr.txt"
    killed: str | None = None
    t0 = time.monotonic()
    with out_path.open("w") as out, err_path.open("w") as err:
        proc = subprocess.Popen(
            command.argv, cwd=command.cwd, env=command.env, stdout=out, stderr=err,
            start_new_session=True,
        )
        try:
            while proc.poll() is None:
                if time.monotonic() - t0 > timeout_s:
                    killed = "timeout"
                else:
                    available = probe.mem_available()
                    if available < mem_floor:
                        killed = (
                            f"strażnik pamięci: MemAvailable {available // 2**20} MiB "
                            f"< {mem_floor // 2**20} MiB"
                        )
                if killed:
                    break
                time.sleep(POLL_S)
        finally:
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            proc.wait()
    return Outcome(
        proc.returncode,
        out_path.read_text(errors="replace"),
        err_path.read_text(errors="replace"),
        killed,
        time.monotonic() - t0,
    )


def git_commit() -> str:
    """HEAD repozytorium; sufiks -dirty, gdy śledzone pliki mają zmiany."""

    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True).stdout.strip()

    head = git("rev-parse", "HEAD") or "nieznany"
    return f"{head}-dirty" if git("status", "--porcelain", "--untracked-files=no") else head


def engine_versions(profile: str) -> dict[str, str]:
    lock = (BALLISTA_DIR / "Cargo.lock").read_text()

    def locked(name: str) -> str:
        m = re.search(rf'name = "{re.escape(name)}"\nversion = "([^"]+)"', lock)
        return m.group(1) if m else "nieznana"

    return {
        "polars_bio": version("polars-bio"),
        "pysail": version("pysail"),
        "pyspark": version("pyspark"),
        "ballista": f"{locked('ballista')} ({profile})",
        "datafusion_ballista": locked("datafusion"),
        "python": platform.python_version(),
    }


def _skipped(reason: str) -> dict:
    return {**dict.fromkeys(_METRIC_KEYS), "valid": False, "invalid_reason": reason}


def _invalidate(rows: list[dict], reason: str) -> list[dict]:
    for row in rows:
        row["valid"] = False
        row["invalid_reason"] = "; ".join(filter(None, [row["invalid_reason"], reason]))
    return rows


def _progress(row: dict) -> str:
    if row["is_reference"]:
        kind = "wzorzec"
    elif row["is_control"]:
        kind = "kontrola"
    elif row["is_warmup"]:
        kind = "rozgrzewka"
    else:
        kind = f"runda {row['rep']}"
    t = f"{row['t_total_s']:9.3f} s" if row["t_total_s"] is not None else "        – s"
    status = "OK" if row["valid"] else f"NIEWAŻNY: {row['invalid_reason']}"
    return f"{row['variant']:<14} N={row['n_nodes']} {kind:<10} {row['scenario_id']:<14} {t}  {status}"


class _Series:
    """Stan jednej serii: konfiguracja, silniki, źródła metryk, zapis i podsumowanie."""

    def __init__(self, cfg, factory, probe, mem_floor, scratch, writer, out, summary, profile):
        self.cfg, self.factory, self.probe, self.mem_floor = cfg, factory, probe, mem_floor
        self.scratch, self.writer, self.out, self.summary = scratch, writer, out, summary
        self.git_commit = git_commit()
        self.versions = json.dumps(engine_versions(profile))

    def row(self, scenario: Scenario, variant: str, n_nodes: int, rep: int, attempt: int,
            measured: dict, *, warmup=False, control=False, reference=False) -> dict:
        return {
            "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
            "git_commit": self.git_commit,
            "engine_versions": self.versions,
            "series": self.cfg.series,
            "seed": self.cfg.seed,
            "scenario_id": scenario.id,
            "op": scenario.op,
            "pair": scenario.data,
            "algorithm": None,
            "variant": variant,
            "n_nodes": n_nodes,
            "rep": rep,
            "attempt": attempt,
            "is_warmup": warmup,
            "is_control": control,
            "is_reference": reference,
            **measured,
        }

    def write(self, rows: list[dict]) -> None:
        for row in rows:
            self.writer.write(row)
            self.summary.rows += 1
            self.summary.invalid += not row["valid"]

    def measure(self, engine, scenario: Scenario, expected: Expected | None) -> tuple[dict, bool]:
        """Jeden przebieg z metrykami (specyfikacja 5). Drugi element: czy silnik trzeba
        uruchomić od nowa (runner zabity albo padł proces silnika)."""
        pids = engine.server_pids()
        for pid in pids.values():
            try:
                self.probe.reset_peak_rss(pid)
            except OSError:
                pass  # martwy proces wykryje odczyt szczytu po przebiegu
        dirs = engine.shuffle_dirs()
        shuffle_before = sum(self.probe.dir_size(d) for d in dirs)
        swap_before = self.probe.pswpout()
        outcome = execute(
            engine.command(scenario), timeout_s=self.cfg.timeout_s, mem_floor=self.mem_floor,
            probe=self.probe, scratch=self.scratch,
        )
        swap_delta = self.probe.pswpout() - swap_before
        report = report_error = None
        if outcome.killed is None and outcome.returncode == 0:
            try:
                report = parse_report(outcome.stdout)
            except ValueError as e:
                report_error = str(e)
        reasons = invalid_reasons(
            returncode=outcome.returncode, killed=outcome.killed, report=report,
            report_error=report_error, expected=expected, pswpout_delta=swap_delta,
            stderr=outcome.stderr,
        )
        peak = {"runner": report["peak_rss_bytes"]} if report else {}
        dead = []
        for name, pid in pids.items():
            try:
                peak[name] = self.probe.peak_rss(pid)
            except (OSError, KeyError):
                dead.append(name)
        if dead:
            reasons.append(f"proces {', '.join(dead)} zakończył się w trakcie przebiegu")
        measured = {
            "valid": not reasons,
            "invalid_reason": "; ".join(reasons) or None,
            "rows": report["rows"] if report else None,
            "checksum": report["checksum"] if report else None,
            "t_total_s": float(report["t_total_s"]) if report else None,
            "wall_s": outcome.wall_s,
            "phases": json.dumps(report["phases"]) if report else None,
            "extra": json.dumps(report["extra"]) if report else None,
            "peak_rss": json.dumps(peak) if peak else None,
            "peak_rss_sum": sum(peak.values()) if peak else None,
            "shuffle_bytes": sum(self.probe.dir_size(d) for d in dirs) - shuffle_before if dirs else None,
            "broadcast_bytes": None,
            "pswpout_delta": swap_delta,
        }
        return measured, bool(outcome.killed or dead)

    def reference_pass(self) -> dict[str, Expected]:
        """Wynik wzorcowy każdego scenariusza i przebiegu kontrolnego: polars-bio na 1 partycji."""
        engine = self.factory(REFERENCE_VARIANT, 1)
        expected: dict[str, Expected] = {}
        engine.start()
        try:
            for scenario in dict.fromkeys([*self.cfg.scenarios, CONTROL]):
                measured, _ = self.measure(engine, scenario, None)
                row = self.row(scenario, REFERENCE_VARIANT, 1, 0, 1, measured, reference=True)
                self.write([row])
                self.out(_progress(row))
                if measured["rows"] is None:
                    raise SeriesError(
                        f"brak wyniku wzorcowego {scenario.id}: {measured['invalid_reason']}"
                    )
                expected[scenario.id] = Expected(measured["rows"], measured["checksum"])
        finally:
            engine.stop()
        return expected

    def run_block(self, block: Block, attempt: int, expected: dict[str, Expected], rows: list[dict]) -> bool:
        """Blok (specyfikacja 6.2): kontrola → rozgrzewka → rundy → kontrola. Wiersze trafiają
        do `rows` na bieżąco (wołający zapisuje je także po przerwaniu). True = dryf."""
        engine = self.factory(block.variant, block.n_nodes)
        control = self.factory(CONTROL_VARIANT, 1)
        skipped: dict[str, str] = {}

        def run(scenario: Scenario, rep: int, *, warmup: bool = False, is_control: bool = False) -> dict:
            if is_control:
                eng, variant, n = control, CONTROL_VARIANT, 1
            else:
                eng, variant, n = engine, block.variant, block.n_nodes
            if not is_control and scenario.id in skipped:
                measured = _skipped(f"pominięty: {skipped[scenario.id]}")
            else:
                measured, restart = self.measure(eng, scenario, expected[scenario.id])
                if restart:
                    if not is_control:
                        skipped[scenario.id] = measured["invalid_reason"]
                    eng.restart()
            row = self.row(scenario, variant, n, rep, attempt, measured, warmup=warmup, control=is_control)
            rows.append(row)
            self.out(_progress(row))
            return row

        engine.start()
        try:
            first = run(CONTROL, 0, is_control=True)
            for w in range(1, self.cfg.warmup + 1):
                for scenario in round_order(self.cfg, block, f"rozgrzewka-{w}"):
                    run(scenario, 0, warmup=True)
            for r in range(1, self.cfg.repeats + 1):
                for scenario in round_order(self.cfg, block, str(r)):
                    run(scenario, r)
            last = run(CONTROL, 1, is_control=True)
        finally:
            engine.stop()
        t_start = first["t_total_s"] if first["valid"] else None
        t_end = last["t_total_s"] if last["valid"] else None
        if not block_drifted(t_start, t_end, self.cfg.control_tolerance):
            return False
        if t_start is not None and t_end is not None:
            reason = f"dryf kontrolny {drift(t_start, t_end):.0%}"
        else:
            reason = "przebieg kontrolny nieważny"
        _invalidate(rows, reason)
        return True

    def run_block_with_retry(self, block: Block, expected: dict[str, Expected]) -> None:
        for attempt in range(1, MAX_BLOCK_ATTEMPTS + 1):
            rows: list[dict] = []
            try:
                drifted = self.run_block(block, attempt, expected, rows)
            except EngineError as e:
                self.write(_invalidate(rows, f"blok przerwany: {e}"))
                self.summary.failures.append(f"{block.label}: {e}")
                self.out(f"{block.label}: blok przerwany — {e}")
                return
            except BaseException:
                self.write(_invalidate(rows, "seria przerwana"))
                raise
            self.write(rows)
            if not drifted:
                return
            last = attempt == MAX_BLOCK_ATTEMPTS
            self.out(f"{block.label}: dryf przebiegu kontrolnego — {'blok nieważny' if last else 'powtarzam blok'}")


def _new_directory(results_dir: Path, series: str) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    for i in itertools.count():
        directory = results_dir / series / (stamp if i == 0 else f"{stamp}-{i}")
        try:
            directory.mkdir(parents=True)
            return directory
        except FileExistsError:
            continue


def run_series(
    cfg: SeriesConfig,
    *,
    data_root: Path,
    results_dir: Path,
    profile: str = "release",
    factory=None,
    probe: Probe | None = None,
    mem_floor: int = MEM_FLOOR_BYTES,
    out: Callable[[str], None] = print,
) -> SeriesSummary:
    """Seria: przebieg wzorcowy, potem bloki (blok z dryfem powtarzany raz). Wiersze trafiają
    do runs.jsonl po każdym bloku, a na końcu — także po przerwaniu — do runs.parquet."""
    directory = _new_directory(results_dir, cfg.series)
    scratch = directory / "tmp"
    scratch.mkdir()
    if factory is None:
        def factory(variant: str, n_nodes: int):
            return make_engine(
                variant, n_nodes, data_root=data_root,
                log_dir=directory / "logs" / f"{variant}-n{n_nodes}", profile=profile,
            )
    summary = SeriesSummary(directory=directory, parquet=directory / "runs.parquet")
    writer = ResultsWriter(directory / "runs.jsonl")
    series = _Series(cfg, factory, probe or Probe(), mem_floor, scratch, writer, out, summary, profile)
    out(f"seria {cfg.series}: wyniki w {directory}")
    try:
        expected = series.reference_pass()
        for block in blocks(cfg):
            series.run_block_with_retry(block, expected)
    finally:
        to_parquet(writer.path, summary.parquet)
        shutil.rmtree(scratch, ignore_errors=True)
    return summary


def preflight(cfg: SeriesConfig, profile: str) -> None:
    """Sprawdzenia przed pierwszym pomiarem: rdzenie emulowanych węzłów i binarki Ballisty."""
    needed = max(cluster_cpus(max(cfg.nodes))) + 1
    if (os.cpu_count() or 0) < needed:
        raise ConfigError(
            f"za mało rdzeni: N = {max(cfg.nodes)} wymaga {needed} wątków, jest {os.cpu_count()}"
        )
    if "ballista" in cfg.variants:
        require_ballista_binaries(profile)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bench.orchestrator",
        description="Seria pomiarowa według konfiguracji YAML (specyfikacja 8).",
    )
    parser.add_argument("config", type=Path, help="plik YAML serii, np. bench/conf/smoke.yaml")
    parser.add_argument(
        "--data-root", type=Path,
        help="katalog nadrzędny databio-8p (domyślnie $BENCH_DATA_ROOT albo ~/bench_data)",
    )
    parser.add_argument("--results-dir", type=Path, default=REPO / "bench" / "results")
    parser.add_argument("--ballista-profile", choices=PROFILES, default="release")
    args = parser.parse_args(argv)
    try:
        cfg = load(args.config)
        root = args.data_root if args.data_root is not None else data_dir().parent
        root = root.expanduser().resolve()
        check_data(cfg, root)
        preflight(cfg, args.ballista_profile)
    except (ConfigError, EngineError, OSError) as e:
        print(f"błąd: {e}", file=sys.stderr)
        return 2
    os.sched_setaffinity(0, SYSTEM_CPUS)
    try:
        summary = run_series(
            cfg, data_root=root, results_dir=args.results_dir.expanduser().resolve(),
            profile=args.ballista_profile,
        )
    except SeriesError as e:
        print(f"seria przerwana: {e}", file=sys.stderr)
        return 1
    print(
        f"przebiegi: {summary.rows}, nieważne: {summary.invalid}, "
        f"przerwane bloki: {len(summary.failures)}; wyniki: {summary.parquet}"
    )
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Uruchom testy jednostkowe, mają przejść**

Run: `python3 -m pytest tests/bench/test_orchestrator.py -v 2>&1 | tail -20`
Expected: 16 passed w około minutę (fałszywy runner to krótki proces Pythona).

- [ ] **Step 5: Test integracyjny na prawdziwych silnikach (najpierw nieudany przez brak pliku, potem zielony)**

`tests/test_orchestrator_integration.py`:

```python
"""Plan 3a, Zadanie 9: orkiestrator na prawdziwych silnikach i zbiorze testowym w układzie
databio-8p (para 1-2, zbiór 1) — wszystkie warianty, N = 1 i 2, po jednym przebiegu.

Wynik wzorcowy: polars-bio na 1 partycji. polars-bio A i B (2 i 4 partycje) liczą merge
i subtract osobno w każdej partycji (błąd polars-bio 0.28) — te przebiegi MUSZĄ wyjść
nieważne przez sumę kontrolną; wszystkie pozostałe — ważne.

Wymaga binarek ballista_node i bench_client (debug). Swap jest wyłączony z kryteriów
(pswpout zastąpiony stałą): jego wykrywanie sprawdzają testy jednostkowe, a tu przypadkowy
swap innej aplikacji nie może dawać fałszywych porażek."""

from __future__ import annotations

import functools
import json
import os

import polars as pl
import pytest
import yaml

from bench import orchestrator as orch
from bench.config import VARIANTS, parse
from bench.data.datasets import DATASETS
from bench.ops import OPS, UNARY_OPS
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts

SCENARIOS = [{"op": op, "dataset": 1} if op in UNARY_OPS else {"op": op, "pair": "1-2"} for op in OPS]
KNOWN_POLARS_BIO_BUG = {
    ("polars_bio_a", 1, "merge"), ("polars_bio_a", 1, "subtract"),
    ("polars_bio_b", 2, "merge"), ("polars_bio_b", 2, "subtract"),
}


@pytest.fixture(scope="module")
def data_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("dane")
    write_parts(FIXTURE_A, root / "databio-8p" / DATASETS[1])
    write_parts(FIXTURE_B, root / "databio-8p" / DATASETS[2])
    return root


@pytest.fixture(scope="module")
def series(data_root, tmp_path_factory):
    cfg = parse({
        "series": "integracja", "scenarios": SCENARIOS, "variants": list(VARIANTS),
        "nodes": [1, 2], "repeats": 1, "warmup": 0, "seed": 1, "control_tolerance": 100.0,
    })
    summary = orch.run_series(
        cfg, data_root=data_root, results_dir=tmp_path_factory.mktemp("wyniki"),
        profile="debug", probe=orch.Probe(pswpout=lambda: 0),
    )
    return summary, pl.read_parquet(summary.parquet)


def test_every_planned_run_is_recorded(series):
    summary, df = series
    # wzorzec: 5 scenariuszy (kontrola overlap/1-2 jest wśród nich); bloki: A1, B2,
    # Ballista 1 i 2, Sail 1 i 2 — każdy 2 przebiegi kontrolne + 5 scenariuszy
    assert df.height == 5 + 6 * (2 + 5)
    assert summary.rows == df.height and not summary.failures


def test_only_known_polars_bio_bug_is_invalid(series):
    _, df = series
    bad = df.filter(~pl.col("valid"))
    assert set(zip(bad["variant"], bad["n_nodes"], bad["op"])) == KNOWN_POLARS_BIO_BUG
    assert all("suma kontrolna" in r for r in bad["invalid_reason"]), bad["invalid_reason"].to_list()


def test_threads_follow_variant_and_n(series):
    _, df = series
    partitions = {
        key: {json.loads(e)["target_partitions"] for e in group["extra"]}
        for key, group in df.filter(pl.col("variant") != "sail").group_by(["variant", "n_nodes"])
    }
    assert partitions == {
        ("polars_bio_ref", 1): {1}, ("polars_bio_a", 1): {2}, ("polars_bio_b", 2): {4},
        ("ballista", 1): {2}, ("ballista", 2): {4},
    }


def test_memory_is_measured_per_process(series):
    _, df = series

    def processes(variant: str, n: int) -> set[frozenset]:
        rows = df.filter((pl.col("variant") == variant) & (pl.col("n_nodes") == n) & ~pl.col("is_control"))
        return {frozenset(json.loads(p)) for p in rows["peak_rss"]}

    assert processes("ballista", 2) == {frozenset({"runner", "scheduler", "executor_1", "executor_2"})}
    assert processes("sail", 1) == {frozenset({"runner", "sail_server"})}
    assert processes("polars_bio_b", 2) == {frozenset({"runner"})}
    assert (df["peak_rss_sum"] > 10 * 2**20).all()


def test_ballista_shuffle_volume_is_measured(series):
    _, df = series
    ballista = df.filter(pl.col("variant") == "ballista")
    assert (ballista.filter(pl.col("op").is_in(["merge", "subtract"]))["shuffle_bytes"] > 0).all()
    assert df.filter(pl.col("variant") != "ballista")["shuffle_bytes"].is_null().all()


def test_main_resolves_relative_data_root(data_root, tmp_path, monkeypatch):
    """Runnery Pythona działają w katalogu repozytorium, a Ballisty w ballista_genomics/ —
    względna ścieżka danych musi zostać zamieniona na bezwzględną przed startem serii."""
    monkeypatch.chdir(data_root.parent)
    monkeypatch.setattr(orch, "Probe", functools.partial(orch.Probe, pswpout=lambda: 0))
    cfg = tmp_path / "seria.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "sciezki", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["polars_bio_a"], "nodes": [1], "repeats": 1, "warmup": 0, "seed": 1,
        "control_tolerance": 100.0,
    }))
    affinity = os.sched_getaffinity(0)
    try:
        code = orch.main([str(cfg), "--data-root", data_root.name, "--results-dir", str(tmp_path / "wyniki")])
    finally:
        os.sched_setaffinity(0, affinity)
    assert code == 0
```

Run: `python3 -m pytest tests/test_orchestrator_integration.py -v 2>&1 | tail -12`
Expected: 6 passed w kilka minut. Wymaga binarek debug z Zadania 3.

Jeśli zawiedzie `test_only_known_polars_bio_bug_is_invalid`, ustal przyczynę ze szczegółów w `invalid_reason` (superpowers:systematic-debugging). Nie poluzowuj zbioru `KNOWN_POLARS_BIO_BUG` bez ruling w ledgerze.

- [ ] **Step 6: Commit**

```bash
git add bench/orchestrator.py tests/bench/fake_runner.py tests/bench/test_orchestrator.py \
  tests/test_orchestrator_integration.py
git commit -m "$(cat <<'EOF'
Plan 3a: orkiestrator serii (wzorzec, bloki, kontrola i dryf, timeout, straznik pamieci)

Wynik wzorcowy: polars-bio na 1 partycji. Blok: kontrola, rozgrzewka, rundy w
kolejnosci z ziarna, kontrola; dryf powtarza blok raz. Pomiary: czas z runnera,
VmHWM procesow (clear_refs), pswpout, wolumen shuffle. Po timeoucie, strazniku
albo smierci procesu silnik jest restartowany, a scenariusz pomijany do konca
bloku. Zapis JSON Lines na biezaco i Parquet na koncu (takze po przerwaniu).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Smoke na parze 1-2 i dokumentacja

**Files:**
- Modify: `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` (sekcje 3, 6, 8.1, 8.3–8.6, 12)
- Modify: `ballista_genomics/OPIS.md` (nowa sekcja na końcu)
- Modify: `wnioski_claude.md` (nowa sekcja na końcu)
- Add: `raporty/polars_bio_blad_partycji.md` (notatka o błędzie #372, napisana 01.10.2026 przed wykonaniem planu)
- Poza repozytorium: pamięć `project_mgr.md` i `MEMORY.md`

**Interfaces:**
- Consumes: całe narzędzie (Zadania 1–9), `bench/conf/smoke.yaml`, dane `~/bench_data/databio-8p` (zbiory 1 i 2).
- Produces: potwierdzenie działania narzędzia na prawdziwych danych oraz dokumentację ustaleń.

- [ ] **Step 1: STOP — checklista RAM przed danymi rzeczywistymi**

Przypomnij użytkownikowi checklistę z pamięci `feedback_ram_przed_eksperymentem` i **poczekaj na potwierdzenie**:
1. Zamknąć aplikacje w tle: SteelSeries GG, Bitwarden; wstrzymać OneDrive.
2. `.wslconfig` (5 GB i 4 GB swapu) aktywny.
3. Przebieg, w którym system sięgnął do swapu, jest nieważny.

Smoke sprawdza narzędzie, a nie daje wyników pracy (Ballista w wersji debug), więc VS Code może zostać otwarty. Jeśli użytkownik woli, polecenia z Kroków 3–4 może uruchomić sam z Windows Terminal.

- [ ] **Step 2: Sprawdź pamięć**

Run: `free -h && grep -E '^(pswpin|pswpout) ' /proc/vmstat`
Expected: `Mem: total` około 4,8 Gi, `Swap: total` 4,0 Gi.

- [ ] **Step 3: Test akceptacyjny na parze 1-2 z nowym protokołem**

Run: `python3 -m pytest tests/test_real_data.py -v 2>&1 | tail -18`

Expected: 13 passed, 2 xfailed. `check_result` sprawdza teraz także sumę kontrolną `bench_client` względem wyroczni na prawdziwych danych. Ballista standalone dla nearest i coverage nadal daje xfail.

- [ ] **Step 4: Smoke**

Run (w tle, timeout 7200 s; `SCRATCH` to katalog brudnopisu sesji):

```bash
cd /home/milosz/praca_magisterska && python3 -m bench.orchestrator bench/conf/smoke.yaml \
  --ballista-profile debug --results-dir "$SCRATCH/smoke" > "$SCRATCH/smoke.log" 2>&1; echo "kod: $?"
```

Expected: `kod: 1`. Ostatnia linia logu: `przebiegi: 68, nieważne: 6, przerwane bloki: 0`. Wiersze:
- wzorzec: 5;
- 9 bloków × (2 przebiegi kontrolne + 5 scenariuszy).

- [ ] **Step 5: Przeanalizuj wynik smoke**

```bash
python3 - "$SCRATCH/smoke" <<'EOF'
import sys, pathlib
import polars as pl
pl.Config.set_tbl_rows(100)
df = pl.read_parquet(next(pathlib.Path(sys.argv[1]).rglob("runs.parquet")))
print(df.filter(~pl.col("valid")).select("variant", "n_nodes", "scenario_id", "invalid_reason"))
print(df.filter(~pl.col("is_reference") & ~pl.col("is_control"))
        .select("variant", "n_nodes", "op", "t_total_s", "peak_rss_sum", "shuffle_bytes")
        .sort("variant", "n_nodes", "op"))
EOF
for n in 1 2 3; do
  echo "sail N=$n: $(grep -ohE 'worker [0-9]+ server is ready' "$SCRATCH"/smoke/smoke/*/logs/sail-n$n/sail_server.log | sort -u | wc -l) workerów"
done
```

Expected:
- nieważne wyłącznie `polars_bio_a` (N = 1) i `polars_bio_b` (N = 2, 3) dla `merge/1` i `subtract/1-2`, czyli 6 wierszy z przyczyną „liczba wierszy …” lub „suma kontrolna …”;
- Ballista i Sail ważne dla wszystkich operacji i N;
- Sail ma 1, 2 i 3 workery.

Inne przyczyny wymagają superpowers:systematic-debugging przed dalszymi krokami: błąd silnika, timeout, swap, strażnik pamięci albo więcej workerów Saila niż N.

Wyjątek: dryf kontrolny (bloki powtórzone, „dryf kontrolny …”). Zapisz wtedy Ruling z obserwacją, że przebieg kontrolny na 1-2 może być za krótki na regułę 10%. Decyzja o długości przebiegu kontrolnego należy do planu 3b.

Liczby z tego kroku sprawdzają narzędzie i nie są wynikami pracy (Ballista w wersji debug). Nie commituj ich.

- [ ] **Step 6: Zaktualizuj specyfikację**

W `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`:

1. Sekcja 3, tabela „Warianty w konfiguracji N” — zastąp cztery wiersze:

```markdown
| **polars-bio A** (węzeł tej samej wielkości) | jeden proces na wątkach węzła 1 (2 wątki: `target_partitions = 2`, `POLARS_MAX_THREADS = 2`); niezależny od N |
| **polars-bio B** (maszyna = cały klaster) | jeden proces na wątkach węzłów 1..N (2N wątków, `target_partitions = 2N`); dla N = 1 tożsamy z A |
| **Ballista** | scheduler (CPU 0–1) + N executorów, executor *i* na wątkach węzła *i*, 2 sloty zadań na executor; `BIO_TARGET_PARTITIONS = 2N` w każdym procesie klastra i w kliencie |
| **Sail** | jeden proces serwera (`local-cluster`, N workerów, równoległość 2N) na wątkach węzłów 1..N — te same zasoby co klaster, ale bez rozproszenia i z blokadą `sail_pb_guard` (asymetria opisana jawnie); 8 slotów na workera, bo przy 2 Sail sam dokłada workery ponad N, a z limitem `worker_max_count` zapytanie wisi (sonda 01.10.2026) |
```

   Pod zdaniem „Który z wariantów A/B jest punktem odniesienia głównym — do ustalenia na dalszym etapie; mierzone są oba.” dopisz akapit:

```markdown
**Wynik wzorcowy (plan 3a).** Każdy pomiar jest sprawdzany względem polars-bio na **1 partycji**
(wariant `polars_bio_ref`, przebieg na początku serii). Przy `target_partitions > 1` polars-bio
0.28 liczy `merge` i `subtract` osobno w każdej partycji — wynik jest błędny, gdy przedziały
chromosomu leżą w różnych plikach. To znany błąd polars-bio #372, naprawiony w 0.29.0, która
wymaga Pythona ≥ 3.11 (system: 3.10); opis: `raporty/polars_bio_blad_partycji.md`. Punkt
odniesienia dla tych operacji w P1 (decyzja z 01.10.2026): polars-bio na 1 partycji, mierzony
dodatkowo; przebiegi A/B z 2 i 2N partycjami zostają w wynikach jako nieważne.
```

2. Sekcja 6:
   - W punkcie 3 zastąp „Rozjazd > 10% między początkiem a końcem bloku ⇒ blok do powtórzenia.” tekstem: „Rozjazd > 10% (`control_tolerance`) między początkiem a końcem bloku ⇒ blok powtarzany raz; po drugim dryfie przebiegi bloku zostają nieważne.”
   - W punkcie 4 zastąp „Każdy pomiar jest zarazem testem poprawności względem polars-bio A.” tekstem: „Każdy pomiar jest zarazem testem poprawności względem wyniku wzorcowego (polars-bio na 1 partycji, sekcja 3).”
   - Na końcu punktu 5 dopisz: „Ponadto: zabicie przez strażnika pamięci (`MemAvailable` < 300 MiB) i śmierć procesu silnika w trakcie przebiegu. Po timeoucie, strażniku albo śmierci procesu silnik jest restartowany, a scenariusz pomijany do końca bloku („pominięty: …”).”

3. Sekcja 8.1 — zastąp blok struktury:

```
bench/
  conf/            smoke.yaml (plan 3a); kalibracja.yaml, p1.yaml, p2.yaml, p3.yaml (plan 3b)
  config.py        konfiguracja serii: walidacja, bloki, kolejność z ziarna
  orchestrator.py  sterowanie blokami, pomiary, walidacja, zapis wyników
  engines.py       silniki bloku: przypięcie do rdzeni, klaster Ballisty, serwer Saila
  validity.py      reguły ważności przebiegu i dryfu
  results.py       zapis wyników (JSON Lines → Parquet)
  metrics.py       /proc: VmHWM, clear_refs, pswpout, MemAvailable; rozmiar katalogów shuffle
  checksum.py      suma kontrolna (implementacja referencyjna)
  procutil.py      porty i gotowość procesów silników
  runners/
    polars_bio_runner.py
    sail_runner.py
    sail_server.py serwer Saila na czas bloku
  data/download.py pobranie databio-8p (poza gitem)
  analyze.py       wyniki -> tabele i wykresy (po pomiarach)
  results/         surowe wyniki (Parquet) + plany EXPLAIN ANALYZE (w repozytorium)
ballista_genomics/src/bin/bench_client.rs   runner Ballisty (Rust)
tests/bench/       testy narzędzia
```

   W akapicie pod blokiem zastąp „czas, fazy i suma kontrolna — plan 3.” tekstem: „w planie 3a doszły czas, suma kontrolna (`checksum.rs`), szczyt pamięci i `BIO_TARGET_PARTITIONS`; fazy — plan 3b.”

4. Sekcja 8.3 — w przykładzie zastąp `"extra": {"broadcast_bytes": 0}}` przez `"extra": {"target_partitions": 4}, "peak_rss_bytes": 104857600}`. Pod przykładem dopisz: „`peak_rss_bytes` — VmHWM procesu runnera (licznik zerowany tuż przed zapytaniem); szczyty procesów długożyjących (scheduler, executory, serwer Saila) mierzy orkiestrator. `extra.broadcast_bytes` — plan 3b.”

5. Sekcja 8.4:
   - W tabeli zastąp wiersz overlap przez `| overlap | chrom₁, start₁, end₁, chrom₂, start₂, end₂ (jak KEY_COLUMNS; chrom₂ = chrom₁) |`.
   - Pod akapitem o nazwach kolumn dopisz: „Definicja (plan 3a, `bench/checksum.py` i `checksum.rs`): kod chromosomu — CRC-32 (IEEE) z UTF-8; kod liczby — wartość modulo 2⁶⁴; brak wartości — 2⁶⁴ − 1; skrót wiersza — `mix64(Σ M_j · kod_j)` (stałe nieparzyste M_j, finalizator splitmix64); suma — Σ skrótów modulo 2⁶⁴, zapisana jako `0x` + 16 cyfr szesnastkowych. Skrót wiersza musi być nieliniowy: sama suma liniowa zależałaby tylko od sum kolumn.”

6. Sekcja 8.5 — na końcu dopisz: „Plan 3a dodaje `is_reference` (przebieg wzorcowy), `attempt` (2 — powtórzenie bloku po dryfie), `wall_s` (czas procesu runnera ze startem, do szacowania długości serii) i `extra` (JSON z runnera, m.in. `target_partitions`). Zapis: `bench/results/<seria>/<znacznik czasu>/runs.jsonl` na bieżąco i `runs.parquet` na końcu, także po przerwaniu serii.”

7. Sekcja 8.6 — zastąp punkt o teście integracyjnym: „integracyjny: orkiestrator na zbiorze testowym w układzie databio-8p, wszystkie warianty, N = 1 i 2 (`tests/test_orchestrator_integration.py`, kilka minut); smoke na danych 1-2 — `python -m bench.orchestrator bench/conf/smoke.yaml` (plan 3a: nieważne wyłącznie przebiegi polars-bio A/B `merge` i `subtract`);”.

8. Sekcja 12 — dopisz punkt: „nowe środowisko z Pythonem ≥ 3.11 i aktualnym polars-bio (0.36.0: poprawka #372, DataFusion 53 jak w Ballistcie) — przed planem 3b; wariant: uv (projektowe `.venv`, `uv.lock`) albo globalny drugi Python — do wyboru.”

- [ ] **Step 7: Dokumentacja Ballisty i obserwacje**

Na końcu `ballista_genomics/OPIS.md` dopisz:

```markdown
## Plan 3a — runner pomiarowy (01.10.2026)

- `bench_client` wypisuje pełny protokół runnera (specyfikacja 8.3):
  - `rows`, `checksum`;
  - `t_total_s` (od `ctx.sql` do ostatniej partii, bez połączenia z klastrem);
  - `phases` (puste; fazy w planie 3b);
  - `extra.target_partitions`;
  - `peak_rss_bytes` (VmHWM klienta, licznik zerowany przed zapytaniem).
- Suma kontrolna (`src/checksum.rs`) ma definicję wspólną z `bench/checksum.py`. Zgodność
  sprawdza tryb `bench_client --op OP --checksum PLIK` na wartościach wzorcowych
  (`tests/test_bench_client_protocol.py`).
- `BIO_TARGET_PARTITIONS` (liczba ≥ 2, domyślnie 4) ustala `target_partitions` wszystkich
  sesji: klienta, schedulera, executorów i sesji wewnętrznych providera. Orkiestrator
  pomiarów ustawia 2N w każdym procesie klastra i w kliencie. Niepoprawna wartość daje
  kod 2 w `bench_client` i `ballista_node`.
- W pomiarach klaster uruchamia orkiestrator (`bench/engines.py`):
  - scheduler na CPU 0–1;
  - executor k na CPU {2k, 2k+1}, 2 sloty;
  - katalogi robocze w katalogu tymczasowym; ich przyrost w przebiegu to `shuffle_bytes`.
```

Na końcu `wnioski_claude.md` dopisz:

```markdown
## Plan 3a — obserwacje z budowy narzędzia pomiarowego (01.10.2026)

1. **polars-bio 0.28 zwraca błędne `merge` i `subtract` przy `target_partitions > 1`.**
   - Operatory liczą każdą partycję osobno i nie deklarują wymaganego rozkładu wejścia (np.
     haszowania po chromosomie). Przedziały chromosomu leżące w różnych plikach nie są więc
     scalane ani odejmowane.
   - Domyślnie polars-bio liczy na 1 partycji i wtedy wynik jest poprawny. Udokumentowany
     przełącznik równoległości (`pb.POLARS_BIO_MAX_THREADS`, czyli `target_partitions`)
     zmienia jednak wynik tych dwóch operacji.
   - `overlap`, `nearest` i `coverage` są poprawne przy każdej liczbie partycji.
   - Ballista liczy te operacje poprawnie dzięki repartycji po chromosomie w `DistBioProvider`.
   - To znany błąd upstream (polars-bio #372), naprawiony w 0.29.0. Ta wersja wymaga
     Pythona ≥ 3.11, a system ma 3.10 — stąd w projekcie 0.28.0. Opis i minimalny przykład:
     `raporty/polars_bio_blad_partycji.md`.
   - Skutki dla pomiarów: wzorcem jest polars-bio na 1 partycji, a przebiegi A/B
     `merge`/`subtract` wychodzą nieważne. Punkt odniesienia w P1 (decyzja z 01.10.2026):
     polars-bio na 1 partycji, mierzony dodatkowo.
   - Dowód: `tests/test_polars_bio_runner.py` (xfail strict).
2. **Sail w trybie `local-cluster` sam dokłada workery.** Gdy etap ma więcej zadań niż
   slotów, Sail uruchamia workery ponad `worker_initial_count`. Z limitem `worker_max_count`
   zapytanie wisi, zamiast czekać na wolne sloty. Emulacja N węzłów używa więc 8 slotów na
   workera (wtedy workerów jest dokładnie N), a o równoległości decyduje przypięcie procesu
   do 2N rdzeni. To uzupełnia obserwację o dojrzałości ścieżki UDTF w Sailu.
3. **Import polars-bio trwał ~270 s przez serwer X, nie przez polars-bio.** Matplotlib
   (importowany przez polars-bio) sprawdzał ekran z `DISPLAY`. Zmienną ustawia
   `/etc/bash.bashrc` na host Windows, na którym nie działa serwer X, więc import czekał na
   timeout TCP. Z `MPLBACKEND=Agg` import trwa ~1 s. Wcześniejsze uwagi o „kilku minutach”
   importu dotyczą tego zjawiska.
```

- [ ] **Step 8: Pełny pakiet testów**

Run: `python3 -m pytest tests/ -q 2>&1 | tail -6`

Expected: wszystkie zielone i 4 xfailed:
- 2 × Ballista standalone na danych 1-2;
- 2 × polars-bio z 2 partycjami dla `merge` i `subtract`.

Liczba passed to dotychczasowe 150 plus nowe testy tego planu.

- [ ] **Step 9: Commit**

```bash
git add docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md ballista_genomics/OPIS.md wnioski_claude.md \
  raporty/polars_bio_blad_partycji.md
git commit -m "$(cat <<'EOF'
Plan 3a: dokumentacja narzedzia pomiarowego i obserwacji (blad polars-bio przy wielu partycjach)

Specyfikacja: wynik wzorcowy na 1 partycji, protokol runnera z peak_rss_bytes,
definicja sumy kontrolnej, nowe pola wynikow, konfiguracja Saila (8 slotow).
Obserwacje: merge/subtract w polars-bio przy target_partitions > 1, skalowanie
workerow w Sailu, import polars-bio czekajacy na serwer X.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 10: Pamięć projektu**

W `/home/milosz/.claude/projects/-home-milosz-praca-magisterska/memory/project_mgr.md` zastąp akapit „NASTĘPNY KROK” opisem stanu:
- plan 3a scalony;
- narzędzie: `python -m bench.orchestrator`;
- znaleziska (polars-bio przy wielu partycjach, Sail i workery, `MPLBACKEND`);
- następny krok: plan 3b, czyli limity gRPC i `broadcast_bytes`, build release, fazy, konwersja UDTF Saila, `algorithm`, kalibracja, wariant polars-bio na 1 partycji (decyzja c). PRZED 3b: nowe środowisko z Pythonem ≥ 3.11 i polars-bio 0.36 (uv albo globalnie — do wyboru).

Zaktualizuj opis w `MEMORY.md`.

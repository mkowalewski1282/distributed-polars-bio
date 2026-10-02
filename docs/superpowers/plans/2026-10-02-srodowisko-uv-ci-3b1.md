# Plan 3b-1 — środowisko uv, aktualne wersje, wyrównanie algorytmów i CI: plan implementacji

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Odtwarzalne jednym poleceniem środowisko (uv, Python 3.12) z najnowszymi wersjami bibliotek, ta sama wersja algorytmów przedziałowych w polars-bio i Ballistcie, kod w spójnej konwencji językowej i testy uruchamiane automatycznie w GitHub Actions — zanim ruszy jakakolwiek seria pomiarowa.

**Architecture:** Migracja etapami, jedna zmiana naraz (podejście A ze specyfikacji):
1. uv na dzisiejszych wersjach;
2. polars-bio 0.36;
3. vendor `datafusion-bio-function-ranges` v0.22.2;
4. Sail 0.7.2 i PySpark 4.2;
5. pozostałe biblioteki;
6. konwencja językowa ze strażnikiem;
7. CI.

Po każdym etapie pełny pakiet testów bez danych rzeczywistych jest zielony, a zmiana trafia do osobnego commitu (zmiana wersji widoczna w diffie `uv.lock`). Na końcu weryfikacja na danych rzeczywistych (po checkliście RAM) i dokumentacja.

**Tech Stack:**
- uv 0.12.22, Python 3.12 (pobierany przez uv);
- start: polars-bio 0.28.0, pysail 0.5.3, pyspark 4.1.1, pytest 6.2.5;
- cel: polars-bio 0.36, pysail 0.7.2, PySpark 4.2.0, numpy 2, pandas 3, pytest 9;
- Rust 1.95.0, Ballista 53.0.0, DataFusion `=53.0.0`;
- GitHub Actions: `actions/checkout@v7`, `Swatinem/rust-cache@v2`, `astral-sh/setup-uv@v10`.

**Spec:** `docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md` (cała). Kontekst: specyfikacja metodyki `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`, plan 3a `docs/superpowers/plans/2026-10-01-narzedzie-pomiarowe-3a.md`, `ballista_genomics/vendor/PATCH.md`, `raporty/polars_bio_blad_partycji.md`.

## Global Constraints

- **Środowisko:**
  - uv 0.12.22 w `~/.local/bin`, bez `sudo`, Python 3.12 pobierany przez uv;
  - `pyproject.toml` z `[tool.uv] package = false`, `requires-python = ">=3.12,<3.13"`, `.python-version` = `3.12`;
  - `uv.lock` w repozytorium, `.venv/` w `.gitignore`;
  - systemowy `python3` (3.10) i biblioteki w `~/.local` zostają **nietknięte**.
- **Wersje:**
  - silniki przypięte do wersji minor (`polars-bio==0.36.*`, `pysail==0.7.*`, `pyspark[connect]==4.2.*`);
  - pozostałe zależności mają po etapie 5 tylko dolne granice; dokładne wersje są w `uv.lock`.
- **Ballista i vendor:**
  - Ballista `53.0.0`, DataFusion `=53.0.0` bez zmian;
  - bez forkowania i modyfikowania Ballisty, Saila i polars-bio;
  - vendor = upstream v0.22.2 plus wyłącznie łatki widoczności (`pub`, re-eksporty, komentarze).
- **Rust:**
  - Rust 1.95.0 z `ballista_genomics/rust-toolchain.toml`;
  - lokalnie **zawsze** `CARGO_BUILD_JOBS=1`;
  - **nigdy** nie uruchamiać Pythona z polars-bio równolegle z budowaniem Rusta.
- **Testy:**
  - polecenie: `uv run pytest …`;
  - testy na danych rzeczywistych wyłącza w Zadaniach 1–5 `-m "not dane"`, a od Zadania 6, po zmianie znacznika, `-m "not real_data"`;
  - testów na danych rzeczywistych nie uruchamiać przed Zadaniem 11.
- **Długie polecenia:** `uv sync` z budowaniem PySparka i `cargo build` trwają od kilku do kilkudziesięciu minut. Uruchamiaj je w tle albo z odpowiednim limitem czasu narzędzia.
- **Zmienne powłoki:** nie przechodzą między krokami (każde polecenie to nowa powłoka). Kroki, które ich potrzebują, ustawiają je same.
- **Zasada zatrzymania (spec, sekcja 4):** jeśli etap 2–5 odsłoni poważny problem (np. Sail 0.7 psuje UDTF, polars-bio 0.36 zmienia semantykę wyniku, pandas 3 psuje UDTF-y Saila), zatrzymaj się i zapytaj użytkownika: zostać przy starszej wersji tej biblioteki czy naprawiać.
- **Konwencja językowa (spec, sekcja 5) obowiązuje KAŻDY nowy i zmieniany kod od Zadania 1:**
  - po angielsku wszystko poza komentarzami: identyfikatory, komunikaty, opisy w CLI, wartości w wynikach, nazwy plików i katalogów w testach;
  - komentarze i docstringi po polsku;
  - dokumentacja `*.md` po polsku.
- **Commity:**
  - komunikaty po polsku bez znaków diakrytycznych, zakończone linią `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`;
  - **żadnego pusha bez zgody użytkownika** (Zadanie 10 pyta).
- **Checklista RAM:** przed Zadaniem 11 (dane rzeczywiste i smoke) przypomnij ją z pamięci `feedback_ram_przed_eksperymentem` i poczekaj na potwierdzenie.
- **CI:** służy wyłącznie sprawdzaniu poprawności, nigdy pomiarom.
- **Repozytorium:**
  - nic o komunikacji z promotorem (metryczka „promotor: dr inż. Marek Wiewiórka” jest dozwolona);
  - `ustalenia_zalozen.txt` nigdy nie trafia do repozytorium;
  - błędu polars-bio #372 nie zgłaszać.

## Review Focus

Wejścia i warunki, których specyfikacja nie wymienia wprost, a które najłatwiej ugryzą użytkownika (najbardziej prawdopodobne na początku):

1. **Uruchomienie narzędzia albo testów złym interpreterem.** Chodzi o odruchowe `python3 -m bench.orchestrator …` (systemowy 3.10), który widzi w `~/.local` polars-bio 0.28 z błędem #372 i Sail 0.5.3.
   - Oczekiwane zachowanie: natychmiastowy, czytelny błąd zamiast serii po cichu liczonej starymi wersjami.
   - Testy: `test_bench_rejects_a_foreign_python`, `test_bench_accepts_the_project_python` (Zadanie 1).
2. **Rozjazd wersji algorytmów.** Przykład: `uv lock --upgrade` podnosi polars-bio w obrębie `0.36.*` do wydania z innym tagiem `datafusion-bio-functions`, a vendor Ballisty zostaje stary.
   - Oczekiwane zachowanie: czerwony test z poleceniem sprawdzenia Cargo.toml polars-bio.
   - Test: `test_ballista_uses_the_ranges_crate_version_of_polars_bio` (Zadanie 3).
3. **Maszyna z mniejszą liczbą CPU niż węzły w teście.** Przykład: runner CI z 4 vCPU przy teście N = 2.
   - Oczekiwane zachowanie: pominięcie z podanym powodem, a nie błąd `taskset`. Test, który przypina procesy, a nie ma znacznika, ma wyjść na jaw lokalnie, przed CI.
   - Testy: `tests/test_nodes_marker.py` (Zadanie 10) i lokalna próba CI pod `taskset -c 0-3`.
4. **Świeży klon bez lokalnych artefaktów** (brak `~/bench_data`, brak `ballista_genomics/output/`, brak bibliotek w `~/.local`).
   - Oczekiwane zachowanie: testy przechodzą albo są pomijane; żaden nie wywraca się przy zbieraniu.
   - Test: próba CI z pustym `BENCH_DATA_ROOT` (Zadanie 10), potem prawdziwe CI.
5. **Przypadki brzegowe strażnika języka.** Chodzi o:
   - `//` wewnątrz napisu Rusta (`"df://localhost"`), literał znakowy `'"'`, surowe napisy `r#"…"#`, zagnieżdżone `/* */`;
   - f-stringi Pythona 3.12 (osobne tokeny `FSTRING_MIDDLE`);
   - docstringi.

   Oczekiwane zachowanie: komentarze i docstringi pomijane, kod sprawdzany.
   - Testy: sześć testów jednostkowych skanera w `tests/test_code_language.py` (Zadanie 6).

## Decyzje planu wykraczające poza literę specyfikacji

Każda jest rozstrzygnięciem wykonawczym. Wykonawca stosuje je bez pytania; użytkownik widzi je przy przeglądzie planu.

1. **Praca na gałęzi `3b-1-uv-ci` w głównym katalogu repozytorium, nie w osobnym worktree.**
   - Worktree miałby własny `ballista_genomics/target`, czyli zimne budowanie Rusta (1–2 h przy `CARGO_BUILD_JOBS=1`) i kilka GB na dysku.
   - Gałąź w miejscu jest izolacją wystarczającą do scalenia na końcu.
2. **Oczekiwany wynik etapu 1 to część wyniku z Pythona 3.10 bez danych rzeczywistych:** 330 passed + 2 xfailed (z 343 + 4). Dane rzeczywiste sprawdza dopiero Zadanie 11, po checkliście RAM.
3. **`bench` odmawia importu pod innym Pythonem niż 3.12** (Review Focus 1). Wersje silników każdego przebiegu i tak trafiają do kolumny `engine_versions`, ale błąd przed serią jest tańszy niż seria do wyrzucenia.
4. **`rust-toolchain.toml` dochodzi w Zadaniu 3, nie w Zadaniu 10.** Etap 3 jest pierwszą przebudową Rusta; ewentualna pełna przebudowa po zmianie nazwy toolchainu (`stable` na `1.95.0`, ten sam kompilator) zdarzy się raz.
5. **Strażnik języka pomija katalog `raporty/`** (obok `ballista_genomics/vendor/`). `raporty/img/generuj_rysunki.py` rysuje polskie podpisy rysunków do polskich raportów — to treść dokumentu, nie komunikaty programu; nazwa pliku też zostaje.
6. **matplotlib nie wchodzi do środowiska** (specyfikacja wylicza zależności). Rysunki generuje się przez `uv run --with matplotlib python raporty/img/generuj_rysunki.py`, co README opisuje.
7. **Dodatki w `pyproject.toml`:**
   - `[tool.uv] required-version = "==0.12.22"`: ta sama wersja uv lokalnie i w CI; `setup-uv` czyta ją z `pyproject.toml`;
   - `environments = ["sys_platform == 'linux'"]`: projekt działa tylko na Linuksie (`taskset`, `/proc`);
   - `[tool.pytest.ini_options] testpaths = ["tests"]`: bez tego pytest bez argumentów przegląda też `ballista_genomics/target`.
8. **Dodatki w CI:**
   - `cargo build --bins --locked` i `uv run --locked`;
   - `Swatinem/rust-cache` z `cache-on-failure: true`, żeby pierwsze, czerwone uruchomienia też zapisywały cache budowania;
   - `permissions: contents: read` i krok z zajętością dysku.

   `CARGO_PROFILE_DEV_DEBUG=0` zostaje zgodnie ze specyfikacją, choć `Cargo.toml` już ma `[profile.dev] debug = false`. Lokalne 22 GB w `target/` to głównie nagromadzone stare artefakty w `target/debug/deps` (21 GB), nie debuginfo — sprostowanie trafia do dokumentacji w Zadaniu 11.
9. **pandas 3 a PySpark 4.2.**
   - PySpark 4.2.0 (`require_minimum_pandas_version`) ostrzega: „PySpark does not yet fully support pandas >= 3.0.0”. Testy samego pysail 0.7.2 przypinają `pandas<3`.
   - Etap 5 próbuje pandas 3 zgodnie ze specyfikacją.
   - Błąd w testach Saila oznacza zasadę zatrzymania z gotową opcją `pandas>=2.3,<3`.
10. **Etykieta ziarna rozgrzewki `rozgrzewka-{w}` zmienia się na `warmup-{w}` (Zadanie 7).** Dla danego ziarna zmienia to kolejność scenariuszy w rozgrzewce. To dopuszczalne: nie ma jeszcze żadnej serii pomiarowej, tylko smoke.
11. **`uv lock --upgrade` w etapie 5 może podnieść wersje patch silników** w obrębie przypięć minor. Diff `uv.lock` to pokaże, a dla polars-bio pilnuje tego test wersji algorytmów (Zadanie 3).
12. **CI sprawdzane przez pull request z gałęzi.** Wyzwalacze ze specyfikacji to push/PR do `master` i ręczne uruchomienie. Push wymaga zgody użytkownika; `gh` nie jest zainstalowane, więc PR otwiera użytkownik w przeglądarce, a stan CI czyta publiczne API GitHuba.

## Fakty sprawdzone przy pisaniu planu (02.10.2026)

**PyPI:**
- polars-bio 0.36.0:
  - pakiet `cp310-abi3`, Python `>=3.11,<3.15`;
  - zależności: `polars>=1.37.1`, `pyarrow>=23.0.1,<25`, `datafusion>=53.0.0,<54` (pakiet Pythona), `tqdm`, `polars-config-meta`;
  - pandas tylko w dodatku `pandas`, matplotlib tylko w dodatku `viz`.
- polars-bio 0.28.0: `cp39-abi3`, `datafusion>=50,<51`, `pyarrow>=21,<23`.
- pysail 0.7.2: `abi3`, bez wymaganych zależności.
- PySpark 4.2.0:
  - tylko sdist, 450 MB; uv buduje z niego pakiet;
  - dodatek `connect`: `pandas>=2.2.0`, `pyarrow>=18`, `grpcio>=1.76`, `grpcio-status>=1.76`, `googleapis-common-protos>=1.71`, `zstandard>=0.25`, `numpy>=1.21`.
- Najnowsze wersje pozostałych:
  - numpy 2.5.3 (`>=3.12`), pandas 3.0.6, pytest 9.1.1, polars 1.44.2;
  - pyarrow 25.0.1, ale polars-bio ogranicza do <25, więc wchodzi 24.x;
  - datafusion (Python) 54.0.0, ale polars-bio ogranicza do <54, więc wchodzi 53.0.0;
  - requests 2.34.2, PyYAML 6.0.3, uv 0.12.22.
- numpy 1.24.0 nie ma pakietu dla Pythona 3.12. Pakiety dla 3.12 mają: datafusion 50.1.0 (abi3), pyarrow 22.0.0, pandas 2.3.3, PyYAML 6.0.2, grpcio 1.80.0.

**polars-bio, API Pythona 0.28 → 0.36 (porównanie źródeł z pakietów):**
- sygnatury `overlap`, `nearest`, `coverage`, `merge`, `subtract` bez zmian; `overlap` doszły parametry `overlap_output="join"` i `distinct_output=False`, których wartości domyślne zachowują dotychczasowe zachowanie;
- bez zmian: `POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"`, opcja `datafusion.bio.coordinate_system_zero_based`, `polars_bio.context.ctx.deregister_table`, obsługa `attrs` pandas i `config_meta` polars.

**`datafusion-bio-functions` v0.22.2:**
- to wersja z Cargo.toml polars-bio pod tagiem `0.36.0`: `datafusion-bio-function-ranges = { git = …, tag = "v0.22.2" }`, DataFusion `=53.0.0` jak u nas;
- crate leży w `datafusion/bio-function-ranges/` (workspace), a zależności workspace są identyczne jak rozwinięte w naszej kopii.
- Pliki objęte łatkami:
  - `lib.rs`, `physical_planner/mod.rs`, `merge.rs` i `count_overlaps.rs` są identyczne w 0.18.0 i 0.22.2;
  - `nearest.rs` i `subtract.rs` zmieniły się poza łatanymi miejscami;
  - struktury `MergeExec`, `SubtractExec`, `NearestExec`, `CountOverlapsExec` mają te same pola.
- Upstream v0.22.2 nadal ma prywatne `mod intervals;` i nie udostępnia węzłów, więc obie łatki są potrzebne.
- API używane przez integrację nie zmieniło się: `IntervalJoinExec::try_new`, `build_nearest_indexes`, `build_coitree_from_batches`, `build_count_index_from_batches`, `create_bio_session`, `BioConfig`, `BioSessionExt`.
- `superintervals`: źródła identyczne, wersja 0.20.0 → 0.24.2.
- Zmiany upstreamu w wykonaniu:
  - przedziały jednozasadowe przy współrzędnych 0-based (`nearest`, `count_overlaps`, `coverage`);
  - `subtract` i `complement` przy współrzędnych 1-based.

  Zbiór testowy nie ma przedziałów jednozasadowych.

**Sail 0.7.2:**
- klucze `mode`, `cluster.worker_initial_count`, `cluster.worker_task_slots`, `cluster.worker_max_idle_time_secs` i `execution.default_parallelism` istnieją z tymi samymi wartościami domyślnymi co w 0.5.3;
- `pysail.spark.SparkConnectServer` ma identyczne API;
- komunikaty logu `creating session {id}` i `worker {id} server is ready on port {port}` nadal istnieją;
- w 0.7.2 serwer drivera jest wspólny (`driver server is ready` raz, przy starcie), a pula workerów należy do drivera sesji. To, czy pula nadal powstaje na sesję, rozstrzyga sonda w Zadaniu 4.

**PySpark 4.2.0:**
- `SparkConnectClient.to_table_as_iterator(plan, observations)` bez zmian;
- ostrzeżenie o pandas ≥ 3: decyzja planu 9.

**GitHub Actions:** `actions/checkout` v7.0.1, `astral-sh/setup-uv` v10.2.0 (wejście `version` domyślnie z `required-version` w `pyproject.toml`), `Swatinem/rust-cache` v2.9.2.

**Lokalnie:**
- Rust `stable` = 1.95.0 (59807616e 2026-04-14), rustup 1.29.0;
- `gh` nie jest zainstalowane;
- `~/.local/bin` jest już w `PATH`.

**Prototyp strażnika języka:**
- 202 fragmenty kodu z polskimi znakami w 48 plikach (bez `vendor/` i `raporty/`);
- przegląd słownikowy (polskie słowa bez znaków diakrytycznych) dokłada pliki, których strażnik nie widzi: m.in. `bio_phys_codec.rs`, `bin/*_local.rs`, `checksum.rs`, `coverage_node.rs`, `dist_provider.rs`.

## Oczekiwana liczba testów

`-m "not dane"` / `-m "not real_data"`; wynik `pytest -q` w ostatniej linii:

| Po zadaniu | Wynik |
|---|---|
| stan wyjściowy (Python 3.10) | 330 passed, 2 xfailed, 15 deselected |
| 1 | 332 passed, 2 xfailed, 15 deselected |
| 2 (#372 zniknął) | 334 passed, 15 deselected |
| 3 | 335 passed, 15 deselected |
| 4, 5 | 335 passed, 15 deselected |
| 6, 7, 8 | 341 passed, 1 xfailed, 15 deselected |
| 9 | 342 passed, 15 deselected |
| 10 (lokalnie, 12 wątków) | 345 passed, 15 deselected |
| 10 (próba CI: `taskset -c 0-3`, puste dane) i CI | 337 passed, 8 skipped, 15 deselected |
| 11 (`-m real_data`) | 13 passed, 2 xfailed, 345 deselected |

Drobne różnice (np. parametryzacja dopisana w trakcie) są dopuszczalne, jeśli wynikają z rulingu zapisanego w ledgerze. Kolejność pozycji w linii podsumowania zależy od wersji pytest (np. `332 passed, 15 deselected, 2 xfailed`) — porównuj liczby, nie kolejność.

## Przygotowanie (przed Zadaniem 1)

- [ ] **Gałąź i plan w repozytorium.** Bieżąca gałąź to `master` (8f4b045 albo nowszy), a jedyną zmianą jest nowy plik planu.

```bash
cd ~/praca_magisterska
git status --short          # Expected: tylko "?? docs/superpowers/plans/2026-10-02-srodowisko-uv-ci-3b1.md"
git checkout -b 3b-1-uv-ci
```

W `docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md` zamień linię statusu:

```markdown
- **Status:** zatwierdzony przez użytkownika (02.10.2026); plan: `docs/superpowers/plans/2026-10-02-srodowisko-uv-ci-3b1.md`
```

```bash
git add docs/superpowers/plans/2026-10-02-srodowisko-uv-ci-3b1.md docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md
git commit -m "Plan 3b-1: plan implementacji (srodowisko uv, aktualne wersje, CI)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 1: Środowisko uv na dzisiejszych wersjach (etap 1)

**Files:**
- Create: `pyproject.toml`, `.python-version`, `uv.lock` (generuje uv)
- Modify: `.gitignore`, `bench/__init__.py`, `tests/bench/test_env.py`

**Interfaces:**
- Consumes: nic (stan po planie 3a).
- Produces:
  - środowisko `.venv/` (`uv sync`), wszystkie dalsze polecenia przez `uv run`;
  - `bench.PROJECT_PYTHON: tuple[int, int] = (3, 12)`;
  - `bench.require_project_python(version_info) -> None` — `ImportError` z „uv run” w komunikacie, gdy `version_info[:2] != PROJECT_PYTHON`; wywoływana przy imporcie `bench`.

**Polecenie testów zadania:** `uv run pytest -m "not dane" -q`

- [ ] **Step 1: Zainstaluj uv 0.12.22** (bez modyfikacji plików powłoki, bo `~/.local/bin` jest już w `PATH`)

```bash
curl -LsSf https://astral.sh/uv/0.12.22/install.sh | UV_NO_MODIFY_PATH=1 sh
uv --version
```

Expected: `uv 0.12.22 (…)`.

- [ ] **Step 2: Zainstaluj Pythona 3.12 przez uv**

```bash
uv python install 3.12
uv python find 3.12
python3 --version
```

Expected:
- ścieżka `~/.local/share/uv/python/cpython-3.12.*-linux-x86_64-gnu/bin/python3.12`;
- systemowy `python3 --version` nadal `Python 3.10.12`.

- [ ] **Step 3: Utwórz `pyproject.toml`, `.python-version` i wpis w `.gitignore`**

`pyproject.toml`:

```toml
[project]
name = "distributed-polars-bio"
version = "0.1.0"
description = "Genomic interval operations from polars-bio on distributed engines (Apache Ballista, Sail) - master's thesis code"
requires-python = ">=3.12,<3.13"
# Etap 1 planu 3b-1: wersje zainstalowane dotąd w ~/.local (Python 3.10). Wyjątek: numpy 1.24.0
# nie ma pakietu dla Pythona 3.12 - najstarsza linia 1.x, która go ma, to 1.26.
dependencies = [
    "polars-bio==0.28.0",
    "pysail==0.5.3",
    "pyspark[connect]==4.1.1",
    "polars==1.39.3",
    "pyarrow==22.0.0",
    "pandas==2.3.3",
    "numpy==1.26.*",
    "pyyaml==6.0.2",
    "requests==2.31.0",
]

[dependency-groups]
dev = ["pytest==6.2.5"]

[tool.uv]
# Projekt "wirtualny": kod działa z katalogu repozytorium, uv niczego nie instaluje jako pakiet.
package = false
# Ta sama wersja uv lokalnie i w CI (astral-sh/setup-uv czyta ją stąd).
required-version = "==0.12.22"
# Narzędzie działa tylko na Linuksie (taskset, /proc).
environments = ["sys_platform == 'linux'"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

`.python-version` (jedna linia): `3.12`

Na końcu `.gitignore`:

```gitignore
# Środowisko Pythona projektu (uv sync; wersje w uv.lock)
.venv/
```

- [ ] **Step 4: Zablokuj wersje i zbuduj środowisko**

```bash
uv lock
uv sync
uv pip list | grep -iE "^(polars|polars-bio|pyarrow|pysail|pyspark|pandas|numpy|pyyaml|requests|pytest|datafusion) "
```

Pierwsze `uv sync` pobiera ~1,5 GB, a sdist PySparka (~430 MB) uv buduje sam, co trwa kilka minut.

Expected: `uv lock` kończy się „Resolved … packages”, a lista zawiera:

```
datafusion 50.1.0
numpy      1.26.4
pandas     2.3.3
polars     1.39.3
polars-bio 0.28.0
pyarrow    22.0.0
pysail     0.5.3
pyspark    4.1.1
pytest     6.2.5
pyyaml     6.0.2
requests   2.31.0
```

- [ ] **Step 5: Sprawdź pytest 6.2.5 na Pythonie 3.12**

```bash
uv run pytest --version
uv run pytest --collect-only -q -m "not dane" | tail -1
```

Expected: `pytest 6.2.5` i `332/347 tests collected (15 deselected)`. Ostrzeżenia `DeprecationWarning` o `ast.Str` są dopuszczalne.

Jeśli pytest się nie uruchamia albo zbieranie kończy się błędem (nie mylić z porażką testu):
1. w `[dependency-groups]` wpisz `dev = ["pytest==7.4.*"]` (pierwsza linia z oficjalnym wsparciem 3.12);
2. uruchom `uv lock && uv sync`;
3. powtórz krok;
4. zapisz ruling w ledgerze i dopisz linię do komunikatu commitu (Step 11).

- [ ] **Step 6: Napisz testy strażnika interpretera** (na końcu `tests/bench/test_env.py`, z importami na górze pliku)

Do importów na górze `tests/bench/test_env.py` dopisz:

```python
import pytest

import bench
```

Na końcu pliku:

```python
def test_bench_rejects_a_foreign_python():
    """Systemowy python3 (3.10) widzi starsze biblioteki z ~/.local — polars-bio 0.28 z błędem
    #372, Sail 0.5.3. Seria uruchomiona nim po cichu mieszałaby wersje silników w wynikach."""
    with pytest.raises(ImportError, match=r"Python 3\.12.*found 3\.10\.12.*uv run"):
        bench.require_project_python((3, 10, 12, "final", 0))


def test_bench_accepts_the_project_python():
    bench.require_project_python((3, 12, 7, "final", 0))
```

- [ ] **Step 7: Uruchom — testy mają nie przejść**

Run: `uv run pytest tests/bench/test_env.py -q`

Expected: `2 failed, 3 passed` — `AttributeError: module 'bench' has no attribute 'require_project_python'`.

- [ ] **Step 8: Zaimplementuj strażnika w `bench/__init__.py`** (cały plik)

```python
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
```

- [ ] **Step 9: Uruchom — testy mają przejść; systemowy Python dostaje czytelny błąd**

```bash
uv run pytest tests/bench/test_env.py -q
python3 -c "import bench"; echo "exit $?"
```

Expected:
- `5 passed`;
- dla `python3`: `ImportError: bench needs Python 3.12 from the project environment, found 3.10.12; run it through uv, …` i `exit 1`.

- [ ] **Step 10: Pełny pakiet bez danych rzeczywistych**

Run: `uv run pytest -m "not dane" -q`

Expected: `332 passed, 2 xfailed, 15 deselected`. Dwa `xfailed` to błąd #372 w `tests/test_polars_bio_runner.py`; reszta odpowiada wynikowi z Pythona 3.10 (330 + 2 nowe testy).

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml uv.lock .python-version .gitignore bench/__init__.py tests/bench/test_env.py
git commit -m "Plan 3b-1 (etap 1): srodowisko uv z Pythonem 3.12 na dotychczasowych wersjach

numpy 1.24.0 nie ma pakietu dla Pythona 3.12 - wziete 1.26.x (najstarsza linia 1.x z pakietem).
Pakiet bench odmawia importu pod innym Pythonem niz 3.12 srodowiska projektu.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Gdy w Step 5 potrzebny był pytest 7.4, dopisz przed `Co-Authored-By` linię: `pytest 6.2.5 nie dziala na Pythonie 3.12 - wziete 7.4.x.`

---

### Task 2: polars-bio 0.36 (etap 2)

**Files:**
- Modify: `pyproject.toml`, `uv.lock`, `tests/test_polars_bio_runner.py`, `tests/test_orchestrator_integration.py`, `raporty/polars_bio_blad_partycji.md`, `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` (akapit „Wynik wzorcowy” w sekcji 3), `wnioski_claude.md`

**Interfaces:**
- Consumes: środowisko z Zadania 1.
- Produces:
  - polars-bio 0.36.0 w `.venv`;
  - `tests/test_orchestrator_integration.py::test_every_run_is_valid` zastępuje `test_only_known_polars_bio_bug_is_invalid`; stała `KNOWN_POLARS_BIO_BUG` znika;
  - sekcja „Plan 3b-1 — aktualizacja środowiska i wersji” w `wnioski_claude.md`, do której kolejne zadania dopisują punkty.

**Polecenie testów zadania:** `uv run pytest -m "not dane" -q`

- [ ] **Step 1: Podnieś polars-bio i wymagany przez niego pyarrow**

W `pyproject.toml` zamień w `dependencies` dwie linie i zaktualizuj komentarz nad listą:

```toml
# Etapy 1-2 planu 3b-1: polars-bio 0.36 z wymaganym przez niego pyarrow (23-24); reszta - wersje
# zainstalowane dotąd w ~/.local (wyjątek: numpy 1.24.0 nie ma pakietu dla Pythona 3.12).
```

```toml
    "polars-bio==0.36.*",
```

```toml
    "pyarrow==24.*",
```

```bash
uv lock && uv sync
uv pip list | grep -iE "^(polars-bio|pyarrow|datafusion|polars) "
```

Expected: `polars-bio 0.36.0`, `pyarrow 24.x`, `datafusion 53.0.0`, `polars 1.39.3` (bez zmian: spełnia `>=1.37.1`). W `git diff uv.lock` zmieniają się tylko te pakiety i ich ewentualne nowe zależności.

- [ ] **Step 2: Sprawdź API używane przez projekt**

```bash
uv run python -c "
import inspect, polars_bio as pb
from polars_bio.context import ctx
print(pb.__version__, pb.POLARS_BIO_MAX_THREADS, hasattr(ctx, 'deregister_table'))
print(inspect.signature(pb.overlap))"
```

Expected:
- `0.36.0 datafusion.execution.target_partitions True`;
- sygnatura `overlap` z `overlap_output='join', distinct_output=False` (nowe parametry z wartościami domyślnymi; reszta jak w 0.28).

- [ ] **Step 3: Testy runnera polars-bio pokażą stan #372**

Run: `uv run pytest tests/test_polars_bio_runner.py -q`

Expected: `2 failed, 17 passed`. Obie porażki to `test_two_partitions[merge]` i `test_two_partitions[subtract]` z adnotacją `[XPASS(strict)]`: błąd #372 zniknął.

**Gałąź „#372 nadal jest”.** Jeśli zamiast tego wyjdzie `17 passed, 2 xfailed`:
1. pomiń Steps 4–7 (oznaczenia i `KNOWN_POLARS_BIO_BUG` zostają jak w planie 3a);
2. w Step 9 użyj wariantów tekstu „nadal”;
3. kontynuuj.

To obserwacja, nie zasada zatrzymania.

- [ ] **Step 4: Usuń oznaczenia xfail z testu dwóch partycji**

W `tests/test_polars_bio_runner.py`:

(a) zamień docstring modułu na:

```python
"""Plan 3a, Zadanie 4: runner polars-bio (specyfikacja 8.3) — świeży proces, polars-bio czyta
Parquet sam, wynik konsumowany strumieniowo, protokół jak w bench_client.

Przy target_partitions > 1 polars-bio 0.28 liczył merge i subtract osobno w każdej partycji
(błąd #372, naprawiony w 0.29.0). Od planu 3b-1 (polars-bio 0.36) test dwóch partycji obejmuje
wszystkie operacje bez oznaczeń xfail."""
```

(b) usuń cały blok `_PARTITION_BUG = pytest.mark.xfail(…)`;

(c) dekorator testu dwóch partycji zamień na:

```python
@pytest.mark.parametrize("op", OPS)
def test_two_partitions(op, parquet_dirs, parquet_expected):
```

Run: `uv run pytest tests/test_polars_bio_runner.py -q`

Expected: `19 passed`.

- [ ] **Step 5: Test integracyjny pokaże, że zbiór znanych błędów jest pusty**

Run: `uv run pytest tests/test_orchestrator_integration.py -q` (kilka minut)

Expected: `1 failed, 6 passed`. Porażka to `test_only_known_polars_bio_bug_is_invalid`, bo nieważnych przebiegów nie ma: `set() == KNOWN_POLARS_BIO_BUG` nie zachodzi.

- [ ] **Step 6: Test „wszystkie przebiegi ważne”**

W `tests/test_orchestrator_integration.py`:

(a) docstring modułu:

```python
"""Plan 3a, Zadanie 9: orkiestrator na prawdziwych silnikach i zbiorze testowym w układzie
databio-8p (para 1-2, zbiór 1) — wszystkie warianty, N = 1 i 2, po jednym przebiegu.

Wynik wzorcowy: polars-bio na 1 partycji. Od planu 3b-1 (polars-bio 0.36) wszystkie przebiegi
mają być ważne — także polars-bio A i B, które w 0.28 liczyły merge i subtract osobno w każdej
partycji (błąd #372, naprawiony w 0.29.0).

Wymaga binarek ballista_node i bench_client (debug). Swap jest wyłączony z kryteriów
(pswpout zastąpiony stałą): jego wykrywanie sprawdzają testy jednostkowe, a tu przypadkowy
swap innej aplikacji nie może dawać fałszywych porażek."""
```

(b) usuń stałą `KNOWN_POLARS_BIO_BUG` (cały blok `KNOWN_POLARS_BIO_BUG = {…}`);

(c) zamień `test_only_known_polars_bio_bug_is_invalid` na:

```python
def test_every_run_is_valid(series):
    _, df = series
    bad = df.filter(~pl.col("valid")).select("variant", "n_nodes", "op", "invalid_reason")
    assert bad.is_empty(), bad.to_dicts()
```

Run: `uv run pytest tests/test_orchestrator_integration.py -q`

Expected: `7 passed`.

- [ ] **Step 7: Pełny pakiet**

Run: `uv run pytest -m "not dane" -q`

Expected: `334 passed, 15 deselected`.

- [ ] **Step 8: Notatka o błędzie i specyfikacja metodyki**

Na końcu `raporty/polars_bio_blad_partycji.md` dopisz (wariant „zniknął”):

```markdown
## Stan w polars-bio 0.36.0 (plan 3b-1, 02.10.2026)

Po migracji na Pythona 3.12 i polars-bio 0.36.0 (`uv.lock`) błąd nie występuje:

- testy `test_two_partitions[merge]` i `[subtract]` w `tests/test_polars_bio_runner.py`, oznaczone
  wcześniej `xfail(strict=True)`, przeszły (XPASS) — oznaczenia usunięte;
- w teście integracyjnym orkiestratora (`tests/test_orchestrator_integration.py`) wszystkie
  przebiegi są ważne, także polars-bio A i B (2 i 4 partycje).

Wynik wzorcowy nadal liczy polars-bio na 1 partycji (wariant `polars_bio_ref`), ale osobny wariant
„polars-bio na 1 partycji” jako punkt odniesienia w P1 (decyzja z 01.10.2026) przestaje być
potrzebny.
```

Wariant „nadal”: ta sama sekcja z treścią „błąd nadal występuje w 0.36.0” (bez XPASS). Zamiast dwóch akapitów o testach wpisz wynik Step 3 (`2 xfailed`) i zdanie, że decyzja (c) z 01.10.2026 obowiązuje dalej.

W `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`, sekcja 3, zamień cały akapit zaczynający się od `**Wynik wzorcowy (plan 3a).**` (do „…zostają w wynikach jako nieważne.”) na:

```markdown
**Wynik wzorcowy.** Każdy pomiar jest sprawdzany względem polars-bio na **1 partycji**
(wariant `polars_bio_ref`, przebieg na początku serii). polars-bio 0.28 liczył przy
`target_partitions > 1` operacje `merge` i `subtract` osobno w każdej partycji (błąd #372,
naprawiony w 0.29.0; opis: `raporty/polars_bio_blad_partycji.md`). Od planu 3b-1 projekt używa
polars-bio 0.36, w którym obie operacje są poprawne przy każdej liczbie partycji: osobny wariant
„polars-bio na 1 partycji” jako punkt odniesienia w P1 (decyzja z 01.10.2026) nie jest potrzebny,
a przebiegi A/B są pełnoprawne.
```

Wariant „nadal”: akapit bez zmian, z dopiskiem na końcu `Stan w polars-bio 0.36: błąd nadal występuje (plan 3b-1).`

- [ ] **Step 9: Sekcja we wnioskach**

Na końcu `wnioski_claude.md` dopisz (wariant „zniknął”):

```markdown
## Plan 3b-1 — aktualizacja środowiska i wersji (od 02.10.2026)

Środowisko odtwarzane przez uv (`pyproject.toml`, `uv.lock`, Python 3.12); wersje podnoszone
etapami, po jednej zmianie. Punkty dopisywane po każdym etapie.

1. **polars-bio 0.36.0: błąd #372 zniknął.**
   - `merge` i `subtract` przy `target_partitions > 1` dają ten sam wynik co na 1 partycji: testy
     xfail strict z planu 3a przeszły, a w teście integracyjnym wszystkie przebiegi są ważne.
   - Wariant „polars-bio na 1 partycji” w P1 jest zbędny; wzorzec nadal na 1 partycji.
   - API używane przez projekt bez zmian: sygnatury operacji, opcje `datafusion.bio.*`,
     `POLARS_BIO_MAX_THREADS`, `execute_stream`.
```

Wariant „nadal”: punkt 1 brzmi „**polars-bio 0.36.0: błąd #372 nadal występuje.**” z opisem wyniku Step 3.

- [ ] **Step 10: Commit**

```bash
git add pyproject.toml uv.lock tests/test_polars_bio_runner.py tests/test_orchestrator_integration.py \
  raporty/polars_bio_blad_partycji.md docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md wnioski_claude.md
git commit -m "Plan 3b-1 (etap 2): polars-bio 0.36 - blad #372 zniknal

Testy dwoch partycji bez xfail; test integracyjny wymaga, by wszystkie przebiegi byly wazne.
Wariant polars-bio na 1 partycji w P1 zbedny (wzorzec nadal na 1 partycji).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Wariant „nadal”: drugi i trzeci wiersz zastąp opisem obserwacji.

---

### Task 3: Biblioteka algorytmów w Ballistcie v0.22.2 i Rust 1.95.0 (etap 3)

**Files:**
- Create: `ballista_genomics/rust-toolchain.toml`, `tests/test_algorithm_versions.py`
- Replace: `ballista_genomics/vendor/datafusion-bio-function-ranges/{src,tests,superintervals,README.md}`
- Modify:
  - `ballista_genomics/vendor/datafusion-bio-function-ranges/Cargo.toml` (wersja);
  - `ballista_genomics/Cargo.lock` (zmienia cargo);
  - `ballista_genomics/Cargo.toml` (komentarz);
  - `ballista_genomics/vendor/PATCH.md`, `ballista_genomics/OPIS.md`, `wnioski_claude.md`.

**Interfaces:**
- Consumes: polars-bio 0.36.0 z Zadania 2.
- Produces:
  - `tests/test_algorithm_versions.py` ze stałą `RANGES_IN_POLARS_BIO: dict[str, str] = {"0.36.0": "0.22.2"}`;
  - zwendorowany crate w wersji `0.22.2` z dwiema łatkami widoczności (te same symbole publiczne co dotąd, więc `ballista_genomics/src` bez zmian);
  - binarki `target/debug/*` przebudowane.

**Polecenie testów zadania:** `uv run pytest -m "not dane" -q`

Uwaga: od Step 9 do jego końca **nie uruchamiaj** żadnych testów (budowanie Rusta).

- [ ] **Step 1: Napisz test zgodności wersji algorytmów**

`tests/test_algorithm_versions.py`:

```python
"""Ballista i polars-bio liczą tą samą wersją algorytmów przedziałowych (plan 3b-1, etap 3).

polars-bio jest budowany z datafusion-bio-function-ranges z gita (tag w Cargo.toml polars-bio);
Ballista używa zwendorowanej kopii tego crate'a z łatkami widoczności (vendor/PATCH.md). Gdy wersje
się rozjadą, porównanie silników miesza efekt silnika z efektem wersji biblioteki — np. upstream
poprawił między 0.18.0 a 0.22.2 przedziały jednozasadowe w nearest i coverage."""

from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
CRATE = "datafusion-bio-function-ranges"

#: Wersja crate'a algorytmów, z którą zbudowano daną wersję polars-bio — Cargo.toml polars-bio pod
#: tagiem wydania (0.36.0: tag = "v0.22.2"). Nowa wersja polars-bio = nowy wpis tutaj i, gdy wersja
#: crate'a się zmienia, aktualizacja vendora według vendor/PATCH.md.
RANGES_IN_POLARS_BIO = {"0.36.0": "0.22.2"}


def test_ballista_uses_the_ranges_crate_version_of_polars_bio():
    polars_bio = version("polars-bio")
    assert polars_bio in RANGES_IN_POLARS_BIO, (
        f"polars-bio {polars_bio}: unknown {CRATE} version - read it from the polars-bio "
        f"Cargo.toml at the release tag, update RANGES_IN_POLARS_BIO and the vendored copy"
    )
    manifest = tomllib.loads((BALLISTA_DIR / "vendor" / CRATE / "Cargo.toml").read_text())
    lock = tomllib.loads((BALLISTA_DIR / "Cargo.lock").read_text())
    locked = {package["name"]: package["version"] for package in lock["package"]}
    expected = RANGES_IN_POLARS_BIO[polars_bio]
    assert (manifest["package"]["version"], locked[CRATE]) == (expected, expected)
```

- [ ] **Step 2: Uruchom — test ma nie przejść**

Run: `uv run pytest tests/test_algorithm_versions.py -q`

Expected: `1 failed` — `('0.18.0', '0.18.0') == ('0.22.2', '0.22.2')`.

- [ ] **Step 3: Przypnij Rusta 1.95.0**

`ballista_genomics/rust-toolchain.toml`:

```toml
# Wersja Rusta integracji z Ballistą - ta sama lokalnie i w CI (specyfikacja 3b-1, sekcja 3).
[toolchain]
channel = "1.95.0"
profile = "minimal"
```

```bash
rustup toolchain install 1.95.0 --profile minimal
cd ballista_genomics && rustc --version && rustup show active-toolchain; cd ..
```

Expected:
- `rustc 1.95.0 (59807616e 2026-04-14)`;
- aktywny toolchain `1.95.0-x86_64-unknown-linux-gnu`, nadpisany przez `…/ballista_genomics/rust-toolchain.toml`.

- [ ] **Step 4: Pobierz źródła upstreamu (0.18.0 — do odtworzenia łatek, v0.22.2 — nowa wersja)**

Katalog roboczy planu `.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/` jest ignorowany przez git.

Kroki 4–8 zaczynają się od tych samych dwóch przypisań (`UP` — źródła upstreamu, `V` — kopia w repozytorium), bo zmienne nie przechodzą między poleceniami. Wszystkie polecenia uruchamiaj z katalogu głównego repozytorium.

```bash
UP=$PWD/.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/upstream
V=$PWD/ballista_genomics/vendor/datafusion-bio-function-ranges
rm -rf "$UP" && mkdir -p "$UP"
git clone -q --depth 1 --branch v0.18.0 https://github.com/biodatageeks/datafusion-bio-functions "$UP/v0.18.0"
git clone -q --depth 1 --branch v0.22.2 https://github.com/biodatageeks/datafusion-bio-functions "$UP/v0.22.2"
grep -n '^version' "$UP/v0.22.2/datafusion/bio-function-ranges/Cargo.toml"
sed -n '/^\[workspace.dependencies\]/,/^$/p' "$UP/v0.22.2/Cargo.toml"
```

Expected:
- `version = "0.22.2"`;
- zależności workspace (`datafusion = { version = "=53.0.0", default-features = false, features = […] }`, `tokio` 1.43.0, `futures` 0.3.31, `log` 0.4.27) identyczne z tymi rozwiniętymi w `$V/Cargo.toml`.

- [ ] **Step 5: Odtwórz łatki jako różnicę kopii względem czystego 0.18.0**

```bash
UP=$PWD/.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/upstream
V=$PWD/ballista_genomics/vendor/datafusion-bio-function-ranges
mkdir -p "$UP/cmp"
cp -r "$UP/v0.18.0/datafusion/bio-function-ranges/src" "$UP/cmp/a"
cp -r "$V/src" "$UP/cmp/b"
(cd "$UP/cmp" && diff -ruN a b > ../patches.diff); grep '^+++ ' "$UP/patches.diff"
grep '^[+-]' "$UP/patches.diff" | grep -v '^[+-][+-]' > "$UP/patch_lines.txt"; wc -l < "$UP/patch_lines.txt"
```

Expected:
- 6 plików: `b/count_overlaps.rs`, `b/lib.rs`, `b/merge.rs`, `b/nearest.rs`, `b/physical_planner/mod.rs`, `b/subtract.rs`;
- `patch_lines.txt` zawiera wyłącznie dodane `pub`, re-eksporty `pub use …` i komentarze `// PATCH …` (przejrzyj).

- [ ] **Step 6: Podmień kopię na v0.22.2**

```bash
UP=$PWD/.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/upstream
V=$PWD/ballista_genomics/vendor/datafusion-bio-function-ranges
rm -rf "$V/src" "$V/tests" "$V/superintervals" "$V/README.md"
cp -r "$UP/v0.22.2/datafusion/bio-function-ranges/src" "$UP/v0.22.2/datafusion/bio-function-ranges/tests" \
      "$UP/v0.22.2/datafusion/bio-function-ranges/superintervals" "$UP/v0.22.2/datafusion/bio-function-ranges/README.md" "$V/"
sed -i 's/^version = "0.18.0"$/version = "0.22.2"/' "$V/Cargo.toml"
grep -n '^version' "$V/Cargo.toml" "$V/superintervals/Cargo.toml"
```

Expected: `version = "0.22.2"` w `Cargo.toml` kopii i `version = "0.24.2"` w `superintervals/Cargo.toml`. Reszta `Cargo.toml` kopii bez zmian, bo zależności są te same co w upstreamie, rozwinięte z workspace.

- [ ] **Step 7: Nałóż łatki**

```bash
UP=$PWD/.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/upstream
V=$PWD/ballista_genomics/vendor/datafusion-bio-function-ranges
(cd "$V/src" && patch -p1 --no-backup-if-mismatch < "$UP/patches.diff")
find "$V" -name '*.rej' -o -name '*.orig'
```

Expected:
- `patching file …` dla 6 plików; przy `nearest.rs` i `subtract.rs` dopuszczalne `Hunk #N succeeded at … (offset … lines)`;
- żadnego `FAILED`, a `find` nic nie wypisuje.

- [ ] **Step 8: Sprawdź regułę z PATCH.md względem czystego v0.22.2**

```bash
UP=$PWD/.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/upstream
V=$PWD/ballista_genomics/vendor/datafusion-bio-function-ranges
diff -ru "$UP/v0.22.2/datafusion/bio-function-ranges/src" "$V/src" | grep '^[+-]' | grep -v '^[+-][+-]' > "$UP/patch_lines_0222.txt"
diff "$UP/patch_lines.txt" "$UP/patch_lines_0222.txt" && echo "lacie identyczne"
```

Expected: `lacie identyczne`. Kopia różni się od upstreamu v0.22.2 dokładnie tymi samymi liniami co wcześniej od 0.18.0.

- [ ] **Step 9: Przebuduj binarki** (żadnych testów w tym czasie)

```bash
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | grep -E "Compiling (superintervals|datafusion-bio|ballista_genomics|datafusion v)|Finished|error" ; cd ..
git diff --stat ballista_genomics/Cargo.lock && git diff ballista_genomics/Cargo.lock | grep '^[+-]version'
```

Expected:
- `Compiling superintervals v0.24.2`, `Compiling datafusion-bio-function-ranges v0.22.2`, `Compiling ballista_genomics v0.1.0`, `Finished …`;
- w `Cargo.lock` zmieniają się tylko wersje tych dwóch crate'ów (0.18.0 → 0.22.2, 0.20.0 → 0.24.2).

Jeśli pojawi się `Compiling datafusion v53.0.0`, zmiana nazwy toolchainu wywołała pełną przebudowę (1–2 h). Poczekaj i zapisz to w ledgerze. Błąd kompilacji w `ballista_genomics/src` to zmiana API vendora — przy sprawdzonych faktach nie powinna wystąpić. Diagnozuj według superpowers:systematic-debugging; poważna zmiana oznacza zasadę zatrzymania.

- [ ] **Step 10: Test zgodności wersji przechodzi**

Run: `uv run pytest tests/test_algorithm_versions.py -q`

Expected: `1 passed`.

- [ ] **Step 11: Pełny pakiet** (porównuje Ballistę z polars-bio, więc sprawdza też zgodność algorytmów)

Run: `uv run pytest -m "not dane" -q`

Expected: `335 passed, 15 deselected`.

- [ ] **Step 12: Dokumentacja vendora i integracji**

(a) `ballista_genomics/Cargo.toml`: zamień dwie linie komentarza nad `datafusion-bio-function-ranges = { path = … }` na:

```toml
# Lokalna kopia datafusion-bio-function-ranges v0.22.2 (ta sama wersja co w polars-bio 0.36)
# z dwiema łatkami widoczności - patrz vendor/PATCH.md. Potrzebna do kodeków węzłów fizycznych
# operacji zakresowych (Faza A.4b i dalej).
```

(b) `ballista_genomics/vendor/PATCH.md`:
- po pierwszym akapicie (przed „Obie łatki mają tę samą naturę”) wstaw sekcję:

```markdown
## Wersja kopii

Kopia to `datafusion/bio-function-ranges` z tagu **v0.22.2** repozytorium
biodatageeks/datafusion-bio-functions — ta sama wersja, z którą zbudowano polars-bio 0.36.0
(pilnuje tego `tests/test_algorithm_versions.py`). `Cargo.toml` kopii ma rozwinięte dziedziczenie
z workspace upstreamu (`version.workspace` itd.), poza tym jest bez zmian. Upstream v0.22.2 nadal
nie udostępnia ani `physical_planner::intervals`, ani węzłów fizycznych operacji, więc obie łatki
są nałożone ponownie bez zmian — struktury mają te same pola co w 0.18.0 (plan 3b-1, 02.10.2026).
```

- blok z regułą utrzymaniową (od „Regułę utrzymaniową warto sprawdzać…” do akapitu o „łatce czystej”) zamień na:

````markdown
Regułę utrzymaniową sprawdza się względem czystego tagu upstreamu:

```bash
git clone -q --depth 1 --branch v0.22.2 https://github.com/biodatageeks/datafusion-bio-functions /tmp/dbf
diff -ru /tmp/dbf/datafusion/bio-function-ranges/src ballista_genomics/vendor/datafusion-bio-function-ranges/src \
  | grep '^[+-]' | grep -v '^[+-][+-]'
```

Jeśli w wyniku pojawi się cokolwiek poza dodanym `pub`, re-eksportem lub komentarzem,
łatka przestała być „czysta" i wymaga osobnej decyzji. Aktualizacja vendora: łatki to różnica
kopii względem czystego tagu poprzedniej wersji, nakładana `patch -p1` na nową (plan 3b-1,
Zadanie 3).
````

(c) Na końcu `ballista_genomics/OPIS.md`:

```markdown
## Plan 3b-1 — wersja algorytmów v0.22.2 (październik 2026)

- Zwendorowany `datafusion-bio-function-ranges` podniesiony z 0.18.0 do **v0.22.2** — tej samej
  wersji, z którą zbudowano polars-bio 0.36.0. Ballista, Sail (polars-bio w UDTF) i wzorzec liczą
  więc tymi samymi algorytmami; pilnuje tego `tests/test_algorithm_versions.py`.
- DataFusion bez zmian (`=53.0.0` w obu wersjach crate'a) — przebudowa objęła tylko crate
  algorytmów i integrację.
- Łatki widoczności bez zmian: te same pola struktur `MergeExec`, `SubtractExec`, `NearestExec`,
  `CountOverlapsExec`. API używane przez kodeki też bez zmian: `IntervalJoinExec::try_new`,
  `build_nearest_indexes`, `build_coitree_from_batches`, `build_count_index_from_batches`.
- Zmiany upstreamu 0.18.0 → 0.22.2 dotyczą wykonania:
  - przedziały jednozasadowe w `nearest` i `count_overlaps`/`coverage` przy współrzędnych
    0-based — wcześniej zapytanie [s, s+1) zwężało się do pustego zakresu porównania;
  - `subtract` i `complement` przy współrzędnych 1-based.

  Zbiór testowy nie ma przedziałów jednozasadowych, więc wyniki testów się nie zmieniły.
- Rust przypięty w `rust-toolchain.toml` (1.95.0) — ta sama wersja lokalnie i w CI.
```

(d) W `wnioski_claude.md`, w sekcji „Plan 3b-1 — …”, dopisz punkt:

```markdown
2. **Ballista i polars-bio liczą tą samą wersją algorytmów (v0.22.2).**
   - Vendor podniesiony z 0.18.0; łatki widoczności bez zmian.
   - Między 0.18.0 a 0.22.2 upstream poprawił m.in. przedziały jednozasadowe przy współrzędnych
     0-based (`nearest`, `count_overlaps`, `coverage`). Przy rozjechanych wersjach takie
     przedziały dawałyby różne wyniki silników bez winy silnika.
   - Wyrównanie wersji jest więc warunkiem porównania silników; pilnuje go
     `tests/test_algorithm_versions.py`.
```

- [ ] **Step 13: Commit**

```bash
git add -A ballista_genomics/vendor ballista_genomics/rust-toolchain.toml ballista_genomics/Cargo.toml \
  ballista_genomics/Cargo.lock ballista_genomics/OPIS.md tests/test_algorithm_versions.py wnioski_claude.md
git commit -m "Plan 3b-1 (etap 3): datafusion-bio-function-ranges v0.22.2 jak w polars-bio 0.36

Vendor podmieniony na upstream v0.22.2 z tymi samymi dwiema latkami widocznosci (te same pola
struktur, regula z PATCH.md sprawdzona wzgledem czystego tagu). Rust przypiety: 1.95.0.
Test pilnuje zgodnosci wersji algorytmow polars-bio i Ballisty.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Sail 0.7.2 i PySpark 4.2 (etap 4)

**Files:**
- Modify: `pyproject.toml`, `uv.lock`, `wnioski_claude.md`
- Modify tylko w razie potrzeby (Step 3, Step 6): `tests/test_sail_runner.py`, `bench/runners/sail_server.py`, `sail_bio.py`, `bench/runners/sail_runner.py`, `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` (wiersz Saila w tabeli wariantów, sekcja 3)
- Create (poza repozytorium): `.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/probe_sail.py`

**Interfaces:**
- Consumes: `bench.procutil.free_port() -> int`, `bench.procutil.wait_for_port(port, proc, name, log, timeout=…)`, `bench.runners.sail_server.sail_env(n_nodes) -> dict[str, str]`, `tests.parquet_fixture.write_parts(rows, directory, n_files=2) -> Path`, `sail_bio.register(spark, op)`, `sail_bio.build_query(spark, op, left, right=None, cols=COLUMNS)`.
- Produces: pysail 0.7.2 i pyspark 4.2.0 w `.venv`; punkt 3 we wnioskach.

**Polecenie testów zadania:** `uv run pytest -m "not dane" -q`

- [ ] **Step 1: Podnieś Saila i PySparka**

W `pyproject.toml` zamień dwie linie i zaktualizuj komentarz nad listą zależności:

```toml
# Etapy 1-4 planu 3b-1: polars-bio 0.36 (z pyarrow 23-24), Sail 0.7 i PySpark 4.2; reszta - wersje
# zainstalowane dotąd w ~/.local (wyjątek: numpy 1.24.0 nie ma pakietu dla Pythona 3.12).
```

```toml
    "pysail==0.7.*",
    "pyspark[connect]==4.2.*",
```

```bash
uv lock && uv sync
uv pip list | grep -iE "^(pysail|pyspark|zstandard|grpcio|pandas|pyarrow) "
```

Expected: `pysail 0.7.2`, `pyspark 4.2.0` (uv buduje sdist ~450 MB), pandas 2.3.3 i pyarrow 24.x bez zmian.

- [ ] **Step 2: Testy Saila**

Run: `uv run pytest tests/test_sail_runner.py tests/test_sail_parquet.py tests/test_sail_parallel_udtf.py tests/bench/test_engines.py -q`

Expected: wszystkie przechodzą. Wśród nich są:
- rejestracja i wykonanie UDTF;
- strumień wyniku (`to_table_as_iterator` w runnerze);
- zmienne `SAIL_*` serwera;
- test pamięci przy LATERAL `test_sail_memory_does_not_scale_with_output_times_group` — ze specyfikacji, etap 4.

- [ ] **Step 3: Reguły adaptacji, gdyby coś nie przeszło** (każda zmiana = ruling w ledgerze)

- `test_server_keeps_exactly_n_workers` nie przechodzi, bo numery workerów w sesji nie zaczynają się od 1 (np. `{'3', '4'}`). Zamień ostatnią asercję na sprawdzenie liczby workerów ostatniej sesji: `assert len(workers) == 2, workers`. Zaktualizuj docstring testu o nowe zachowanie (po polsku) i opisz je w Step 6.
- Odrzucona zmienna `SAIL_*` (błąd konfiguracji przy starcie serwera). Klucze były sprawdzone w `application.yaml` 0.7.2, więc to nieoczekiwane. Diagnozuj (superpowers:systematic-debugging) i porównaj z `crates/sail-common/src/config/application.yaml` pod tagiem `v0.7.2`.
- Błąd rejestracji albo wykonania UDTF albo inny wynik niż polars-bio. Diagnozuj według superpowers:systematic-debugging. Gdy naprawa wymaga przebudowy ścieżki UDTF, uruchom **zasadę zatrzymania** — pytanie do użytkownika: zostać przy `pysail==0.5.*` / `pyspark[connect]==4.1.*` czy naprawiać.

- [ ] **Step 4: Pełny pakiet**

Run: `uv run pytest -m "not dane" -q`

Expected: `335 passed, 15 deselected`.

- [ ] **Step 5: Sondy ustaleń o Sailu z planu 3a**

Sondy używają małego zbioru testowego, więc to nie jest pomiar ani próba na danych rzeczywistych i checklista RAM nie jest potrzebna. Zapisz plik `.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/probe_sail.py`:

```python
"""Sondy Saila po aktualizacji (plan 3b-1, Zadanie 4) — plik roboczy, nie trafia do repozytorium.

1. Pula workerów na sesję i dodatkowy worker przy N = 1: serwer N = 1, 10 przebiegów runnera
   (każdy = osobny proces = osobna sesja) na zbiorze testowym w 8 plikach na stronę (16 zadań
   skanowania > 8 slotów workera); z logu serwera — workery każdej sesji.
2. To samo dla N = 2 (5 przebiegów).
3. Kilka sesji po kolei w JEDNYM procesie klienta (5 sesji, limit 120 s na sesję).

Uruchomienie z katalogu repozytorium: uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/probe_sail.py"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path.cwd()
sys.path.insert(0, str(REPO))

from bench.procutil import free_port, wait_for_port  # noqa: E402
from bench.runners.sail_server import sail_env  # noqa: E402
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts  # noqa: E402

SESSION_TIMEOUT_S = 120


def start_server(n_nodes: int, log: Path) -> tuple[subprocess.Popen, int]:
    port = free_port()
    with log.open("w") as f:
        proc = subprocess.Popen(
            [sys.executable, "-m", "bench.runners.sail_server", "--port", str(port)],
            cwd=REPO, env={**os.environ, **sail_env(n_nodes)}, stdout=f, stderr=subprocess.STDOUT,
        )
    wait_for_port(port, proc, name="sail_server", log=log)
    return proc, port


def stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def workers_per_session(log: Path) -> list[list[str]]:
    sessions = log.read_text().split("creating session")[1:]
    return [sorted(set(re.findall(r"worker (\d+) server is ready", s)), key=int) for s in sessions]


def run_runner(port: int, left: Path, right: Path) -> None:
    r = subprocess.run(
        [sys.executable, "-m", "bench.runners.sail_runner", "--op", "overlap", "--left", str(left),
         "--right", str(right), "--remote", f"sc://127.0.0.1:{port}"],
        cwd=REPO, capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0:
        raise RuntimeError(f"runner exit code {r.returncode}: {r.stderr[-2000:]}")


def probe_pool(n_nodes: int, runs: int, left: Path, right: Path, logs: Path) -> None:
    log = logs / f"server_n{n_nodes}.log"
    proc, port = start_server(n_nodes, log)
    try:
        for _ in range(runs):
            run_runner(port, left, right)
    finally:
        stop(proc)
    sessions = workers_per_session(log)
    extra = sum(len(workers) > n_nodes for workers in sessions)
    print(f"N = {n_nodes}: {len(sessions)} sessions; workers per session: {sessions}; "
          f"sessions with more than N workers: {extra}")


SESSIONS_IN_ONE_PROCESS = """
import sys, time
sys.path.insert(0, {repo!r})
from pyspark.sql import SparkSession
import sail_bio
for i in range({count}):
    t = time.perf_counter()
    spark = SparkSession.builder.remote("sc://127.0.0.1:{port}").create()
    sail_bio.register(spark, "overlap")
    rows = len(sail_bio.build_query(spark, "overlap", {left!r}, {right!r}).collect())
    spark.stop()
    print(f"session {{i + 1}}: {{rows}} rows in {{time.perf_counter() - t:.1f}} s", flush=True)
"""


def probe_sessions_in_one_process(left: Path, right: Path, logs: Path, count: int = 5) -> None:
    proc, port = start_server(1, logs / "server_sessions.log")
    code = SESSIONS_IN_ONE_PROCESS.format(repo=str(REPO), count=count, port=port, left=str(left), right=str(right))
    try:
        r = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True,
                           timeout=SESSION_TIMEOUT_S * count)
        print(r.stdout.strip() or f"exit code {r.returncode}: {r.stderr[-2000:]}")
    except subprocess.TimeoutExpired as e:
        done = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        print(f"TIMEOUT after {SESSION_TIMEOUT_S * count} s; finished sessions:\n{done.strip()}")
    finally:
        stop(proc)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="sail_probe_") as tmp:
        root = Path(tmp)
        left = write_parts(FIXTURE_A, root / "a", n_files=8)
        right = write_parts(FIXTURE_B, root / "b", n_files=8)
        logs = root / "logs"
        logs.mkdir()
        probe_pool(1, 10, left, right, logs)
        probe_pool(2, 5, left, right, logs)
        probe_sessions_in_one_process(left, right, logs)


if __name__ == "__main__":
    main()
```

Run: `uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/probe_sail.py 2>&1 | tee .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/probe_sail.txt`

Expected: trzy linie wyników (kilka minut). Ustalenia z 0.5.3 dla porównania:
- każda sesja ma własne workery numerowane od 1;
- przy N = 1 sesja sporadycznie dostaje 2 workery;
- przy N = 2 workery są zawsze 2;
- w jednym procesie klienta zapytanie czwartej sesji wisi.

Interpretacja wyników:
- pusta lista workerów w sesjach po pierwszej oznacza, że pula przeżywa sesję (zmiana zachowania);
- `TIMEOUT` z trzema zakończonymi sesjami oznacza, że zawieszanie czwartej sesji nadal występuje.

Wyniki zapisz w ledgerze.

- [ ] **Step 6: Opisz, co obowiązuje w 0.7.2**

W `wnioski_claude.md`, w sekcji „Plan 3b-1 — …”, dopisz punkt 3. Wpisz wynik sond, zachowując strukturę:

```markdown
3. **Sail 0.7.2 (PySpark 4.2.0): ustalenia z 0.5.3 sprawdzone od nowa (sondy <data>).**
   - Zmienne `SAIL_*` używane przez narzędzie bez zmian (te same klucze i wartości domyślne
     w `application.yaml` 0.5.3 i 0.7.2). API `pysail.spark.SparkConnectServer` bez zmian.
   - Pula workerów na sesję: <obowiązuje / zmiana: opis z sondy 1>.
   - Dodatkowy worker przy N = 1 (16 zadań skanowania > 8 slotów): <k z 10 sesji>; przy N = 2:
     <k z 5>.
   - Sesje po kolei w jednym procesie klienta: <wszystkie 5 zakończone / zapytanie sesji k
     wisi>.
   - Serwer drivera jest w 0.7.2 wspólny dla sesji („driver server is ready” raz, przy starcie
     serwera); pula workerów należy do drivera sesji.
```

Jeśli którekolwiek ustalenie z 0.5.3 przestało obowiązywać, zaktualizuj w tym samym commicie zdania, które je opisują:
- docstring `sail_env` i komentarz `WORKER_TASK_SLOTS` w `bench/runners/sail_server.py`;
- docstring `test_server_keeps_exactly_n_workers` w `tests/test_sail_runner.py`;
- wiersz **Sail** w tabeli wariantów w sekcji 3 specyfikacji metodyki.

Zdania, które nadal obowiązują, zostają.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock wnioski_claude.md   # + pliki zmienione w Step 3 i Step 6
git commit -m "Plan 3b-1 (etap 4): Sail 0.7.2 i PySpark 4.2.0

Sondy z planu 3a powtorzone (pula workerow na sesje, dodatkowy worker przy N = 1, kilka sesji
w jednym procesie klienta) - wyniki we wnioskach.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Pozostałe biblioteki (etap 5)

**Files:**
- Modify: `pyproject.toml`, `uv.lock`, `wnioski_claude.md`; kod tylko w razie potrzeby (najbardziej narażone: `bench/checksum.py`, `sail_bio.py`)

**Interfaces:**
- Consumes: środowisko po Zadaniu 4.
- Produces: końcowa postać `pyproject.toml`: silniki przypięte do minor, reszta z dolnymi granicami. Najnowsze wersje są w `uv.lock`.

**Polecenie testów zadania:** `uv run pytest -m "not dane" -q`

- [ ] **Step 1: Końcowa postać zależności**

W `pyproject.toml` zamień komentarz i listę `dependencies` oraz grupę `dev`:

```toml
# Silniki będące przedmiotem pomiarów są przypięte do wersji minor, żeby `uv lock --upgrade`
# nie zmienił ich w trakcie pomiarów (specyfikacja 3b-1, sekcja 3). Pozostałe zależności mają
# tylko dolne granice; dokładne wersje są w uv.lock.
dependencies = [
    "polars-bio==0.36.*",
    "pysail==0.7.*",
    "pyspark[connect]==4.2.*",
    "polars>=1.44",
    "pyarrow>=24",
    "pandas>=3.0",
    "numpy>=2.5",
    "pyyaml>=6.0.3",
    "requests>=2.34",
]

[dependency-groups]
dev = ["pytest>=9.1"]
```

- [ ] **Step 2: Najnowsze wersje**

```bash
uv lock --upgrade && uv sync
uv pip list | grep -iE "^(polars|polars-bio|pyarrow|pysail|pyspark|pandas|numpy|pyyaml|requests|pytest|datafusion) "
```

Expected (albo nowsze wersje patch):
- numpy 2.5.x, pandas 3.0.x, pytest 9.1.x, polars 1.44.x;
- pyarrow 24.x (ograniczenie polars-bio), datafusion 53.0.0;
- requests 2.34.x, pyyaml 6.0.3.

Zmianę wersji patch silnika w obrębie przypięcia zapisz w ledgerze (decyzja planu 11).

- [ ] **Step 3: Najpierw najbardziej narażone miejsca**

Run: `uv run pytest tests/bench/test_checksum.py tests/test_bench_client_protocol.py tests/bench/test_ops.py tests/test_sail_parquet.py tests/test_sail_runner.py -q`

Expected: wszystkie przechodzą.
- Suma kontrolna używa jawnych skalarów `np.uint64` w `np.errstate(over="ignore")`, więc reguły promocji numpy 2 jej nie zmieniają.
- Wartości wzorcowe dla Pythona i Rusta są w `tests/bench/checksum_vectors.py`.

Porażka w UDTF-ach Saila pod pandas 3 to **zasada zatrzymania**. Pytanie do użytkownika:
- (a) zostać przy pandas 2.x: `"pandas>=2.3,<3"`, bo PySpark 4.2 sam zaleca pandas < 3, a testy pysail 0.7.2 przypinają `pandas<3`;
- (b) naprawiać.

- [ ] **Step 4: Pełny pakiet i nowe ostrzeżenia**

```bash
uv run pytest -m "not dane" -q 2>&1 | tee .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/etap5_pytest.txt | tail -3
grep -iE "warning" .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/etap5_pytest.txt | sort | uniq -c | sort -rn | head -20
```

Expected:
- `335 passed, 15 deselected`;
- w podsumowaniu ostrzeżeń dopuszczalny `FutureWarning: PySpark does not yet fully support pandas >= 3.0.0` (zapisz w ledgerze, ile razy i z których testów).

- [ ] **Step 5: Wnioski**

W `wnioski_claude.md`, w sekcji „Plan 3b-1 — …”, dopisz punkt 4 z rzeczywistymi wersjami ze Step 2:

```markdown
4. **Pozostałe biblioteki do najnowszych wersji (numpy 2.5, pandas 3.0, pytest 9.1, polars 1.44).**
   - Pakiet testów bez zmian w kodzie. Suma kontrolna jest odporna na reguły promocji numpy 2,
     bo używa jawnych skalarów `uint64`.
   - PySpark 4.2.0 ostrzega, że pandas ≥ 3 „nie jest jeszcze w pełni wspierany”, a testy samego
     pysail 0.7.2 przypinają `pandas<3`. Ścieżka UDTF projektu (wiersze, nie pandas UDF) działa,
     a testy porównują jej wynik z polars-bio.
   - Do sprawdzenia w planie 3b-2 przy przejściu na UDTF ze strzałką (Arrow), gdzie PySpark
     konwertuje partie przez pandas.
```

Jeśli w kodzie były potrzebne zmiany, opisz je w tym punkcie zamiast zdania „bez zmian w kodzie”.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock wnioski_claude.md   # + ewentualne poprawki kodu
git commit -m "Plan 3b-1 (etap 5): pozostale biblioteki do najnowszych wersji (numpy 2, pandas 3, pytest 9)

Silniki przypiete do wersji minor, reszta z dolnymi granicami; dokladne wersje w uv.lock.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Strażnik konwencji językowej i znacznik `real_data` (etap 6, część 1)

**Files:**
- Create: `tests/test_code_language.py`
- Modify: `tests/conftest.py`, `tests/test_real_data.py`
- Create (poza repozytorium): `.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py`

**Interfaces:**
- Produces (dla Zadań 7–9):
  - `tests.test_code_language.python_code(source: str) -> list[tuple[int, str]]`;
  - `rust_code(source: str) -> list[tuple[int, str]]`;
  - `violations(path: Path) -> list[tuple[int, str]]`;
  - `source_files() -> list[Path]`, `REPO: Path`, `EXCLUDED: tuple[str, ...]`;
  - test całego repozytorium `test_source_code_outside_comments_is_english` z oznaczeniem `xfail(strict=True)` do Zadania 9;
  - znacznik `real_data` zamiast `dane`.

**Polecenie testów zadania:** `uv run pytest -m "not real_data" -q`

- [ ] **Step 1: Napisz testy skanera** (najpierw tylko testy)

`tests/test_code_language.py`:

```python
"""Strażnik konwencji językowej (plan 3b-1, etap 6): w plikach źródłowych po polsku są tylko
komentarze i docstringi. Test szuka polskich znaków diakrytycznych w pozostałym kodzie (nazwy,
napisy, komunikaty, f-stringi). Polskich słów bez znaków diakrytycznych nie wykryje — chroni przed
nawrotem, nie zastępuje przeglądu.

Zakres: pliki .py śledzone przez git i pliki .rs w ballista_genomics/src. Poza zakresem: kod
zwendorowany (należy do upstreamu) i źródła raportów w raporty/ (podpisy rysunków do polskich
raportów to treść dokumentu, nie komunikaty programu)."""

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
```

- [ ] **Step 2: Uruchom — testy mają nie przejść**

Run: `uv run pytest tests/test_code_language.py -q`

Expected: `6 failed` — `NameError: name 'violations' is not defined` (i `source_files`).

- [ ] **Step 3: Dopisz skaner i test całego repozytorium**

W `tests/test_code_language.py`, między stałą `EXCLUDED` a funkcją `_write`, wstaw:

```python
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
    return [REPO / name for name in listed if not name.startswith(EXCLUDED)]
```

Na końcu pliku dopisz:

```python
def test_source_code_outside_comments_is_english():
    found = [
        f"{path.relative_to(REPO)}:{line}: {text[:100]}"
        for path in source_files()
        for line, text in violations(path)
    ]
    assert not found, f"{len(found)} code fragments with Polish letters:\n" + "\n".join(found[:60])
```

- [ ] **Step 4: Uruchom — skaner działa, repozytorium jeszcze nie**

Run: `uv run pytest tests/test_code_language.py -q`

Expected: `1 failed, 6 passed`. Porażka to `test_source_code_outside_comments_is_english` z komunikatem `~202 code fragments with Polish letters` (stan z prototypu: 48 plików).

- [ ] **Step 5: Oznacz test całego repozytorium jako xfail do końca tłumaczenia**

Nad `def test_source_code_outside_comments_is_english():` dopisz:

```python
@pytest.mark.xfail(strict=True, reason="translation in progress (plan 3b-1, tasks 7-9)")
```

Run: `uv run pytest tests/test_code_language.py -q`

Expected: `6 passed, 1 xfailed`.

- [ ] **Step 6: Znacznik `real_data`** (najpierw sprawdzenie, że dziś go nie ma)

Run: `uv run pytest -m real_data --collect-only -q | tail -1`

Expected: zero wybranych testów, np. `no tests collected (357 deselected)`.

W `tests/conftest.py` zamień rejestrację znacznika:

```python
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_data: needs the downloaded databio-8p dataset (python -m bench.data.download)",
    )
```

W `tests/test_real_data.py`, w `pytestmark`, zamień `pytest.mark.dane,` na `pytest.mark.real_data,`.

Run: `uv run pytest -m real_data --collect-only -q | tail -1`

Expected: `15/357 tests collected (342 deselected)`.

- [ ] **Step 7: Pomocnik do przeglądu słów bez znaków diakrytycznych** (plik roboczy, nie do repozytorium)

`.superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py`:

```python
"""Jednorazowy przegląd (plan 3b-1, Zadania 7–9): polskie słowa BEZ znaków diakrytycznych w kodzie
poza komentarzami i docstringami — tego strażnik nie wykrywa. Trafienia przegląda się ręcznie.

Uruchomienie z katalogu repozytorium:
  uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py [pliki…]"""
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.getcwd())
from tests.test_code_language import REPO, python_code, rust_code, source_files  # noqa: E402

POLISH_WORDS = re.compile(
    r"\b(nie|brak\w*|liczb[aey]|wynik\w*|wiersz\w*|plik\w*|katalog\w*|dane|danych|oczekiw\w*"
    r"|nieznan\w*|niepoprawn\w*|wymag\w*|zbior\w*|przebieg\w*|seri[aie]|blok[iu]?|rund[aey]"
    r"|wzorz?ec\w*|sum[ay]|kontroln\w*|silnik\w*|wariant\w*|proces[uy]?|uruchom\w*|zapis\w*"
    r"|tylko|albo|oraz|jest|bez|przy|dla|lub|czy|gdy|jako|przez|czas|faza|lokalnie|rozproszon\w*"
    r"|klaster\w*|tabel[aiy]|operacj\w*|lewe\w*|prawe\w*|wejscia|uzycie|dostalem|podaj\w*"
    r"|logi|wyniki|dziennik\w*|integracja|sciezk\w*|zl[ya]|etap|lista|dowiazanie|nie_ma\w*"
    r"|serwer\w*|cichy|testowy|pamiec\w*|konczy|czasy)\b",
    re.IGNORECASE,
)

paths = [Path(arg).resolve() for arg in sys.argv[1:]] or source_files()
for path in paths:
    source = path.read_text(encoding="utf-8")
    code = python_code(source) if path.suffix == ".py" else rust_code(source)
    for line, text in code:
        for word in sorted({m.group(0) for m in POLISH_WORDS.finditer(text)}):
            print(f"{path.relative_to(REPO)}:{line}: {word}  |  {text.strip().replace(chr(10), ' ')[:90]}")
```

Run: `uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py | wc -l`

Expected: kilkaset trafień (prototyp: ~560).

- [ ] **Step 8: Pełny pakiet i commit**

Run: `uv run pytest -m "not real_data" -q`

Expected: `341 passed, 1 xfailed, 15 deselected`.

```bash
git add tests/test_code_language.py tests/conftest.py tests/test_real_data.py
git commit -m "Plan 3b-1 (etap 6): straznik konwencji jezykowej i znacznik real_data

Test skanuje kod Pythona i Rusta poza komentarzami i docstringami; do konca tlumaczenia oznaczony
xfail (strict). Znacznik testow na danych rzeczywistych: dane -> real_data.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Zasady tłumaczenia (Zadania 7–9)

Zakres i wyjątki (specyfikacja, sekcja 5):
- po angielsku: identyfikatory, komunikaty błędów i wyjątków, `help`/`description` w CLI, wypisywany postęp, wartości zapisywane w wynikach (`invalid_reason`), nazwy plików i katalogów tworzonych w testach, komunikaty asercji;
- **bez zmian:** komentarze, docstringi, nazwy kolumn danych (`contig`, `pos_start`, …), nazwy zmiennych środowiska, kod w `ballista_genomics/vendor/` i `raporty/`.

Kolejność w każdym zadaniu (TDD):
1. Najpierw testy, które sprawdzają komunikaty (`match=`, `in r.stderr`, `== [...]`): zmień oczekiwane teksty na angielskie z tabel niżej i uruchom je — mają **nie przejść**, bo kod mówi jeszcze po polsku.
2. Potem przetłumacz kod; testy przechodzą.
3. Na koniec strażnik (`uv run pytest tests/test_code_language.py --runxfail -q` pokazuje pozostałe fragmenty z polskimi znakami) i `polish_words.py` dla plików zadania: każde trafienie przetłumacz albo uznaj za fałszywe (np. angielskie słowo) i zapisz w ledgerze.

Każdy test, który dopasowuje komunikat, zmienia się w tym samym commicie co komunikat.

**Słownik pojęć** (spójność w całym kodzie):

| PL | EN |
|---|---|
| przebieg | run |
| seria | series |
| blok | block |
| runda | round |
| rozgrzewka | warm-up (`warmup` w nazwach) |
| przebieg kontrolny | control run |
| wynik wzorcowy / wzorzec | reference |
| suma kontrolna | checksum |
| liczba wierszy | row count |
| nieważny | invalid |
| pominięty | skipped |
| dryf | drift |
| strażnik pamięci | memory guard |
| węzeł | node |
| rdzeń / wątek | core / thread |
| silnik | engine |
| zbiór (danych) | dataset |
| para (zbiorów) | pair |
| wariant | variant |
| scenariusz | scenario |
| kod wyjścia | exit code |
| binarka | binary |
| użycie | usage |
| błąd | error |
| UWAGA | WARNING |
| rozproszony | distributed |
| lokalnie | local |
| czas | time |
| wynik zapisany do | result written to |
| łączenie z | connecting to |

**Nazwy plików i katalogów w testach:**

| było | jest |
|---|---|
| `wyniki` | `results` |
| `logi` | `logs` |
| `dane` | `data` |
| `nie_ma_takiego_katalogu` | `no_such_dir` |
| `nie_ma` | `missing` |
| `brak` | `missing` |
| `seria.yaml` | `series.yaml` |
| `zly.yaml` | `broken.yaml` |
| `zla.yaml` | `bad.yaml` |
| `lista.yaml` | `list.yaml` |
| `etap` | `stage` |
| `pamiec_sie_konczy` | `memory_running_out` |
| `dziennik.txt` | `journal.txt` |
| `dowiazanie` | `symlink` |
| `czasy_kontroli.txt` | `control_times.txt` |
| `sail_serwer` | `sail_server` |
| `serwer_testowy` | `test_server` |
| `cichy` | `silent` |
| `executor_zly` | `executor_bad` |
| serie `integracja`, `sciezki` | `integration`, `paths` |

---

### Task 7: Tłumaczenie — `bench/`, runnery, `sail_bio.py` i ich testy (etap 6, część 2)

**Files:**
- Modify (kod): `bench/checksum.py`, `bench/config.py`, `bench/data/datasets.py`, `bench/data/download.py`, `bench/engines.py`, `bench/metrics.py`, `bench/ops.py`, `bench/orchestrator.py`, `bench/procutil.py`, `bench/results.py`, `bench/runners/common.py`, `bench/runners/polars_bio_runner.py`, `bench/runners/sail_runner.py`, `bench/runners/sail_server.py`, `bench/validity.py`, `sail_bio.py`
- Modify (testy): `tests/bench/*.py` (w tym `fake_runner.py`), `tests/test_orchestrator_integration.py`, `tests/test_polars_bio_runner.py`, `tests/test_sail_runner.py`, `tests/test_sail_parquet.py`

**Interfaces:**
- Consumes: `tests.test_code_language` (Zadanie 6), `polish_words.py`.
- Produces: wartości `invalid_reason` po angielsku (tabela niżej) — od tej chwili to format danych w `runs.parquet`. Komunikaty `bench` po angielsku.

**Polecenie testów zadania:** `uv run pytest -m "not real_data" -q`

**Wartości `invalid_reason` i postęp orkiestratora** (`bench/validity.py`, `bench/orchestrator.py`):

| było | jest |
|---|---|
| `stdout runnera: oczekiwano jednej linii JSON, jest {n}` | `runner stdout: expected one JSON line, got {n}` |
| `stdout runnera: niepoprawny JSON ({e})` | `runner stdout: invalid JSON ({e})` |
| `stdout runnera: oczekiwano obiektu JSON` | `runner stdout: expected a JSON object` |
| `stdout runnera: brak pól {missing}` | `runner stdout: missing fields {missing}` |
| `stdout runnera: rows = …` (tak samo `checksum`, `t_total_s`, `peak_rss_bytes`) | `runner stdout: rows = …` |
| `stdout runnera: phases i extra muszą być obiektami` | `runner stdout: phases and extra must be objects` |
| `(pusty stderr)` | `(empty stderr)` |
| `kod wyjścia {code}: {tail}` | `exit code {code}: {tail}` |
| `brak raportu runnera` | `no runner report` |
| `liczba wierszy {n} ≠ wzorzec {m}` | `row count {n} ≠ reference {m}` |
| `suma kontrolna {x} ≠ wzorzec {y}` | `checksum {x} ≠ reference {y}` |
| `strażnik pamięci: MemAvailable …` | `memory guard: MemAvailable …` |
| `proces {names} zakończył się w trakcie przebiegu` | `process {names} exited during the run` |
| `pominięty: {reason}` | `skipped: {reason}` |
| `dryf kontrolny {p}` | `control drift {p}` |
| `przebieg kontrolny nieważny` | `control run invalid` |
| `blok przerwany: {e}` | `block interrupted: {e}` |
| `seria przerwana` | `series interrupted` |
| `sygnał {SIG}` | `signal {SIG}` |
| `brak wyniku wzorcowego {id}: {reason}` | `no reference result for {id}: {reason}` |
| `NIEWAŻNY: {reason}` (postęp) | `INVALID: {reason}` |
| `{label}: dryf przebiegu kontrolnego — blok nieważny` / `— powtarzam blok` | `{label}: control run drift - block invalid` / `- repeating the block` |
| `seria przerwana: {e}` (stderr `main`) | `series interrupted: {e}` |
| `przebiegi: …, nieważne: …, przerwane bloki: …; wyniki: …` | `runs: …, invalid: …, interrupted blocks: …; results: …` |
| `błąd: {e}` | `error: {e}` |
| `za mało rdzeni: N = {n} wymaga {k} wątków, jest {c}` | `not enough CPUs: N = {n} needs {k} threads, found {c}` |
| etykieta ziarna `rozgrzewka-{w}`, rodzaj przebiegu `rozgrzewka` | `warmup-{w}`, `warmup` (decyzja planu 10) |

**Komunikaty dopasowywane przez testy:**

| test | dopasowanie było | komunikat po zmianie zawiera |
|---|---|---|
| `test_config.py` | `nieznane klucze` | `unknown keys` |
| `test_config.py`, `test_orchestrator.py` | `brak wymaganych kluczy` | `missing required keys` |
| `test_config.py` | `scenarios: niepusta lista` | `scenarios: expected a non-empty list` |
| `test_config.py` | `słownika` (dwa miejsca) | `expected a mapping` |
| `test_config.py` | `plan 3b` | `the algorithm parameter is not supported yet (plan 3b-3)` (dopasowanie `plan 3b` zostaje) |
| `test_config.py` | `nieznana operacja` | `unknown operation` |
| `test_config.py` | `jednym zbiorze` | `single dataset` |
| `test_config.py` | `parze zbiorów` | `pair of datasets` |
| `test_config.py` | `powtórzony scenariusz` | `repeated scenario` |
| `test_config.py` | `variants: nieznane` | `variants: unknown` |
| `test_config.py` | `variants: niepusta lista` | `variants: expected a non-empty list` |
| `test_config.py` | `powtórzone` | `repeated items` |
| `test_config.py` | `żadnego bloku` | `no blocks` |
| `test_config.py` | `niepoprawny YAML` | `invalid YAML` |
| `test_config.py` | `python -m bench.data.download` | `missing data (python -m bench.data.download)` |
| `test_engines.py` | `nieznany wariant` | `unknown variant` |
| `test_engines.py`, `test_orchestrator.py` | `brak binarki …cargo build --release` | `missing binary …cargo build --release` |
| `test_procutil.py` | `proces serwer_testowy zakończył się \(kod 3\)` | `process test_server exited \(code 3\)` |
| `test_procutil.py` | `nie nasłuchuje` | `not listening` |
| `test_metrics.py` | `jednostce` | `unexpected unit` |
| `test_checksum.py` | `nieznana operacja` | `unknown operation` |
| `test_results.py` | `wiersz wyniku` | `result row` |
| `test_download.py` | `niekompletne` | `incomplete` |
| `test_datasets.py` | `niepoprawny identyfikator` | `invalid data identifier` |
| `test_sail_parquet.py` | `wymaga` | `requires` (`sail_bio`: `{op} requires one table` / `two tables`) |
| `test_orchestrator.py` (`fake_runner.py`) | `kod wyjścia 1: silnik padł: błąd testowy` | `exit code 1: engine crashed: test error` |
| `test_orchestrator.py` | `executor nie wstał` | `executor did not start` |
| `test_validity.py` | stderr `bench_client: brak danych: /x` | `bench_client: missing data: /x` |

- [ ] **Step 1: Zmień oczekiwania testów** z obu tabel (komunikaty, wartości `invalid_reason`, nazwy plików i katalogów). Dotyczy plików testów z listy **Files** i `tests/bench/fake_runner.py`.

- [ ] **Step 2: Uruchom — testy mają nie przejść**

Run: `uv run pytest tests/bench tests/test_polars_bio_runner.py tests/test_sail_runner.py tests/test_sail_parquet.py -q`

Expected: porażki wyłącznie z powodu niedopasowanych tekstów (`Regex pattern did not match`, asercje na komunikatach i wartościach `invalid_reason`), bo kod mówi jeszcze po polsku.

- [ ] **Step 3: Przetłumacz kod** z listy **Files** według tabel i słownika: komunikaty, `help`/`description` w argparse, nagłówek tabeli w `bench/data/download.py` (`zbiór`, `pliki`, `wiersze` → `dataset`, `files`, `rows`), postęp, wartości wyników. Docstringi i komentarze zostają.

- [ ] **Step 4: Testy przechodzą**

Run: `uv run pytest tests/bench tests/test_polars_bio_runner.py tests/test_sail_runner.py tests/test_sail_parquet.py tests/test_orchestrator_integration.py -q`

Expected: wszystkie przechodzą.

- [ ] **Step 5: Strażnik i przegląd słów dla plików zadania**

```bash
uv run python -c "
from tests.test_code_language import REPO, violations
import sys
files = [p for p in sys.argv[1:]]
left = [(f, v) for f in files for v in violations(REPO / f)]
print(len(left), left[:20])" bench/*.py bench/data/*.py bench/runners/*.py sail_bio.py tests/bench/*.py \
  tests/test_orchestrator_integration.py tests/test_polars_bio_runner.py tests/test_sail_runner.py tests/test_sail_parquet.py
uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py bench/*.py bench/data/*.py bench/runners/*.py \
  sail_bio.py tests/bench/*.py tests/test_orchestrator_integration.py tests/test_polars_bio_runner.py tests/test_sail_runner.py tests/test_sail_parquet.py
```

Expected:
- `0 []`;
- `polish_words.py` wypisuje wyłącznie fałszywe trafienia (przejrzyj każde i zapisz w ledgerze, jeśli zostaje).

- [ ] **Step 6: Pełny pakiet**

Run: `uv run pytest -m "not real_data" -q`

Expected: `341 passed, 1 xfailed, 15 deselected`.

- [ ] **Step 7: Commit**

```bash
git add bench sail_bio.py tests
git commit -m "Plan 3b-1 (etap 6): komunikaty i wartosci wynikow narzedzia pomiarowego po angielsku

invalid_reason, postep orkiestratora, bledy konfiguracji i pomoc CLI w bench/ i sail_bio.py;
testy dopasowujace komunikaty i nazwy katalogow w testach zmienione razem z kodem.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Tłumaczenie — Rust (`ballista_genomics/src`) i testy dopasowujące jego komunikaty (etap 6, część 3)

**Files:**
- Modify: wszystkie pliki `ballista_genomics/src/**/*.rs` z polskim tekstem poza komentarzami: `bin/ballista_node.rs`, `bin/bench_client.rs`, `bin/coverage_local.rs`, `bin/dist_ops.rs`, `bin/merge_local.rs`, `bin/nearest_local.rs`, `bin/subtract_local.rs`, `bio_phys_codec.rs`, `checksum.rs`, `cli.rs`, `codec_io.rs`, `coverage_node.rs`, `dist_provider.rs`, `dist_udtf.rs`, `physical_codec.rs`, `runner.rs`, `scenario.rs`
- Modify (testy): `tests/test_ballista_parquet.py`, `tests/test_bench_client_protocol.py`, `tests/test_ballista_multiprocess.py` (tylko asercje na wyjściu binarek)

**Interfaces:**
- Consumes: `tests.test_code_language`, `polish_words.py`.
- Produces: wyjście binarek po angielsku. `usage:` zamiast `użycie:`; `Connecting to external Ballista scheduler: {url}` zamiast `Łączenie z zewnętrznym schedulerem Ballisty: {url}`.

**Polecenie testów zadania:** `uv run pytest -m "not real_data" -q`

Uwaga: w Step 4 budowanie Rusta — wtedy **żadnych** testów.

**Komunikaty Rusta** (napisy i makra `println!`/`eprintln!`/`format!`, stałe `USAGE`):

| plik | było | jest |
|---|---|---|
| `bin/ballista_node.rs` | `ballista_node: scheduler nasłuchuje na {addr}` | `ballista_node: scheduler listening on {addr}` |
| `bin/ballista_node.rs` | `ballista_node: executor (flight {}, grpc {}, katalog {}, sloty {}, kodery: {})` | `ballista_node: executor (flight {}, grpc {}, work dir {}, slots {}, codecs: {})` |
| `bin/ballista_node.rs` | `NIE (kontrola negatywna)` | `NO (negative control)` |
| `bin/ballista_node.rs` | `podaj rolę: scheduler albo executor` | `role required: scheduler or executor` |
| `bin/ballista_node.rs`, `bin/bench_client.rs` | `błąd: {msg}\n{USAGE}` | `error: {msg}\n{USAGE}` |
| `bin/ballista_node.rs`, `bin/bench_client.rs` | `const USAGE: &str = "użycie: …"` | `"usage: …"`; `<ŚCIEŻKA>` → `<PATH>`, `<PLIK.parquet>` → `<FILE.parquet>`, `<OPERACJA>` → `<OPERATION>`, `<kontig,start,koniec>` → `<contig,start,end>`, opisy w linijkach po angielsku |
| `bin/bench_client.rs` | `nieznana operacja: {op_name}` | `unknown operation: {op_name}` |
| `bin/bench_client.rs` | `--checksum nie łączy się z {other}` | `--checksum cannot be combined with {other}` |
| `bin/bench_client.rs` | `--cols: trzy nazwy oddzielone przecinkami, dostałem: {cols_raw}` | `--cols: expected three comma-separated names, got: {cols_raw}` |
| `bin/bench_client.rs` | `--cols: pusta nazwa kolumny w {cols_raw}` | `--cols: empty column name in {cols_raw}` |
| `bin/bench_client.rs` | `brak VmHWM w /proc/self/status` | `no VmHWM in /proc/self/status` |
| `bin/*_local.rs` | `  Faza C: <op> (lokalnie, datafusion-bio-function-ranges)` | `  Phase C: <op> (local, datafusion-bio-function-ranges)` |
| `bin/*_local.rs`, `runner.rs` | `Czas: {:.4}s` | `Time: {:.4}s` |
| `bin/*_local.rs` | `Wynik zapisany do output/<op>_local_result.csv` | `Result written to output/<op>_local_result.csv` |
| `bin/dist_ops.rs` | `użycie: dist_ops <…>, dostałem: {:?}` | `usage: dist_ops <…>, got: {:?}` |
| `bio_phys_codec.rs` | `SubtractExec: brak lewego wejscia` / `prawego` | `SubtractExec: missing left input` / `right input` |
| `bio_phys_codec.rs` | `NearestExec:` / `DistCoverageExec:` / `MergeExec: brak wejscia` | `…: missing input` |
| `bio_phys_codec.rs` | `BioRangesPhysicalCodec: nieznany tag operacji {other}` | `BioRangesPhysicalCodec: unknown operation tag {other}` |
| `checksum.rs` | `suma kontrolna {}: brak kolumny {name} w wyniku` | `checksum {}: no column {name} in the result` |
| `cli.rs` | `brak wartości dla {arg}` | `missing value for {arg}` |
| `cli.rs` | `nieznany argument: {arg}` | `unknown argument: {arg}` |
| `cli.rs` | `powtórzony argument: {arg}` | `repeated argument: {arg}` |
| `cli.rs` | `brak wymaganej flagi {name}` | `missing required flag {name}` |
| `cli.rs` | `niepoprawna wartość flagi {name}` | `invalid value for flag {name}` |
| `codec_io.rs` | `codec_io: bufor obcięty (…)` | `codec_io: truncated buffer (…)` |
| `codec_io.rs` | `codec_io: niepoprawny UTF-8: {e}` | `codec_io: invalid UTF-8: {e}` |
| `coverage_node.rs` | `DistCoverageExec: brak {what}` (+ wartości `what` w wywołaniach) | `DistCoverageExec: missing {what}` (wartości `what` po angielsku) |
| `dist_provider.rs` | `brak danych tabeli {}: {}` | `no data for table {}: {}` |
| `dist_udtf.rs` | `{fname}(): argument {idx} musi być literałem tekstowym / całkowitoliczbowym / logicznym, dostałem: {other}` | `{fname}(): argument {idx} must be a string / an integer / a boolean literal, got: {other}` |
| `dist_udtf.rs` | `{fname}(): brakuje argumentu {idx}` | `{fname}(): missing argument {idx}` |
| `physical_codec.rs` | `IntervalJoinPhysicalCodec: nie udało się odtworzyć ColIntervals z filter` | `IntervalJoinPhysicalCodec: could not rebuild ColIntervals from the filter` |
| `runner.rs` | `{TARGET_PARTITIONS_ENV}: liczba całkowita ≥ 2, jest {v:?}` | `{TARGET_PARTITIONS_ENV}: expected an integer ≥ 2, got {v:?}` |
| `runner.rs` | tytuły `dist_<op> w pełni rozproszony (…)` | `dist_<op>, fully distributed (…)`: `hash shuffle by chrom`, `two-sided hash shuffle`, `broadcast of the left table`, `broadcast + carrier node`; overlap: `dist_overlap + COITrees, fully distributed` |
| `runner.rs` | `Łączenie z zewnętrznym schedulerem Ballisty: {url}` | `Connecting to external Ballista scheduler: {url}` |
| `runner.rs` | `Łączenie z Ballista standalone (scheduler + executor in-proc)...` | `Connecting to Ballista standalone (scheduler + executor in-proc)...` |
| `runner.rs` | `Klaster Ballista gotowy.` | `Ballista cluster ready.` |
| `runner.rs` | `Wynik zapisany do {}` | `Result written to {}` |
| `runner.rs` | `Plan rozproszony (EXPLAIN ANALYZE) zapisany do {}` | `Distributed plan (EXPLAIN ANALYZE) written to {}` |
| `runner.rs` | `UWAGA: EXPLAIN ANALYZE nie wykonało się: {e}` / `nie sparsowało się` | `WARNING: EXPLAIN ANALYZE failed: {e}` / `could not be parsed` |
| `scenario.rs` | `merge działa na jednej tabeli — bez --right` | `merge takes a single table (no --right)` |
| `scenario.rs` | `{} wymaga --right` | `{} requires --right` |

**Testy dopasowujące wyjście binarek:**

| plik:linia (stan przed zmianą) | było | jest |
|---|---|---|
| `tests/test_ballista_parquet.py:86` | `"użycie" in r.stderr` | `"usage" in r.stderr` |
| `tests/test_bench_client_protocol.py:62` | `"użycie" in r.stderr` | `"usage" in r.stderr` |
| `tests/test_ballista_multiprocess.py:208` | `"użycie" in r.stderr, f"{args}: brak instrukcji użycia: …"` | `"usage" in r.stderr, f"{args}: no usage text: …"` |
| `tests/test_ballista_multiprocess.py:407` | `"zewnętrznym schedulerem" in result.stdout` | `"external Ballista scheduler" in result.stdout` |

- [ ] **Step 1: Zmień oczekiwania testów** z tabeli wyżej.

- [ ] **Step 2: Uruchom — testy mają nie przejść**

Run: `uv run pytest tests/test_ballista_parquet.py tests/test_bench_client_protocol.py "tests/test_ballista_multiprocess.py::test_ballista_node_rejects_bad_arguments" "tests/test_ballista_multiprocess.py::test_operation_runs_distributed_across_processes" -q`

Expected: porażki na `"usage"` i `"external Ballista scheduler"` (binarki wypisują jeszcze polskie teksty).

- [ ] **Step 3: Przetłumacz napisy w plikach `.rs`** według tabeli. Komentarze `//`, `///`, `//!` zostają po polsku. Napisy niewymienione w tabeli tłumacz według słownika (strażnik i `polish_words.py` wskażą resztę).

- [ ] **Step 4: Przebuduj** (bez testów w tym czasie)

```bash
cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | grep -E "warning: unused|error|Finished"; cd ..
```

Expected: `Finished …`, bez błędów i bez nowych ostrzeżeń.

- [ ] **Step 5: Testy przechodzą; strażnik i przegląd słów dla Rusta**

```bash
uv run pytest tests/test_ballista_parquet.py tests/test_bench_client_protocol.py tests/test_ballista_multiprocess.py -q
uv run python -c "
from tests.test_code_language import REPO, violations
left = [(str(p.relative_to(REPO)), v) for p in sorted((REPO / 'ballista_genomics/src').rglob('*.rs')) for v in violations(p)]
print(len(left), left[:20])"
uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py $(git ls-files 'ballista_genomics/src/*.rs')
```

Expected:
- testy przechodzą;
- strażnik wypisuje `0 []`;
- `polish_words.py` daje wyłącznie fałszywe trafienia (przejrzyj każde).

- [ ] **Step 6: Pełny pakiet**

Run: `uv run pytest -m "not real_data" -q`

Expected: `341 passed, 1 xfailed, 15 deselected`.

- [ ] **Step 7: Commit**

```bash
git add ballista_genomics/src tests/test_ballista_parquet.py tests/test_bench_client_protocol.py tests/test_ballista_multiprocess.py
git commit -m "Plan 3b-1 (etap 6): komunikaty i pomoc binarek Ballisty po angielsku

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Tłumaczenie — pozostały kod Pythona; strażnik bez xfail (etap 6, część 4)

**Files:**
- Modify: pozostałe pliki `.py` z polskim tekstem poza komentarzami i docstringami, zgodnie z listą wypisaną w Step 1. Według prototypu:
  - `tests/test_ballista_distributed_ops.py`, `tests/test_ballista_distribution_evidence.py`, `tests/test_ballista_multiprocess.py` (reszta), `tests/test_ballista_overlap.py`;
  - `tests/test_coverage_subtract_correctness.py`, `tests/test_merge_correctness.py`, `tests/test_nearest_correctness.py`;
  - `tests/test_sail_parallel_udtf.py`, `tests/test_env_isolation.py`, `tests/test_real_data.py`;
  - `tests/*_oracle.py`, `tests/generic_oracle.py`, `tests/conftest.py`;
  - `sail_coverage_subtract_udtf.py`, `sail_merge_udtf.py`, `sail_nearest_udtf.py`, `sail_overlap_udtf.py`, `sail_pb_guard.py`, `overlap_comparison.py`.
- Modify: `tests/test_code_language.py` (usunięcie `xfail`)

**Interfaces:**
- Consumes: `tests.test_code_language`, `polish_words.py`.
- Produces: `test_source_code_outside_comments_is_english` bez oznaczeń, zielony — od tej chwili chroni repozytorium także w CI.

**Polecenie testów zadania:** `uv run pytest -m "not real_data" -q`

- [ ] **Step 1: Lista pozostałych miejsc**

```bash
uv run python -c "
from tests.test_code_language import REPO, source_files, violations
left = {str(p.relative_to(REPO)): len(violations(p)) for p in source_files() if violations(p)}
print(sum(left.values()), left)"
uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py | cut -d: -f1 | sort | uniq -c
```

Expected: wyłącznie pliki z listy **Files** (plus ewentualne fałszywe trafienia z Zadań 7–8 zapisane w ledgerze).

- [ ] **Step 2: Usuń oznaczenie xfail** z `test_source_code_outside_comments_is_english` w `tests/test_code_language.py` (linia `@pytest.mark.xfail(strict=True, reason="translation in progress (plan 3b-1, tasks 7-9)")`).

- [ ] **Step 3: Uruchom — strażnik ma nie przejść**

Run: `uv run pytest tests/test_code_language.py -q`

Expected: `1 failed, 6 passed` — lista pozostałych fragmentów z polskimi znakami.

- [ ] **Step 4: Przetłumacz pozostałe pliki** według słownika: komunikaty asercji, `print` w skryptach `sail_*_udtf.py` i `overlap_comparison.py`, komunikaty `pytest.skip`/`pytest.fail`, nazwy procesów i katalogów w testach (np. `executor_zly` → `executor_bad` razem z `match="executor_bad"`). Przetłumacz też trafienia `polish_words.py` w tych plikach.

- [ ] **Step 5: Strażnik przechodzi, przegląd słów pusty**

```bash
uv run pytest tests/test_code_language.py -q
uv run python .superpowers/sdd/2026-10-02-srodowisko-uv-ci-3b1/polish_words.py
```

Expected:
- `7 passed`;
- `polish_words.py` wypisuje tylko fałszywe trafienia zapisane w ledgerze, a każde nowe przejrzyj.

- [ ] **Step 6: Pełny pakiet**

Run: `uv run pytest -m "not real_data" -q`

Expected: `342 passed, 15 deselected`.

- [ ] **Step 7: Commit**

```bash
git add tests sail_*.py overlap_comparison.py
git commit -m "Plan 3b-1 (etap 6): reszta kodu Pythona po angielsku; straznik konwencji jezykowej wlaczony

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Znacznik `nodes(n)`, CI na GitHub Actions i README (etap 7)

**Files:**
- Create: `tests/test_nodes_marker.py`, `.github/workflows/tests.yml`, `README.md`
- Modify: `tests/conftest.py`, `tests/bench/test_engines.py`, `tests/test_orchestrator_integration.py`

**Interfaces:**
- Consumes: `bench.engines.SYSTEM_CPUS: tuple[int, ...] = (0, 1)`, `bench.engines.cluster_cpus(n_nodes: int) -> tuple[int, ...]` (CPU węzłów 1..n).
- Produces:
  - znacznik `@pytest.mark.nodes(n)`: hook `tests.conftest.pytest_runtest_setup(item)` pomija test przez `pytest.skip`, gdy któregoś z CPU `SYSTEM_CPUS ∪ cluster_cpus(n)` nie ma w `os.sched_getaffinity(0)`;
  - workflow `tests`; README z odznaką.

**Polecenie testów zadania:** `uv run pytest -m "not real_data" -q`

- [ ] **Step 1: Testy znacznika**

`tests/test_nodes_marker.py`:

```python
"""Znacznik nodes(n) (plan 3b-1, etap 7): test przypinający procesy do węzłów 1..n jest pomijany,
gdy procesowi brakuje CPU rdzenia systemowego albo któregoś z węzłów — np. runner CI z 4 vCPU
przy N = 2 (specyfikacja metodyki, sekcja 3: węzeł k = CPU {2k, 2k+1})."""

from __future__ import annotations

import os

import pytest

from tests import conftest


class _Item:
    """Minimalny zamiennik pytest.Item — hook używa tylko get_closest_marker."""

    def __init__(self, mark):
        self._mark = mark

    def get_closest_marker(self, name):
        return self._mark if self._mark is not None and self._mark.name == name else None


def _cpus(monkeypatch, cpus) -> None:
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: set(cpus))


def test_two_nodes_are_skipped_on_four_cpus(monkeypatch):
    _cpus(monkeypatch, range(4))
    with pytest.raises(pytest.skip.Exception, match=r"CPUs 0-5.*\[4, 5\]"):
        conftest.pytest_runtest_setup(_Item(pytest.mark.nodes(2).mark))


def test_one_node_runs_on_four_cpus(monkeypatch):
    _cpus(monkeypatch, range(4))
    conftest.pytest_runtest_setup(_Item(pytest.mark.nodes(1).mark))


def test_unmarked_test_ignores_affinity(monkeypatch):
    _cpus(monkeypatch, [])
    conftest.pytest_runtest_setup(_Item(None))
```

- [ ] **Step 2: Uruchom — testy mają nie przejść**

Run: `uv run pytest tests/test_nodes_marker.py -q`

Expected: `3 failed` — `AttributeError: module 'tests.conftest' has no attribute 'pytest_runtest_setup'`.

- [ ] **Step 3: Hook i rejestracja znacznika w `tests/conftest.py`**

`pytest_configure` zamień na (dopisana druga linia):

```python
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_data: needs the downloaded databio-8p dataset (python -m bench.data.download)",
    )
    config.addinivalue_line(
        "markers",
        "nodes(n): pins processes to emulated nodes 1..n (CPUs 0..2n+1); "
        "skipped when the process cannot use all of them",
    )


def pytest_runtest_setup(item):
    """Znacznik nodes(n): test przypinający procesy do węzłów 1..n jest pomijany, gdy procesowi
    brakuje któregoś z CPU rdzenia systemowego i węzłów (specyfikacja 3b-1, sekcja 6)."""
    marker = item.get_closest_marker("nodes")
    if marker is None:
        return
    from bench.engines import SYSTEM_CPUS, cluster_cpus

    n = marker.args[0]
    missing = sorted({*SYSTEM_CPUS, *cluster_cpus(n)} - os.sched_getaffinity(0))
    if missing:
        pytest.skip(f"needs CPUs 0-{2 * n + 1} for {n} node(s); unavailable: {missing}")
```

Run: `uv run pytest tests/test_nodes_marker.py -q`

Expected: `3 passed`.

- [ ] **Step 4: Oznacz testy przypinające procesy**

- `tests/bench/test_engines.py`:
  - `@pytest.mark.nodes(2)` nad `test_ballista_cluster_is_pinned_and_configured` i `test_sail_server_is_pinned_and_configured`;
  - `@pytest.mark.nodes(1)` nad `test_ballista_start_retries_when_executor_registration_fails` i `test_ballista_restart_replaces_processes`.
- `tests/test_orchestrator_integration.py`:
  - `@pytest.mark.nodes(2)` nad każdym testem używającym fixture `series`: `test_every_planned_run_is_recorded`, `test_every_run_is_valid`, `test_threads_follow_variant_and_n`, `test_every_runner_reports_checksum_time`, `test_memory_is_measured_per_process`, `test_ballista_shuffle_volume_is_measured`;
  - `@pytest.mark.nodes(1)` nad `test_main_resolves_relative_data_root`.

Run: `uv run pytest -m "not real_data" -q`

Expected: `345 passed, 15 deselected`. Na 12 wątkach nic nie jest pomijane.

- [ ] **Step 5: Próba CI lokalnie — 4 CPU i brak danych** (Review Focus 3 i 4)

```bash
BENCH_DATA_ROOT=$(mktemp -d) taskset -c 0-3 uv run pytest -m "not real_data" -q -ra 2>&1 | tail -15
```

Expected:
- `337 passed, 8 skipped, 15 deselected`;
- 8 linii `SKIPPED … needs CPUs 0-5 for 2 node(s); unavailable: [4, 5]`.

Porażka z `taskset: failed to set pid …'s affinity` wskazuje test bez znacznika — dodaj go (ruling w ledgerze) i powtórz.

- [ ] **Step 6: Workflow `.github/workflows/tests.yml`**

```yaml
# Testy poprawności (specyfikacja 3b-1, sekcja 6): budowanie Ballisty i pakiet testów bez danych
# rzeczywistych. CI służy wyłącznie sprawdzaniu poprawności - nigdy pomiarom.
name: tests

on:
  push:
    branches: [master]
  pull_request:
    branches: [master]
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

permissions:
  contents: read

jobs:
  tests:
    runs-on: ubuntu-24.04
    timeout-minutes: 60
    env:
      CARGO_TERM_COLOR: always
      # Bez debuginfo (Cargo.toml ma już debug = false; zmienna chroni przed zmianą profilu).
      CARGO_PROFILE_DEV_DEBUG: "0"
    steps:
      - uses: actions/checkout@v7

      # Rust z ballista_genomics/rust-toolchain.toml (przed cache - klucz zawiera wersję rustc).
      - name: Rust toolchain
        working-directory: ballista_genomics
        run: |
          rustup toolchain install "$(sed -n 's/^channel = "\(.*\)"$/\1/p' rust-toolchain.toml)" --profile minimal
          rustc --version

      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: ballista_genomics -> target
          cache-on-failure: true

      - name: Build Ballista binaries
        working-directory: ballista_genomics
        run: cargo build --bins --locked

      - name: Disk usage
        run: df -h / && du -sh ballista_genomics/target

      # Wersja uv z required-version w pyproject.toml.
      - uses: astral-sh/setup-uv@v10
        with:
          enable-cache: true
          cache-python: true

      - name: Python environment
        run: uv sync --locked

      - name: Tests (without real data)
        run: uv run --locked pytest -m "not real_data" -ra
```

Run: `uv run python -c "import yaml; d = yaml.safe_load(open('.github/workflows/tests.yml')); print(sorted(d[True]), [s.get('name', s.get('uses')) for s in d['jobs']['tests']['steps']])"`

Expected:
- `['pull_request', 'push', 'workflow_dispatch']` (PyYAML czyta klucz `on` jako `True`);
- lista 8 kroków w kolejności z pliku.

- [ ] **Step 7: `README.md`**

````markdown
# distributed-polars-bio

[![tests](https://github.com/mkowalewski1282/distributed-polars-bio/actions/workflows/tests.yml/badge.svg?branch=master)](https://github.com/mkowalewski1282/distributed-polars-bio/actions/workflows/tests.yml)

Kod pracy magisterskiej „Porównanie udostępnianych na otwartych licencjach rozproszonych silników
zapytań pod kątem analiz danych genomicznych” (promotor: dr inż. Marek Wiewiórka). Operacje na
przedziałach genomowych z [polars-bio](https://github.com/biodatageeks/polars-bio) (`overlap`,
`nearest`, `coverage`, `merge`, `subtract`) wykonywane w Apache Ballista (`ballista_genomics/`)
i w Sailu (`sail_bio.py`) oraz narzędzie pomiarowe (`bench/`).

## Środowisko

Wymagania: Linux x86_64 (narzędzie używa `taskset` i `/proc`), [uv](https://docs.astral.sh/uv/)
0.12.22 i [rustup](https://rustup.rs/).

```bash
curl -LsSf https://astral.sh/uv/0.12.22/install.sh | sh   # uv w ~/.local/bin
uv sync                                                   # Python 3.12 i biblioteki z uv.lock w .venv/
```

Polecenia uruchamia się przez `uv run …` albo po `source .venv/bin/activate`; w VS Code jako
interpreter trzeba wybrać `.venv/bin/python`. Pakiet `bench` nie działa pod innym Pythonem niż
3.12 ze środowiska projektu — systemowy `python3` ma inne wersje bibliotek.

## Budowanie Ballisty

```bash
cd ballista_genomics
CARGO_BUILD_JOBS=1 cargo build --bins     # wersja Rusta z rust-toolchain.toml
```

`CARGO_BUILD_JOBS=1` chroni maszynę z małą ilością pamięci (WSL, 5 GB) przed jej wyczerpaniem
przy kompilacji; na większej maszynie można je pominąć.

## Testy

```bash
uv run pytest -m "not real_data"          # cały pakiet poza danymi rzeczywistymi (jak w CI)
uv run python -m bench.data.download      # zbiór databio-8p w ~/bench_data (albo $BENCH_DATA_ROOT)
uv run pytest -m real_data                # testy na danych rzeczywistych
```

Testy przypinające procesy do emulowanych węzłów (`@pytest.mark.nodes(n)`) potrzebują CPU
0..2n+1; na maszynie z mniejszą liczbą CPU są pomijane z podaniem powodu.

CI (GitHub Actions, `.github/workflows/tests.yml`) buduje Ballistę i uruchamia pakiet bez danych
rzeczywistych. CI służy wyłącznie sprawdzaniu poprawności, nigdy pomiarom.

## Narzędzie pomiarowe

```bash
uv run python -m bench.orchestrator bench/conf/smoke.yaml --ballista-profile debug
```

Wyniki trafiają do `bench/results/<seria>/<czas>/runs.parquet`. Metodyka:
`docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`.

## Dokumentacja

- `raporty/sprawozdanie_stanu_prac.md` — stan prac;
- `ballista_genomics/OPIS.md` — integracja z Ballistą;
- `ballista_genomics/vendor/PATCH.md` — łatki w zwendorowanej bibliotece algorytmów;
- `wnioski_claude.md` — obserwacje z implementacji;
- rysunki do raportów: `uv run --with matplotlib python raporty/img/generuj_rysunki.py`.
````

W komentarzu `bench/conf/smoke.yaml` zamień `python -m bench.orchestrator bench/conf/smoke.yaml` na `uv run python -m bench.orchestrator bench/conf/smoke.yaml`.

- [ ] **Step 8: Pełny pakiet i commit**

Run: `uv run pytest -m "not real_data" -q`

Expected: `345 passed, 15 deselected`.

```bash
git add tests/test_nodes_marker.py tests/conftest.py tests/bench/test_engines.py tests/test_orchestrator_integration.py \
  .github/workflows/tests.yml README.md bench/conf/smoke.yaml
git commit -m "Plan 3b-1 (etap 7): CI na GitHub Actions, znacznik nodes(n) i README

Testy przypinajace procesy do wezlow sa pomijane, gdy brakuje ich CPU (runner z 4 vCPU).
CI: Rust z rust-toolchain.toml, cache budowania, uv sync --locked, pytest bez danych rzeczywistych.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 9: STOP — zgoda na push (CI działa tylko na GitHubie)**

Zapytaj użytkownika:

> Workflow jest gotowy, ale CI uruchomi się dopiero na GitHubie. Wyzwalacze to push i PR do `master` oraz ręczne uruchomienie.
> - **(a) — rekomendowane:** wypycham gałąź `3b-1-uv-ci`, a Ty otwierasz pull request do `master` z linku, który wypisze `git push` (`gh` nie jest zainstalowane). CI uruchomi się na PR-ze; ja śledzę wynik.
> - **(b):** nic teraz nie wypycham; CI sprawdzimy po scaleniu z `master` na końcu planu.
>
> Którą opcję wybierasz?

Przy (a):

```bash
git push -u origin 3b-1-uv-ci
# po otwarciu PR przez użytkownika, co kilka minut:
curl -s "https://api.github.com/repos/mkowalewski1282/distributed-polars-bio/actions/runs?branch=3b-1-uv-ci&per_page=1" \
  | python3 -c "import json,sys; r=json.load(sys.stdin)['workflow_runs'][0]; print(r['status'], r['conclusion'], r['html_url'])"
```

Expected: ostatecznie `completed success <url>`.

Gdy CI jest czerwone:
- przyczynę czytaj w logu pod `html_url`;
- test przechodzący lokalnie, a padający w CI, traktuj jak błąd (zależność od lokalnych artefaktów albo od liczby CPU) według superpowers:systematic-debugging;
- poprawki to kolejne commity na gałęzi.

Przekroczenie 60 minut przy pierwszym, zimnym budowaniu to ruling: podnieś `timeout-minutes` (np. do 90) z uzasadnieniem w komunikacie commitu.

Brak miejsca na dysku przy budowaniu (`No space left on device`; specyfikacja, ryzyko „14 GB dysku runnera”) to również ruling. Przed krokiem `Build Ballista binaries` dodaj krok:

```yaml
      - name: Free disk space
        run: sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc && df -h /
```

---

### Task 11: Weryfikacja na danych rzeczywistych i dokumentacja

**Files:**
- Modify:
  - `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` (sekcja 3: oprogramowanie; sekcja 8.6: smoke; sekcja 12);
  - `raporty/polars_bio_blad_partycji.md`, `wnioski_claude.md`, `docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md` (sprostowanie o 22 GB).
- Poza repozytorium: pamięć projektu (`project_mgr.md`, `MEMORY.md`).

**Interfaces:**
- Consumes: cały stan po Zadaniach 1–10, dane `~/bench_data/databio-8p`.
- Produces: kryteria ukończenia 1–6 ze specyfikacji (sekcja 8) sprawdzone i opisane.

**Polecenie testów zadania:** `uv run pytest -q` (cały pakiet, z danymi rzeczywistymi — po checkliście RAM)

- [ ] **Step 1: STOP — checklista RAM**

Przypomnij użytkownikowi checklistę z pamięci `feedback_ram_przed_eksperymentem` i **poczekaj na potwierdzenie**:
1. zamknij VS Code (albo uruchamiaj z Windows Terminal) i aplikacje w tle: SteelSeries GG, Bitwarden; wstrzymaj OneDrive;
2. `.wslconfig` 5 GB RAM + 4 GB swapu musi być aktywny (po zmianie `wsl --shutdown`);
3. przebieg, w którym system sięgnął do swapu, jest nieważny.

- [ ] **Step 2: Testy na danych rzeczywistych**

Run: `uv run pytest -m real_data -q`

Expected: `13 passed, 2 xfailed, 345 deselected`. Dwa `xfailed` to Ballista standalone przy broadcaście (limit 4 MiB, specyfikacja 9.2) — bez zmian względem planu 3a.

- [ ] **Step 3: Smoke** (Ballista debug; kilkadziesiąt minut)

```bash
uv run python -m bench.orchestrator bench/conf/smoke.yaml --ballista-profile debug 2>&1 | tail -5
uv run python - "$(ls -d bench/results/smoke/*/ | tail -1)runs.parquet" <<'EOF'
import json, sys
import polars as pl
df = pl.read_parquet(sys.argv[1])
bad = df.filter(~pl.col("valid"))
print(df.height, "runs;", bad.height, "invalid")
print(bad.group_by("invalid_reason").len().sort("len", descending=True))
print("wrong results:", bad.filter(pl.col("invalid_reason").str.contains("checksum|row count")).height)
print(json.loads(df["engine_versions"][0]))
EOF
```

Expected:
- ostatnia linia orkiestratora: `runs: …, invalid: …, interrupted blocks: 0; results: …`;
- `wrong results: 0`; nieważne wyłącznie z powodu dryfu przebiegu kontrolnego (`control drift …`, `control run invalid` — sprawa planu 3b-2);
- wersje `{"polars_bio": "0.36.0", "pysail": "0.7.2", "pyspark": "4.2.0", "ballista": "53.0.0 (debug)", "datafusion_ballista": "53.0.0", "python": "3.12.…"}`.

Jeśli #372 nie zniknął (gałąź z Zadania 2), dopuszczalne są też nieważne `subtract` polars-bio A/B z `checksum … ≠ reference …`.

- [ ] **Step 4: Specyfikacja metodyki**

(a) W sekcji 3, po akapicie `**Maszyna:** …`, wstaw:

```markdown
**Oprogramowanie (od planu 3b-1):**
- Python 3.12 w środowisku uv (`pyproject.toml`, `uv.lock`);
- polars-bio 0.36, pysail 0.7.2, PySpark 4.2.0 (klient Spark Connect);
- Ballista 53.0.0 z DataFusion 53.0.0;
- `datafusion-bio-function-ranges` v0.22.2 — ta sama wersja algorytmów w polars-bio i w integracji
  z Ballistą (`tests/test_algorithm_versions.py`);
- Rust 1.95.0 (`ballista_genomics/rust-toolchain.toml`).

Dokładne wersje każdego przebiegu orkiestrator zapisuje w kolumnie `engine_versions`.
```

(b) W sekcji 8.6 zamień nawias zaczynający się od `(smoke 02.10.2026: poza dryfem przebiegu` (do `nie ma czego scalać);`) na wynik ze Step 3, np.:

```markdown
(smoke po planie 3b-1, <data>: poza dryfem przebiegu kontrolnego wszystkie przebiegi ważne);
```

(c) W sekcji 12 usuń punkt „nowe środowisko z Pythonem ≥ 3.11 i aktualnym polars-bio … do wyboru.” (zrobione w planie 3b-1).

- [ ] **Step 5: Notatka o #372, wnioski, sprostowanie w specyfikacji 3b-1**

(a) Na końcu sekcji „Stan w polars-bio 0.36.0” w `raporty/polars_bio_blad_partycji.md` dopisz wynik smoke na danych 1-2. Na przykład: „Smoke na danych 1-2 (<data>): `subtract` polars-bio A/B ma 209 940 wierszy przy 2, 4 i 6 partycjach — tyle co wzorzec; w planie 3a było 205 673, 202 854 i 201 506.” Liczby wierszy przeczytaj z `runs.parquet` (kolumna `rows`, `op == "subtract"`).

(b) W `wnioski_claude.md`, w sekcji „Plan 3b-1 — …”, dopisz punkt:

```markdown
5. **Weryfikacja końcowa (<data>).**
   - Testy na danych rzeczywistych: 13 passed, 2 xfailed (standalone przy broadcaście — bez
     zmian).
   - Smoke na parze 1-2: <wynik ze Step 3>.
   - CI na GitHub Actions: <zielone / adres przebiegu>.
   - Lokalne 22 GB w `ballista_genomics/target` to głównie nagromadzone stare artefakty
     (`target/debug/deps`), nie debuginfo — `Cargo.toml` od dawna ma `debug = false`. Świeże
     budowanie w CI zajmuje <du -sh z kroku „Disk usage”>.
```

(c) W `docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md`, sekcja 6, krok 4: zamień „lokalnie `target/debug` zajmuje 22 GB, głównie przez debuginfo” na „lokalnie `target/debug` zajmuje 22 GB, głównie przez nagromadzone stare artefakty; `Cargo.toml` już wyłącza debuginfo (`debug = false`), zmienna chroni przed zmianą profilu”.

- [ ] **Step 6: Pełny pakiet, commit**

Run: `uv run pytest -q`

Expected: `358 passed, 2 xfailed` — 345 bez danych rzeczywistych + 13, a dwa `xfailed` to standalone przy broadcaście. Dane, CPU i stan checklisty RAM jak w Step 1.

```bash
git add docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md \
  raporty/polars_bio_blad_partycji.md wnioski_claude.md
git commit -m "Plan 3b-1: weryfikacja na danych rzeczywistych i dokumentacja wersji

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 7: Pamięć projektu** (poza repozytorium)

W `~/.claude/projects/-home-milosz-praca-magisterska/memory/project_mgr.md` zapisz:
- plan 3b-1 wykonany: gałąź `3b-1-uv-ci`, commity, stan scalenia;
- stan #372, wersje i wyniki sond Saila;
- polecenia `uv run …`;
- że następny krok to brainstorming 3b-2.

W `MEMORY.md` zaktualizuj linię „Project context”.

---

## Po zadaniach

Dalej według wybranego trybu wykonania:
- przegląd całej gałęzi przez świeżego recenzenta (superpowers:executing-plans albo superpowers:subagent-driven-development);
- poprawki ważnych uwag;
- superpowers:finishing-a-development-branch.

Kryteria ukończenia (specyfikacja, sekcja 8):
1. `uv sync --locked` odtwarza środowisko na czystej maszynie — sprawdza to CI (Zadanie 10).
2. Pełny pakiet zielony lokalnie, łącznie z `real_data`; smoke daje oczekiwane wyniki (Zadanie 11).
3. CI na GitHubie zielone (Zadanie 10, Step 9; przy opcji (b) po scaleniu).
4. Ballista i polars-bio używają tej samej wersji `datafusion-bio-functions`, v0.22.2 (Zadanie 3, `tests/test_algorithm_versions.py`).
5. Stan #372 w polars-bio 0.36 ustalony i opisany (Zadanie 2).
6. Strażnik konwencji językowej przechodzi (Zadanie 9).

# Etap 4: aktualne wersje, trzy repozytoria i procedura 1/2/4/8 — plan wykonania

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Obie integracje na bieżących wersjach silników, narzędzie pomiarowe z semantyką P ∈ {1, 2, 4, 8}
i raportem, kod rozdzielony na trzy publiczne repozytoria z diagramami integracji.

**Architecture:** Wszystkie zmiany kodu (etapy A i B) powstają w `distributed-polars-bio` na gałęzi
`etap-4`, gdzie działa pełny pakiet testów. Podział (etap C) to powtarzalny skrypt: mapowanie plików
+ nakładki (README, pyproject, CI, licencje) + przepisanie kilku importów, uruchamiany na wskazanym
commicie. Repo testowe dołącza dwa pozostałe jako submoduły z względnymi URL-ami.

**Tech Stack:** Rust 1.95 (Ballista 55.0.0, DataFusion 55.2.0, vendor datafusion-bio-function-ranges
v0.25.0), Python 3.12 + uv 0.12.22 (polars-bio 0.37.0, pysail 0.7.3, pyspark[connect] 4.2.1, polars,
pyarrow 24, matplotlib), pytest 9, GitHub Actions, Mermaid.

**Spec:** `docs/superpowers/specs/2026-11-15-trzy-repozytoria-i-procedura-1248-design.md` (czytać razem
z planem; numery sekcji „spec X.Y” odnoszą się do niej).

## Global Constraints

- Wersje: Ballista `55.0.0`; `datafusion = "=55.2.0"`; vendor z tagu `v0.25.0`; `polars-bio==0.37.*`;
  `pysail==0.7.*` (0.7.3 w `uv.lock`); `pyspark[connect]==4.2.*` (4.2.1); `pyarrow` 24.x; Python 3.12.
- **Żadnych zmian w kodzie polars-bio** (fork, łatka, własny build). Używanie wewnętrznych API pb
  (`range_op.range_operation`, `BioSessionContext`, `_metadata`) jest dozwolone jak dotąd.
- **Commity bez stopki `Co-Authored-By`** ani żadnej atrybucji Claude. Komunikaty: w
  `distributed-polars-bio` po polsku bez diakrytyków (Task 1–13), w nowych repozytoriach **po angielsku**.
- Język: w Task 1–11 jak dotąd (kod po angielsku, komentarze po polsku; strażnik
  `tests/test_code_language.py`). Od Task 12 **wszystko, co trafia do nowych repozytoriów, po angielsku**:
  kod, komentarze, docstringi, komentarze w YAML/TOML, README, dokumentacja techniczna, komunikaty
  błędów i commitów. Dokumenty robocze (spec, plan) zostają po polsku.
- Nowe repozytoria: bez `docs/superpowers/`, `wnioski_claude.md`, `raporty/`, `ustalenia_zalozen.txt`,
  bez odniesień do planów i specyfikacji, bez linków do `distributed-polars-bio`.
- `git push` (gdziekolwiek) **tylko po wyraźnej zgodzie użytkownika** w tej rozmowie. Nowe
  repozytoria: kod wyłącznie przez gałąź `import` i PR do `main` (przegląd diffu na GitHubie),
  scalenie merge commitem przez użytkownika.
- Budowanie Rusta zawsze z `CARGO_BUILD_JOBS=1`; nie uruchamiać Pythona z polars-bio równolegle
  z buildem Rusta (5 GB RAM).
- Przed każdym pomiarem czasu (etap D, badanie szumu) **checklista RAM** (pamięć
  `feedback_ram_przed_eksperymentem`): zamknięty VS Code, aplikacje w tle, `.wslconfig` 5 GB, pusty
  swap; serię uruchamia użytkownik z Windows Terminal.
- Termin odcięcia: 2 dni przed terminem etapu (datę poda użytkownik). Po nim — sekcja „Ścieżka odcięcia”.
- Repozytoria lokalne po podziale: `/home/milosz/repos/{ballista-genomics,sail-genomics,genomics-engines-bench}`.

## Review Focus

1. **Maszyna bez SMT albo z maską affinity** (kontener, `taskset` na całym procesie, Intel z
   numeracją rodzeństwa i/i+N) — układ CPU ma używać tylko dozwolonych CPU, `smt` = False bez
   rodzeństwa. Testy: Task 7 (`test_layout_without_smt`, `test_layout_respects_affinity`,
   `test_layout_interleaved_numbering`).
2. **Maszyna z mniej niż 8 CPU poza rdzeniem systemowym** (CI: 4 vCPU) — za duże P pominięte
   z powodem, seria kończy się kodem 0, raport pokazuje lukę. Testy: Task 8
   (`test_oversized_parallelism_is_skipped_not_failed`), Task 9 (`test_missing_cell_shows_reason`).
3. **Przerwana seria** (jest `runs.jsonl`, brak `runs.parquet`) i seria bez ważnych przebiegów
   jednego silnika — raport ma się wygenerować. Test: Task 9 (`test_report_from_interrupted_series`).
4. **Binarki Ballisty w innym miejscu niż w monorepo** (submoduł `engines/ballista-genomics`) — błąd
   z poleceniem budowania zamiast `FileNotFoundError`. Test: Task 8 (`test_ballista_dir_prefers_submodule`).
5. **Ponowne uruchomienie skryptu podziału na istniejący katalog** — nie może nadpisać repozytoriów
   z historią. Test: Task 11 (`test_split_refuses_existing_output`).

---

## Ścieżka odcięcia (jeśli Task 4–5 nie są zielone w terminie)

Zadania 4–5 robić na osobnej gałęzi `etap-4-ballista55` (odgałęzionej od `etap-4` po Task 3).
Pozostałe zadania (6–14) idą na `etap-4`, które ma wtedy Ballistę 53 + vendor v0.22.2. Jeśli
w terminie odcięcia `etap-4-ballista55` nie przechodzi pakietu: Task 11–14 na `etap-4` bez
scalania Ballisty 55 (strażnik wersji ma już wyjątek v0.22.2 z Task 3); port dokończyć potem w
`/home/milosz/repos/ballista-genomics` (przeniesienie zmian z gałęzi jako zwykłe commity), przesunąć
submoduł w repo testowym, zaktualizować diagram Ballisty. Jeśli zielone przed terminem: scalić
`etap-4-ballista55` do `etap-4` (fast-forward/merge) przed Task 11.

---

## Etap A — wersje

### Task 1: Gałąź, tag i usunięcie ścieżek UDTF/blokady

**Files:**
- Delete: `sail_bio.py`, `sail_overlap_udtf.py`, `sail_merge_udtf.py`, `sail_nearest_udtf.py`,
  `sail_coverage_subtract_udtf.py`, `sail_pb_guard.py`, `bench/conf/sail_paths.yaml`,
  `tests/test_sail_parallel_udtf.py`, `tests/test_sail_parquet.py`
- Modify: `sail_arrow.py` (tylko tryb `context`), `bench/config.py` (warianty), `bench/engines.py`
  (`SAIL_PATHS`), `bench/runners/sail_runner.py` (`PATHS`, `--path`), `tests/test_sail_runner.py`,
  `tests/test_sail_arrow.py`, `tests/test_real_data.py`, `tests/bench/test_preflight.py`,
  `bench/conf/smoke.yaml`
- Create: `sail_session.py` (tymczasowo; w Task 2 przechodzi do pakietu)

**Interfaces:**
- Produces: `sail_session.local_session()` — context manager zwracający `SparkSession` na lokalnym
  serwerze Saila (treść 1:1 z `sail_bio.sail_session`); `VARIANTS` bez `sail_lock`, `sail_udtf`;
  `sail_runner` bez `--path` (zawsze applyInArrow z pulą).

- [ ] **Step 1:** `git switch master && git tag przed-etapem-4 && git switch -c etap-4` (tag lokalny).
- [ ] **Step 2:** Usunąć pliki z listy „Delete”. `test_sail_parquet.py` w całości dotyczy `sail_bio`
  (UDTF) — usuwamy; jego test pamięci LATERAL nie ma odpowiednika w applyInArrow.
- [ ] **Step 3:** `sail_arrow.py`: usunąć `MODES`, parametr `mode` w `evaluate`/`build_query`, gałąź
  `lock` i `wait_s`; `read_phases(path) -> tuple[float, int]` (suma `pb_s`, liczba wywołań). Plik faz
  zapisuje `{"pb_s": …}`.
- [ ] **Step 4:** `sail_runner.py`: usunąć `PATHS`, `--path`, ścieżkę `udtf`, `phases.lock_wait_s`;
  zostają `phases.first_batch_s`, `phases.pb_s`, `extra.udf_groups`, `extra.checksum_s`.
  `engines.py`: usunąć `SAIL_PATHS`; `SailEngine` bez parametru `path`. `config.py`:
  `VARIANTS = ("polars_bio_a", "polars_bio_b", "ballista", "sail")` (A/B usuwa Task 7).
- [ ] **Step 5:** Testy: `test_sail_runner.py` — parametryzacja tylko po operacji (bez `path`);
  `test_sail_arrow.py` — bez `mode`; `test_real_data.py::test_sail_on_1_2` — przez
  `sail_arrow.build_query` w sesji z `sail_session.local_session()` (zamiast `sail_bio.run_op`), wynik
  `.toArrow()` → `row_multiset`; `test_preflight.py` — `check_tools(["sail"])`; `smoke.yaml` bez
  usuniętych wariantów (jeśli były).
- [ ] **Step 6:** `grep -rnE "sail_bio|sail_pb_guard|sail_udtf|sail_lock|arrow-lock|udtf" --include=*.py --include=*.yaml . | grep -v .venv`
  Expected: brak trafień poza komentarzami historycznymi w `bench/results/**` (dane) — w kodzie zero.
- [ ] **Step 7:** `uv run pytest -m "not real_data" -q` → PASS (liczba testów mniejsza niż 493 o usunięte).
  `uv run pytest -m real_data -q -k sail` → PASS.
- [ ] **Step 8:** Commit: `git commit -am "Etap 4: usuniecie sciezek UDTF i blokady Saila"` (+ `git add`
  nowego `sail_session.py`).

### Task 2: Pakiet `sail_genomics` (applyInArrow z pulą, własny schemat)

**Files:**
- Create: `sail_genomics/__init__.py`, `sail_genomics/schema.py`, `sail_genomics/arrow.py`,
  `sail_genomics/pb_context.py`, `sail_genomics/session.py`
- Delete: `sail_arrow.py`, `sail_pb_context.py`, `sail_session.py`
- Modify: `bench/runners/sail_runner.py`, `bench/runners/sail_server.py` (import pakietu, jeśli
  serwer go preładowuje), `tests/test_sail_arrow.py`, `tests/test_sail_pb_context.py`,
  `tests/test_sail_runner.py`, `tests/test_real_data.py`, `tests/test_code_language.py` (jeśli ma
  listę plików)

**Interfaces:**
- Produces (pakiet nie importuje NICZEGO z `bench`):
  - `sail_genomics.schema`: `OPS: tuple[str, ...]`, `UNARY_OPS: frozenset[str]`,
    `OUTPUT_COLUMNS: dict[str, tuple[str, ...]]` (identyczne z `bench.ops.OUTPUT_COLUMNS`),
    `DEFAULT_INPUT_COLUMNS = ("contig", "pos_start", "pos_end")`, `ddl_schema(op) -> str`,
    `arrow_schema(op) -> pa.Schema`, `rename_polars_bio(op: str, df: pl.DataFrame, pb_cols: tuple[str, str, str]) -> pl.DataFrame`
  - `sail_genomics.arrow`: `evaluate(op, left: pa.Table, right: pa.Table | None, phase_log: str | None) -> pa.Table`,
    `build_query(spark, op, left, right=None, cols=DEFAULT_INPUT_COLUMNS, *, phase_log=None) -> pyspark DataFrame`,
    `read_phases(path) -> tuple[float, int]`
  - `sail_genomics.pb_context`: `install()`, `uninstall()`, `installed() -> bool` (treść z `sail_pb_context.py`)
  - `sail_genomics.session`: `local_session()` (z Task 1)
  - `sail_genomics/__init__.py` re-eksportuje `build_query`, `local_session`, `OPS`, `UNARY_OPS`, `OUTPUT_COLUMNS`

- [ ] **Step 1: Test zgodności schematów** w `tests/test_sail_arrow.py`:

```python
def test_package_schema_matches_bench_schema():
    from bench import ops
    import sail_genomics
    assert sail_genomics.OUTPUT_COLUMNS == ops.OUTPUT_COLUMNS
    assert set(sail_genomics.UNARY_OPS) == set(ops.UNARY_OPS)
    assert tuple(sail_genomics.OPS) == tuple(ops.OPS)

def test_package_does_not_import_bench():
    import ast, pathlib
    for path in pathlib.Path("sail_genomics").glob("*.py"):
        tree = ast.parse(path.read_text())
        names = [n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)] + \
                [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        assert not any((m or "").split(".")[0] == "bench" for m in names), path
```

- [ ] **Step 2:** `uv run pytest tests/test_sail_arrow.py -k "package" -q` → FAIL (`No module named 'sail_genomics'`).
- [ ] **Step 3:** Utworzyć pakiet: przenieść kod z `sail_arrow.py`/`sail_pb_context.py`/`sail_session.py`;
  w domknięciach `build_query` import `from sail_genomics import arrow` (moduł importowalny po nazwie
  w procesie serwera — cloudpickle serializuje przez referencję); `evaluate` instaluje pulę przez
  `sail_genomics.pb_context.install()` (idempotentnie). Usunąć stare pliki, poprawić importy w
  runnerze i testach (`import sail_arrow` → `from sail_genomics import arrow`).
- [ ] **Step 4:** `uv run pytest -m "not real_data" -q` → PASS; `uv run pytest -m real_data -q -k sail` → PASS.
- [ ] **Step 5:** Commit `"Etap 4: pakiet sail_genomics (applyInArrow z pula, wlasny schemat)"`.

### Task 3: Podbicie strony Pythona (polars-bio 0.37, Sail 0.7.3, PySpark 4.2.1, polars)

**Files:**
- Modify: `pyproject.toml`, `uv.lock`, `tests/test_algorithm_versions.py`, `sail_genomics/arrow.py`
  (układ współrzędnych), `bench/runners/polars_bio_runner.py`, `tests/generic_oracle.py` i inne
  miejsca wołające `pb.*` (wg testów), `ballista_genomics/OPIS.md` (sekcja o puli/wersjach)

**Interfaces:**
- Produces: `tests/test_algorithm_versions.py`: `RANGES_IN_POLARS_BIO = {"0.36.0": "0.22.2", "0.37.0": "0.24.0"}`,
  `EQUIVALENT_RANGES: dict[tuple[str, str], str]` = {(`"0.24.0"`, `"0.22.2"`): powód, (`"0.24.0"`, `"0.25.0"`): powód}
  — test przechodzi, gdy vendor == oczekiwana albo (oczekiwana, vendor) ∈ `EQUIVALENT_RANGES`.

- [ ] **Step 1: Test strażnika** (`tests/test_algorithm_versions.py`): dodać
  `test_equivalence_exceptions_are_explicit` — każda para w `EQUIVALENT_RANGES` ma niepusty powód
  zawierający „DataFusion” albo „identical”, a para (`"0.24.0"`, `"0.23.0"`) **nie** jest dopuszczona.
  Zmienić główny test na regułę z Interfaces.
- [ ] **Step 2:** `pyproject.toml`: `polars-bio==0.37.*`, `pyspark[connect]==4.2.*`, `pysail==0.7.*`,
  `polars>=2.1,<2.2` (komentarz o przypięciu polars<2 usunąć/zmienić); `uv lock --upgrade-package polars-bio
  --upgrade-package pysail --upgrade-package pyspark --upgrade-package polars && uv sync`.
  Sprawdzić: `uv pip list | grep -iE "polars|sail|pyspark|datafusion|pyarrow"` → polars-bio 0.37.0,
  pysail 0.7.3, pyspark 4.2.1, polars 2.1.x, pyarrow 24.x, datafusion 53.x.
- [ ] **Step 3:** `uv run pytest -m "not real_data" -q -x` i naprawy:
  - układ współrzędnych (pb #490): runner polars-bio podaje ścieżki — ustawienia
    `coordinate_system_zero_based=True` zostają; ramki w `sail_genomics.arrow._frame` oznaczyć przez
    publiczne API, jeśli 0.37 je ma (`pb.scan_parquet(..., zero_based=True)` dotyczy plików; dla ramek
    zostaje `polars_bio._metadata.set_coordinate_system` — sprawdzić, czy nazwa przetrwała), inaczej
    odpowiednik z 0.37; **cel: zero ostrzeżeń „Coordinate system metadata is missing”** — usunąć
    `warnings.filterwarnings` z runnera, jeśli przestaje być potrzebne;
  - pula: `tests/test_sail_pb_context.py` (współbieżność 5 operacji) musi przejść; jeśli podmiana
    `range_operation` nie daje się dostosować w ~pół dnia → wariant zastępczy ze spec 4.2 (tryb `lock`
    z `git show przed-etapem-4:sail_pb_guard.py`), zapisać to w commicie i zgłosić użytkownikowi;
  - zapisać w `OPIS.md`, czy bez puli nadal występuje „The table s1 already exists” (test
    `test_sail_pb_context.py` ma przypadek kontrolny bez puli — jego wynik decyduje o zdaniu);
  - polars 2: jeśli naprawy w kodzie narzędzia/testach wykraczają poza zmiany nazw API (orientacyjnie
    > ~1 h), wrócić do `polars>=1.44,<2` z komentarzem i powodem w commicie.
- [ ] **Step 4:** `uv run pytest -m "not real_data" -q` → PASS; `uv run python -m bench.data.download`
  (jeśli brak danych); `uv run pytest -m real_data -q` → PASS (xfail-e jak dotąd albo usunięte, jeśli
  zniknęły — wtedy usunąć znacznik, nie zostawiać XPASS).
- [ ] **Step 5:** Smoke debug bez pomiaru czasu (sprawdza tylko potok i poprawność; nie wymaga
  checklisty): `uv run python -m bench.orchestrator bench/conf/smoke.yaml --ballista-profile debug`
  → kod 0 albo tylko nieważne z dryfu; zero `checksum … ≠ reference`. Wcześniej
  `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins` (Rust bez zmian — przyrostowo).
- [ ] **Step 6:** Commit `"Etap 4: polars-bio 0.37, Sail 0.7.3, PySpark 4.2.1, polars 2"`.
  **Punkt kontrolny:** zapytać użytkownika o zgodę na `git push -u origin etap-4` (CI). Po zgodzie —
  CI zielone (sprawdzić publicznym API: `curl -s https://api.github.com/repos/mkowalewski1282/distributed-polars-bio/actions/runs?branch=etap-4`).
- [ ] **Step 7:** `git switch -c etap-4-ballista55` (Task 4–5 na tej gałęzi; patrz „Ścieżka odcięcia”).

### Task 4: Ballista 55 + vendor v0.25.0 — kompilacja

**Files:**
- Modify: `ballista_genomics/Cargo.toml`, `ballista_genomics/Cargo.lock`, `ballista_genomics/src/*.rs`,
  `ballista_genomics/src/bin/*.rs`, `ballista_genomics/vendor/PATCH.md`
- Replace: `ballista_genomics/vendor/datafusion-bio-function-ranges/` (kopia z v0.25.0 + łatki)

**Interfaces:**
- Produces: binarki `ballista_node`, `bench_client`, `dist_ops`, `*_local` z tym samym CLI co dziś.

- [ ] **Step 1: Vendor.** Łatki = różnica obecnej kopii względem czystego v0.22.2:
  `git clone -q --depth 1 --branch v0.22.2 https://github.com/biodatageeks/datafusion-bio-functions /tmp/dbf-old`,
  `diff -ru /tmp/dbf-old/datafusion/bio-function-ranges ballista_genomics/vendor/datafusion-bio-function-ranges > $SCRATCH/vendor.patch`
  (bez `Cargo.toml`, którego dziedziczenie z workspace rozwijamy ręcznie); klon v0.25.0 do
  `/tmp/dbf-new`, skopiować `datafusion/bio-function-ranges` w miejsce vendora, rozwinąć
  `*.workspace = true` w jego `Cargo.toml` wartościami z workspace v0.25.0, `patch -p1` łatek.
  Sprawdzenie reguły z `PATCH.md` (diff względem `/tmp/dbf-new` → tylko `pub`, re-eksporty,
  komentarze). Zaktualizować w `PATCH.md` wersję kopii (v0.25.0, polars-bio 0.37 → v0.24.0, okres
  przejściowy) i polecenie kontrolne (tag v0.25.0).
- [ ] **Step 2:** `Cargo.toml`: `ballista`, `ballista-core`, `ballista-scheduler`, `ballista-executor` =
  `"55.0.0"`; `datafusion = "=55.2.0"`; `datafusion-proto = "55"`; `prost`/`tonic`/`arrow` zgodnie
  z Ballistą 55 (`cargo tree -i` w razie konfliktu). `cargo update -p ballista` itd. aż `Cargo.lock`
  ma jedną wersję `datafusion` (55.2.0): `cargo tree -d | grep -E "^datafusion "` → pusto.
- [ ] **Step 3:** `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tee $SCRATCH/build.log`
  i port aż do zera błędów. Znane zmiany API (spec 3): `as_any` usunięte z `ExecutionPlan`/`TableProvider`
  (downcast przez `plan.downcast_ref::<T>()`), wymagane `apply_expressions` w naszych
  `ExecutionPlan` (`coverage_node.rs`: `TreeNodeRecursion::Continue`, jeśli węzeł nie ma wyrażeń;
  wzór: `MergeExec` w vendorze v0.25.0), `Distribution::KeyPartitioned` zamiast `HashPartitioned`;
  zmiany kodeków Ballisty (`LogicalExtensionCodec`/`PhysicalExtensionCodec`), konfiguracji
  (#2387) i API klienta (`SessionStateExt`, `remote_with_state`) — wg błędów kompilatora i
  `~/.cargo/registry/src/*/ballista-*-55.0.0/`. Nie zmieniać logiki operacji.
- [ ] **Step 4:** `CARGO_BUILD_JOBS=1 cargo test --lib --locked` → PASS.
- [ ] **Step 5:** Commit `"Etap 4: Ballista 55, DataFusion 55.2, vendor v0.25.0 - kompilacja"`.

### Task 5: Ballista 55 — testy, AQE, konfiguracja, release

**Files:**
- Modify: `ballista_genomics/src/{cluster.rs,runner.rs,cli.rs,bio_phys_codec.rs}` (wg potrzeb),
  `bench/engines.py` (komentarz `BALLISTA_START_ATTEMPTS`), `bench/logscan.py` (jeśli zmienił się
  format logu), `tests/test_ballista_distribution_evidence.py`, `tests/test_ballista_multiprocess.py`,
  `tests/test_ballista_grpc_limit.py`, `ballista_genomics/OPIS.md`

- [ ] **Step 1:** `uv run pytest tests/test_ballista_*.py tests/test_bench_client_protocol.py tests/test_*_correctness.py -q`
  i lista porażek do `$SCRATCH/ballista55-failures.txt`.
- [ ] **Step 2: AQE (reguła spec 4.1).** Dla każdej operacji porównać `EXPLAIN ANALYZE` (2 executory,
  `bench_client --explain-analyze`) z asercjami `test_ballista_distribution_evidence.py`. Ta sama
  strategia (overlap: `CollectLeft` z całą lewą stroną w każdym zadaniu; merge/subtract: podział po
  chromosomie; nearest/coverage: nasz broadcast) → zaktualizować asercje do nowej postaci planu.
  Inna strategia / brak rozproszenia / błąd dekodowania → wyłączyć AQE jawnie w konfiguracji sesji
  klienta, schedulera i executorów (`cluster.rs`, klient) i opisać w `OPIS.md` („Konfiguracja Ballisty
  55: AQE …, bo …”). W obu przypadkach test `test_aqe_setting_is_explicit` w
  `test_ballista_distribution_evidence.py`: wartość AQE odczytana z planu/konfiguracji jest równa
  tej, którą deklaruje `OPIS.md` (jedno źródło — stała w Ruście drukowana przez `bench_client --version-info`
  albo log startu; wybór wykonawcy).
- [ ] **Step 3:** Klucze konfiguracji (push, RoundRobin, `BALLISTA_GRPC_MAX_MESSAGE_BYTES`, sloty)
  pod nazwami Ballisty 55; `test_ballista_grpc_limit.py` → PASS (szybki błąd przy za małym limicie);
  `test_ballista_broadcast_bytes.py` → PASS (logscan widzi linie z `broadcast_bytes`).
- [ ] **Step 4:** Wyścig rejestracji (#2225): komentarz przy `BALLISTA_START_ATTEMPTS` przepisać
  (ponawianie zostaje jako zabezpieczenie, przyczyna z Ballisty 53 naprawiona w 55). Poprawić
  komentarz `runner.rs:65` (per-task), odłożony w 3b-2.
- [ ] **Step 5:** Pełny pakiet: `uv run pytest -m "not real_data" -q` → PASS; `uv run pytest -m real_data -q` → PASS.
- [ ] **Step 6:** Release: `CARGO_BUILD_JOBS=1 cargo build --release --bins` (zimny build ≥ 1 h —
  uruchomić w tle, bez Pythona z polars-bio w tym czasie). Smoke release (bez pomiaru, tylko
  poprawność): `uv run python -m bench.orchestrator bench/conf/smoke.yaml` → zero błędów sum
  kontrolnych.
- [ ] **Step 7:** `OPIS.md`: sekcja „Ballista 55” (zmiany: shuffle sortujący, AQE, wyścig, wersje).
  Commit `"Etap 4: Ballista 55 - testy, AQE, konfiguracja"`. Scalić do `etap-4`:
  `git switch etap-4 && git merge --ff-only etap-4-ballista55` (jeśli niemożliwe — zwykły merge).
  Punkt kontrolny: za zgodą użytkownika `git push origin etap-4` → CI zielone.

## Etap B — semantyka P

### Task 6: Sonda slotów Saila 0.7.3 (decyzja `WORKER_TASK_SLOTS`)

**Files:**
- Create: `$SCRATCH/sail_slots_probe.py` (poza repo)
- Modify (po decyzji): `bench/runners/sail_server.py` (`WORKER_TASK_SLOTS` i komentarz)

- [ ] **Step 1:** Skrypt: dla P ∈ {1, 2, 4} i slots ∈ {1, 8} uruchamia `bench.runners.sail_server` z
  `sail_env(P)` nadpisanym `SAIL_CLUSTER__WORKER_TASK_SLOTS=slots`, `SAIL_EXECUTION__DEFAULT_PARALLELISM=P`,
  przypięty `taskset` do P CPU; wykonuje `sail_runner` (overlap 7-0) z limitem 300 s; zapisuje: kod
  wyjścia, sumę kontrolną vs wzorzec (`polars_bio_runner --threads 1`), liczbę workerów z logu
  (`bench.logscan.sail_workers`).
- [ ] **Step 2:** Uruchomić (to nie jest pomiar czasu — checklista RAM niewymagana, ale bez innych
  ciężkich procesów). **Reguła:** slots = 1, jeśli dla każdego P: poprawna suma, workery == P,
  brak zawieszenia; inaczej slots = 8. Wynik (tabela P × slots) zapisać do `$SCRATCH/sail_slots.md`
  — trafi do README repo testowego (Task 13).
- [ ] **Step 3:** Ustawić `WORKER_TASK_SLOTS` z komentarzem (data, wersja Saila, wynik sondy).
  Commit `"Etap 4: sloty workerow Saila wg sondy 0.7.3"`.

### Task 7: Układ CPU z topologii (`bench/cpus.py`)

**Files:**
- Create: `bench/cpus.py`, `tests/bench/test_cpus.py`, `tests/bench/fake_sysfs.py`

**Interfaces:**
- Produces:
```python
SYSFS_CPU = Path("/sys/devices/system/cpu")
class TopologyError(RuntimeError): ...
class NotEnoughCpus(ValueError): ...
@dataclass(frozen=True)
class CpuLayout:
    system: tuple[int, ...]                   # wszystkie dozwolone wątki pierwszego rdzenia
    order: tuple[int, ...]                    # kolejność przydziału CPU dla P (bez rdzenia systemowego)
    siblings: tuple[frozenset[int], ...]      # rdzenie poza systemowym: zbiory dozwolonych wątków
def read_layout(sysfs: Path = SYSFS_CPU, affinity: set[int] | None = None) -> CpuLayout
def cpus_for(layout: CpuLayout, p: int) -> tuple[int, ...]       # NotEnoughCpus, gdy p > len(order)
def uses_smt(layout: CpuLayout, cpus) -> bool
def control_cpus(layout: CpuLayout) -> tuple[int, ...]           # wątki pierwszego rdzenia z order; bez SMT: order[:2]
def max_parallelism(layout: CpuLayout) -> int
```
  Algorytm `order`: rdzenie = grupy po (`physical_package_id`, `core_id`) ograniczone do `affinity`
  (domyślnie `os.sched_getaffinity(0)`), posortowane po najmniejszym CPU; pierwszy → `system`;
  z pozostałych: najpierw najmniejszy wątek każdego rdzenia (w kolejności rdzeni), potem drugi itd.

- [ ] **Step 1: Testy** (`fake_sysfs.make(tmp_path, cores: list[list[int]], package=0)` tworzy
  `cpuN/topology/{core_id,physical_package_id,thread_siblings_list}`):

```python
def test_laptop_layout(tmp_path):            # 6 rdzeni, rdzeń k = CPU 2k, 2k+1
    lay = read_layout(make(tmp_path, [[2*k, 2*k+1] for k in range(6)]), affinity=set(range(12)))
    assert lay.system == (0, 1)
    assert lay.order == (2, 4, 6, 8, 10, 3, 5, 7, 9, 11)
    assert cpus_for(lay, 1) == (2,) and cpus_for(lay, 2) == (2, 4)
    assert cpus_for(lay, 4) == (2, 4, 6, 8)
    assert cpus_for(lay, 8) == (2, 4, 6, 8, 10, 3, 5, 7)
    assert not uses_smt(lay, cpus_for(lay, 4)) and uses_smt(lay, cpus_for(lay, 8))
    assert control_cpus(lay) == (2, 3) and max_parallelism(lay) == 10

def test_layout_without_smt(tmp_path):
    lay = read_layout(make(tmp_path, [[0], [1], [2], [3]]), affinity={0, 1, 2, 3})
    assert lay.system == (0,) and lay.order == (1, 2, 3)
    assert control_cpus(lay) == (1, 2) and not uses_smt(lay, (1, 2, 3))
    with pytest.raises(NotEnoughCpus): cpus_for(lay, 4)

def test_layout_respects_affinity(tmp_path):
    lay = read_layout(make(tmp_path, [[2*k, 2*k+1] for k in range(6)]), affinity={4, 5, 6, 7})
    assert lay.system == (4, 5) and lay.order == (6, 7) and uses_smt(lay, (6, 7))

def test_layout_interleaved_numbering(tmp_path):   # Intel: rodzeństwo i oraz i+4
    lay = read_layout(make(tmp_path, [[0, 4], [1, 5], [2, 6], [3, 7]]), affinity=set(range(8)))
    assert lay.system == (0, 4) and lay.order == (1, 2, 3, 5, 6, 7)

def test_unreadable_topology_raises(tmp_path):
    with pytest.raises(TopologyError): read_layout(tmp_path / "missing", affinity={0})
```

- [ ] **Step 2:** `uv run pytest tests/bench/test_cpus.py -q` → FAIL (brak modułu).
- [ ] **Step 3:** Implementacja w `bench/cpus.py`.
- [ ] **Step 4:** `uv run pytest tests/bench/test_cpus.py -q` → PASS; na laptopie
  `uv run python -c "from bench.cpus import *; l=read_layout(); print(l.system, l.order)"` →
  `(0, 1) (2, 4, 6, 8, 10, 3, 5, 7, 9, 11)`.
- [ ] **Step 5:** Commit `"Etap 4: uklad CPU z topologii"`.

### Task 8: Warianty, P i układ CPU w konfiguracji, silnikach, orkiestratorze i testach

**Files:**
- Modify: `bench/config.py`, `bench/results.py`, `bench/engines.py`, `bench/orchestrator.py`,
  `bench/preflight.py`, `bench/control_noise.py`, `bench/runners/sail_server.py`, `tests/conftest.py`,
  `tests/test_nodes_marker.py` → rename `tests/test_parallelism_marker.py`, `tests/bench/test_config.py`,
  `tests/bench/test_engines.py`, `tests/bench/test_orchestrator.py`, `tests/bench/test_preflight.py`,
  `tests/bench/test_results.py`, `tests/bench/test_control_noise.py`, `tests/test_orchestrator_integration.py`,
  testy z `@pytest.mark.nodes(n)`, `bench/conf/{smoke,control_noise,broadcast_check}.yaml`,
  `bench/data/datasets.py` (komentarze), `tests/test_code_language.py` (jeśli wylicza pliki)

**Interfaces:**
- Consumes: `bench.cpus` (Task 7), `WORKER_TASK_SLOTS` (Task 6).
- Produces:
  - `config.VARIANTS = ("polars_bio", "ballista", "sail")`, `DEFAULT_PARALLELISM = (1, 2, 4, 8)`;
    klucz YAML `parallelism` (opcjonalny, lista int ≥ 1); `nodes` → `ConfigError("nodes: renamed to parallelism")`;
    `Block(variant: str, parallelism: int)`, `label == f"{variant} P={parallelism}"`;
    `ExpectedInvalid(scenario, variant, parallelism: int | None, reason)`, klucz YAML `parallelism`;
    `SeriesConfig.parallelism: tuple[int, ...]`; `blocks(cfg)` = każdy wariant × każde P.
  - `results.SCHEMA`: `"n_nodes"` → `"parallelism": pl.Int64`; nowe `"cpus": pl.Utf8` (JSON lista),
    `"smt": pl.Boolean`.
  - `engines`: `REFERENCE_VARIANT = "polars_bio_ref"` (1 wątek, `cpus_for(layout, 1)`),
    `CONTROL_VARIANT = "polars_bio_control"` (2 wątki, `control_cpus(layout)`);
    `make_engine(variant, parallelism, *, layout: CpuLayout, data_root, log_dir, profile)`;
    każdy silnik ma atrybut `cpus: tuple[int, ...]`; `BallistaEngine(parallelism, cpus, *, system, profile, data_root, log_dir)` —
    executor k na `(cpus[k-1],)`, `--concurrent-tasks 1`, `BIO_TARGET_PARTITIONS = str(parallelism)`,
    scheduler i klient na `system`; `SailEngine(parallelism, cpus, *, system, data_root, log_dir)` —
    serwer na `cpus`, runner na `system`, `sail_env(parallelism)`; `PolarsBioEngine(threads, cpus, data_root)`
    bez zmian; `BALLISTA_DIR` = `REPO/"engines"/"ballista-genomics"`, jeśli ma `Cargo.toml`, inaczej
    `REPO/"ballista_genomics"` (funkcja `ballista_dir() -> Path`). Usunąć `SYSTEM_CPUS`, `SLOTS_PER_NODE`,
    `node_cpus`, `cluster_cpus`.
  - `sail_server.sail_env(parallelism: int) -> dict` (`WORKER_INITIAL_COUNT = P`,
    `DEFAULT_PARALLELISM = P`, `WORKER_TASK_SLOTS = WORKER_TASK_SLOTS`, reszta jak dziś).
  - `preflight.check_cpus(cfg, layout) -> list[int]` — zwraca pominięte P (ostrzeżenie na wyjściu);
    `ConfigError`, gdy `max_parallelism(layout) < 1` albo `len(control_cpus(layout)) < 2`; `TopologyError` →
    `ConfigError` z przyczyną.
  - Orkiestrator: blok z P > `max_parallelism` → po jednym wierszu na scenariusz:
    `valid=False`, `invalid_reason="skipped: P exceeds available CPUs"`, `cpus="[]"`, `smt=None`;
    takie wiersze liczone jako oczekiwane (`summary.expected`), nie psują kodu wyjścia. Wiersze
    pomiarowe: `cpus=json.dumps(engine.cpus)`, `smt=uses_smt(layout, engine.cpus)`. Katalogi
    `logs/<variant>-p<P>`, `explain/<variant>-p<P>`. Linie postępu `P=`.
  - `control_noise.load_times(directory)`: czasy przebiegów kontrolnych z początku bloku
    (`is_control & control_position == "start"`, ważne); `control_noise.yaml`: `variants: [polars_bio]`,
    `parallelism: [1]`, `repeats: 1`, `control_repeats: 20`, `control_tolerance: 100.0`.
  - Znacznik testów `parallelism(p)`: pomija test, gdy `max_parallelism(read_layout()) < p` (albo
    topologia nieczytelna) — powód w komunikacie.

- [ ] **Step 1: Testy (najpierw porażki):**
  - `tests/bench/test_config.py`: `test_parallelism_defaults_to_1_2_4_8`,
    `test_nodes_key_reports_rename` (`ConfigError` z „renamed to parallelism”),
    `test_blocks_are_variants_times_parallelism` (3 warianty × [1, 2, 4, 8] → 12 bloków, etykieta `"ballista P=4"`),
    `test_removed_variants_are_rejected` (`polars_bio_a`, `sail_lock` → `ConfigError`).
  - `tests/bench/test_engines.py` (fałszywy `CpuLayout` z Task 7): `test_ballista_pins_one_executor_per_cpu`
    (P = 4 → 4 polecenia executorów z `taskset -c 2`, `-c 4`, `-c 6`, `-c 8`, każde z
    `--concurrent-tasks 1`; `BIO_TARGET_PARTITIONS == "4"`), `test_sail_env_uses_parallelism`
    (`sail_env(4)`: `WORKER_INITIAL_COUNT == "4"`, `DEFAULT_PARALLELISM == "4"`),
    `test_control_engine_uses_two_threads_on_first_core` (`threads == 2`, `cpus == (2, 3)`),
    `test_ballista_dir_prefers_submodule` (tmp REPO z `engines/ballista-genomics/Cargo.toml` → ta ścieżka;
    bez niej → `ballista_genomics`).
  - `tests/bench/test_orchestrator.py` (silniki-atrapy jak dziś): `test_oversized_parallelism_is_skipped_not_failed`
    (układ z `max_parallelism == 4`, `parallelism: [4, 8]` → wiersze P = 8 z
    `invalid_reason == "skipped: P exceeds available CPUs"`, kod wyjścia 0),
    `test_rows_record_cpus_and_smt` (P = 8 na układzie laptopa → `smt is True`, `json.loads(cpus) == [2,4,6,8,10,3,5,7]`).
  - `tests/bench/test_preflight.py`: `test_check_cpus_returns_skipped_parallelism`,
    `test_check_cpus_fails_without_control_cpus` (układ z 1 CPU poza systemowym).
  - `tests/bench/test_control_noise.py`: `load_times` bierze przebiegi kontrolne `start`.
  - `tests/test_parallelism_marker.py` (dawny `test_nodes_marker.py`): pomijanie przy za małym układzie.
- [ ] **Step 2:** `uv run pytest tests/bench -q` → FAIL w nowych testach.
- [ ] **Step 3:** Implementacja wg Interfaces; testy integracyjne i Ballisty z `@pytest.mark.nodes(n)` →
  `@pytest.mark.parallelism(p)` (dawne N węzłów × 2 sloty → P = 2N tam, gdzie test liczy sloty;
  testy dowodów rozproszenia z 2 executorami → `parallelism(2)`); konfiguracje YAML: `nodes` →
  `parallelism`, warianty nowe (`smoke.yaml`: `variants: [polars_bio, ballista, sail]`,
  `parallelism: [1, 2]` — krótko; `broadcast_check.yaml`: `parallelism: [1, 2, 4, 8]`).
  Usunąć `polars_bio_a`/`polars_bio_b` z kodu i komentarzy; `polars_bio_runner` bez zmian.
- [ ] **Step 4:** `uv run pytest -m "not real_data" -q` → PASS; `uv run pytest -m real_data -q` → PASS;
  smoke debug `uv run python -m bench.orchestrator bench/conf/smoke.yaml --ballista-profile debug` → kod 0
  (tylko poprawność, nie pomiar), w `runs.parquet` kolumny `parallelism`, `cpus`, `smt`.
- [ ] **Step 5:** Commit `"Etap 4: semantyka P (rownolegosc), uklad CPU w silnikach i orkiestratorze"`.

### Task 9: Raport serii (`bench/report.py`)

**Files:**
- Create: `bench/report.py`, `tests/bench/test_report.py`
- Modify: `pyproject.toml`, `uv.lock` (`matplotlib`)

**Interfaces:**
- Consumes: `results.SCHEMA`, `results.to_parquet` (Task 8).
- Produces:
```python
def load_runs(series_dir: Path) -> pl.DataFrame   # runs.parquet; brak → runs.jsonl przez schemat results
def summarize(runs: pl.DataFrame) -> pl.DataFrame
    # tylko is_warmup=False, is_control=False, is_reference=False; kolumny:
    # op, pair, variant, parallelism, smt, n_valid, median_s, min_s, max_s,
    # speedup_self (mediana P=1 tego wariantu / mediana), speedup_vs_polars_bio_1,
    # missing_reason (najczęstszy invalid_reason, gdy n_valid == 0, inaczej null)
def write_report(series_dir: Path) -> list[Path]  # summary.md, summary.csv, plots/<op>_<pair>.png
def main(argv: list[str] | None = None) -> int     # python -m bench.report <katalog serii>
```
  Wykres: oś X = P (log2, znaczniki 1, 2, 4, 8), oś Y = mediana `t_total_s`, linia na wariant,
  punkty `smt` pustym znacznikiem, tytuł `op pair`. `summary.md`: tabela na (op, pair) + nota
  o oznaczeniu SMT i o tym, że P = 1 polars-bio to punkt odniesienia jednowątkowy.

- [ ] **Step 1: Testy** (syntetyczne wiersze zgodne z `results.SCHEMA`):
  `test_summary_uses_only_valid_measured_runs` (rozgrzewka, kontrola i wzorzec pominięte; mediana z 3
  ważnych = oczekiwana), `test_speedups` (polars_bio P=1 mediana 2.0, ballista P=2 mediana 1.0 →
  `speedup_vs_polars_bio_1 == 2.0`, `speedup_self` ballista P=2 = mediana P=1 / 1.0),
  `test_missing_cell_shows_reason` (komórka tylko z nieważnymi → `n_valid == 0`,
  `missing_reason == "skipped: P exceeds available CPUs"`, w `summary.md` widoczny powód),
  `test_report_from_interrupted_series` (tylko `runs.jsonl` → `write_report` tworzy
  `summary.md`, `summary.csv` i ≥ 1 PNG), `test_smt_flag_is_carried` (wiersze P = 8 z `smt=True` →
  `smt` w podsumowaniu True).
- [ ] **Step 2:** FAIL → **Step 3:** przed kodem wykresów załadować skill `dataviz`; implementacja
  (`matplotlib`, backend `Agg`) → **Step 4:** `uv run pytest tests/bench/test_report.py -q` PASS;
  `uv run python -m bench.report bench/results/smoke/<najnowszy>` tworzy pliki.
- [ ] **Step 5:** Commit `"Etap 4: raport serii (tabela median i wykresy od P)"`.

### Task 10: Konfiguracja `scaling` i rachunek czasu

**Files:**
- Create: `bench/conf/scaling.yaml`
- Modify: `bench/conf/smoke.yaml` (komentarz), README sekcja narzędzia (monorepo — krótko)

- [ ] **Step 1:** `scaling.yaml`: `series: scaling`; scenariusze: overlap/nearest/coverage/subtract na
  `"2-7"` i `"7-0"`, merge na `7` i `0`; `variants: [polars_bio, ballista, sail]`;
  `parallelism: [1, 2, 4, 8]`; `repeats: 5`, `warmup: 1`, `control_repeats: 10`, `settle_s: 3.0`,
  `seed: 20261115`. Komentarz z rachunkiem: 12 bloków × (2·10 kontroli + 10 scenariuszy × 6) = 960
  przebiegów + 10 wzorców; ~5,5 s na przebieg (czas 0,2–3 s + ~1 s startu + 3 s `settle_s`) ≈ 1,5 h;
  z powtórzeniem każdego bloku po dryfie ≤ 3 h (limit 4 h).
- [ ] **Step 2:** `uv run python -c "from bench.config import load, blocks; c=load('bench/conf/scaling.yaml'); print(len(blocks(c)))"` → `12`.
- [ ] **Step 3:** Commit `"Etap 4: konfiguracja serii scaling 1/2/4/8"`.

## Etap C — trzy repozytoria

### Task 11: Skrypt podziału (mapowanie, nakładki, przepisanie importów)

**Files:**
- Create: `tools/split/split.py`, `tools/split/overlay/{ballista-genomics,sail-genomics,genomics-engines-bench}/…`
  (na razie puste katalogi + `.gitignore` każdego repo), `tests/test_split.py`

**Interfaces:**
- Produces: `uv run python tools/split/split.py --source <commit> --out <katalog>` → `<katalog>/<repo>/`
  (bez `.git`), kod wyjścia ≠ 0 przy trafieniach strażnika treści. Funkcje:
```python
REPOS = ("ballista-genomics", "sail-genomics", "genomics-engines-bench")
MAPPING: dict[str, list[tuple[str, str]]]     # repo -> [(ścieżka w monorepo/glob, ścieżka docelowa)]
IMPORT_REWRITES: dict[str, list[tuple[str, str]]]  # repo -> [(regex, zamiana)] stosowane do *.py
WORKSHOP_PATTERNS: tuple[str, ...]            # regexy strażnika treści (niżej)
def export(source: str, out: Path) -> None    # git archive <source> → katalog tymczasowy → mapowanie + nakładki + przepisania
def scan_workshop_refs(root: Path) -> list[str]   # "plik:linia: tekst"
```
  `export` odmawia, gdy `out` istnieje i nie jest pusty (`SystemExit` z komunikatem).
  **Mapowanie** (spec 6.1–6.3):
  - `ballista-genomics`: `ballista_genomics/{Cargo.toml,Cargo.lock,rust-toolchain.toml,src/**,vendor/**,data/**}` → korzeń;
    testy `tests/{__init__.py,conftest.py,parquet_fixture.py,generic_oracle.py,overlap_oracle.py,merge_oracle.py,nearest_oracle.py,coverage_subtract_oracle.py,test_ballista_overlap.py,test_ballista_distributed_ops.py,test_ballista_distribution_evidence.py,test_ballista_multiprocess.py,test_ballista_parquet.py,test_merge_correctness.py,test_nearest_correctness.py,test_coverage_subtract_correctness.py,test_oracle_sanity.py,test_generic_oracle.py,test_code_language.py}` → `tests/`;
    `bench/ops.py` → `tests/support/ops.py`, `bench/checksum.py` → `tests/support/checksum.py`.
  - `sail-genomics`: `sail_genomics/**` → `src/sail_genomics/`; `tests/{__init__.py,parquet_fixture.py,generic_oracle.py,test_sail_pb_context.py,test_code_language.py}` → `tests/` + nakładkowe testy pakietu (Task 12).
  - `genomics-engines-bench`: `bench/**` bez `bench/results/**` → `bench/`; `tests/**` poza plikami
    przypisanymi wyłącznie Ballistcie (`test_ballista_overlap`, `test_ballista_distributed_ops`,
    `test_*_correctness`, `test_oracle_sanity`) → `tests/`; `pyproject.toml`, `.python-version` przez
    nakładkę.
  `bench/cpus.py` → `tests/support/cpus.py` (znacznik `parallelism` w conftest Ballisty).
  **Przepisania importów:** `ballista-genomics`: `from bench.ops` → `from tests.support.ops`,
  `from bench.checksum` → `from tests.support.checksum`, `from bench import checksum` →
  `from tests.support import checksum`, `from bench.cpus` → `from tests.support.cpus`;
  `Path(__file__).resolve().parent.parent / "ballista_genomics"` (stała `BALLISTA_DIR` w 9 plikach
  testów) → `Path(__file__).resolve().parent.parent`. `genomics-engines-bench`: ta sama stała →
  `Path(__file__).resolve().parent.parent / "engines" / "ballista-genomics"`.
  `sail-genomics`: `from bench.ops import …` w testach → `from sail_genomics.schema import …`.
  Po przepisaniu: w `ballista-genomics` i `sail-genomics` zero importów `bench` (sprawdzane w `export`).
  **Strażnik treści** (`WORKSHOP_PATTERNS`, bez rozróżniania wielkości liter): `specyfikacj`,
  `\bspec(?:ification)?\b`, `\bplan(?:u|ie)? (?:2|3|3a|3b|P0)\b`, `zadani[ea] \d`, `\btask \d`,
  `docs/superpowers`, `wnioski_claude`, `raporty/`, `ustalenia_zalozen`, `promotor`, `supervisor`,
  `claude`, `\bfaz[aye] [A-H]\b`, `\bphase [A-H]\b`, `przegl[aą]d(?:u)? (?:3a|3b|końcow)`,
  `\breview (?:3a|3b)\b`. **Strażnik języka** (`POLISH_PATTERNS`): `[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]` oraz
  słowa `\b(?:jest|oraz|przez|ktory|ktora|gdy|tylko|wiec|czyli|albo|zeby|dlatego)\b`.
  `scan_workshop_refs` zwraca trafienia obu. Wyłączenia: `vendor/**` (kod upstreamu), pliki binarne,
  `bench/data/datasets.py` tylko jeśli nazwy zbiorów to wymuszają (nie powinny).

- [ ] **Step 1: Testy** `tests/test_split.py`: `test_split_refuses_existing_output` (niepusty `out` →
  `SystemExit`), `test_mapping_covers_every_moved_test` (każdy `tests/test_*.py` monorepo poza listą
  „zostaje w warsztacie” jest w dokładnie jednym repo albo w kilku jawnie), `test_scan_finds_workshop_refs`
  (plik z „specyfikacja 3b-2” i „Zadanie 27” → 2 trafienia; komentarz „# liczba wątków” → 1 trafienie
  języka; „vendor/x.rs” z „spec” → 0),
  `test_rewrites_remove_bench_imports` (przykładowy plik testu Ballisty po przepisaniu nie ma `bench`).
- [ ] **Step 2:** FAIL → **Step 3:** implementacja → **Step 4:** PASS.
- [ ] **Step 5:** Próbny eksport: `uv run python tools/split/split.py --source HEAD --out $SCRATCH/split-try`
  → lista trafień strażnika (oczekiwane: dużo — Task 12 je usuwa). Zapisać do `$SCRATCH/workshop-refs.txt`.
- [ ] **Step 6:** Commit `"Etap 4: skrypt podzialu na trzy repozytoria"`.

### Task 12: Tłumaczenie na angielski, oczyszczenie komentarzy i nakładki (pyproject, CI, licencje, setup)

**Files:**
- Modify: pliki przenoszone z trafieniami z `$SCRATCH/workshop-refs.txt` (komentarze i docstringi:
  `bench/**`, `ballista_genomics/src/**`, `sail_genomics/**`, `tests/**`)
- Create (nakładki w `tools/split/overlay/<repo>/`):
  - wspólne: `LICENSE` (Apache-2.0, tekst z https://www.apache.org/licenses/LICENSE-2.0.txt),
    `.gitignore`, `tests/test_no_workshop_refs.py` (te same `WORKSHOP_PATTERNS` i `POLISH_PATTERNS`,
    skan repo poza `vendor/`); CI każdego repo wyzwalane `push` na `main`, `pull_request` do `main`,
    `workflow_dispatch`
  - `ballista-genomics/`: `pyproject.toml` (Python 3.12, `package = false`, zależności testów:
    `polars-bio==0.37.*`, `pyarrow`, `polars`, `pandas`, `pyyaml` jeśli potrzebne, grupa dev `pytest`),
    `.python-version`, `.github/workflows/tests.yml` (jak obecny CI: toolchain, rust-cache, protoc 28.3
    z sha256, `cargo build --bins --locked`, `cargo test --lib --locked`, setup-uv, `uv sync --locked`,
    `uv run --locked pytest -m "not real_data" -ra`), `vendor/datafusion-bio-function-ranges/LICENSE`
    (Apache-2.0) + akapit pochodzenia w `vendor/PATCH.md`, `tests/support/__init__.py`, `tests/conftest.py`
    (bez importów `bench`: fikstury `parquet_dirs`/`parquet_expected` z `tests.support.ops`, znacznik
    `parallelism` przez `tests.support.cpus` — kopię `bench/cpus.py` z mapowania)
  - `sail-genomics/`: `pyproject.toml` (`package = true`, `build-backend = "uv_build"`, nazwa
    `sail-genomics`, zależności: `pysail==0.7.*`, `pyspark[connect]==4.2.*`, `polars-bio==0.37.*`,
    `pyarrow>=24,<25`, `polars`), `.python-version`, CI (`uv sync --locked`, `pytest`),
    `tests/conftest.py` (fikstura sesji przez `sail_genomics.local_session()`, `parquet_dirs`,
    `parquet_expected` liczony przez `generic_oracle` + `sail_genomics.schema`), `tests/test_arrow.py`
    (poprawność 5 operacji vs wyrocznia; pusta lewa grupa; `read_phases`; złe argumenty — odpowiedniki
    testów z `test_sail_arrow.py` bez serwera z `bench`)
  - `genomics-engines-bench/`: `pyproject.toml` (jak monorepo + `matplotlib` + `sail-genomics` z
    `[tool.uv.sources] sail-genomics = { path = "engines/sail-genomics", editable = true }`), `.python-version`,
    `setup.sh` (`git submodule update --init --recursive`, `uv sync --locked`,
    `(cd engines/ballista-genomics && cargo build --release --bins)`, `uv run python -m bench.data.download`;
    `set -euo pipefail`; komunikat o czasie buildu), CI (`actions/checkout` z `submodules: true`, build w
    `engines/ballista-genomics`, `uv sync --locked`, pytest), `run_series.sh` (dla użytkownika: checklista
    RAM jako tekst + `smoke` + `scaling` + `bench.report`)
- Modify: `tools/split/split.py` (`uv lock` w każdym repo po skopiowaniu nakładek — wymaga sieci)

- [ ] **Step 1:** We **wszystkich** plikach przenoszonych do nowych repozytoriów (wg `MAPPING`, bez
  `vendor/`): komentarze, docstringi i komentarze w YAML przetłumaczyć na angielski; przy okazji
  odwołania do dokumentów roboczych → treść merytoryczna („why”) albo usunięcie odwołania; liczby
  i daty pomiarów zostają z opisem warunków. Zero zmian logiki (przegląd `git diff -w`; w diffie
  zmieniają się tylko linie komentarzy/docstringów). Komunikaty błędów i pomoc CLI są już po
  angielsku (konwencja 3b-1) — sprawdzić. Praca w kilku commitach (po katalogach: `bench/`, Rust,
  `tests/`, `sail_genomics/`), żeby recenzent mógł porównać tłumaczenie z oryginałem.
  `tests/test_code_language.py` w monorepo: dodać `test_moved_files_are_english` — pliki z `MAPPING`
  (import z `tools/split/split.py`) nie mają trafień `POLISH_PATTERNS`.
- [ ] **Step 2:** `uv run pytest -m "not real_data" -q` w monorepo → PASS (strażnik języka też).
- [ ] **Step 3:** Nakładki wg listy; `export` uruchamia `uv lock` w każdym repo wyjściowym.
- [ ] **Step 4:** Weryfikacja na kopii roboczej (`$SCRATCH/split-try2`, świeży katalog):
  - `split.py --source HEAD --out $SCRATCH/split-try2` → kod 0 (zero trafień strażnika);
  - `cd $SCRATCH/split-try2/ballista-genomics && uv sync --locked && CARGO_TARGET_DIR=/home/milosz/praca_magisterska/ballista_genomics/target CARGO_BUILD_JOBS=1 cargo build --bins && uv run pytest -m "not real_data" -q` → PASS
    (wspólny `CARGO_TARGET_DIR` oszczędza zimny build; testy szukają binarek w `target/` repo —
    jeśli używają stałej ścieżki, ustawić `target` jako dowiązanie symboliczne tylko na czas
    weryfikacji, nie w repo);
  - `cd ../sail-genomics && uv sync --locked && uv run pytest -q` → PASS;
  - repo testowe: weryfikacja dopiero z submodułami w Task 14.
- [ ] **Step 5:** Commity `"Etap 4: tlumaczenie komentarzy na angielski (<katalog>)"` i
  `"Etap 4: naklady repozytoriow"`.

### Task 13: README z diagramami i dokumentacja techniczna

**Files:**
- Create: `tools/split/overlay/ballista-genomics/README.md`, `tools/split/overlay/ballista-genomics/OPIS.md`
  (zastępuje kopię; dokumentacja techniczna bez historii faz), `tools/split/overlay/sail-genomics/README.md`,
  `tools/split/overlay/genomics-engines-bench/README.md`
- Create (pomocniczo, poza repo): `$SCRATCH/explain/` — `EXPLAIN ANALYZE` każdej operacji na 2-7 przy P = 2

- [ ] **Step 1:** Zebrać materiał: `bench_client --explain-analyze` (klaster P = 2, release) dla 5 operacji;
  log startu Saila i plan Spark (`df.explain(True)` przez `sail_genomics.build_query`) dla overlap i merge.
- [ ] **Step 2: `ballista-genomics/README.md`** (po angielsku, jak całe Task 13) — sekcje: What it is; Diagram przebiegu
  zapytania (Mermaid `sequenceDiagram`: `dist_ops`/`bench_client` → `SessionContext` z planistą bio
  i `BioLogicalCodec` → scheduler (`ballista_node scheduler`, dekodowanie planu logicznego, reguły
  fizyczne, podział na etapy) → `BioPhysicalCodec::try_encode` → executory (`ballista_node executor`,
  `try_decode`, `IntervalJoinExec`/`MergeExec`/`SubtractExec`/`NearestExec`/`CountOverlapsExec`/`DistCoverageExec`)
  → shuffle/broadcast → strumień wyniku do klienta; nazwy typów i funkcji dokładnie jak w `src/`);
  Schemat elementów (Mermaid `flowchart`: nasz kod / Ballista / DataFusion / bio-functions (vendor + łatki));
  Tabela strategii na operację (z `EXPLAIN` ze Step 1); Wersje (Ballista 55, DataFusion 55.2,
  bio-functions v0.25.0; polars-bio 0.37 → v0.24.0 do 0.38, logika identyczna); Budowanie
  (rustup, protoc 28.3, `cargo build --release --bins`, czas); Przykład (`dist_ops` na `data/`,
  tryb wieloprocesowy z `ballista_node`); Testy; Licencja.
- [ ] **Step 3: `sail-genomics/README.md`** — Co to jest; Diagram przebiegu (Mermaid
  `sequenceDiagram`: PySpark → Spark Connect gRPC → serwer Saila (driver) → `groupBy("chrom").cogroup(...).applyInArrow`
  → shuffle po chromosomie → worker (wątek w procesie local-cluster) → `sail_genomics.arrow.evaluate`
  → `pb_context` (kontekst z puli) → `polars_bio.range_op.range_operation` (DataFusion 53 w procesie)
  → tabela Arrow → klient; zaznaczone przejścia Rust↔Python i miejsce przemianowania kolumn);
  Schemat elementów; Dlaczego applyInArrow, a nie UDTF (liczby historyczne: 11.10.2026, laptop,
  Sail 0.7.2, overlap 7-0, N = 1: UDTF 41,6 s, applyInArrow z pulą 2,57 s; LATERAL powiela wiersz
  zewnętrzny; brak argumentów TABLE); Dlaczego pula kontekstów (wynik z Task 3); Ograniczenia
  (local-cluster = jeden proces; prawdziwa wieloprocesowość = Kubernetes); Użycie (przykład z
  `local_session` i `build_query`); Testy; Licencja.
- [ ] **Step 4: `genomics-engines-bench/README.md`** — Co mierzy (P, równe zasoby, tabela silnik →
  procesy/sloty/CPU); Schemat procesów i CPU przy P (Mermaid `flowchart` dla laptopa P = 4 i P = 8 z SMT);
  Szybki start (`git clone --recursive`, `./setup.sh`, `uv run python -m bench.orchestrator bench/conf/smoke.yaml`,
  `scaling.yaml`, `bench.report`); Protokół (rozgrzewka, kontrola overlap 7-0 polars-bio 2 wątki, szum 1,8%
  przy `settle_s` 3 s z badania 10.10.2026, swap → nieważny, wzorzec 1 partycja + suma kontrolna);
  Sloty Saila (wynik Task 6); Wymagania (Linux x86_64, uv 0.12.22, rustup, protoc, RAM); Jak czytać
  `summary.md`; Znane ograniczenia (asymetria DataFusion 53/55 do polars-bio 0.38; klient Ballisty w Ruście,
  Saila w Pythonie; local mode; P = 8 na 6 rdzeniach z SMT); Wyniki (miejsce na Task 15); Licencja.
- [ ] **Step 5:** Sprawdzić każdy węzeł diagramów: `grep -rn "<nazwa>"` w kodzie (lista nazw z diagramów
  → każda ma trafienie w `src/`/`sail_genomics/`/`bench/` albo jest typem z Ballisty/DataFusion/pb
  sprawdzonym w `~/.cargo/registry` lub `.venv`). Mermaid poprawny składniowo (`npx -y @mermaid-js/mermaid-cli@11.4.2 -i README.md -o $SCRATCH/mmd.svg`
  albo ręcznie, jeśli brak node — wtedy zgłosić).
- [ ] **Step 6:** Commit `"Etap 4: README z diagramami i dokumentacja techniczna repozytoriow"`.

### Task 14: Publikacja przez PR: gałęzie `import`, submoduły, weryfikacja, przegląd na GitHubie

**Files:**
- Create: `/home/milosz/repos/{ballista-genomics,sail-genomics,genomics-engines-bench}` (klony z GitHuba,
  gałąź `import`), `/home/milosz/repos-staging/` (wynik `split.py`, poza repozytoriami)
- Modify: `README.md` monorepo (nota z linkami do nowych repozytoriów)

Przepływ (spec, sekcja 2, wiersz „GitHub”): użytkownik zakłada repozytoria z README (prywatne do
czasu scalenia — chyba że zdecyduje inaczej; wtedy kroki „zmiana widoczności” odpadają), kod idzie
gałęzią `import`, użytkownik przegląda diff w PR i scala merge commitem (nie squash/rebase —
hashe commitów muszą przetrwać dla submodułów).

- [ ] **Step 1:** Scalić `etap-4` do `master` w monorepo (fast-forward; push za zgodą). Eksport:
  `uv run python tools/split/split.py --source master --out /home/milosz/repos-staging` → kod 0.
- [ ] **Step 2: ZGODA UŻYTKOWNIKA (1).** Upewnić się, że repozytoria `ballista-genomics`,
  `sail-genomics`, `genomics-engines-bench` istnieją (z README; bez `.gitignore`/licencji). Dla
  `ballista-genomics` i `sail-genomics`: `git clone git@github.com:mkowalewski1282/<repo>.git /home/milosz/repos/<repo>`,
  `git switch -c import`, skopiować `/home/milosz/repos-staging/<repo>/` do klonu (`rsync -a --exclude .git`),
  szybka weryfikacja (testy jak w Task 12 Step 4), `git add -A && git commit -m "Import integration code"`
  (autor z konfiguracji gita użytkownika; bez stopek).
- [ ] **Step 3:** Przegląd treści w obu klonach (spec 6.5):
  `git grep -nIiE "specyfikacj|docs/superpowers|wnioski_claude|raporty/|ustalenia_zalozen|promotor|supervisor|claude|Wiewi"`,
  `git grep -nIP "[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]" -- ':!vendor'` i `git log --format=%B | grep -iE "claude|co-authored"`
  → wszystko puste; `git ls-files` bez `docs/superpowers`, `raporty`, `wnioski_claude.md`. Wynik pokazać
  użytkownikowi.
- [ ] **Step 4: ZGODA UŻYTKOWNIKA (2) na push.** `git push -u origin import` w obu repozytoriach;
  przekazać linki „Compare & pull request”
  (`https://github.com/mkowalewski1282/<repo>/compare/main...import?expand=1`). Użytkownik otwiera PR,
  ogląda diff, czeka na zielone CI na PR, scala **„Create a merge commit”**, potem (jeśli prywatne)
  Settings → Change visibility → Public. Poczekać na potwierdzenie obu.
- [ ] **Step 5:** `genomics-engines-bench`: klon do `/home/milosz/repos/genomics-engines-bench`,
  `git switch -c import`, `rsync` ze stagingu; `git submodule add ../ballista-genomics engines/ballista-genomics`
  i `git submodule add ../sail-genomics engines/sail-genomics` (względne URL rozwiązywane względem
  `origin` repo testowego — SSH lokalnie, HTTPS u kogoś, kto klonuje przez HTTPS); submoduły na `main`
  sąsiadów **po scaleniu** (`git -C engines/<repo> checkout origin/main`); `uv lock && uv sync --locked`;
  build Ballisty w submodule (wspólny `CARGO_TARGET_DIR` jak w Task 12 albo pełny build release w tle);
  `uv run pytest -m "not real_data" -q` → PASS; `uv run pytest -m real_data -q` → PASS; przegląd treści
  jak Step 3; `git commit -m "Import benchmark procedure"`.
- [ ] **Step 6: ZGODA UŻYTKOWNIKA (3) na push.** `git push -u origin import`; link do PR; użytkownik
  przegląda, CI zielone (submoduły publiczne), scala merge commitem, zmienia widoczność na publiczną.
- [ ] **Step 7:** Klon kontrolny w scratchpadzie:
  `git clone --recursive https://github.com/mkowalewski1282/genomics-engines-bench $SCRATCH/clone-check`
  → `git -C $SCRATCH/clone-check submodule status` bez `-`; CI na `main` zielone w trzech repozytoriach
  (publiczne API Actions). Lokalne klony: `git switch main && git pull`.
- [ ] **Step 8:** Monorepo: nota w `README.md` („kod rozwija się w …” + 3 linki); commit; push za zgodą.
- [ ] **Step 9:** Przekazać użytkownikowi: linki do repozytoriów, **gdzie są diagramy** (sekcje README
  każdego repo z nazwami sekcji) do jego przeglądu i uwag, przypomnienie o zaproszeniu współpracowników
  (Settings → Collaborators) w trzech repozytoriach.

## Etap D — pierwsza seria (jeśli starczy czasu)

### Task 15: Seria `scaling` na laptopie i wyniki w repo testowym

**Files:**
- Create: `/home/milosz/repos/genomics-engines-bench/bench/results/scaling/<znacznik>/{runs.parquet,summary.md,summary.csv,plots/*.png,env.json}`
- Modify: `/home/milosz/repos/genomics-engines-bench/README.md` (sekcja „Wyniki”)

- [ ] **Step 1:** Wypisać użytkownikowi **checklistę RAM** i poczekać na potwierdzenie; użytkownik
  uruchamia z Windows Terminal: `cd ~/repos/genomics-engines-bench && ./run_series.sh`
  (smoke → scaling → report).
- [ ] **Step 2:** Po serii: `summary.md` bez nieoczekiwanych nieważnych (poza swapem przy P = 8 i
  pominięciami); przejrzeć wykresy.
- [ ] **Step 3:** README „Wyniki”: link do `summary.md`, 1–2 wykresy, opis maszyny walidacyjnej
  (Ryzen 5 4600H, 6 rdzeni/12 wątków, WSL2 5 GB, P = 8 z SMT) i zastrzeżenie, że to wyniki walidacyjne,
  nie końcowe. Gałąź `results-scaling`, commit `"Add scaling series results from the validation machine"`;
  push za zgodą, PR do `main` (przegląd użytkownika, merge commit); CI zielone.

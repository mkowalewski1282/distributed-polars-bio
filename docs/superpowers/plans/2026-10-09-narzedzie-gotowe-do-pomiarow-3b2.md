# Plan 3b-2 — narzędzie gotowe do pomiarów: plan implementacji

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Narzędzie pomiarowe, którego wyniki można wstawić do rozdziału eksperymentalnego: czas bez kosztu naszego kodu sprawdzającego, przebieg kontrolny odporny na szum, reguła swapu, szybka ścieżka Saila (`applyInArrow` z kontekstem polars-bio na wątek), Ballista w buildzie release z podniesionym limitem gRPC, `broadcast_bytes` i `EXPLAIN ANALYZE`, fazy oraz ~20 drobnych poprawek z przeglądów.

**Architecture:** Wykonanie etapami ze specyfikacji (sekcja 8):
1. protokół pomiaru (Zadania 1–7);
2. Sail (Zadania 8–12);
3. Ballista (Zadania 13–17);
4. drobne poprawki i build release (Zadania 18–26);
5. weryfikacja na danych rzeczywistych (Zadanie 27, z użytkownikiem);
6. dokumentacja (Zadanie 28).

Każde zadanie kończy się zielonym pakietem `uv run pytest -m "not real_data"` i osobnym commitem. Interfejs silnika (`bench/engines.py`) dostaje kolejne metody — każde zadanie, które je dodaje, uzupełnia też `FakeEngine` w `tests/bench/test_orchestrator.py`.

**Tech Stack:**
- Python 3.12 (uv), polars-bio 0.36.0, pysail 0.7.2, PySpark 4.2.0 (Spark Connect), polars, pyarrow, numpy, pytest 9;
- Rust 1.95.0, Ballista 53.0.0, DataFusion `=53.0.0`, `datafusion-bio-function-ranges` v0.22.2 (vendor);
- GitHub Actions (CI tylko do poprawności).

**Spec:** `docs/superpowers/specs/2026-10-09-narzedzie-gotowe-do-pomiarow-3b2-design.md` (cała). Kontekst: specyfikacja metodyki `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`, plan 3a `docs/superpowers/plans/2026-10-01-narzedzie-pomiarowe-3a.md`, `wnioski_claude.md`, `ballista_genomics/OPIS.md`.

## Global Constraints

- **Bez zmian w polars-bio, Ballistcie i Sailu** (bez forków i łatek). Wewnętrzne funkcje polars-bio wolno **wołać** (`range_operation`, `RangeOptions`, `_metadata.set_coordinate_system`), nie wolno ich **zmieniać**. **Każda ewentualna zmiana w polars-bio wymaga wyraźnego, potrójnego potwierdzenia użytkownika (pogrubione pytanie, trzy razy) — w razie potrzeby zatrzymaj się i zapytaj.**
- **Wersje:** polars-bio zostaje `0.36.0` (`uv.lock`); `pyproject.toml` dostaje `polars>=1.44,<2` (Zadanie 25). Bez `uv lock --upgrade`.
- **Konwencja językowa (pilnuje `tests/test_code_language.py`):** po angielsku wszystko poza komentarzami — identyfikatory, komunikaty, opisy CLI, wartości w wynikach (`invalid_reason` itd.), nazwy plików i katalogów tworzonych w testach; komentarze i docstringi po polsku; dokumentacja `*.md` po polsku.
- **Rust:**
  - lokalnie **zawsze** `CARGO_BUILD_JOBS=1`;
  - **nigdy** nie uruchamiaj Pythona z polars-bio równolegle z budowaniem Rusta;
  - po każdej zmianie w `ballista_genomics/src` przebuduj debug: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins` (kilka–kilkanaście minut; uruchamiaj w tle albo z limitem czasu narzędzia ≥ 30 min);
  - build release **tylko w Zadaniu 26**.
- **Testy:** polecenie `uv run pytest …` z katalogu repozytorium; bez danych rzeczywistych: `-m "not real_data"`. Testów `real_data`, smoke i serii na danych rzeczywistych **nie** uruchamiaj przed Zadaniem 27 (wyjątek: Zadanie 12 — sonda w ograniczonym czasie, opisana tam).
- **Wartości ustalone w specyfikacji (kopiować dokładnie):**
  - przebieg kontrolny: `{op: overlap, pair: "7-0"}`, `control_repeats` = 10, `control_tolerance` = 0.10, porównanie **median**;
  - badanie szumu: 20 powtórzeń, kryterium odsetka fałszywych dryfów ≤ 5%;
  - limit gRPC: `BALLISTA_GRPC_MAX_MESSAGE_BYTES`, domyślnie 512 MiB = 536870912 B;
  - warianty Saila: `sail` (Arrow + kontekst na wątek), `sail_lock` (Arrow + blokada), `sail_udtf` (UDTF z `sail_bio.py`);
  - fazy: `phases.first_batch_s` (wszystkie), `phases.pb_s`, `phases.lock_wait_s` (Sail), `extra.udf_groups`, `extra.sail_workers`;
  - nowe kolumny wyników: `control_position`, `base_rss`; kolumna `broadcast_bytes` wypełniana;
  - klucze konfiguracji: `control`, `control_repeats`, `explain`, `expected_invalid`.
- **Commity:** komunikaty po polsku bez znaków diakrytycznych, zakończone linią `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; pracujemy na gałęzi `3b-2-narzedzie`; **żadnego pusha bez zgody użytkownika**.
- **Zmienne powłoki** nie przechodzą między krokami (każde polecenie to nowa powłoka).
- **Checklista RAM** (pamięć `feedback_ram_przed_eksperymentem`): przed Zadaniem 27 przypomnij ją i poczekaj na potwierdzenie użytkownika.
- **Zasada zatrzymania (spec, sekcja 8):** poważny problem (ścieżka Arrow w Sailu daje błędny wynik, błąd limitu gRPC nie dociera do klienta i zapytanie wisi, badanie szumu pokazuje > 5% fałszywych dryfów) ⇒ zatrzymaj się i zapytaj użytkownika.
- **CI** służy wyłącznie poprawności, nigdy pomiarom.

## Review Focus

Wejścia i stany, których spec nie wymienia wprost, a które najpewniej ugryzą użytkownika — każde ma test w zadaniu, które jest właścicielem kodu:

1. **Pusty wynik (0 wierszy)** — runner musi zgłosić `rows = 0`, sumę `0x0000000000000000` i `first_batch_s ≤ t_total_s` (brak pierwszej partii ⇒ `first_batch_s = t_total_s`). Testy: Zadania 1, 2, 3.
2. **Wszystkie (albo większość) przebiegów kontrolnych nieważne na jednym końcu bloku** — blok ma dostać przyczynę `control run invalid`, a nie wyjątek z mediany pustej listy. Test: Zadanie 5.
3. **Niepusty swap na starcie serii** (np. po wcześniejszej pracy bez `wsl --shutdown`) — seria odmawia startu z kodem 2 i czytelnym komunikatem, zanim powstanie katalog wyników. Test: Zadanie 6.
4. **Brak albo pusty plik faz Saila** (żadna grupa nie wywołała polars-bio) — runner zgłasza `pb_s = 0`, `udf_groups = 0`, a nie wyjątek. Test: Zadanie 9 (`read_phases`).
5. **Log procesu silnika krótszy niż zapamiętany offset** (plik zastąpiony, np. po restarcie) — liczniki z logów czytają wtedy od początku pliku zamiast rzucać wyjątek albo zwracać pusty tekst. Test: Zadanie 11 (`logscan.text_since`).

## Decyzje planu wykraczające poza literę specyfikacji

1. **Łączenie partii przed sumą kontrolną:** `Checksum.update_all` łączy kolejne partie w tabele po ≥ 65 536 wierszy (`GROUP_ROWS`) — bez tego suma na 1877 partiach Saila kosztowała 1,9 s (sonda 3b-2).
2. **Interfejs silnika** (`bench/engines.py`) dostaje metody: `log_mark()` / `log_metrics(mark)` (Zadanie 11), `explain_command(scenario, target)` (Zadanie 16), `dead()` i `restart(grace_s=...)` (Zadanie 18), `env_record()` (Zadanie 20).
3. **Rozgrzanie sesji Saila przed zegarem:** `spark.range(1).count()` we wszystkich ścieżkach — ścieżka Arrow nie rejestruje funkcji, a to rejestracja UDTF była dotąd pierwszym RPC sesji (start drivera i workerów).
4. **`extra.udf_groups`** = liczba wywołań polars-bio, czyli chromosomów z niepustą lewą stroną (na zbiorze testowym: 4 — chr1, chr2, chr10, chrA).
5. **Strażnik broadcastu:** sprawdzenie w `DistBioProvider::scan` (planowanie fizyczne — w schedulerze), dokładny rozmiar IPC tabeli rozsyłanej × `TASKS_PER_MESSAGE = 2` (sloty executora w narzędziu) ≤ limit; minimalny dopuszczalny limit = 16 MiB (domyślny limit Ballisty).
6. **`broadcast_bytes`** z logów executorów: linia `broadcast payload decoded: N bytes` (poziom info) przy dekodowaniu `NearestExec` i `DistCoverageExec`.
7. **Nieudany `EXPLAIN ANALYZE`** trafia do `summary.failures` (seria kończy się kodem 1) — smoke ma sprawdzać ten tryb.
8. **Testy jednostkowe Rusta** (`#[cfg(test)]` w `dist_udtf.rs`) i krok `cargo test --lib --locked` w CI — dla pozycji 18 (argument `'strict'|'weak'` obowiązkowy), której nie da się sprawdzić z Pythona.
9. **Badanie szumu** liczy tylko rundy mierzone (bez przebiegów kontrolnych bloku), 10 000 losowych podziałów, ziarno 0.
10. **`.python-version` = `3.12.15`** (pełna wersja) — strażnik interpretera porównuje pełną wersję z tym plikiem.
11. **`expected_invalid`** przyjmuje warianty z `VARIANTS` (bez `polars_bio_ref` — błąd wzorca i tak przerywa serię).
12. **Pozycja 12 (brak wartości w kolumnie chromosomu w sumie kontrolnej)** jest już pokryta: `tests/bench/test_checksum.py::test_accumulator_matches_definition_on_mixed_types` losuje `None` w `chrom`. Nowy test tylko dla „żywego runnera przy przerwaniu”.
13. **Build release na końcu Etapu 4** (Zadanie 26), a nie Etapu 3 — Zadanie 24 też zmienia Rusta, a drugi build release trwałby kolejne godziny.
14. **`env.json`** w katalogu serii: zmienne środowiska ustawione przez narzędzie dla każdego bloku (pozycja 5 listy drobnych poprawek „zapisywane w wynikach”).

## Fakty sprawdzone przy pisaniu planu (09.10.2026)

- **Ballista 53, limity gRPC** (źródła w `~/.cargo/registry/src/*/ballista-*-53.0.0`):
  - scheduler: `SchedulerConfig::with_grpc_server_max_decoding_message_size(u32)` i `with_grpc_server_max_encoding_message_size(u32)` (domyślnie 16777216);
  - scheduler → executor (klient gRPC): `GrpcClientConfig::from(&ballista_config)` z `override_config_producer` — czyli `ballista.client.grpc_max_message_size` z konfiguracji sesji (`SessionConfigExt::with_ballista_grpc_client_max_message_size(usize)`);
  - executor: pola `ExecutorProcessConfig { grpc_max_decoding_message_size: u32, grpc_max_encoding_message_size: u32, .. }`;
  - klient: ta sama opcja sesji co wyżej.
- **Sail 0.7.2, kandydaci na rozmiar partii** (napisy w `_native.abi3.so`): zmienna `SAIL_EXECUTION__BATCH_SIZE`; opcje Spark `spark.sql.execution.arrow.maxRecordsPerBatch`, `spark.sql.execution.arrow.maxBytesPerBatch`, `spark.sql.execution.arrow.maxRecordsPerOutputBatch`, `spark.connect.grpc.arrow.maxBatchSize`.
- **Log Saila:** start workera sesji — `worker (\d+) server is ready` (ten sam wzorzec w `tests/test_sail_runner.py::test_server_keeps_exactly_n_workers`).
- **polars-bio 0.36:** wszystkie pięć operacji woła `polars_bio.range_op.range_operation(df1, df2, range_options, output_type, ctx, …)` z `ctx` jako piątym argumentem pozycyjnym; współbieżne wywołania w wątkach: `PanicException … The table s1 already exists`; podmiana `range_operation` na wersję z kontekstem wątku — 0 rozbieżności (5 operacji × 6 wątków × 3 rundy, para 1-2).
- **Zbiór testowy** (`tests/parquet_fixture.py`): lewa strona A ma chromosomy chr1, chr2, chr10, chrA; prawa B — chr1, chr2, chr10, chrB.
- **`protoc-28.3-linux-x86_64.zip`**: SHA-256 `0ad949f04a6a174da83cdcbdb36dee0a4925272a5b6d83f79a6bf9852076d53f`.
- **Wszystkie wywołania `dist_*`** w kodzie (`scenario.rs`, `runner.rs`) już podają `'strict'`; binarki `*_local` wołają funkcje vendora (bez `dist_`).
- Wersja crate'a algorytmów jest w `ballista_genomics/Cargo.lock` (`name = "datafusion-bio-function-ranges"`, `version = "0.22.2"`).

## Struktura plików

| Plik | Rola | Zadania |
|---|---|---|
| `bench/checksum.py` | `Checksum.update_all`, `GROUP_ROWS` | 1 |
| `bench/runners/polars_bio_runner.py` | czas do ostatniej partii, suma po zegarze, `first_batch_s` | 1 |
| `bench/runners/sail_runner.py` | jak wyżej + `--path`, rozgrzanie sesji, fazy z pliku | 2, 10, 12 |
| `ballista_genomics/src/bin/bench_client.rs` | jak wyżej + `--explain-analyze`, walidacja limitu gRPC | 3, 13, 16 |
| `bench/config.py` | `control`, `control_repeats`, warianty Saila, `explain`, `expected_invalid` | 4, 11, 16, 23 |
| `bench/validity.py` | `control_drift` (mediany), `pswpin` | 5, 6 |
| `bench/results.py` | kolumny `control_position`, `base_rss` | 5, 21 |
| `bench/orchestrator.py` | przebiegi kontrolne ×N, swap, logi, explain, śmierć silnika, postęp, stderr, podsumowanie | 4–6, 11, 15–23 |
| `bench/metrics.py` | `swap_used`, `rss` | 6, 21 |
| `bench/control_noise.py`, `bench/conf/control_noise.yaml` (nowe) | badanie szumu przebiegu kontrolnego | 7 |
| `sail_pb_context.py` (nowy) | kontekst polars-bio na wątek (pomost) | 8 |
| `sail_arrow.py` (nowy) | ścieżka Arrow w Sailu, plik faz | 9 |
| `bench/logscan.py` (nowy) | liczniki z logów silników (workery Saila, `broadcast_bytes`) | 11, 15 |
| `bench/engines.py` | warianty Saila, logi, limit gRPC, explain, strażnik binarek, śmierć/twarde zatrzymanie, środowisko | 11, 13, 15–18, 20 |
| `bench/conf/sail_paths.yaml` (nowy) | porównanie ścieżek Saila | 11 |
| `bench/runners/sail_server.py` | ewentualne ustawienie partii (Zadanie 12) | 12 |
| `ballista_genomics/src/runner.rs` | `grpc_max_message_bytes()`, `pub fn extract_text` | 13, 16 |
| `ballista_genomics/src/cluster.rs`, `src/bin/ballista_node.rs` | limit gRPC w kliencie, schedulerze i executorze | 13 |
| `ballista_genomics/src/broadcast.rs` (nowy), `src/dist_provider.rs`, `src/codec_io.rs`, `src/lib.rs` | strażnik rozmiaru broadcastu | 14 |
| `ballista_genomics/src/bio_phys_codec.rs`, `Cargo.toml` | log rozmiaru ładunku (`tracing`) | 15 |
| `ballista_genomics/src/dist_udtf.rs` | `'strict'|'weak'` obowiązkowe, testy jednostkowe | 24 |
| `bench/preflight.py` (nowy) | sprawdzenia przed serią (CPU, SMT, narzędzia, swap) | 19 |
| `bench/__init__.py`, `.python-version` | pełna wersja Pythona | 23 |
| `.github/workflows/tests.yml` | `protoc` z sumą SHA-256, `cargo test --lib` | 23, 24 |
| `.gitignore` | wyjątek dla `bench/results/**/explain/` | 16 |
| `tests/...` | testy opisane w zadaniach | wszystkie |

## Przygotowanie (przed Zadaniem 1)

- [ ] **P.1: Gałąź i stan wyjściowy**

```bash
cd /home/milosz/praca_magisterska && git status --short && git switch -c 3b-2-narzedzie && git log --oneline -1
```
Oczekiwane: `?? bench/results/` (wyniki smoke z 04.10 — zacommitowane w Zadaniu 25), gałąź `3b-2-narzedzie` od bieżącego `master` (commit z tym planem).

- [ ] **P.2: Binarki debug i zielony pakiet**

```bash
cd /home/milosz/praca_magisterska/ballista_genomics && ls -la target/debug/ballista_node target/debug/bench_client target/debug/dist_ops target/debug/nearest_local
cd /home/milosz/praca_magisterska && uv run pytest -m "not real_data" -q 2>&1 | tail -5
```
Oczekiwane: binarki istnieją; `0 failed` (liczba passed/xfailed zapisz — to punkt odniesienia; xfail: regresja LATERAL Saila i standalone przy broadcaście). Jeśli brakuje binarek: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins`.

---

# Etap 1 — protokół pomiaru (spec, sekcja 4)

### Zadanie 1: Suma kontrolna po zatrzymaniu zegara — `Checksum.update_all` i runner polars-bio

**Files:**
- Modify: `bench/checksum.py` (stała `GROUP_ROWS`, metoda `Checksum.update_all`)
- Modify: `bench/runners/polars_bio_runner.py` (funkcja `run`, docstring modułu)
- Test: `tests/bench/test_checksum.py`, `tests/test_polars_bio_runner.py`, `tests/test_orchestrator_integration.py`

**Interfaces:**
- Produces: `bench.checksum.GROUP_ROWS: int = 65536`; `Checksum.update_all(parts: Iterable[pa.Table | pa.RecordBatch]) -> None` (części w schemacie znormalizowanym; łączy kolejne partie w tabele po ≥ `GROUP_ROWS` wierszy). Linia JSON runnera polars-bio: `phases == {"first_batch_s": float}`, `extra == {"target_partitions": int, "checksum_s": float}`; `checksum_s` NIE jest częścią `t_total_s`.

- [ ] **Step 1: Testy `update_all` (dopisz na końcu `tests/bench/test_checksum.py`)**

```python
def _coverage_rows_and_table(n: int = 5000):
    rng = random.Random(11)
    rows = [
        (rng.choice(["chr1", "chr2", "chrX", None]), rng.randrange(2**31), rng.randrange(2**31),
         rng.choice([None, rng.randrange(10**6)]))
        for _ in range(n)
    ]
    table = pa.table({
        "chrom": pa.array([r[0] for r in rows], pa.string_view()),
        "start": pa.array([r[1] for r in rows], pa.int32()),
        "end": pa.array([r[2] for r in rows], pa.int64()),
        "coverage": pa.array([r[3] for r in rows], pa.int64()),
    })
    return rows, table


def test_update_all_matches_definition_for_many_small_parts(monkeypatch):
    """Sail oddaje wynik w partiach po ~1,5 tys. wierszy — update_all łączy je przed liczeniem
    (mały GROUP_ROWS wymusza kilka grup i resztę); części to na przemian Table i RecordBatch."""
    monkeypatch.setattr(cs, "GROUP_ROWS", 700)
    rows, table = _coverage_rows_and_table()
    parts = [table.slice(i, 100) for i in range(0, table.num_rows, 100)]
    parts = [p.combine_chunks().to_batches()[0] if i % 2 else p for i, p in enumerate(parts)]
    acc = cs.Checksum("coverage")
    acc.update_all(parts)
    assert acc.rows == len(rows)
    assert acc.hex() == cs.format_checksum(cs.checksum_rows("coverage", rows))


def test_update_all_of_nothing_is_zero():
    acc = cs.Checksum("overlap")
    acc.update_all([])
    assert (acc.rows, acc.hex()) == (0, "0x0000000000000000")
```

- [ ] **Step 2: Uruchom — oczekiwane FAIL**

Run: `uv run pytest tests/bench/test_checksum.py -k update_all -v`
Expected: FAIL — `AttributeError: ... has no attribute 'GROUP_ROWS'` / `'update_all'`.

- [ ] **Step 3: Implementacja w `bench/checksum.py`**

Pod stałymi `_MIX1`/`_MIX2` dodaj:

```python
#: Partie wyniku łączone przed liczeniem sumy kontrolnej (Checksum.update_all): koszt numpy jest
#: stały na wywołanie, a Sail oddaje wynik w partiach po ~1,5 tys. wierszy — sonda 3b-2: 1877
#: partii i 1,9 s sumy kontrolnej na overlap 7-0, gdy liczono ją osobno na każdej partii.
GROUP_ROWS = 1 << 16
```

W klasie `Checksum`, pod metodą `update`, dodaj:

```python
    def update_all(self, parts: Iterable) -> None:
        """Dokłada wiele porcji (pyarrow.Table / RecordBatch w schemacie znormalizowanym),
        łącząc kolejne partie w tabele po co najmniej GROUP_ROWS wierszy."""
        group: list[pa.RecordBatch] = []
        size = 0
        for part in parts:
            for batch in part.to_batches() if isinstance(part, pa.Table) else [part]:
                group.append(batch)
                size += batch.num_rows
                if size >= GROUP_ROWS:
                    self.update(pa.Table.from_batches(group))
                    group, size = [], 0
        if group:
            self.update(pa.Table.from_batches(group))
```

W docstringu modułu zastąp zdanie „`Checksum` liczy to samo wektorowo (numpy) na partiach Arrow — dla runnerów, które konsumują wynik strumieniowo (5 mln wierszy ≈ 0,5 s).” zdaniem: „`Checksum` liczy to samo wektorowo (numpy) na partiach Arrow; runnery wołają `update_all` po zatrzymaniu zegara, na partiach trzymanych w pamięci (specyfikacja 3b-2, 4.1).”

- [ ] **Step 4: Uruchom — PASS**

Run: `uv run pytest tests/bench/test_checksum.py -v`
Expected: wszystkie PASS.

- [ ] **Step 5: Testy runnera polars-bio — nowy protokół i pusty wynik**

W `tests/test_polars_bio_runner.py`:
1. dodaj import `from tests.parquet_fixture import write_parts`;
2. w `test_single_partition_matches_polars_bio_oracle` zastąp linie od `assert set(got["extra"]) == ...` do końca funkcji przez:

```python
    assert set(got["extra"]) == {"target_partitions", "checksum_s"}
    assert set(got["phases"]) == {"first_batch_s"}
    assert got["extra"]["target_partitions"] == 1
    assert got["t_total_s"] > 0 and got["peak_rss_bytes"] > 10 * 2**20
    # Suma kontrolna liczona PO zatrzymaniu zegara (specyfikacja 3b-2, 4.1): jej czas jest
    # osobno, a faza „do pierwszej partii” mieści się w czasie całkowitym.
    assert got["extra"]["checksum_s"] > 0
    assert 0 < got["phases"]["first_batch_s"] <= got["t_total_s"]
```

3. dopisz na końcu pliku:

```python
def test_empty_result_reports_zero_rows(tmp_path):
    """Rozłączne chromosomy: overlap bez wyniku — zero wierszy, suma zerowa, faza w granicach."""
    left = write_parts([("chrA", 10, 20), ("chrA", 30, 40)], tmp_path / "a")
    right = write_parts([("chrB", 10, 20), ("chrB", 30, 40)], tmp_path / "b")
    got = report(run_runner(["--op", "overlap", "--left", str(left), "--right", str(right), "--threads", "1"]))
    assert (got["rows"], got["checksum"]) == (0, "0x0000000000000000")
    assert 0 <= got["phases"]["first_batch_s"] <= got["t_total_s"]
```

Run: `uv run pytest tests/test_polars_bio_runner.py -v`
Expected: FAIL (`phases` puste — `set() != {'first_batch_s'}`).

- [ ] **Step 6: Implementacja w `bench/runners/polars_bio_runner.py`**

W docstringu modułu zastąp akapit zaczynający się od „Czas: od wywołania operacji…” (do „`extra.checksum_s`.”) tekstem:

```
Czas: od wywołania operacji do odebrania ostatniej partii; bez startu procesu, importów
i ustawień. Partie są trzymane w pamięci, a suma kontrolna liczona PO zatrzymaniu zegara
(specyfikacja 3b-2, 4.1) — jej czas podaje informacyjnie `extra.checksum_s`.
`phases.first_batch_s`: czas do pierwszej partii (brak partii → równy t_total_s).
```

Funkcję `run` zastąp:

```python
def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], threads: int) -> str:
    require_paths(left, right)
    import polars_bio as pb

    # Ścieżki nie niosą metadanych układu współrzędnych; dane databio-8p są 0-based.
    warnings.filterwarnings("ignore", message="Coordinate system metadata is missing")
    pb.set_option("datafusion.bio.coordinate_system_zero_based", True)
    pb.set_option(pb.POLARS_BIO_MAX_THREADS, threads)
    reset_peak_rss()
    t0 = time.perf_counter()
    if op in UNARY_OPS:
        df = pb.merge(source(left), cols=list(cols), output_type="datafusion.DataFrame")
    else:
        df = getattr(pb, op)(
            source(left), source(right), cols1=list(cols), cols2=list(cols),
            output_type="datafusion.DataFrame",
        )
    batches = []
    first_batch_s = None
    for batch in df.execute_stream():
        batches.append(batch.to_pyarrow())
        if first_batch_s is None:
            first_batch_s = time.perf_counter() - t0
    t_total_s = time.perf_counter() - t0
    t = time.perf_counter()
    checksum = Checksum(op)
    checksum.update_all(normalize_arrow(op, b, cols) for b in batches)
    checksum_s = time.perf_counter() - t
    return report_line(
        rows=checksum.rows,
        checksum=checksum.hex(),
        t_total_s=t_total_s,
        peak_rss_bytes=peak_rss(),
        phases={"first_batch_s": t_total_s if first_batch_s is None else first_batch_s},
        extra={
            "target_partitions": int(pb.get_option(pb.POLARS_BIO_MAX_THREADS)),
            "checksum_s": checksum_s,
        },
    )
```

- [ ] **Step 7: Test integracyjny orkiestratora — asercja czasu sumy**

W `tests/test_orchestrator_integration.py` funkcję `test_every_runner_reports_checksum_time` zastąp (pełną wersję z `first_batch_s` dla wszystkich silników dopisze Zadanie 3):

```python
@pytest.mark.nodes(2)
def test_every_runner_reports_checksum_time(series):
    """Koszt sumy kontrolnej zależy od silnika (Python albo Rust) — każdy runner go podaje
    (przegląd końcowy planu 3a); od planu 3b-2 nie jest częścią t_total_s."""
    _, df = series
    assert all(json.loads(e)["checksum_s"] > 0 for e in df["extra"])
```

- [ ] **Step 8: Uruchom — PASS**

Run: `uv run pytest tests/bench/test_checksum.py tests/test_polars_bio_runner.py -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 9: Commit**

```bash
git add bench/checksum.py bench/runners/polars_bio_runner.py tests/bench/test_checksum.py tests/test_polars_bio_runner.py tests/test_orchestrator_integration.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 1): suma kontrolna po zatrzymaniu zegara w runnerze polars-bio

Partie wyniku trzymane w pamieci, suma liczona po ostatniej partii na grupach
po >= 65536 wierszy (Checksum.update_all); faza first_batch_s.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 2: Runner Saila — ten sam protokół i rozgrzanie sesji przed zegarem

**Files:**
- Modify: `bench/runners/sail_runner.py`
- Test: `tests/test_sail_runner.py`

**Interfaces:**
- Consumes: `Checksum.update_all` (Zadanie 1).
- Produces: `bench.runners.sail_runner.warm_up(spark) -> None` (pierwsze zapytanie sesji przed zegarem); linia JSON runnera Saila: `phases == {"first_batch_s": float}`, `extra == {"checksum_s": float}` (w tym zadaniu ścieżka UDTF; `--path` dochodzi w Zadaniu 10).

- [ ] **Step 1: Testy**

W `tests/test_sail_runner.py`:
1. dodaj import `from tests.parquet_fixture import write_parts`;
2. w `test_runner_matches_polars_bio` zastąp linie od `assert got["t_total_s"] > 0 and got["phases"] == {} ...` do `assert 0 < got["extra"]["checksum_s"] <= got["t_total_s"]` przez:

```python
    assert got["t_total_s"] > 0 and set(got["phases"]) == {"first_batch_s"}
    assert set(got["extra"]) == {"checksum_s"} and got["extra"]["checksum_s"] > 0
    assert 0 < got["phases"]["first_batch_s"] <= got["t_total_s"]
```

3. dopisz:

```python
def test_empty_result_reports_zero_rows(server, tmp_path):
    left = write_parts([("chrA", 10, 20), ("chrA", 30, 40)], tmp_path / "a")
    right = write_parts([("chrB", 10, 20), ("chrB", 30, 40)], tmp_path / "b")
    got = report(run_runner(["--op", "overlap", "--left", str(left), "--right", str(right), "--remote", server.url]))
    assert (got["rows"], got["checksum"]) == (0, "0x0000000000000000")
    assert 0 <= got["phases"]["first_batch_s"] <= got["t_total_s"]
```

Run: `uv run pytest tests/test_sail_runner.py -v`
Expected: FAIL (`phases` puste).

- [ ] **Step 2: Implementacja w `bench/runners/sail_runner.py`**

W docstringu modułu zastąp akapit „Wynik jest konsumowany strumieniowo…” (do końca docstringu) tekstem:

```
Wynik jest odbierany jako tabele Arrow przez `client.to_table_as_iterator` — to samo, czego
używa publiczne toLocalIterator, ale bez zamiany na obiekty Row — i trzymany w pamięci; zegar
staje po ostatniej partii, a suma kontrolna jest liczona potem (specyfikacja 3b-2, 4.1; jej czas:
`extra.checksum_s`). `phases.first_batch_s`: czas do pierwszej partii. Szczyt pamięci: proces
klienta (z trzymanym wynikiem); szczyt serwera mierzy orkiestrator.
```

Pod funkcją `iter_arrow` dodaj:

```python
def warm_up(spark) -> None:
    """Pierwsze zapytanie sesji, przed zegarem: Sail uruchamia driver i workery sesji przy
    pierwszym RPC (sonda 02.10.2026). Ścieżka Arrow nie rejestruje funkcji, a to rejestracja UDTF
    była dotąd tym RPC — rozgrzanie wyrównuje obie ścieżki."""
    spark.range(1).count()
```

Funkcję `run` zastąp:

```python
def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], remote: str) -> str:
    require_paths(left, right)
    from pyspark.sql import SparkSession

    import sail_bio

    spark = SparkSession.builder.remote(remote).create()
    try:
        sail_bio.register(spark, op)
        warm_up(spark)
        reset_peak_rss()
        t0 = time.perf_counter()
        df = sail_bio.build_query(spark, op, left, right, cols)
        tables = []
        first_batch_s = None
        for table in iter_arrow(spark, df):
            tables.append(table)
            if first_batch_s is None:
                first_batch_s = time.perf_counter() - t0
        t_total_s = time.perf_counter() - t0
    finally:
        spark.stop()
    t = time.perf_counter()
    checksum = Checksum(op)
    checksum.update_all(tables)
    checksum_s = time.perf_counter() - t
    return report_line(
        rows=checksum.rows, checksum=checksum.hex(), t_total_s=t_total_s, peak_rss_bytes=peak_rss(),
        phases={"first_batch_s": t_total_s if first_batch_s is None else first_batch_s},
        extra={"checksum_s": checksum_s},
    )
```

- [ ] **Step 3: Uruchom — PASS**

Run: `uv run pytest tests/test_sail_runner.py -v`
Expected: PASS (w tym `test_server_keeps_exactly_n_workers`).

- [ ] **Step 4: Commit**

```bash
git add bench/runners/sail_runner.py tests/test_sail_runner.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 1): runner Saila - suma po zegarze, first_batch_s, rozgrzanie sesji

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 3: `bench_client` (Ballista) — ten sam protokół

**Files:**
- Modify: `ballista_genomics/src/bin/bench_client.rs` (docstring, `struct Report`, `run`, linia JSON w `main`)
- Test: `tests/test_bench_client_protocol.py`, `tests/test_orchestrator_integration.py`

**Interfaces:**
- Produces: linia JSON `bench_client`: `"phases": {"first_batch_s": F}`, `"extra": {"target_partitions": P, "checksum_s": C}`; `checksum_s` poza `t_total_s`; `--output` zapisywany po zatrzymaniu zegara.

- [ ] **Step 1: Testy**

W `tests/test_bench_client_protocol.py`:
1. dodaj import `from tests.parquet_fixture import write_parts`;
2. w `test_report_follows_protocol_and_matches_oracle` zastąp linie od `assert got["t_total_s"] > 0 and got["phases"] == {}` do `assert 0 < got["extra"]["checksum_s"] <= got["t_total_s"]` przez:

```python
    assert got["t_total_s"] > 0 and set(got["phases"]) == {"first_batch_s"}
    assert set(got["extra"]) == {"target_partitions", "checksum_s"}
    assert got["extra"]["target_partitions"] == 4
    # Suma kontrolna liczona PO zatrzymaniu zegara (specyfikacja 3b-2, 4.1).
    assert got["extra"]["checksum_s"] > 0
    assert 0 < got["phases"]["first_batch_s"] <= got["t_total_s"]
```

3. dopisz:

```python
def test_empty_result_reports_zero_rows(tmp_path):
    left = write_parts([("chrA", 10, 20), ("chrA", 30, 40)], tmp_path / "a")
    right = write_parts([("chrB", 10, 20), ("chrB", 30, 40)], tmp_path / "b")
    got = report(run_client(["--op", "overlap", "--left", str(left), "--right", str(right)]))
    assert (got["rows"], got["checksum"]) == (0, "0x0000000000000000")
    assert 0 <= got["phases"]["first_batch_s"] <= got["t_total_s"]
```

W `tests/test_orchestrator_integration.py` funkcję `test_every_runner_reports_checksum_time` zastąp:

```python
@pytest.mark.nodes(2)
def test_every_runner_reports_checksum_time_and_first_batch(series):
    """Koszt sumy kontrolnej zależy od silnika (Python albo Rust) — każdy runner go podaje,
    poza t_total_s (specyfikacja 3b-2, 4.1); faza do pierwszej partii mieści się w t_total_s."""
    _, df = series
    assert all(json.loads(e)["checksum_s"] > 0 for e in df["extra"])
    first = [json.loads(p)["first_batch_s"] for p in df["phases"]]
    assert all(0 <= f <= t for f, t in zip(first, df["t_total_s"]))
```

- [ ] **Step 2: Implementacja w `bench_client.rs`**

W docstringu pliku zastąp dwie pozycje listy („czas: …” i „suma kontrolna: …”) oraz zdanie „Wynik jest konsumowany strumieniowo (bez zbierania w pamięci klienta), w schemacie znormalizowanym (`scenario.rs`).” przez:

```rust
//! - czas: od wysłania zapytania (`ctx.sql`) do odebrania ostatniej partii — bez startu
//!   procesu i połączenia z klastrem; `phases.first_batch_s` — czas do pierwszej partii;
//! - suma kontrolna: `checksum.rs` (specyfikacja 8.4), liczona PO zatrzymaniu zegara na
//!   partiach trzymanych w pamięci (specyfikacja 3b-2, 4.1); jej czas — `extra.checksum_s`;
```
oraz
```rust
//! Wynik jest trzymany w pamięci klienta (w schemacie znormalizowanym, `scenario.rs`) do
//! końca pomiaru czasu.
```
Zmień też przykład linii JSON w docstringu na `"phases": {"first_batch_s": F}`.

`struct Report` zastąp:

```rust
struct Report {
    rows: u64,
    checksum: String,
    t_total_s: f64,
    first_batch_s: f64,
    checksum_s: f64,
    target_partitions: usize,
    peak_rss_bytes: u64,
}
```

Funkcję `run` zastąp:

```rust
async fn run(scenario: &Scenario, output: Option<&str>) -> Result<Report> {
    let ctx = connect_from_env().await?;
    let target_partitions = ctx.state().config().target_partitions();
    reset_peak_rss()?;
    let t0 = Instant::now();
    let mut stream = ctx.sql(&scenario.sql()).await?.execute_stream().await?;
    let schema = stream.schema();
    let mut batches = Vec::new();
    let mut first_batch: Option<Duration> = None;
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
        first_batch.get_or_insert_with(|| t0.elapsed());
    }
    let total = t0.elapsed();
    let t = Instant::now();
    let mut checksum = Checksum::new(scenario.op);
    for batch in &batches {
        checksum.update(batch)?;
    }
    let checksum_s = t.elapsed().as_secs_f64();
    if let Some(path) = output {
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    Ok(Report {
        rows: checksum.rows(),
        checksum: checksum.hex(),
        t_total_s: total.as_secs_f64(),
        first_batch_s: first_batch.unwrap_or(total).as_secs_f64(),
        checksum_s,
        target_partitions,
        peak_rss_bytes: peak_rss_bytes()?,
    })
}
```

W `main` gałąź `Mode::Run` zastąp:

```rust
        Mode::Run { scenario, output } => run(&scenario, output.as_deref()).await.map(|r| {
            format!(
                "{{\"rows\": {}, \"checksum\": \"{}\", \"t_total_s\": {:.6}, \
                 \"phases\": {{\"first_batch_s\": {:.6}}}, \
                 \"extra\": {{\"target_partitions\": {}, \"checksum_s\": {:.9}}}, \
                 \"peak_rss_bytes\": {}}}",
                r.rows, r.checksum, r.t_total_s, r.first_batch_s, r.target_partitions,
                r.checksum_s, r.peak_rss_bytes
            )
        }),
```

- [ ] **Step 3: Przebuduj debug**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -3`
Expected: `Finished` bez ostrzeżeń o nieużywanych importach (jeśli `Duration` przestał być potrzebny gdzie indziej — zostaje, bo używa go `first_batch`).

- [ ] **Step 4: Uruchom — PASS**

Run: `uv run pytest tests/test_bench_client_protocol.py tests/test_ballista_parquet.py -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed` (test integracyjny sprawdza `first_batch_s` wszystkich runnerów).

- [ ] **Step 5: Commit**

```bash
git add ballista_genomics/src/bin/bench_client.rs tests/test_bench_client_protocol.py tests/test_orchestrator_integration.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 1): bench_client - suma po zegarze i first_batch_s

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 4: Konfiguracja — zadanie kontrolne `control` i `control_repeats`

**Files:**
- Modify: `bench/config.py` (`DEFAULTS`, `_scenario`, `SeriesConfig`, `parse`, `check_data`; usunięcie stałej `CONTROL`)
- Modify: `bench/orchestrator.py` (import; `reference_pass`, `run_block` — `self.cfg.control` zamiast `CONTROL`)
- Test: `tests/bench/test_config.py`, `tests/bench/test_orchestrator.py`, `tests/test_orchestrator_integration.py`

**Interfaces:**
- Produces: `SeriesConfig.control: Scenario` (domyślnie `Scenario("overlap", "7-0")`), `SeriesConfig.control_repeats: int` (domyślnie 10, ≥ 1); stała `bench.config.CONTROL` znika; `_scenario(item, where: str) -> Scenario`.

- [ ] **Step 1: Testy konfiguracji (`tests/bench/test_config.py`)**

1. W `test_minimal_config_gets_spec_defaults` dopisz:

```python
    # Przebieg kontrolny (specyfikacja 3b-2, 4.2): overlap 7-0, 10 przebiegów na każdym końcu bloku.
    assert (cfg.control, cfg.control_repeats) == (Scenario("overlap", "7-0"), 10)
```

2. Do listy parametrów `test_invalid_config_is_rejected_before_any_run` dopisz:

```python
        ({"control": "overlap"}, "control: expected a mapping"),
        ({"control": {"op": "merge", "pair": "1-2"}}, "control: merge works on a single dataset"),
        ({"control": {"op": "overlap", "pair": "1-9"}}, "control: pair"),
        ({"control_repeats": 0}, "control_repeats"),
        ({"control_repeats": True}, "control_repeats"),
```

3. Dopisz test:

```python
def test_control_can_be_overridden():
    cfg = parse(raw(control={"op": "merge", "dataset": 1}, control_repeats=3))
    assert (cfg.control, cfg.control_repeats) == (Scenario("merge", "1"), 3)
```

4. `test_check_data_requires_parts_for_scenarios_and_control_pair` zastąp:

```python
def test_check_data_requires_parts_for_scenarios_and_control_pair(tmp_path):
    cfg = parse(raw(scenarios=[{"op": "merge", "dataset": 1}]))
    with pytest.raises(ConfigError, match="chainRn4"):  # zbiór 0 — z pary kontrolnej 7-0
        check_data(cfg, tmp_path)
    for idx in (1, 7, 0):
        d = tmp_path / "databio-8p" / DATASETS[idx]
        d.mkdir(parents=True)
        (d / "part-00000.parquet").write_bytes(b"")
    check_data(cfg, tmp_path)
```

Run: `uv run pytest tests/bench/test_config.py -v`
Expected: FAIL (`SeriesConfig` nie ma `control`).

- [ ] **Step 2: Implementacja w `bench/config.py`**

1. `DEFAULTS` zastąp:

```python
#: Wartości domyślne ze specyfikacji 8.2 i 6.3 (tolerancja dryfu przebiegu kontrolnego: 10%)
#: oraz 3b-2, 4.2: zadanie kontrolne overlap 7-0 (~1–2 s w polars-bio A; para 1-2 trwała ~0,13 s
#: i szum przekraczał próg) wykonywane po 10 razy na początku i na końcu bloku.
DEFAULTS = {
    "repeats": 5, "warmup": 1, "timeout_s": 1200, "control_tolerance": 0.10,
    "control": {"op": "overlap", "pair": "7-0"}, "control_repeats": 10,
}
```

2. Usuń stałą `CONTROL` razem z komentarzem nad nią.
3. W `SeriesConfig` dopisz na końcu pól:

```python
    control: Scenario
    control_repeats: int
```

4. Sygnaturę `def _scenario(item, i: int) -> Scenario:` zmień na `def _scenario(item, where: str) -> Scenario:` i usuń pierwszą linię ciała (`where = f"scenarios[{i}]"`).
5. W `parse`: linię ze scenariuszami zmień na

```python
    scenarios = tuple(_scenario(item, f"scenarios[{i}]") for i, item in enumerate(_list(raw, "scenarios")))
```

a w wywołaniu `SeriesConfig(...)` dopisz argumenty:

```python
        control=_scenario(raw["control"], "control"),
        control_repeats=_int(raw, "control_repeats", 1),
```

6. W `check_data`: `for scenario in (*cfg.scenarios, CONTROL):` → `for scenario in (*cfg.scenarios, cfg.control):` i docstring: „Każdy zbiór serii — i zadania kontrolnego — ma mieć pliki part-*.parquet.”

- [ ] **Step 3: Orkiestrator — `cfg.control`**

W `bench/orchestrator.py`:
1. z importu `from bench.config import (...)` usuń `CONTROL`;
2. w `reference_pass`: `dict.fromkeys([*self.cfg.scenarios, CONTROL])` → `dict.fromkeys([*self.cfg.scenarios, self.cfg.control])`;
3. w `run_block`: `run(CONTROL, 0, is_control=True)` → `run(self.cfg.control, 0, is_control=True)` i analogicznie `run(CONTROL, 1, ...)`.

- [ ] **Step 4: Testy orkiestratora — zadanie kontrolne jawnie**

1. W `tests/bench/test_orchestrator.py`, w słowniku `raw` funkcji `config(**overrides)` dopisz (testy logiki bloków dalej używają pary 1-2 i jednego przebiegu na koniec bloku):

```python
        "control": {"op": "overlap", "pair": "1-2"},
        "control_repeats": 1,
```

2. W `test_main_fails_fast_without_ballista_binaries` do słownika YAML dopisz `"control": {"op": "overlap", "pair": "1-2"},` (inaczej `check_data` zgłosi brak zbiorów 7 i 0 przed sprawdzeniem binarek).
3. W `tests/test_orchestrator_integration.py`: w `series` do słownika `parse({...})` dopisz `"control": {"op": "overlap", "pair": "1-2"}, "control_repeats": 1,`; w `test_main_resolves_relative_data_root` do słownika YAML dopisz te same dwa klucze.

- [ ] **Step 5: Uruchom — PASS**

Run: `uv run pytest tests/bench/test_config.py tests/bench/test_orchestrator.py -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 6: Commit**

```bash
git add bench/config.py bench/orchestrator.py tests/bench/test_config.py tests/bench/test_orchestrator.py tests/test_orchestrator_integration.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 1): zadanie kontrolne w konfiguracji (control, control_repeats)

Domyslnie overlap 7-0 i 10 przebiegow na kazdym koncu bloku; stala CONTROL usunieta.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 5: Przebiegi kontrolne ×`control_repeats`, dryf median, kolumna `control_position`

**Files:**
- Modify: `bench/validity.py` (usunięcie `block_drifted`, nowe `control_drift`)
- Modify: `bench/orchestrator.py` (`_Series.row`, `_progress`, `run_block`, import)
- Modify: `bench/results.py` (kolumna `control_position`)
- Test: `tests/bench/test_validity.py`, `tests/bench/test_orchestrator.py`, `tests/bench/test_results.py`

**Interfaces:**
- Consumes: `SeriesConfig.control`, `control_repeats` (Zadanie 4).
- Produces: `bench.validity.control_drift(start: list[float], end: list[float], *, repeats: int, tolerance: float) -> str | None` (przyczyna `"control run invalid"` / `"control drift N%"` albo `None`); kolumna wyników `control_position: str | None` (`"start"` / `"end"`); przebiegi kontrolne mają `rep` = 1..`control_repeats`. `_Series.row(..., *, warmup=False, control_position=None, reference=False)`.

- [ ] **Step 1: Testy reguły (`tests/bench/test_validity.py`)**

1. W imporcie `from bench.validity import ...` zamień `block_drifted` na `control_drift`.
2. `test_drift_rule` zastąp:

```python
def test_drift_rule():
    assert drift(1.0, 1.2) == pytest.approx(0.2)


def test_control_drift_compares_medians():
    # pojedyncze wyskoki (5,0 i 9,0) nie przesuwają median 1,1 i 1,05 — dryf 4,5% < 10%
    assert control_drift([1.0, 5.0, 1.1], [1.05, 1.0, 9.0], repeats=3, tolerance=0.10) is None
    assert control_drift([1.0, 1.0, 1.0], [1.3, 1.3, 1.0], repeats=3, tolerance=0.10) == "control drift 30%"


def test_control_drift_needs_half_of_the_control_runs_valid():
    assert control_drift([1.0], [1.0, 1.0, 1.0], repeats=3, tolerance=0.10) == "control run invalid"
    assert control_drift([1.0, 1.0], [], repeats=3, tolerance=0.10) == "control run invalid"
    assert control_drift([1.0, 1.0], [1.0, 1.0], repeats=4, tolerance=0.10) is None
    assert control_drift([1.0], [1.05], repeats=1, tolerance=0.10) is None
```

Run: `uv run pytest tests/bench/test_validity.py -v`
Expected: FAIL (`ImportError: cannot import name 'control_drift'`).

- [ ] **Step 2: Implementacja w `bench/validity.py`**

Dodaj `import statistics` do importów. Funkcję `block_drifted` zastąp:

```python
def control_drift(start: list[float], end: list[float], *, repeats: int, tolerance: float) -> str | None:
    """Przyczyna unieważnienia bloku po przebiegach kontrolnych albo None (specyfikacja 3b-2, 4.2).

    `start`, `end` — czasy WAŻNYCH przebiegów kontrolnych z początku i z końca bloku. Mniej niż
    połowa ważnych na którymś końcu → „control run invalid”; względna różnica median powyżej
    tolerancji → „control drift N%”. Mediana z kilku przebiegów odcina pojedyncze wyskoki."""
    if not start or not end or 2 * len(start) < repeats or 2 * len(end) < repeats:
        return "control run invalid"
    d = drift(statistics.median(start), statistics.median(end))
    return f"control drift {d:.0%}" if d > tolerance else None
```

Zaktualizuj docstring modułu: „Reguły ważności przebiegu (specyfikacja 6.4–6.5) i dryfu przebiegów kontrolnych (6.3; 3b-2, 4.2).”

Run: `uv run pytest tests/bench/test_validity.py -v`
Expected: PASS.

- [ ] **Step 3: Testy orkiestratora i wyników**

W `tests/bench/test_results.py`, w `full_row` dopisz do `row.update(...)` argument `control_position=None,`.

W `tests/bench/test_orchestrator.py`:
1. w `test_series_runs_reference_then_blocks_with_control_warmup_and_rounds` zastąp linię `assert control["rep"].to_list() == [0, 1, 0, 1]` przez:

```python
    assert control["rep"].to_list() == [1, 1, 1, 1]
    assert control["control_position"].to_list() == ["start", "end", "start", "end"]
    assert df.filter(~pl.col("is_control"))["control_position"].is_null().all()
```

2. dopisz testy (po `test_drift_twice_leaves_block_invalid_without_third_attempt`):

```python
def test_control_medians_ignore_single_outliers(tmp_path):
    factory = _control_times(tmp_path, [1.0, 5.0, 1.1, 1.05, 1.0, 9.0])
    cfg = config(variants=["ballista"], control_repeats=3, control_tolerance=0.1)
    summary = run(tmp_path, cfg=cfg, factory=factory)
    df = rows(summary).filter(~pl.col("is_reference"))
    assert set(df["attempt"]) == {1} and df["valid"].all()
    control = df.filter(pl.col("is_control"))
    assert control["rep"].to_list() == [1, 2, 3, 1, 2, 3]
    assert control["control_position"].to_list() == ["start"] * 3 + ["end"] * 3


def test_control_drift_of_medians_repeats_block(tmp_path):
    factory = _control_times(tmp_path, [1.0, 1.0, 1.0, 1.3, 1.3, 1.0] + [1.0] * 6)
    cfg = config(variants=["ballista"], control_repeats=3, control_tolerance=0.1)
    df = rows(run(tmp_path, cfg=cfg, factory=factory)).filter(~pl.col("is_reference"))
    first = df.filter(pl.col("attempt") == 1)
    assert set(first["invalid_reason"]) == {"control drift 30%"}
    assert df.filter(pl.col("attempt") == 2)["valid"].all()


def test_invalid_control_runs_invalidate_the_block(tmp_path):
    """Wszystkie przebiegi kontrolne nieważne (zła suma kontrolna polars-bio A) — blok dostaje
    przyczynę „control run invalid” zamiast wyjątku z mediany pustej listy, także po powtórzeniu."""
    def args_for(variant, n, scenario):
        return ["--checksum", "0x00000000000000bb"] if variant == "polars_bio_a" else []

    cfg = config(variants=["ballista"], control_repeats=2)
    df = rows(run(tmp_path, cfg=cfg, factory=make_factory(args_for))).filter(~pl.col("is_reference"))
    measured = df.filter(~pl.col("is_control"))
    assert set(measured["attempt"]) == {1, 2}
    assert set(measured["invalid_reason"]) == {"control run invalid"}
```

Run: `uv run pytest tests/bench/test_orchestrator.py tests/bench/test_results.py -v`
Expected: FAIL (brak kolumny `control_position`).

- [ ] **Step 4: Implementacja — wyniki i orkiestrator**

W `bench/results.py`:
1. komentarz przy `"rep"` zmień na `# runda 1..R; 0 — rozgrzewka i wzorzec; przebiegi kontrolne: 1..control_repeats`;
2. pod `"is_reference": pl.Boolean,` dodaj:

```python
    "control_position": pl.Utf8,  # "start" / "end" — przebieg kontrolny na początku / końcu bloku (plan 3b-2)
```

3. w komentarzu nad `SCHEMA` dopisz „; control_position (plan 3b-2)”.

W `bench/orchestrator.py`:
1. import z `bench.validity` zmień na `from bench.validity import Expected, control_drift, invalid_reasons, parse_report`;
2. metodę `_Series.row` zastąp:

```python
    def row(self, scenario: Scenario, variant: str, n_nodes: int, rep: int, attempt: int,
            measured: dict, *, warmup=False, control_position: str | None = None,
            reference=False) -> dict:
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
            "is_control": control_position is not None,
            "is_reference": reference,
            "control_position": control_position,
            **measured,
        }
```

3. w `_progress` gałąź `elif row["is_control"]:` zmień na:

```python
    elif row["is_control"]:
        kind = f"control {row['control_position']} {row['rep']}"
```

i szerokość pola `{kind:<10}` na `{kind:<16}`;
4. metodę `run_block` zastąp:

```python
    def run_block(self, block: Block, attempt: int, expected: dict[str, Expected], rows: list[dict]) -> bool:
        """Blok (specyfikacja 6.2; 3b-2, 4.2): przebiegi kontrolne → rozgrzewka → rundy →
        przebiegi kontrolne. Wiersze trafiają do `rows` na bieżąco (wołający zapisuje je także
        po przerwaniu). True = dryf albo nieważne przebiegi kontrolne."""
        engine = self.factory(block.variant, block.n_nodes)
        control = self.factory(CONTROL_VARIANT, 1)
        skipped: dict[str, str] = {}

        def run(scenario: Scenario, rep: int, *, warmup: bool = False,
                position: str | None = None) -> dict:
            is_control = position is not None
            if is_control:
                eng, variant, n = control, CONTROL_VARIANT, 1
            else:
                eng, variant, n = engine, block.variant, block.n_nodes
            if not is_control and scenario.id in skipped:
                measured = _skipped(f"skipped: {skipped[scenario.id]}")
            else:
                measured, restart = self.measure(eng, scenario, expected[scenario.id])
                if restart:
                    if not is_control:
                        skipped[scenario.id] = measured["invalid_reason"]
                    eng.restart()
            row = self.row(scenario, variant, n, rep, attempt, measured, warmup=warmup,
                           control_position=position)
            rows.append(row)
            self.out(_progress(row))
            return row

        def controls(position: str) -> list[float]:
            """Czasy ważnych przebiegów kontrolnych z jednego końca bloku."""
            times = []
            for rep in range(1, self.cfg.control_repeats + 1):
                row = run(self.cfg.control, rep, position=position)
                if row["valid"]:
                    times.append(row["t_total_s"])
            return times

        engine.start()
        try:
            start = controls("start")
            for w in range(1, self.cfg.warmup + 1):
                for scenario in round_order(self.cfg, block, f"warmup-{w}"):
                    run(scenario, 0, warmup=True)
            for r in range(1, self.cfg.repeats + 1):
                for scenario in round_order(self.cfg, block, str(r)):
                    run(scenario, r)
            end = controls("end")
        finally:
            engine.stop()
        reason = control_drift(
            start, end, repeats=self.cfg.control_repeats, tolerance=self.cfg.control_tolerance
        )
        if reason is None:
            return False
        _invalidate(rows, reason)
        return True
```

5. w docstringu modułu zastąp „przebieg kontrolny, rozgrzewka, rundy w losowej kolejności z ziarna, przebieg kontrolny” przez „przebiegi kontrolne, rozgrzewka, rundy w losowej kolejności z ziarna, przebiegi kontrolne”.

- [ ] **Step 5: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 6: Commit**

```bash
git add bench/validity.py bench/orchestrator.py bench/results.py tests/bench/test_validity.py tests/bench/test_orchestrator.py tests/bench/test_results.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 1): przebiegi kontrolne z mediana control_repeats na kazdym koncu bloku

Dryf liczony z median waznych przebiegow; mniej niz polowa waznych na ktoryms koncu
uniewaznia blok; kolumna control_position.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 6: Swap — pusty na starcie serii, `pswpin` unieważnia przebieg

**Files:**
- Modify: `bench/metrics.py` (`swap_used`)
- Modify: `bench/validity.py` (`invalid_reasons` z `pswpin_delta`)
- Modify: `bench/orchestrator.py` (`measure`, `preflight`)
- Test: `tests/bench/test_metrics.py`, `tests/bench/test_validity.py`, `tests/bench/test_orchestrator.py`, `tests/test_orchestrator_integration.py`

**Interfaces:**
- Produces: `bench.metrics.swap_used(proc: Path = PROC) -> int` (bajty, `SwapTotal − SwapFree`); `invalid_reasons(..., pswpout_delta, pswpin_delta, stderr)` — nowy argument słownikowy `pswpin_delta: int | None`; przyczyna `"swap: pswpin +N"`. `preflight` rzuca `ConfigError` przy niepustym swapie.

- [ ] **Step 1: Testy**

`tests/bench/test_metrics.py`:
1. `MEMINFO` zastąp:

```python
MEMINFO = """MemTotal:        5000000 kB
MemFree:         3000000 kB
MemAvailable:    3900000 kB
Buffers:           10000 kB
SwapTotal:       4194300 kB
SwapFree:        4194300 kB
"""
```

2. dopisz:

```python
def test_swap_used_is_total_minus_free(fake_proc):
    assert metrics.swap_used(fake_proc) == 0
    (fake_proc / "meminfo").write_text(MEMINFO.replace("SwapFree:        4194300", "SwapFree:        4194000"))
    assert metrics.swap_used(fake_proc) == 300 * 1024


def test_real_swap_usage_is_readable():
    assert metrics.swap_used() >= 0
```

`tests/bench/test_validity.py`:
1. w helperze `reasons` dodaj do słownika `args` wpis `pswpin_delta=0,`;
2. dopisz:

```python
def test_swap_in_invalidates_otherwise_good_run():
    assert reasons(pswpin_delta=3) == ["swap: pswpin +3"]
    assert reasons(pswpout_delta=2, pswpin_delta=3) == ["swap: pswpout +2", "swap: pswpin +3"]
```

`tests/bench/test_orchestrator.py`:
1. test `test_swap_in_is_recorded_without_invalidating` zastąp:

```python
def test_swap_in_invalidates_run(tmp_path):
    """Seria startuje od pustego swapu (sprawdzenie przed serią), więc każde wczytanie ze swapu
    w trakcie przebiegu pochodzi z pomiaru — przebieg jest nieważny (specyfikacja 3b-2, 4.3)."""
    marker = tmp_path / "swapin"

    def args_for(variant, n, scenario):
        return ["--touch", str(marker)] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(
        tmp_path, factory=make_factory(args_for),
        probe=probe(pswpin=lambda: 7 if marker.exists() else 0),
    )
    bad = rows(summary).filter(~pl.col("valid"))
    assert bad.height == 1
    assert bad["invalid_reason"][0] == "swap: pswpin +7" and bad["pswpin_delta"][0] == 7
```

2. w `test_main_fails_fast_without_ballista_binaries` dodaj do parametrów `monkeypatch` (jest) i pierwszą linię ciała `monkeypatch.setattr(metrics, "swap_used", lambda: 0)`;
3. dopisz:

```python
def test_main_refuses_to_start_with_used_swap(tmp_path, monkeypatch, capsys):
    """Niepusty swap na starcie: każdy przebieg mógłby wczytywać cudze strony — seria nie startuje
    (kod 2), zanim powstanie katalog wyników."""
    root = _fixture_data(tmp_path / "data")
    cfg = tmp_path / "series.yaml"
    cfg.write_text(yaml.safe_dump({
        "series": "s", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "control": {"op": "overlap", "pair": "1-2"},
        "variants": ["polars_bio_a"], "nodes": [1], "seed": 1,
    }))
    monkeypatch.setattr(metrics, "swap_used", lambda: 300 * 2**20)
    code = orch.main([str(cfg), "--data-root", str(root), "--results-dir", str(tmp_path / "results")])
    assert code == 2
    assert "swap is not empty (300 MiB used)" in capsys.readouterr().err
    assert not (tmp_path / "results").exists()
```

`tests/test_orchestrator_integration.py`:
1. w `series`: `probe=orch.Probe(pswpout=lambda: 0)` → `probe=orch.Probe(pswpout=lambda: 0, pswpin=lambda: 0)` i w docstringu modułu zastąp „Swap jest wyłączony z kryteriów (pswpout zastąpiony stałą)” przez „Swap jest wyłączony z kryteriów (pswpout i pswpin zastąpione stałą)”;
2. w `test_main_resolves_relative_data_root`: `functools.partial(orch.Probe, pswpout=lambda: 0)` → `functools.partial(orch.Probe, pswpout=lambda: 0, pswpin=lambda: 0)` oraz dopisz `monkeypatch.setattr(metrics, "swap_used", lambda: 0)` (z importem `from bench import metrics`).

Run: `uv run pytest tests/bench/test_metrics.py tests/bench/test_validity.py tests/bench/test_orchestrator.py -v`
Expected: FAIL.

- [ ] **Step 2: Implementacja**

`bench/metrics.py` — pod `mem_available` dodaj:

```python
def swap_used(proc: Path = PROC) -> int:
    """Zajęty swap w bajtach: SwapTotal − SwapFree z /proc/meminfo."""
    meminfo = proc / "meminfo"
    return _kib_field(meminfo, "SwapTotal") - _kib_field(meminfo, "SwapFree")
```

i w docstringu modułu po „dostępna pamięć (MemAvailable),” dopisz „zajęty swap,”.

`bench/validity.py` — w `invalid_reasons`: dodaj parametr `pswpin_delta: int | None,` po `pswpout_delta`, w docstringu pozycję listy „przyrost pswpout (specyfikacja 6.5: swap unieważnia czas i pamięć).” zastąp „przyrost pswpout albo pswpin (specyfikacja 6.5; 3b-2, 4.3: seria startuje od pustego swapu, więc każde wczytanie pochodzi z pomiaru).”, a na końcu ciała, po bloku `if pswpout_delta:`, dodaj:

```python
    if pswpin_delta:
        reasons.append(f"swap: pswpin +{pswpin_delta}")
```

`bench/orchestrator.py`:
1. w `measure` usuń komentarz nad `swap_in_delta = ...` (trzy linie „Wczytania ze swapu tylko zapisywane…”) i w wywołaniu `invalid_reasons(...)` dopisz `pswpin_delta=swap_in_delta,`;
2. `preflight` zastąp:

```python
def preflight(cfg: SeriesConfig, profile: str) -> None:
    """Sprawdzenia przed pierwszym pomiarem: rdzenie emulowanych węzłów, pusty swap
    (specyfikacja 3b-2, 4.3) i binarki Ballisty."""
    needed = max(cluster_cpus(max(cfg.nodes))) + 1
    if (os.cpu_count() or 0) < needed:
        raise ConfigError(
            f"not enough CPUs: N = {max(cfg.nodes)} needs {needed} threads, found {os.cpu_count()}"
        )
    used = metrics.swap_used()
    if used > 0:
        raise ConfigError(
            f"swap is not empty ({used // 2**20} MiB used) - a run touching swap is invalid, so "
            "a series starts from empty swap: run 'wsl --shutdown' in Windows and reopen the terminal"
        )
    if "ballista" in cfg.variants:
        require_ballista_binaries(profile)
```

- [ ] **Step 3: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 4: Commit**

```bash
git add bench/metrics.py bench/validity.py bench/orchestrator.py tests/bench/test_metrics.py tests/bench/test_validity.py tests/bench/test_orchestrator.py tests/test_orchestrator_integration.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 1): seria od pustego swapu, pswpin uniewaznia przebieg

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 7: Badanie szumu przebiegu kontrolnego — `bench/control_noise.py` i `control_noise.yaml`

**Files:**
- Create: `bench/control_noise.py`, `bench/conf/control_noise.yaml`
- Test: `tests/bench/test_control_noise.py`

**Interfaces:**
- Consumes: `bench.validity.drift`, `bench.results.SCHEMA` (w teście).
- Produces: `bench.control_noise.false_drift_rate(times: Sequence[float], *, group: int, tolerance: float, samples: int = 10000, seed: int = 0) -> float`; `load_times(directory: Path) -> list[float]` (ważne rundy mierzone wariantu `polars_bio_a`, bez wzorca, rozgrzewki i przebiegów kontrolnych); CLI `uv run python -m bench.control_noise <katalog serii> [--group 10] [--tolerance 0.10]`.

- [ ] **Step 1: Testy (`tests/bench/test_control_noise.py`)**

```python
"""Badanie szumu przebiegu kontrolnego (plan 3b-2, Zadanie 7; specyfikacja 4.2): jak często dwie
mediany po 10 czasów różnią się o ponad próg przy niezmienionej maszynie."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench import control_noise as cn
from bench.config import Scenario, load
from bench.results import SCHEMA, ResultsWriter, to_parquet

REPO = Path(__file__).resolve().parents[2]


def test_identical_times_never_drift():
    assert cn.false_drift_rate([1.5] * 20, group=10, tolerance=0.10, samples=500) == 0.0


def test_bimodal_times_sometimes_drift_and_result_is_reproducible():
    times = [1.0] * 10 + [2.0] * 10
    rate = cn.false_drift_rate(times, group=10, tolerance=0.10, samples=2000, seed=3)
    assert 0.0 < rate < 1.0
    assert rate == cn.false_drift_rate(times, group=10, tolerance=0.10, samples=2000, seed=3)


def test_too_few_times_is_an_error():
    with pytest.raises(ValueError, match="needs at least 20 times"):
        cn.false_drift_rate([1.0] * 19, group=10, tolerance=0.10)


def _row(**values) -> dict:
    row = dict.fromkeys(SCHEMA)
    row.update(
        variant="polars_bio_a", scenario_id="overlap/7-0", is_reference=False, is_warmup=False,
        is_control=False, valid=True, t_total_s=1.0,
    )
    row.update(values)
    return row


def _series_dir(tmp_path: Path, rows: list[dict]) -> Path:
    writer = ResultsWriter(tmp_path / "runs.jsonl")
    for row in rows:
        writer.write(row)
    to_parquet(writer.path, tmp_path / "runs.parquet")
    return tmp_path


def test_load_times_takes_only_valid_measured_rounds(tmp_path):
    directory = _series_dir(tmp_path, [
        _row(t_total_s=1.1), _row(t_total_s=1.2),
        _row(t_total_s=9.0, valid=False), _row(t_total_s=9.0, is_warmup=True),
        _row(t_total_s=9.0, is_control=True), _row(t_total_s=9.0, is_reference=True, variant="polars_bio_ref"),
    ])
    assert cn.load_times(directory) == [1.1, 1.2]


def test_main_reports_spread_and_false_drift_rate(tmp_path, capsys):
    directory = _series_dir(tmp_path, [_row(t_total_s=1.0 + i / 100) for i in range(20)])
    assert cn.main([str(directory)]) == 0
    out = capsys.readouterr().out
    assert "runs: 20" in out and "false drift rate" in out


def test_main_rejects_too_few_runs(tmp_path, capsys):
    directory = _series_dir(tmp_path, [_row() for _ in range(5)])
    assert cn.main([str(directory)]) == 2
    assert "needs at least 20 times" in capsys.readouterr().err


def test_noise_study_config():
    cfg = load(REPO / "bench" / "conf" / "control_noise.yaml")
    assert cfg.series == "control-noise" and cfg.scenarios == (Scenario("overlap", "7-0"),)
    assert (cfg.variants, cfg.nodes, cfg.repeats, cfg.warmup) == (("polars_bio_a",), (1,), 20, 1)
```

Run: `uv run pytest tests/bench/test_control_noise.py -v`
Expected: FAIL (`ModuleNotFoundError: bench.control_noise`).

- [ ] **Step 2: Implementacja `bench/control_noise.py`**

```python
"""Badanie szumu przebiegu kontrolnego (specyfikacja 3b-2, sekcja 4.2).

Seria bench/conf/control_noise.yaml mierzy zadanie kontrolne (overlap 7-0, polars-bio A) 20 razy
pod rząd. Ten moduł liczy z jej wyników, w jakim odsetku losowych podziałów czasów na dwie
rozłączne grupy po `group` (jak przebiegi kontrolne z początku i końca bloku) względna różnica
median przekracza próg — czyli odsetek fałszywych dryfów przy niezmienionej maszynie.
Kryterium specyfikacji: ≤ 5% przy progu 10% i grupach po 10.

Uruchomienie:
    uv run python -m bench.control_noise bench/results/control-noise/<znacznik czasu>"""

from __future__ import annotations

import argparse
import random
import statistics
import sys
from pathlib import Path
from typing import Sequence

import polars as pl

from bench.validity import drift


def false_drift_rate(times: Sequence[float], *, group: int, tolerance: float,
                     samples: int = 10000, seed: int = 0) -> float:
    """Odsetek losowych podziałów (ziarno `seed`) z dryfem median powyżej `tolerance`."""
    times = list(times)
    if len(times) < 2 * group:
        raise ValueError(f"the noise study needs at least {2 * group} times, got {len(times)}")
    rng = random.Random(seed)
    hits = 0
    for _ in range(samples):
        picked = rng.sample(times, 2 * group)
        if drift(statistics.median(picked[:group]), statistics.median(picked[group:])) > tolerance:
            hits += 1
    return hits / samples


def load_times(directory: Path) -> list[float]:
    """Czasy ważnych rund mierzonych polars-bio A z runs.parquet serii (bez wzorca, rozgrzewki
    i przebiegów kontrolnych bloku)."""
    df = pl.read_parquet(Path(directory) / "runs.parquet")
    measured = df.filter(
        pl.col("valid") & ~pl.col("is_reference") & ~pl.col("is_warmup") & ~pl.col("is_control")
        & (pl.col("variant") == "polars_bio_a")
    )
    return measured["t_total_s"].to_list()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bench.control_noise",
        description="False drift rate of the control run from a noise-study series.",
    )
    parser.add_argument("directory", type=Path, help="series results directory (with runs.parquet)")
    parser.add_argument("--group", type=int, default=10, help="control runs per block end (default 10)")
    parser.add_argument("--tolerance", type=float, default=0.10, help="drift threshold (default 0.10)")
    args = parser.parse_args(argv)
    times = load_times(args.directory)
    try:
        rate = false_drift_rate(times, group=args.group, tolerance=args.tolerance)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print(
        f"runs: {len(times)}, median {statistics.median(times):.3f} s, min {min(times):.3f} s, "
        f"max {max(times):.3f} s, spread {max(times) / min(times) - 1:.1%}"
    )
    print(
        f"false drift rate (two groups of {args.group}, tolerance {args.tolerance:.0%}): "
        f"{rate:.1%} (10000 random splits)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Konfiguracja `bench/conf/control_noise.yaml`**

```yaml
# Badanie szumu przebiegu kontrolnego (specyfikacja 3b-2, 4.2): zadanie kontrolne (overlap 7-0,
# polars-bio A) zmierzone 20 razy pod rząd. Wynik ocenia:
#   uv run python -m bench.control_noise bench/results/control-noise/<znacznik czasu>
# Seria pomiarowa — tylko po checkliście RAM (zamknięty VS Code, pusty swap).
series: control-noise
scenarios:
  - {op: overlap, pair: "7-0"}
variants: [polars_bio_a]
nodes: [1]
repeats: 20
warmup: 1
# Przebiegi kontrolne samego bloku mierzą to samo zadanie; jeden na koniec wystarczy.
control_repeats: 1
seed: 20261009
```

- [ ] **Step 4: Uruchom — PASS**

Run: `uv run pytest tests/bench/test_control_noise.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bench/control_noise.py bench/conf/control_noise.yaml tests/bench/test_control_noise.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 1): badanie szumu przebiegu kontrolnego (control_noise)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

# Etap 2 — Sail (spec, sekcja 5)

### Zadanie 8: Pomost — kontekst polars-bio na wątek (`sail_pb_context.py`)

**Files:**
- Create: `sail_pb_context.py` (katalog główny repozytorium, obok `sail_pb_guard.py`)
- Test: `tests/test_sail_pb_context.py`

**Interfaces:**
- Produces: moduł `sail_pb_context` — import instaluje podmianę; `install() -> None` (idempotentne), `uninstall() -> None` (przywraca oryginał), `installed() -> bool`, `thread_context() -> BioSessionContext` (jeden na wątek), `OPTIONS: dict[str, str]`, `_original` (oryginalna `range_operation`), `_range_operation` (podmiana).

- [ ] **Step 1: Testy (`tests/test_sail_pb_context.py`)**

```python
"""Kontekst polars-bio na wątek — rozwiązanie pomostowe (plan 3b-2, Zadanie 8; specyfikacja 5.1).

Podmiana polars_bio.range_op.range_operation (sail_pb_context) ma dawać wyniki identyczne
z publicznym pb.*(), także przy równoczesnych wywołaniach w wątkach bez blokady. Test pilnuje też
wnętrza polars-bio: gdy nowa wersja zmieni sposób wywołania range_operation, wyniki się rozjadą.

Bez podmiany równoczesne wywołania kończą się błędem „The table s1 already exists” (sonda 3b-2) —
tego nie testujemy wprost, bo wyścig na małych danych bywa niewidoczny."""

from __future__ import annotations

import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import polars_bio as pb
import polars_bio.range_op as range_op
import pytest
from polars_bio._metadata import set_coordinate_system

from bench.ops import OPS, UNARY_OPS
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B

REPO = Path(__file__).resolve().parent.parent
COLS = ["chrom", "start", "end"]
SCHEMA = {"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64}


def _zero_based(df: pl.DataFrame) -> pl.DataFrame:
    set_coordinate_system(df, True)
    return df


def _by_chrom(rows) -> dict[str, pl.DataFrame]:
    df = pl.DataFrame(rows, schema=SCHEMA, orient="row")
    return {c: _zero_based(df.filter(pl.col("chrom") == c)) for c in sorted(set(df["chrom"]))}


LEFT, RIGHT = _by_chrom(FIXTURE_A), _by_chrom(FIXTURE_B)


def _call(op: str, chrom: str) -> pl.DataFrame:
    """Jedna operacja polars-bio na jednym chromosomie — jak funkcja Saila na jednej grupie."""
    left = LEFT[chrom]
    right = RIGHT[chrom] if chrom in RIGHT else _zero_based(pl.DataFrame(schema=SCHEMA))
    if op in UNARY_OPS:
        result = pb.merge(left, cols=COLS, output_type="polars.DataFrame")
    else:
        result = getattr(pb, op)(left, right, cols1=COLS, cols2=COLS, output_type="polars.DataFrame")
    return result.sort(result.columns)


@pytest.fixture
def bridge():
    """Moduł pomostu z WYŁĄCZONĄ podmianą — test włącza ją jawnie; po teście przywracany oryginał,
    żeby pozostałe testy w tym procesie liczyły na globalnym kontekście polars-bio."""
    import sail_pb_context

    sail_pb_context.uninstall()  # import instaluje podmianę
    yield sail_pb_context
    sail_pb_context.uninstall()


@pytest.mark.parametrize("op", OPS)
def test_threads_without_lock_match_plain_polars_bio(op, bridge):
    expected = {chrom: _call(op, chrom) for chrom in LEFT}
    bridge.install()
    jobs = list(LEFT) * 6
    for _ in range(3):
        with ThreadPoolExecutor(6) as pool:
            results = list(pool.map(lambda chrom: (chrom, _call(op, chrom)), jobs))
        mismatched = sorted({chrom for chrom, got in results if not got.equals(expected[chrom])})
        assert not mismatched, f"{op}: results differ for {mismatched}"


def test_install_is_idempotent_and_uninstall_restores_polars_bio(bridge):
    original = range_op.range_operation
    bridge.install()
    bridge.install()
    assert bridge.installed() and range_op.range_operation is bridge._range_operation
    bridge.uninstall()
    assert not bridge.installed() and range_op.range_operation is original is bridge._original


def test_each_thread_gets_its_own_context(bridge):
    main = bridge.thread_context()
    assert bridge.thread_context() is main
    other = []
    worker = threading.Thread(target=lambda: other.append(bridge.thread_context()))
    worker.start()
    worker.join()
    assert other[0] is not main


def test_import_installs_the_bridge():
    """Proces serwera Saila importuje moduł po nazwie — sam import ma włączyć podmianę."""
    code = (
        "import polars_bio.range_op as r, sail_pb_context as s; "
        "print(r.range_operation is s._range_operation)"
    )
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    assert r.stdout.strip() == "True"
```

Run: `uv run pytest tests/test_sail_pb_context.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'sail_pb_context'`).

- [ ] **Step 2: Implementacja `sail_pb_context.py`**

```python
"""Osobny kontekst polars-bio dla każdego wątku — rozwiązanie pomostowe (specyfikacja 3b-2, 5.1).

polars-bio 0.36 trzyma jeden globalny kontekst (BioSessionContext) i rejestruje w nim tabele
operacji na ramkach pod stałymi nazwami s1/s2: równoczesne wywołania pb.*() w wątkach jednego
procesu kończą się błędem „The table s1 already exists” (w 0.28 — cichym nadpisaniem tabel).
Sail wykonuje funkcje Pythona w wątkach procesu serwera, więc dotąd chroniła je blokada
(sail_pb_guard) — kosztem liczenia jednego chromosomu naraz.

Wszystkie pięć operacji (overlap, nearest, coverage, merge, subtract) budują RangeOptions
i wołają polars_bio.range_op.range_operation(df1, df2, options, output_type, ctx, …). Moduł
podmienia tę funkcję na wersję, która w miejsce ctx wstawia kontekst bieżącego wątku, tworzony
przy pierwszym użyciu z tymi samymi opcjami co globalny. Publiczne pb.*() zostają bez zmian —
z własnymi ustawieniami domyślnymi — a wątki przestają dzielić tabele (sonda 3b-2: 0 rozbieżności,
5 operacji × 6 wątków × 3 rundy; overlap 7-0 2,4× szybciej na 6 wątkach).

Zależność od wnętrza polars-bio, nie od publicznego API: wersja przypięta w pyproject.toml
(polars-bio==0.36.*), a tests/test_sail_pb_context.py porównuje wyniki z podmianą i bez — zmiana
wnętrza w nowej wersji wyjdzie od razu. Kodu polars-bio nie zmieniamy (decyzja 3b-2).

Import instaluje podmianę: moduł importuje po nazwie funkcja Saila (sail_arrow) w procesie
serwera, jak sail_pb_guard. Działa tylko w procesie, który go zaimportował — runner polars-bio
(wzorzec, warianty A i B) go nie importuje."""

from __future__ import annotations

import threading

import polars_bio.range_op as _range_op
from polars_bio.polars_bio import BioSessionContext

#: Opcje kontekstu wątku: jak w globalnym kontekście polars-bio 0.36 (polars_bio/context.py,
#: Context.__init__) oraz układ 0-based — dane databio-8p są 0-based, półotwarte.
OPTIONS = {
    "datafusion.execution.target_partitions": "1",
    "datafusion.execution.parquet.schema_force_view_types": "true",
    "datafusion.execution.skip_physical_aggregate_schema_check": "true",
    "datafusion.bio.coordinate_system_zero_based": "true",
    "datafusion.bio.coordinate_system_check": "false",
    "bio.interval_join_algorithm": "coitrees",
}

_local = threading.local()
_original = _range_op.range_operation


def thread_context() -> BioSessionContext:
    """Kontekst polars-bio bieżącego wątku (tworzony przy pierwszym użyciu)."""
    ctx = getattr(_local, "ctx", None)
    if ctx is None:
        ctx = BioSessionContext(seed=f"thread-{threading.get_ident()}")
        for key, value in OPTIONS.items():
            ctx.set_option(key, value)
        _local.ctx = ctx
    return ctx


def _range_operation(*args, **kwargs):
    """range_operation polars-bio z kontekstem bieżącego wątku zamiast globalnego (5. argument)."""
    if "ctx" in kwargs:
        kwargs["ctx"] = thread_context()
    else:
        args = (*args[:4], thread_context(), *args[5:])
    return _original(*args, **kwargs)


def install() -> None:
    _range_op.range_operation = _range_operation


def uninstall() -> None:
    _range_op.range_operation = _original


def installed() -> bool:
    return _range_op.range_operation is _range_operation


install()
```

- [ ] **Step 3: Uruchom — PASS**

Run: `uv run pytest tests/test_sail_pb_context.py -v`
Expected: PASS (5 operacji + 3 testy).
Run: `uv run pytest tests/test_code_language.py -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add sail_pb_context.py tests/test_sail_pb_context.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 2): kontekst polars-bio na watek (sail_pb_context) - pomost bez blokady

Podmiana polars_bio.range_op.range_operation wstawia kontekst biezacego watku; kod polars-bio
bez zmian, zgodnosc z pb.*() pilnowana testem przy 6 watkach.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 9: Ścieżka Arrow w Sailu (`sail_arrow.py`) i plik faz

**Files:**
- Create: `sail_arrow.py` (katalog główny repozytorium)
- Test: `tests/test_sail_arrow.py`

**Interfaces:**
- Consumes: `sail_pb_context` (Zadanie 8), `sail_pb_guard.PB_LOCK`, `bench.ops.OUTPUT_COLUMNS`, `UNARY_OPS`, `normalize_polars_bio`, `bench.data.datasets.COLUMNS`.
- Produces: `sail_arrow.MODES = ("context", "lock")`; `ddl_schema(op) -> str`; `arrow_schema(op) -> pa.Schema`; `evaluate(op, left: pa.Table, right: pa.Table | None, mode: str, phase_log: str | None) -> pa.Table`; `build_query(spark, op, left, right=None, cols=COLUMNS, *, mode="context", phase_log=None)` (leniwy DataFrame w schemacie znormalizowanym); `read_phases(path: str | Path | None) -> tuple[float, float, int]` (suma `pb_s`, suma `wait_s`, liczba wywołań; brak/pusty plik → zera). Linia pliku faz: `{"pb_s": float, "wait_s": float}`.

- [ ] **Step 1: Testy (`tests/test_sail_arrow.py`)**

```python
"""Ścieżka Arrow w Sailu (plan 3b-2, Zadanie 9; specyfikacja 5.2): groupBy/cogroup(...).applyInArrow
z polars-bio w trybie „kontekst na wątek” i „blokada” — wynik zgodny z polars-bio, plik faz."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

import sail_arrow
from bench import checksum as cs
from bench.ops import OPS, UNARY_OPS
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_server import sail_env

REPO = Path(__file__).resolve().parent.parent
#: Chromosomy lewej strony zbioru testowego (tests/parquet_fixture.py): chr1, chr2, chr10, chrA —
#: tylu wywołań polars-bio oczekujemy (chrB jest tylko po prawej, więc grupa ma pustą lewą stronę).
LEFT_CHROMOSOMES = 4


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    port = free_port()
    log = tmp_path_factory.mktemp("sail_arrow") / "server.log"
    with log.open("w") as f:
        proc = subprocess.Popen(
            [sys.executable, "-m", "bench.runners.sail_server", "--port", str(port)],
            cwd=REPO, env={**os.environ, **sail_env(2)}, stdout=f, stderr=subprocess.STDOUT,
        )
    session = None
    try:
        wait_for_port(port, proc, name="sail_server", log=log)
        from pyspark.sql import SparkSession

        session = SparkSession.builder.remote(f"sc://127.0.0.1:{port}").create()
        yield session
    finally:
        if session is not None:
            session.stop()
        proc.terminate()
        proc.wait(timeout=30)


@pytest.mark.parametrize("mode", sail_arrow.MODES)
@pytest.mark.parametrize("op", OPS)
def test_arrow_path_matches_polars_bio(op, mode, spark, parquet_dirs, parquet_expected, tmp_path):
    left, right = parquet_dirs
    phases = tmp_path / "phases.jsonl"
    df = sail_arrow.build_query(
        spark, op, left, None if op in UNARY_OPS else right, mode=mode, phase_log=str(phases)
    )
    checksum = cs.Checksum(op)
    checksum.update(df.toArrow())
    expected = parquet_expected[op]
    assert checksum.rows == sum(expected.values())
    assert checksum.hex() == cs.format_checksum(cs.checksum_rows(op, expected.elements()))
    pb_s, wait_s, calls = sail_arrow.read_phases(phases)
    assert calls == LEFT_CHROMOSOMES and pb_s > 0
    assert wait_s == 0.0 if mode == "context" else wait_s >= 0.0


def test_empty_left_group_returns_empty_table_without_calling_polars_bio(tmp_path):
    empty = pa.table({"chrom": pa.array([], pa.string()), "start": pa.array([], pa.int64()),
                      "end": pa.array([], pa.int64())})
    phases = tmp_path / "phases.jsonl"
    for mode in sail_arrow.MODES:
        result = sail_arrow.evaluate("overlap", empty, empty, mode, str(phases))
        assert result.num_rows == 0 and result.schema == sail_arrow.arrow_schema("overlap")
    assert not phases.exists()


def test_read_phases_sums_records_and_tolerates_missing_or_empty_file(tmp_path):
    assert sail_arrow.read_phases(None) == (0.0, 0.0, 0)
    assert sail_arrow.read_phases(tmp_path / "missing.jsonl") == (0.0, 0.0, 0)
    path = tmp_path / "phases.jsonl"
    path.write_text("")
    assert sail_arrow.read_phases(path) == (0.0, 0.0, 0)
    path.write_text(json.dumps({"pb_s": 0.5, "wait_s": 0.25}) + "\n" + json.dumps({"pb_s": 1.0, "wait_s": 0.0}) + "\n")
    assert sail_arrow.read_phases(str(path)) == (1.5, 0.25, 2)


def test_schemas_follow_normalized_output_columns():
    assert sail_arrow.ddl_schema("merge") == "chrom string, start long, end long, n_intervals long"
    assert sail_arrow.arrow_schema("subtract") == pa.schema(
        [("chrom", pa.string()), ("start", pa.int64()), ("end", pa.int64())]
    )


@pytest.mark.parametrize(
    "op, right, mode, message",
    [
        ("join", "b", "context", "unknown operation"),
        ("overlap", "b", "fast", "unknown mode"),
        ("merge", "b", "context", "merge requires one table"),
        ("overlap", None, "context", "overlap requires two tables"),
    ],
)
def test_build_query_rejects_bad_arguments(op, right, mode, message):
    with pytest.raises(ValueError, match=message):
        sail_arrow.build_query(None, op, "a", right, mode=mode)
```

Run: `uv run pytest tests/test_sail_arrow.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'sail_arrow'`).

- [ ] **Step 2: Implementacja `sail_arrow.py`**

```python
"""Operacje polars-bio w Sailu przez applyInArrow — ścieżka pomiarowa (specyfikacja 3b-2, 5.2).

Zamiast UDTF z LATERAL (sail_bio.py) dane są grupowane po chromosomie: groupBy (merge) albo
cogroup dwóch tabel (pozostałe operacje), a funkcja Pythona dostaje tabelę Arrow jednego
chromosomu i zwraca tabelę Arrow wyniku. Znikają collect_list, LATERAL i explode, a wynik
przechodzi przez Arrow bez zamiany na wiersze Pythona — sonda 3b-2: 10–20× szybciej niż UDTF
na parze 1-2, wynik zgodny z polars-bio dla pięciu operacji.

Tryby dostępu do polars-bio w procesie serwera Saila:
- "context" — osobny kontekst polars-bio na wątek (sail_pb_context), bez blokady: chromosomy
  liczą się równolegle; wariant główny `sail`;
- "lock" — globalny kontekst i blokada sail_pb_guard.PB_LOCK (jeden chromosom naraz); wariant
  porównawczy `sail_lock`.

Chromosom tylko po prawej stronie (pusta lewa) daje pusty wynik bez wołania polars-bio; tylko po
lewej — polars-bio dostaje pustą prawą stronę i sam rozstrzyga semantykę (jak w sail_bio.py).

Fazy: gdy podano plik faz, każde wywołanie polars-bio dopisuje do niego linię JSON
{"pb_s": …, "wait_s": …} — czas wywołania i czekania na blokadę; runner sumuje je po zapytaniu
(read_phases). Plik leży na tej samej maszynie co serwer (lokalny klaster); dopisywanie krótkich
linii w trybie append jest bezpieczne między wątkami.

Moduł musi być importowalny po nazwie w procesie serwera (katalog roboczy = repozytorium):
cloudpickle serializuje funkcję przekazaną do applyInArrow, a ta importuje sail_arrow."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pyarrow as pa

from bench.data.datasets import COLUMNS
from bench.ops import OUTPUT_COLUMNS, UNARY_OPS, normalize_polars_bio

MODES = ("context", "lock")
#: Nazwy kolumn przedziału, pod którymi dane trafiają do polars-bio.
_PB_COLS = ("chrom", "start", "end")


def ddl_schema(op: str) -> str:
    """Schemat wyniku dla applyInArrow (DDL Sparka) w schemacie znormalizowanym."""
    return ", ".join(f"{c} {'string' if c.startswith('chrom') else 'long'}" for c in OUTPUT_COLUMNS[op])


def arrow_schema(op: str) -> pa.Schema:
    return pa.schema([(c, pa.string() if c.startswith("chrom") else pa.int64()) for c in OUTPUT_COLUMNS[op]])


def _frame(table: pa.Table):
    """Tabela Arrow grupy → polars (bez kopii) z metadanymi układu 0-based dla polars-bio."""
    import polars as pl
    from polars_bio._metadata import set_coordinate_system

    df = pl.from_arrow(table).select(
        pl.col("chrom").cast(pl.Utf8), pl.col("start").cast(pl.Int64), pl.col("end").cast(pl.Int64)
    )
    set_coordinate_system(df, True)
    return df


def _call_polars_bio(op: str, left, right):
    import polars_bio as pb

    cols = list(_PB_COLS)
    if op in UNARY_OPS:
        return pb.merge(left, cols=cols, output_type="polars.DataFrame")
    return getattr(pb, op)(left, right, cols1=cols, cols2=cols, output_type="polars.DataFrame")


def evaluate(op: str, left: pa.Table, right: pa.Table | None, mode: str, phase_log: str | None) -> pa.Table:
    """Treść funkcji applyInArrow: jedna operacja polars-bio na jednym chromosomie."""
    schema = arrow_schema(op)
    if left.num_rows == 0:
        return schema.empty_table()  # każda z operacji zwraca wiersze wynikające z lewej tabeli
    left_df = _frame(left)
    right_df = None if right is None else _frame(right)
    wait_s = 0.0
    if mode == "context":
        import sail_pb_context  # noqa: F401 — import instaluje kontekst na wątek

        t = time.perf_counter()
        result = _call_polars_bio(op, left_df, right_df)
        pb_s = time.perf_counter() - t
    elif mode == "lock":
        from sail_pb_guard import PB_LOCK

        t = time.perf_counter()
        with PB_LOCK:
            wait_s = time.perf_counter() - t
            t = time.perf_counter()
            result = _call_polars_bio(op, left_df, right_df)
            pb_s = time.perf_counter() - t
    else:
        raise ValueError(f"unknown mode {mode!r}; allowed: {', '.join(MODES)}")
    if phase_log:
        with open(phase_log, "a", encoding="utf-8") as f:
            f.write(json.dumps({"pb_s": pb_s, "wait_s": wait_s}) + "\n")
    if result.height == 0:
        return schema.empty_table()
    return normalize_polars_bio(op, result, _PB_COLS).to_arrow().cast(schema)


def build_query(spark, op: str, left, right=None, cols: tuple[str, str, str] = COLUMNS, *,
                mode: str = "context", phase_log: str | None = None):
    """Zapytanie (leniwy DataFrame) na plikach Parquet (plik albo katalog) z wynikiem w schemacie
    znormalizowanym (bench/ops.py)."""
    if op not in OUTPUT_COLUMNS:
        raise ValueError(f"unknown operation {op!r}")
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; allowed: {', '.join(MODES)}")
    if (op in UNARY_OPS) != (right is None):
        raise ValueError(f"{op} requires {'one table' if op in UNARY_OPS else 'two tables'}")
    from pyspark.sql import functions as F

    c, s, e = cols

    def load(path):
        return spark.read.parquet(str(path)).select(
            F.col(c).cast("string").alias("chrom"),
            F.col(s).cast("long").alias("start"),
            F.col(e).cast("long").alias("end"),
        )

    schema = ddl_schema(op)
    if op in UNARY_OPS:
        def unary(table):
            import sail_arrow

            return sail_arrow.evaluate(op, table, None, mode, phase_log)

        return load(left).groupBy("chrom").applyInArrow(unary, schema)

    def binary(left_table, right_table):
        import sail_arrow

        return sail_arrow.evaluate(op, left_table, right_table, mode, phase_log)

    return load(left).groupBy("chrom").cogroup(load(right).groupBy("chrom")).applyInArrow(binary, schema)


def read_phases(path: str | Path | None) -> tuple[float, float, int]:
    """Suma czasów polars-bio, suma czasów czekania na blokadę i liczba wywołań z pliku faz.
    Brak pliku albo pusty plik (żadna grupa nie wywołała polars-bio) → zera."""
    if path is None:
        return 0.0, 0.0, 0
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return 0.0, 0.0, 0
    records = [json.loads(line) for line in lines if line.strip()]
    return sum(r["pb_s"] for r in records), sum(r["wait_s"] for r in records), len(records)
```

- [ ] **Step 3: Uruchom — PASS**

Run: `uv run pytest tests/test_sail_arrow.py -v`
Expected: PASS (10 przypadków zgodności + testy jednostkowe). Jeśli któryś przypadek zgodności FAIL na sumie kontrolnej — **zatrzymaj się** (zasada zatrzymania: ścieżka Arrow daje błędny wynik) i opisz rozbieżność użytkownikowi.

- [ ] **Step 4: Commit**

```bash
git add sail_arrow.py tests/test_sail_arrow.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 2): sciezka Arrow w Sailu (groupBy/cogroup applyInArrow) i plik faz

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 10: Runner Saila — wybór ścieżki (`--path`) i fazy z pliku

**Files:**
- Modify: `bench/runners/sail_runner.py`
- Test: `tests/test_sail_runner.py`

**Interfaces:**
- Consumes: `sail_arrow.build_query`, `sail_arrow.read_phases` (Zadanie 9), `warm_up` (Zadanie 2).
- Produces: `bench.runners.sail_runner.PATHS = {"arrow": "context", "arrow-lock": "lock", "udtf": None}`; argument `--path {arrow,arrow-lock,udtf}` (domyślnie `arrow`); `run(op, left, right, cols, remote, path="arrow") -> str`. Linia JSON: `arrow` — `phases {"first_batch_s", "pb_s"}`, `extra {"checksum_s", "udf_groups"}`; `arrow-lock` — dodatkowo `phases.lock_wait_s`; `udtf` — `phases {"first_batch_s"}`, `extra {"checksum_s"}`.

- [ ] **Step 1: Testy (`tests/test_sail_runner.py`)**

`test_runner_matches_polars_bio` zastąp:

```python
#: Fazy i dodatki linii JSON według ścieżki runnera (specyfikacja 3b-2, 5.5).
PATH_KEYS = {
    "arrow": ({"first_batch_s", "pb_s"}, {"checksum_s", "udf_groups"}),
    "arrow-lock": ({"first_batch_s", "pb_s", "lock_wait_s"}, {"checksum_s", "udf_groups"}),
    "udtf": ({"first_batch_s"}, {"checksum_s"}),
}


@pytest.mark.parametrize("path", list(PATH_KEYS))
@pytest.mark.parametrize("op", OPS)
def test_runner_matches_polars_bio(op, path, server, parquet_dirs, parquet_expected):
    got = report(run_runner([*scenario_args(op, *parquet_dirs), "--remote", server.url, "--path", path]))
    assert set(got) == PROTOCOL_KEYS
    expected = parquet_expected[op]
    assert got["rows"] == sum(expected.values())
    assert got["checksum"] == cs.format_checksum(cs.checksum_rows(op, expected.elements()))
    phases, extra = PATH_KEYS[path]
    assert set(got["phases"]) == phases and set(got["extra"]) == extra
    assert got["t_total_s"] > 0 and got["extra"]["checksum_s"] > 0
    assert 0 < got["phases"]["first_batch_s"] <= got["t_total_s"]
    if path != "udtf":
        # chr1, chr2, chr10, chrA — chromosomy z niepustą lewą stroną
        assert got["extra"]["udf_groups"] == 4 and got["phases"]["pb_s"] > 0
    assert got["peak_rss_bytes"] > 10 * 2**20
```

Do listy parametrów `test_runner_rejects_bad_arguments` dopisz:

```python
        ["--op", "merge", "--left", "x", "--remote", "sc://127.0.0.1:1", "--path", "fast"],
```

Run: `uv run pytest tests/test_sail_runner.py -v`
Expected: FAIL (`unrecognized arguments: --path`).

- [ ] **Step 2: Implementacja w `bench/runners/sail_runner.py`**

1. Na początku docstringu modułu, po pierwszym zdaniu, dopisz akapit:

```
Ścieżka (`--path`, specyfikacja 3b-2, 5.3): `arrow` — applyInArrow z kontekstem polars-bio na
wątek (sail_arrow, wariant `sail`), `arrow-lock` — applyInArrow z blokadą (`sail_lock`), `udtf` —
UDTF z LATERAL (sail_bio, `sail_udtf`). Ścieżki Arrow zapisują fazy z wnętrza funkcji (czas
wywołań polars-bio, czas czekania na blokadę) do pliku tymczasowego, który runner sumuje po
zapytaniu: `phases.pb_s`, `phases.lock_wait_s` (tylko arrow-lock), `extra.udf_groups`.
```

2. Importy uzupełnij o `import os` i `import tempfile`.
3. Pod `PROG = "sail_runner"` dodaj:

```python
#: Ścieżka runnera → tryb sail_arrow (None: UDTF z sail_bio).
PATHS = {"arrow": "context", "arrow-lock": "lock", "udtf": None}
```

4. Funkcję `run` zastąp:

```python
def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], remote: str,
        path: str = "arrow") -> str:
    require_paths(left, right)
    from pyspark.sql import SparkSession

    import sail_arrow
    import sail_bio

    mode = PATHS[path]
    phase_log = None
    if mode is not None:
        fd, phase_log = tempfile.mkstemp(prefix="sail_phases_", suffix=".jsonl")
        os.close(fd)
    try:
        spark = SparkSession.builder.remote(remote).create()
        try:
            if mode is None:
                sail_bio.register(spark, op)
            warm_up(spark)
            reset_peak_rss()
            t0 = time.perf_counter()
            if mode is None:
                df = sail_bio.build_query(spark, op, left, right, cols)
            else:
                df = sail_arrow.build_query(spark, op, left, right, cols, mode=mode, phase_log=phase_log)
            tables = []
            first_batch_s = None
            for table in iter_arrow(spark, df):
                tables.append(table)
                if first_batch_s is None:
                    first_batch_s = time.perf_counter() - t0
            t_total_s = time.perf_counter() - t0
        finally:
            spark.stop()
        t = time.perf_counter()
        checksum = Checksum(op)
        checksum.update_all(tables)
        checksum_s = time.perf_counter() - t
        phases = {"first_batch_s": t_total_s if first_batch_s is None else first_batch_s}
        extra = {"checksum_s": checksum_s}
        if mode is not None:
            pb_s, wait_s, calls = sail_arrow.read_phases(phase_log)
            phases["pb_s"] = pb_s
            if mode == "lock":
                phases["lock_wait_s"] = wait_s
            extra["udf_groups"] = calls
    finally:
        if phase_log is not None:
            os.unlink(phase_log)
    return report_line(
        rows=checksum.rows, checksum=checksum.hex(), t_total_s=t_total_s, peak_rss_bytes=peak_rss(),
        phases=phases, extra=extra,
    )
```

5. W `main` dopisz argument i przekaż go:

```python
    parser.add_argument(
        "--path", choices=list(PATHS), default="arrow",
        help="arrow (applyInArrow, per-thread polars-bio context), arrow-lock (applyInArrow with "
             "a lock) or udtf (LATERAL UDTF)",
    )
    args = parse_scenario(parser, argv)
    return main_guard(PROG, lambda: run(args.op, args.left, args.right, args.cols, args.remote, args.path))
```

- [ ] **Step 3: Uruchom — PASS**

Run: `uv run pytest tests/test_sail_runner.py -v`
Expected: PASS (15 przypadków zgodności, pusty wynik na ścieżce `arrow`, workery, błędne argumenty).

- [ ] **Step 4: Ostrzeżenia PySparka 4.2 przy pandas 3 na ścieżce Arrow (spec 5.7)**

Plan 3b-1 odnotował `FutureWarning` („pandas ≥ 3 nie jest jeszcze w pełni wspierany”) i `Pandas4Warning` w PySparku 4.2. Sprawdź, czy pojawiają się na ścieżce Arrow (sonda 3b-2 pokazała wywołania `require_minimum_pandas_version()` przy `applyInArrow`):

```bash
cd /home/milosz/praca_magisterska && uv run python - <<'PY'
import os, subprocess, sys, tempfile
from pathlib import Path
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_server import sail_env
from tests.parquet_fixture import FIXTURE_A, FIXTURE_B, write_parts
tmp = Path(tempfile.mkdtemp()); a = write_parts(FIXTURE_A, tmp / "a"); b = write_parts(FIXTURE_B, tmp / "b")
port = free_port(); log = tmp / "server.log"
proc = subprocess.Popen([sys.executable, "-m", "bench.runners.sail_server", "--port", str(port)],
                        env={**os.environ, **sail_env(1)}, stdout=log.open("w"), stderr=subprocess.STDOUT)
try:
    wait_for_port(port, proc, name="sail_server", log=log)
    for path in ("arrow", "udtf"):
        r = subprocess.run([sys.executable, "-W", "always", "-m", "bench.runners.sail_runner", "--op", "overlap",
                            "--left", str(a), "--right", str(b), "--remote", f"sc://127.0.0.1:{port}", "--path", path],
                           capture_output=True, text=True, timeout=300)
        warnings = sorted({l.strip() for l in r.stderr.splitlines() if "Warning" in l})
        print(path, r.returncode, warnings)
finally:
    proc.terminate(); proc.wait(30)
print("server", sorted({l.strip() for l in log.read_text().splitlines() if "Warning" in l}))
PY
```

Zanotuj wynik (które ostrzeżenia, w kliencie czy w serwerze, na której ścieżce) — trafi do `wnioski_claude.md` w Zadaniu 28. Ostrzeżenia nie zmieniają wyniku (testy zgodności z Step 3 przechodzą); niczego nie wyciszaj.

- [ ] **Step 5: Commit**

```bash
git add bench/runners/sail_runner.py tests/test_sail_runner.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 2): runner Saila z wyborem sciezki (arrow, arrow-lock, udtf) i fazami

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 11: Warianty `sail_lock` / `sail_udtf`, liczniki z logów (`logscan`), workery Saila, `sail_paths.yaml`

**Files:**
- Create: `bench/logscan.py`, `bench/conf/sail_paths.yaml`
- Modify: `bench/config.py` (`VARIANTS`), `bench/engines.py` (`SailEngine(path)`, `make_engine`, metody `log_mark`/`log_metrics` we wszystkich silnikach), `bench/orchestrator.py` (`measure`)
- Test: `tests/bench/test_logscan.py` (nowy), `tests/bench/test_engines.py`, `tests/bench/test_config.py`, `tests/bench/test_orchestrator.py`, `tests/test_orchestrator_integration.py`, `tests/test_sail_runner.py`

**Interfaces:**
- Consumes: `sail_runner --path` (Zadanie 10).
- Produces:
  - `bench.logscan`: `WORKER_READY`, `BROADCAST_DECODED` (wzorce), `mark(paths) -> dict[Path, int]`, `text_since(mark: dict[Path, int]) -> str`, `sail_workers(text) -> int`, `broadcast_bytes(text) -> int`;
  - `bench.config.VARIANTS = ("polars_bio_a", "polars_bio_b", "ballista", "sail", "sail_lock", "sail_udtf")`;
  - interfejs silnika: `log_mark() -> dict[Path, int]`, `log_metrics(mark) -> dict` (Sail: `{"sail_workers": int}`; polars-bio i — do Zadania 15 — Ballista: `{}`);
  - `SailEngine(n_nodes, *, data_root, log_dir, path="arrow")`; `bench.engines.SAIL_PATHS = {"sail": "arrow", "sail_lock": "arrow-lock", "sail_udtf": "udtf"}`;
  - orchestrator: klucz `broadcast_bytes` z `log_metrics` trafia do kolumny `broadcast_bytes`, pozostałe klucze — do JSON `extra`.

- [ ] **Step 1: Testy `logscan` (`tests/bench/test_logscan.py`)**

```python
"""Liczniki z logów procesów silników z czasu jednego przebiegu (plan 3b-2, Zadanie 11)."""

from __future__ import annotations

from bench import logscan

SAIL_LOG = """INFO creating session abc
INFO worker 1 server is ready
INFO worker 2 server is ready
INFO worker 1 server is ready
"""


def test_sail_workers_counts_distinct_ids():
    assert logscan.sail_workers(SAIL_LOG) == 2
    assert logscan.sail_workers("") == 0


def test_broadcast_bytes_sums_decoded_payloads():
    text = (
        "2026-10-09 INFO ballista_genomics::bio_phys_codec: broadcast payload decoded: 1000 bytes\n"
        "noise\n"
        "2026-10-09 INFO ballista_genomics::bio_phys_codec: broadcast payload decoded: 24 bytes\n"
    )
    assert logscan.broadcast_bytes(text) == 1024
    assert logscan.broadcast_bytes("") == 0


def test_text_since_reads_only_new_lines(tmp_path):
    log = tmp_path / "engine.log"
    log.write_text("before\n")
    missing = tmp_path / "later.log"
    mark = logscan.mark([log, missing])
    assert mark == {log: 7, missing: 0}
    with log.open("a") as f:
        f.write("after\n")
    missing.write_text("created\n")
    assert logscan.text_since(mark) == "after\ncreated\n"


def test_text_since_rereads_a_file_shorter_than_the_mark(tmp_path):
    """Plik zastąpiony albo skrócony (np. po restarcie silnika) — czytamy od początku."""
    log = tmp_path / "engine.log"
    log.write_text("a long line written before the mark\n")
    mark = logscan.mark([log])
    log.write_text("new\n")
    assert logscan.text_since(mark) == "new\n"
```

Run: `uv run pytest tests/bench/test_logscan.py -v`
Expected: FAIL (`ImportError`).

- [ ] **Step 2: Implementacja `bench/logscan.py`**

```python
"""Liczniki z logów procesów silników z czasu jednego przebiegu (specyfikacja 3b-2, 5.6 i 6.3).

Orkiestrator zapamiętuje rozmiary logów przed przebiegiem (`mark`) i po przebiegu czyta tylko
dopisany tekst (`text_since`): liczbę workerów sesji Saila i rozmiar ładunków broadcastu
odebranych przez executory Ballisty."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

#: Start workera sesji w logu serwera Saila 0.7.2 (tests/test_sail_runner.py).
WORKER_READY = re.compile(r"worker (\d+) server is ready")
#: Linia executora Ballisty przy dekodowaniu węzła z broadcastem (bio_phys_codec.rs, plan 3b-2).
BROADCAST_DECODED = re.compile(r"broadcast payload decoded: (\d+) bytes")


def mark(paths: Iterable[Path]) -> dict[Path, int]:
    """Bieżące rozmiary plików (brak pliku → 0)."""
    return {Path(p): (Path(p).stat().st_size if Path(p).exists() else 0) for p in paths}


def text_since(mark: dict[Path, int]) -> str:
    """Tekst dopisany do plików od `mark`; plik krótszy niż zapamiętany offset (zastąpiony albo
    skrócony) jest czytany od początku."""
    parts = []
    for path, offset in mark.items():
        if not path.exists():
            continue
        if path.stat().st_size < offset:
            offset = 0
        with path.open("rb") as f:
            f.seek(offset)
            parts.append(f.read().decode("utf-8", errors="replace"))
    return "".join(parts)


def sail_workers(text: str) -> int:
    return len(set(WORKER_READY.findall(text)))


def broadcast_bytes(text: str) -> int:
    return sum(int(n) for n in BROADCAST_DECODED.findall(text))
```

Run: `uv run pytest tests/bench/test_logscan.py -v`
Expected: PASS.

- [ ] **Step 3: Testy wariantów, silników i orkiestratora**

`tests/bench/test_config.py`:
1. w `test_blocks_follow_matrix_columns` zastąp wywołanie i oczekiwanie:

```python
    cfg = parse(raw(
        variants=["sail_udtf", "sail", "polars_bio_b", "ballista", "sail_lock", "polars_bio_a"], nodes=[3, 1],
    ))
    assert blocks(cfg) == [
        Block("polars_bio_a", 1), Block("polars_bio_b", 3), Block("ballista", 1),
        Block("ballista", 3), Block("sail", 1), Block("sail", 3), Block("sail_lock", 1),
        Block("sail_lock", 3), Block("sail_udtf", 1), Block("sail_udtf", 3),
    ]
```

2. dopisz:

```python
def test_sail_paths_config_compares_the_three_sail_paths():
    cfg = load(REPO / "bench" / "conf" / "sail_paths.yaml")
    assert cfg.series == "sail-paths" and cfg.variants == ("sail", "sail_lock", "sail_udtf")
    assert {s.id for s in cfg.scenarios} == {
        "overlap/1-2", "nearest/1-2", "coverage/1-2", "merge/1", "subtract/1-2", "overlap/7-0",
    }
    assert (cfg.nodes, cfg.repeats, cfg.warmup, len(blocks(cfg))) == ((1, 3), 3, 1, 6)
```

`tests/bench/test_engines.py` — dopisz:

```python
@pytest.mark.parametrize("variant, path", [("sail", "arrow"), ("sail_lock", "arrow-lock"), ("sail_udtf", "udtf")])
def test_sail_variants_choose_runner_path(variant, path, tmp_path):
    engine = engines.make_engine(variant, 2, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    argv = engine.command(Scenario("overlap", "1-2")).argv
    assert argv[argv.index("--path") + 1] == path


def test_sail_engine_counts_session_workers_from_its_log(tmp_path):
    engine = engines.make_engine("sail", 2, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    log = tmp_path / "logs" / "sail_server.log"
    log.parent.mkdir()
    log.write_text("worker 1 server is ready\nworker 2 server is ready\nworker 3 server is ready\n")
    mark = engine.log_mark()
    with log.open("a") as f:
        f.write("creating session\nworker 1 server is ready\nworker 2 server is ready\n")
    assert engine.log_metrics(mark) == {"sail_workers": 2}


def test_polars_bio_engine_has_no_log_metrics(tmp_path):
    engine = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert engine.log_metrics(engine.log_mark()) == {}
```

`tests/bench/test_orchestrator.py`:
1. w `FakeEngine.__init__` dopisz `self.logged: dict = {}`, a do klasy metody:

```python
    def log_mark(self):
        return {}

    def log_metrics(self, mark):
        return dict(self.logged)
```

2. dopisz test:

```python
def test_engine_log_metrics_reach_results(tmp_path):
    """Liczniki z logów silnika: broadcast_bytes → kolumna, pozostałe (np. sail_workers) → extra."""
    def customize(engine):
        if engine.variant == "ballista":
            engine.logged = {"broadcast_bytes": 10, "sail_workers": 2}

    df = rows(run(tmp_path, factory=make_factory(customize=customize)))
    ballista = block(df, "ballista")
    assert set(ballista["broadcast_bytes"]) == {10}
    assert all(json.loads(e)["sail_workers"] == 2 for e in ballista["extra"])
    assert block(df, "polars_bio_a")["broadcast_bytes"].is_null().all()
```

`tests/test_orchestrator_integration.py`:
1. w `test_every_planned_run_is_recorded` komentarz i asercję zastąp:

```python
    # wzorzec: 5 scenariuszy (kontrola overlap/1-2 jest wśród nich); bloki: A1, B2, Ballista 1 i 2,
    # Sail, Sail z blokadą i Sail UDTF po 1 i 2 — każdy 2 przebiegi kontrolne + 5 scenariuszy
    assert df.height == 5 + 10 * (2 + 5)
```

2. w `test_threads_follow_variant_and_n`: `df.filter(pl.col("variant") != "sail")` → `df.filter(~pl.col("variant").str.starts_with("sail"))`;
3. dopisz:

```python
@pytest.mark.nodes(2)
def test_sail_reports_workers_and_phases(series):
    """Każda sesja Saila ma N workerów (Sail 0.7.2); ścieżki Arrow mierzą czas polars-bio
    (4 chromosomy z niepustą lewą stroną), tylko sail_lock — czekanie na blokadę."""
    _, df = series
    sail = df.filter(pl.col("variant").str.starts_with("sail") & ~pl.col("is_control"))
    assert set(sail["variant"]) == {"sail", "sail_lock", "sail_udtf"}
    for row in sail.iter_rows(named=True):
        extra, phases = json.loads(row["extra"]), json.loads(row["phases"])
        assert extra["sail_workers"] == row["n_nodes"], row
        if row["variant"] == "sail_udtf":
            assert set(phases) == {"first_batch_s"}
        else:
            assert phases["pb_s"] > 0 and extra["udf_groups"] == 4
        assert ("lock_wait_s" in phases) == (row["variant"] == "sail_lock")
```

`tests/test_sail_runner.py` — w `test_server_keeps_exactly_n_workers` zastąp linię `workers = set(re.findall(r"worker (\d+) server is ready", session))` i asercję przez (ten sam wzorzec co orkiestrator):

```python
    assert logscan.sail_workers(session) == 2
```

z importem `from bench import logscan` (i usuń nieużywany `import re`, jeśli nic innego go nie używa).

Run: `uv run pytest tests/bench -v`
Expected: FAIL (brak wariantów, `log_mark`).

- [ ] **Step 4: Implementacja**

`bench/config.py`: `VARIANTS = ("polars_bio_a", "polars_bio_b", "ballista", "sail", "sail_lock", "sail_udtf")` z komentarzem nad:

```python
#: Warianty (specyfikacja 3; 3b-2, 5.3): sail — applyInArrow z kontekstem polars-bio na wątek
#: (główny), sail_lock — applyInArrow z blokadą, sail_udtf — UDTF z LATERAL (tylko porównanie).
```

W docstringu `blocks` „Ballista i Sail dla każdego N” → „Ballista i warianty Saila dla każdego N”.

`bench/engines.py`:
1. import: `from bench import logscan`;
2. pod `BALLISTA_START_ATTEMPTS` dodaj:

```python
#: Wariant Saila → ścieżka runnera (bench/runners/sail_runner.py, specyfikacja 3b-2, 5.3).
SAIL_PATHS = {"sail": "arrow", "sail_lock": "arrow-lock", "sail_udtf": "udtf"}
```

3. w docstringu modułu, w opisie interfejsu silnika, dopisz: „log_mark() i log_metrics(mark) → liczniki z logów procesów silnika z czasu przebiegu (specyfikacja 3b-2: workery Saila, broadcast Ballisty)”;
4. `PolarsBioEngine` i `BallistaEngine` — dodaj metody (Ballista dostanie liczniki w Zadaniu 15):

```python
    def log_mark(self) -> dict[Path, int]:
        return {}

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        return {}
```

5. `SailEngine.__init__` zastąp:

```python
    def __init__(self, n_nodes: int, *, data_root: Path, log_dir: Path, path: str = "arrow"):
        self.n, self.data_root, self.log_dir, self.path = n_nodes, data_root, log_dir, path
        self.proc: subprocess.Popen | None = None
        self.port: int | None = None
```

6. w `SailEngine.command` listę argumentów zmień na `[*scenario_args(scenario, self.data_root), "--remote", f"sc://127.0.0.1:{self.port}", "--path", self.path]`;
7. do `SailEngine` dodaj:

```python
    def log_mark(self) -> dict[Path, int]:
        return logscan.mark([self.log_dir / "sail_server.log"])

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        """Workery sesji przebiegu: Sail tworzy driver i N workerów dla każdej sesji Spark Connect
        (przy pierwszym RPC sesji) — liczba z logu pokazuje, czy Sail nie dołożył workera."""
        return {"sail_workers": logscan.sail_workers(logscan.text_since(mark))}
```

8. w `make_engine` gałąź `if variant == "sail":` zastąp:

```python
    if variant in SAIL_PATHS:
        return SailEngine(n_nodes, data_root=data_root, log_dir=log_dir, path=SAIL_PATHS[variant])
```

`bench/orchestrator.py`, metoda `measure`:
1. po linii `swap_before, swap_in_before = ...` dodaj `mark = engine.log_mark()`;
2. po pętli wyznaczającej `dead` dodaj:

```python
        logged = engine.log_metrics(mark)
        broadcast = logged.pop("broadcast_bytes", None)
        extra = {**report["extra"], **logged} if report else None
```

3. w słowniku `measured`: `"extra": json.dumps(report["extra"]) if report else None,` → `"extra": json.dumps(extra) if extra is not None else None,` oraz `"broadcast_bytes": None,` → `"broadcast_bytes": broadcast,`.

`bench/conf/sail_paths.yaml`:

```yaml
# Porównanie ścieżek Saila (specyfikacja 3b-2, 5.3): UDTF z LATERAL, applyInArrow z blokadą
# i applyInArrow z kontekstem polars-bio na wątek — ile kosztuje sam mechanizm integracji.
# Seria pomiarowa — tylko po checkliście RAM (zamknięty VS Code, pusty swap):
#   uv run python -m bench.orchestrator bench/conf/sail_paths.yaml
series: sail-paths
scenarios:
  - {op: overlap, pair: "1-2"}
  - {op: nearest, pair: "1-2"}
  - {op: coverage, pair: "1-2"}
  - {op: merge, dataset: 1}
  - {op: subtract, pair: "1-2"}
  - {op: overlap, pair: "7-0"}
variants: [sail, sail_lock, sail_udtf]
nodes: [1, 3]
repeats: 3
warmup: 1
seed: 20261009
```

- [ ] **Step 5: Uruchom — PASS**

Run: `uv run pytest tests/bench tests/test_sail_runner.py -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed` (test integracyjny ma teraz 10 bloków — kilka minut dłużej).

- [ ] **Step 6: Commit**

```bash
git add bench/logscan.py bench/conf/sail_paths.yaml bench/config.py bench/engines.py bench/orchestrator.py tests/bench/test_logscan.py tests/bench/test_engines.py tests/bench/test_config.py tests/bench/test_orchestrator.py tests/test_orchestrator_integration.py tests/test_sail_runner.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 2): warianty sail_lock i sail_udtf, liczba workerow Saila z logu

Liczniki z logow silnika z czasu przebiegu (bench/logscan.py); konfiguracja
porownania sciezek Saila (sail_paths.yaml).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 12: Wielkość partii wyniku Saila — sonda w ograniczonym czasie (pół dnia)

**Files:**
- Create (poza repozytorium, do wyrzucenia): `<scratchpad>/sail_batches_probe.py`
- Modify (tylko gdy któreś ustawienie pomoże): `bench/runners/sail_server.py` (`sail_env`) **albo** `bench/runners/sail_runner.py` (`SESSION_CONF`, `configure`)
- Modify: `wnioski_claude.md` (sekcja „Plan 3b-2”, punkt o partiach)
- Test: `tests/test_sail_runner.py` (gdy zmiana)

**Interfaces:**
- Consumes: `sail_arrow.build_query`, `sail_runner.iter_arrow`, `warm_up`.
- Produces (tylko w gałęzi B): `bench.runners.sail_runner.SESSION_CONF: dict[str, str]`, `configure(spark) -> None` (wołane po utworzeniu sesji, przed `warm_up`).

**Uwaga o pamięci:** sonda czyta parę 7-0 (serwer Saila do ~1,5 GB). To nie jest pomiar do pracy — VS Code może zostać otwarty, czasy są orientacyjne — ale skrypt ma strażnika `MemAvailable` (przerywa poniżej 500 MiB). Limit czasu zadania: pół dnia; po jego upływie bez wyniku przejdź do Step 5 (gałąź C).

- [ ] **Step 1: Skrypt sondy (`<scratchpad>/sail_batches_probe.py`)**

```python
# Sonda (plan 3b-2, Zadanie 12; do wyrzucenia): liczba partii i czas odbioru wyniku Saila
# (ścieżka arrow, overlap 7-0, N = 3) przy kandydackich ustawieniach rozmiaru partii.
import os, subprocess, sys, threading, time
REPO = "/home/milosz/praca_magisterska"
sys.path.insert(0, REPO)
os.environ.setdefault("MPLBACKEND", "Agg")
from bench.checksum import Checksum
from bench.data.datasets import dataset_dir
from bench.engines import cluster_cpus, taskset
from bench.procutil import free_port, wait_for_port
from bench.runners.sail_runner import iter_arrow, warm_up
from bench.runners.sail_server import sail_env
import sail_arrow

def guard():
    while True:
        kb = int(next(l for l in open("/proc/meminfo") if l.startswith("MemAvailable")).split()[1])
        if kb < 500 * 1024:
            print("GUARD: MemAvailable < 500 MiB - abort", flush=True); os._exit(3)
        time.sleep(0.2)
threading.Thread(target=guard, daemon=True).start()

CANDIDATES = [
    ("baseline", {}, {}),
    ("SAIL_EXECUTION__BATCH_SIZE=65536", {"SAIL_EXECUTION__BATCH_SIZE": "65536"}, {}),
    ("arrow.maxRecordsPerBatch=65536", {}, {"spark.sql.execution.arrow.maxRecordsPerBatch": "65536"}),
    ("arrow.maxRecordsPerOutputBatch=65536", {}, {"spark.sql.execution.arrow.maxRecordsPerOutputBatch": "65536"}),
    ("connect.grpc.arrow.maxBatchSize=64m", {}, {"spark.connect.grpc.arrow.maxBatchSize": "64m"}),
]

def measure(port, conf):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote(f"sc://127.0.0.1:{port}").create()
    try:
        for key, value in conf.items():
            spark.conf.set(key, value)
        warm_up(spark)
        t0 = time.perf_counter()
        df = sail_arrow.build_query(spark, "overlap", dataset_dir(7), dataset_dir(0), mode="context")
        tables, first = [], None
        for table in iter_arrow(spark, df):
            tables.append(table)
            first = first if first is not None else time.perf_counter() - t0
        total = time.perf_counter() - t0
    finally:
        spark.stop()
    checksum = Checksum("overlap"); checksum.update_all(tables)
    return len(tables), checksum.rows, checksum.hex(), first, total

for label, env, conf in CANDIDATES:
    port = free_port()
    log = f"{os.path.dirname(os.path.abspath(__file__))}/probe_server.log"
    proc = subprocess.Popen(
        [*taskset(cluster_cpus(3)), sys.executable, "-m", "bench.runners.sail_server", "--port", str(port)],
        cwd=REPO, env={**os.environ, **sail_env(3), **env}, stdout=open(log, "w"), stderr=subprocess.STDOUT,
    )
    try:
        wait_for_port(port, proc, name="sail_server", log=log)
        results = [measure(port, conf) for _ in range(3)]
        batches, rows, digest, _, _ = results[-1]
        receive = min(total - first for _, _, _, first, total in results)
        total = min(r[4] for r in results)
        print(f"{label:40s} batches={batches:5d} rows={rows} checksum={digest} "
              f"t_total_min={total:.2f}s receive_min={receive:.2f}s", flush=True)
    except Exception as e:
        print(f"{label:40s} ERROR {type(e).__name__}: {str(e)[:300]}", flush=True)
    finally:
        proc.terminate(); proc.wait(30)
```

- [ ] **Step 2: Uruchom sondę**

Run: `cd /home/milosz/praca_magisterska && timeout 1800 uv run python <scratchpad>/sail_batches_probe.py 2>&1 | grep -v -i "warn\|require_minimum"`
Expected: 5 linii; wiersze `rows=2761621` i ta sama suma kontrolna we wszystkich (inna suma = **zatrzymaj się**, błąd wyniku). Zapisz wyniki.

- [ ] **Step 3: Decyzja**

Reguła: przyjmij **jedno** ustawienie, które daje najmniejszy `receive_min`, jeśli zmniejsza go o **co najmniej 20%** względem `baseline` i nie zmienia sumy kontrolnej. Ustawienie środowiskowe (`SAIL_*`) → gałąź A; opcja sesji (`spark.*`) → gałąź B; brak takiego ustawienia → gałąź C.

- [ ] **Step 4A (gałąź A — zmienna środowiskowa serwera): `sail_env`**

W `bench/runners/sail_server.py`, w słowniku zwracanym przez `sail_env`, dopisz zwycięską zmienną z komentarzem nad słownikiem, np.:

```python
    # Rozmiar partii wykonania (sonda 3b-2, Zadanie 12): overlap 7-0, N = 3 — <liczba> partii
    # zamiast 1877, odbiór wyniku <x> s zamiast <y> s, ta sama suma kontrolna.
```
(wpisz zmierzone liczby). W `tests/test_sail_runner.py::test_sail_env_for_three_nodes` dopisz tę zmienną do oczekiwanego słownika. Run: `uv run pytest tests/test_sail_runner.py -v` → PASS.

- [ ] **Step 4B (gałąź B — opcja sesji): `SESSION_CONF` w runnerze**

W `bench/runners/sail_runner.py` pod `PATHS` dodaj (wpisz zwycięską opcję i zmierzone liczby):

```python
#: Opcje sesji Spark Connect ustawiane przed pomiarem (sonda 3b-2, Zadanie 12): overlap 7-0,
#: N = 3 — <liczba> partii wyniku zamiast 1877, odbiór <x> s zamiast <y> s, ta sama suma kontrolna.
SESSION_CONF = {"<klucz>": "<wartość>"}


def configure(spark) -> None:
    """Ustawia SESSION_CONF w sesji — przed rozgrzaniem i pomiarem."""
    for key, value in SESSION_CONF.items():
        spark.conf.set(key, value)
```

W `run`, zaraz po `spark = SparkSession.builder.remote(remote).create()` (wewnątrz `try`), wywołaj `configure(spark)` jako pierwszą instrukcję. Test w `tests/test_sail_runner.py`:

```python
def test_configure_sets_session_options():
    from bench.runners import sail_runner

    calls = []

    class Conf:
        def set(self, key, value):
            calls.append((key, value))

    class Spark:
        conf = Conf()

    sail_runner.configure(Spark())
    assert calls == list(sail_runner.SESSION_CONF.items()) and calls
```

Run: `uv run pytest tests/test_sail_runner.py -v` → PASS.

- [ ] **Step 5: Zapis wyniku w `wnioski_claude.md` (każda gałąź)**

Na końcu `wnioski_claude.md` dodaj (jeśli jeszcze nie ma) sekcję `## Plan 3b-2 — narzędzie gotowe do pomiarów (od 09.10.2026)` z punktem „Partie wyniku Saila”: tabela z 5 wierszami sondy (etykieta, partie, odbiór, t_total), decyzja i — w gałęzi C — zdanie: „Żadne publiczne ustawienie Saila 0.7.2 nie zmniejszyło liczby partii wyniku; odbiór wyniku przez klienta PySpark pozostaje osobną fazą (`t_total_s − first_batch_s`), a asymetria klientów (Ballista: Rust, Sail: Python) jest opisana w metodyce.”

- [ ] **Step 6: Commit**

```bash
git add wnioski_claude.md bench/runners/sail_server.py bench/runners/sail_runner.py tests/test_sail_runner.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 2): sonda rozmiaru partii wyniku Saila i jej wynik

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```
(`git add` niezmienionych plików nic nie robi.)

---

# Etap 3 — Ballista (spec, sekcja 6)

### Zadanie 13: Limit wiadomości gRPC (`BALLISTA_GRPC_MAX_MESSAGE_BYTES`)

**Files:**
- Modify: `ballista_genomics/src/runner.rs` (stałe i `grpc_max_message_bytes`)
- Modify: `ballista_genomics/src/cluster.rs` (`bio_ballista_config`)
- Modify: `ballista_genomics/src/bin/ballista_node.rs` (walidacja, scheduler, executor, komunikat startowy)
- Modify: `ballista_genomics/src/bin/bench_client.rs` (walidacja, `USAGE`)
- Modify: `bench/engines.py` (`GRPC_MAX_MESSAGE_BYTES`, `BallistaEngine(..., grpc_max_message_bytes=...)`)
- Test: `tests/test_bench_client_protocol.py`, `tests/bench/test_engines.py`

**Interfaces:**
- Produces (Rust, `ballista_genomics::runner`): `GRPC_MAX_MESSAGE_ENV = "BALLISTA_GRPC_MAX_MESSAGE_BYTES"`, `DEFAULT_GRPC_MAX_MESSAGE_BYTES = 536870912`, `MIN_GRPC_MAX_MESSAGE_BYTES = 16777216`, `grpc_max_message_bytes() -> Result<usize, String>` (zakres 16 MiB … `u32::MAX`; pusta/brak → domyślna).
- Produces (Python): `bench.engines.GRPC_MAX_MESSAGE_BYTES = 512 * 2**20`; `BallistaEngine(n_nodes, *, profile, data_root, log_dir, grpc_max_message_bytes=GRPC_MAX_MESSAGE_BYTES)` — wartość w `engine.env["BALLISTA_GRPC_MAX_MESSAGE_BYTES"]` wszystkich procesów klastra i klienta.
- Komunikaty startowe `ballista_node` kończą się `grpc max message <N> B`.

- [ ] **Step 1: Testy**

`tests/test_bench_client_protocol.py` — dopisz:

```python
@pytest.mark.parametrize("value", ["1", "x", str(16 * 2**20 - 1), str(2**32)])
def test_invalid_grpc_limit_is_a_usage_error(value, parquet_dirs):
    env = {**os.environ, "BALLISTA_GRPC_MAX_MESSAGE_BYTES": value}
    r = run_client(["--op", "merge", "--left", str(parquet_dirs[0])], env=env, timeout=30)
    assert r.returncode == 2, r.stderr
    assert "BALLISTA_GRPC_MAX_MESSAGE_BYTES" in r.stderr and r.stdout == ""


def test_ballista_node_rejects_invalid_grpc_limit():
    _require(NODE_BINARY)
    r = subprocess.run(
        [str(NODE_BINARY), "scheduler", "--port", "1"],
        cwd=BALLISTA_DIR,
        env={**os.environ, "BALLISTA_GRPC_MAX_MESSAGE_BYTES": "1024"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 2, r.stderr
    assert "BALLISTA_GRPC_MAX_MESSAGE_BYTES" in r.stderr
```

`tests/bench/test_engines.py` — w `test_ballista_cluster_is_pinned_and_configured`, po asercji `BIO_TARGET_PARTITIONS=4`, dopisz:

```python
        # Limit gRPC 512 MiB w każdym procesie (specyfikacja 3b-2, 6.2) — i potwierdzenie z węzłów.
        assert all("BALLISTA_GRPC_MAX_MESSAGE_BYTES=536870912" in environ(pid) for pid in pids.values())
        assert cmd.env["BALLISTA_GRPC_MAX_MESSAGE_BYTES"] == "536870912"
        for name in pids:
            assert "grpc max message 536870912 B" in (tmp_path / "logs" / f"{name}.log").read_text()
```

(asercję o `cmd` umieść po linii `cmd = engine.command(Scenario("merge", "1"))` — przenieś dopisany blok pod nią).

Run: `uv run pytest tests/test_bench_client_protocol.py -k grpc -v`
Expected: FAIL (kod 0 albo 1 zamiast 2).

- [ ] **Step 2: Rust — `runner.rs`**

Pod funkcją `target_partitions` dodaj:

```rust
/// Limit wiadomości gRPC (bajty) klienta, schedulera i executorów (specyfikacja 3b-2, 6.2).
/// Ładunek planu z broadcastem (nearest, coverage) niesie całą tabelę rozsyłaną w każdym zadaniu,
/// a domyślne 16 MiB Ballisty nie mieści par 2-7 i 7-0 (zapytanie wisiało). Orkiestrator
/// przekazuje tę samą wartość wszystkim procesom klastra i klientowi.
pub const GRPC_MAX_MESSAGE_ENV: &str = "BALLISTA_GRPC_MAX_MESSAGE_BYTES";
pub const DEFAULT_GRPC_MAX_MESSAGE_BYTES: usize = 512 * 1024 * 1024;
/// Najmniejsza dopuszczalna wartość — domyślny limit Ballisty; niżej psułby zwykłe wiadomości.
pub const MIN_GRPC_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Wartość z `BALLISTA_GRPC_MAX_MESSAGE_BYTES`: liczba całkowita od 16 MiB do `u32::MAX` (pola
/// konfiguracji serwerów gRPC Ballisty są typu u32); brak albo pusta → 512 MiB. Binarki
/// sprawdzają ją przy starcie.
pub fn grpc_max_message_bytes() -> std::result::Result<usize, String> {
    match std::env::var(GRPC_MAX_MESSAGE_ENV) {
        Ok(v) if !v.is_empty() => v
            .parse::<usize>()
            .ok()
            .filter(|n| (MIN_GRPC_MAX_MESSAGE_BYTES..=u32::MAX as usize).contains(n))
            .ok_or_else(|| {
                format!(
                    "{GRPC_MAX_MESSAGE_ENV}: expected an integer from {MIN_GRPC_MAX_MESSAGE_BYTES} \
                     to {}, got {v:?}",
                    u32::MAX
                )
            }),
        _ => Ok(DEFAULT_GRPC_MAX_MESSAGE_BYTES),
    }
}
```

- [ ] **Step 3: Rust — `cluster.rs`**

Import: `use crate::runner::{bio_session_config, grpc_max_message_bytes};` (zamiast samego `bio_session_config`). Funkcję `bio_ballista_config` zastąp:

```rust
/// `bio_session_config()` z zarejestrowanymi oboma koderami i limitem wiadomości gRPC klienta
/// (`ballista.client.grpc_max_message_size`). Ta konfiguracja trafia do klienta, a przez
/// `override_config_producer` — do schedulera (jego klient gRPC do executorów) i executorów.
pub fn bio_ballista_config() -> SessionConfig {
    // Binarki sprawdzają zmienną przy starcie (błąd użycia, kod 2), więc niepoprawna wartość
    // w tym miejscu to błąd programisty.
    let limit = grpc_max_message_bytes().unwrap_or_else(|e| panic!("{e}"));
    bio_session_config()
        .with_ballista_logical_extension_codec(bio_logical_codec())
        .with_ballista_physical_extension_codec(bio_physical_codec())
        .with_ballista_grpc_client_max_message_size(limit)
}
```

- [ ] **Step 4: Rust — `ballista_node.rs`**

1. Import: `use ballista_genomics::runner::{bio_session_config, grpc_max_message_bytes, target_partitions};`.
2. `run_scheduler(port: u16)` → `run_scheduler(port: u16, grpc_limit: usize)`; w łańcuchu `SchedulerConfig::default()...` po `.with_override_session_builder(...)` dopisz:

```rust
        // Limit wiadomości gRPC serwera schedulera (specyfikacja 3b-2, 6.2).
        .with_grpc_server_max_decoding_message_size(grpc_limit as u32)
        .with_grpc_server_max_encoding_message_size(grpc_limit as u32);
```
(usuń średnik z poprzedniej linii), a komunikat startowy zmień na:

```rust
    println!("ballista_node: scheduler listening on {addr}, grpc max message {grpc_limit} B");
```

3. `run_executor(o: ExecutorOpts)` → `run_executor(o: ExecutorOpts, grpc_limit: usize)`; w `ExecutorProcessConfig { ... }` przed `..ExecutorProcessConfig::default()` dopisz:

```rust
        // Limit wiadomości gRPC serwera executora — tu przychodzą zadania z ładunkiem broadcastu.
        grpc_max_decoding_message_size: grpc_limit as u32,
        grpc_max_encoding_message_size: grpc_limit as u32,
```

a w `println!` executora: format `"ballista_node: executor (flight {}, grpc {}, work dir {}, slots {}, codecs: {}), grpc max message {} B"` z dodatkowym argumentem `grpc_limit`.

4. W `main`, pod sprawdzeniem `target_partitions()`:

```rust
    let grpc_limit = grpc_max_message_bytes().unwrap_or_else(|e| usage_error(&e));
```

i wywołania `run_scheduler(port, grpc_limit)`, `run_executor(opts, grpc_limit)`.

- [ ] **Step 5: Rust — `bench_client.rs`**

1. Import: `use ballista_genomics::runner::{connect_from_env, grpc_max_message_bytes, target_partitions};`.
2. W `parse`, pod `target_partitions()?;`, dopisz `grpc_max_message_bytes()?;`.
3. W `USAGE` linię `Environment: ...` zastąp:

```
  Environment: BALLISTA_SCHEDULER_URL (cluster), BIO_TARGET_PARTITIONS (≥ 2, default 4),
  BALLISTA_GRPC_MAX_MESSAGE_BYTES (16 MiB .. 4 GiB - 1, default 512 MiB).
```

- [ ] **Step 6: Python — `bench/engines.py`**

Pod `SLOTS_PER_NODE` dodaj:

```python
#: Limit wiadomości gRPC Ballisty (specyfikacja 3b-2, 6.2): w nearest i coverage każde zadanie
#: niesie całą tabelę rozsyłaną (~29 MiB dla 2-7, ~56 MiB dla 7-0), a wiadomość do executora —
#: kilka zadań; domyślne 16 MiB Ballisty nie wystarcza.
GRPC_MAX_MESSAGE_BYTES = 512 * 2**20
```

`BallistaEngine.__init__` zastąp:

```python
    def __init__(self, n_nodes: int, *, profile: str, data_root: Path, log_dir: Path,
                 grpc_max_message_bytes: int = GRPC_MAX_MESSAGE_BYTES):
        self.n, self.profile, self.data_root, self.log_dir = n_nodes, profile, data_root, log_dir
        self.env = {
            **os.environ,
            "BIO_TARGET_PARTITIONS": str(SLOTS_PER_NODE * n_nodes),
            "BALLISTA_GRPC_MAX_MESSAGE_BYTES": str(grpc_max_message_bytes),
        }
        self.procs: dict[str, subprocess.Popen] = {}
        self.work_dirs: dict[str, Path] = {}
        self.port: int | None = None
        self._tmp: Path | None = None
```

i docstring klasy uzupełnij: „…; BIO_TARGET_PARTITIONS = 2N i BALLISTA_GRPC_MAX_MESSAGE_BYTES we wszystkich procesach i w kliencie.”

- [ ] **Step 7: Przebuduj debug i uruchom — PASS**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -3`
Expected: `Finished`.
Run: `uv run pytest tests/test_bench_client_protocol.py tests/bench/test_engines.py tests/test_ballista_multiprocess.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add ballista_genomics/src/runner.rs ballista_genomics/src/cluster.rs ballista_genomics/src/bin/ballista_node.rs ballista_genomics/src/bin/bench_client.rs bench/engines.py tests/test_bench_client_protocol.py tests/bench/test_engines.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 3): limit wiadomosci gRPC Ballisty 512 MiB (BALLISTA_GRPC_MAX_MESSAGE_BYTES)

Ten sam limit w kliencie, schedulerze i executorach przez publiczne opcje Ballisty 53.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 14: Strażnik broadcastu — czytelny błąd zamiast zawieszonego zapytania

**Files:**
- Create: `ballista_genomics/src/broadcast.rs`
- Modify: `ballista_genomics/src/codec_io.rs` (`ipc_len`), `src/dist_provider.rs` (`scan`), `src/lib.rs` (moduł i mapa modułów)
- Test: `tests/test_ballista_grpc_limit.py` (nowy)

**Interfaces:**
- Consumes: `grpc_max_message_bytes`, `GRPC_MAX_MESSAGE_ENV` (Zadanie 13); `BallistaEngine(grpc_max_message_bytes=...)`.
- Produces (Rust): `ballista_genomics::broadcast::{TASKS_PER_MESSAGE = 2, broadcast_batch(&Arc<dyn ExecutionPlan>) -> Option<Arc<RecordBatch>>, check_broadcast_fits(&Arc<dyn ExecutionPlan>, limit: usize) -> Result<()>}`; `codec_io::ipc_len(&RecordBatch) -> Result<usize>`. Komunikat błędu zawiera `exceeds the gRPC message limit` i `BALLISTA_GRPC_MAX_MESSAGE_BYTES`.

- [ ] **Step 1: Test (`tests/test_ballista_grpc_limit.py`)**

```python
"""Strażnik broadcastu (plan 3b-2, Zadanie 14; specyfikacja 6.2): gdy ładunki z tabelą rozsyłaną
nie mieszczą się w limicie wiadomości gRPC, zapytanie kończy się czytelnym błędem — zamiast
wisieć do timeoutu, jak robi to Ballista 53 w trybie push (uznaje executor za utraconego i ponawia).

Tabela rozsyłana w coverage to prawa strona (df2): 600 tys. przedziałów ≈ 10–15 MB w Arrow IPC,
czyli × 2 zadania na wiadomość > 16 MiB. Wymaga binarek debug (brak = BŁĄD, jak w P0)."""

from __future__ import annotations

import subprocess
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from bench import engines
from tests.parquet_fixture import FIXTURE_A, write_parts

BIG_ROWS = 600_000


@pytest.fixture(scope="module")
def pair(tmp_path_factory):
    root = tmp_path_factory.mktemp("big_broadcast")
    left = write_parts(FIXTURE_A, root / "a")
    right = root / "b"
    right.mkdir()
    starts = pa.array(range(0, 10 * BIG_ROWS, 10), pa.int32())
    pq.write_table(
        pa.table({"contig": pa.array(["chr1"] * BIG_ROWS, pa.string()), "pos_start": starts,
                  "pos_end": pc.add(starts, 5)}),
        right / "part-00000.parquet",
    )
    return left, right


def run_coverage(engine, left, right, timeout: int = 180):
    argv = [str(engines.ballista_binary("bench_client", "debug")), "--op", "coverage",
            "--left", str(left), "--right", str(right)]
    env = {**engine.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{engine.port}"}
    t0 = time.monotonic()
    r = subprocess.run(argv, cwd=engines.BALLISTA_DIR, env=env, capture_output=True, text=True, timeout=timeout)
    return r, time.monotonic() - t0


def cluster(tmp_path, limit: int):
    return engines.BallistaEngine(
        1, profile="debug", data_root=tmp_path, log_dir=tmp_path / "logs", grpc_max_message_bytes=limit,
    )


@pytest.mark.nodes(1)
def test_broadcast_over_the_limit_fails_fast_with_a_clear_error(pair, tmp_path):
    engine = cluster(tmp_path, 16 * 2**20)
    engine.start()
    try:
        r, elapsed = run_coverage(engine, *pair)
    finally:
        engine.stop()
    assert r.returncode == 1, f"stdout: {r.stdout}\nstderr: {r.stderr[-3000:]}"
    assert "exceeds the gRPC message limit" in r.stderr and "BALLISTA_GRPC_MAX_MESSAGE_BYTES" in r.stderr
    assert elapsed < 60


@pytest.mark.nodes(1)
def test_same_broadcast_fits_under_the_default_limit(pair, tmp_path):
    engine = cluster(tmp_path, engines.GRPC_MAX_MESSAGE_BYTES)
    engine.start()
    try:
        r, _ = run_coverage(engine, *pair)
    finally:
        engine.stop()
    assert r.returncode == 0, r.stderr[-3000:]
    assert '"rows": 12' in r.stdout  # coverage: wiersz na każdy przedział lewej tabeli (FIXTURE_A)
```

Run: `uv run pytest tests/test_ballista_grpc_limit.py -v`
Expected: FAIL — pierwszy test kończy się timeoutem (180 s) albo innym błędem niż oczekiwany.

- [ ] **Step 2: Rust — `codec_io.rs`**

Pod `write_batch_ipc` dodaj:

```rust
/// Rozmiar partii zakodowanej tak jak w ładunku planu (`write_batch_ipc`), w bajtach.
pub fn ipc_len(batch: &RecordBatch) -> Result<usize> {
    let mut buf = Vec::new();
    write_batch_ipc(&mut buf, batch)?;
    Ok(buf.len())
}
```

W komentarzu bloku „GRANICA SKALOWALNOSCI” dopisz zdanie: „Od planu 3b-2 limit podnosi `BALLISTA_GRPC_MAX_MESSAGE_BYTES` (512 MiB), a przekroczenie wykrywa przy planowaniu `broadcast.rs`.”

- [ ] **Step 3: Rust — `broadcast.rs` (nowy)**

```rust
//! Ładunek broadcastu a limit wiadomości gRPC (specyfikacja 3b-2, 6.2).
//!
//! `nearest` i `coverage` rozsyłają całą tabelę indeksowaną w ładunku planu fizycznego KAŻDEGO
//! zadania (`bio_phys_codec.rs`). Gdy wiadomość do executora przekroczy limit gRPC, Ballista 53
//! w trybie push nie zgłasza błędu zapytania, tylko uznaje executor za utraconego i ponawia —
//! zapytanie wisi do timeoutu. Rozmiar sprawdzamy więc przy planowaniu fizycznym (w schedulerze),
//! gdzie tabela rozsyłana jest materializowana, i kończymy planowanie czytelnym błędem, który
//! dociera do klienta jako błąd zapytania.

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_bio_function_ranges::NearestExec;

use crate::codec_io::ipc_len;
use crate::coverage_node::DistCoverageExec;
use crate::runner::GRPC_MAX_MESSAGE_ENV;

/// Zadania jednego etapu w jednej wiadomości do executora — sloty zadań executora w narzędziu
/// pomiarowym (`bench/engines.py`, `SLOTS_PER_NODE = 2`).
pub const TASKS_PER_MESSAGE: usize = 2;

/// Tabela rozsyłana przez pierwszy węzeł z broadcastem w drzewie planu.
pub fn broadcast_batch(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<RecordBatch>> {
    let any = plan.as_any();
    if let Some(n) = any.downcast_ref::<NearestExec>() {
        return Some(Arc::clone(&n.left_batch));
    }
    if let Some(c) = any.downcast_ref::<DistCoverageExec>() {
        return Some(Arc::clone(&c.left_batch));
    }
    plan.children().into_iter().find_map(|child| broadcast_batch(child))
}

/// Błąd planowania, gdy ładunki broadcastu dla `TASKS_PER_MESSAGE` zadań nie mieszczą się
/// w limicie wiadomości gRPC `limit` (bajty).
pub fn check_broadcast_fits(plan: &Arc<dyn ExecutionPlan>, limit: usize) -> Result<()> {
    let Some(batch) = broadcast_batch(plan) else {
        return Ok(());
    };
    let size = ipc_len(&batch)?;
    if size.saturating_mul(TASKS_PER_MESSAGE) > limit {
        return Err(DataFusionError::Plan(format!(
            "broadcast table of {size} bytes ({} rows) x {TASKS_PER_MESSAGE} tasks per message \
             exceeds the gRPC message limit of {limit} bytes ({GRPC_MAX_MESSAGE_ENV})",
            batch.num_rows()
        )));
    }
    Ok(())
}
```

- [ ] **Step 4: Rust — `dist_provider.rs` i `lib.rs`**

`dist_provider.rs`: import `use crate::broadcast::check_broadcast_fits;` oraz `use crate::runner::{bio_session_config, grpc_max_message_bytes};` (zamiast `use crate::runner::bio_session_config;`). Ciało `scan` zastąp:

```rust
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        // Strażnik broadcastu (specyfikacja 3b-2, 6.2): planowanie fizyczne odbywa się
        // w schedulerze, więc błąd dociera do klienta jako błąd zapytania, zamiast zawiesić je
        // przy wysyłce zadań do executorów.
        let grpc_limit = grpc_max_message_bytes().map_err(DataFusionError::Configuration)?;
        check_broadcast_fits(&plan, grpc_limit)?;
        Ok(plan)
```

`lib.rs`: dodaj `pub mod broadcast;` (alfabetycznie, po `bio_phys_codec`) i w mapie modułów linię:

```rust
//! - `broadcast`       — strażnik rozmiaru ładunku broadcastu względem limitu gRPC (plan 3b-2)
```

- [ ] **Step 5: Przebuduj debug i uruchom — PASS**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -3`
Run: `uv run pytest tests/test_ballista_grpc_limit.py -v`
Expected: PASS. **Jeśli pierwszy test nadal wisi do timeoutu** (błąd planowania nie dociera do klienta) — zatrzymaj się (zasada zatrzymania) i opisz użytkownikowi, co pokazują logi schedulera (`<tmp>/logs/scheduler.log`).
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed` (strażnik działa też w trybie standalone testów — z limitem 512 MiB nic nie zmienia).

- [ ] **Step 6: Commit**

```bash
git add ballista_genomics/src/broadcast.rs ballista_genomics/src/codec_io.rs ballista_genomics/src/dist_provider.rs ballista_genomics/src/lib.rs tests/test_ballista_grpc_limit.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 3): straznik broadcastu - czytelny blad zamiast zawieszonego zapytania

Rozmiar IPC tabeli rozsylanej x 2 zadania na wiadomosc porownywany z limitem gRPC
przy planowaniu fizycznym w schedulerze.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 15: `broadcast_bytes` — rozmiar ładunków odebranych przez executory

**Files:**
- Modify: `ballista_genomics/Cargo.toml` (`tracing = "0.1"`), `ballista_genomics/src/bio_phys_codec.rs` (log przy dekodowaniu)
- Modify: `bench/engines.py` (`BallistaEngine.log_mark`, `log_metrics`)
- Test: `tests/test_ballista_broadcast_bytes.py` (nowy), `tests/test_orchestrator_integration.py`

**Interfaces:**
- Consumes: `bench.logscan.mark`, `text_since`, `broadcast_bytes` (Zadanie 11).
- Produces: linia logu executora `broadcast payload decoded: <N> bytes` (poziom info, cel `ballista_genomics::bio_phys_codec`) przy każdym dekodowaniu `NearestExec`/`DistCoverageExec`; `BallistaEngine.log_metrics(mark) -> {"broadcast_bytes": int}` (suma z logów executorów).

- [ ] **Step 1: Testy**

`tests/test_ballista_broadcast_bytes.py`:

```python
"""broadcast_bytes (plan 3b-2, Zadanie 15; specyfikacja 6.3): executory logują rozmiar każdego
odebranego ładunku broadcastu, a silnik sumuje te wpisy z czasu przebiegu — bajty, które
faktycznie dotarły (ładunek × liczba zadań). Wymaga binarek debug."""

from __future__ import annotations

import subprocess

import pytest

from bench import engines
from bench.config import Scenario
from bench.ops import OPS, UNARY_OPS


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("broadcast_bytes")
    engine = engines.BallistaEngine(2, profile="debug", data_root=tmp, log_dir=tmp / "logs")
    engine.start()
    yield engine
    engine.stop()


@pytest.mark.nodes(2)
@pytest.mark.parametrize("op", OPS)
def test_broadcast_bytes_only_for_broadcast_operations(op, cluster, parquet_dirs):
    left, right = parquet_dirs
    argv = [str(engines.ballista_binary("bench_client", "debug")), "--op", op, "--left", str(left)]
    if op not in UNARY_OPS:
        argv += ["--right", str(right)]
    env = {**cluster.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{cluster.port}"}
    mark = cluster.log_mark()
    r = subprocess.run(argv, cwd=engines.BALLISTA_DIR, env=env, capture_output=True, text=True, timeout=180)
    assert r.returncode == 0, r.stderr[-3000:]
    got = cluster.log_metrics(mark)["broadcast_bytes"]
    if op in ("nearest", "coverage"):
        assert got > 0
    else:
        assert got == 0
```

`tests/test_orchestrator_integration.py` — dopisz:

```python
@pytest.mark.nodes(2)
def test_ballista_broadcast_volume_is_measured(series):
    _, df = series
    ballista = df.filter((pl.col("variant") == "ballista") & ~pl.col("is_reference"))
    broadcast = ballista.filter(pl.col("op").is_in(["nearest", "coverage"]))
    assert (broadcast["broadcast_bytes"] > 0).all()
    assert (ballista.filter(~pl.col("op").is_in(["nearest", "coverage"]))["broadcast_bytes"] == 0).all()
    assert df.filter(pl.col("variant") != "ballista")["broadcast_bytes"].is_null().all()
```

Run: `uv run pytest tests/test_ballista_broadcast_bytes.py -v`
Expected: FAIL (`KeyError: 'broadcast_bytes'`).

- [ ] **Step 2: Rust**

`Cargo.toml`, pod `tracing-subscriber`:

```toml
# Linie logu węzłów z rozmiarem odebranego ładunku broadcastu (plan 3b-2); wersja z Cargo.lock.
tracing = "0.1"
```

`bio_phys_codec.rs` — pod funkcją `build_nearest_exec` dodaj:

```rust
/// Rozmiar odebranego ładunku broadcastu: orkiestrator sumuje te linie z logów executorów
/// w kolumnę `broadcast_bytes` (specyfikacja 3b-2, 6.3; wzorzec w `bench/logscan.py`).
fn log_broadcast_decoded(bytes: usize) {
    tracing::info!("broadcast payload decoded: {bytes} bytes");
}
```

W `decode_nearest` i `decode_coverage` linię `let left_batch = read_batch_ipc(buf, pos)?;` zastąp:

```rust
    let payload_start = *pos;
    let left_batch = read_batch_ipc(buf, pos)?;
    log_broadcast_decoded(*pos - payload_start);
```

- [ ] **Step 3: Python — `BallistaEngine`**

Metody `log_mark` i `log_metrics` (dodane w Zadaniu 11) zastąp:

```python
    def log_mark(self) -> dict[Path, int]:
        return logscan.mark(self.log_dir / f"executor_{k}.log" for k in range(1, self.n + 1))

    def log_metrics(self, mark: dict[Path, int]) -> dict:
        """Bajty ładunków broadcastu odebranych przez executory w czasie przebiegu
        (bio_phys_codec.rs loguje każde dekodowanie węzła z broadcastem)."""
        return {"broadcast_bytes": logscan.broadcast_bytes(logscan.text_since(mark))}
```

- [ ] **Step 4: Przebuduj debug i uruchom — PASS**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -3`
Run: `uv run pytest tests/test_ballista_broadcast_bytes.py -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 5: Commit**

```bash
git add ballista_genomics/Cargo.toml ballista_genomics/Cargo.lock ballista_genomics/src/bio_phys_codec.rs bench/engines.py tests/test_ballista_broadcast_bytes.py tests/test_orchestrator_integration.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 3): broadcast_bytes z logow executorow Ballisty

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 16: `EXPLAIN ANALYZE` w dodatkowym, niemierzonym przebiegu

**Files:**
- Modify: `ballista_genomics/src/runner.rs` (`pub fn extract_text`), `src/bin/bench_client.rs` (tryb `--explain-analyze`)
- Modify: `bench/config.py` (`explain`), `bench/engines.py` (`explain_command`), `bench/orchestrator.py` (`_Series(directory)`, `explain_pass`), `bench/conf/smoke.yaml`, `.gitignore`
- Test: `tests/test_bench_client_protocol.py`, `tests/bench/test_config.py`, `tests/bench/test_orchestrator.py`, `tests/bench/fake_runner.py`

**Interfaces:**
- Produces: `bench_client --op OP --left L [--right R] [--cols C] --explain-analyze <FILE>` — zapisuje plan `EXPLAIN ANALYZE` (tekst) do pliku, na stdout `{"explain_bytes": N}`; nie łączy się z `--output` ani `--checksum` (kod 2). `SeriesConfig.explain: bool` (domyślnie `False`). Interfejs silnika: `explain_command(scenario, target: Path) -> Command | None` (Ballista — polecenie; pozostałe — `None`). Pliki: `<katalog serii>/explain/<wariant>-n<N>/<op>_<dane>.txt`.

- [ ] **Step 1: Testy**

`tests/test_bench_client_protocol.py` — dopisz:

```python
def test_explain_analyze_writes_the_distributed_plan(parquet_dirs, tmp_path):
    target = tmp_path / "merge_1.txt"
    r = run_client(["--op", "merge", "--left", str(parquet_dirs[0]), "--explain-analyze", str(target)])
    assert r.returncode == 0, r.stderr[-3000:]
    text = target.read_text()
    assert "MergeExec" in text and "ShuffleWriterExec" in text
    assert json.loads(r.stdout) == {"explain_bytes": len(text.encode())}


@pytest.mark.parametrize(
    "args",
    [
        ["--op", "merge", "--left", "x", "--explain-analyze", "a.txt", "--output", "b.parquet"],
        ["--op", "merge", "--checksum", "x.parquet", "--explain-analyze", "a.txt"],
    ],
)
def test_explain_analyze_rejects_conflicting_modes(args):
    r = run_client(args, timeout=30)
    assert r.returncode == 2, f"{args}: exit code {r.returncode}, stderr: {r.stderr}"
    assert r.stdout == ""
```

`tests/bench/fake_runner.py` — dopisz argument i zachowanie (przed `if args.touch:`):

```python
parser.add_argument("--write", type=Path, help="file written instead of a report (EXPLAIN ANALYZE)")
```

a po `if args.exit: ...` (przed `if args.garbage:`):

```python
if args.write:
    args.write.write_text("plan of the fake engine\n")
    sys.exit(0)
```

`tests/bench/test_config.py`:
1. w `test_minimal_config_gets_spec_defaults` dopisz `assert cfg.explain is False`;
2. do parametrów błędów dopisz `({"explain": "yes"}, "explain: expected true or false"),`;
3. w `test_smoke_config_is_the_full_matrix_on_pair_1_2` dopisz `assert cfg.explain is True`.

`tests/bench/test_orchestrator.py`:
1. w `FakeEngine` dodaj:

```python
    def explain_command(self, scenario, target):
        if self.variant != "ballista":
            return None
        args = self.explain_args or ["--write", str(target)]
        return Command([sys.executable, str(FAKE_RUNNER), *args], dict(os.environ), REPO)
```

i w `__init__` `self.explain_args: list[str] | None = None`;
2. dopisz testy:

```python
def test_explain_pass_writes_plans_of_ballista_blocks(tmp_path):
    summary = run(tmp_path, cfg=config(explain=True))
    explain = summary.directory / "explain"
    assert sorted(p.relative_to(explain).as_posix() for p in explain.rglob("*.txt")) == [
        "ballista-n1/merge_1.txt", "ballista-n1/overlap_1-2.txt",
    ]
    assert rows(summary).height == 2 + 2 * (2 + 2 + 4)  # przebiegi explain nie trafiają do wyników
    assert summary.ok


def test_failed_explain_is_reported_as_series_failure(tmp_path):
    def customize(engine):
        engine.explain_args = ["--exit", "1"]

    summary = run(tmp_path, cfg=config(explain=True), factory=make_factory(customize=customize))
    assert summary.failures == [
        "ballista N=1: EXPLAIN ANALYZE overlap/1-2: engine crashed: test error",
        "ballista N=1: EXPLAIN ANALYZE merge/1: engine crashed: test error",
    ]
    assert not summary.ok


def test_no_explain_pass_by_default(tmp_path):
    assert not (run(tmp_path).directory / "explain").exists()
```

Run: `uv run pytest tests/bench/test_config.py tests/bench/test_orchestrator.py -k explain -v`
Expected: FAIL.

- [ ] **Step 2: Rust — `runner.rs` i `bench_client.rs`**

`runner.rs`: `fn extract_text(` → `pub fn extract_text(`.

`bench_client.rs`:
1. import: `use ballista_genomics::runner::{connect_from_env, extract_text, grpc_max_message_bytes, target_partitions};`;
2. `USAGE` uzupełnij o wariant i opis:

```
  bench_client --op <OPERATION> --left <PATH> [--right <PATH>] [--cols <contig,start,end>]
               --explain-analyze <FILE.txt>
  ...
  --explain-analyze: write the EXPLAIN ANALYZE plan (with operator metrics) to a file instead of
  measuring; stdout: {"explain_bytes": N}.
```

3. `enum Mode` — dodaj wariant:

```rust
    /// Plan EXPLAIN ANALYZE scenariusza do pliku (dodatkowy, niemierzony przebieg; plan 3b-2).
    Explain { scenario: Scenario, file: String },
```

4. w `parse`: do listy flag dodaj `"--explain-analyze"`; w bloku `--checksum` do listy konfliktów dodaj `"--explain-analyze"`; na końcu funkcji zamiast `Ok(Mode::Run { ... })`:

```rust
    let output = optional(&flags, "--output")?;
    match optional::<String>(&flags, "--explain-analyze")? {
        Some(_) if output.is_some() => Err("--explain-analyze cannot be combined with --output".to_string()),
        Some(file) => Ok(Mode::Explain { scenario, file }),
        None => Ok(Mode::Run { scenario, output }),
    }
```

5. nowa funkcja (pod `run`):

```rust
/// Dodatkowy, niemierzony przebieg: plan EXPLAIN ANALYZE z metrykami operatorów do pliku
/// (specyfikacja 3b-2, 6.4). Zwraca liczbę zapisanych bajtów.
async fn explain(scenario: &Scenario, file: &str) -> Result<usize> {
    let ctx = connect_from_env().await?;
    let batches = ctx
        .sql(&format!("EXPLAIN ANALYZE {}", scenario.sql()))
        .await?
        .collect()
        .await?;
    let text = extract_text(&batches);
    std::fs::write(file, &text)?;
    Ok(text.len())
}
```

6. w `main` dodaj gałąź:

```rust
        Mode::Explain { scenario, file } => explain(&scenario, &file)
            .await
            .map(|bytes| format!("{{\"explain_bytes\": {bytes}}}")),
```

- [ ] **Step 3: Python — konfiguracja, silnik, orkiestrator**

`bench/config.py`:
1. do `DEFAULTS` dodaj `"explain": False,` z komentarzem nad słownikiem (dopisz zdanie): „explain: dodatkowy przebieg EXPLAIN ANALYZE każdego scenariusza w blokach Ballisty (3b-2, 6.4).”;
2. w `SeriesConfig` pole `explain: bool`;
3. w `parse` przed `cfg = SeriesConfig(...)`:

```python
    explain = raw["explain"]
    if not isinstance(explain, bool):
        raise ConfigError(f"explain: expected true or false, got {explain!r}")
```

i argument `explain=explain,`.

`bench/engines.py`:
1. `PolarsBioEngine` i `SailEngine` — dodaj:

```python
    def explain_command(self, scenario: Scenario, target: Path) -> Command | None:
        return None
```

2. `BallistaEngine` — dodaj:

```python
    def explain_command(self, scenario: Scenario, target: Path) -> Command:
        """Dodatkowy, niemierzony przebieg EXPLAIN ANALYZE (specyfikacja 3b-2, 6.4)."""
        command = self.command(scenario)
        return Command([*command.argv, "--explain-analyze", str(target)], command.env, command.cwd)
```

3. w docstringu modułu dopisz do opisu interfejsu: „explain_command(scenariusz, plik) → Command albo None (EXPLAIN ANALYZE; tylko Ballista)”.

`bench/orchestrator.py`:
1. import: `from bench.validity import Expected, control_drift, invalid_reasons, parse_report, stderr_tail`;
2. `_Series.__init__` — nowy parametr `directory` (na początku listy) i atrybut:

```python
    def __init__(self, cfg, factory, probe, mem_floor, directory, scratch, writer, out, summary, profile):
        self.cfg, self.factory, self.probe, self.mem_floor = cfg, factory, probe, mem_floor
        self.directory, self.scratch, self.writer, self.out, self.summary = directory, scratch, writer, out, summary
```

(w `run_series`: `_Series(cfg, factory, probe or Probe(), mem_floor, directory, scratch, writer, out, summary, profile)`);
3. nowa metoda `_Series` (pod `measure`):

```python
    def explain_pass(self, engine, block: Block) -> None:
        """Dodatkowy, niemierzony przebieg EXPLAIN ANALYZE każdego scenariusza (specyfikacja 3b-2,
        6.4): plan z metrykami operatorów do pliku; nie trafia do wyników. Niepowodzenie →
        podsumowanie serii (seria kończy się kodem 1)."""
        for scenario in self.cfg.scenarios:
            target = (self.directory / "explain" / f"{block.variant}-n{block.n_nodes}"
                      / f"{scenario.id.replace('/', '_')}.txt")
            command = engine.explain_command(scenario, target)
            if command is None:
                return
            target.parent.mkdir(parents=True, exist_ok=True)
            self.out(f"{block.label}: EXPLAIN ANALYZE {scenario.id}")
            outcome = execute(
                command, timeout_s=self.cfg.timeout_s, mem_floor=self.mem_floor, probe=self.probe,
                scratch=self.scratch,
            )
            if outcome.killed or outcome.returncode != 0 or not target.exists() or not target.stat().st_size:
                why = outcome.killed or stderr_tail(outcome.stderr)
                self.summary.failures.append(f"{block.label}: EXPLAIN ANALYZE {scenario.id}: {why}")
```

4. w `run_block`, po `end = controls("end")` (wewnątrz `try`), dodaj:

```python
            if self.cfg.explain:
                self.explain_pass(engine, block)
```

`bench/conf/smoke.yaml` — przed `seed:` dopisz:

```yaml
# Smoke sprawdza też dodatkowy przebieg EXPLAIN ANALYZE w blokach Ballisty (plan 3b-2).
explain: true
```

`.gitignore` — pod regułami `bench/results/**`:

```
# Plany EXPLAIN ANALYZE serii (plan 3b-2) — w repozytorium mimo reguły *.txt
!bench/results/**/explain/**/*.txt
```

- [ ] **Step 4: Przebuduj debug i uruchom — PASS**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -3`
Run: `uv run pytest tests/test_bench_client_protocol.py tests/bench -v`
Expected: PASS.
Run: `mkdir -p bench/results/x/y/explain/ballista-n1 && touch bench/results/x/y/explain/ballista-n1/merge_1.txt && git check-ignore -v bench/results/x/y/explain/ballista-n1/merge_1.txt; echo "exit=$?"; rm -r bench/results/x`
Expected: `exit=1` (plik NIE jest ignorowany).
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 5: Commit**

```bash
git add ballista_genomics/src/runner.rs ballista_genomics/src/bin/bench_client.rs bench/config.py bench/engines.py bench/orchestrator.py bench/conf/smoke.yaml .gitignore tests/test_bench_client_protocol.py tests/bench/test_config.py tests/bench/test_orchestrator.py tests/bench/fake_runner.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 3): EXPLAIN ANALYZE Ballisty w dodatkowym, niemierzonym przebiegu

Tryb bench_client --explain-analyze, klucz konfiguracji explain, plany w katalogu
serii (explain/<wariant>-n<N>/), wyjatek w .gitignore.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 17: Strażnik starej binarki, sumy SHA-256 i wersja biblioteki algorytmów w `engine_versions`

**Files:**
- Modify: `bench/engines.py` (`ballista_sources`, `require_ballista_binaries`)
- Modify: `bench/orchestrator.py` (`engine_versions`, `_Series.__init__`)
- Test: `tests/bench/test_engines.py`, `tests/bench/test_orchestrator.py`

**Interfaces:**
- Produces: `bench.engines.ballista_sources() -> list[Path]` (`*.rs` w `src/` i `vendor/`, pliki `Cargo.toml` tamże, `Cargo.lock`); `require_ballista_binaries(profile)` rzuca `EngineError` „… is older than …”, gdy binarka jest starsza od najnowszego źródła; `engine_versions(profile, *, ballista: bool) -> dict[str, str]` z kluczami `datafusion_bio_function_ranges` oraz (gdy `ballista`) `ballista_node_sha256`, `bench_client_sha256`.

- [ ] **Step 1: Testy**

`tests/bench/test_engines.py` — dopisz:

```python
def _fake_ballista(tmp_path: Path) -> Path:
    root = tmp_path / "ballista_genomics"
    (root / "src").mkdir(parents=True)
    (root / "src" / "lib.rs").write_text("// source\n")
    (root / "Cargo.toml").write_text("[package]\n")
    (root / "Cargo.lock").write_text("")
    (root / "vendor" / "crate" / "src").mkdir(parents=True)
    (root / "vendor" / "crate" / "PATCH.md").write_text("docs\n")
    (root / "target" / "debug").mkdir(parents=True)
    for name in ("ballista_node", "bench_client"):
        (root / "target" / "debug" / name).write_bytes(b"binary")
    for source in (root / "Cargo.toml", root / "Cargo.lock"):
        _set_mtime(source, 500_000)  # manifesty starsze od binarek — o wyniku decyduje lib.rs
    return root


def _set_mtime(path: Path, t: float) -> None:
    os.utime(path, (t, t))


def test_stale_ballista_binary_is_rejected(tmp_path, monkeypatch):
    root = _fake_ballista(tmp_path)
    monkeypatch.setattr(engines, "BALLISTA_DIR", root)
    for binary in (root / "target" / "debug").iterdir():
        _set_mtime(binary, 1_000_000)
    _set_mtime(root / "src" / "lib.rs", 2_000_000)
    with pytest.raises(EngineError, match=r"bench_client.* is older than .*lib\.rs.*cargo build"):
        engines.require_ballista_binaries("debug")
    _set_mtime(root / "src" / "lib.rs", 500_000)
    engines.require_ballista_binaries("debug")  # binarki nowsze od źródeł


def test_documentation_in_vendor_does_not_make_binaries_stale(tmp_path, monkeypatch):
    root = _fake_ballista(tmp_path)
    monkeypatch.setattr(engines, "BALLISTA_DIR", root)
    _set_mtime(root / "vendor" / "crate" / "PATCH.md", 9_000_000_000)
    engines.require_ballista_binaries("debug")
```

(dodaj `import os` na górze pliku, jeśli go brak).

`tests/bench/test_orchestrator.py` — dopisz:

```python
def test_engine_versions_record_algorithm_crate_and_binary_hashes(tmp_path, monkeypatch):
    import hashlib

    root = tmp_path / "ballista_genomics"
    (root / "target" / "release").mkdir(parents=True)
    (root / "Cargo.lock").write_text(
        'name = "ballista"\nversion = "53.0.0"\n\nname = "datafusion"\nversion = "53.0.0"\n\n'
        'name = "datafusion-bio-function-ranges"\nversion = "0.22.2"\n'
    )
    (root / "target" / "release" / "ballista_node").write_bytes(b"node")
    monkeypatch.setattr(engines, "BALLISTA_DIR", root)
    monkeypatch.setattr(orch, "BALLISTA_DIR", root)
    versions = orch.engine_versions("release", ballista=True)
    assert versions["datafusion_bio_function_ranges"] == "0.22.2"
    assert versions["ballista_node_sha256"] == hashlib.sha256(b"node").hexdigest()
    assert versions["bench_client_sha256"] == "missing"
    assert "ballista_node_sha256" not in orch.engine_versions("release", ballista=False)
```

Run: `uv run pytest tests/bench/test_engines.py tests/bench/test_orchestrator.py -k "stale or vendor or engine_versions" -v`
Expected: FAIL.

- [ ] **Step 2: Implementacja**

`bench/engines.py` — `require_ballista_binaries` zastąp:

```python
def ballista_sources() -> list[Path]:
    """Źródła binarek Ballisty: kod Rusta (src/, vendor/), manifesty i Cargo.lock — bez
    dokumentacji, której zmiana nie wymaga przebudowy."""
    files = [BALLISTA_DIR / "Cargo.toml", BALLISTA_DIR / "Cargo.lock"]
    for sub in ("src", "vendor"):
        root = BALLISTA_DIR / sub
        files.extend(p for p in root.rglob("*.rs") if p.is_file())
        files.extend(p for p in root.rglob("Cargo.toml") if p.is_file())
    return [p for p in files if p.exists()]


def require_ballista_binaries(profile: str) -> None:
    """Binarki istnieją i są nowsze od źródeł — inaczej pomiar objąłby kod sprzed ostatniej
    zmiany (przegląd 3a: pochodzenie binarek)."""
    binaries = (ballista_binary("ballista_node", profile), ballista_binary("bench_client", profile))
    flag = "--release " if profile == "release" else ""
    build = f"cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build {flag}--bin ballista_node --bin bench_client"
    missing = [str(b) for b in binaries if not b.exists()]
    if missing:
        raise EngineError(f"missing binary {', '.join(missing)} - build it: {build}")
    newest = max(ballista_sources(), key=lambda p: p.stat().st_mtime, default=None)
    if newest is not None:
        stale = [str(b) for b in binaries if b.stat().st_mtime < newest.stat().st_mtime]
        if stale:
            raise EngineError(f"binary {', '.join(stale)} is older than {newest} - rebuild it: {build}")
```

`bench/orchestrator.py`:
1. import `hashlib` oraz z `bench.engines` dodatkowo `ballista_binary`;
2. `engine_versions` zastąp:

```python
def engine_versions(profile: str, *, ballista: bool) -> dict[str, str]:
    """Wersje silników i bibliotek przebiegu; dla serii z Ballistą — sumy SHA-256 binarek
    (dokładnie wiadomo, czym mierzono; przegląd 3a)."""
    lock = (BALLISTA_DIR / "Cargo.lock").read_text()

    def locked(name: str) -> str:
        m = re.search(rf'name = "{re.escape(name)}"\nversion = "([^"]+)"', lock)
        return m.group(1) if m else "unknown"

    versions = {
        "polars_bio": version("polars-bio"),
        "pysail": version("pysail"),
        "pyspark": version("pyspark"),
        "ballista": f"{locked('ballista')} ({profile})",
        "datafusion_ballista": locked("datafusion"),
        "datafusion_bio_function_ranges": locked("datafusion-bio-function-ranges"),
        "python": platform.python_version(),
    }
    if ballista:
        for name in ("ballista_node", "bench_client"):
            path = ballista_binary(name, profile)
            versions[f"{name}_sha256"] = (
                hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "missing"
            )
    return versions
```

3. w `_Series.__init__`: `self.versions = json.dumps(engine_versions(profile, ballista="ballista" in cfg.variants))`.

- [ ] **Step 3: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed` (binarki debug są świeże po Zadaniu 16; jeśli test klastra zgłosi „is older than”, przebuduj: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins`).

- [ ] **Step 4: Commit**

```bash
git add bench/engines.py bench/orchestrator.py tests/bench/test_engines.py tests/bench/test_orchestrator.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 3): straznik starej binarki Ballisty, SHA-256 binarek i wersja crate'a algorytmow

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

# Etap 4 — drobne poprawki (spec, sekcja 7) i build release

### Zadanie 18: Niezawodność serii — śmierć silnika w trakcie przebiegu, twarde zatrzymanie, ponawianie startu (pozycje 1–3)

**Files:**
- Modify: `bench/orchestrator.py` (`execute(..., dead=...)`, `measure`, `run_block`, stała `HARD_STOP_GRACE_S`)
- Modify: `bench/engines.py` (`dead()`, `stop(grace_s)`, `restart(grace_s)` we wszystkich silnikach; `_wait` łapie `ValueError`; ponawianie startu Saila)
- Test: `tests/bench/test_orchestrator.py`, `tests/bench/test_engines.py`

**Interfaces:**
- Produces: `execute(command, *, timeout_s, mem_floor, probe, scratch, dead: Callable[[], list[str]] = lambda: [])` — gdy `dead()` zwróci nazwy, runner jest zabijany od razu z przyczyną `process <nazwy> exited during the run`; `bench.orchestrator.HARD_STOP_GRACE_S = 3.0`; interfejs silnika: `dead() -> list[str]`, `stop(grace_s: float = ...)`, `restart(grace_s: float = ...)`; `bench.engines.BALLISTA_STOP_GRACE_S = 10.0`, `SAIL_STOP_GRACE_S = 30.0`, `SAIL_START_ATTEMPTS = 3`.

- [ ] **Step 1: Testy orkiestratora**

W `tests/bench/test_orchestrator.py`:
1. `FakeEngine` — w `__init__` dopisz `self.dead_once: Path | None = None` i `self.graces: list = []`; metodę `restart` zastąp, a metodę `dead` dodaj:

```python
    def restart(self, grace_s=None):
        self.log.append(("restart", self.variant, self.n))
        self.graces.append(grace_s)

    def dead(self):
        """Martwy proces silnika — raz, gdy runner utworzy plik `dead_once` (sygnał z testu)."""
        if self.dead_once is not None and self.dead_once.exists():
            self.dead_once.unlink()
            return ["executor_1"]
        return []
```

2. dopisz test:

```python
def test_engine_death_during_run_kills_runner_at_once(tmp_path):
    """Śmierć procesu silnika w trakcie przebiegu: runner przerwany od razu (nie po timeoucie),
    scenariusz pominięty do końca bloku, silnik zatrzymany twardo i uruchomiony od nowa."""
    marker = tmp_path / "executor_died"
    engines_by_variant = {}

    def args_for(variant, n, scenario):
        if (variant, scenario.op) == ("ballista", "overlap"):
            return ["--touch", str(marker), "--sleep", "60"]
        return []

    def customize(engine):
        engines_by_variant[engine.variant] = engine
        if engine.variant == "ballista":
            engine.dead_once = marker

    summary = run(tmp_path, cfg=config(timeout_s=120), factory=make_factory(args_for, customize))
    overlap = block(rows(summary), "ballista").filter(pl.col("op") == "overlap").sort("rep")
    died = "process executor_1 exited during the run"
    assert overlap["invalid_reason"].to_list() == [died, f"skipped: {died}", f"skipped: {died}"]
    assert overlap["wall_s"][0] < 30
    assert engines_by_variant["ballista"].graces == [orch.HARD_STOP_GRACE_S]
    assert block(rows(summary), "ballista").filter(pl.col("op") == "merge")["valid"].all()
```

Run: `uv run pytest tests/bench/test_orchestrator.py -k "death or dead or timeout" -v`
Expected: FAIL (runner czeka do timeoutu, `graces == [None]`).

- [ ] **Step 2: Implementacja w orkiestratorze**

1. Pod `MAX_BLOCK_ATTEMPTS` dodaj:

```python
#: Po timeoucie, strażniku pamięci albo śmierci procesu silnik jest restartowany; proces, który
#: nie zakończy się w tym czasie po SIGTERM, dostaje SIGKILL (przegląd 3a).
HARD_STOP_GRACE_S = 3.0
```

2. `execute` — sygnatura i pętla:

```python
def execute(command: Command, *, timeout_s: float, mem_floor: int, probe: Probe, scratch: Path,
            dead: Callable[[], list[str]] = lambda: []) -> Outcome:
    """Uruchamia runner z wyjściem do plików — potoki mogłyby zakleszczyć runner zalewający
    stderr — pilnując limitu czasu, strażnika pamięci i procesów silnika (`dead`: nazwy procesów,
    które padły); zabija całą grupę procesów runnera."""
```

a w pętli `while proc.poll() is None:` blok wyboru przyczyny zastąp:

```python
                if time.monotonic() - t0 > timeout_s:
                    killed = "timeout"
                else:
                    available = probe.mem_available()
                    if available < mem_floor:
                        killed = (
                            f"memory guard: MemAvailable {available // 2**20} MiB "
                            f"< {mem_floor // 2**20} MiB"
                        )
                    else:
                        gone = dead()
                        if gone:
                            killed = f"process {', '.join(gone)} exited during the run"
```

3. w `measure`: wywołanie `execute(...)` uzupełnij o `dead=engine.dead`, a blok

```python
        if dead:
            reasons.append(f"process {', '.join(dead)} exited during the run")
```

zastąp (bez dublowania przyczyny, gdy runner już został zabity z tego powodu):

```python
        if dead and outcome.killed is None:
            reasons.append(f"process {', '.join(dead)} exited during the run")
```

4. w `run_block` (funkcja `run`) `eng.restart()` → `eng.restart(grace_s=HARD_STOP_GRACE_S)`.

- [ ] **Step 3: Testy silników**

W `tests/bench/test_engines.py` dopisz (z importami `import os`, `import signal`, `import subprocess`, `import time`, jeśli ich brak):

```python
def test_stop_kills_a_process_that_ignores_sigterm(tmp_path):
    engine = engines.make_engine("ballista", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    proc = subprocess.Popen(
        [sys.executable, "-c",
         "import signal, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
         "print('ready', flush=True); time.sleep(60)"],
        stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout.readline().strip() == "ready"
    engine.procs = {"scheduler": proc}
    t0 = time.monotonic()
    engine.stop(grace_s=1)
    assert time.monotonic() - t0 < 10 and proc.poll() is not None


@pytest.mark.nodes(1)
def test_ballista_reports_a_dead_process(tmp_path):
    engine = engines.make_engine("ballista", 1, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    engine.start()
    try:
        assert engine.dead() == []
        os.kill(engine.server_pids()["executor_1"], signal.SIGKILL)
        deadline = time.monotonic() + 10
        while not engine.dead() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert engine.dead() == ["executor_1"]
    finally:
        engine.stop()


@pytest.mark.nodes(1)
def test_sail_start_retries_on_a_taken_port(tmp_path, monkeypatch):
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))  # zajęty bez listen(): serwer nie zbinduje portu i zakończy się
    taken = blocker.getsockname()[1]
    real_free_port = engines.free_port
    calls = itertools.count(1)
    monkeypatch.setattr(engines, "free_port", lambda: taken if next(calls) == 1 else real_free_port())
    engine = engines.make_engine("sail", 1, data_root=tmp_path, log_dir=tmp_path / "logs", profile="debug")
    try:
        engine.start()
        assert engine.port != taken and engine.dead() == []
    finally:
        engine.stop()
        blocker.close()


def test_polars_bio_engine_never_reports_dead_processes(tmp_path):
    engine = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert engine.dead() == []
    engine.restart(grace_s=1)
```

Run: `uv run pytest tests/bench/test_engines.py -k "sigterm or dead or retries" -v`
Expected: FAIL.

- [ ] **Step 4: Implementacja w `bench/engines.py`**

1. Pod `BALLISTA_START_ATTEMPTS` dodaj:

```python
#: Czas na zakończenie po SIGTERM przy zwykłym zatrzymaniu silnika (potem SIGKILL).
BALLISTA_STOP_GRACE_S = 10.0
SAIL_STOP_GRACE_S = 30.0
#: Próby startu serwera Saila (jak BALLISTA_START_ATTEMPTS; przegląd 3a: Sail nie ponawiał startu).
SAIL_START_ATTEMPTS = 3
```

2. W docstringu modułu, w opisie interfejsu, dopisz: „dead() → nazwy procesów silnika, które padły; stop(grace_s) i restart(grace_s) — SIGKILL po grace_s sekundach od SIGTERM”.
3. `PolarsBioEngine` — metody zastąp/dodaj:

```python
    def stop(self, grace_s: float = 0.0) -> None:
        pass

    def restart(self, grace_s: float = 0.0) -> None:
        pass

    def dead(self) -> list[str]:
        return []
```

4. `BallistaEngine`:
   - w `_wait` linię `except OSError:` zmień na `except (OSError, ValueError):` z komentarzem `# scheduler jeszcze nie odpowiada albo odpowiada niepełnym JSON-em`;
   - `stop` zastąp:

```python
    def stop(self, grace_s: float = BALLISTA_STOP_GRACE_S) -> None:
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + grace_s
        for proc in self.procs.values():
            try:
                proc.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        self.procs.clear()
        self.work_dirs.clear()
        if self._tmp is not None:
            shutil.rmtree(self._tmp, ignore_errors=True)
            self._tmp = None

    def restart(self, grace_s: float = BALLISTA_STOP_GRACE_S) -> None:
        self.stop(grace_s)
        self.start()

    def dead(self) -> list[str]:
        return [name for name, proc in self.procs.items() if proc.poll() is not None]
```

5. `SailEngine`: dotychczasowe ciało `start` przenieś do `_start_once` i dodaj nowe `start`; `stop`, `restart`, `dead` jak niżej:

```python
    def start(self) -> None:
        """Start serwera; po porażce (np. zajęty port) — od nowa, na nowym porcie, do
        SAIL_START_ATTEMPTS razy. Log kolejnych prób jest dopisywany."""
        for attempt in range(1, SAIL_START_ATTEMPTS + 1):
            try:
                self._start_once()
                return
            except EngineError:
                if attempt == SAIL_START_ATTEMPTS:
                    raise

    def stop(self, grace_s: float = SAIL_STOP_GRACE_S) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=grace_s)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None

    def restart(self, grace_s: float = SAIL_STOP_GRACE_S) -> None:
        self.stop(grace_s)
        self.start()

    def dead(self) -> list[str]:
        return ["sail_server"] if self.proc is not None and self.proc.poll() is not None else []
```

(`_start_once` = dawne `start` bez zmian: `wait_for_port` rzuca `EngineError`, a blok `except BaseException: self.stop(); raise` zostaje.)

- [ ] **Step 5: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 6: Commit**

```bash
git add bench/orchestrator.py bench/engines.py tests/bench/test_orchestrator.py tests/bench/test_engines.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): smierc silnika wykrywana w trakcie przebiegu, twarde zatrzymanie, ponawianie startu Saila

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 19: Sprawdzenia przed serią — `bench/preflight.py` (pozycja 4)

**Files:**
- Create: `bench/preflight.py`
- Modify: `bench/orchestrator.py` (`preflight` → `bench.preflight.run`)
- Test: `tests/bench/test_preflight.py` (nowy), `tests/bench/test_orchestrator.py`, `tests/test_orchestrator_integration.py`

**Interfaces:**
- Consumes: `bench.metrics.swap_used` (Zadanie 6), `bench.engines.SYSTEM_CPUS`, `cluster_cpus`, `require_ballista_binaries`.
- Produces: `bench.preflight.SYSFS_CPU: Path`; `check_cpus(n_nodes, *, affinity=None, sysfs=None) -> None`; `check_tools(variants) -> None`; `check_swap(used: int) -> None`; `run(cfg, profile) -> None` (wszystkie rzucają `ConfigError`, binarki — `EngineError`). `bench.orchestrator.preflight` = `bench.preflight.run`.

- [ ] **Step 1: Testy (`tests/bench/test_preflight.py`)**

```python
"""Sprawdzenia przed serią (plan 3b-2, Zadanie 19; przegląd 3a, pozycja 4): CPU węzłów dostępne
dla procesu, pary SMT zgodne z tabelą węzłów, taskset i pysail, pusty swap."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench import preflight
from bench.config import ConfigError


def sysfs(tmp_path: Path, siblings: dict[int, str]) -> Path:
    for cpu, text in siblings.items():
        topology = tmp_path / f"cpu{cpu}" / "topology"
        topology.mkdir(parents=True)
        (topology / "thread_siblings_list").write_text(text + "\n")
    return tmp_path


SMT_PAIRS = {0: "0-1", 2: "2-3", 4: "4-5", 6: "6-7"}


def test_cpus_of_the_node_layout_pass(tmp_path):
    preflight.check_cpus(3, affinity=set(range(12)), sysfs=sysfs(tmp_path, SMT_PAIRS))


def test_missing_cpus_in_affinity_are_reported(tmp_path):
    with pytest.raises(ConfigError, match=r"CPUs \[6, 7\] are not available"):
        preflight.check_cpus(3, affinity=set(range(6)), sysfs=sysfs(tmp_path, SMT_PAIRS))


def test_layout_without_smt_pairs_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="CPU 2 and 3 are not SMT siblings"):
        preflight.check_cpus(2, affinity=set(range(12)), sysfs=sysfs(tmp_path, {0: "0-1", 2: "2,8", 4: "4-5"}))


def test_unreadable_topology_is_reported(tmp_path):
    with pytest.raises(ConfigError, match="cannot read SMT topology of CPU 0"):
        preflight.check_cpus(1, affinity=set(range(12)), sysfs=tmp_path)


def test_missing_taskset_is_reported(monkeypatch):
    monkeypatch.setattr(preflight.shutil, "which", lambda name: None)
    with pytest.raises(ConfigError, match="taskset"):
        preflight.check_tools(["polars_bio_a"])


def test_pysail_is_required_only_for_sail_variants(monkeypatch):
    monkeypatch.setattr(preflight.importlib.util, "find_spec", lambda name: None)
    preflight.check_tools(["polars_bio_a", "ballista"])
    with pytest.raises(ConfigError, match="pysail"):
        preflight.check_tools(["sail_lock"])


def test_used_swap_is_rejected():
    preflight.check_swap(0)
    with pytest.raises(ConfigError, match=r"swap is not empty \(2 MiB used\)"):
        preflight.check_swap(2 * 2**20)


def test_this_machine_has_the_node_layout():
    """Maszyna pomiarowa (Ryzen 5 4600H, WSL2): rdzeń k = wątki {2k, 2k+1} (lscpu -e)."""
    if not (preflight.SYSFS_CPU / "cpu7").exists():
        pytest.skip("fewer than 8 CPUs (CI runner)")
    preflight.check_cpus(3, affinity=set(range(8)))
```

Run: `uv run pytest tests/bench/test_preflight.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 2: Implementacja `bench/preflight.py`**

```python
"""Sprawdzenia przed pierwszym pomiarem serii (specyfikacja 3; 3b-2, pozycja 4 z przeglądu 3a):
procesowi dostępne są CPU rdzenia systemowego i węzłów, te CPU tworzą pary SMT zgodne z tabelą
węzłów (rdzeń k = wątki {2k, 2k+1}), są taskset i pysail, swap jest pusty (3b-2, 4.3), a binarki
Ballisty istnieją i są świeże. Każdy błąd przerywa serię przed pierwszym pomiarem (kod 2)."""

from __future__ import annotations

import importlib.util
import os
import shutil
from pathlib import Path

from bench import metrics
from bench.config import ConfigError, SeriesConfig
from bench.engines import SYSTEM_CPUS, cluster_cpus, require_ballista_binaries

SYSFS_CPU = Path("/sys/devices/system/cpu")


def _cpu_list(text: str) -> set[int]:
    """Lista CPU w zapisie jądra, np. '0-1' albo '2,8' → zbiór numerów."""
    cpus: set[int] = set()
    for part in text.strip().split(","):
        first, _, last = part.partition("-")
        cpus.update(range(int(first), int(last or first) + 1))
    return cpus


def check_cpus(n_nodes: int, *, affinity: set[int] | None = None, sysfs: Path | None = None) -> None:
    needed = {*SYSTEM_CPUS, *cluster_cpus(n_nodes)}
    affinity = os.sched_getaffinity(0) if affinity is None else affinity
    missing = sorted(needed - set(affinity))
    if missing:
        raise ConfigError(
            f"CPUs {missing} are not available to this process (N = {n_nodes} needs CPUs "
            f"0-{max(needed)}); check the CPU affinity of the shell"
        )
    sysfs = SYSFS_CPU if sysfs is None else sysfs
    for core in range(n_nodes + 1):  # rdzeń 0 — system, 1..N — węzły
        first = 2 * core
        path = sysfs / f"cpu{first}" / "topology" / "thread_siblings_list"
        try:
            siblings = _cpu_list(path.read_text())
        except (OSError, ValueError) as e:
            raise ConfigError(f"cannot read SMT topology of CPU {first}: {e}") from e
        if siblings != {first, first + 1}:
            raise ConfigError(
                f"CPU {first} and {first + 1} are not SMT siblings (siblings of CPU {first}: "
                f"{sorted(siblings)}) - the node layout of the methodology spec (section 3) does "
                "not hold on this machine"
            )


def check_tools(variants) -> None:
    if shutil.which("taskset") is None:
        raise ConfigError("taskset not found (package util-linux) - it pins engines to emulated nodes")
    if any(v.startswith("sail") for v in variants) and importlib.util.find_spec("pysail") is None:
        raise ConfigError("pysail is not importable - run 'uv sync'")


def check_swap(used: int) -> None:
    if used > 0:
        raise ConfigError(
            f"swap is not empty ({used // 2**20} MiB used) - a run touching swap is invalid, so "
            "a series starts from empty swap: run 'wsl --shutdown' in Windows and reopen the terminal"
        )


def run(cfg: SeriesConfig, profile: str) -> None:
    check_cpus(max(cfg.nodes))
    check_swap(metrics.swap_used())
    check_tools(cfg.variants)
    if "ballista" in cfg.variants:
        require_ballista_binaries(profile)
```

- [ ] **Step 3: Orkiestrator i testy wywołujące `main`**

`bench/orchestrator.py`:
1. import `from bench import metrics, preflight as preflight_checks`;
2. funkcję `preflight` zastąp:

```python
def preflight(cfg: SeriesConfig, profile: str) -> None:
    """Sprawdzenia przed pierwszym pomiarem (bench/preflight.py)."""
    preflight_checks.run(cfg, profile)
```

3. usuń nieużywany już import `cluster_cpus` z `bench.engines`, jeśli nic innego go nie używa (`grep -n cluster_cpus bench/orchestrator.py`).

W `tests/bench/test_orchestrator.py`: w `test_main_fails_fast_without_ballista_binaries` i `test_main_refuses_to_start_with_used_swap` dopisz na początku ciała `monkeypatch.setattr(preflight, "check_cpus", lambda *args, **kwargs: None)` (z importem `from bench import preflight`) — runner CI ma inną topologię. To samo w `tests/test_orchestrator_integration.py::test_main_resolves_relative_data_root` (import `from bench import preflight`).

- [ ] **Step 4: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS (na tej maszynie także `test_this_machine_has_the_node_layout`).
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 5: Commit**

```bash
git add bench/preflight.py bench/orchestrator.py tests/bench/test_preflight.py tests/bench/test_orchestrator.py tests/test_orchestrator_integration.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): sprawdzenia przed seria - CPU i pary SMT, taskset, pysail, swap

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 20: Środowisko procesów silników — czyszczenie odziedziczonych zmiennych i `env.json` (pozycja 5)

**Files:**
- Modify: `bench/engines.py` (`INHERITED_PREFIXES`, `base_env`, `_python`, środowiska silników, `env_record`)
- Modify: `bench/orchestrator.py` (zapis `env.json`)
- Test: `tests/bench/test_engines.py`, `tests/bench/test_orchestrator.py`

**Interfaces:**
- Produces: `bench.engines.INHERITED_PREFIXES`, `base_env() -> dict[str, str]`; interfejs silnika `env_record() -> dict[str, str]` (zmienne ustawione przez narzędzie: polars-bio — `POLARS_MAX_THREADS`; Ballista — `BIO_TARGET_PARTITIONS`, `BALLISTA_GRPC_MAX_MESSAGE_BYTES`; Sail — `sail_env(n)` i `POLARS_MAX_THREADS = 2N`); plik `<katalog serii>/env.json`: `{etykieta bloku: env_record, "control": env_record przebiegu kontrolnego}`.

- [ ] **Step 1: Testy**

`tests/bench/test_engines.py` — dopisz:

```python
def test_inherited_engine_variables_are_dropped(tmp_path, monkeypatch):
    for name, value in {"SAIL_MODE": "local", "POLARS_MAX_THREADS": "12", "RUST_LOG": "warn",
                        "BIO_TARGET_PARTITIONS": "8", "BALLISTA_GRPC_MAX_MESSAGE_BYTES": "1"}.items():
        monkeypatch.setenv(name, value)
    env = engines.base_env()
    assert not {"SAIL_MODE", "POLARS_MAX_THREADS", "RUST_LOG", "BIO_TARGET_PARTITIONS",
                "BALLISTA_GRPC_MAX_MESSAGE_BYTES"} & set(env)
    assert env["MPLBACKEND"] == "Agg" and env["PATH"] == os.environ["PATH"]
    pb_env = engines.make_engine("polars_bio_a", 1, data_root=tmp_path, log_dir=tmp_path, profile="debug").command(
        Scenario("overlap", "1-2")).env
    assert pb_env["POLARS_MAX_THREADS"] == "2" and "SAIL_MODE" not in pb_env and "RUST_LOG" not in pb_env
    ballista = engines.make_engine("ballista", 2, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert ballista.env["BIO_TARGET_PARTITIONS"] == "4" and "RUST_LOG" not in ballista.env


def test_env_records_list_what_the_tool_sets(tmp_path):
    make = lambda v, n: engines.make_engine(v, n, data_root=tmp_path, log_dir=tmp_path, profile="debug")
    assert make("polars_bio_b", 3).env_record() == {"POLARS_MAX_THREADS": "6"}
    assert make("ballista", 2).env_record() == {
        "BIO_TARGET_PARTITIONS": "4", "BALLISTA_GRPC_MAX_MESSAGE_BYTES": "536870912",
    }
    sail = make("sail_lock", 3).env_record()
    assert sail["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] == "3" and sail["POLARS_MAX_THREADS"] == "6"
```

W `test_sail_server_is_pinned_and_configured` do zbioru oczekiwanych zmiennych dopisz `"POLARS_MAX_THREADS=4",`.

`tests/bench/test_orchestrator.py`:
1. w `FakeEngine` dodaj:

```python
    def env_record(self):
        return {"ENGINE": f"{self.variant}-{self.n}"}
```

2. dopisz test:

```python
def test_series_records_engine_environment(tmp_path):
    summary = run(tmp_path)
    assert json.loads((summary.directory / "env.json").read_text()) == {
        "control": {"ENGINE": "polars_bio_a-1"},
        "polars_bio_a N=1": {"ENGINE": "polars_bio_a-1"},
        "ballista N=1": {"ENGINE": "ballista-1"},
    }
```

Run: `uv run pytest tests/bench/test_engines.py tests/bench/test_orchestrator.py -k "env" -v`
Expected: FAIL.

- [ ] **Step 2: Implementacja w `bench/engines.py`**

1. Pod `SAIL_START_ATTEMPTS` dodaj:

```python
#: Zmienne sterujące silnikami, usuwane ze środowiska odziedziczonego po powłoce: wartość
#: wyeksportowana np. w .bashrc (SAIL_MODE, POLARS_MAX_THREADS, RUST_LOG…) zmieniłaby cicho
#: konfigurację przebiegu (przegląd 3a). Silnik ustawia potrzebne sam — lista w env_record().
INHERITED_PREFIXES = (
    "SAIL_", "BALLISTA_", "POLARS_", "BIO_", "SPARK_", "PYSPARK_", "RUST_LOG", "DIST_OUTPUT_DIR",
)


def base_env() -> dict[str, str]:
    """Środowisko procesów silników i runnerów: odziedziczone bez zmiennych sterujących
    silnikami, z backendem matplotlib bez okien (bench/__init__.py)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(INHERITED_PREFIXES)}
    env["MPLBACKEND"] = "Agg"
    return env
```

2. `_python`: `{**os.environ, "MPLBACKEND": "Agg", **env}` → `{**base_env(), **env}`.
3. `PolarsBioEngine` — dodaj:

```python
    def env_record(self) -> dict[str, str]:
        return {"POLARS_MAX_THREADS": str(self.threads)}
```

4. `BallistaEngine.__init__`: `**os.environ,` → `**base_env(),`; dodaj:

```python
    def env_record(self) -> dict[str, str]:
        keys = ("BIO_TARGET_PARTITIONS", "BALLISTA_GRPC_MAX_MESSAGE_BYTES")
        return {k: self.env[k] for k in keys}
```

5. `SailEngine`: w `_start_once` środowisko serwera `{**os.environ, "MPLBACKEND": "Agg", **sail_env(self.n)}` → `{**base_env(), **self.env_record()}`; dodaj:

```python
    def env_record(self) -> dict[str, str]:
        """Zmienne SAIL_* (local-cluster, N workerów) i jawna liczba wątków polars-bio w procesie
        serwera — 2N, jak rdzenie węzłów 1..N (przegląd 3a: serwer bez jawnego POLARS_MAX_THREADS)."""
        return {**sail_env(self.n), "POLARS_MAX_THREADS": str(SLOTS_PER_NODE * self.n)}
```

6. W docstringu modułu, w opisie interfejsu, dopisz „env_record() → zmienne środowiska ustawione przez narzędzie (zapis w env.json serii)”.

- [ ] **Step 3: Implementacja w `bench/orchestrator.py`**

W `_Series.__init__` dopisz `self.env_records: dict[str, dict[str, str]] = {}` i metodę:

```python
    def record_env(self, label: str, engine) -> None:
        """Zmienne środowiska ustawione przez narzędzie dla bloku → env.json serii (przegląd 3a)."""
        self.env_records[label] = engine.env_record()
        (self.directory / "env.json").write_text(json.dumps(self.env_records, indent=2, sort_keys=True))
```

W `run_block`, zaraz po utworzeniu `engine` i `control`:

```python
        self.record_env("control", control)
        self.record_env(block.label, engine)
```

- [ ] **Step 4: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 5: Commit**

```bash
git add bench/engines.py bench/orchestrator.py tests/bench/test_engines.py tests/bench/test_orchestrator.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): srodowisko silnikow bez odziedziczonych zmiennych sterujacych, env.json serii

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 21: Postęp na bieżąco, stderr nieudanych przebiegów, `base_rss` (pozycje 6, 7, 9)

**Files:**
- Modify: `bench/orchestrator.py` (`_print`, `_kind`, `_progress`, `_start_line`, `measure(..., label)`, `Probe.rss`, `_METRIC_KEYS`)
- Modify: `bench/metrics.py` (`rss`), `bench/results.py` (`base_rss`)
- Test: `tests/bench/test_orchestrator.py`, `tests/bench/test_metrics.py`, `tests/bench/test_results.py`

**Interfaces:**
- Produces: `bench.metrics.rss(pid, proc=PROC) -> int` (VmRSS w bajtach); `Probe.rss`; kolumna `base_rss` (JSON: proces długożyjący → RSS tuż po wyzerowaniu licznika szczytu); `run_series(..., out=_print)` (domyślnie `print(..., flush=True)`); linia startu przebiegu kończy się słowem `start`; stderr nieudanych przebiegów: `<katalog serii>/logs/failed/NNNN-<wariant>-n<N>-<op>_<dane>.stderr.txt`.

- [ ] **Step 1: Testy**

`tests/bench/test_metrics.py` — dopisz:

```python
def test_rss_reads_vmrss_in_bytes(fake_proc):
    assert metrics.rss(4242, fake_proc) == 100000 * 1024
```

`tests/bench/test_results.py` — w `full_row` dopisz `base_rss=None,`.

`tests/bench/test_orchestrator.py`:
1. w `probe(**overrides)` dopisz do słownika `rss=lambda pid: 1000,`;
2. w helperze `run` zastąp ciało:

```python
def run(tmp_path, cfg=None, **kwargs):
    kwargs.setdefault("factory", make_factory())
    kwargs.setdefault("probe", probe())
    kwargs.setdefault("out", lambda line: None)
    return orch.run_series(cfg or config(), data_root=tmp_path, results_dir=tmp_path / "results", **kwargs)
```

3. dopisz testy:

```python
def test_every_run_is_announced_before_it_starts(tmp_path):
    lines: list[str] = []
    summary = run(tmp_path, out=lines.append)
    starts = [line for line in lines if line.endswith(" start")]
    assert len(starts) == rows(summary).height
    assert lines[1].startswith("polars_bio_ref") and lines[1].endswith(" start")
    assert lines[2].startswith("polars_bio_ref") and lines[2].endswith("OK")


def test_stderr_of_failed_runs_is_kept(tmp_path):
    def args_for(variant, n, scenario):
        return ["--exit", "1"] if (variant, scenario.op) == ("ballista", "overlap") else []

    summary = run(tmp_path, factory=make_factory(args_for))
    kept = sorted((summary.directory / "logs" / "failed").iterdir())
    assert len(kept) == 3  # rozgrzewka i dwie rundy
    assert kept[0].name.endswith("-ballista-n1-overlap_1-2.stderr.txt")
    assert "engine crashed: test error" in kept[0].read_text()


def test_base_rss_of_long_lived_processes_is_recorded(tmp_path):
    def customize(engine):
        if engine.variant == "ballista":
            engine.pids = {"executor_1": 4242}

    df = rows(run(tmp_path, factory=make_factory(customize=customize)))
    assert set(block(df, "ballista")["base_rss"]) == {json.dumps({"executor_1": 1000})}
    assert block(df, "polars_bio_a")["base_rss"].is_null().all()
```

Run: `uv run pytest tests/bench/test_orchestrator.py tests/bench/test_metrics.py tests/bench/test_results.py -v`
Expected: FAIL.

- [ ] **Step 2: Implementacja**

`bench/metrics.py` — pod `peak_rss`:

```python
def rss(pid: int | str = "self", proc: Path = PROC) -> int:
    """Bieżąca pamięć rezydentna procesu (VmRSS) w bajtach."""
    return _kib_field(proc / str(pid) / "status", "VmRSS")
```

`bench/results.py` — pod `"peak_rss_sum": ...`:

```python
    "base_rss": pl.Utf8,  # JSON: proces długożyjący -> RSS tuż po wyzerowaniu szczytu (plan 3b-2)
```

i w komentarzu nad `SCHEMA` dopisz „, base_rss”.

`bench/orchestrator.py`:
1. `Probe` — pole `rss: Callable[[int], int] = metrics.rss`;
2. `_METRIC_KEYS` — dopisz `"base_rss"` po `"peak_rss_sum"`;
3. nad `_progress` dodaj i `_progress` zastąp:

```python
def _print(line: str) -> None:
    """Postęp serii na bieżąco — także przy przekierowaniu do pliku (przegląd 3a)."""
    print(line, flush=True)


def _kind(*, reference: bool = False, position: str | None = None, warmup: bool = False, rep: int = 0) -> str:
    if reference:
        return "reference"
    if position is not None:
        return f"control {position} {rep}"
    return "warmup" if warmup else f"round {rep}"


def _start_line(variant: str, n_nodes: int, kind: str, scenario_id: str) -> str:
    return f"{variant:<14} N={n_nodes} {kind:<16} {scenario_id:<14}   start"


def _progress(row: dict) -> str:
    kind = _kind(reference=row["is_reference"], position=row["control_position"],
                 warmup=row["is_warmup"], rep=row["rep"])
    t = f"{row['t_total_s']:9.3f} s" if row["t_total_s"] is not None else "        – s"
    status = "OK" if row["valid"] else f"INVALID: {row['invalid_reason']}"
    return f"{row['variant']:<14} N={row['n_nodes']} {kind:<16} {row['scenario_id']:<14} {t}  {status}"
```

4. `run_series`: domyślny argument `out: Callable[[str], None] = print` → `out: Callable[[str], None] = _print`;
5. `_Series.__init__`: dopisz `self.failed_logs = 0`;
6. `measure` — sygnatura `def measure(self, engine, scenario: Scenario, expected: Expected | None, label: str)`; pętlę zerowania szczytów zastąp:

```python
        base = {}
        for name, pid in pids.items():
            try:
                self.probe.reset_peak_rss(pid)
                base[name] = self.probe.rss(pid)
            except OSError:
                pass  # martwy proces wykryje odczyt szczytu po przebiegu
```

po wyznaczeniu `reasons` (przed `peak = ...`) dodaj:

```python
        if reasons and outcome.stderr.strip():
            self.failed_logs += 1
            path = (self.directory / "logs" / "failed"
                    / f"{self.failed_logs:04d}-{label}-{scenario.id.replace('/', '_')}.stderr.txt")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(outcome.stderr)
```

a w słowniku `measured` po `"peak_rss_sum": ...` dopisz `"base_rss": json.dumps(base) if base else None,`;
7. `reference_pass`: przed `measured, _ = self.measure(...)` dodaj `self.out(_start_line(REFERENCE_VARIANT, 1, "reference", scenario.id))` i wywołanie zmień na `self.measure(engine, scenario, None, f"{REFERENCE_VARIANT}-n1")`;
8. `run_block`, funkcja `run`: w gałęzi `else:` (pomiar) przed `measured, restart = self.measure(...)` dodaj

```python
                self.out(_start_line(variant, n, _kind(position=position, warmup=warmup, rep=rep), scenario.id))
```

i wywołanie zmień na `self.measure(eng, scenario, expected[scenario.id], f"{variant}-n{n}")`.

- [ ] **Step 3: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 4: Commit**

```bash
git add bench/orchestrator.py bench/metrics.py bench/results.py tests/bench/test_orchestrator.py tests/bench/test_metrics.py tests/bench/test_results.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): postep na biezaco z linia startu, stderr nieudanych przebiegow, base_rss

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 22: Automatyczne zaliczenie serii — `expected_invalid` i podsumowanie OK/FAILED (pozycja 17)

**Files:**
- Modify: `bench/config.py` (`ExpectedInvalid`, `expected_invalid`), `bench/orchestrator.py` (`SeriesSummary`, `_Series.write`, `summary_lines`, `main`)
- Test: `tests/bench/test_config.py`, `tests/bench/test_orchestrator.py`

**Interfaces:**
- Produces: `bench.config.ExpectedInvalid(scenario: str, variant: str, nodes: int | None = None, reason: str | None = None)` z `matches(row: dict) -> bool`; `SeriesConfig.expected_invalid: tuple[ExpectedInvalid, ...]` (domyślnie pusta); `SeriesSummary.expected: int`, `SeriesSummary.unexpected: list[str]`, `ok` = brak nieoczekiwanych nieważnych i brak niepowodzeń; `bench.orchestrator.summary_lines(series: str, summary) -> list[str]` (pierwsza linia: `<seria> OK: …` albo `<seria> FAILED: …`).

- [ ] **Step 1: Testy**

`tests/bench/test_config.py`:
1. w `test_minimal_config_gets_spec_defaults` dopisz `assert cfg.expected_invalid == ()`;
2. do parametrów błędów dopisz:

```python
        ({"expected_invalid": "all"}, "expected_invalid: expected a list"),
        ({"expected_invalid": [{"scenario": "overlap/1-2"}]}, r"expected_invalid\[0\]: missing keys"),
        ({"expected_invalid": [{"scenario": "nearest/1-2", "variant": "sail"}]}, "not a scenario of this series"),
        ({"expected_invalid": [{"scenario": "merge/1", "variant": "spark"}]}, "unknown variant"),
        ({"expected_invalid": [{"scenario": "merge/1", "variant": "sail", "x": 1}]}, "unknown keys"),
        ({"expected_invalid": [{"scenario": "merge/1", "variant": "sail", "nodes": 4}]}, "nodes"),
```

3. dopisz:

```python
def test_expected_invalid_entries_match_rows():
    cfg = parse(raw(expected_invalid=[
        {"scenario": "merge/1", "variant": "ballista", "nodes": 3, "reason": "timeout"},
    ]))
    (entry,) = cfg.expected_invalid
    row = {"scenario_id": "merge/1", "variant": "ballista", "n_nodes": 3, "invalid_reason": "timeout"}
    assert entry.matches(row)
    assert not entry.matches({**row, "n_nodes": 1})
    assert not entry.matches({**row, "invalid_reason": "skipped: timeout"})
    assert not entry.matches({**row, "variant": "sail"})
```

`tests/bench/test_orchestrator.py` — dopisz:

```python
def _wrong_ballista_merge(variant, n, scenario):
    return ["--checksum", "0x00000000000000bb"] if (variant, scenario.op) == ("ballista", "merge") else []


def test_expected_invalid_runs_do_not_fail_the_series(tmp_path):
    cfg = config(expected_invalid=[{"scenario": "merge/1", "variant": "ballista", "reason": "checksum"}])
    summary = run(tmp_path, cfg=cfg, factory=make_factory(_wrong_ballista_merge))
    assert summary.ok and (summary.invalid, summary.expected, summary.unexpected) == (3, 3, [])
    assert orch.summary_lines("test", summary)[0].startswith("test OK: runs 18, invalid 3 (expected 3)")


def test_unexpected_invalid_runs_fail_the_series_and_are_listed(tmp_path):
    summary = run(tmp_path, factory=make_factory(_wrong_ballista_merge))
    assert not summary.ok and summary.expected == 0 and len(summary.unexpected) == 3
    lines = orch.summary_lines("test", summary)
    assert lines[0].startswith("test FAILED:")
    assert lines[1] == (
        "  unexpected: ballista N=1 merge/1: checksum 0x00000000000000bb ≠ reference 0x00000000000000aa"
    )
```

Run: `uv run pytest tests/bench/test_config.py tests/bench/test_orchestrator.py -k "expected" -v`
Expected: FAIL.

- [ ] **Step 2: Implementacja — `bench/config.py`**

1. Pod klasą `Block` dodaj:

```python
@dataclass(frozen=True)
class ExpectedInvalid:
    """Oczekiwany nieważny przebieg (specyfikacja 3b-2, pozycja 17): seria z takim przebiegiem
    nadal jest zaliczona (np. smoke z udokumentowanym ograniczeniem silnika)."""

    scenario: str
    variant: str
    nodes: int | None = None
    #: Początek przyczyny nieważności; None — dowolna.
    reason: str | None = None

    def matches(self, row: dict) -> bool:
        return (
            row["scenario_id"] == self.scenario
            and row["variant"] == self.variant
            and (self.nodes is None or row["n_nodes"] == self.nodes)
            and (self.reason is None or (row["invalid_reason"] or "").startswith(self.reason))
        )
```

2. `DEFAULTS` — dopisz `"expected_invalid": [],`;
3. `SeriesConfig` — pole `expected_invalid: tuple[ExpectedInvalid, ...]`;
4. nad `parse` dodaj:

```python
_EXPECTED_KEYS = {"scenario", "variant", "nodes", "reason"}


def _expected_invalid(raw_list, scenario_ids: set[str]) -> tuple[ExpectedInvalid, ...]:
    if not isinstance(raw_list, list):
        raise ConfigError(f"expected_invalid: expected a list, got {raw_list!r}")
    out = []
    for i, item in enumerate(raw_list):
        where = f"expected_invalid[{i}]"
        if not isinstance(item, dict):
            raise ConfigError(f"{where}: expected a mapping, got {item!r}")
        unknown = set(item) - _EXPECTED_KEYS
        if unknown:
            raise ConfigError(f"{where}: unknown keys {sorted(unknown)}")
        missing = [k for k in ("scenario", "variant") if k not in item]
        if missing:
            raise ConfigError(f"{where}: missing keys {missing}")
        if item["scenario"] not in scenario_ids:
            raise ConfigError(f"{where}: {item['scenario']!r} is not a scenario of this series")
        if item["variant"] not in VARIANTS:
            raise ConfigError(f"{where}: unknown variant {item['variant']!r}")
        nodes = item.get("nodes")
        if nodes is not None and (isinstance(nodes, bool) or nodes not in NODES):
            raise ConfigError(f"{where}: nodes from {NODES}, got {nodes!r}")
        reason = item.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise ConfigError(f"{where}: reason must be text, got {reason!r}")
        out.append(ExpectedInvalid(item["scenario"], item["variant"], nodes, reason))
    return tuple(out)
```

5. w `parse` przed `cfg = SeriesConfig(...)`:

```python
    control = _scenario(raw["control"], "control")
    expected_invalid = _expected_invalid(raw["expected_invalid"], {*ids, control.id})
```

i w `SeriesConfig(...)`: `control=control,` (zamiast `_scenario(...)` w miejscu) oraz `expected_invalid=expected_invalid,`.

- [ ] **Step 3: Implementacja — `bench/orchestrator.py`**

1. `SeriesSummary` zastąp:

```python
@dataclass
class SeriesSummary:
    directory: Path
    parquet: Path
    rows: int = 0
    invalid: int = 0
    #: Nieważne przebiegi z listy expected_invalid konfiguracji.
    expected: int = 0
    #: Pozostałe nieważne przebiegi: „wariant N=n scenariusz: przyczyna”.
    unexpected: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.unexpected and not self.failures
```

2. `_Series.write` zastąp:

```python
    def write(self, rows: list[dict]) -> None:
        for row in rows:
            self.writer.write(row)
            self.summary.rows += 1
            if row["valid"]:
                continue
            self.summary.invalid += 1
            if any(e.matches(row) for e in self.cfg.expected_invalid):
                self.summary.expected += 1
            else:
                self.summary.unexpected.append(
                    f"{row['variant']} N={row['n_nodes']} {row['scenario_id']}: {row['invalid_reason']}"
                )
```

3. nad `main` dodaj:

```python
#: Ile nieoczekiwanych nieważnych przebiegów wypisać w podsumowaniu.
SHOWN_UNEXPECTED = 20


def summary_lines(series: str, summary: SeriesSummary) -> list[str]:
    """Podsumowanie serii: zaliczona (OK) albo nie (FAILED) i lista przyczyn (specyfikacja 3b-2,
    pozycja 17 — automatyczne zaliczenie smoke)."""
    status = "OK" if summary.ok else "FAILED"
    lines = [
        f"{series} {status}: runs {summary.rows}, invalid {summary.invalid} (expected "
        f"{summary.expected}), failures {len(summary.failures)}; results: {summary.parquet}"
    ]
    lines += [f"  unexpected: {u}" for u in summary.unexpected[:SHOWN_UNEXPECTED]]
    if len(summary.unexpected) > SHOWN_UNEXPECTED:
        lines.append(f"  ... and {len(summary.unexpected) - SHOWN_UNEXPECTED} more unexpected invalid runs")
    lines += [f"  failure: {f}" for f in summary.failures]
    return lines
```

4. w `main` końcowe `print(f"runs: ...")` zastąp pętlą `for line in summary_lines(cfg.series, summary): print(line)`, a w docstringu modułu opis kodów wyjścia: „0 — seria zaliczona (każdy nieważny przebieg jest na liście expected_invalid); 1 — nieoczekiwane nieważne przebiegi albo przerwane bloki; 2 — …”.

- [ ] **Step 4: Uruchom — PASS**

Run: `uv run pytest tests/bench -v`
Expected: PASS.
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

- [ ] **Step 5: Commit**

```bash
git add bench/config.py bench/orchestrator.py tests/bench/test_config.py tests/bench/test_orchestrator.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): automatyczne zaliczenie serii (expected_invalid, podsumowanie OK/FAILED)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 23: Testy i CI — żywy runner przy przerwaniu, `nearest` local-vs-dist, kontrola pozytywna LATERAL, `protoc` z SHA-256, pełna wersja Pythona (pozycje 12–16)

**Files:**
- Modify: `tests/bench/test_orchestrator.py`, `tests/test_ballista_distributed_ops.py`, `tests/test_sail_parallel_udtf.py`, `.github/workflows/tests.yml`, `.python-version`, `bench/__init__.py`, `tests/bench/test_env.py`

**Interfaces:**
- Produces: `bench.PROJECT_PYTHON` = krotka z `.python-version` (np. `(3, 12, 15)`); `require_project_python(version_info)` porównuje pełną wersję.

- [ ] **Step 1: Pozycja 12 — żywy runner przy przerwaniu (`tests/bench/test_orchestrator.py`)**

```python
def test_interrupt_during_a_run_kills_the_running_runner(tmp_path):
    """Ctrl-C (KeyboardInterrupt) w trakcie przebiegu: grupa procesów runnera jest zabijana —
    żaden runner nie zostaje na rdzeniach węzłów (przegląd 3a)."""
    started = tmp_path / "runner_started"

    def args_for(variant, n, scenario):
        return ["--touch", str(started), "--sleep", "60"] if variant == "ballista" else []

    def mem_available():
        if started.exists():
            raise KeyboardInterrupt
        return 4 << 30

    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, factory=make_factory(args_for), probe=probe(mem_available=mem_available))
    deadline = time.monotonic() + 10
    while [p for p in Path("/proc").glob("[0-9]*") if _cmdline_contains(p, str(started))]:
        assert time.monotonic() < deadline, "runner still alive after the interrupt"
        time.sleep(0.1)
```

(Brak wartości w kolumnie chromosomu w sumie kontrolnej jest już pokryty: `tests/bench/test_checksum.py::test_accumulator_matches_definition_on_mixed_types` losuje `None` w `chrom` — decyzja planu 12.)

Run: `uv run pytest tests/bench/test_orchestrator.py -k interrupt -v` → PASS (zachowanie istnieje; test je przypina).

- [ ] **Step 2: Pozycja 13 — `nearest` local-vs-dist bez zależności od innego testu**

W `tests/test_ballista_distributed_ops.py`, w `test_distributed_nearest_agrees_with_local_nearest`, zastąp blok

```python
    local_csv = OUTPUT_DIR / "nearest_local_result.csv"
    if not local_csv.exists():
        pytest.skip(...)
```

przez:

```python
    # Wynik lokalny liczony tutaj, a nie brany z pliku innego testu — w CI kolejność testów
    # pomijała ten test (przegląd 3b-1).
    local_binary = BALLISTA_DIR / "target" / "debug" / "nearest_local"
    assert local_binary.exists(), "build it: cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins"
    local = subprocess.run([str(local_binary)], cwd=BALLISTA_DIR, capture_output=True, text=True, timeout=60)
    assert local.returncode == 0, local.stderr[-3000:]
    local_csv = OUTPUT_DIR / "nearest_local_result.csv"
```

Run: `uv run pytest tests/test_ballista_distributed_ops.py -k nearest -v -rs` → PASS, bez `SKIPPED`.

- [ ] **Step 3: Pozycja 14 — kontrola pozytywna regresji LATERAL (`tests/test_sail_parallel_udtf.py`)**

Na końcu pliku dopisz:

```python
def test_lateral_udtf_output_named_differently_from_outer_column():
    """Kontrola pozytywna do testu xfail wyżej: ten sam kształt zapytania z inną nazwą kolumny
    wyniku przechodzi — xfail przypina więc konkretną regresję Saila 0.7.2, a nie dowolny błąd
    ścieżki LATERAL."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udtf
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start(background=True)
    ip, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://{ip}:{port}").create()
    try:
        @udtf(returnType="out_chrom: string, start: long, end: long")
        class Echo:
            def eval(self, chrom, rows):
                for r in rows:
                    yield (chrom, int(r["start"]), int(r["end"]))

        spark.udtf.register("echo", Echo)
        rows = spark.sql(
            "SELECT o.* FROM (SELECT 'chr1' AS chrom, array(named_struct('start', 1L, 'end', 2L)) AS rows) g, "
            "LATERAL echo(g.chrom, g.rows) o"
        ).collect()
        assert [tuple(r) for r in rows] == [("chr1", 1, 2)]
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        server.stop()
```

Run: `uv run pytest tests/test_sail_parallel_udtf.py -k lateral -v` → 1 passed, 1 xfailed.

- [ ] **Step 4: Pozycja 15 — `protoc` w CI z `curl -f` i SHA-256**

W `.github/workflows/tests.yml` krok `Install protoc` zastąp:

```yaml
      - name: Install protoc
        env:
          PROTOC_SHA256: 0ad949f04a6a174da83cdcbdb36dee0a4925272a5b6d83f79a6bf9852076d53f
        run: |
          curl -fsSL -o "$RUNNER_TEMP/protoc.zip" https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip
          echo "$PROTOC_SHA256  $RUNNER_TEMP/protoc.zip" | sha256sum -c -
          sudo unzip -q -o "$RUNNER_TEMP/protoc.zip" -d /usr/local bin/protoc 'include/*'
          protoc --version
```

- [ ] **Step 5: Pozycja 16 — pełna wersja Pythona**

1. `.python-version`: `3.12` → `3.12.15`.
2. `bench/__init__.py` — blok od `PROJECT_PYTHON = (3, 12)` do końca `require_project_python` zastąp:

```python
from pathlib import Path

#: Python środowiska projektu — pełna wersja z .python-version (plan 3b-2; uv pobiera dokładnie
#: tę). Systemowy python3 (3.10) widzi starsze biblioteki z ~/.local — polars-bio 0.28 z błędem
#: #372, Sail 0.5.3 — więc seria uruchomiona nim po cichu mieszałaby wersje silników w wynikach;
#: inna wersja 3.12.x to inne środowisko niż to, w którym zmierzono pozostałe wyniki.
PROJECT_PYTHON = tuple(
    int(part)
    for part in (Path(__file__).resolve().parent.parent / ".python-version").read_text().split()[0].split(".")
)


def require_project_python(version_info) -> None:
    """ImportError, gdy interpreter nie jest Pythonem środowiska projektu (uv)."""
    if tuple(version_info[: len(PROJECT_PYTHON)]) != PROJECT_PYTHON:
        found = ".".join(str(part) for part in version_info[:3])
        wanted = ".".join(str(part) for part in PROJECT_PYTHON)
        raise ImportError(
            f"bench needs Python {wanted} from the project environment, found {found}; run it "
            f"through uv, e.g. 'uv run python -m bench.orchestrator ...' or 'uv run pytest'"
        )
```

(import `from pathlib import Path` umieść przy pozostałych importach na górze pliku).
3. `tests/bench/test_env.py` — dwa ostatnie testy zastąp:

```python
def test_bench_rejects_a_foreign_python():
    """Systemowy python3 (3.10) widzi starsze biblioteki z ~/.local — polars-bio 0.28 z błędem
    #372, Sail 0.5.3. Seria uruchomiona nim po cichu mieszałaby wersje silników w wynikach."""
    with pytest.raises(ImportError, match=r"Python 3\.12\.15.*found 3\.10\.12.*uv run"):
        bench.require_project_python((3, 10, 12, "final", 0))


def test_bench_rejects_another_patch_release():
    with pytest.raises(ImportError, match=r"found 3\.12\.7"):
        bench.require_project_python((3, 12, 7, "final", 0))


def test_bench_accepts_the_project_python():
    assert bench.PROJECT_PYTHON == (3, 12, 15)
    bench.require_project_python((3, 12, 15, "final", 0))
```

Run: `uv sync --locked && uv run pytest tests/bench/test_env.py -v`
Expected: PASS (środowisko `.venv` ma Pythona 3.12.15).

- [ ] **Step 6: Pakiet i commit**

Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

```bash
git add tests/bench/test_orchestrator.py tests/test_ballista_distributed_ops.py tests/test_sail_parallel_udtf.py .github/workflows/tests.yml .python-version bench/__init__.py tests/bench/test_env.py
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): testy i CI - przerwanie z zywym runnerem, nearest bez zaleznosci, kontrola LATERAL, protoc z SHA-256, pelna wersja Pythona

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 24: Argument `'strict'|'weak'` obowiązkowy w funkcjach `dist_*` (pozycja 18) i testy jednostkowe Rusta w CI

**Files:**
- Modify: `ballista_genomics/src/dist_udtf.rs` (`expect_filter_mode` zamiast `optional_strict`, liczby argumentów, komunikaty, komentarz przy `nearest`, moduł testów)
- Modify: `.github/workflows/tests.yml` (krok `cargo test --lib`)

**Interfaces:**
- Produces: `ballista_genomics::dist_udtf::expect_filter_mode(args: &[Expr], idx: usize, fname: &str) -> Result<bool>` (`'strict'` → true, `'weak'` → false, inny literał albo brak → błąd planowania); `optional_strict` znika.

- [ ] **Step 1: Testy jednostkowe (na końcu `dist_udtf.rs`)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::lit;

    fn merge_args(mode: Option<&str>) -> Vec<Expr> {
        let mut args = vec![lit("t"), lit("p.parquet"), lit("c"), lit("s"), lit("e"), lit(0i64)];
        if let Some(m) = mode {
            args.push(lit(m));
        }
        args
    }

    #[test]
    fn coordinate_mode_is_required() {
        let err = parse_merge(&merge_args(None)).unwrap_err().to_string();
        assert!(err.contains("'strict'|'weak'"), "{err}");
    }

    #[test]
    fn strict_and_weak_are_parsed() {
        assert!(matches!(parse_merge(&merge_args(Some("strict"))).unwrap(), DistPayload::Merge { strict: true, .. }));
        assert!(matches!(parse_merge(&merge_args(Some("weak"))).unwrap(), DistPayload::Merge { strict: false, .. }));
    }

    #[test]
    fn unknown_mode_is_rejected() {
        let err = parse_merge(&merge_args(Some("loose"))).unwrap_err().to_string();
        assert!(err.contains("must be 'strict'"), "{err}");
    }
}
```

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo test --lib --locked dist_udtf 2>&1 | tail -15`
Expected: FAIL (`coordinate_mode_is_required`, `unknown_mode_is_rejected`) — pierwsze budowanie testów trwa kilkanaście minut.

- [ ] **Step 2: Implementacja w `dist_udtf.rs`**

1. Funkcję `optional_strict` (z komentarzem nad nią) zastąp:

```rust
/// Obowiązkowy argument konwencji współrzędnych: `'strict'` (0-based, półotwarte — dane
/// databio-8p) albo `'weak'` (1-based, zamknięte). Dawniej brak argumentu oznaczał `'weak'`, co
/// ukryło błąd w `nearest` aż do biblioteki v0.22.2 (plan 3b-1) — dlatego bez wartości domyślnej
/// (plan 3b-2).
pub fn expect_filter_mode(args: &[Expr], idx: usize, fname: &str) -> Result<bool> {
    match args.get(idx) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) if v.eq_ignore_ascii_case("strict") => Ok(true),
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) if v.eq_ignore_ascii_case("weak") => Ok(false),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fname}(): argument {idx} must be 'strict' (0-based, half-open) or 'weak' (1-based, \
             closed), got: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): missing argument {idx}: 'strict'|'weak'"
        ))),
    }
}
```

2. W każdej funkcji `parse_*` liczba wymaganych argumentów rośnie o 1, a w komunikacie `[, 'strict'|'weak']` → `, 'strict'|'weak'`:
   - `parse_merge`: `args.len() < 6` → `< 7`; `strict: optional_strict(args, 6)` → `strict: expect_filter_mode(args, 6, "dist_merge")?`;
   - `parse_subtract`: `< 7` → `< 8`; `optional_strict(args, 7)` → `expect_filter_mode(args, 7, "dist_subtract")?`;
   - `parse_nearest`: `< 10` → `< 11`; `optional_strict(args, 10)` → `expect_filter_mode(args, 10, "dist_nearest")?`, a komentarz „Domyslnie WEAK - `nearest_local.rs` nie przekazuje 'strict', …” zastąp: `// Konwencja obowiązkowa (plan 3b-2): dane 0-based wymagają 'strict' — bez niego od v0.22.2 odległość wychodziła o 1 mniejsza niż w polars-bio (plan 3b-1).`;
   - `parse_coverage`: `< 7` → `< 8`; `optional_strict(args, 7)` → `expect_filter_mode(args, 7, "dist_coverage")?`;
   - `parse_overlap`: `< 7` → `< 8`; `optional_strict(args, 7)` → `expect_filter_mode(args, 7, "dist_overlap")?`.
3. Sprawdź, że nic poza `dist_udtf.rs` nie używało `optional_strict`: `grep -rn optional_strict ballista_genomics/src` → brak wyników.

- [ ] **Step 3: CI — testy jednostkowe Rusta**

W `.github/workflows/tests.yml`, po kroku `Build Ballista binaries`:

```yaml
      # Testy jednostkowe Rusta (plan 3b-2: m.in. obowiązkowa konwencja współrzędnych w dist_*).
      - name: Rust unit tests
        working-directory: ballista_genomics
        run: cargo test --lib --locked
```

- [ ] **Step 4: Uruchom — PASS**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo test --lib --locked 2>&1 | tail -8`
Expected: `test result: ok` (3 testy).
Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins 2>&1 | tail -3`
Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed` (wszystkie SQL-e w `scenario.rs` i `runner.rs` podają `'strict'`).

- [ ] **Step 5: Commit**

```bash
git add ballista_genomics/src/dist_udtf.rs .github/workflows/tests.yml
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): konwencja wspolrzednych obowiazkowa w funkcjach dist_*, testy jednostkowe Rusta w CI

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 25: Porządki — `polars<2`, podpowiedzi `uv run`, przyczyny w metodyce, wyniki smoke, konfiguracja sprawdzenia broadcastu (pozycje 10, 11, 19–21)

**Files:**
- Modify: `pyproject.toml`, `uv.lock`, `bench/config.py`, `tests/conftest.py`, `bench/orchestrator.py` (docstring), `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`
- Create: `bench/conf/broadcast_check.yaml`
- Add: `bench/results/smoke/20261004-175713/runs.parquet`
- Test: `tests/bench/test_config.py`

**Interfaces:**
- Produces: `bench/conf/broadcast_check.yaml` (seria `broadcast-check`: `nearest`, `coverage` × 2-7, 7-0, Ballista, N = 1, 2, 3, 1 przebieg, bez rozgrzewki) — używana w Zadaniu 27.

- [ ] **Step 1: `polars<2` (pozycja 21)**

W `pyproject.toml` linię `"polars>=1.44",` zastąp:

```toml
    # polars 2.0 (06.10.2026) psuje polars-bio 0.36 (nazwy kolumn CSV bez nagłówka); polars-bio
    # 0.36.1 przypina polars<2, obsługa polars 2 zapowiedziana w 0.37 (plan 3b-2).
    "polars>=1.44,<2",
```

Run: `uv lock && git diff --stat uv.lock && git diff uv.lock | grep -E '^[-+]version' ; uv sync --locked`
Expected: zmiana tylko w metadanych wymagań (`grep` nic nie wypisuje — żadna wersja pakietu się nie zmienia), `uv sync --locked` bez błędów.

- [ ] **Step 2: Podpowiedzi `uv run python -m` (pozycja 20)**

- `bench/config.py`, `check_data`: `"missing data (python -m bench.data.download): "` → `"missing data (uv run python -m bench.data.download): "`;
- `tests/conftest.py`, opis znacznika `real_data`: `(python -m bench.data.download)` → `(uv run python -m bench.data.download)`;
- `bench/orchestrator.py`, docstring: `python -m bench.orchestrator bench/conf/smoke.yaml [--ballista-profile debug]` → `uv run python -m bench.orchestrator bench/conf/smoke.yaml [--ballista-profile debug]`.

Run: `grep -rn "'python -m\|\"python -m\|(python -m" bench tests | grep -v "uv run"` → tylko `prog="python -m bench.…"` w argparse (nazwa programu, nie podpowiedź) — zostaje.

- [ ] **Step 3: Przyczyny nieważności w metodyce (pozycja 19)**

W `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`:
- linia 161: `(„pominięty: …”)` → `` (`"skipped: …"`) ``;
- linia 333: `(przyczyna „seria przerwana”)` → `` (przyczyna `"series interrupted"`) ``.

- [ ] **Step 4: Konfiguracja sprawdzenia broadcastu + test**

`bench/conf/broadcast_check.yaml`:

```yaml
# Sprawdzenie komórek z broadcastem (specyfikacja 3b-2, sekcja 9, punkt 5): nearest i coverage
# na 2-7 i 7-0 w Ballistcie przy N = 1, 2, 3 — ukończenie przy limicie gRPC 512 MiB,
# broadcast_bytes i szczyt pamięci (dane wejściowe także dla kalibracji w 3b-3).
# Seria pomiarowa — tylko po checkliście RAM (zamknięty VS Code, pusty swap).
series: broadcast-check
scenarios:
  - {op: nearest, pair: "2-7"}
  - {op: coverage, pair: "2-7"}
  - {op: nearest, pair: "7-0"}
  - {op: coverage, pair: "7-0"}
variants: [ballista]
nodes: [1, 2, 3]
repeats: 1
warmup: 0
control_repeats: 3
# Plany EXPLAIN ANALYZE z metrykami operatorów (spec 3b-2, 6.4) — czas budowy indeksu na zadanie.
explain: true
seed: 20261009
```

W `tests/bench/test_config.py` dopisz:

```python
def test_broadcast_check_config_covers_the_broadcast_cells():
    cfg = load(REPO / "bench" / "conf" / "broadcast_check.yaml")
    assert cfg.variants == ("ballista",) and cfg.nodes == (1, 2, 3)
    assert {s.id for s in cfg.scenarios} == {"nearest/2-7", "coverage/2-7", "nearest/7-0", "coverage/7-0"}
    assert cfg.explain is True
```

Run: `uv run pytest tests/bench/test_config.py -v` → PASS.

- [ ] **Step 5: Wyniki smoke z 04.10 w repozytorium (pozycja 11)**

Run: `git add bench/results/smoke/20261004-175713/runs.parquet && git status --short bench/results`
Expected: tylko `A  bench/results/smoke/20261004-175713/runs.parquet` (JSON Lines, logi i `tmp/` są w `.gitignore`).

- [ ] **Step 6: Pakiet i commit**

Run: `uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

```bash
git add pyproject.toml uv.lock bench/config.py tests/conftest.py bench/orchestrator.py docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md bench/conf/broadcast_check.yaml tests/bench/test_config.py bench/results/smoke/20261004-175713/runs.parquet
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): polars<2, podpowiedzi uv run, przyczyny w metodyce, wyniki smoke z 04.10, konfiguracja broadcast-check

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

### Zadanie 26: Build release Ballisty

**Files:**
- Modify: `ballista_genomics/OPIS.md` (sekcja o buildzie release)

Ostatnie zadanie zmieniające Rusta jest za nami (Zadanie 24) — build release robimy raz.

- [ ] **Step 1: Stan maszyny**

Run: `free -m && df -h / && du -sh /home/milosz/praca_magisterska/ballista_genomics/target`
Expected: ≥ 3 GB dostępnej pamięci, ≥ 30 GB wolnego dysku. Jeśli pamięci jest mniej — poproś użytkownika o zamknięcie aplikacji w tle (VS Code może zostać; to nie pomiar).

- [ ] **Step 2: Build release (w tle; może trwać godzinę lub dłużej)**

Run (w tle, z zapisem do pliku):

```bash
cd /home/milosz/praca_magisterska/ballista_genomics && /usr/bin/time -v env CARGO_BUILD_JOBS=1 cargo build --release --bins --locked > <scratchpad>/release_build.log 2>&1; echo "exit=$?" >> <scratchpad>/release_build.log
```

Po zakończeniu: `tail -30 <scratchpad>/release_build.log` — zanotuj `Elapsed (wall clock) time`, `Maximum resident set size` i `exit=0`. Jeśli build przerwał brak pamięci (`SIGKILL`, `signal: 9`) — **zatrzymaj się** i zapytaj użytkownika (opcje: zamknięcie aplikacji, mniejsza równoległość generowania kodu `CARGO_PROFILE_RELEASE_CODEGEN_UNITS`).

- [ ] **Step 3: Sprawdzenie binarek release**

Run:

```bash
cd /home/milosz/praca_magisterska && ls -la ballista_genomics/target/release/ballista_node ballista_genomics/target/release/bench_client && du -sh ballista_genomics/target/release
uv run python - <<'EOF'
import subprocess, tempfile
from pathlib import Path
from bench import engines
from tests.parquet_fixture import FIXTURE_A, write_parts

engines.require_ballista_binaries("release")
tmp = Path(tempfile.mkdtemp())
left = write_parts(FIXTURE_A, tmp / "a")
engine = engines.BallistaEngine(1, profile="release", data_root=tmp, log_dir=tmp / "logs")
engine.start()
try:
    r = subprocess.run(
        [str(engines.ballista_binary("bench_client", "release")), "--op", "merge", "--left", str(left)],
        cwd=engines.BALLISTA_DIR, env={**engine.env, "BALLISTA_SCHEDULER_URL": f"df://localhost:{engine.port}"},
        capture_output=True, text=True, timeout=120,
    )
    print(r.returncode, r.stdout.strip(), r.stderr[-500:])
finally:
    engine.stop()
EOF
```

Expected: binarki istnieją; `0 {"rows": …, "checksum": …}`.

- [ ] **Step 4: Zapis w `ballista_genomics/OPIS.md`**

Na końcu pliku dodaj sekcję:

```markdown
## Plan 3b-2 — build release (październik 2026)

- Profil: domyślny `release` Cargo (opt-level 3, bez LTO, `codegen-units` domyślne) — ta sama
  klasa optymalizacji co oficjalne paczki polars-bio 0.36.0 (ich `Cargo.toml` nie ma sekcji
  `[profile]`); wersja kompilatora: Rust 1.95.0 (`rust-toolchain.toml`).
- Polecenie: `CARGO_BUILD_JOBS=1 cargo build --release --bins --locked`.
- Pierwszy build: <czas> (wall clock), szczyt pamięci <MB>, `target/release` <GB>.
- Narzędzie pomiarowe używa domyślnie binarek release; przed serią sprawdza, czy są nowsze od
  źródeł (`bench/engines.py`, `require_ballista_binaries`), a ich sumy SHA-256 zapisuje
  w `engine_versions`.
```

(wpisz zanotowane liczby).

- [ ] **Step 5: Commit**

```bash
git add ballista_genomics/OPIS.md
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 4): build release Ballisty - czas i zasoby w OPIS.md

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

# Etap 5 — weryfikacja na danych rzeczywistych (spec, sekcja 9)

### Zadanie 27: Weryfikacja na danych rzeczywistych (z użytkownikiem)

**Files:**
- Add: `bench/results/{control-noise,smoke,sail-paths,broadcast-check}/<znacznik czasu>/{runs.parquet,env.json}` i pliki `explain/**/*.txt` (smoke)

- [ ] **Step 1: Checklista RAM i sposób uruchomienia — STOP do potwierdzenia**

Przypomnij użytkownikowi checklistę z pamięci `feedback_ram_przed_eksperymentem`:
- zamknięty VS Code i aplikacje w tle;
- `.wslconfig` 5 GB / 4 GB swap;
- laptop na zasilaczu, plan zasilania „wysoka wydajność”;
- **pusty swap**: `wsl --shutdown` w Windows i ponowne otwarcie terminala WSL (narzędzie i tak odmówi startu przy niepustym swapie).

Ta sesja działa w VS Code, więc zapytaj, jak uruchamiamy serie: (a) Claude Code w zwykłym terminalu WSL (Windows Terminal) bez VS Code, albo (b) użytkownik uruchamia polecenia z kroków 2–7 sam i wkleja podsumowania. **Nie uruchamiaj niczego z kroków 2–7 bez potwierdzenia.**

- [ ] **Step 2: Stan maszyny**

Run: `free -m && cd /home/milosz/praca_magisterska && git status --short && git log --oneline -1`
Expected: `Swap: … used 0`; czyste drzewo na gałęzi `3b-2-narzedzie`.

- [ ] **Step 3: Pełny pakiet testów z danymi rzeczywistymi**

Run: `uv run pytest -q 2>&1 | tail -5`
Expected: `0 failed` (xfail: regresja LATERAL Saila, standalone przy broadcaście).

- [ ] **Step 4: Badanie szumu przebiegu kontrolnego**

Run: `uv run python -m bench.orchestrator bench/conf/control_noise.yaml 2>&1 | tail -5`
Expected: ostatnie linie `control-noise OK: …`.
Run: `uv run python -m bench.control_noise "$(ls -d bench/results/control-noise/* | tail -1)"`
Expected: `false drift rate … ≤ 5.0%`. Zapisz obie linie wyniku. **Powyżej 5% — zatrzymaj się** i pokaż użytkownikowi liczby (mediana, rozrzut, odsetek) z propozycją progu (np. najmniejszy próg z odsetkiem ≤ 5%: `--tolerance 0.15` itd.).

- [ ] **Step 5: Smoke na buildzie release**

Run: `uv run python -m bench.orchestrator bench/conf/smoke.yaml 2>&1 | tail -15`
Expected: `smoke OK: …` (żaden nieoczekiwany nieważny przebieg, żaden blok powtórzony przez dryf, żadne niepowodzenie `EXPLAIN ANALYZE`).
Sprawdź wynik:

```bash
uv run python - <<'EOF'
import glob, json
import polars as pl
d = sorted(glob.glob("bench/results/smoke/*/"))[-1]
df = pl.read_parquet(d + "runs.parquet").filter(~pl.col("is_reference") & ~pl.col("is_control"))
print(df.group_by(["variant", "n_nodes", "op"]).agg(pl.col("t_total_s").median().round(3)).sort(["op", "variant", "n_nodes"]))
sail = df.filter(pl.col("variant").str.starts_with("sail"))
print("sail workers == N:", all(json.loads(e)["sail_workers"] == n for e, n in zip(sail["extra"], sail["n_nodes"])))
b = df.filter(pl.col("variant") == "ballista")
print(b.select("op", "n_nodes", "broadcast_bytes").sort(["op", "n_nodes"]))
print("explain files:", len(glob.glob(d + "explain/**/*.txt", recursive=True)))
EOF
```

Expected: `sail workers == N: True`; `broadcast_bytes > 0` tylko dla `nearest` i `coverage`; 15 plików explain (5 scenariuszy × N = 1, 2, 3). Jeśli smoke jest `FAILED` — przeanalizuj przyczyny; ograniczenie silnika dopisz do `expected_invalid` w `smoke.yaml` **tylko po akceptacji użytkownika**, błąd narzędzia — napraw (TDD) i powtórz smoke.

- [ ] **Step 6: Porównanie ścieżek Saila**

Run: `uv run python -m bench.orchestrator bench/conf/sail_paths.yaml 2>&1 | tail -5`
Expected: `sail-paths OK: …` (jeśli `sail_udtf` na 7-0 przekroczy limit czasu albo pamięci — to też wynik: zapisz przyczynę; zaakceptowany przez użytkownika wpis w `expected_invalid` i powtórka).

```bash
uv run python - <<'EOF'
import glob
import polars as pl
d = sorted(glob.glob("bench/results/sail-paths/*/"))[-1]
df = pl.read_parquet(d + "runs.parquet").filter(
    ~pl.col("is_reference") & ~pl.col("is_warmup") & ~pl.col("is_control") & pl.col("valid"))
print(df.group_by(["scenario_id", "n_nodes", "variant"]).agg(
    pl.col("t_total_s").median().round(3).alias("median_s"), pl.len().alias("n")).sort(["scenario_id", "n_nodes", "variant"]))
EOF
```

Zapisz tabelę (do `wnioski_claude.md` w Zadaniu 28).

- [ ] **Step 7: Komórki z broadcastem w Ballistcie**

Run: `uv run python -m bench.orchestrator bench/conf/broadcast_check.yaml 2>&1 | tail -8`

```bash
uv run python - <<'EOF'
import glob
import polars as pl
d = sorted(glob.glob("bench/results/broadcast-check/*/"))[-1]
df = pl.read_parquet(d + "runs.parquet").filter(~pl.col("is_reference") & ~pl.col("is_control"))
print(df.select("scenario_id", "n_nodes", "valid", "invalid_reason", "t_total_s", "broadcast_bytes", "peak_rss_sum").sort(["scenario_id", "n_nodes"]))
EOF
```

Expected: wszystkie przebiegi ważne; `broadcast_bytes` rośnie z N (więcej zadań). Przebieg nieważny przez strażnika pamięci albo błąd limitu — **nie naprawiaj w tym zadaniu**: zapisz przyczynę i zapytaj użytkownika (spec 6.2: plan zapasowy ze ścieżką zamiast danych tylko po jego decyzji).

- [ ] **Step 8: Commit wyników**

```bash
git add bench/results/control-noise bench/results/smoke bench/results/sail-paths bench/results/broadcast-check
git status --short bench/results
```
Expected: same `runs.parquet`, `env.json` i `explain/**/*.txt` (JSON Lines, `logs/` i `tmp/` ignorowane).

```bash
git commit -m "$(cat <<'EOF'
Plan 3b-2 (etap 5): weryfikacja na danych rzeczywistych - szum kontroli, smoke na release, sciezki Saila, broadcast Ballisty

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

# Etap 6 — dokumentacja (spec, sekcja 8, etap 6)

### Zadanie 28: Dokumentacja

**Files:**
- Modify: `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`, `docs/superpowers/specs/2026-10-09-narzedzie-gotowe-do-pomiarow-3b2-design.md`, `ballista_genomics/OPIS.md`, `wnioski_claude.md`, `README.md`
- Modify (poza repozytorium): pamięć projektu `/home/milosz/.claude/projects/-home-milosz-praca-magisterska/memory/project_mgr.md`

- [ ] **Step 1: Specyfikacja metodyki**

Zaktualizuj (po polsku, zwięźle, bez kopiowania całych opisów z 3b-2 — odsyłacz do specyfikacji 3b-2 tam, gdzie szczegóły):
1. **Sekcja 3, tabela wariantów, wiersz Sail:** serwer `local-cluster` jak dotąd, ale funkcja to `groupBy/cogroup(...).applyInArrow` z kontekstem polars-bio na wątek (bez blokady `sail_pb_guard`), `POLARS_MAX_THREADS = 2N` w procesie serwera; warianty porównawcze `sail_lock` i `sail_udtf` (spec 3b-2, 5.3); usuń zdanie o asymetrii „z blokadą `sail_pb_guard`” i zastąp: „jeden proces (asymetria wieloprocesowości opisana jawnie); klient PySpark w Pythonie (Ballista: klient w Ruście) — czas odbioru wyniku jest osobną fazą”.
2. **Sekcja 5, tabela metryk:** wiersz „Fazy” — polars-bio: `first_batch_s`; Ballista: `first_batch_s` + `EXPLAIN ANALYZE` (dodatkowy przebieg, `explain: true`); Sail: `first_batch_s`, `pb_s`, `lock_wait_s` (tylko `sail_lock`), `udf_groups`, `sail_workers`. Wiersz pamięci: dopisz `base_rss`. Wiersz wolumenu: `broadcast_bytes` z logów executorów (bajty ładunków odebranych).
3. **Sekcja 6:**
   - pkt 2–3: przebieg kontrolny `overlap` 7-0 w polars-bio A, `control_repeats` = 10 na każdym końcu bloku, dryf median > 10% ⇒ powtórzenie; mniej niż połowa ważnych ⇒ jak dryf; wynik badania szumu (Zadanie 27, krok 4: odsetek fałszywych dryfów);
   - pkt 4: partie trzymane w pamięci, zegar staje po ostatniej partii, suma kontrolna po zegarze (`checksum_s` informacyjnie);
   - pkt 5: seria startuje od pustego swapu (sprawdzenie przed serią), przyrost `pswpout` **lub** `pswpin` unieważnia przebieg; usuń zdanie „Reguła — plan 3b (…)”; dopisz śmierć procesu silnika wykrywaną w trakcie przebiegu.
4. **Sekcja 8:** 8.1 — nowe moduły (`control_noise.py`, `logscan.py`, `preflight.py`, `sail_arrow.py`, `sail_pb_context.py`) i konfiguracje (`control_noise.yaml`, `sail_paths.yaml`, `broadcast_check.yaml`); 8.2 — klucze `control`, `control_repeats`, `explain`, `expected_invalid`; 8.3 — przykład linii JSON z `"phases": {"first_batch_s": …}`, zdanie o `checksum_s` poza czasem (usuń „czy odejmować go od czasu, ustala plan 3b”) i o `broadcast_bytes` (orkiestrator, nie runner); 8.5 — kolumny `control_position`, `base_rss`, `env.json` serii.
5. **Sekcja 9.2:** limit 512 MiB (`BALLISTA_GRPC_MAX_MESSAGE_BYTES`) wdrożony, strażnik broadcastu przy planowaniu, wynik `broadcast-check` (Zadanie 27, krok 7).
6. **Sekcja 11 (ryzyka):** wiersz „Asymetria klientów (PySpark vs Rust)” — skutek: czas odbioru wyniku Saila zawyżony względem Ballisty; środek: faza odbioru (`t_total_s − first_batch_s`) raportowana osobno, wynik sondy partii (Zadanie 12).
7. **Sekcja 12:** usuń punkty zrealizowane w 3b-2.

- [ ] **Step 2: Specyfikacja 3b-2 — status**

Linię `- **Status:** …` zastąp: `- **Status:** zatwierdzony (09.10.2026); plan: \`docs/superpowers/plans/2026-10-09-narzedzie-gotowe-do-pomiarow-3b2.md\`; wykonany <data>`.

- [ ] **Step 3: `ballista_genomics/OPIS.md`**

Sekcję „Plan 3b-2” (z Zadania 26) uzupełnij o punkty: limit gRPC (zmienna, wartości, gdzie ustawiany: `bio_ballista_config`, `ballista_node`), strażnik broadcastu (`broadcast.rs`, `TASKS_PER_MESSAGE`, dlaczego przy planowaniu: zachowanie Ballisty 53 w trybie push przy zbyt dużej wiadomości), linia logu `broadcast payload decoded: N bytes` i jej użycie, tryb `bench_client --explain-analyze`, obowiązkowe `'strict'|'weak'` w `dist_*`, wynik `broadcast-check`.

- [ ] **Step 4: `wnioski_claude.md`**

Sekcję „Plan 3b-2 — narzędzie gotowe do pomiarów” uzupełnij o (każdy punkt z liczbami):
1. ścieżka Arrow vs UDTF (sonda 08.10 + tabela z Zadania 27, krok 6) — wniosek o koszcie mechanizmu integracji i dojrzałości API rozszerzeń Saila;
2. globalny kontekst polars-bio (`s1`/`s2`) i pomost — zysk z braku blokady (sonda: ~10% na 7-0; porównanie `sail` vs `sail_lock` z kroku 6); obserwacja dla pracy: polars-bio nie jest bezpieczny wątkowo w jednym procesie, co ogranicza integrację z silnikami wykonującymi funkcje Pythona w wątkach;
3. odbiór wyniku przez klienta Pythona (1877 partii, sonda partii z Zadania 12);
4. badanie szumu przebiegu kontrolnego (Zadanie 27, krok 4);
5. smoke na release (czasy Ballisty release vs debug z 04.10 — przyspieszenie z samej kompilacji);
6. broadcast Ballisty (`broadcast_bytes` vs N; pamięć; Zadanie 27, krok 7);
7. ostrzeżenia PySparka 4.2 przy pandas 3 na ścieżce Arrow (Zadanie 10, krok 4) — uzupełnienie punktu 4 sekcji „Plan 3b-1”.

- [ ] **Step 5: `README.md`**

W sekcji poleceń dopisz: badanie szumu (`bench/conf/control_noise.yaml` + `uv run python -m bench.control_noise <katalog>`), porównanie ścieżek Saila (`sail_paths.yaml`), sprawdzenie broadcastu (`broadcast_check.yaml`), build release (`cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --release --bins`) i zdanie, że serie pomiarowe wymagają pustego swapu (narzędzie sprawdza to przed startem).

- [ ] **Step 6: Strażnik języka i commit**

Run: `uv run pytest tests/test_code_language.py -q && uv run pytest -m "not real_data" -q`
Expected: `0 failed`.

```bash
git add docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md docs/superpowers/specs/2026-10-09-narzedzie-gotowe-do-pomiarow-3b2-design.md ballista_genomics/OPIS.md wnioski_claude.md README.md
git commit -m "$(cat <<'EOF'
Plan 3b-2: dokumentacja - metodyka, OPIS, wnioski, README

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 7: Pamięć projektu**

W `project_mgr.md` (pamięć, poza repozytorium) zastąp akapit „SPEC 3b-2 NAPISANA …” stanem po wykonaniu: gałąź, ostatni commit, najważniejsze liczby (szum, smoke, ścieżki Saila, broadcast), odłożone sprawy i „NASTĘPNY KROK: przegląd końcowy i scalenie, potem brainstorming 3b-3”. Zaktualizuj też linię w `MEMORY.md`.

---

## Po zadaniach

1. **Przegląd końcowy całej gałęzi** (`superpowers:requesting-code-review` / recenzent na najmocniejszym modelu) względem specyfikacji 3b-2 i tego planu; poprawki w osobnych commitach.
2. **CI:** push gałęzi i PR **tylko po zgodzie użytkownika**; CI ma być zielone (nowy krok `cargo test --lib`, `protoc` z SHA-256, Python 3.12.15).
3. **Scalenie:** `superpowers:finishing-a-development-branch` — fast-forward do `master` po akceptacji użytkownika.
4. **Następny krok projektu:** brainstorming 3b-3 (kalibracja, konfiguracje `kalibracja/p1/p2/p3`, parametr `algorithm`).

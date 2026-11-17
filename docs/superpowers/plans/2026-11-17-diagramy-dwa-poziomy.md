# Diagramy README w dwóch poziomach — plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Cel:** Przerobić diagramy Mermaid w README `ballista-genomics` i `sail-genomics` na dwa
poziomy: przegląd i szczegóły podzielone na fazy.

**Architektura:** Zmieniają się tylko nakładki `tools/split/overlay/<repo>/README.md`.
Jednorazowy skrypt kontrolny w scratchpadzie porównuje nowe bloki `mermaid` ze starymi
(commit `6667d59`): sprawdza nazwy, długość wierszy, liczbę elementów, znaki groźne dla
parsera i odnośniki. Na końcu eksport do `/home/milosz/repos-staging` i przegląd użytkownika.

**Technologie:** Markdown + Mermaid (sequenceDiagram, flowchart), Python 3 (skrypt
kontrolny), `tools/split/split.py`.

**Spec:** `docs/superpowers/specs/2026-11-17-diagramy-dwa-poziomy-design.md`

## Global Constraints

- Treść README po angielsku, bez polskich liter i bez nazw wrażliwych. Strażnik eksportu
  ma dać 0 trafień.
- Bez nowych nazw z kodu: tylko nazwy obecne w starych blokach `mermaid` (sprawdzone z kodem
  i logami, spec etapu 4 §6.4) albo zwykłe słowa.
- Konwencje z próbki (zaakceptowana 17.11.2026, plik
  `scratchpad/sample-ballista-query-flow.md`):
  - pierwsza linia bloku: `%%{init: {"sequence": {"mirrorActors": false}}}%%`
    (tylko sequenceDiagram);
  - strzałka to wiadomość między procesami, `Note over` to praca w procesie;
  - WIELKIE LITERY oznaczają serializację planu oraz przejście danych między procesami
    albo Rust↔Python;
  - numeracja ręczna: przegląd 1..n, szczegóły z literą (4a…);
  - podpisy łamane `<br/>`, nie więcej niż 40 znaków w wierszu;
  - przegląd i każda faza mają nie więcej niż 10 elementów (strzałek + notatek albo
    węzłów).
- Układ przy każdym diagramie:
  1. przegląd;
  2. lista „In words”;
  3. wiersz `In detail: [...](#slug) · ...`;
  4. nagłówki faz `#### Detail ...`, wyłącznie ASCII: litery, cyfry, spacje.
- Commity w monorepo: po polsku, bez polskich znaków, bez stopki Co-Authored-By. Bez pusha.

## Review Focus

1. Błąd parsera Mermaid na GitHubie: `;` albo `#` w bloku, cudzysłów w etykiecie
   flowchart, wiersz zaczynający się od `end`. Skrypt kontrolny to zgłasza; wszystkie
   etykiety flowchart muszą być w `["..."]`.
2. Martwe odnośniki „In detail”: każdy `](#slug)` musi pasować do nagłówka w tym samym
   pliku (slug w stylu GitHuba). Sprawdza skrypt.
3. Zgubiona nazwa ze starego diagramu. Sprawdza skrypt: zgłasza identyfikatory ze starych
   bloków, których nie ma w nowych.
4. Diagram znów za szeroki albo za wysoki. Skrypt zgłasza wiersze dłuższe niż 40 znaków
   i bloki z więcej niż 10 elementami.
5. Przeciek do eksportu (polskie litery, nazwy wrażliwe). Eksport z kodem 0 i 0 trafień
   strażnika (Task 6).

---

### Task 1: Skrypt kontrolny diagramów

**Files:**
- Create: `/tmp/claude-1000/-home-milosz-praca-magisterska/b15fc680-bae5-4e0e-b945-83bcf20c2b9e/scratchpad/check_diagrams.py`
  (narzędzie warsztatowe, poza repo)

**Interfaces:**
- Produces: `python3 -I check_diagrams.py <old_ref> <readme_path>`. Kod 0, gdy brak
  problemów. Wypisuje:
  - `MISSING <name>`;
  - `LONG <block>:<line>: <segment>`;
  - `BIG <block>: <n> elements`;
  - `BAD <block>:<line>: <char>`;
  - `ANCHOR <slug>`.

- [ ] **Step 1: Napisz skrypt**

Funkcje:
- `blocks(text) -> list[str]`: treści bloków ` ```mermaid `.
- `names(text) -> set[str]`: tokeny `[\w.:]+` zawierające `_`, `::`, `.` między literami
  albo wewnętrzną wielką literę (CamelCase), bez liczb i słów kluczowych Mermaid. Przed
  wyciąganiem `::<br/>` zamienia się na `::`, a pozostałe `<br/>` na spację.
- MISSING: nazwy ze starych bloków (`git show <old_ref>:<ścieżka względem repo>`), których
  nie ma jako podciąg w znormalizowanym tekście nowych bloków.
- LONG: segment etykiety, czyli tekst po pierwszym `:` w wierszu sequence albo wewnątrz
  `["..."]` / `|"..."|` we flowchart, podzielony po `<br/>`, ma więcej niż 40 znaków.
- BIG: elementy bloku to wiersze z `->>`, `-->>` lub `Note over` (sequence) albo węzły
  `id["..."]` (flowchart). Zgłoszenie, gdy jest ich więcej niż 10.
- BAD: `;` lub `#` w wierszu bloku poza dyrektywą `%%{...}%%`, albo wiersz, którego
  pierwszym słowem jest `end` poza zamknięciem `loop`/`subgraph`.
- ANCHOR: slug z `](#...)` nie pasuje do żadnego nagłówka pliku. Slug nagłówka: małe
  litery, usunięte znaki spoza `[a-z0-9 _-]`, spacje zamienione na `-`.

- [ ] **Step 2: Sprawdź na starym README**

Run: `python3 -I check_diagrams.py 6667d59 tools/split/overlay/ballista-genomics/README.md`
(z katalogu repo).
Expected: brak `MISSING` (stare porównane ze sobą); są `LONG` i `BIG` dla obecnych
diagramów, co potwierdza, że kontrola działa. Kod różny od 0.

### Task 2: Ballista — Query flow w dwóch poziomach

**Files:**
- Modify: `tools/split/overlay/ballista-genomics/README.md` (sekcja `## Query flow`, obecnie
  od wiersza 61 do akapitu o trybie standalone)

**Interfaces:**
- Consumes: `check_diagrams.py` (Task 1). Przegląd i faza „steps 4 to 6” z próbki
  `scratchpad/sample-ballista-query-flow.md`, przeniesione bez zmian treści.

- [ ] **Step 1: Wstaw przegląd i „In words” z próbki**

Akapit wstępu sekcji zostaje: wersje i znaczenie WIELKICH LITER; dochodzi zdanie
o konwencji strzałka/prostokąt.

- [ ] **Step 2: Wstaw cztery fazy**

Nagłówki:
- `#### Detail steps 1 and 2 client and logical plan`;
- `#### Detail step 3 planning in the scheduler`;
- `#### Detail steps 4 to 6 tasks execution shuffle`;
- `#### Detail steps 7 and 8 result`.

Treść faz:
- **Fazy 1–2:**
  - Note C: `runner::connect_from_env`, `bio_session_state(bio_ballista_config())`,
    `SessionContext::remote_with_state`;
  - `ctx.sql`, `DistTableFunction::call → DistBioProvider::build`;
  - C→S: `execute_query_push(ExecuteQueryParams)`, `BallistaQueryPlanner →
    DistributedQueryExec::execute`, `LogicalPlanNode`,
    `BioDistLogicalCodec::try_encode_table_provider (DistPayload::encode)`.
- **Faza 3:**
  - `SchedulerServer::parse_plan`, `try_decode_table_provider`, `DistPayload::decode →
    DistBioProvider::build`;
  - `TaskManager::submit_plan → AdaptiveExecutionGraph::try_new`, `AdaptivePlanner::try_new
    → create_physical_plan → DistBioProvider::scan`, `collect_left_at_planning`,
    `check_broadcast_fits`;
  - `EnsureRequirements` (hash `RepartitionExec`, `CoalescePartitionsExec`),
    `DistributedExchangeRule`, stages ending in `ShuffleWriterExec` /
    `SortShuffleWriterExec`.
  - Jeśli to więcej niż 10 elementów, rozbić na notatki 3a–3f.
- **Fazy 4–6:** jak w próbce.
- **Fazy 7–8:**
  - S→C: job status stream `GetJobStatusResult`;
  - C→E: `fetch_partition` → Arrow Flight `do_get`;
  - E→C: RecordBatch stream (`bench_client` checksums, `dist_ops` prints).

Akapit o trybie standalone zostaje po fazach.

- [ ] **Step 3: Kontrola**

Run: `python3 -I check_diagrams.py 6667d59 tools/split/overlay/ballista-genomics/README.md`
Expected: brak `LONG`, `BIG`, `BAD`, `ANCHOR` dla bloków Query flow. `MISSING` może jeszcze
wskazywać tylko nazwy z bloku Components (Task 3).

- [ ] **Step 4: Commit** — `Etap 4: Ballista README - query flow w dwoch poziomach`

### Task 3: Ballista — Components w dwóch poziomach

**Files:**
- Modify: `tools/split/overlay/ballista-genomics/README.md` (sekcja `## Components`)

- [ ] **Step 1: Przegląd**

Flowchart z 4 węzłami:
- `ballista_genomics (this repository)`;
- `Apache Ballista 55.0.0`;
- `Apache DataFusion 55.2.0`;
- `datafusion-bio-function-ranges v0.25.0 (vendor/, patched)`.

Podpisane krawędzie:
- uses client, scheduler, executor;
- registers our codecs in the session;
- our providers delegate scans and algorithms to;
- our codecs implement the codec interfaces of;
- Ballista plans and runs on.

„In words”: po jednym zdaniu na pudełko.

- [ ] **Step 2: Szczegóły (a) building and planning the query**

Węzły: `BINS`, `CLUSTER`, `UDTF`, `PROV`, `COVP`, `BSESS`, `BPROV`, `BNODES`, `BCLIENT`,
`DFCORE`, ze starymi etykietami (łamanymi do 40 znaków), i ich stare krawędzie.

- [ ] **Step 3: Szczegóły (b) codecs and serialization**

Węzły: `LCODEC`, `PCODEC`, `IJCODEC`, `CIO`, `BCODEC`, `DFPROTO`, `ARROW`, `BNODES`,
`BIDX` (patch 1 i 2), plus `BSCHED`, `BEXEC`, `BSHUF`, `DFOPT`. Stare krawędzie bez zmian.
Gdy przekroczony limit 10 węzłów, `BSCHED`/`BEXEC`/`BSHUF`/`DFOPT` przechodzą do (a) albo
do trzeciego, małego diagramu `#### Detail Ballista scheduler and executor`.

- [ ] **Step 4: Kontrola**

Run: jak w Task 2, Step 3.
Expected: kod 0 (żadnych `MISSING`, `LONG`, `BIG`, `BAD`, `ANCHOR`).

- [ ] **Step 5: Commit** — `Etap 4: Ballista README - komponenty w dwoch poziomach`

### Task 4: Sail — Query flow w dwóch poziomach

**Files:**
- Modify: `tools/split/overlay/sail-genomics/README.md` (sekcja `## Query flow`)

- [ ] **Step 1: Przegląd z 8 krokami i „In words”**

Uczestnicy: PySpark client, Sail driver, Sail workers, `sail_genomics` (Python),
polars-bio. Kroki:
1. build query (groupBy/cogroup/applyInArrow);
2. PLAN via Spark Connect gRPC;
3. driver plans stages;
4. workers shuffle rows by chromosome and collect them per chromosome;
5. RUST TO PYTHON: our function gets two Arrow tables;
6. polars-bio range operation (context from the pool; PYTHON TO RUST inside polars-bio);
7. RENAME and PYTHON TO RUST: one Arrow table per chromosome;
8. result batches to the client.

- [ ] **Step 2: Fazy**

Nagłówki:
- `#### Detail steps 1 to 3 client and plan`;
- `#### Detail step 4 stages and shuffle in the workers`;
- `#### Detail steps 5 to 7 our function and polars-bio`;
- `#### Detail step 8 result`.

Każdą wiadomość i notatkę starego diagramu przenieść do właściwej fazy. Lista „Notes on
the diagram” zostaje pod fazami.

- [ ] **Step 3: Kontrola**

Run: `python3 -I check_diagrams.py 6667d59 tools/split/overlay/sail-genomics/README.md`
Expected: dla bloków Query flow brak `LONG`, `BIG`, `BAD`, `ANCHOR`.

- [ ] **Step 4: Commit** — `Etap 4: Sail README - query flow w dwoch poziomach`

### Task 5: Sail — Components w dwóch poziomach

**Files:**
- Modify: `tools/split/overlay/sail-genomics/README.md` (sekcja `## Components`)

- [ ] **Step 1: Przegląd**

Flowchart z 4 węzłami:
- `sail_genomics (this repository)`;
- `PySpark 4.2.1 (client, Python)`;
- `Sail 0.7.3 (pysail, Rust, embeds DataFusion 55.2)`;
- `polars-bio 0.37.0 (Python + Rust, DataFusion 53)`.

Krawędzie:
- builds the query with;
- Spark Connect gRPC;
- RUST TO PYTHON: calls our function;
- calls range operations, replaces range_operation.

Plus „In words”.

- [ ] **Step 2: Szczegóły**

Stary diagram (14 węzłów) przekracza limit 10, więc dzielę go na dwa:
- `#### Detail client side`: `OURS`: `BQ`, `SES`, `SCH` + `PYSPARK` + `SRV`;
- `#### Detail inside a worker`: `WRK`, `PYU`, `EV`, `FR`, `PBC`, `API`, `RO`, `RUST`, `SCH`.

Stare etykiety i krawędzie bez zmian.

- [ ] **Step 3: Kontrola**

Run: jak w Task 4, Step 3.
Expected: kod 0.

- [ ] **Step 4: Commit** — `Etap 4: Sail README - komponenty w dwoch poziomach`

### Task 6: Eksport, kontrola i przegląd użytkownika

**Files:**
- Regenerate: `/home/milosz/repos-staging/` (eksport z nowego HEAD; stary podgląd jest
  zastępowany w całości)
- Regenerate: `scratchpad/readme-preview.html` (`build_preview.py`)

- [ ] **Step 1: Testy podziału**

Run: `uv run pytest tests/test_split.py -q`
Expected: wszystkie przechodzą (18 testów).

- [ ] **Step 2: Eksport**

Run: `uv run python tools/split/split.py --source HEAD --out $SCRATCH/split-diagrams`
Expected: kod 0, 0 trafień strażnika we wszystkich trzech repo.

Potem podmiana `/home/milosz/repos-staging` na nowy eksport. Sprawdzenie: `grep -P
'[^\x00-\x7F]'` po polskich literach (`ąćęłńóśźż`) w trzech README daje 0.

- [ ] **Step 3: Podgląd offline**

Run: `python3 -I build_preview.py`, potem otwarcie w przeglądarce Windows.
Expected: pasek stanu „all N diagrams rendered”. Jeśli pojawi się błąd składni: poprawka
w nakładce, powrót do Step 2.

- [ ] **Step 4: Ledger i przegląd**

Wpis do `progress.md`. Użytkownik ogląda README w VS Code. Uwagi wracają jako poprawki
w Task 2–5. Task 14 dopiero po akceptacji.

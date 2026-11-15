# Etap 4: aktualne wersje, trzy repozytoria i procedura 1/2/4/8 — projekt

- **Data:** 15 listopada 2026
- **Status:** zatwierdzony (15.11.2026); diagramy użytkownik sprawdza po wykonaniu (bez wstrzymywania prac)
- **Zakres:** podbicie wersji (Ballista 55, datafusion-bio-functions v0.25.0, Sail 0.7.3,
  polars-bio 0.37.0), nowa semantyka równoległości P ∈ {1, 2, 4, 8} w narzędziu pomiarowym,
  raport z serii, podział kodu na trzy publiczne repozytoria (Ballista, Sail, procedura testowa)
  z diagramami integracji, pierwsza seria 1/2/4/8 na laptopie (jeśli starczy czasu)
- **Powiązane:** `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` (metodyka),
  `docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md` (3b-1: uv, CI, zasada tych samych
  algorytmów), `docs/superpowers/specs/2026-10-09-narzedzie-gotowe-do-pomiarow-3b2-design.md` (3b-2:
  protokół pomiaru, applyInArrow, przebieg kontrolny), `ballista_genomics/vendor/PATCH.md`,
  `ballista_genomics/OPIS.md`

## 1. Kontekst i cel

Kod pracy żyje dziś w jednym repozytorium (`distributed-polars-bio`): integracja z Ballistą
(`ballista_genomics/`, Rust), integracja z Sailem (`sail_*.py`), narzędzie pomiarowe (`bench/`),
testy, raporty i dokumenty robocze (`docs/superpowers/`, `wnioski_claude.md`, `raporty/`).
Silniki są w wersjach z maja–września 2026 (Ballista 53, DataFusion 53, polars-bio 0.36.0,
Sail 0.7.2).

**Cel etapu:**

1. Obie integracje działają na **bieżących wersjach** silników.
2. Każda integracja jest w **osobnym repozytorium**, które da się zrozumieć i uruchomić
   samodzielnie, z **dokładnym diagramem przebiegu integracji** w README.
3. Trzecie repozytorium zawiera **procedurę testową**, która jednym poleceniem daje wyniki
   (na razie local mode) dla polars-bio, Ballisty i Saila przy **P = 1, 2, 4, 8**.
4. Repozytoria są publiczne, z czystą historią, bez dokumentów roboczych.

**Odbiorca repozytoriów:** doświadczony programista (ekosystem DataFusion, polars-bio), który
chce sam sprawdzić, jak działa integracja, i uruchomić procedurę na **Linuksie** (założenie;
macOS poza zakresem — patrz sekcja 9).

**Termin:** w ciągu tygodnia. Minimum: etapy A–C
(sekcja 8); etap D (pierwsza seria z laptopa) — jeśli starczy czasu.

## 2. Decyzje (brainstorming 15.11.2026)

| Kwestia | Decyzja |
|---|---|
| Wersje | **Wariant A:** Ballista 55.0.0 + vendor datafusion-bio-functions **v0.25.0** (DataFusion =55.2.0) z obiema łatkami widoczności; polars-bio **0.37.0** od razu; po wydaniu polars-bio 0.38 — drugie, małe podbicie (sekcja 4.4) |
| Sail | pysail 0.7.3, PySpark 4.2.1 (nie 4.3 — Sail testuje się na 4.2) |
| Integracja Saila w nowym repo | **Tylko applyInArrow z pulą kontekstów** (`sail_arrow` tryb `context` + `sail_pb_context`). Bez UDTF (`sail_*_udtf.py`, `sail_bio.py`, wariant `sail_udtf`) i bez wariantu z blokadą (`sail_pb_guard`, `sail_lock`) |
| Semantyka P | **Interpretacja 1:** polars-bio = 1 proces z P wątkami; Ballista = P executorów × 1 slot; Sail = P workerów × 1 slot — równe zasoby przy każdym P |
| Układ CPU | **Wariant (a):** wyznaczany z topologii maszyny; rdzeń systemowy osobno; najpierw po jednym wątku na każdy rdzeń fizyczny, potem drugie wątki SMT; przebieg używający rodzeństwa SMT jest oznaczony |
| Repozytoria | Trzy publiczne repozytoria na koncie użytkownika: `ballista-genomics`, `sail-genomics`, `genomics-engines-bench`; **czysta historia** (kod jako pierwszy commit), licencja **Apache-2.0** |
| Zależność repo testowego | **Submoduły gita** (przypięte commity) |
| Kolejność | Najpierw podbicie wersji i zmiana semantyki P w `distributed-polars-bio` (pełny pakiet testów + CI), dopiero potem podział; **podział nie czeka na port Ballisty** po terminie odcięcia (sekcja 8) |
| Raport | Nowe polecenie: z serii → tabela median (silnik × operacja × para × P) i wykresy czasu od P |
| Treść nowych repozytoriów | Bez `docs/superpowers/`, `wnioski_claude.md`, `raporty/`, `ustalenia_zalozen.txt`, bez dokumentów roboczych, bez planów i specyfikacji |
| Commity | **Bez stopki `Co-Authored-By`** (żadnej atrybucji Claude) — we wszystkich repozytoriach; w nowych repozytoriach komunikaty **po angielsku**, w `distributed-polars-bio` po polsku bez diakrytyków (jak dotąd) |
| Język | **Nowe repozytoria w całości po angielsku** (decyzja użytkownika 15.11.2026): kod, komentarze, docstringi, README, dokumentacja techniczna, komunikaty commitów; strażnik języka w nowych repozytoriach zaostrzony (żadnych polskich znaków poza danymi testowymi). Komentarze tłumaczone w `distributed-polars-bio` przed podziałem (skrypt podziału bierze kod stamtąd), razem z usuwaniem odwołań do dokumentów roboczych. Dokumenty robocze (specyfikacje, plany, sprawozdania) zostają po polsku |
| GitHub | Użytkownik zakłada trzy repozytoria **prywatne z README** (opcja „Add a README file”; bez `.gitignore` i licencji) — `main` ma wtedy jeden commit użytkownika. Kod trafia na gałąź `import` (push przez SSH **tylko za zgodą**), użytkownik otwiera **PR do `main`**, przegląda diff na GitHubie (CI na PR) i scala **„Create a merge commit”** (zachowuje hashe — potrzebne submodułom). Po scaleniu zmienia widoczność na **publiczną**. Kolejność: `ballista-genomics`, `sail-genomics`, potem `genomics-engines-bench` (CI repo testowego klonuje submoduły, więc oba muszą być już publiczne). Zaproszenia współpracowników po upublicznieniu wszystkich trzech |
| Stare repo | `distributed-polars-bio` zostaje publiczne jako warsztat: specyfikacje, plany, raporty, historia; ta specyfikacja i plan etapu trafiają tam |

## 3. Wersje i zgodność (rozpoznanie 15.11.2026)

| Składnik | Dziś w repo | Cel | DataFusion |
|---|---|---|---|
| Ballista (`ballista`, `-core`, `-scheduler`, `-executor`) | 53.0.0 | **55.0.0** (06.10.2026) | ^55 |
| `datafusion`, `datafusion-proto` (Rust) | =53.0.0 | **=55.2.0** (wymusza vendor) | 55.2 |
| datafusion-bio-function-ranges (vendor) | v0.22.2 | **v0.25.0** | =55.2.0 |
| polars-bio (PyPI) | 0.36.0 | **0.37.0** (07.11.2026) | 53 (bio-functions v0.24.0) |
| pysail | 0.7.2 | **0.7.3** (30.10.2026) | 55.2 (wewnątrz; algorytmy liczy polars-bio) |
| pyspark[connect] | 4.2.0 | **4.2.1** | — |
| polars | 1.44.2 (`<2`) | **2.1.x**, jeśli kod narzędzia przejdzie (polars-bio 0.37 dopuszcza `<2.2`) | — |
| pyarrow | 24.0.0 | 24.x (polars-bio wymaga `<25`) | — |
| datafusion (Python) | 53.0.0 | 53.x (polars-bio 0.37 wymaga `<54`) | — |

**Zasada tych samych algorytmów** (3b-1): Ballista i polars-bio mają liczyć tym samym kodem
algorytmów przedziałowych. Stan w wariancie A:

- Kod crate'a `datafusion-bio-function-ranges` **nie zmienił logiki** między v0.22.2 a v0.24.x
  (zmienione tylko numery wersji w `Cargo.toml`).
- v0.24.x → v0.25.0: wyłącznie przeniesienie na API DataFusion 55 (`as_any` usunięte,
  `apply_expressions`, `Distribution::KeyPartitioned` zamiast `HashPartitioned`) oraz zmiana
  funkcji haszującej klucze równościowe w `IntervalJoinExec` z `ahash::RandomState::with_seeds(0,0,0,0)`
  na `datafusion::common::hash_utils::RandomState::default()` (foldhash; deterministyczna). Wynik
  operacji się nie zmienia; wydajność może się minimalnie różnić.
- v0.25.0 → v0.26.0: bez zmian w crate'cie ranges.
- **Okres przejściowy:** polars-bio 0.37.0 liczy na DataFusion 53 z bio-functions v0.24.0, Ballista
  na DataFusion 55.2 z v0.25.0. To jawnie opisana asymetria (README repo testowego); znika
  z polars-bio 0.38 (gałąź główna polars-bio od 14.11.2026 ma DataFusion 55.2 i bio-functions
  v0.25.0; wydania jeszcze nie ma).
- Łatki widoczności (vendor/PATCH.md) **nadal potrzebne** w v0.25.0/v0.26.0 (`mod intervals;`,
  prywatne `struct MergeExec` itd.).

**Zmiany w Ballistcie 54–55 istotne dla nas** (z notatek wydań):

- usunięty writer hash-shuffle (#2106) — shuffle oparty na sortowaniu; partycjonowanie po kluczu
  zostaje, zmienia się implementacja;
- AQE domyślnie włączone (#2315) — może zmienić plan (etapy, broadcast), a więc testy dowodów
  rozproszenia na `EXPLAIN ANALYZE`;
- porządki w konfiguracji (#2387) i jednolicie konfigurowalny rozmiar wiadomości gRPC (#2173) —
  nazwy kluczy, które ustawiamy, mogły się zmienić;
- executor wiąże port gRPC przed rejestracją w schedulerze (#2225) — naprawia wyścig, który
  obchodzimy ponawianiem startu (`BALLISTA_START_ATTEMPTS`); ponawianie może zostać, ale
  przestaje być konieczne;
- „broadcast_join_threshold authoritative” (#2086), broadcast w AQE (#1752) — dotyczą złączeń
  DataFusion, nie naszego broadcastu (Arrow IPC w koderach), ale planista może inaczej
  traktować nasze węzły.

**Zmiany w polars-bio 0.37 istotne dla nas:**

- reguły układu współrzędnych (#490): wejście bez metadanych (nasze ramki z Parquetu)
  daje ostrzeżenie i ustawienie globalne w trybie łagodnym; ramka z metadanymi połączona
  z ramką bez nich → `MissingCoordinateSystemError`. Trzeba sprawdzić, jak nasze runnery
  i `sail_arrow` podają dane, i jawnie oznaczyć układ (databio-8p: 0-based, półotwarte);
- pula kontekstów (`sail_pb_context`) zależy od wewnętrznych `polars_bio.range_op.range_operation`
  (5. argument `ctx`) i `polars_bio.polars_bio.BioSessionContext` — `range_op.py` zmienił się
  w 0.37 (+73 −38), więc podmianę trzeba zweryfikować testem (już istnieje:
  `tests/test_sail_pb_context.py`);
- #494 (osobna tabela na `collect`) dotyczy skanów plików, nie operacji na ramkach — globalny
  kontekst z tabelami `s1`/`s2` prawdopodobnie zostaje; pula nadal potrzebna (do potwierdzenia
  testem współbieżności).

## 4. Etap A — podbicie wersji w `distributed-polars-bio`

Praca na gałęzi w obecnym repo, gdzie są pełny pakiet testów i CI. Etap kończy się zielonym
pakietem lokalnie (z `real_data`) i zielonym CI.

### 4.0 Porządki przed portem (decyzja wykonawcy-projektanta)

Kodu, którego żadne nowe repo nie przeniesie, nie portujemy. Na początku etapu A w
`distributed-polars-bio` lokalny tag `przed-etapem-4` na bieżącym `master`, potem usunięcie:
`sail_bio.py`, `sail_*_udtf.py`, `sail_pb_guard.py`, tryb `lock` w `sail_arrow`, warianty
`sail_lock` i `sail_udtf` (`bench/config.py`, `engines.py`, `sail_runner.py`), konfiguracja
`sail_paths.yaml`, testy tych ścieżek (`test_sail_parallel_udtf.py`, części UDTF
w `test_sail_parquet.py`, `test_sail_runner.py`, `test_real_data.py`; xfail regresji LATERAL).
Wyniki serii `sail-paths` w `bench/results/` zostają (dane historyczne). Warianty
`polars_bio_a`/`polars_bio_b` usuwa etap B (nie wymagają portu). Kod pozostaje w historii gita
i pod tagiem.

### 4.1 Rust (Ballista)

- `Cargo.toml`: Ballista 55.0.0, `datafusion = "=55.2.0"`, `datafusion-proto = "55"`, pozostałe
  zależności (prost, tonic itd.) zgodnie z Ballistą 55.
- Vendor: nowa kopia `datafusion/bio-function-ranges` z tagu **v0.25.0** + obie łatki widoczności
  nałożone procedurą z `vendor/PATCH.md` (różnica względem czystego tagu = tylko `pub`
  i re-eksporty — reguła utrzymaniowa bez zmian). `PATCH.md` zaktualizowany (wersja kopii).
- Nasz kod (`bio_phys_codec.rs`, `physical_codec.rs`, `logical_codec.rs`, `coverage_node.rs`,
  `dist_provider.rs`, `cluster.rs`, binarki) przeniesiony na API DataFusion 54 i 55 oraz
  Ballisty 55.
- **AQE:** najpierw z ustawieniem domyślnym (włączone). Jeśli dowody rozproszenia
  (`test_ballista_distribution_evidence.py`, `test_ballista_multiprocess.py`) pokażą, że AQE
  zmienia strategię którejś operacji (np. inny podział niż hash po chromosomie, brak
  rozproszenia, utrata naszego broadcastu) albo psuje dekodowanie naszych węzłów — wyłączyć AQE
  jawnie w konfiguracji sesji (klient, scheduler, executory) i opisać to jako decyzję
  konfiguracyjną w `OPIS.md`. Jeśli AQE tylko zmienia postać planu przy tej samej strategii —
  zostaje włączone, a asercje testów są aktualizowane do nowej postaci planu.
- Klucze konfiguracji ustawiane przez nas (push, RoundRobin, rozmiar gRPC, `broadcast_bytes`
  z logów) sprawdzone pod nazwami z Ballisty 55; strażnik rozmiaru gRPC
  (`test_ballista_grpc_limit.py`) i `logscan` (linie logu z `broadcast_bytes`) mają działać jak
  dotąd.
- Komentarz `runner.rs:65` (per-task), odłożony w 3b-2 „przy następnej zmianie Rusta” — poprawić
  przy okazji.

### 4.2 Python

- `pyproject.toml`: `polars-bio==0.37.*`, `pysail==0.7.*` (0.7.3 w `uv.lock`), `pyspark[connect]==4.2.*`
  (4.2.1), `polars` — próba `>=2.1,<2.2`; jeśli kod narzędzia lub testy wymagają nietrywialnych
  zmian pod polars 2, zostaje `<2` z komentarzem (decyzja wykonawcy, opisana w commicie).
  `pyarrow` 24.x, `datafusion` 53.x (wynikają z polars-bio).
- Dane do operacji polars-bio i Saila jawnie oznaczone jako 0-based (sekcja 3); żadnych nowych
  ostrzeżeń o układzie współrzędnych w logach serii.
- Pula kontekstów zweryfikowana na 0.37 (test współbieżności 5 operacji; czy błąd
  „table s1 already exists” bez puli nadal występuje — zapisać w `OPIS.md`/README Saila jako
  aktualny stan).
- **Wariant zastępczy (decyzja wykonawcy-projektanta):** jeśli podmiany `range_operation` nie da
  się dostosować do 0.37 w rozsądnym czasie (orientacyjnie pół dnia), ścieżką Saila zostaje
  applyInArrow z **globalnym kontekstem i blokadą** (tryb `lock`, dziś ~10% wolniejszy na 7-0) —
  wtedy `sail_pb_guard` i tryb `lock` przechodzą do `sail-genomics` zamiast puli, a README to
  opisuje. Bez zmian w polars-bio i bez cofania Saila do polars-bio 0.36.

### 4.3 Strażnik wersji algorytmów

`tests/test_algorithm_versions.py` zostaje strażnikiem, ale dostaje **jawny wyjątek okresu
przejściowego**: dla polars-bio 0.37.0 oczekiwany vendor to v0.25.0 (pb ma v0.24.0), z
uzasadnieniem w kodzie (logika crate'a identyczna, różnica = port na DataFusion 55; sekcja 3).
Każda inna kombinacja wersji nadal oblewa test. Wyjątek znika przy polars-bio 0.38.

### 4.4 Po wydaniu polars-bio 0.38 (poza zakresem tego etapu, instrukcja na później)

Podbicie polars-bio w repozytoriach Saila i testowym; odczyt tagu bio-functions z `Cargo.toml`
polars-bio 0.38 — jeśli inny niż v0.25.0 (np. v0.26.0, bez zmian w ranges), podmiana vendora
w repo Ballisty i przesunięcie submodułu; usunięcie wyjątku z 4.3; pełny pakiet testów; nota
w README repo testowego, że asymetria DataFusion 53/55 zniknęła.

## 5. Etap B — semantyka P w narzędziu pomiarowym

Zmiana w `bench/` (jeszcze w `distributed-polars-bio`), tak żeby podział (etap C) był już tylko
przenoszeniem plików.

### 5.1 Warianty i parametr

- Warianty serii: **`polars_bio`**, **`ballista`**, **`sail`**. Usunięte: `polars_bio_a`,
  `polars_bio_b`, `sail_lock`, `sail_udtf`.
- Klucz konfiguracji `nodes` → **`parallelism`** (lista P, domyślnie `[1, 2, 4, 8]`); kolumna
  wyników `n_nodes` → **`parallelism`**.
- **polars-bio:** jeden proces przypięty do P CPU z układu (5.2), `POLARS_MAX_THREADS = P`,
  `target_partitions = P`. Przy P = 1 to punkt odniesienia jednowątkowy (dawny wariant A był
  2-wątkowy — nie jest tym samym; opisać w README).
- **Ballista:** scheduler i klient (`bench_client`) na rdzeniu systemowym; P executorów, każdy
  `--concurrent-tasks 1`, każdy przypięty do **jednego** CPU z układu; `BIO_TARGET_PARTITIONS = P`.
- **Sail:** jeden proces serwera (local-cluster) przypięty do P CPU z układu,
  `SAIL_CLUSTER__WORKER_INITIAL_COUNT = P`, `SAIL_EXECUTION__DEFAULT_PARALLELISM = P`, docelowo
  `SAIL_CLUSTER__WORKER_TASK_SLOTS = 1`. **Ryzyko znane z 01.10.2026:** przy małej liczbie
  slotów Sail dokładał workery ponad N, a z limitem `worker_max_count` zapytanie wisiało. Na
  początku etapu B sonda na Sailu 0.7.3 (P = 1, 2, 4; overlap 7-0): liczba workerów z logu
  (`sail_workers`) i poprawność. Jeśli 1 slot daje dodatkowe workery albo zawieszenie —
  zostają sloty jak dziś (8), a równoległość ogranicza przypięcie do P CPU i `DEFAULT_PARALLELISM = P`;
  decyzja i wynik sondy opisane w README repo testowego.
- **Wzorzec poprawności** (`polars_bio_ref`, 1 partycja) i **przebieg kontrolny** zostają bez
  zmian merytorycznych: kontrola = `overlap` 7-0, polars-bio 2 wątki, przypięta do obu wątków
  pierwszego niesystemowego rdzenia (konfiguracja zbadana w 3b-2: szum 1,8% z `settle_s`).

### 5.2 Układ CPU z topologii

- Odczyt z `/sys/devices/system/cpu/cpu*/topology/` (`core_id`, `thread_siblings_list`,
  `physical_package_id`), z uwzględnieniem maski `sched_getaffinity` procesu.
- **Rdzeń systemowy** = pierwszy rdzeń fizyczny (wszystkie jego wątki): scheduler Ballisty,
  klienci, orkiestrator.
- **Kolejność CPU dla P:** pierwsze wątki rdzeni 1, 2, …, C−1, potem drugie wątki tych rdzeni
  w tej samej kolejności. Laptop (6 rdzeni, 12 wątków, rdzeń k = CPU 2k, 2k+1): P = 1 → {2};
  2 → {2, 4}; 4 → {2, 4, 6, 8}; 8 → {2, 4, 6, 8, 10, 3, 5, 7}.
- Kolumny wyników: `cpus` (lista CPU bloku) i **`smt`** (prawda, gdy zbiór zawiera rodzeństwo
  SMT). Raport i wykresy wyróżniają punkty `smt`.
- P większe niż liczba CPU poza rdzeniem systemowym → blok pominięty z powodem
  (`invalid_reason = "skipped: P exceeds available CPUs"`), seria trwa dalej.
- Preflight sprawdza zamiast sztywnych par z metodyki (`check_cpus`): czytelność topologii,
  dostępność `taskset` i to, że mieści się **co najmniej P = 1** — tylko wtedy przerywa serię.
  Za duże P nie przerywają: preflight wypisuje ostrzeżenie z listą P, które zostaną pominięte
  (decyzja wykonawcy-projektanta: domyślna konfiguracja `scaling` ma działać na mniejszej maszynie
  i pokazać luki w raporcie).
- Znacznik testów `nodes(n)` → **`parallelism(p)`**: test pomijany, gdy maszyna nie ma P CPU poza
  rdzeniem systemowym (CI: 4 vCPU → do P = 2).

### 5.3 Raport

- Polecenie `uv run python -m bench.report <katalog serii>` → w katalogu serii:
  `summary.md` (tabela median i rozrzutu: operacja × para × silnik × P, liczba ważnych przebiegów,
  `smt`, przyspieszenie względem P = 1 danego silnika i względem polars-bio P = 1) oraz
  `summary.csv`, a także wykresy PNG: czas (mediana) od P, jedna linia na silnik, osobny wykres
  na operację × parę; oś P w skali log2; punkty `smt` wyróżnione.
- Liczone tylko z przebiegów ważnych; komórka bez ważnych przebiegów pokazana jako brak
  z powodem (np. swap, dryf kontroli).
- Zależność `matplotlib` w `pyproject` repo testowego (backend `Agg` jak dziś).

### 5.4 Domyślna seria

`bench/conf/scaling.yaml`: operacje `overlap`, `nearest`, `coverage`, `subtract` na parach
**2-7** i **7-0**, `merge` na zbiorach **7** i **0**; warianty `polars_bio`, `ballista`, `sail`;
`parallelism: [1, 2, 4, 8]`; liczba powtórzeń, rozgrzewka i powtórzenia kontroli dobrane tak,
żeby seria trwała **≤ 4 h** na laptopie (plan podaje rachunek; orientacyjnie: 120 bloków,
przebiegi 0,2–3 s + ~1 s narzutu + 3 s `settle_s`). Pary większe niż 7-0 (np. zbiory 4, 5, 6, 8)
— poza zakresem tego etapu (pamięć 5 GB, skala danych do ustalenia).

Ostrzeżenie pamięciowe: przy P = 8 Ballista trzyma kopię strony broadcastowanej w każdym
executorze (`nearest`/`coverage`); jeśli przebieg sięgnie swapu, jest nieważny (reguła z 3b-2).
Wynik serii z takimi komórkami jest poprawny — raport pokazuje brak z powodem.

## 6. Etap C — podział na trzy repozytoria

### 6.1 `ballista-genomics`

- **Zawartość:** crate `ballista_genomics` (cały `src/`, w tym `ballista_node`, `bench_client`,
  `dist_ops`, binarki `*_local` używane w testach), `vendor/` z `PATCH.md`, `data/` (małe dane
  demonstracyjne testów), `rust-toolchain.toml`, `Cargo.lock`, testy Ballisty
  (`test_ballista_*.py`, `test_bench_client_protocol.py`, pomocnicze wyrocznie i fikstury, których
  używają), `OPIS.md` przepisany na dokumentację techniczną integracji (bez historii faz
  i odniesień do planów).
- **Środowisko testów:** własne `pyproject.toml` + `uv.lock` (pytest, polars-bio, pyarrow, polars
  — tyle, ile potrzebują testy).
- **Licencja:** `LICENSE` (Apache-2.0) + tekst licencji Apache-2.0 i informacja o pochodzeniu dla
  `vendor/datafusion-bio-function-ranges` (dziś brak — kopia ma tylko `superintervals/LICENSE`).
- **CI:** build binarek, `cargo test --lib`, pytest (bez danych rzeczywistych); protoc jak dziś.
- **README** (po angielsku): czym jest integracja; **diagram przebiegu zapytania** (6.4); tabela
  strategii rozproszenia na operację (overlap — `CollectLeft` z indeksem z całej lewej strony,
  merge/subtract — podział po chromosomie, nearest/coverage — broadcast lewej tabeli przez Arrow
  IPC); co jest nasze, co z Ballisty/DataFusion, co z bio-functions, czego dotyczą łatki;
  budowanie (rustup, protoc, `cargo build --release --bins`, czas buildu); uruchomienie przykładu
  (`dist_ops` na danych z `data/`, tryb wieloprocesowy z `ballista_node`); testy.

### 6.2 `sail-genomics`

- **Zawartość:** integracja applyInArrow jako **instalowalny pakiet Pythona** (np. `sail_genomics`
  z modułami odpowiadającymi `sail_arrow` w trybie `context` i `sail_pb_context`; tryb `lock`
  i `sail_pb_guard` usunięte). Moduł musi być importowalny po nazwie w procesie serwera Saila
  (cloudpickle serializuje funkcje modułu przez referencję) — dlatego pakiet, nie skrypt.
  Testy Saila (`test_sail_arrow.py`, `test_sail_pb_context.py`, `test_sail_runner.py`
  w części dotyczącej ścieżki arrow, wyrocznie i fikstury). Testy i kod UDTF/LATERAL
  (`sail_bio.py`, `sail_*_udtf.py`, `test_sail_parallel_udtf.py`, `test_sail_parquet.py`
  w części UDTF, xfail regresji LATERAL) **nie** są przenoszone.
- **Środowisko:** `pyproject.toml` (`package = true`) + `uv.lock`: pysail 0.7.3, pyspark[connect]
  4.2.1, polars-bio 0.37.0, pyarrow, polars.
- **CI:** `uv sync --locked`, pytest.
- **README** (po angielsku): **diagram przebiegu zapytania** (6.4); dlaczego applyInArrow, a nie UDTF
  — jeden akapit z liczbami z serii `sail-paths` 3b-2 (overlap 7-0, N = 1: UDTF 41,6 s vs
  applyInArrow z pulą 2,57 s; LATERAL powiela wiersz zewnętrzny → pamięć kwadratowa; brak
  argumentów TABLE w UDTF); dlaczego pula kontekstów (globalny kontekst polars-bio, stałe nazwy
  tabel); ograniczenia (local-cluster = jeden proces, prawdziwa wieloprocesowość wymaga
  Kubernetesa); użycie z PySparka (przykład), testy.
- **Schemat wyniku (decyzja wykonawcy-projektanta):** applyInArrow wymaga schematu wyjścia
  z góry, więc pakiet **sam definiuje schemat wyniku każdej operacji** — ten sam co dzisiejszy
  schemat znormalizowany (`chrom_1, start_1, …`, `bench/ops.py: OUTPUT_COLUMNS`) — i robi
  przemianowanie kolumn polars-bio wewnątrz funkcji (jak dziś). Nazwy kolumn wejścia
  (`contig, pos_start, pos_end` w databio-8p) są parametrem z wartością domyślną. Pakiet nie
  importuje niczego z `bench`. Repo testowe zachowuje własną definicję schematu (potrzebną też
  Ballistcie i sumie kontrolnej), a jego test sprawdza, że jest identyczna ze schematem pakietu.
- **Liczby historyczne bez linku (decyzja użytkownika: nowe repozytoria nie odsyłają do
  `distributed-polars-bio`):** akapit „dlaczego nie UDTF” podaje pomiar z datą i warunkami
  (11.10.2026, laptop, Sail 0.7.2) jako wynik historyczny — kodu UDTF w repo nie ma.

### 6.3 `genomics-engines-bench`

- **Zawartość:** pakiet `bench/` (orkiestrator, runnery, `conf/`, pobieranie danych, raport),
  testy narzędzia (`tests/bench/`, `test_orchestrator_integration.py`, `test_polars_bio_runner.py`,
  `test_real_data.py`, `test_env_isolation.py`, `test_nodes_marker.py` → `parallelism`,
  `test_algorithm_versions.py` — sprawdza zgodność polars-bio z vendorem w submodule Ballisty),
  strażnik języka.
- **Submoduły:** `ballista-genomics` i `sail-genomics` (np. w `engines/`), przypięte commity.
  Sail jako zależność ścieżkowa (`[tool.uv.sources]`, editable) w `pyproject`; binarki Ballisty
  budowane z submodułu, narzędzie szuka ich w jego `target/`.
- **Jedno polecenie przygotowania** (skrypt, np. `./setup.sh`): `git submodule update --init`,
  `uv sync --locked`, `cargo build --release --bins` w submodule Ballisty, pobranie databio-8p.
  README: `git clone --recursive …` → `./setup.sh` → seria → raport.
- **Wyniki:** konfiguracje `smoke` (szybkie sprawdzenie, ~kilka minut) i `scaling` (5.4) oraz
  narzędzia sprawdzające protokół, dostosowane do nowych wariantów: badanie szumu przebiegu
  kontrolnego (`bench/control_noise.py` + `control_noise.yaml` — źródło liczby 1,8% w README)
  i `broadcast_check.yaml` (rozmiar broadcastu Ballisty przy P = 1…8).
  Wyniki oficjalnej serii (etap D) commitowane w repo (`results/…/runs.parquet`, `summary.md`,
  wykresy), żeby dało się je obejrzeć bez uruchamiania.
- **CI:** checkout z submodułami, build Ballisty, `uv sync --locked`, `pytest -m "not real_data"`.
- **README** (po angielsku): co mierzy procedura (P, równe zasoby, układ CPU z topologii, SMT),
  protokół przebiegu (rozgrzewka, kontrola, `settle_s`, swap → nieważny, wzorzec poprawności
  i suma kontrolna), wymagania maszyny (Linux x86_64, uv, rustup, protoc, RAM), jak czytać
  `summary.md`, znane ograniczenia (asymetria DataFusion 53/55 do polars-bio 0.38; klient Ballisty
  w Ruście, Saila w Pythonie; local mode), schemat elementów (który proces gdzie, na których CPU).

### 6.4 Diagramy

- **Mermaid** w README (GitHub renderuje bez narzędzi).
- Każde repo integracji: (1) **diagram sekwencji** jednego zapytania od klienta do wyniku,
  (2) **schemat elementów** z podziałem: nasz kod / silnik / DataFusion / polars-bio lub
  bio-functions. Repo testowe: schemat procesów i CPU przy danym P dla każdego silnika.
- **„Dokładny”** znaczy: każda strzałka i każdy węzeł odpowiada nazwanemu plikowi, funkcji, typowi
  albo komunikatowi (np. `BioPhysicalCodec::try_encode`, `IntervalJoinExec`, `cogroup(...).applyInArrow`,
  `range_operation` z kontekstem z puli), a miejsca przejścia danych Rust↔Python i serializacji
  planu są zaznaczone. Diagramy sprawdzone z kodem i z `EXPLAIN ANALYZE` / logami po podbiciu
  wersji (AQE może zmienić etapy) — nie z pamięci.

### 6.5 Procedura podziału

- Pliki kopiowane z `distributed-polars-bio` po zakończeniu etapów A i B (ten sam commit
  źródłowy dla wszystkich trzech); w nowym repo jeden commit początkowy „import kodu”
  z odniesieniem tylko do opisu technicznego (bez odwołań do planów).
- Przed pierwszym wypchnięciem: przegląd treści każdego repo — `grep` po nazwiskach, słowach
  „promotor”, „Claude”, „spec”, „plan”, ścieżkach `docs/superpowers`, `wnioski_claude`,
  `raporty/`, `ustalenia_zalozen` w plikach i komunikatach commitów; komentarze w kodzie
  odwołujące się do „planu 3b-2”, „Zadania 27”, „specyfikacji 8.4” itp. przepisane na treść
  merytoryczną (komentarz ma tłumaczyć „dlaczego”, a nie odsyłać do dokumentu, którego w repo nie
  ma). Lista znalezisk pokazana użytkownikowi przed wypchnięciem.
- Publikacja przez PR (wiersz „GitHub” w sekcji 2): gałąź `import` na `main` z README użytkownika,
  push **po jego zgodzie**, przegląd diffu i CI na PR, scalenie merge commitem, zmiana widoczności
  na publiczną.
- `distributed-polars-bio`: krótka nota w README, że kod rozwija się w trzech nowych
  repozytoriach (linki); reszta bez zmian.

## 7. Etap D — pierwsza seria 1/2/4/8 (jeśli starczy czasu)

- Przed serią **checklista RAM** (zamknięty VS Code, aplikacje w tle, `.wslconfig` 5 GB, pusty swap)
  — seria uruchamiana przez użytkownika z Windows Terminal skryptem przygotowanym w repo testowym.
- Najpierw `smoke`, potem `scaling`; potem `bench.report`.
- Wyniki z laptopa opisane w README jako **wyniki z maszyny walidacyjnej** (6 rdzeni, 5 GB dla WSL2,
  P = 8 z SMT), nie jako wyniki końcowe pracy.

## 8. Kolejność i punkt odcięcia

| Etap | Treść | Priorytet |
|---|---|---|
| A | Podbicie wersji w `distributed-polars-bio`, testy + CI zielone | Python wymagany; Ballista 55 — do terminu odcięcia |
| B | Semantyka P, układ CPU z topologii, raport, konfiguracja `scaling` | wymagane |
| C | Trzy repozytoria, README z diagramami, CI, licencje, wypchnięcie, zaproszenia | wymagane |
| D | Seria `scaling` na laptopie + raport w repo testowym | jeśli starczy czasu |

**Podział nie czeka na port Ballisty (decyzja użytkownika 15.11.2026, wariant a):**

- W etapie A najpierw strona Pythona (Sail 0.7.3, PySpark 4.2.1, polars-bio 0.37, polars,
  porządki z 4.0) — osobny, zielony punkt kontrolny (commit + CI) — potem port Ballisty.
- **Termin odcięcia: 2 dni przed terminem etapu** (data jeszcze nieznana — użytkownik ją poda). Jeśli
  do tego czasu port Ballisty nie ma zielonego pakietu, etap C startuje z Ballistą 53
  (vendor v0.22.2) i Pythonem po podbiciu; strażnik wersji algorytmów dostaje drugi, przejściowy
  wyjątek (vendor v0.22.2 przy polars-bio 0.37.0 — logika crate'a identyczna, sekcja 3), a port
  kończy się już w `ballista-genomics` (z przesunięciem submodułu w repo testowym i aktualizacją
  diagramu Ballisty, jeśli AQE zmieni etapy).
- Dlatego podział (6.5) jest **powtarzalnym skryptem** (źródło: wskazany commit
  `distributed-polars-bio`; wynik: trzy katalogi gotowe do pierwszego commita), a etap B nie
  zależy od wersji Ballisty (semantyka P, układ CPU i raport działają na 53 i 55).
- Jeśli etap A się przedłuża, dopuszczalne jest też skrócenie B do minimum potrzebnego procedurze
  (warianty, P, układ CPU) i dosunięcie raportu w kolejnym kroku.

## 9. Poza zakresem

- macOS i Docker (założenie: odbiorca ma Linuksa; wrócimy do tematu, jeśli okaże się inaczej).
- Klaster wieloprocesowy dla Saila (Kubernetes), chmura, dane większe niż 7-0, COST jako osobna
  metryka (P = 1 polars-bio jest punktem odniesienia, ale metryki nie liczymy), `algorithm`
  (indeksy w overlap), operacje `cluster` i `complement`.
- Zmiany w kodzie polars-bio (zasada: obejścia we własnym repo; zmiana w polars-bio tylko po
  wyraźnej zgodzie użytkownika).
- Przenoszenie `deploy/` (GCP, docker-compose) — zostaje w `distributed-polars-bio`.

## 10. Ryzyka

| Ryzyko | Skutek | Postępowanie |
|---|---|---|
| Port na DataFusion 54+55 i Ballistę 55 większy niż zakładany | etap A zjada czas przed terminem etapu | termin odcięcia 2 dni przed terminem etapu, podział z Ballistą 53 (sekcja 8); stan i blokery na bieżąco do użytkownika |
| AQE zmienia plany naszych operacji | dowody rozproszenia nieaktualne albo inna strategia | reguła z 4.1 (AQE wyłączone jawnie, jeśli zmienia strategię) |
| Sail przy 1 slocie dokłada workery / wisi | P workerów nie da się wymusić | sonda na starcie etapu B, wariant zastępczy z 5.1 |
| polars-bio 0.37: reguły układu współrzędnych, zmiany w `range_op.py` | błędy, ostrzeżenia, przesunięcie o 1, pula przestaje działać | jawne oznaczenie 0-based; testy poprawności vs wzorzec i test puli |
| Pamięć przy P = 8 | komórki nieważne (swap) | raport pokazuje brak z powodem; bez obniżania progu |
| polars-bio 0.38 nie wyjdzie przed terminem etapu | asymetria DataFusion 53/55 | opisana w README; instrukcja 4.4 |
| Pozostałości dokumentów roboczych w nowych repo | treść, której użytkownik nie chce pokazywać | przegląd z 6.5 przed wypchnięciem |
| Zimny build release po zmianie DataFusion (≥ 1 h przy `CARGO_BUILD_JOBS=1`), drugi build w submodule | czas, dysk (dziś 29 GB w `target/`, wolne 156 GB) | lokalnie wspólny `CARGO_TARGET_DIR` dla monorepo i submodułu, jeśli to nie psuje strażnika nieaktualnych binarek; inaczej osobny build |

## 11. Co zostaje wykonawcy

- Dokładne nazwy modułów pakietu Saila, katalog submodułów, nazwy skryptów i plików wyników.
- Liczby powtórzeń, rozgrzewki i kontroli w `scaling.yaml` w granicy ≤ 4 h (z rachunkiem w planie).
- Podział testów i wyroczni między repozytoria (dopuszczalne powielenie kilku małych plików
  pomocniczych zamiast czwartego, wspólnego pakietu).
- Szczegóły CI (cache, czasy), byle każde repo miało zielone CI przed zaproszeniem.
- Forma wykresów (z zachowaniem: mediana, oś P log2, wyróżnione `smt`, linia na silnik).

# Etap 3b-2: narzędzie gotowe do pomiarów — projekt

- **Data:** 9 października 2026
- **Status:** zatwierdzony (09.10.2026); plan: `docs/superpowers/plans/2026-10-09-narzedzie-gotowe-do-pomiarow-3b2.md`; wykonany 11.10.2026
- **Zakres:** protokół pomiaru (czas bez sumy kontrolnej, przebieg kontrolny, swap), nowa ścieżka
  Saila (`applyInArrow` z pulą kontekstów polars-bio), Ballista gotowa do pomiarów
  (build release, limity gRPC, `broadcast_bytes`, `EXPLAIN ANALYZE`), fazy, drobne poprawki
  z przeglądów planów 3a i 3b-1
- **Powiązane:** `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` (metodyka),
  `docs/superpowers/specs/2026-10-02-srodowisko-uv-ci-design.md` (3b-1, sekcja 1 — podział 3b),
  `docs/superpowers/plans/2026-10-01-narzedzie-pomiarowe-3a.md` (plan 3a),
  `wnioski_claude.md` (obserwacje planów 3a i 3b-1), `ballista_genomics/OPIS.md`

## 1. Kontekst i cel

Etap 3b jest podzielony na trzy podprojekty (specyfikacja 3b-1, sekcja 1). 3b-1 (środowisko uv,
aktualne wersje, CI) jest wykonany i scalony. Ten dokument opisuje **3b-2**; po nim **3b-3**
(kalibracja, konfiguracje `kalibracja/p1/p2/p3`, parametr `algorithm`).

Narzędzie pomiarowe z planu 3a działa: smoke z 04.10.2026 (103 przebiegi, Ballista w buildzie
debug) dał zero błędnych wyników. Liczby z niego nie nadają się jednak jeszcze do pracy:

- **przebieg kontrolny jest za krótki** — `overlap` 1-2 w polars-bio A trwa ~0,13 s, więc zwykły
  szum przekracza próg 10%: 56 ze 103 przebiegów odrzucono, choć maszyna nie zwolniła;
- **Sail mierzy głównie nasz kod pośredniczący** — 2,4–6,4 s na parze 1-2 wobec 0,05–0,22 s
  polars-bio i 0,5–2,5 s Ballisty; czas nie zmienia się z N (`overlap`: 5,4 / 5,8 / 5,5 s);
- **Ballista jest w buildzie debug**, bez optymalizacji kompilatora;
- **komórki z broadcastem** (`nearest`, `coverage` na 2-7 i 7-0) przekraczają domyślny limit
  wiadomości gRPC 16 MiB — zapytanie wisi;
- **suma kontrolna jest w mierzonym czasie**, a jej koszt zależy od języka runnera (Python albo
  Rust), nie od silnika;
- brakuje faz, `broadcast_bytes`, reguły dla odczytów ze swapu i liczby workerów Saila;
- z przeglądów planów 3a i 3b-1 zostało ~20 drobnych uwag.

**Cel:** narzędzie, którego wyniki można wstawić do rozdziału eksperymentalnego — tak, żeby 3b-3
sprowadzało się do napisania konfiguracji i uruchomienia serii.

## 2. Decyzje (brainstorming 08–09.10.2026)

| Kwestia | Decyzja |
|---|---|
| Ścieżka Saila | **maksymalna optymalizacja** w granicach publicznego API Saila, PySparka i polars-bio, bez zmian w ich kodzie (ta sama reguła co dla Ballisty: build i konfiguracja). Główna ścieżka: `groupBy/cogroup(...).applyInArrow`; stary UDTF zostaje w repo i jest porównany raz |
| Blokada polars-bio w Sailu | **rozwiązanie pomostowe we własnym repo**: pula kontekstów polars-bio (kontekst na równoczesne wywołanie, nie na wątek — sekcja 5.1), bez blokady. Globalny kontekst polars-bio jest docelowo do zmiany, ale **bez zmian w polars-bio w tym etapie**; każda ewentualna zmiana w polars-bio wymaga wyraźnego, potrójnego potwierdzenia użytkownika |
| Wersja polars-bio | zostaje **0.36.0** (`uv.lock`); w `pyproject.toml` dochodzi `polars<2` (polars 2.0 z 06.10.2026 psuje polars-bio; 0.36.1 tylko to przypina) |
| Czas pomiaru | zegar stop po **ostatniej partii**; suma kontrolna liczona **po** zatrzymaniu zegara; `t_total_s` = metryka główna, `checksum_s` zapisywany informacyjnie (czas „z sumą” = suma obu) |
| Przebieg kontrolny | `overlap` **7-0** w polars-bio A, **10** powtórzeń na początku i na końcu bloku (`control_repeats`), porównanie **median**, próg **10%**; próg sprawdzany badaniem szumu |
| Swap | seria startuje od **pustego swapu**; przebieg z przyrostem `pswpout` **albo** `pswpin` jest nieważny („wszystko, co dotknie swapu, jest do wyrzucenia”) |
| Klient Saila | **oficjalny klient PySpark** (Spark Connect), dostrojony konfiguracją; odbiór wyniku jako osobna faza; asymetria wobec klienta Ballisty w Ruście opisana jawnie |
| Kształt etapu | jedna specyfikacja, wykonanie etapami (sekcja 8); każdy etap z zielonym pakietem testów i osobnym commitem; pomiary na końcu, po checkliście RAM |
| Drobne poprawki | cała lista z sekcji 7, bez pozycji 22 (czas zadania po stronie schedulera Ballisty) |

## 3. Ustalenia z sondy (08–09.10.2026)

Sondy na tej maszynie (skrypty jednorazowe, poza repozytorium; czasy orientacyjne — VS Code
otwarty, bez przypięcia klienta):

1. **`applyInArrow` w Sailu 0.7.2 działa i jest poprawne.** `groupBy("chrom").applyInArrow`
   (`merge`) i `cogroup(...).applyInArrow` (pozostałe operacje) dają wynik zgodny z polars-bio
   (liczba wierszy i suma kontrolna) dla pięciu operacji przy N = 1 i N = 3. Na parze 1-2 są
   10–20 razy szybsze od UDTF:

   | Para 1-2, N = 1 | UDTF (`sail_bio.py`) | Arrow |
   |---|---|---|
   | overlap | 5,61 s | 0,33 s |
   | nearest | 6,33 s | 0,46 s |
   | coverage | 5,75 s | 0,31 s |
   | merge (zbiór 1) | 2,36 s | 0,23 s |
   | subtract | 5,71 s | 0,28 s |

2. **polars-bio 0.36 nadal ma jeden globalny kontekst.** Operacje na ramkach rejestrują tabele
   pod stałymi nazwami `s1`/`s2`; równoczesne wywołania w wątkach kończą się błędem
   `The table s1 already exists`. Osobny `BioSessionContext` na wywołanie daje wynik identyczny
   z `pb.*()` i prawdziwą równoległość (GIL zwalniany w trakcie liczenia): `overlap` 7-0 na
   ramkach chromosomów — 2,4× na 6 wątkach.
3. **Podmiana kontekstu w jednym punkcie wystarcza.** Wszystkie pięć operacji (`overlap`,
   `nearest`, `coverage`, `merge`, `subtract`) budują `RangeOptions` i wołają
   `polars_bio.range_op.range_operation(df1, df2, options, output_type, ctx, …)`. Podmiana tej
   funkcji na wersję, która w miejsce `ctx` wstawia kontekst bieżącego wątku, czyni publiczne
   `pb.*()` bezpiecznymi wątkowo (sonda z kontekstem na wątek): 6 wątków, 3 rundy, 24 grupy,
   zero rozbieżności dla pięciu
   operacji.
4. **W Sailu blokada kosztuje dziś ~10%.** `overlap` 7-0, Arrow: z blokadą 6,49 / 6,47 / 5,40 s,
   z kontekstem na wątek 5,91 / 5,57 / 4,79 s (N = 1 / 2 / 3) — liczby z wcześniejszej wersji
   z kontekstem na wątek, zastąpionej pulą (sekcja 5.1, „Zmiana w Zadaniu 27b”).
5. **Najwięcej kosztuje odbiór wyniku przez klienta w Pythonie.** `overlap` 7-0 (2,76 mln
   wierszy), N = 3: skan + shuffle + `cogroup` 0,43 s; liczenie w polars-bio ~0,64 s
   (zapytanie zakończone `count()` — 1,07 s); z przesłaniem wyniku do klienta i sumą kontrolną
   4,5–4,7 s. Wynik przychodzi w **1877 partiach** po ~1,5 tys. wierszy; proces klienta zużywa
   5,4 s CPU, z czego **1,9 s to suma kontrolna** liczona na każdej małej partii osobno.

## 4. Protokół pomiaru (wszystkie silniki)

### 4.1 Czas i suma kontrolna

Wszystkie trzy runnery — `bench/runners/polars_bio_runner.py`, `bench/runners/sail_runner.py`
i `ballista_genomics/src/bin/bench_client.rs` — mierzą tak samo:

1. zegar startuje tuż przed wysłaniem zapytania (jak dotąd);
2. partie wyniku są odbierane i **trzymane w pamięci** (lista partii Arrow / `Vec<RecordBatch>`);
3. czas pierwszej partii → `phases.first_batch_s`;
4. zegar stop po ostatniej partii → `t_total_s` (metryka główna);
5. dopiero potem suma kontrolna i liczba wierszy na wszystkich partiach naraz (małe partie łączone
   przed liczeniem); jej czas → `extra.checksum_s` (informacyjnie, poza `t_total_s`).

Czas odbioru wyniku = `t_total_s − first_batch_s`, liczony jednakowo dla wszystkich silników.
Szczyt pamięci runnera (`peak_rss_bytes`) obejmuje teraz trzymany wynik — w polars-bio runner jest
zarazem silnikiem, co odpowiada zwykłemu użyciu polars-bio (wynik jako gotowa ramka); opis
w metodyce. Zapis wyniku do pliku (`bench_client --output`, używany w testach) — po zatrzymaniu
zegara.

### 4.2 Przebieg kontrolny

- Nowy klucz konfiguracji **`control`** (domyślnie `{op: overlap, pair: "7-0"}`) zastępuje stałą
  `CONTROL` w `bench/config.py`; wariant kontrolny bez zmian (polars-bio A). Zbiory zadania
  kontrolnego wchodzą do sprawdzenia danych przed serią. Testy podmieniają zadanie na zbiór testowy.
- Nowy klucz **`control_repeats`** (domyślnie **10**): tyle przebiegów kontrolnych na początku
  i na końcu każdego bloku.
- **Dryf** = |mediana końca − mediana początku| / mediana początku, liczony z ważnych przebiegów.
  Dryf > `control_tolerance` (domyślnie 0,10) ⇒ blok powtarzany raz; po drugim dryfie przebiegi
  bloku nieważne (jak dotąd, przyczyna `control drift N%`).
- Mniej niż połowa ważnych przebiegów kontrolnych na którymś końcu ⇒ jak dryf (przyczyna
  `control run invalid`).
- Wiersze przebiegów kontrolnych: `is_control = true`, `rep` = 1..`control_repeats`, nowa kolumna
  **`control_position`** (`"start"` / `"end"`; pusta dla pozostałych).
- **Badanie szumu:** konfiguracja `bench/conf/control_noise.yaml` — jeden scenariusz (`overlap`
  7-0), wariant `polars_bio_a`, N = 1, 20 powtórzeń, 1 rozgrzewka. Moduł `bench/control_noise.py`
  liczy z `runs.parquet`, w jakim odsetku losowych podziałów 20 czasów na dwie grupy po 10 różnica
  median przekracza próg — czyli odsetek fałszywych dryfów przy niezmienionej maszynie.
  Kryterium: ≤ 5% (sekcja 10).

### 4.3 Swap

- **Sprawdzenie przed serią:** `SwapTotal − SwapFree` z `/proc/meminfo` musi wynosić 0. Inaczej
  seria nie startuje; komunikat podaje zajętość w MiB i podpowiada `wsl --shutdown`.
- **Nieważność:** przyrost `pswpout` **lub** `pswpin` w trakcie przebiegu (przyczyny
  `swap: pswpout +N`, `swap: pswpin +N`). Przebieg, który wczyta stronę wyrzuconą przez
  wcześniejszy przebieg, też jest nieważny.

### 4.4 Schemat wyników

Bez zmian poza:

- nowe kolumny: `control_position` (4.2), `base_rss` (JSON: proces → RSS tuż po wyzerowaniu
  licznika szczytu; sekcja 7, pozycja 9);
- wypełniana kolumna `broadcast_bytes` (6.3);
- nowy klucz konfiguracji **`settle_s`** (pauza przed każdym przebiegiem, domyślnie 0; sekcja 9.1);
- nowe klucze w JSON-ach: `phases.first_batch_s` (wszystkie silniki), `phases.pb_s`,
  `phases.lock_wait_s` (Sail), `extra.udf_groups`, `extra.sail_workers` (Sail).

## 5. Sail

### 5.1 Pomost: pula kontekstów polars-bio

- Nowy moduł **`sail_pb_context.py`** (katalog główny repo, obok `sail_pb_guard.py`;
  importowalny po nazwie, bo funkcje dla Saila serializuje cloudpickle, a moduły idą przez
  referencję).
- Przy imporcie podmienia `polars_bio.range_op.range_operation` na wersję, która przekazuje dalej
  wszystkie argumenty, ale w miejsce `ctx` wstawia **kontekst z puli**: każde równoczesne wywołanie
  bierze `BioSessionContext` z puli (tworzony, gdy pula jest pusta), po udanym wywołaniu zwraca go
  do puli, po wyjątku porzuca. (Pierwotny projekt: `threading.local`, kontekst na wątek —
  zastąpiony, patrz niżej.)
- Kontekst z puli dostaje te same opcje co globalny kontekst polars-bio (`target_partitions = 1`,
  `schema_force_view_types`, `skip_physical_aggregate_schema_check`, `bio.interval_join_algorithm =
  coitrees`, sprawdzanie układu współrzędnych wyłączone) oraz `coordinate_system_zero_based = true`.
- Kod Saila woła zwykłe, publiczne `pb.overlap()`, `pb.merge()` itd. — z ich własnymi ustawieniami
  domyślnymi; logika budowania `RangeOptions` nie jest kopiowana.
- Podmiana działa wyłącznie w procesie serwera Saila. Runner polars-bio (wzorzec, A, B) jej nie
  importuje.
- Zależność od wnętrza polars-bio: wersja przypięta (`polars-bio==0.36.*`), a test zgodności
  (5.7) wykryje zmianę w nowej wersji.
- Niuans: komentarz przy `OPTIONS` w `sail_pb_context.py` przecenia opcję `zero_based` — o układzie
  współrzędnych decydują metadane ramki (ustawiane przed wywołaniem), opcja kontekstu jest tylko
  zapasowa.

**Zmiana w Zadaniu 27b (11.10.2026): pula zamiast `threading.local`.** Sail woła Python z wątków
Rusta i przy każdym wywołaniu odtwarza stan wątku Pythona, więc kontekst „na wątek” stawał się
nowym kontekstem na każde wywołanie. Sonda 11.10.2026: 162 konteksty na 3 zapytania (54 na
zapytanie `overlap` 7-0 — po jednym na grupę chromosomową) przy 2 wątkach natywnych; RSS serwera
Saila po 12 zapytaniach 0,94 → 2,65 GB (~150 MB na zapytanie) wobec 0,95 → 1,83 GB z pulą
(= `sail_lock`: 0,90 → 1,80 GB); szczyt serwera z pulą 2,21 GB po 24 zapytaniach (blisko
plateau); identyczne wiersze i sumy kontrolne, czasy nieco niższe. Poza Sailem (zwykłe wątki
Pythona) żadna wersja nie rośnie (RSS stały ~0,88–0,95 GB). Odrzucone po drodze hipotezy: limit
czasu sesji Sail (sesje są usuwane zaraz po przebiegu), areny glibc (`MALLOC_ARENA_MAX=2`),
ustawienia alokatora Arrow/jemalloc — bez efektu. Poprzednia próba serii (przerwana, nieprzechowana):
przy wersji na wątek szczyt serwera `sail` N = 1 na 7-0 rósł w jednym bloku 1,32 → 1,91 → 2,08 →
2,52 GB aż do swapu.

### 5.2 Zapytanie

- `merge`: `tabela.groupBy("chrom").applyInArrow(f, schemat)`;
- pozostałe: `lewa.groupBy("chrom").cogroup(prawa.groupBy("chrom")).applyInArrow(f, schemat)`;
- wejście jak dotąd: `spark.read.parquet`, kolumny rzutowane na `chrom` (string), `start`, `end`
  (long);
- `f`: tabela Arrow chromosomu → polars (bez kopii) → oznaczenie 0-based (metadane polars-bio) →
  `pb.<operacja>(…, output_type="polars.DataFrame")` → kolumny w schemacie znormalizowanym
  (`bench/ops.py`) → Arrow rzutowane na zadeklarowany schemat;
- pusta lewa strona ⇒ pusty wynik; pusta prawa ⇒ o wyniku decyduje polars-bio (jak w
  `sail_bio.py`: `nearest` bez sąsiada, `coverage` 0, `subtract` bez zmian);
- znikają `collect_list`, LATERAL i `explode`;
- kod ścieżki Arrow: nowy moduł `sail_arrow.py` (ten sam podział co `sail_bio.py`: `build_query`,
  funkcje dla `applyInArrow`; bez rejestracji, bo `applyInArrow` jej nie wymaga).

### 5.3 Warianty

| Wariant | Ścieżka | Rola |
|---|---|---|
| `sail` | Arrow + pula kontekstów (`sail_pb_context`) | główny, w macierzy P1–P3 |
| `sail_lock` | Arrow + blokada (`sail_pb_guard`) | tylko porównanie |
| `sail_udtf` | UDTF z `sail_bio.py` bez zmian | tylko porównanie |

Wszystkie trzy korzystają z tego samego serwera bloku (`SailEngine`); runner Saila dostaje
argument wybierający ścieżkę. Porównanie: `bench/conf/sail_paths.yaml` — pięć operacji na 1-2
(`merge`: zbiór 1) i `overlap` 7-0, N ∈ {1, 3}, 3 powtórzenia, 1 rozgrzewka. Wynik: sekcja 9.1.

### 5.4 Wielkość partii wyniku (ograniczone czasowo: pół dnia)

Szukanie w publicznej konfiguracji Saila i Spark Connect ustawienia, które zmniejsza liczbę małych
partii wyniku (np. rozmiar partii wykonania Saila, limit wierszy lub bajtów partii Arrow w Spark
Connect). Pomiar na `overlap` 7-0: liczba partii i czas odbioru (`t_total_s − first_batch_s`).
Znalezione ustawienie trafia do `sail_env` (z uzasadnieniem w komentarzu); jeśli żadne nie
działa — opis w `wnioski_claude.md` i w metodyce (asymetria klientów).

### 5.5 Fazy

- `phases.first_batch_s` (4.1);
- z wnętrza `f`: każde wywołanie polars-bio dopisuje do pliku przebiegu (ścieżka w katalogu
  tymczasowym serii, przekazana przez runner w domknięciu funkcji) czas wywołania, a w `sail_lock`
  także czas czekania na blokadę; runner po zapytaniu sumuje wpisy do `phases.pb_s`,
  `phases.lock_wait_s` i `extra.udf_groups` (liczba grup) i usuwa plik;
- `pb_s` sumuje czasy z wielu wątków, więc może przekraczać `t_total_s` — opis przy wynikach.

### 5.6 Liczba workerów

Orkiestrator czyta fragment logu serwera Saila z czasu przebiegu i liczy uruchomione workery sesji
→ `extra.sail_workers`. Format wpisów Saila 0.7.2 ustala plan (po sprawdzeniu logu).

### 5.7 Testy

- pomost: `pb.*()` z podmianą = `pb.*()` bez podmiany (pięć operacji, zbiór testowy), także przy
  6 wątkach bez blokady; pilnuje też zmian wnętrza polars-bio;
- ścieżka Arrow w Sailu zgodna z wyrocznią na zbiorze testowym — pięć operacji, N = 1 i 2,
  warianty `sail` i `sail_lock`;
- fazy: `pb_s > 0`, `udf_groups` = liczba chromosomów; `lock_wait_s` tylko w `sail_lock`;
- dotychczasowe testy UDTF bez zmian;
- sprawdzenie, czy ostrzeżenia PySparka 4.2 przy pandas 3 (`FutureWarning`, `Pandas4Warning`;
  `wnioski_claude.md`, plan 3b-1) dotyczą ścieżki Arrow — wynik w `wnioski_claude.md`.

## 6. Ballista

### 6.1 Build release

- `CARGO_BUILD_JOBS=1 cargo build --release --bins` w **domyślnym profilu release** (opt-level 3,
  bez LTO). Oficjalne paczki polars-bio też powstają w domyślnym profilu (`Cargo.toml` polars-bio
  0.36.0 nie ma sekcji `[profile]`) — ta sama klasa optymalizacji; wersje kompilatora mogą się
  różnić (opis w metodyce).
- Testy i CI zostają na buildzie debug; release służy pomiarom (orkiestrator już dziś domyślnie
  używa `release`).
- Czas pierwszego builda release mierzony i zapisany w `ballista_genomics/OPIS.md`.
- **Strażnik starej binarki** (sprawdzenie przed serią): binarki `ballista_node` i `bench_client`
  wybranego profilu muszą być nowsze od najnowszego pliku w `ballista_genomics/src/`,
  `ballista_genomics/vendor/`, `Cargo.toml` i `Cargo.lock`; inaczej seria nie startuje.
- **Pochodzenie:** sumy SHA-256 binarek w `engine_versions`.

### 6.2 Limity gRPC

- Jeden limit wiadomości, domyślnie **512 MiB**, ze zmiennej **`BALLISTA_GRPC_MAX_MESSAGE_BYTES`**,
  ustawiany publicznymi opcjami Ballisty 53 (specyfikacja metodyki 9.2): scheduler —
  `grpc_server_max_decoding_message_size`; executor — `grpc_max_decoding_message_size`
  i `grpc_max_encoding_message_size`; klient — `ballista.client.grpc_max_message_size`.
  Orkiestrator przekazuje tę samą wartość wszystkim procesom klastra i klientowi.
- Uzasadnienie wartości: tabela rozsyłana (prawa strona `nearest` i `coverage`) to szacunkowo
  ~29 MiB dla 2-7 (ex-anno, 1,19 mln wierszy) i ~56 MiB dla 7-0 (chainRn4, 2,35 mln);
  wiadomość do executora niesie plan etapu raz (nie raz na zadanie), z narzutem, więc potrzebny
  zapas (strażnik: `MESSAGE_SIZE_MARGIN = 2`).
- **Szybki błąd zamiast wiszenia:** po przekroczeniu limitu Ballista 53 (tryb push) nie zgłasza
  błędu zapytania, tylko ponawia (zapytanie wisi do timeoutu 20 min). Tabela rozsyłana jest
  materializowana przy planowaniu fizycznym w schedulerze (`DistCoverageProvider::scan`,
  odpowiednik w `nearest`) — tam sprawdzamy, czy wiadomość startowa (jedna na executor i etap,
  `MultiTaskDefinition`) zmieści się w limicie: rozmiar tabeli × `MESSAGE_SIZE_MARGIN` = 2
  (margines bezpieczeństwa na resztę planu); jeśli nie, planowanie kończy się błędem z rozmiarem
  i limitem, który dociera do klienta jako błąd zapytania. Dokładne miejsce sprawdzenia (planowanie albo
  kodowanie) ustala plan; warunek: zapytanie kończy się czytelnym błędem.
- Plan zapasowy ze specyfikacji metodyki 9.2 (ładunek ze ścieżką do pliku zamiast danych) — tylko
  jeśli podniesienie limitu nie wystarczy, po decyzji użytkownika.

### 6.3 `broadcast_bytes`

Dekodowanie naszych węzłów w executorze (`bio_phys_codec.rs`) loguje rozmiar odebranego ładunku
broadcastu (stały prefiks, poziom info). Orkiestrator sumuje wpisy z logów executorów z czasu
przebiegu → kolumna `broadcast_bytes` (bajty, które faktycznie dotarły: ładunek × liczba
wiadomości startowych — jedna na executor i etap w trybie push — a nie × liczba zadań).
Dla operacji bez broadcastu: 0. Wielkość potrzebna do hipotezy P2.

### 6.4 Fazy

- `phases.first_batch_s` (4.1);
- **`EXPLAIN ANALYZE`** w dodatkowym, niemierzonym przebiegu: klucz konfiguracji **`explain`**
  (domyślnie `false`; `true` w `smoke.yaml`, żeby smoke sprawdzał także ten tryb, i w konfiguracjach
  serii pomiarowych). Po rundach mierzonych każdego
  bloku Ballisty — jeden przebieg `bench_client --explain-analyze <plik>` na scenariusz; surowy
  plan z metrykami do `bench/results/<seria>/<czas>/explain/ballista-n<N>/<scenariusz>.txt`.
  Przebieg nie trafia do `runs.parquet`; błąd zapisywany w podsumowaniu serii. Parsowanie plików —
  analiza (poza 3b-2).

### 6.5 Testy

- przekroczenie limitu: zbiór testowy z obniżonym `BALLISTA_GRPC_MAX_MESSAGE_BYTES` ⇒ czytelny
  błąd zapytania w rozsądnym czasie, nie zawieszenie;
- `broadcast_bytes > 0` dla `nearest` i `coverage`, `0` dla `overlap`, `merge`, `subtract`;
- plik `EXPLAIN ANALYZE` powstaje i zawiera nasze węzły;
- strażnik starej binarki na plikach z ustawionym czasem modyfikacji.

## 7. Drobne poprawki (przeglądy planów 3a i 3b-1 oraz brainstorming 3b-2)

**A. Niezawodność serii**
1. Śmierć procesu silnika (scheduler, executor, serwer Saila) wykrywana w trakcie przebiegu —
   runner przerywany od razu, nie po timeoucie.
2. Po timeoucie albo strażniku pamięci silnik zatrzymywany twardo (SIGKILL), jeśli nie zakończy się
   sam w kilka sekund.
3. Oczekiwanie na start Ballisty obsługuje martwy proces, nie tylko `OSError`; serwer Saila
   ponawia start jak Ballista.
4. Sprawdzenia przed serią: `taskset` dostępny, import `pysail` działa, CPU 0–7 to pary SMT zgodne
   z tabelą węzłów, proces ma te CPU w `sched_getaffinity`.
5. Środowisko procesów silników czyszczone z odziedziczonych `SAIL_*`, `POLARS_MAX_THREADS`,
   `BIO_TARGET_PARTITIONS`, `RUST_LOG` itp. i zapisywane w wynikach; serwer Saila dostaje jawne
   `POLARS_MAX_THREADS`.

**B. Zapis i ślad**
6. Postęp orkiestratora wypisywany z `flush` (także przy przekierowaniu do pliku) oraz linia przy
   starcie każdego przebiegu.
7. Pełny stderr nieudanych przebiegów w `logs/`.
8. `engine_versions` z wersją `datafusion-bio-function-ranges` (v0.22.2), pełną wersją Pythona
   i sumami SHA-256 binarek Ballisty (6.1).
9. Szczyt pamięci procesów długożyjących także jako przyrost: RSS tuż po wyzerowaniu licznika →
   kolumna `base_rss` (rekomendacja przeglądu 3a).
10. `.gitignore`: wyjątek od `*.txt` dla `bench/results/**/explain/` (inaczej plany
    `EXPLAIN ANALYZE` nie trafią do repozytorium).
11. Wyniki serii (`runs.parquet`, pliki explain) w repozytorium zgodnie z metodyką 8.1 — także
    smoke; zacommitowanie smoke z 04.10.2026.

**C. Testy i CI**
12. Testy: żywy runner przy przerwaniu serii; brak wartości w kolumnie chromosomu w sumie
    kontrolnej.
13. Test `nearest` local-vs-dist niezależny od plików `output/` innego testu (dziś pomijany w CI).
14. Test xfail regresji Saila (LATERAL) z kontrolą pozytywną (ten sam kształt, inna nazwa kolumny
    — przechodzi).
15. CI: `protoc` pobierany z `curl -f` i sprawdzeniem SHA-256.
16. `.python-version` przypięty do pełnej wersji (dziś 3.12.15), a strażnik interpretera w `bench`
    sprawdza pełną wersję, nie tylko 3.12.
17. Automatyczne zaliczenie smoke: klucz konfiguracji **`expected_invalid`** (lista: scenariusz,
    wariant, opcjonalnie N i prefiks przyczyny) i na końcu serii podsumowanie
    `smoke OK` / `smoke FAILED` z listą nieoczekiwanych nieważności.

**D. Porządki**
18. `ballista_genomics/src/dist_udtf.rs:151` — komentarz przeczy poprawce `nearest` z 3b-1;
    domyślne `'weak'` w funkcjach `dist_*` zastąpione argumentem obowiązkowym.
19. Specyfikacja metodyki: stare polskie przyczyny nieważności („pominięty”, „seria przerwana”)
    zastąpione obecnymi angielskimi.
20. Podpowiedzi `python -m …` w komunikatach (`bench/config.py`, `tests/conftest.py`) →
    `uv run python -m …`.
21. `polars<2` w `pyproject.toml`.
22. ~~Czas zadania po stronie schedulera Ballisty (`job_execution_time_ms`)~~ — odrzucone
    (pokrywają to częściowo `first_batch_s` i `EXPLAIN ANALYZE`).

## 8. Etapy wykonania

Każdy etap kończy się zielonym pakietem `uv run pytest -m "not real_data"` i osobnym commitem.
Zmiany Rusta budowane z `CARGO_BUILD_JOBS=1`; nie uruchamiać Pythona z polars-bio równolegle
z budowaniem.

1. **Protokół pomiaru** (sekcja 4): czas bez sumy w trzech runnerach, `first_batch_s`, przebieg
   kontrolny (`control`, `control_repeats`, mediany, `control_position`), swap (sprawdzenie przed
   serią, `pswpin`), `bench/conf/control_noise.yaml` i `bench/control_noise.py`.
2. **Sail** (sekcja 5): `sail_pb_context.py`, `sail_arrow.py`, warianty, fazy, liczba workerów,
   `bench/conf/sail_paths.yaml`; dostrojenie partii (5.4) na końcu etapu, w limicie czasu.
3. **Ballista** (sekcja 6): limity gRPC i szybki błąd, `broadcast_bytes`, `EXPLAIN ANALYZE`,
   strażnik starej binarki i SHA-256; na końcu etapu build release (czas zapisany).
4. **Drobne poprawki** (sekcja 7).
5. **Weryfikacja na danych rzeczywistych** (sekcja 9) — po checkliście RAM i potwierdzeniu
   użytkownika.
6. **Dokumentacja:** specyfikacja metodyki (sekcje 3, 5, 6, 8 i przyczyny nieważności),
   `ballista_genomics/OPIS.md`, `wnioski_claude.md` (sekcja „Plan 3b-2”), `README.md`, pamięć
   projektu.

**Zasada zatrzymania:** jeśli któryś etap odsłoni poważny problem (np. ścieżka Arrow w Sailu daje
błędny wynik na danych rzeczywistych, podniesienie limitu gRPC nie wystarcza, badanie szumu
pokazuje, że próg 10% jest za ciasny), wykonawca zatrzymuje się i pyta użytkownika. **Żadnych zmian
w polars-bio** bez wyraźnego, potrójnego potwierdzenia użytkownika.

## 9. Weryfikacja na danych rzeczywistych

Po checkliście RAM (zamknięty VS Code i aplikacje w tle, `.wslconfig` 5 GB, pusty swap — WSL po
`wsl --shutdown`), w tej kolejności:

1. pełny pakiet testów, łącznie z `real_data`;
2. badanie szumu: `uv run python -m bench.orchestrator bench/conf/control_noise.yaml`, potem
   `uv run python -m bench.control_noise <katalog wyników serii>`;
3. smoke na buildzie release (`bench/conf/smoke.yaml`, wszystkie warianty, w tym `sail`);
4. porównanie ścieżek Saila (`bench/conf/sail_paths.yaml`);
5. broadcast Ballisty: `nearest` i `coverage` na 2-7 i 7-0 przy N = 1, 2, 3 — ukończenie,
   `broadcast_bytes`, szczyt pamięci (dane wejściowe także dla kalibracji w 3b-3).

### 9.1 Wyniki (10–11.10.2026)

Maszyna: AMD Ryzen 5 4600H (6C/12T), host Windows 10.0.19045 z 7,4 GB RAM, WSL 3.0.1.0, jądro
6.18.40.1, `.wslconfig` pamięć 5 GB / swap 4 GB; plan zasilania Windows bez zmian (użytkownik nie
może go zmienić), suwak trybu zasilania na najlepszej wydajności, zasilacz (odstępstwo od
metodyki).

**Przerwa `settle_s` (Zadanie 27a).** Klucz konfiguracji: pauza przed każdym mierzonym,
kontrolnym, rozgrzewkowym i wzorcowym przebiegiem (nie przed `EXPLAIN ANALYZE`); domyślnie 0,
w czterech konfiguracjach na laptopie 3,0 s. Powód — zwracanie wolnych stron WSL2 (*free page
reporting*): `hv_balloon` gościa zgłasza zwolnioną pamięć Hyper-V (bloki 2 MB) po ~2 s
(`PAGE_REPORTING_DELAY` jądra), host odrzuca jej zaplecze; przebieg startujący ≥ 2 s po
poprzednim „zimno” zapełnia strony (+~0,1 s na ~650 MB RSS, w całości w `first_batch_s`).
Dowody: przemiatanie pauzy — 0,5 s mieszane, 1,0 s wszystkie szybkie (~0,58–0,60 s), 2,0 i 3,0 s
wszystkie wolne (~0,68–0,76 s); dotknięcie 1,4 GB tuż przed przebiegiem — głównie szybkie;
3 s pętli CPU w pauzie — nadal wolno (to nie stan zasilania CPU); `[wsl2] pageReporting=false`
to nieznany klucz w WSL 3.0.1 — bez efektu. Pauza czyni każdy przebieg startem z „zimnej”
pamięci: czasy bezwzględne ~+0,1 s na 7-0 wobec przebiegów jeden po drugim; stan jest celowy
i deterministyczny.

**Badanie szumu** (20 przebiegów polars-bio A `overlap` 7-0, dwie grupy po 10, próg 10%,
kryterium ≤ 5%):

| Seria | Mediana | Rozrzut | Odsetek fałszywych dryfów |
|---|---|---|---|
| 20261010-224800 (bez pauzy) | 0,615 s | 38,2% (CV 11,2%) | 47,7% (próg 12%: 38,8%, 15%: 20,8%, 18%: 7,5%, 20%: 1,5%, 25%: 0,0%) |
| 20261010-225255 (suwak: najlepsza wydajność, bez pauzy) | 0,657 s | 36,1% | 65,0% (ścisła naprzemienność) |
| 20261010-235042 (`settle_s` 3,0) | 0,722 s (min 0,688, max 0,798) | 16,0% (CV 5,6%) | **1,8%** — spełnione (próg 8%: 11,4%, 5%: 23,4%) |

Bez pauzy rozkład dwumodalny (szybkie ~0,58 s, wolne ~0,72 s; różnica w `first_batch_s`:
0,27 vs 0,37 s). Z pauzą pozostaje łagodny wzorzec dwupoziomowy (0,69–0,72 vs 0,77–0,80 s).

**Smoke na release** (`smoke/20261010-235255`, ~37 min, `settle_s` 3 s): 281 przebiegów, 50
nieważnych = 2 bloki powtórzone przez dryf × 25 przebiegów (Ballista N = 3, próba 1: mediany
kontrolne 0,812 → 0,670 s, 17%; Sail N = 3, próba 1: 0,705 → 0,844 s, 20%; oba powtórzenia czyste,
2,3% i 2,5%; pozostałe bloki 0,2–3,9%), błędów 0, niezgodności sum 0, swap 0, `sail_workers` = N,
`broadcast_bytes` `nearest`/`coverage` 1-2 = 10 709 132 × N (N = 1, 2, 3), pozostałe operacje 0,
15 plików EXPLAIN. Diagnoza: przejściowe spowolnienia maszyny; użytkownik uznał serię za
zaliczoną.

**Porównanie ścieżek Saila** (`sail-paths/20261011-170650`; seria FAILED, wynik przyjęty przez
użytkownika): 358 przebiegów, 103 nieważne, błędów 0, identyczne sumy i liczby wierszy we
wszystkich wariantach. Mediany ważnych przebiegów (s), N = 1 / N = 3:

| Scenariusz | `sail` (pula) | `sail_lock` | `sail_udtf` |
|---|---|---|---|
| overlap 1-2 | 0,257 / 0,224 | 0,312 / 0,334 | 5,24 / 5,34 |
| nearest 1-2 | 0,418 / 0,372 | 0,452 / 0,433 | 5,89 / 6,30 |
| coverage 1-2 | 0,283 / 0,291 | 0,318 / 0,322 | 5,48 / 5,23 |
| merge 1 | 0,196 / 0,198 | 0,220 / 0,221 | 2,37 / 2,60 |
| subtract 1-2 | 0,256 / 0,261 | 0,281 / 0,289 | 5,48 / 5,64 |
| overlap 7-0 | 2,572 / 2,184 | 2,858 / 2,501 | 41,57 / brak ważnego |

`sail_lock`/`sail`: 1,08–1,49 (7-0: 1,11 / 1,15); `sail_udtf`/`sail`: 12–24×. Komórki UDTF N = 3
dla par 1-2 mają 1–2 ważne przebiegi. Szczyt RSS serwera Saila na 7-0: `sail` 1,93 / 2,30 GB,
`sail_lock` 1,80 / 2,13 GB, `sail_udtf` 3,12 GB (N = 1, bez swapu; 3 ważne przebiegi na
komórkę) i 3,37–3,79 GB (N = 3). Swap wystąpił tylko przy `sail_udtf` N = 3: `pswpout`
+2 783 … +12 055 stron w rozgrzewce i we wszystkich 3 rundach → nieważne, plus kaskada `pswpin` unieważniająca 8 innych przebiegów UDTF i 3 końcowe przebiegi kontrolne.
Nieważne: bloki powtórzone przez dryf (`sail` N = 3, próba 1: 10,7%, powtórzenie 0,0%;
`sail_udtf` N = 1, próba 1: 13,4%, powtórzenie czyste) = 24 + 24 przebiegi + 40 kontrolnych;
reszta to swap. Wpisu `expected_invalid` nie da się użyć dla kaskady `pswpin` (dotyka innych
scenariuszy i wariantu kontrolnego), więc serii nie powtarzano.

**Broadcast Ballisty** (`broadcast-check/20261011-185454`): OK — 35 przebiegów, 0 nieważnych,
0 błędów, swap 0, sumy zgodne z wzorcem polars-bio. Pojedyncze przebiegi, N = 1 / 2 / 3 (s):
`nearest` 2-7 1,13 / 1,04 / 1,17; `coverage` 2-7 0,67 / 0,63 / 0,66; `nearest` 7-0 2,31 / 2,24 /
2,48; `coverage` 7-0 1,18 / 1,29 / 1,38. `broadcast_bytes` dokładnie liniowo z N: 2-7
29 198 988 × N, 7-0 57 383 884 × N (dużo poniżej 512 MiB). `peak_rss_sum` 0,74–2,76 GB
(pojedyncze przebiegi, zaszumione). Pliki EXPLAIN dla n1–n3.

**Dryf kontroli (otwarte).** Dryf ze znakiem = (mediana końca − mediana początku) / mediana
początku, z ważnych przebiegów kontrolnych. Sail-paths, pierwsze próby: `sail` N = 1 −2,2%,
`sail` N = 3 +10,7%, `sail_lock` N = 1 +8,0%, `sail_lock` N = 3 +8,9%, `sail_udtf` N = 1
+13,4%, `sail_udtf` N = 3 +7,9%; powtórzenia: `sail` N = 3 +0,1%, `sail_udtf` N = 1 +2,9% — koniec
wolniejszy w 6 z 8 bloków (1 mniej więcej równy, 1 szybszy). Smoke (`20261010-235255`) takiego
wzorca nie pokazuje: 5 bloków z szybszym końcem, 5 z wolniejszym, 1 równy; największy dryf
(Ballista N = 3, 17,5%) miał końcowe kontrole szybsze (0,812 → 0,670 s). Wniosek: tendencja
widoczna w sail-paths (bloki z ciężkimi przebiegami Saila), niepotwierdzona ogólnie. Hipotezy bez
zmian: wyczerpany budżet taktowania CPU po ciężkim bloku albo pamięć trzymana przez silnik
w trakcie końcowych kontroli. Do ponownej oceny przed 3b-3 na wartościach ze znakiem dla
wszystkich zacommitowanych serii.

**Uwagi operacyjne.** Czyszczenie swapu: `sudo swapoff -a && sudo swapon /dev/sdc` (urządzenie
swapu WSL nie jest w fstab, więc `swapon -a` go nie włącza). `vm.swappiness` = 60 (domyślne)
daje drobny `pswpout` już przy ~2,9 GB szczytu procesu i ~4,2 GB `MemAvailable`; obniżenie to
opcja, niezastosowana. Testy `real_data` uruchamiać przed `wsl --shutdown` albo po wyczyszczeniu
swapu; logi serii przez `tee` lub przekierowanie; sprawdzenie VS Code:
`pgrep -fc '[v]scode-server'`.

## 10. Kryteria ukończenia

1. Pakiet testów zielony lokalnie (z `real_data`) i w CI. **Status:** lokalnie spełnione; część CI
   oczekuje — wymaga pusha, a ten wymaga zgody użytkownika.
2. Smoke na release zaliczony automatycznie (`smoke OK`): każdy przebieg ważny albo na liście
   `expected_invalid`; żaden blok nie powtórzony przez dryf. **Status:** zaakceptowane przez
   użytkownika (10.10.2026): seria zgłosiła FAILED z powodu 2 bloków powtórzonych przez dryf
   (powtórzenia czyste), bez błędów i niezgodności sum.
3. Badanie szumu: odsetek fałszywych dryfów przy progu 10% ≤ 5%; inaczej decyzja użytkownika
   o progu, na podstawie liczb. **Status:** spełnione (1,8% z `settle_s` = 3,0 s).
4. Komórki ⚠ macierzy (Ballista: `nearest`, `coverage` na 2-7 i 7-0) wykonują się albo mają
   opisany powód niewykonalności. **Status:** spełnione (`broadcast-check`, sekcja 9.1).
5. Sail liczy ścieżką Arrow z pulą kontekstów; porównanie trzech ścieżek zapisane w wynikach
   i w `wnioski_claude.md`. **Status:** zaakceptowane przez użytkownika (11.10.2026): sail-paths
   zgłosiło FAILED (pamięć UDTF N = 3 na 7-0 i kaskada `pswpin`, 2 powtórzenia przez dryf), ale
   wyniki porównania są kompletne w granicach opisanych w 9.1.
6. Fazy zapisywane: `first_batch_s` we wszystkich silnikach, `pb_s` w Sailu, pliki `EXPLAIN
   ANALYZE` dla Ballisty; `broadcast_bytes` wypełnione. **Status:** spełnione.
7. Dokumentacja zaktualizowana (etap 6). **Status:** spełnione.

## 11. Ryzyka

| Ryzyko | Skutek | Środek zaradczy |
|---|---|---|
| Build release trwa bardzo długo albo brakuje pamięci przy linkowaniu | etap 3 się wydłuża | build przy zamkniętym VS Code; w razie braku pamięci mniejsza równoległość generowania kodu w profilu release, opisana w OPIS.md |
| Sail nie ma ustawienia wielkości partii wyniku | odbiór wyniku dominuje czas Saila przy dużych wynikach | czas odbioru jako osobna faza; asymetria klientów opisana w pracy |
| Nowa wersja polars-bio zmienia `range_operation` | pomost przestaje działać | wersja przypięta `0.36.*`; test zgodności pomostu (5.7) |
| 512 MiB za mało albo broadcast 7-0 przy N = 3 nie mieści się w pamięci | puste komórki macierzy | szybki błąd zamiast wiszenia; kalibracja w 3b-3; plan zapasowy (ścieżka zamiast danych) tylko po decyzji użytkownika |
| Wynik trzymany w pamięci klienta | wyższy szczyt pamięci runnera | dla par lokalnych do ~250 MB (`overlap` 7-3, 4,4 mln wierszy); strażnik pamięci |
| `applyInArrow` przekazuje cały chromosom jako jedną tabelę | przy dużych grupach (chmura) limit pamięci | lokalnie bez znaczenia; uwaga w opisie |
| Błędy ścieżki Arrow w Sailu (niedojrzałe API rozszerzeń) | błędne wyniki albo zawieszenia | testy z wyrocznią dla pięciu operacji; suma kontrolna w każdym pomiarze; znaleziska do wniosków |
| Próg 10% za ciasny mimo dłuższego zadania | fałszywe powtórzenia bloków | badanie szumu i decyzja użytkownika na liczbach (kryterium 3) |

## 12. Poza zakresem

- kalibracja, konfiguracje `kalibracja/p1/p2/p3`, parametr `algorithm` (3b-3);
- analiza wyników (`bench/analyze.py`, parsowanie `EXPLAIN ANALYZE`);
- zmiany w polars-bio (w tym oficjalny mechanizm kontekstu na wywołanie) — tylko po wyraźnym,
  potrójnym potwierdzeniu użytkownika;
- klient Saila w Ruście; Sail na Kubernetesie; obraz Dockera; chmura;
- czas zadania po stronie schedulera Ballisty (sekcja 7, pozycja 22).

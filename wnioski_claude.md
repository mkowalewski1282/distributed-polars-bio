# Wnioski: implementacja operacji polars-bio w Ballistrze i Sailu

*Zaktualizowano: sierpień 2026, po empirycznej implementacji i weryfikacji wszystkich pięciu
operacji na obu silnikach (Fazy A–C planu pracy). Poprzednia wersja tego dokumentu zawierała
spekulacyjne mapowanie na ręcznie pisane zapytania SQL (window functions, cross joiny) —
**nigdy faktycznie nie zaimplementowane i zastąpione lepszym podejściem**: zamiast tłumaczyć
operacje na SQL od zera, oba silniki reużywają prawdziwy silnik algorytmiczny polars-bio
(`datafusion-bio-function-ranges` w Ballistrze, `pb.<operacja>()` wołane z wnętrza UDTF w Sailu).*

## Ogólna ocena wykonalności

polars-bio i Sail/Ballista mają wspólny fundament — wszystkie trzy opierają się na
**DataFusion** (Rust). Więcej: polars-bio nie implementuje algorytmów samodzielnie, tylko
deleguje do crate'a `datafusion-bio-function-ranges` (biodatageeks) — ten sam crate da się
podłączyć bezpośrednio do Ballisty. Migracja wszystkich pięciu operacji okazała się w pełni
wykonalna na obu silnikach.

## Status implementacji (zweryfikowane testami pytest)

| Operacja | Ballista (lokalnie) | Ballista (w pełni rozproszone) | Sail (UDTF) |
|---|---|---|---|
| `overlap` | ✅ | ✅ (jedyna z LogicalExtensionCodec, Faza A.4) | ✅ |
| `merge` | ✅ | ❌ (prywatny `MergeExec`, brak furtki) | ✅ |
| `nearest` | ✅ | ❌ (prywatny `NearestExec`) | ✅ |
| `coverage` | ✅ | ❌ (prywatny `CountOverlapsExec`) | ✅ |
| `subtract` | ✅ | ❌ (prywatny `SubtractExec`) | ✅ |

## Kluczowe odkrycie: dlaczego tylko `overlap` osiągnęło pełną dystrybucję w Ballistrze

`OverlapProvider::scan()` w bio-function-ranges deleguje przez `session.sql(query)` — buduje
zwykły SQL join dynamicznie. Dzięki temu dało się ominąć problem serializacji planu
fizycznego, po prostu dobierając "zwykłą" (nie bio-ową) sesję DataFusion po stronie
schedulera Ballisty (patrz `ballista_genomics/OPIS.md`, Faza A.4).

Wszystkie pozostałe operacje (`merge`, `nearest`, `coverage`/`count_overlaps`, `subtract`,
a także niezaimplementowane `cluster`/`complement`) budują WŁASNE, PRYWATNE struktury
`ExecutionPlan` bezpośrednio w `scan()` (np. `MergeExec`, `NearestExec`) — nie ma tu żadnej
furtki analogicznej do `overlap`. Pełna dystrybucja wymagałaby `PhysicalExtensionCodec` dla
tych typów, co przy prywatnych polach oznacza współpracę z autorami crate'a (biodatageeks)
albo reimplementację operatora od zera.

## Empirycznie znalezione różnice semantyczne między silnikami (nie błędy — realne cechy API)

1. **`nearest`, orientacja** (KOREKTA z 01.10.2026, plan 2): natywny `nearest()`
   z bio-function-ranges zwraca wiersz na każdy wiersz PRAWEJ tabeli (z najbliższym
   sąsiadem z lewej), a `pb.nearest(a, b)` — na każdy wiersz `a`; to ta sama odwrócona
   konwencja co w `coverage` (punkt 2). Wcześniej opisywana tu „różnica w rozstrzyganiu
   remisów” była artefaktem odwróconej orientacji: przy poprawnej oba silniki wybierają na
   danych testowych tych samych sąsiadów, także przy remisie
   (`test_ballista_and_pb_nearest_pick_same_neighbours`). Porównania nadal opierają się na
   odległościach, bo reguły remisów nie są udokumentowane — patrz `tests/nearest_oracle.py`.
2. **`coverage`, kolejność argumentów:** `pb.coverage(a, b)` i `coverage('reads','targets',...)`
   z bio-function-ranges mają odwróconą konwencję — `pb.coverage(a, b)` raportuje pokrycie
   interwałów `a` przez `b`, SQL-owa funkcja odwrotnie (pierwszy argument to "reads"
   dostarczające pokrycie). Trzeba zamienić kolejność argumentów, żeby wyniki się zgadzały —
   patrz `ballista_genomics/src/bin/coverage_local.rs`.
3. **`zero-length interval` (overlap):** polars-bio traktuje interwał `[x,x)` jako punkt `x`
   (może się nakładać z innymi interwałami), nie jako pusty zbiór — mimo że w czystej
   półotwartej arytmetyce `[x,x)` nie zawiera żadnej pozycji.

## Znaleziska dot. Saila (pysail 0.5.3), niezależne od operacji

- Argumenty TABLE dla UDTF nie są wspierane w ogóle (żadna forma: SQL `PARTITION BY`,
  DataFrame API, czy bez opcji) — działają tylko argumenty skalarne.
- `LATERAL` + skalarny UDTF nad tabelą rozłożoną na >1 partycję fizyczną gubi/dubluje wiersze
  — wymusza `.repartition(1)` przed `LATERAL` jako obejście (kosztem równoległości).
  **KOREKTA (Faza H):** przyczyną był globalny, mutowalny kontekst polars-bio, nie Sail —
  rozwiązanie bez utraty równoległości: blokada w `sail_pb_guard.py`.
- `SparkSession.getOrCreate()` cache'uje sesję jako globalny singleton procesu — uruchomienie
  dwóch niezależnych `SparkConnectServer` w JEDNYM procesie Pythona (np. dwa testy pytest w
  jednym pliku) kończy się błędem połączenia na drugim. Trzeba obsłużyć wiele UDTF-ów w
  jednej sesji zamiast tworzyć nową za każdym razem.
- Dekorator `@udtf` sprawdza tryb "remote" (Connect vs klasyczny PySpark) w MOMENCIE
  DEFINICJI klasy, nie rejestracji — klasa musi być budowana (przez funkcję fabrykującą)
  DOPIERO po utworzeniu sesji Spark Connect.

### Dojrzałość ścieżki UDTF w Sailu — obserwacja zbiorcza (plan 2, 01.10.2026)

Na danych rzeczywistych (`databio-8p`, para 1-2) ujawniło się, że ścieżka rozszerzeń Saila
(UDTF, PR #1519, pysail 0.5.3) wymaga omijania kolejnych raf, choć sam silnik liczy poprawnie:

1. **Brak argumentów TABLE dla UDTF** → całą grupę (wszystkie przedziały chromosomu) trzeba
   spakować `collect_list` do JEDNEGO wiersza i przekazać jako argument skalarny.
2. **`LATERAL` powiela wiersz zewnętrzny dla każdego wiersza wyniku UDTF-a**, razem z listą
   wszystkich przedziałów chromosomu (silnik nie odcina kolumn, których dalej nie używamy).
   Gdy UDTF zwracał wiersz na wynik, pamięć rosła jak (wiersze wyniku) × (rozmiar grupy):
   +3,8 GB przy 5000 przedziałach w jednej grupie; na parze 1-2 proces był zabijany przez
   OOM, a raz wywrócił WSL. To własność Saila, nie polars-bio (czysto pythonowy UDTF
   zachowuje się tak samo). Obejście w `sail_bio.py`: UDTF zwraca jeden wiersz z tablicą
   wyników, rozwijaną `explode` poza `LATERAL` (100 tys. wierszy wyniku przy 314 MB).
3. **Dane przechodzą przez Pythona** (Row → pandas → polars-bio → krotki) — narzut
   konwersji, który pomiary planu 3 muszą zminimalizować albo jawnie opisać.
4. **Równoległość ≈ liczba chromosomów** (jedno wywołanie UDTF na chromosom), a cały
   chromosom jest naraz w pamięci jednego wywołania.
5. **Wieloprocesowość tylko na Kubernetesie** (`local-cluster` to jeden proces) — stąd jawna
   asymetria w macierzy pomiarów i osobna próba K8s.

**Ocena:** to nie fundamentalna wada silnika (zapytania liczą się poprawnie), lecz
niedojrzałość jego mechanizmu rozszerzeń. Ballista pozwala na głęboką integrację (własne
kodery planu i operatory fizyczne — operacje polars-bio wykonują się natywnie, w Rust),
Sail — tylko przez UDTF z danymi przechodzącymi przez Pythona. Materiał do rozdziału
porównawczego pracy: kryteria jakościowe „nakład i ryzyko integracji” oraz „dojrzałość API
rozszerzeń”; czy ograniczenia te przekładają się na wydajność — rozstrzygną pomiary.

Pełne opisy każdego znaleziska, z dokładnymi komunikatami błędów i historią prób: komentarze
w kodzie (`sail_overlap_udtf.py`, `sail_coverage_subtract_udtf.py`,
`ballista_genomics/OPIS.md`) oraz plik planu pracy.

## Rekomendowany kolejny krok

Testy na rzeczywistych plikach BED w celu oceny wydajności na większych danych (Faza D planu
pracy) — dotąd wszystkie testy używały syntetycznych danych (5 interwałów × 2 zbiory).

## Plan 3a — obserwacje z budowy narzędzia pomiarowego (01–02.10.2026)

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
   - Na danych 1-2 (smoke) błąd widać tylko w `subtract`: przy 2, 4 i 6 partycjach 205 673,
     202 854 i 201 506 wierszy zamiast 209 940. `merge` zbioru 1 jest poprawny, bo fBrain nie
     ma nakładających się przedziałów — nie ma czego scalać. To, czy błąd się ujawni, zależy
     więc od danych; wykrywa go dopiero suma kontrolna.
   - Skutki dla pomiarów: wzorcem jest polars-bio na 1 partycji, a przebiegi A/B
     `merge`/`subtract` z błędnym wynikiem wychodzą nieważne. Punkt odniesienia w P1 (decyzja
     z 01.10.2026): polars-bio na 1 partycji, mierzony dodatkowo.
   - Dowód: `tests/test_polars_bio_runner.py` (xfail strict).
2. **Sail w trybie `local-cluster` tworzy driver i workery dla każdej sesji i sam skaluje ich
   liczbę.**
   - Driver i pula workerów powstają przy pierwszym RPC sesji Spark Connect i znikają razem
     z sesją. Każdy przebieg (osobny proces runnera = osobna sesja) ma więc świeże workery;
     ich start wypada przed pomiarem czasu, przy rejestracji UDTF.
   - Gdy etap ma więcej zadań niż wolnych slotów, Sail uruchamia workery ponad
     `worker_initial_count`. Z limitem `worker_max_count` zapytanie wisi, zamiast czekać na
     wolne sloty.
   - Narzędzie używa 8 slotów na workera. Przy N = 2 i 3 workerów było w smoke zawsze N. Przy
     N = 1 w pierwszym smoke 2 z 5 sesji dostały drugiego workera (16 zadań skanowania dwóch
     zbiorów po 8 plików > 8 slotów), w drugim żadna z 10. O zasobach i tak decyduje
     przypięcie procesu do 2N rdzeni. Do planu 3b: zapisywać liczbę workerów na przebieg.
   - Kilka sesji otwieranych po kolei w jednym procesie klienta: zapytanie czwartej sesji
     wisi (2 z 2 prób, zbiór testowy). Osobne procesy, jak w narzędziu, działają bez problemu.
   - To uzupełnia obserwację o dojrzałości ścieżki UDTF w Sailu.
3. **Ballista 53 ma wyścig przy starcie executora.** Executor rejestruje się w schedulerze,
   zanim jego serwer gRPC przyjmuje połączenia (komentarz w kodzie Ballisty: „TODO the
   executor registration should happen only after the executor grpc server started”).
   Scheduler w trybie push od razu łączy się zwrotnie z executorem, więc rejestracja bywa
   odrzucona („Connection refused”), a executor kończy się kodem 1. W smoke zdarzyło się to
   raz na 11 startów executorów i przerwało blok N = 3. Narzędzie ponawia teraz start klastra
   (do 3 razy, na nowych portach).
4. **Przebieg kontrolny `overlap` 1-2 jest za krótki na regułę 10%.** polars-bio A liczy go
   w ~0,12–0,19 s, więc rozjazd między początkiem a końcem bloku wynosił w smoke 12–54%
   i większość bloków była powtarzana. Długość przebiegu kontrolnego (np. większa para albo
   mediana kilku powtórzeń) — do ustalenia w planie 3b.
5. **Import polars-bio trwał ~270 s przez serwer X, nie przez polars-bio.** Matplotlib
   (importowany przez polars-bio) sprawdzał ekran z `DISPLAY`. Zmienną ustawia
   `/etc/bash.bashrc` na host Windows, na którym nie działa serwer X, więc import czekał na
   timeout TCP. Z `MPLBACKEND=Agg` import trwa ~1 s. Wcześniejsze uwagi o „kilku minutach”
   importu dotyczą tego zjawiska.

## Plan 3b-1 — aktualizacja środowiska i wersji (od 02.10.2026)

Środowisko odtwarzane przez uv (`pyproject.toml`, `uv.lock`, Python 3.12); wersje podnoszone
etapami, po jednej zmianie. Punkty dopisywane po każdym etapie.

1. **polars-bio 0.36.0: błąd #372 zniknął.**
   - `merge` i `subtract` przy `target_partitions > 1` dają ten sam wynik co na 1 partycji: testy
     xfail strict z planu 3a przeszły, a w teście integracyjnym wszystkie przebiegi są ważne.
   - Wariant „polars-bio na 1 partycji” w P1 jest zbędny; wzorzec nadal na 1 partycji.
   - API używane przez projekt bez zmian: sygnatury operacji, opcje `datafusion.bio.*`,
     `POLARS_BIO_MAX_THREADS`, `execute_stream`.
2. **Ballista i polars-bio liczą tą samą wersją algorytmów (v0.22.2).**
   - Vendor podniesiony z 0.18.0; łatki widoczności bez zmian.
   - Między 0.18.0 a 0.22.2 upstream poprawił m.in. przedziały jednozasadowe przy współrzędnych
     0-based (`nearest`, `count_overlaps`, `coverage`). Przy rozjechanych wersjach takie
     przedziały dawałyby różne wyniki silników bez winy silnika.
   - Wyrównanie wersji jest więc warunkiem porównania silników; pilnuje go
     `tests/test_algorithm_versions.py`.
   - Aktualizacja odsłoniła błąd integracji: `nearest` w Ballistcie był wywoływany bez `'strict'`,
     czyli w konwencji 1-based, choć dane są 0-based. W 0.18.0 odległość nie zależała od
     konwencji, więc błąd był niewidoczny. W 0.22.2 odległość wychodziła o 1 mniejsza niż
     w polars-bio (9 zamiast 10 na zbiorze testowym). Poprawione; opis w `ballista_genomics/OPIS.md`.
     Wniosek dla pracy: zgodność wyników „przypadkiem” przy jednej wersji biblioteki nie dowodzi
     poprawnej konfiguracji — test porównawczy wykrył błąd dopiero, gdy biblioteka zaczęła
     rozróżniać konwencje.
3. **Sail 0.7.2 (PySpark 4.2.0): ustalenia z 0.5.3 sprawdzone od nowa (sondy 03.10.2026).**
   - Zmienne `SAIL_*` używane przez narzędzie bez zmian (te same klucze i wartości domyślne
     w `application.yaml` 0.5.3 i 0.7.2). API `pysail.spark.SparkConnectServer` bez zmian.
   - Pula workerów na sesję: obowiązuje — każda sesja (osobny proces runnera) dostaje własne
     workery numerowane od 1.
   - Dodatkowy worker przy N = 1 (zbiór testowy w 8 plikach na stronę, 16 zadań skanowania
     > 8 slotów): 0 z 10 sesji; przy N = 2 zawsze 2 workery. Na danych rzeczywistych (smoke
     04.10.2026) również dokładnie N workerów w każdej sesji (N = 1: 10 z 10, N = 2: 5 z 5,
     N = 3: 10 z 10); w planie 3a przy N = 1 zdarzał się dodatkowy worker.
   - Sesje po kolei w jednym procesie klienta: wszystkie 5 zakończone (0,1–0,3 s) —
     zawieszanie czwartej sesji z 0.5.3 zniknęło.
   - Serwer drivera jest w 0.7.2 wspólny dla sesji („driver server is ready” raz, przy starcie
     serwera); pula workerów należy do drivera sesji.
   - **Nowa regresja:** w `… g, LATERAL f(g.chrom, …) o` wynik UDTF z kolumną o tej samej nazwie
     co kolumna relacji zewnętrznej (`chrom`) nie przechodzi konwersji do Arrow („Could not
     convert 'chr1' with type str: tried to convert to int64”). Inne nazwy kolumn wyniku albo
     wywołanie bez LATERAL działają; w 0.5.3 działało. Ścieżka pomiarowa (`sail_bio.py`, jedna
     kolumna wyniku `res`) nie jest dotknięta. Skrypty Faz C/H nazywają teraz kolumnę grupującą
     `group_chrom`; regresję przypina test xfail(strict)
     `test_lateral_udtf_output_named_like_outer_column`. Kolejny przykład niedojrzałości ścieżki
     UDTF w Sailu (porównaj obserwację zbiorczą z planu 2).
4. **Pozostałe biblioteki do najnowszych wersji (numpy 2.5.3, pandas 3.0.6, pytest 9.1.1,
   polars 1.44.2).**
   - Pakiet testów bez zmian w kodzie. Suma kontrolna jest odporna na reguły promocji numpy 2,
     bo używa jawnych skalarów `uint64`.
   - PySpark 4.2.0 ostrzega, że pandas ≥ 3 „nie jest jeszcze w pełni wspierany”
     (`FutureWarning`), i używa w `pyspark/sql/pandas/types.py` przestarzałego w pandas 3
     parametru `copy` (`Pandas4Warning`). Testy samego pysail 0.7.2 przypinają `pandas<3`.
   - Ścieżka UDTF projektu (wiersze, nie pandas UDF) działa, a testy porównują jej wynik
     z polars-bio.
   - Do sprawdzenia w planie 3b-2 przy przejściu na UDTF ze strzałką (Arrow), gdzie PySpark
     konwertuje partie przez pandas — wynik: sekcja „Plan 3b-2”, punkt 7.
5. **Weryfikacja końcowa (04.10.2026).**
   - Testy na danych rzeczywistych: 13 passed, 2 xfailed (standalone przy broadcaście — bez
     zmian).
   - Smoke na parze 1-2 (Ballista debug): 103 przebiegi, żaden z błędnym wynikiem; 56 nieważnych
     wyłącznie z powodu dryfu przebiegu kontrolnego (11–23%; sprawa planu 3b-2). `subtract`
     polars-bio A/B poprawny przy 2, 4 i 6 partycjach. Swap: zero.
   - CI na GitHub Actions zielone (pierwsze przebiegi odsłoniły brak `protoc` na runnerze —
     potrzebuje go skrypt budowania zależności `substrait`; CI instaluje go teraz sam).
   - Lokalne 22 GB w `ballista_genomics/target` to głównie nagromadzone stare artefakty
     (`target/debug/deps`), nie debuginfo — `Cargo.toml` od dawna ma `debug = false`.

## Plan 3b-2 — narzędzie gotowe do pomiarów (od 09.10.2026)

1. **Partie wyniku Saila (sonda 10.10.2026, plan 3b-2, Zadanie 12).**
   - Ścieżka `applyInArrow` z kontekstem polars-bio na wątek (wersja później zastąpiona pulą, punkt 2), `overlap` 7-0 (2 761 621 wierszy),
     N = 3, klient PySpark (`to_table_as_iterator`); najmniejsze czasy z 3 powtórzeń, VS Code
     otwarty — liczby orientacyjne, nie pomiar do pracy.

     | Ustawienie | Partie wyniku | Odbiór (`t_total_s − first_batch_s`) | `t_total_s` |
     |---|---|---|---|
     | domyślne | 1877 | 2,79 s | 3,75 s |
     | `SAIL_EXECUTION__BATCH_SIZE=65536` (serwer) | 273 | 1,59 s | 2,70 s |
     | `spark.sql.execution.arrow.maxRecordsPerBatch=65536` (sesja) | 1877 | 2,64 s | 3,38 s |
     | `spark.sql.execution.arrow.maxRecordsPerOutputBatch=65536` (sesja) | 1877 | 2,64 s | 3,46 s |
     | `spark.connect.grpc.arrow.maxBatchSize=64m` (sesja) | 1877 | 2,63 s | 3,40 s |

   - We wszystkich wariantach ta sama liczba wierszy i suma kontrolna (`0x19f5668a8b28be3a`).
   - Decyzja (reguła planu: ≥ 20% krótszy odbiór przy tej samej sumie): `SAIL_EXECUTION__BATCH_SIZE
     = 65536` w `sail_env` (`bench/runners/sail_server.py`) — odbiór krótszy o 43%, cały przebieg
     o 28%. Opcje sesji Spark Connect nie zmieniają liczby partii — na liczbę partii wyniku
     wpływa rozmiar partii wykonania Saila (uściślenie niżej).
   - Do opisu w metodyce: to strojenie silnika (rozmiar partii wykonania dotyczy całego planu,
     nie tylko odbioru), w granicach publicznej konfiguracji Saila; Ballista zostaje przy
     domyślnym rozmiarze partii DataFusion. Odbiór wyniku przez klienta w Pythonie pozostaje
     osobną fazą (asymetria klientów: Ballista — Rust, Sail — Python).
   - Uściślenie: 273 partie to nie 2,76 mln / 65 536 — rozmiar partii wykonania wpływa na liczbę
     partii wyniku, ale ta nie wynika z niego wprost. `SAIL_EXECUTION__BATCH_SIZE=65536` działa na
     wszystkie trzy warianty Saila, lecz zmierzono go tylko na ścieżce Arrow. To strojenie silnika
     wobec domyślnych 8192 w Ballistcie — asymetria do opisania w pracy.
2. **Globalny kontekst polars-bio, pomost i pula kontekstów.**
   - polars-bio 0.36 ma jeden globalny kontekst (`s1`/`s2`); równoczesne wywołania w wątkach jednego
     procesu kończą się błędem `The table s1 already exists`. Obserwacja dla pracy: polars-bio nie
     jest bezpieczny wątkowo w jednym procesie, co ogranicza integrację z silnikami wykonującymi
     funkcje Pythona w wątkach (Sail). Pomost we własnym repo (podmiana `range_operation`, bez
     zmian w polars-bio) usuwa blokadę.
   - Zysk z braku blokady (`sail_lock` / `sail`, mediany ważnych przebiegów, sail-paths 11.10.2026):
     1,08–1,49 na parach 1-2; na `overlap` 7-0 1,11 (N = 1) i 1,15 (N = 3): 2,572 → 2,858 s
     i 2,184 → 2,501 s. Sonda z 08.10 (kontekst na wątek) dawała ~10% na 7-0.
   - Lekcja o puli: `threading.local` jest błędnym wyborem, gdy Python jest wołany z obcych
     (Rustowych) wątków — Sail odtwarza stan wątku Pythona przy każdym wywołaniu, więc „kontekst
     na wątek” stawał się kontekstem na wywołanie (sonda 11.10.2026: 162 konteksty na 3 zapytania,
     54 na zapytanie `overlap` 7-0 przy 2 wątkach natywnych). Pamięć serwera Saila po 12 zapytaniach:
     0,94 → 2,65 GB (~150 MB na zapytanie) wobec 0,95 → 1,83 GB z pulą (`sail_lock`: 0,90 → 1,80
     GB); szczyt puli 2,21 GB po 24 zapytaniach (blisko plateau); te same wiersze i sumy. Poza
     Sailem (zwykłe wątki) żadna wersja nie rośnie. Odrzucone hipotezy: limit czasu sesji, areny
     glibc (`MALLOC_ARENA_MAX=2`), ustawienia alokatora Arrow/jemalloc.
3. **Ścieżka Arrow a UDTF w Sailu (koszt mechanizmu integracji).**
   - Sonda 08.10 (para 1-2, N = 1): UDTF 5,61 / 6,33 / 5,75 / 2,36 / 5,71 s vs Arrow 0,33 / 0,46 /
     0,31 / 0,23 / 0,28 s (overlap / nearest / coverage / merge / subtract).
   - Seria sail-paths (mediany, N = 1 / 3): UDTF/pula 12–24×; np. `overlap` 1-2: 5,24 / 5,34 s vs
     0,257 / 0,224 s; `overlap` 7-0: 41,57 s vs 2,572 s (N = 1). UDTF na 7-0 trzyma serwer w
     3,12 GB (N = 1) i 3,37–3,79 GB (N = 3) wobec 1,8–2,3 GB ścieżek Arrow → swap i nieważne
     przebiegi.
   - Wszystkie trzy warianty dają identyczne wiersze i sumy kontrolne, więc różnica to koszt
     mechanizmu, nie algorytmu: wzorzec `collect_list` + LATERAL + `explode` kontra wymiana tabel
     Arrow. Wniosek o dojrzałości API rozszerzeń Saila: `applyInArrow` (API PySparka) działa od
     razu i jest szybkie; ścieżka UDTF jest wolna, pamięciożerna i miała regresję w 0.7.2
     (LATERAL, punkt 3 sekcji „Plan 3b-1”).
4. **Badanie szumu przebiegu kontrolnego i przerwa `settle_s`.**
   - Para 1-2 (~0,13 s) była za krótka, więc kontrolą jest `overlap` 7-0 (~0,6–0,7 s w polars-bio A),
     10 powtórzeń, mediany. Mimo to bez pauzy odsetek fałszywych dryfów przy progu 10% wyniósł 47,7%
     (mediana 0,615 s, rozrzut 38,2%) i 65,0% (suwak trybu zasilania na najlepszej wydajności:
     mediana 0,657 s, rozrzut 36,1%) — rozkład dwumodalny ~0,58 / ~0,72 s, różnica w całości w
     `first_batch_s` (0,27 vs 0,37 s).
   - Przyczyna: WSL2 po ~2 s zwraca zwolnioną pamięć do Hyper-V (*free page reporting*), więc
     przebieg startujący później „zimno” zapełnia strony (+~0,1 s na ~650 MB RSS). Dowody:
     przemiatanie pauzy (0,5 s mieszane; 1,0 s szybkie; 2,0 i 3,0 s wolne), dotknięcie 1,4 GB przed
     przebiegiem → głównie szybkie, pętla CPU w pauzie → nadal wolno; `pageReporting=false` w
     `.wslconfig` bez efektu.
   - Rozwiązanie: `settle_s` = 3,0 s. Seria 20261010-235042: mediana 0,722 s (0,688–0,798), rozrzut
     16,0%, CV 5,6%, odsetek fałszywych dryfów **1,8%** (kryterium ≤ 5%: spełnione); przy progu 8%
     byłoby 11,4%, 5% — 23,4%. Pauza wprowadza stan deterministycznie zimny (czasy ~+0,1 s na 7-0).
5. **Smoke na release (10.10.2026, `settle_s` 3 s, ~37 min).**
   - 281 przebiegów, 0 błędów, 0 niezgodności sum, swap 0; 50 nieważnych = 2 bloki powtórzone
     przez dryf (Ballista N = 3: 17%, Sail N = 3: 20%; powtórzenia 2,4% i 2,5%) — przejściowe
     spowolnienia maszyny, seria uznana za zaliczoną.
   - Ballista release vs debug (smoke 04.10.2026), mediana `t_total_s` przebiegów mierzonych
     (po wszystkich przebiegach, bo debug nie ma ważnego przebiegu przy N = 3):

     | Scenariusz | N | release [s] | debug [s] | debug / release |
     |---|---|---|---|---|
     | overlap 1-2 | 1 / 2 / 3 | 0,136 / 0,138 / 0,164 | 1,39 / 1,37 / 1,44 | 10,2 / 9,9 / 8,7 |
     | nearest 1-2 | 1 / 2 / 3 | 0,366 / 0,342 / 0,365 | 2,50 / 2,32 / 2,33 | 6,8 / 6,8 / 6,4 |
     | coverage 1-2 | 1 / 2 / 3 | 0,210 / 0,294 / 0,265 | 1,43 / 1,45 / 1,46 | 6,8 / 4,9 / 5,5 |
     | merge 1 | 1 / 2 / 3 | 0,068 / 0,081 / 0,079 | 0,67 / 0,51 / 0,56 | 9,9 / 6,3 / 7,1 |
     | subtract 1-2 | 1 / 2 / 3 | 0,159 / 0,091 / 0,110 | 1,10 / 0,79 / 0,81 | 6,9 / 8,6 / 7,3 |

     Debug jest 4,9–10,2× wolniejszy (mediana ilorazów ≈ 6,9×). Serie nie są w pełni
     porównywalne: debug liczył sumę kontrolną w `t_total_s` (`checksum_s` ≈ 0,16 s wobec 0,005 s
     w release, więc część różnicy to zmiana protokołu), nie miał pauzy `settle_s`, a przebiegi
     były pojedyncze (1–2 przebiegi na komórkę, bez rozgrzewki). Rząd wielkości jest jednak
     pewny: build debug nie nadaje się do pomiarów.
6. **Broadcast Ballisty (`broadcast-check` 11.10.2026, strażnik gRPC).**
   - `nearest` i `coverage` na 2-7 i 7-0 wykonują się przy N = 1, 2, 3 (35 przebiegów, 0 nieważnych,
     sumy zgodne z polars-bio). Pojedyncze przebiegi (N = 1 / 2 / 3): `nearest` 2-7 1,13 / 1,04 /
     1,17 s; `coverage` 2-7 0,67 / 0,63 / 0,66 s; `nearest` 7-0 2,31 / 2,24 / 2,48 s; `coverage` 7-0
     1,18 / 1,29 / 1,38 s.
   - `broadcast_bytes` rośnie dokładnie liniowo z N: 2-7 = 29 198 988 × N, 7-0 = 57 383 884 × N
     (smoke 1-2: 10 709 132 × N) — to ładunek × liczba wiadomości startowych (jedna na executor
     i etap w trybie push), a **nie** × liczba zadań (poprawka względem założeń planu: Ballista 53
     wysyła plan etapu raz na executor). Dużo poniżej 512 MiB; `peak_rss_sum` 0,74–2,76 GB
     (pojedyncze przebiegi, zaszumione).
   - Strażnik broadcastu koduje tabelę do IPC jeszcze raz w schedulerze, w mierzonym czasie
     (chwilowo ≈ 2× ładunek, 29–57 MB); koszt niemierzony osobno.
7. **Ostrzeżenia PySparka 4.2 przy pandas 3 na ścieżce Arrow (Zadanie 10; uzupełnienie punktu 4
   sekcji „Plan 3b-1”).** Po stronie klienta `FutureWarning` „PySpark does not yet fully support
   pandas >= 3.0.0” (`pyspark/sql/connect/utils.py:41`, `pyspark/testing/utils.py:127`) na obu
   ścieżkach; ścieżka UDTF dodatkowo `pyspark/sql/connect/udtf.py:88`. Bez `Pandas4Warning`,
   żadnych ostrzeżeń po stronie serwera.
8. **Otwarte: końcowe przebiegi kontrolne są wolniejsze od początkowych.** W sail-paths dryfy
   2,2%, 10,7%, 8,0%, 8,9% i 13,4%; różnica w `first_batch_s` (start ~0,28–0,31 s, koniec
   ~0,33–0,36 s), końcowe często przyspieszają w ciągu 10 przebiegów. Hipotezy niesprawdzone:
   wyczerpany budżet taktowania CPU po ciężkim bloku albo pamięć trzymana przez silnik podczas
   końcowych kontroli. Do zbadania przed 3b-3.

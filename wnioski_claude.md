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

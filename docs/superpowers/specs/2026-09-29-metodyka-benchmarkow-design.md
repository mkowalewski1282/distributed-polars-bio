# Metodyka pomiarów wydajności rozproszonych operacji genomicznych — projekt

- **Data:** 29 września 2026
- **Status:** projekt zaakceptowany; kolejny krok — plan implementacji
- **Zakres:** metodyka benchmarków (pytania badawcze, środowisko, dane, metryki, protokół,
  macierz eksperymentów) oraz narzędzie pomiarowe, które ją realizuje
- **Powiązane:** `raporty/sprawozdanie_stanu_prac.md` (stan wyjściowy),
  `raporty/podsumowanie_brainstorming.md` (uzasadnienia decyzji)

---

## 1. Kontekst i cel

Etap weryfikacji poprawności jest zamknięty: pięć operacji polars-bio (`overlap`, `merge`,
`nearest`, `coverage`, `subtract`) działa na Apache Ballista (wszystkie rozproszone, przez
własne kodery planu) i na LakeSail/Sail (natywny UDTF). Nie wykonano dotąd żadnego pomiaru
wydajności; wszystkie testy operują na zbiorze pięciu interwałów.

Celem kolejnego etapu jest **naukowo uzasadniony rozdział eksperymentalny**: pomiary, które
odpowiadają na jawnie postawione pytania badawcze, zamiast tabeli czasów bez tezy.

**Twarde ograniczenie środowiskowe:** dostępna jest wyłącznie maszyna lokalna. Metodyka
i narzędzie muszą dać się w całości zwalidować i wykonać lokalnie; pomiary lokalne są
**pełnoprawnymi wynikami pracy**. Chmura (np. GCP) jest opcjonalnym rozszerzeniem skali tymi
samymi narzędziami — dostęp i finansowanie do ustalenia na dalszym etapie.

## 2. Pytania badawcze i hipotezy

| Id | Pytanie | Hipoteza |
|---|---|---|
| **P0** | Czy wykonanie w Ballistcie jest rzeczywiście rozproszone (osobne procesy, praca na więcej niż jednym executorze)? | — (warunek wstępny; osobne zadanie, bez pomiarów czasu, sekcja 10) |
| **P1** | Czy i o ile wykonanie rozproszone jest szybsze od polars-bio na jednej maszynie — dla której operacji i od jakiej klasy rozmiaru danych? | Dla klasy S rozproszenie przegrywa (dominuje narzut planowania, serializacji i shuffle); przewaga pojawia się od pewnego rozmiaru, o ile lokalnie w ogóle. |
| **P2** | Jak zmienia się czas przy 1 → 2 → 3 węzłach i stałych danych (skalowanie silne)? | Operacje z shuffle (`merge`, `subtract`) skalują się lepiej niż operacje z broadcastem (`nearest`, `coverage`), bo broadcast powiela budowę indeksu na każdym węźle. |
| **P3** (poboczne) | Czy ranking algorytmów indeksowania w `overlap` zmienia się po rozproszeniu? | Ranking przesuwa się na korzyść algorytmów z tańszą budową indeksu — w wersji rozproszonej indeks lewej tabeli budowany jest co najmniej raz na executor (`IntervalJoinExec`, tryb `CollectLeft`, `OnceAsync` na instancję planu), a nie raz na zapytanie. |
| — | Ballista vs Sail | Porównanie przekrojowe wynikające z P1–P2, uzupełnione o **kryteria jakościowe**: nakład integracji (kodery vs UDTF), wymagania operacyjne (Sail wymaga Kubernetesa do wieloprocesowości), ograniczenia (limit wiadomości gRPC przy broadcaście, blokada globalnego kontekstu polars-bio, sufit równoległości ≈ liczba chromosomów). |

Skalowanie słabe (dane rosnące razem z klastrem) jest **poza zakresem**: wymagałoby generatora
danych, a przyjęto wyłącznie zbiory rzeczywiste. Oś rozmiaru danych pokrywa P1 — z zastrzeżeniem,
że zbiory AIList różnią się także strukturą (stopień zagnieżdżenia interwałów, *non-flatness*).

Punkt odniesienia metodologiczny: metryka COST (McSherry, Isard, Murray, *Scalability! But at
what COST?*, HotOS 2015) — system rozproszony porównuje się z kompetentną implementacją na
jednej maszynie, a nie wyłącznie z samym sobą przy mniejszej liczbie węzłów.

## 3. Środowisko i emulacja węzłów

**Maszyna:** AMD Ryzen 5 4600H (6 rdzeni, 12 wątków), WSL2. Hosta 7,4 GB RAM; WSL domyślnie
3,5 GB. Na czas pomiarów WSL podniesiony do 5 GB i 4 GB swapu (`.wslconfig`), przy czym każdy
przebieg z użyciem swapu jest nieważny (sekcja 6).

**Definicja węzła:** 1 rdzeń fizyczny = 2 wątki (para SMT). Topologia (`lscpu -e`): rdzeń *k*
to wątki {2k, 2k+1}. Przydział:

| Rdzeń | Wątki (CPU) | Rola |
|---|---|---|
| 0 | 0–1 | system, scheduler Ballisty, klienci (Ballista, PySpark), orkiestrator |
| 1 | 2–3 | węzeł 1 |
| 2 | 4–5 | węzeł 2 |
| 3 | 6–7 | węzeł 3 |
| 4–5 | 8–11 | nieużywane (redukcja zakłóceń) |

Konfiguracje: **N ∈ {1, 2, 3}** węzły. Przypięcie przez `taskset`; liczby wątków ustawiane
jawnie w silnikach (`target_partitions`, liczba slotów, `POLARS_MAX_THREADS` / ustawienia
wątków polars-bio), bez polegania na autodetekcji.

**Warianty w konfiguracji N:**

| Wariant | Procesy i przypięcie |
|---|---|
| **polars-bio A** (węzeł tej samej wielkości) | jeden proces na wątkach węzła 1 (2 wątki); niezależny od N |
| **polars-bio B** (maszyna = cały klaster) | jeden proces na wątkach węzłów 1..N (2N wątków); dla N = 1 tożsamy z A |
| **Ballista** | scheduler (CPU 0–1) + N executorów, executor *i* na wątkach węzła *i*, 2 sloty zadań na executor |
| **Sail** | jeden proces serwera (`local-cluster`) na wątkach węzłów 1..N — te same zasoby co klaster, ale bez rozproszenia i z blokadą `sail_pb_guard` (asymetria opisana jawnie) |

Który z wariantów A/B jest punktem odniesienia głównym — do ustalenia na dalszym etapie; mierzone
są oba.

**Pamięć:** bez limitów per węzeł; szczyt pamięci jest **mierzony** (sekcja 5). Limity przez
cgroup v2 (dostępny, wymaga `sudo`, brak systemd) dodawane tylko w razie potrzeby.

**Ograniczenia emulacji (do opisania w pracy):** przypięcie dotyczy wirtualnych CPU maszyny
WSL, które Hyper-V może przenosić między rdzeniami fizycznymi; węzły współdzielą L3,
przepustowość pamięci i dysk; brak fizycznej sieci (ruch przez interfejs pętli zwrotnej).

## 4. Dane

- **Źródło:** zestaw `databio-8p` z repozytorium polars-bio-bench — 9 zbiorów AIList
  w formacie Parquet, każdy podzielony na 8 plików (równoległe czytanie: 8 partycji źródłowych
  > 6 slotów przy N = 3). Pobieranie tym samym mechanizmem co w polars-bio-bench; dane poza
  repozytorium.
- **Identyfikatory zbiorów:** 0 chainRn4 (2 351 tys.), 1 fBrain (199 tys.), 2 exons (439 tys.),
  3 chainOrnAna1 (1 957 tys.), 4 chainVicPac2 (7 684 tys.), 5 chainXenTro3Link (50 981 tys.),
  6 chainMonDom5Link (128 187 tys.), 7 ex-anno (1 194 tys.), 8 ex-rna (9 945 tys.).
- **Dokładne liczby wierszy** (po pobraniu, `python -m bench.data.download`): 0 — 2 350 965,
  1 — 198 621, 2 — 438 694, 3 — 1 956 864, 4 — 7 684 066, 5 — 50 980 975, 6 — 128 186 542,
  7 — 1 194 285, 8 — 9 944 559. Schemat każdego pliku: `contig` (string), `pos_start`,
  `pos_end` (int32); wiersze nieposortowane, chromosomy rozrzucone po wszystkich plikach
  (np. exons, chainRn4: 24 kontigi w każdym z 8 plików; ex-anno: 54 kontigi, 38–44 w pliku). Archiwum
  zawiera też `__MACOSX/` (pliki `._*.parquet`, które nie są Parquetem), sumy `.crc`
  i znaczniki `_SUCCESS` — pomijane przy rozpakowaniu.
- **Pary:** identyfikatory jak w polars-bio-bench (`a-b` = df1 zbioru *a*, df2 zbioru *b*).
  Klasy rozmiaru według liczby wierszy wyniku `overlap`: S < 10⁶, M 10⁶–10⁸, L 10⁸–10⁹, XL > 10⁹.
- **Układ współrzędnych:** 0-based, półotwarty (jak BED), we wszystkich wariantach.
- **Format:** Parquet w obu ścieżkach (dziś CSV — zmiana w sekcji 9).
- **Reguła kalibracji (wybór zbiorów lokalnych):** przed pomiarami polars-bio A wykonuje każdą
  kandydacką parę; para wchodzi do macierzy lokalnej, jeśli szczyt pamięci ≤ ⅓ `MemAvailable`
  zmierzonego na starcie kalibracji. Uzasadnienie: przy broadcaście każdy z maks. 3 węzłów
  trzyma własną kopię tabeli i indeksu. Pary odrzucone → macierz chmurowa.
- **Architektura lakehouse** (tabele Delta Lake / Apache Iceberg, m.in. partycjonowanie po
  chromosomie w warstwie składowania) — kierunek docelowy, **poza tym etapem**; narzędzie ma
  wymienne źródło danych, więc wariant lakehouse dochodzi bez przebudowy.

## 5. Metryki

| Metryka | polars-bio | Ballista | Sail |
|---|---|---|---|
| **Czas** — od wysłania zapytania do skonsumowania ostatniego wiersza wyniku, mierzony w runnerze (`perf_counter` / `Instant`), bez startu procesu, importów i startu klastra | ✓ | ✓ | ✓ |
| **Fazy** | tylko całość | etapy i operatory z `EXPLAIN ANALYZE` (osobny, dodatkowy przebieg) | suma czasu wywołań `pb.*()` i suma czasu oczekiwania na blokadę, logowane z UDTF |
| **Szczyt pamięci** | `VmHWM` z `/proc/<pid>/status` świeżego procesu | `VmHWM` każdego procesu; licznik zerowany przed przebiegiem (`echo 5 > /proc/<pid>/clear_refs`) | jak Ballista (jeden proces) |
| **Wolumen shuffle** | — | rozmiar plików shuffle w katalogach roboczych executorów (przyrost w przebiegu) + rozmiar ładunku broadcast w planie | niedostępny (asymetria, jawnie) |

Pamięć klastra raportowana jako suma szczytów procesów — **górne oszacowanie** (szczyty nie
muszą być jednoczesne) — oraz maksimum per węzeł.

## 6. Protokół pomiaru

1. **Przygotowanie serii:** zamknięty VS Code (seria uruchamiana skryptem z Windows Terminal),
   zamknięte aplikacje w tle, `.wslconfig` = 5 GB / 4 GB swap, laptop na zasilaczu, plan
   zasilania „wysoka wydajność”.
2. **Bloki:** dla każdej pary (N, silnik): start klastra → przebieg kontrolny → 1 przebieg
   rozgrzewkowy na scenariusz (odrzucany) → **5 rund mierzonych**; w każdej rundzie scenariusze
   w losowej kolejności (ziarno zapisywane) → przebieg kontrolny → zatrzymanie klastra.
3. **Przebieg kontrolny:** `overlap` na parze 1-2, polars-bio A. Rozjazd > 10% między
   początkiem a końcem bloku ⇒ blok do powtórzenia.
4. **Konsumpcja wyniku:** strumieniowo po stronie klienta, bez zapisu na dysk; liczba wierszy
   i suma kontrolna niezależna od kolejności wierszy (sekcja 8.4). Każdy pomiar jest zarazem
   testem poprawności względem polars-bio A.
5. **Nieważność przebiegu:** przyrost `pswpout` w `/proc/vmstat` > 0; niezgodność liczby wierszy
   lub sumy kontrolnej; przekroczenie limitu 20 minut. Timeout zapisywany jako wynik („timeout”),
   bez ponawiania.
6. **Statystyki:** mediana (wartość główna), min i max (rozrzut), średnia (zgodność
   z polars-bio-bench). Przyspieszenia liczone z median.
7. **Zapis:** jeden wiersz na przebieg (sekcja 8.5); surowe plany `EXPLAIN ANALYZE` do plików.

Różnice względem polars-bio-bench (`src/run-benchmarks.py`, `timeit.repeat`, 3 powtórzenia,
min/max/średnia, pamięć przez `mprof` w osobnych przebiegach, wszystkie narzędzia w jednym
procesie, czas bez wczytania danych): mierzymy **od pliku do wyniku** (równoległe czytanie
przez executory jest częścią korzyści z rozproszenia; wczytanie widoczne w fazach), w **osobnych
procesach** (globalny kontekst polars-bio, klaster wieloprocesowy), pamięć przez `/proc` w tym
samym przebiegu (bez próbkowania), 5 powtórzeń + rozgrzewka, dodatkowo mediana.

## 7. Macierz eksperymentów

### 7.1 Macierz główna (P1 + P2)

Kolumny: wartości N = 1 / 2 / 3. „polars-bio 1” = wariant A, „2 / 3” = wariant B.

| Operacja | Dane | Klasa | polars-bio | Ballista | Sail |
|---|---|:---:|:---:|:---:|:---:|
| overlap | 1-2 (54 tys. wierszy wyniku) | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| overlap | 2-7 (274 tys.) | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| overlap | 7-0 (2,8 mln) | M | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| overlap | 7-3 (4,4 mln) | M | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| overlap | 0-8 (164 mln) | L | ☁ | ☁ | ☁ |
| nearest | 1-2 | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| nearest | 2-7 | S | ✓ ✓ ✓ | ⚠ ⚠ ⚠ | ✓ ✓ ✓ |
| nearest | 7-0 | M | ✓ ✓ ✓ | ⚠ ⚠ ⚠ | ✓ ✓ ✓ |
| coverage | 1-2 | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| coverage | 2-7 | S | ✓ ✓ ✓ | ⚠ ⚠ ⚠ | ✓ ✓ ✓ |
| coverage | 7-0 | M | ✓ ✓ ✓ | ⚠ ⚠ ⚠ | ✓ ✓ ✓ |
| merge | 1 (fBrain) | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| merge | 2 (exons) | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| merge | 0 (chainRn4) | M | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| subtract | 1-2 | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| subtract | 2-7 | S | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |
| subtract | 7-0 | M | ✓ ✓ ✓ | ✓ ✓ ✓ | ✓ ✓ ✓ |

Legenda: ✓ — zaplanowane lokalnie; ☁ — tylko chmura (reguła kalibracji); ⚠ — ryzyko limitu
wiadomości gRPC przy broadcaście w Ballistcie (sekcja 9.2); jeśli nie da się go usunąć, komórka
otrzymuje wynik „niewykonalne w tym wzorcu” z uzasadnieniem. Wszystkie pary lokalne są
**kandydatami** — ostateczną listę ustala kalibracja.

Wolumen: 16 scenariuszy lokalnych × 9 wariantów × (1 + 5) przebiegów ≈ 860 przebiegów
+ 48 przebiegów `EXPLAIN ANALYZE` (Ballista, 16 × 3).

### 7.2 Macierz P3 (algorytmy w `overlap`)

| Dane | Algorytmy | polars-bio A | polars-bio B (N = 3) | Ballista N = 3 | Sail N = 3 |
|---|---|:---:|:---:|:---:|:---:|
| 1-2, 2-7, 7-0 | Coitrees, IntervalTree, ArrayIntervalTree, Lapper, SuperIntervals | ✓ | ✓ | ✓ | ✓ |

Wolumen: 3 × 5 × 4 × 6 ≈ 360 przebiegów.

### 7.3 Kolejność etapów

| # | Etap | Wynik |
|---|---|---|
| 0 | **Smoke** — pełna macierz na parze 1-2 (merge: zbiór 1), 1 przebieg, bez rozgrzewki | poprawność narzędzia; uruchamiany także przed każdą dużą serią |
| 1 | **Kalibracja** | lista par lokalnych + dane do oszacowania kosztów chmury |
| 2 | **P1** — kolumny N = 1 i N = 3 | „czy i kiedy rozproszenie się opłaca” |
| 3 | **P2** — dołożenie N = 2 | pełna krzywa skalowania |
| 4 | **P3** | ranking algorytmów lokalnie vs rozproszone |
| 5 | **Analiza** | tabele i wykresy do pracy (mediany, przyspieszenia, COST) |

Wyniki każdego etapu są samodzielnie użyteczne — przerwanie po etapie 2 nadal daje odpowiedź
na P1.

## 8. Narzędzie pomiarowe

### 8.1 Struktura

```
bench/
  conf/            smoke.yaml, kalibracja.yaml, p1.yaml, p2.yaml, p3.yaml
  orchestrator.py  sterowanie blokami, walidacja, zapis wyników
  metrics.py       /proc: VmHWM, clear_refs, pswpout; rozmiar katalogów shuffle
  checksum.py      suma kontrolna (implementacja referencyjna)
  runners/
    polars_bio_runner.py
    sail_runner.py
  data/download.py pobranie databio-8p (poza gitem)
  analyze.py       wyniki -> tabele i wykresy
  results/         surowe wyniki (Parquet) + plany EXPLAIN ANALYZE (w repozytorium)
ballista_genomics/src/bin/bench_client.rs   runner Ballisty (Rust)
tests/bench/       testy narzędzia
```

Ballista nie wymaga wrappera pythonowego: runnerem jest binarka Rust `bench_client`,
uruchamiana przez orkiestrator jako proces. Istniejąca binarka `dist_ops` (używana przez testy)
pozostaje bez zmian. `bench_client` powstał w planie 2 z częścią protokołu (liczba wierszy,
zapis wyniku do Parquet, wynik w schemacie 8.4); czas, fazy i suma kontrolna — plan 3.

### 8.2 Konfiguracja (YAML)

Każdy plik opisuje serię: listę scenariuszy (`op`, `pair` lub `dataset`, opcjonalnie
`algorithm`), listę wariantów (`polars_bio_a`, `polars_bio_b`, `ballista`, `sail`), wartości N,
liczbę powtórzeń (`repeats`, domyślnie 5), rozgrzewkę (`warmup`, domyślnie 1), limit czasu
(`timeout_s`, domyślnie 1200), ziarno losowania kolejności (`seed`). Konfiguracja jest
walidowana przed startem serii (nieznane klucze, brak plików danych ⇒ błąd przed pierwszym
pomiarem).

### 8.3 Protokół runnera

Runner to osobny proces wywoływany z argumentami scenariusza. Na standardowe wyjście zwraca
**jedną linię JSON**:

```json
{"rows": 54246, "checksum": "0x…", "t_total_s": 1.234,
 "phases": {"...": 0.0}, "extra": {"broadcast_bytes": 0}}
```

Błąd silnika ⇒ niezerowy kod wyjścia i komunikat na stderr; orkiestrator zapisuje przebieg jako
nieważny z przyczyną.

### 8.4 Suma kontrolna

Niezależna od kolejności wierszy: suma (mod 2⁶⁴) skrótów wierszy, gdzie skrót wiersza to
kombinacja `crc32(chrom)` i kolumn liczbowych operacji z ustalonymi mnożnikami. Kolumny per
operacja:

| Operacja | Kolumny w skrócie wiersza |
|---|---|
| overlap | chrom, start₁, end₁, start₂, end₂ |
| nearest | chrom, start₁, end₁, distance (bez identyfikacji sąsiada — remisy) |
| coverage | chrom, start, end, coverage |
| merge | chrom, start, end, n_intervals |
| subtract | chrom, start, end |

Nazwy kolumn wyjściowych polars-bio 0.28 (zweryfikowane): `overlap` — `chrom_1, start_1, end_1,
chrom_2, start_2, end_2`; `nearest` — jak `overlap` + `distance`; `coverage` — `chrom, start, end,
coverage` (wiersze df1); `merge` — `chrom, start, end, n_intervals`; `subtract` — `chrom, start,
end`. Runnery Ballisty i Saila normalizują nazwy do tego schematu przed liczeniem sumy.

Implementacja referencyjna w Pythonie (`bench/checksum.py`) i równoważna w Rust (`bench_client`);
zgodność sprawdzana testem na wspólnych wartościach wzorcowych.

### 8.5 Schemat wyników

Jeden wiersz na przebieg: `timestamp`, `git_commit`, `engine_versions`, `series`, `seed`,
`scenario_id`, `op`, `pair`, `algorithm`, `variant`, `n_nodes`, `rep`, `is_warmup`, `is_control`,
`valid`, `invalid_reason`, `rows`, `checksum`, `t_total_s`, `phases` (JSON), `peak_rss`
(JSON: proces → bajty), `peak_rss_sum`, `shuffle_bytes`, `broadcast_bytes`, `pswpout_delta`.

### 8.6 Testy narzędzia (TDD)

- jednostkowe: parsowanie `/proc` (na plikach wzorcowych), walidacja YAML, suma kontrolna
  (Python i Rust na wspólnych wartościach wzorcowych), generowanie kolejności z ziarna,
  reguły nieważności przebiegu;
- integracyjny: konfiguracja `smoke` (kilka minut, oznaczony markerem pytest);
- istniejące 42 testy bez zmian.

## 9. Zmiany w istniejącym kodzie (warunki wstępne)

### 9.1 P0 — Ballista w osobnych procesach (zadanie wstępne)

Własna binarka `ballista_genomics/src/bin/ballista_node.rs` z dwiema rolami (`scheduler`,
`executor`) — jedna zamiast dwóch, bo na tej maszynie dominującym kosztem iteracji jest
linkowanie. Uruchamia scheduler i executor Ballisty 53 z zarejestrowanymi koderami
(`SchedulerConfig::override_logical_codec` / `override_physical_codec`, analogicznie
`ExecutorProcessConfig`) oraz z bio-owym stanem sesji po stronie schedulera (tryb standalone
przekazywał go niejawnie ze stanu klienta); flaga `--no-codecs` w executorze (kontrola
negatywna). Scheduler w trybie push z rozdziałem zadań round-robin (deterministyczne
rozłożenie zadań na executory) i z wyłączonym sprzątaniem danych zakończonych zadań. W `runner.rs` tryb `remote_with_state(url, state)`, gdy
podany jest adres schedulera; bez niego — dotychczasowy tryb standalone.

Test `tests/test_ballista_multiprocess.py` stawia scheduler i 2 executory (natywnie, bez Dockera,
po 2 sloty zadań) i dla 5 operacji sprawdza cztery dowody:

1. cztery różne PID-y (scheduler, 2 executory, klient);
2. wynik zgodny z polars-bio (znormalizowane zbiory krotek, jak w istniejących testach);
3. zadania wykonane na **obu** executorach i dane shuffle przekazane między nimi (logi
   i katalogi robocze executorów) — dla operacji z co najmniej dwoma zadaniami na etap
   (`merge`, `subtract`, `nearest`, `coverage`; przekazanie shuffle — `merge`, `subtract`).
   `overlap` czyta pojedyncze pliki i ma jedno zadanie na etap (brak równoległości hash),
   więc dla niego dowodem jest wykonanie etapu w procesie executora, a nie na obu;
4. executor uruchomiony z `--no-codecs` odrzuca plan przy dekodowaniu („Could not deserialize ...”,
   komunikat zwrócony schedulerowi), więc zapytanie nie daje wyniku — plan jest dekodowany po
   stronie executora. Ballista 53 w trybie push nie zgłasza tego jako błędu zapytania: uznaje
   executor za utraconego i ponawia po jego ponownej rejestracji (zapytanie wisi).

Bez pomiarów czasu. Znane ryzyko poza zakresem P0: ładunek planu przekazuje **ścieżki** plików,
więc procesy muszą współdzielić system plików (lokalnie spełnione; w kontenerach/chmurze —
wspólny magazyn).

### 9.2 Limit wiadomości gRPC

Ballista 53 pozwala zmienić limit bez modyfikacji silnika: klient —
`ballista.client.grpc_max_message_size` (konfiguracja sesji); scheduler —
`grpc_server_max_decoding_message_size`; executor — `grpc_max_decoding_message_size`
/ `grpc_max_encoding_message_size`. Limit podnoszony w binarkach z 9.1; skuteczność
potwierdza przebieg smoke na parze z tabelą broadcastowaną > 16 MB. Plan zapasowy: ładunek
broadcastu zawiera ścieżkę do pliku Parquet zamiast danych, a executor czyta tabelę sam.
Koszt wzorca (ładunek × liczba zadań) jest raportowany jako `broadcast_bytes`.

Ustalenia planu 2 (`ballista_genomics/OPIS.md`): stroną broadcastowaną w `nearest`
i `coverage` jest df2 (tabela indeksowana), nie df1. Para 1-2 mieści się w domyślnym
limicie 16 MiB klastra z osobnych procesów (broadcast exons, szacunkowo ok. 10 MiB na
zadanie); 2-7 i 7-0 według szacunku go przekraczają. Tryb standalone (jeden proces, tryb
pull) ma niekonfigurowalny limit 4 MiB klienta gRPC executora i przy broadcaście na
prawdziwych danych zapytanie wisi — standalone nie jest wariantem pomiarowym.

### 9.3 Pozostałe

- CSV → Parquet — **zrobione (plan 2)**: `dist_provider.rs` rozpoznaje Parquet po ścieżce
  (plik `*.parquet` albo katalog z takimi plikami); Sail — `sail_bio.py`
  (`spark.read.parquet`, UDTF-y dla danych bez kolumny z nazwą). Pobieranie danych:
  `bench/data/download.py` (zamiast `gdown` — `requests`, bez nowej zależności).
- Parametr `algorithm` dla `overlap`: przez koder Ballisty i UDTF Saila (P3).
- Instrumentacja UDTF Saila: czas wywołania `pb.*()` i czas oczekiwania na blokadę w
  `sail_pb_guard`, zapisywane do pliku przebiegu.

## 10. Zadania osobne (poza tą specyfikacją)

- **P0** — projekt w sekcji 9.1; wykonywany jako pierwszy.
- **Próba Saila na Kubernetesie:** kind lub k3d, 2 workery Saila, dane z konfiguracji smoke,
  WSL 5 GB; ograniczona do jednej sesji roboczej (~4 h). Sukces = smoke przechodzi z workerami
  w osobnych podach; wtedy emulację węzłów zapewniają limity CPU/pamięci podów, a kolumny Sail
  N = 2 (i ewentualnie 3) otrzymują wariant rozproszony. Porażka = udokumentowany wynik
  negatywny.
- **Chmura:** te same serie w większej skali; **oszacowanie kosztów** wykonywane po kalibracji,
  na podstawie zmierzonych czasów lokalnych i aktualnego cennika.
- **Lakehouse:** wariant źródła danych i hipoteza „partycjonowanie po chromosomie w warstwie
  składowania usuwa shuffle”.
- **Drobniejsze partycjonowanie** (chromosom + przedziały genomu) — kierunek dalszych prac;
  lokalnie niewidoczne (sufit ≈ 24 partycje ≫ 3 węzły).

## 11. Ryzyka i ograniczenia trafności

| Ryzyko | Wpływ | Środek zaradczy |
|---|---|---|
| Emulacja węzłów na jednej maszynie (wspólne L3, pamięć, dysk; brak sieci) | zawyżona sprawność rozproszenia względem prawdziwego klastra | opis w pracy; chmura jako rozszerzenie |
| Wirtualne CPU w WSL przenoszone przez Hyper-V | szum pomiarowy | 5 powtórzeń, mediana, przebieg kontrolny |
| Przegrzewanie / tryb oszczędzania energii laptopa | dryf czasów | zasilacz, plan wydajności, losowa kolejność, przebieg kontrolny |
| Asymetria Saila (jeden proces, blokada) | porównanie Ballista vs Sail niesymetryczne | jawny opis, pomiar czasu blokady, próba K8s |
| Zbiory AIList różnią się strukturą, nie tylko rozmiarem | pomieszanie efektu rozmiaru i struktury w P1 | raportowanie *non-flatness* przy każdej parze |
| Limit gRPC przy broadcaście | brak wyników dla części komórek | sekcja 9.2 |
| Pamięć 5 GB | mniej par lokalnych | reguła kalibracji; pary odrzucone → chmura |

## 12. Do ustalenia na dalszym etapie

- główny wariant punktu odniesienia (A czy B);
- dostęp do chmury i jej finansowanie;
- konkretna architektura lakehouse i sposób podłączenia źródła danych;
- zakres operacji `complement` i `cluster` (poza obecnym etapem).

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

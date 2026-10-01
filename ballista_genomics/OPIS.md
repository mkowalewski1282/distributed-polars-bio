# Opis prototypu: overlap w DataFusion/Ballista przez datafusion-bio-function-ranges

*Zaktualizowano: sierpień 2026, Faza A planu pracy magisterskiej.*

## Faza A.2 (ZWERYFIKOWANA): prawdziwy silnik polars-bio w DataFusion

Poprzednia wersja tego prototypu (`genomic_overlap` UDF) reimplementowała naiwny
warunek overlap jako predykat w `WHERE` — **nie korzystała z COITrees w ogóle**,
tylko z cross-joina O(n·m). Obecna wersja `src/main.rs` używa prawdziwego silnika
algorytmicznego stojącego za polars-bio: crate'a
[`datafusion-bio-function-ranges`](https://github.com/biodatageeks/datafusion-bio-functions)
(git dependency, Apache-2.0), przez wygodną funkcję `create_bio_session()`, która:

- instaluje `IntervalJoinPhysicalOptimizationRule` (przepisuje range-join na
  `IntervalJoinExec` z wyborem algorytmu: Coitrees/IntervalTree/Lapper/SuperIntervals),
- rejestruje SQL table function `overlap(left, right, cols..., 'strict'|'weak')`.

**UWAGA (nieoczywista pułapka):** domyślny `filter_op` to `"weak"` (granice
stykające się liczą jako overlap — `>=`/`<=`). polars-bio/bedtools/UCSC używają
half-open `[start, end)` z ostrą nierównością — to odpowiada `filter_op="strict"`
w tym crate, mimo mylącej nazwy. Trzeba jawnie podać `'strict'`.

### Wynik (zweryfikowany, `cargo run`)

8 par nakładających się interwałów, identyczne z lokalnym `pb.overlap()` — i
tym razem faktycznie liczone przez COITrees, nie ręcznie napisany warunek.

---

## Faza A.3 (ZWERYFIKOWANA): prawdziwa Ballista standalone — błąd LogicalExtensionCodec potwierdzony

`src/main.rs` uruchamia prawdziwy klaster Ballista (`SessionContext::standalone_with_state()`,
feature `standalone` w `Cargo.toml`, sesja skonfigurowana przez `BioSessionExt::new_with_bio`)
zamiast gołego DataFusion jak w Fazie A.2 — z tym samym operatorem `overlap()`.

### Wynik (zweryfikowany, `cargo run`)

Klaster startuje poprawnie (scheduler + executor in-proc, logi potwierdzają połączenie).
Zapytanie z `overlap('intervals_a', 'intervals_b', ...)` kończy się błędem:

```
Error: Internal("failed to serialize logical plan: Context(\"Error serializing custom table
at .../datafusion-proto-53.1.0/src/logical_plan/mod.rs:1194\",
NotImplemented(\"LogicalExtensionCodec is not provided\"))")
```

To dokładnie ten sam problem co w poprzednim prototypie (naiwny UDF), ale teraz udokumentowany
na PRAWDZIWEJ funkcji silnika polars-bio, nie na placeholderze. Ważny szczegół z treści błędu:
problem dotyczy konkretnie serializacji **custom table** (`OverlapProvider` jako `TableProvider`
zarejestrowany przez `register_udtf`) w logicznym planie — czyli potrzebny jest
`LogicalExtensionCodec` implementujący `try_encode_table_provider`/`try_decode_table_provider`,
nie ogólny kodek UDF-ów.

### Dlaczego to jest trudniejsze niż Faza A.2: LogicalExtensionCodec

Ballista to klaster — scheduler i executory to osobne procesy/komponenty
komunikujące się przez sieć (protobuf). Standardowe operacje DataFusion mają
wbudowany kodek. Niestandardowy `TableProvider` (`OverlapProvider` z
bio-function-ranges) — nie, potrzebuje własnego `LogicalExtensionCodec`.

## Faza A.4 (ZWERYFIKOWANA): LogicalExtensionCodec — pełne, poprawne wykonanie rozproszone

**Wynik: 8/8 par, identyczne z lokalnym `pb.overlap()`, na prawdziwym klastrze Ballista
(scheduler + executor, z realnym shuffle między nimi — widoczne w planie jako
`ShuffleReaderExec`).** To zamyka główny cel Fazy A: pokazanie, że silnik polars-bio da się
"wpiąć" w Ballistę jako runtime extension (rejestracja + kodek), bez forkowania Ballisty.

### Jak to zrobiono

Zamiast serializować prywatne pola `OverlapProvider` z bio-function-ranges (niedostępne
spoza crate'a), zdefiniowano **własny** `TableProvider` — `DistOverlapProvider` — z jawnymi,
naszymi polami (nazwy tabel, ścieżki CSV, kolumny, `strict`/`weak`), który w `scan()`
**deleguje** do prawdziwego `OverlapProvider::new(...)` (konstruktory tego typu SĄ publiczne).
Zarejestrowany pod własną nazwą SQL `dist_overlap(left_table, left_csv, right_table,
right_csv, col_chrom, col_start, col_end, 'strict')`.

`LogicalExtensionCodec::try_encode_table_provider`/`try_decode_table_provider`
serializują tylko te jawne parametry (ręczne, minimalne kodowanie binarne — bez protobuf,
bez `protoc`, bo to tylko kilka stringów + bool) i **odtwarzają** `DistOverlapProvider` po
drugiej stronie sieci, budując dla niego świeżą, samodzielną sesję (rejestruje CSV od nowa)
— zgodnie z przewidywaniem z Fazy A.3: nie trzeba serializować planu, tylko "przepis" na
jego odtworzenie.

**Nie trzeba było omijać `SessionContextExt::standalone_with_state()`** ani dodawać
`ballista-core`/`-scheduler`/`-executor` jako osobnych zależności — wystarczyło ustawić
kodek na `SessionConfig` przez `SessionConfigExt::with_ballista_logical_extension_codec(...)`
(re-eksportowane przez `ballista::prelude`) — Ballista sama czyta go z konfiguracji sesji
przy starcie schedulera i executora (potwierdzone w źródle:
`ballista-executor-53.0.0/src/standalone.rs::new_standalone_executor_from_state`).

### Ślepa uliczka po drodze: IntervalJoinExec i dwupoziomowy problem sesji

Po naprawieniu logicznego kodeka pojawił się KOLEJNY, przewidziany błąd: fizyczny węzeł
`IntervalJoinExec` (algorithm: Coitrees, produkowany przez
`IntervalJoinPhysicalOptimizationRule`) też nie ma domyślnego kodeka — a jego pola są
prywatne (tak jak `OverlapProvider`), więc nie da się dla niego łatwo napisać
`PhysicalExtensionCodec` z zewnątrz crate'a.

Próba obejścia przez `BioConfig.prefer_interval_join = false` **nie zadziałała** — ta flaga
jest czytana tylko przez `BioQueryPlanner` (custom query planner), a
`IntervalJoinPhysicalOptimizationRule` to OSOBNY mechanizm (physical optimizer rule),
instalowany bezwarunkowo przez `BioSessionExt::new_with_bio()` i niezależny od tego configu.

Rzeczywiste rozwiązanie miało DWIE warstwy (obie konieczne):
1. Wewnętrzna, jednorazowa sesja w `DistOverlapProvider::build()` (ta, która faktycznie
   wykonuje `OverlapProvider::scan()` → `session.sql(join_query)`) musi być **zwykłą**
   sesją DataFusion (`SessionContext::new()`), nie bio-ową — bo `join_query()` to zwykły
   SQL join, nie potrzebuje żadnych funkcji bio.
2. **To nie wystarczyło samo w sobie** — sesja SCHEDULERA/klienta (budowana w `main()`,
   przekazywana do `standalone_with_state()`) TEŻ musiała przestać być bio-owa. Powód:
   reguły fizycznego optymalizatora działają przez `transform_up`/`transform_down` na
   CAŁYM drzewie planu — nawet jeśli poddrzewo zwrócone przez nasz `TableProvider` zostało
   zbudowane przez sesję BEZ tej reguły, sesja SCHEDULERA (jeśli bio-owa) i tak ją
   ponownie zastosuje przy planowaniu całego zapytania, bo widzi cały finalny plan.

**Koszt TEGO rozwiązania (Faza A.4, HISTORYCZNY — naprawiony w Fazie A.5 niżej):** ta w
pełni rozproszona ścieżka wykonania NIE używała COITrees — dostawała standardowy
`HashJoinExec` + filtr. To zostało naprawione, patrz sekcja poniżej.

## Faza A.5 (ZWERYFIKOWANA): PhysicalExtensionCodec dla IntervalJoinExec — COITrees w pełni rozproszone

**Wynik: 8/8 par, identyczne z baseline, na prawdziwym klastrze Ballista, TERAZ Z COITrees**
(`IntervalJoinExec`, `algorithm: Coitrees`, w planie fizycznym executora). Zweryfikowane
formalnym testem: `tests/test_ballista_overlap.py`.

### Dlaczego wcześniejsza ocena ("prywatne pola, trzeba reimplementować od zera") była zbyt pesymistyczna

Sprawdzone dokładniej: `IntervalJoinExec` **ma publiczny konstruktor** `try_new(...)` i
publiczne gettery dla większości pól (`left()`, `right()`, `on()`, `filter()`, `join_type()`,
`partition_mode()`, `null_equals_null()`). Jedyny brakujący element to `ColIntervals`
(argument `try_new`) — zdefiniowany w module `intervals`, zadeklarowanym jako `mod
intervals;` (bez `pub`) w `physical_planner/mod.rs`. W Rust to sprawia, że `ColIntervals`
jest **całkowicie nienazywalny i niekonstruowalny spoza crate'a** mimo bycia `pub struct`
— cała ścieżka do typu musi być publiczna, nie tylko sam typ.

### Rozwiązanie: lokalna, jednoliniowa łatka widoczności (`vendor/`)

**To NIE jest fork Ballisty ani Saila** — to poprawka widoczności w pomocniczej bibliotece
`datafusion-bio-function-ranges` (Apache-2.0), zwendorowana lokalnie w
`ballista_genomics/vendor/datafusion-bio-function-ranges/` (pełna historia i uzasadnienie:
`vendor/PATCH.md`). Zmiana:
```diff
-mod intervals;
+pub mod intervals;
```
plus wygodny re-export `ColInterval`/`ColIntervals`/`parse` w `lib.rs`. To bezpieczna,
bezkonfliktowa zmiana widoczności (żadnej logiki), gotowa do zgłoszenia jako mały PR
upstream do biodatageeks — gdyby wylądowała, katalog `vendor/` przestałby być potrzebny.

### Implementacja kodeka (`src/physical_codec.rs`)

`IntervalJoinExec` to węzeł BINARNY (join) — DataFusion serializuje jego dzieci (left/right)
automatycznie i rekurencyjnie (`PhysicalExtensionNode { node, inputs }` — `inputs` już
zawiera zdeserializowane poddrzewa), więc kodek musi serializować tylko WŁASNE parametry:
- `on`/`filter.expression()` — `Arc<dyn PhysicalExpr>`, serializowane przez publiczne funkcje
  `datafusion_proto::physical_plan::{to_proto::serialize_physical_expr, from_proto::parse_physical_expr}`
  (te same, których DataFusion używa dla standardowych joinów — nic nie trzeba pisać od zera).
- `filter`'s schema (pośredni schemat) — NIE serializowany osobno, odtwarzany z
  `column_indices` + schematów left/right (dokładnie do tego `column_indices` służy).
- `ColIntervals` — NIE serializowany wprost, odtwarzany z `filter` przez `parse_intervals()`
  (ta sama funkcja, której `IntervalJoinPhysicalOptimizationRule` używa za pierwszym razem —
  wynik identyczny niezależnie od strony).
- `join_type`, `partition_mode`, `null_equals_null` — proste enumy/boole.
- `algorithm`, `low_memory` — BRAK publicznych getterów na `IntervalJoinExec` (sprawdzone),
  ale nie są potrzebne: to wartości z `BioConfig` sesji, znane kodekowi z góry (identyczne
  na schedulerze i executorze), nie odczytywane z instancji węzła.

Reszta — ręczne, minimalne kodowanie binarne (jak w `main.rs` dla `DistOverlapProvider`),
bez protobuf/`.proto` (poza tym, że same `PhysicalExprNode` protobuf messages z
`serialize_physical_expr` są zagnieżdżone jako bajty przez `prost::Message::encode_to_vec()`).

### Napotkany, nieoczywisty bug runtime: `PartitionMode::Auto`

Pierwsza próba (kodek działał, plan docierał do executora) failowała runtime'owym błędem:
`"Invalid IntervalSearchJoinExec, unsupported PartitionMode Auto in execute()"`. Przyczyna:
`with_config_rt_bio()` USUWA standardową regułę `join_selection` (ta, która normalnie
rozstrzyga `Auto` → `Partitioned`/`CollectLeft` przed wykonaniem) i zastępuje ją wyłącznie
`IntervalJoinPhysicalOptimizationRule`, która NIE rozstrzyga `Auto` sama — węzeł zostaje z
`partition_mode=Auto`. Lokalnie (jeden proces) to nie przeszkadzało, ale rozproszony
executor Ballisty odrzuca `Auto` w `execute()`. Poprawka: kodek wymusza `Partitioned` przy
odtwarzaniu węzła (dane i tak są partycjonowane wg klucza joina między executory — to
jedyny sensowny tryb w tym kontekście). **Nieaktualne od planu 2:** założenie o partycjonowaniu
okazało się błędne — przy danych z kilku plików `Partitioned` gubił pary; koder zamienia teraz
`Auto` na `CollectLeft` (patrz „Plan 2 → Znaleziska”).

### Zmiana architektoniczna: powrót do sesji bio-owych

Faza A.4 celowo używała ZWYKŁYCH (nie-bio) sesji — i wewnętrznej w `DistOverlapProvider`, i
schedulera w `main()` — żeby UNIKNĄĆ tworzenia `IntervalJoinExec` (brak kodeka). Teraz, mając
kodek, obie te sesje wróciły do `new_with_bio()` — inaczej `IntervalJoinPhysicalOptimizationRule`
w ogóle by nie zadziałała i join zostałby zwykłym `HashJoinExec` (poprawnie, ale bez COITrees).

### Walidacja: czy to przenośne do polars-bio?

Tak, z zastrzeżeniem: cały mechanizm (`DistOverlapProvider` + oba kodeki) żyje w
`ballista_genomics/` (nasz kod, poza polars-bio — zgodnie z ustaleniami sesji), więc
przeniesienie do polars-bio wymagałoby: (1) przepisania go z prototypu (Rust binary) na
faktyczny moduł biblioteki polars-bio (nowa `DistributedOverlapRule`, patrz
`architektura_draft.md`), (2) decyzji czy wendorowana łatka (`vendor/PATCH.md`) zostaje
lokalna czy czeka na prawdziwy PR upstream do biodatageeks/datafusion-bio-functions (zalecane:
zgłosić PR — to jednoliniowa, bezpieczna zmiana widoczności, powinna zostać łatwo
zaakceptowana), (3) rozszerzenia kodeka na pozostałe operacje (merge/nearest/coverage/subtract),
z których KAŻDA ma WŁASNY, analogicznie prywatny typ Exec (`MergeExec`, `NearestExec`, itd.) —
wzorzec z tej sesji (zbadaj gettery/konstruktor, ewentualnie zwenduj+załataj) powinien się
powtarzać, ale wymaga osobnej pracy per operacja.

### Jak faktycznie zaimplementowano kodek (korekta wcześniejszego przewidywania)

Pierwotnie zakładano (patrz historia commitów), że trzeba ominąć
`SessionContextExt::standalone_with_state()` i wywoływać niżej-poziomowe funkcje
`ballista_scheduler`/`ballista_executor` bezpośrednio, bo `BallistaCodec::default()`
jest tam rzekomo zaszyty na sztywno. **To nieprecyzyjne** — `standalone_with_state()`
faktycznie tworzy `BallistaCodec::default()` gdy buduje sesję "od zera"
(`SessionStateExt::new_ballista_state`), ale gdy przekazujemy WŁASNY `SessionState`
(przez `standalone_with_state`), Ballista wywołuje `upgrade_for_ballista()`, który
czyta kodek z **konfiguracji przekazanej sesji** — więc wystarczy ustawić go
WCZEŚNIEJ, na `SessionConfig`, przez `SessionConfigExt::with_ballista_logical_extension_codec(...)`
(re-eksportowane przez `ballista::prelude`) — potwierdzone w źródle:
`ballista-executor-53.0.0/src/standalone.rs::new_standalone_executor_from_state`
czyta `session_state.config().ballista_logical_extension_codec()`. Nie trzeba więc
dodawać `ballista-core`/`-scheduler`/`-executor` jako osobnych zależności.

Domyślny kodek Ballisty (do delegowania wszystkiego, co nie jest naszym typem) też
nie wymaga nazywania konkretnego typu `BallistaLogicalExtensionCodec` — wystarczy
`SessionConfig::new().ballista_logical_extension_codec()`, co zwraca gotowy
`Arc<dyn LogicalExtensionCodec>` (domyślny, bo config jest pusty).

Punkt odniesienia, który pomógł zrozumieć KSZTAŁT rozwiązania (wzorzec "spróbuj
obsłużyć nasz typ, inaczej deleguj do `inner`"):
[ballista_extensions](https://github.com/milenkovicm/ballista_extensions)
(autor — `milenkovicm` — jest aktywnym committerem Ballisty, potwierdzone w
release notes 53.0.0/54.0.0).

---

## Wnioski dla pracy magisterskiej (zaktualizowane po Fazie A.5)

| | DataFusion (lokalnie) | Ballista distributed | Sail + UDTF |
|---|---|---|---|
| Rejestracja funkcji | `create_bio_session()` (crate bio-function-ranges) | własny wrapper (`DistOverlapProvider`) + `dist_overlap()` | Python `@udtf` (PR #1519) |
| Serializacja planu | nie potrzebna | `LogicalExtensionCodec` + `PhysicalExtensionCodec` (~50+250 linii, bez protobuf poza reużyciem `PhysicalExprNode`) | nie dotyczy (UDTF to Python call, nie węzeł planu DataFusion) |
| Używa COITrees? | ✅ tak | ✅ **tak, także w pełni rozproszonej ścieżce** (Faza A.5) | ✅ tak (woła prawdziwe pb.overlap() per grupa) |
| Status w tej pracy | ✅ zweryfikowane, identyczne z baseline | ✅ **zweryfikowane end-to-end na prawdziwym klastrze** (scheduler+executor, realny shuffle), identyczne z baseline, Z COITrees | ✅ zweryfikowane (ze znanym, udokumentowanym bugiem partycjonowania w LATERAL+UDTF — patrz `sail_overlap_udtf.py`) |
| Wymagał zmian poza własnym kodem? | nie | ✅ tak — jednoliniowa łatka widoczności w bio-function-ranges (`vendor/PATCH.md`), NIE fork Ballisty | nie |

**Zaktualizowany kluczowy wniosek:** oba silniki DAJĄ SIĘ rozszerzyć bez forkowania SIEBIE —
to jest osiągnięty, zweryfikowany wynik tej pracy dla operacji `overlap`, **z zachowaniem
pełnej wydajności algorytmicznej (COITrees) po stronie Ballisty**. Różnią się jednak
głębokością/dojrzałością tej integracji:
- **Ballista**: mechanizm jest kompletny (UDF/TableProvider + LogicalExtensionCodec +
  PhysicalExtensionCodec) i osiągalny w praktyce — wymaga własnego kodu Rust do
  serializacji, a dla operatorów z prywatnymi polami w bibliotekach zewnętrznych (jak
  `IntervalJoinExec`) dodatkowo małej, bezpiecznej łatki widoczności w TEJ bibliotece
  (nie w Ballistrze) — ale to się okazało wykonalne, nie ślepą uliczką.
- **Sail**: mechanizm jest prostszy (Python, brak potrzeby serializacji planu), ale
  ograniczony funkcjonalnie (tylko argumenty skalarne, nie TABLE) i ma realne bugi
  wykonania (LATERAL + wiele partycji).

Żaden z silników nie ma dziś (sierpień 2026) w pełni dojrzałego, GOTOWEGO OD RĘKI mechanizmu
integracji na poziomie planu zapytania dla DOWOLNEGO customowego operatora zewnętrznej
biblioteki — dla Saila taki mechanizm (`SailExtension`/FFI) jest w aktywnej fazie
projektowej (patrz `architektura_draft.md`); dla Ballisty mechanizm (`PhysicalExtensionCodec`)
istnieje i DZIAŁA, ale dla operatorów z prywatnymi polami wymaga (małej) współpracy/łatki
po stronie biblioteki rozszerzającej — w tej pracy pokonane bez forkowania Ballisty/Saila,
tylko wendorowaniem jednoliniowej poprawki widoczności w pomocniczym crate'cie.

## P0 — klaster z osobnych procesów (wrzesień 2026)

Cel: dowód, że wykonanie jest rzeczywiście rozproszone, a nie tylko poprawne
w trybie standalone (scheduler i executor w jednym procesie). Bez pomiarów czasu.

### Jak uruchomić ręcznie

Wszystkie polecenia z katalogu `ballista_genomics/` (ładunki planu przenoszą
ścieżki względne do danych), każde w osobnym terminalu. Kolejność ma znaczenie:
executor łączy się ze schedulerem tylko raz przy starcie, więc scheduler musi
już nasłuchiwać — inaczej executor kończy pracę z kodem 1.

    ./target/debug/ballista_node scheduler --port 50050
    ./target/debug/ballista_node executor --scheduler-port 50050 --port 50051 \
        --grpc-port 50052 --work-dir /tmp/ex1 --concurrent-tasks 2
    ./target/debug/ballista_node executor --scheduler-port 50050 --port 50061 \
        --grpc-port 50062 --work-dir /tmp/ex2 --concurrent-tasks 2
    BALLISTA_SCHEDULER_URL=df://localhost:50050 ./target/debug/dist_ops merge

Opcjonalnie `DIST_OUTPUT_DIR=<katalog>` kieruje wynik i plan EXPLAIN ANALYZE
poza domyślny `output/`.

### Co musiało się zmienić względem standalone

- Scheduler buduje bio-owy stan sesji sam (`cluster::bio_session_state`) —
  standalone dostawał go niejawnie od klienta.
- Kodery rejestrowane są w każdym procesie osobno (klient, scheduler, executor).
- Scheduler w trybie push z rozdziałem round-robin: przy pull i milisekundowych
  zadaniach jeden executor potrafiłby zgarnąć całą pracę.
- Sprzątanie danych zakończonych zadań wyłączone — pliki etapów są dowodem.

### Dowody (`tests/test_ballista_multiprocess.py`)

1. Cztery różne procesy; scheduler widzi dwa executory o różnych
   identyfikatorach i portach.
2. Wynik każdej z pięciu operacji zgodny z wyrocznią polars-bio. Zastrzeżenie (plan 2):
   dla `nearest` zgodność na tym zbiorze była przypadkowa — `dist_ops` ma orientację odwrotną
   do `pb.nearest` (patrz „Plan 2 → Znaleziska”); poprawną orientację sprawdza
   `tests/test_ballista_parquet.py`.
3. Pliki etapów w katalogach roboczych obu executorów. Każda operacja to dwa
   zapytania do klastra (wynik i EXPLAIN ANALYZE); w komórkach — numery etapów,
   dla których dany executor zapisał dane:

| Operacja | executor_1: etapy | executor_2: etapy |
|---|---|---|
| overlap | zapytanie 1: 1; zapytanie 2: 1 | — |
| merge | zapytanie 1: 1, 2, 3; zapytanie 2: 1, 2, 3 | zapytanie 1: 1, 2; zapytanie 2: 1, 2 |
| subtract | zapytanie 1: 1, 2, 3, 4; zapytanie 2: 1, 2, 3, 4 | zapytanie 1: 1, 2, 3; zapytanie 2: 1, 2, 3 |
| nearest | zapytanie 1: 1, 2; zapytanie 2: 1, 2 | zapytanie 1: 1; zapytanie 2: 1 |
| coverage | zapytanie 1: 1, 2; zapytanie 2: 1, 2 | zapytanie 1: 1; zapytanie 2: 1 |

   W `merge` i `subtract` oba executory liczyły etap 1 (zapis shuffle po
   chromosomie) i kolejne etapy, więc dane musiały przejść między procesami.
   `overlap` czyta pojedyncze pliki i nie ma równoległości hash (znane
   ograniczenie) — cały etap trafia do jednego executora.
4. Kontrola negatywna: executor uruchomiony z `--no-codecs` liczy etap 1
   (zwykłe węzły DataFusion), ale etapu z `MergeExec` nie potrafi zdekodować
   i zwraca schedulerowi:

       Status { code: InvalidArgument, message: "DataFusion error: Internal error:
       Could not deserialize BallistaPhysicalPlanNode: failed to decode Protobuf
       message: invalid tag value: 0 ..." }

   Plan jest więc dekodowany w executorze. Ballista 53 w trybie push nie
   zgłasza tego jako błędu zapytania: uznaje executor za utraconego
   („Removing executor …”), a po jego ponownej rejestracji ponawia — zapytanie
   wisi. Test sprawdza brak wyniku w oknie 45 s i powyższy komunikat w logu
   schedulera; z koderami to samo zapytanie kończy się w mniej niż sekundę (sprawdzone
   mutacyjnie).

### Znaleziska

- Tryb zdalny planuje końcowe sortowanie `overlap` w jednym etapie, standalone
  w dwóch (osobny etap `SortPreservingMergeExec`); złączenie `IntervalJoinExec`
  w obu trybach wykonuje się w etapie na executorze. Pozostałe cztery operacje
  mają w obu trybach tę samą liczbę etapów (merge 3, subtract 4, nearest 2,
  coverage 2).
- Odporność Ballisty: błąd dekodowania planu przy uruchamianiu zadania
  traktowany jest jak utrata executora, a nie jak błąd zadania — zapytanie
  nie kończy się błędem, tylko czeka.

Znane ryzyko poza zakresem P0: procesy muszą współdzielić system plików (ładunek
planu zawiera ścieżki) — w kontenerach i w chmurze potrzebny wspólny magazyn.

## Plan 2 — dane databio-8p i Parquet (wrzesień–październik 2026)

**Dane.** `python -m bench.data.download` pobiera `databio-8p.zip` z polars-bio-bench
(Google Drive, 1,2 GB) do `$BENCH_DATA_ROOT` (domyślnie `~/bench_data`), rozpakowuje wyłącznie
pliki `databio-8p/<zbiór>/part-*.parquet` i weryfikuje: 8 plików na zbiór, schemat
`contig: string, pos_start: int32, pos_end: int32`, liczby wierszy ze specyfikacji (sekcja 4).
Identyfikatory zbiorów i par: `bench/data/datasets.py` (`resolve("1-2")`).

**Ballista.** `DistBioProvider` rozpoznaje format po ścieżce: plik `*.parquet` albo katalog
z plikami `*.parquet` → Parquet, inaczej CSV. Brak ścieżki to błąd z jej nazwą (wcześniej błąd
rejestracji był połykany). Runner `bench_client` wykonuje jeden scenariusz:

    ./target/debug/bench_client --op overlap \
        --left ~/bench_data/databio-8p/fBrain-DS14718 --right ~/bench_data/databio-8p/exons
    {"rows": <liczba>}

Wynik w schemacie znormalizowanym (specyfikacja 8.4), konsumowany strumieniowo; `--output`
zapisuje go do Parquet; `RUST_LOG=info` włącza logi Ballisty na stderr. Klaster z osobnych
procesów — jak w P0, przez `BALLISTA_SCHEDULER_URL`.

**Sail.** `sail_bio.py`: `spark.read.parquet`, UDTF-y dla danych bez kolumny z nazwą, wynik
w schemacie 8.4.

**Testy.** `tests/test_ballista_parquet.py` i `tests/test_sail_parquet.py` — zbiór testowy
o typach prawdziwych danych, z przypadkami brzegowymi; `tests/test_real_data.py` (marker
`dane`) — para 1-2 w Ballistcie standalone, na klastrze i w Sailu, zgodność z polars-bio.

| Para 1-2 (fBrain × exons; merge: fBrain) | overlap | nearest | coverage | merge | subtract |
|---|:---:|:---:|:---:|:---:|:---:|
| Ballista, klaster z osobnych procesów | ✓ | ✓ | ✓ | ✓ | ✓ |
| Ballista standalone (jeden proces) | ✓ | wisi (4 MiB) | wisi (4 MiB) | ✓ | ✓ |
| Sail (`sail_bio.py`) | ✓ | ✓ | ✓ | ✓ | ✓ |

### Znaleziska

- **Overlap gubił pary przy danych z kilku plików** (poprawione w `physical_codec.rs`).
  Węzeł `IntervalJoinExec` ma w planie tryb `PartitionMode::Auto` — z nim wymagany rozkład
  wejść jest „nieokreślony”, więc planista nie wstawia ani repartycji po chromosomie, ani
  scalenia lewej strony. Koder (Faza A.5) zamieniał `Auto` na `Partitioned`, a wtedy
  partycja *i* lewej tabeli łączyła się tylko z partycją *i* prawej (na zbiorze testowym
  5 z 10 par). Teraz `CollectLeft`: każde zadanie buduje indeks z całej lewej strony, zadania
  dzielą się prawą. Dotąd niewidoczne, bo overlap w `dist_ops` czyta pojedyncze pliki
  (jedna partycja). Zgodne z hipotezą P3 (indeks budowany w każdym zadaniu). Tryb jest
  zmieniany dopiero przy dekodowaniu na executorze, więc plan schedulera i `EXPLAIN ANALYZE`
  nadal pokazują `mode=Auto` bez scalenia lewej strony — a w rzeczywistości każde zadanie
  czyta i indeksuje całą lewą tabelę (ważne przy analizie metryk w planie 3).
- **Konwencja stron w `nearest`** (jak w coverage): `dist_nearest(lewa, prawa)` zwraca
  wiersz na każdy wiersz PRAWEJ tabeli z najbliższym sąsiadem z lewej (lewa jest
  indeksowana i broadcastowana), a `pb.nearest(df1, df2)` — wiersz na każdy wiersz df1.
  `bench_client` zamienia strony. **`dist_ops` (dane zabawkowe) ma orientację odwrotną do
  polars-bio**; `test_ballista_distributed_nearest_matches_oracle_distances` przechodzi
  przypadkiem (5 × 5 przedziałów, wszystkie odległości 0, remisy) — do decyzji, czy poprawić
  ten test i SQL w `runner.rs`.
- **Tryb standalone Ballisty nie udźwiga broadcastu na prawdziwych danych.** Executor
  w standalone pobiera zadania (tryb pull) klientem gRPC z domyślnym limitem tonic 4 MiB,
  którego nie da się zmienić przez `SessionContext::standalone_with_state` (dałoby się
  przez własne uruchomienie schedulera i executora in-proc publicznymi funkcjami
  `new_standalone_*_from_state` z własnym klientem gRPC — bez zmian w Ballistcie); odpowiedź z kilkoma zadaniami — każde niesie całą
  tabelę broadcastowaną — jest większa („decoded message length too large: found 9808588
  bytes, the limit is: 4194304 bytes” już przy 100 tys. wierszy broadcastu). Executor ponawia
  w nieskończoność, więc zapytanie wisi, zamiast zakończyć się błędem. Klaster z osobnych
  procesów (tryb push, limit 16 MiB) liczy parę 1-2 poprawnie — broadcast exons (439 tys.
  wierszy) to szacunkowo ok. 10 MiB na zadanie. Dla 2-7 (broadcast ex-anno, 1,19 mln)
  i 7-0 (chainRn4, 2,35 mln) szacunek przekracza 16 MiB — podniesienie limitów w planie 3
  (specyfikacja 9.2).
- **LATERAL w Sailu powiela wiersz zewnętrzny dla każdego wiersza wyniku UDTF-a** — razem
  z listą wszystkich przedziałów chromosomu przekazaną jako argument. Przy UDTF-ie zwracającym
  wiersz na wynik pamięć rośnie jak (wiersze wyniku) × (rozmiar grupy): +3,8 GB przy 5000
  przedziałach w jednej grupie; na parze 1-2 proces był zabijany przez OOM, a raz wywrócił
  WSL. To własność Saila, nie polars-bio (czysto pythonowy UDTF zachowuje się tak samo).
  `sail_bio.py` zwraca więc jeden wiersz na chromosom z tablicą wyników, rozwijaną
  `explode` poza LATERAL (100 tys. wierszy wyniku przy szczycie 314 MB). Demonstracyjne
  `sail_*_udtf.py` mają ten sam wzorzec kwadratowy — działają tylko na małych danych.
- Archiwum zawiera `__MACOSX/` z plikami `._part-*.parquet` — rozszerzenie `.parquet`, ale
  to nie Parquet; wczytanie ich wzorcem `**/*.parquet` wywróciłoby czytanie.
- Chromosom obecny tylko w lewej tabeli (np. kontigi `SIRV*` w `ex-anno`): polars-bio daje
  w `nearest` wiersz bez sąsiada, w `coverage` pokrycie 0, w `subtract` przedział bez zmian.
  Demonstracyjny UDTF coverage w Sailu (`sail_coverage_subtract_udtf.py`) pomija taki
  chromosom — w `sail_bio.py` polars-bio dostaje pustą prawą stronę.
- DataFusion 53 czyta tekst z Parqueta jako `Utf8View`, a pozycje databio-8p mają typ `int32`
  — obie ścieżki (dostawca i nasze węzły) to obsługują; zbiór testowy ma te same typy.

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
jedyny sensowny tryb w tym kontekście).

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

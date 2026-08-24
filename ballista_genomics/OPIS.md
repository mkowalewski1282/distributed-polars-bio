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

**Koszt tego rozwiązania:** ta w pełni rozproszona ścieżka wykonania (`dist_overlap` przez
prawdziwy klaster Ballista) NIE używa COITrees — dostaje standardowy `HashJoinExec` +
filtr (serializowalny domyślnym kodekiem Ballisty). Lokalna ścieżka (Faza A.2, `overlap()`
bez dystrybucji) nadal używa COITrees bez zmian. To udokumentowane ograniczenie **tylko**
tej jednej, w pełni rozproszonej ścieżki — nie unieważnia wyniku Fazy A.2.

### Otwarte (opcjonalne, nierozpoczęte): PhysicalExtensionCodec dla IntervalJoinExec

Żeby odzyskać COITrees w trybie rozproszonym, trzeba by napisać `PhysicalExtensionCodec`
dla `IntervalJoinExec`. Ponieważ jego pola są prywatne, wzorzec "własny wrapper delegujący
do prawdziwego typu" (jak przy `DistOverlapProvider`) nie zadziała bezpośrednio dla
ExecutionPlan (nie da się "podmienić" węzła w środku już zbudowanego drzewa równie łatwo
jak przy TableProvider). Realna droga: albo zgłosić upstream (biodatageeks) prośbę o
publiczne pola/konstruktor dla `IntervalJoinExec`, albo zaimplementować analogiczny,
własny fizyczny operator interval-join od zera (spory nakład pracy, osobny temat).

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

## Wnioski dla pracy magisterskiej (zaktualizowane po Fazie A.4)

| | DataFusion (lokalnie) | Ballista distributed | Sail + UDTF |
|---|---|---|---|
| Rejestracja funkcji | `create_bio_session()` (crate bio-function-ranges) | własny wrapper (`DistOverlapProvider`) + `dist_overlap()` | Python `@udtf` (PR #1519) |
| Serializacja planu | nie potrzebna | `LogicalExtensionCodec` (~50 linii, bez protobuf) | nie dotyczy (UDTF to Python call, nie węzeł planu DataFusion) |
| Używa COITrees? | ✅ tak | ⚠️ nie w pełni rozproszonej ścieżce (patrz Faza A.4, IntervalJoinExec) | ✅ tak (woła prawdziwe pb.overlap() per grupa) |
| Status w tej pracy | ✅ zweryfikowane, identyczne z baseline | ✅ **zweryfikowane end-to-end na prawdziwym klastrze** (scheduler+executor, realny shuffle), identyczne z baseline | ✅ zweryfikowane (ze znanym, udokumentowanym bugiem partycjonowania w LATERAL+UDTF — patrz `sail_overlap_udtf.py`) |

**Zaktualizowany kluczowy wniosek:** oba silniki DAJĄ SIĘ rozszerzyć bez forkowania —
to jest osiągnięty, zweryfikowany wynik tej pracy dla operacji `overlap`. Różnią się
jednak głębokością/dojrzałością tej integracji:
- **Ballista**: mechanizm jest kompletny (UDF/TableProvider + LogicalExtensionCodec +
  PhysicalExtensionCodec), ale wymaga własnego kodu Rust do serializacji — i ma
  praktyczne ograniczenie: customowe fizyczne węzły wykonania (jak `IntervalJoinExec`,
  algorithm=Coitrees) z zewnętrznych bibliotek, jeśli mają prywatne pola, są trudne do
  poprawnego zserializowania bez zmian po stronie tej biblioteki — w tej pracy
  rozwiązane kompromisem (rozproszona ścieżka bez COITrees, lokalna z COITrees).
- **Sail**: mechanizm jest prostszy (Python, brak potrzeby serializacji planu), ale
  ograniczony funkcjonalnie (tylko argumenty skalarne, nie TABLE) i ma realne bugi
  wykonania (LATERAL + wiele partycji).

Żaden z silników nie ma dziś (sierpień 2026) w pełni dojrzałego mechanizmu integracji
NA POZIOMIE PLANU zapytania, który obsłużyłby customowe fizyczne operatory z
zewnętrznych bibliotek "za darmo" — dla Saila taki mechanizm (`SailExtension`/FFI)
jest w aktywnej fazie projektowej (patrz `architektura_draft.md`), dla Ballisty
istnieje (`PhysicalExtensionCodec`), ale wymaga współpracy z autorami biblioteki
rozszerzającej (publiczne pola/konstruktory) dla pełnej głębi integracji.

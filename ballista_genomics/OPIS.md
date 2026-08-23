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

## Faza A.3 (W TRAKCIE): prawdziwa Ballista standalone

`src/main.rs` w obecnej wersji próbuje uruchomić prawdziwy klaster Ballista
(`SessionContext::standalone_with_state()`, feature `standalone` w `Cargo.toml`)
zamiast gołego DataFusion jak poprzednio — z tym samym operatorem `overlap()`.

### Dlaczego to jest trudniejsze niż Faza A.2: LogicalExtensionCodec

Ballista to klaster — scheduler i executory to osobne procesy/komponenty
komunikujące się przez sieć (protobuf). Standardowe operacje DataFusion mają
wbudowany kodek. Niestandardowy operator `overlap()` (z bio-function-ranges) —
nie, potrzebuje własnego `LogicalExtensionCodec`/`PhysicalExtensionCodec`.

Poprzedni prototyp (naiwny UDF) napotykał błąd nawet na placeholderze:
```
LogicalExtensionCodec is not provided for scalar function genomic_overlap
```
Obecny cel: udokumentować dokładnie ten sam problem na PRAWDZIWEJ funkcji, a
następnie zaimplementować kodek.

### Jak podejść do implementacji kodeka

`ballista::extension::SessionContextExt::standalone()`/`standalone_with_state()`
używają wewnętrznie `BallistaCodec::default()` — **na sztywno**, bez możliwości
wstrzyknięcia własnego kodeka przez tę wygodną funkcję. Żeby użyć customowego
`LogicalExtensionCodec`/`PhysicalExtensionCodec`, trzeba pominąć
`SessionContextExt::standalone()` i zamiast tego wywołać bezpośrednio niższe
funkcje z `ballista_scheduler::standalone` / `ballista_executor`
(`new_standalone_executor(scheduler, concurrent_tasks, custom_codec)`), które
JUŻ przyjmują `BallistaCodec` jako parametr — dokładnie ten sam wzorzec co
`ballista::extension::Extension::setup_standalone()` (źródło:
`~/.cargo/registry/.../ballista-53.0.0/src/extension.rs`), tylko z własnym
kodekiem zamiast `BallistaCodec::default()`.

Punkt odniesienia dla implementacji samego kodeka:
[ballista_extensions](https://github.com/milenkovicm/ballista_extensions)
(autor — `milenkovicm` — jest aktywnym committerem Ballisty, potwierdzone w
release notes 53.0.0/54.0.0).

`BallistaLogicalExtensionCodec` (wbudowany kodek Ballisty) sam w sobie wspiera
listę kodeków próbowanych po kolei (`try_any`) — więc realistyczne podejście to
kodek, który najpierw próbuje obsłużyć nasz customowy węzeł planu, a dla
wszystkiego innego deleguje do domyślnego zachowania Ballisty.

---

## Wnioski dla pracy magisterskiej (zaktualizowane)

| | DataFusion (lokalnie) | Ballista distributed | Sail + UDTF |
|---|---|---|---|
| Rejestracja funkcji | `create_bio_session()` (crate bio-function-ranges) | to samo + LogicalExtensionCodec | Python `@udtf` (PR #1519) |
| Serializacja planu | nie potrzebna | wymagana dla operatora `overlap` | nie dotyczy (UDTF to Python call, nie węzeł planu DataFusion) |
| Status w tej pracy | ✅ zweryfikowane, wyniki identyczne z baseline | 🔧 w trakcie (Faza A.3/A.4) | ✅ zweryfikowane (ze znanym, udokumentowanym bugiem partycjonowania w LATERAL+UDTF — patrz `sail_overlap_udtf.py`) |

**Zaktualizowany kluczowy wniosek:** oba silniki dziś realnie oferują tylko
*płytką* integrację bez forka: Ballista przez UDF/operator + kodek serializacji
(dojrzalszy, bardziej pracochłonny mechanizm), Sail przez Python UDTF (prostszy,
ale ograniczony do argumentów skalarnych — brak wsparcia dla argumentów TABLE, i
ze znalezionym bugiem w wykonaniu `LATERAL` nad wieloma partycjami). Żaden z
silników nie ma dziś (sierpień 2026) w pełni dojrzałego, bezforkowego mechanizmu
integracji na poziomie planu zapytania — dla Saila taki mechanizm
(`SailExtension`/FFI) jest w aktywnej fazie projektowej (patrz
`architektura_draft.md`).

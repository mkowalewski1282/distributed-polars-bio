# Lokalne łatki: datafusion-bio-function-ranges

**To NIE jest fork Ballisty ani Saila** (celem projektu jest uniknięcie forkowania
silników rozproszonych) — to poprawki **widoczności** w pomocniczej bibliotece
(`datafusion-bio-function-ranges`, biodatageeks, Apache-2.0), zwendorowanej lokalnie tylko
dlatego, że upstream jeszcze ich nie ma.

Obie łatki mają tę samą naturę: **zero linii logiki, wyłącznie słowa `pub` i re-eksporty**.
Regułę utrzymaniową warto sprawdzać po każdej aktualizacji vendora:

```bash
git diff -- ballista_genomics/vendor/ | grep '^[+-]' | grep -v '^[+-][+-]'
```

Jeśli w wyniku pojawi się cokolwiek poza dodanym `pub`, re-eksportem lub komentarzem,
łatka przestała być „czysta" i wymaga osobnej decyzji.

## Łatka 1: `pub mod intervals` — `ColIntervals` nienazywalny

### Co i dlaczego

`IntervalJoinExec` (fizyczny operator overlap-join z COITrees) ma publiczny konstruktor
`try_new(...)`, ale jeden z wymaganych argumentów — `ColIntervals` — jest zdefiniowany w
module zadeklarowanym jako `mod intervals;` (bez `pub`) wewnątrz `physical_planner/mod.rs`.

W Rust widoczność wymaga, żeby CAŁA ścieżka do typu była publiczna — samo `pub struct
ColIntervals { ... }` nie wystarczy, jeśli moduł go zawierający jest prywatny. Efekt:
`ColIntervals` jest **całkowicie nienazywalny i niekonstruowalny spoza crate'a**, mimo że
sam jest zadeklarowany jako `pub`. To był fundamentalny bloker dla napisania
`PhysicalExtensionCodec` dla `IntervalJoinExec` (Faza A.4 planu pracy magisterskiej,
rozszerzenie na pełną dystrybucję z COITrees w Ballistrze) — bez tego nie da się w ogóle
wywołać `IntervalJoinExec::try_new()` spoza crate'a.

### Zmiana (dokładnie 2 linijki)

`src/physical_planner/mod.rs`:
```diff
-mod intervals;
+pub mod intervals;
```

`src/lib.rs` (wygoda, nieobowiązkowe, ale ułatwia import):
```diff
+pub use physical_planner::intervals::{ColInterval, ColIntervals, parse as parse_intervals};
```

### Status: gotowe do zgłoszenia upstream

Ta zmiana nie modyfikuje żadnej logiki, tylko widoczność — bezpieczna, bezkonfliktowa,
łatwa do zaakceptowania jako osobny, mały PR do
[biodatageeks/datafusion-bio-functions](https://github.com/biodatageeks/datafusion-bio-functions).
Gdyby taki PR wylądował, ten katalog `vendor/` przestałby być potrzebny — wystarczyłoby
wrócić do zwykłej zależności `git`/przyszłej wersji na crates.io w `Cargo.toml`.

## Łatka 2: publiczne węzły fizyczne operacji zakresowych

### Problem

`MergeExec`, `SubtractExec`, `NearestExec` i `CountOverlapsExec` są zadeklarowane **bez
`pub`** i nie mają ŻADNYCH inherent impls — ani konstruktorów, ani getterów. Powstają
wyłącznie jako literały struktury wewnątrz `scan()` odpowiedniego providera.

Spoza crate'a nie da się ich zatem ani **nazwać** (`downcast_ref::<MergeExec>()` w
`try_encode`), ani **odczytać**, ani **skonstruować** (`try_decode`). Bez tego nie da się
napisać `PhysicalExtensionCodec`, a bez niego Ballista nie prześle planu fizycznego do
executora — czyli operacja nie może wykonać się rozproszona.

Uwaga: same **moduły** (`pub mod merge`, `pub mod nearest`, …) są już publiczne, więc
problem jest o poziom niżej niż przy Łatce 1.

### Zmiana (38× `pub` + 5 re-eksportów, 0 linii logiki)

| plik | element |
|---|---|
| `src/merge.rs` | `MergeExec` + 6 pól |
| `src/subtract.rs` | `SubtractExec` + 9 pól |
| `src/nearest.rs` | `NearestExec` + 11 pól, oraz `fn build_nearest_indexes` → `pub fn` |
| `src/count_overlaps.rs` | `CountOverlapsExec` + 6 pól, oraz `enum CountOverlapsIndex` → `pub enum` |
| `src/lib.rs` | re-eksporty powyższych + `build_coitree_from_batches`/`build_count_index_from_batches` |

### Dlaczego `pub` przy polach, a nie konstruktor + gettery

Rust nie ma literału częściowego — żeby zbudować `MergeExec` spoza crate'a, **wszystkie**
pola muszą być publiczne. `pub` przy polach załatwia jednocześnie konstruktor i gettery
przy zerowym dodanym kodzie, zgodnie z zasadą „łatka = czysta widoczność" przyjętą przy
Łatce 1.

Gdyby zgłaszać to upstream, idiomatyczniejszym kształtem byłoby `pub fn try_new(...)`
plus gettery (enkapsuluje wyliczanie `cache`). Kod pracy jest na taką zmianę przygotowany:
cała konstrukcja tych węzłów jest zamknięta w funkcjach `build_*_exec()` w
`src/bio_phys_codec.rs` i `src/coverage_node.rs` — podmiana kształtu API vendora oznacza
edycję tych kilku funkcji, nic więcej.

### Czego łatka NIE robi

Nie zmienia `cache: Arc<PlanProperties>` w nic serializowalnego — i nie musi.
`with_new_children()` przelicza `cache` deterministycznie ze `schema` i liczby partycji
dziecka, więc kodek po zdekodowaniu od razu je wywołuje i dostaje dokładnie to, co
policzyłby vendor (włącznie z właściwym `EmissionType`).

### Czego łatka NIE wystarczyła załatwić

`coverage` wymagał dodatkowo **własnego węzła-nośnika** po naszej stronie
(`DistCoverageExec`, `src/coverage_node.rs`) — nie z powodu widoczności, lecz dlatego, że
`CountOverlapsProvider::scan()` materializuje lewą tabelę, przenosi ją do konstruktora
indeksu i **porzuca**. Powstały `CountOverlapsExec` nie przechowuje ani danych lewej
tabeli, ani nazw jej kolumn, ani flagi `coverage`, a samego indeksu nie da się odczytać
(`COITree` bez serde). Żadna łatka widoczności tego nie naprawi — to kwestia tego, co
węzeł w ogóle pamięta.

Osobno: **`cluster` pozostaje niewykonalny** i nie jest objęty łatką. `ClusterIdCoordinator`
(`src/cluster.rs`) to bariera rendez-vous z `Vec<Waker>` w `Mutex`, działająca wyłącznie
w obrębie jednego procesu. Po rozproszeniu każdy executor dostałby własną kopię, więc plan
albo zawisłby, albo cicho zduplikował ID klastrów. To bloker **semantyczny**, nie
widocznościowy.

### Status: gotowe do zgłoszenia upstream

Tak jak Łatka 1 — zero zmian logiki, więc naturalny kandydat na jeden mały PR
(„udostępnij węzły fizyczne operacji zakresowych, żeby dało się dla nich napisać
`PhysicalExtensionCodec`").

## Dlaczego wendorowane lokalnie, a nie jako fork na GitHubie

Zwendorowanie (kopia w repo, nie osobny fork na koncie użytkownika) było wybrane celowo:
nie wymaga zakładania nowego repozytorium/konta w imieniu użytkownika ani utrzymywania
zdalnego forka — cała zmiana mieści się w tym jednym pliku, więc jest w pełni
transparentna i łatwa do zweryfikowania w diffie commita.

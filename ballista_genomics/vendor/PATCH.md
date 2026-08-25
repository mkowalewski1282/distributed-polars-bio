# Lokalna łatka: datafusion-bio-function-ranges

**To NIE jest fork Ballisty ani Saila** (celem projektu jest uniknięcie forkowania
silników rozproszonych) — to jednoliniowa poprawka widoczności w pomocniczej bibliotece
(`datafusion-bio-function-ranges`, biodatageeks, Apache-2.0), zwendorowana lokalnie tylko
dlatego, że upstream jeszcze jej nie ma.

## Co i dlaczego

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

## Zmiana (dokładnie 2 linijki)

`src/physical_planner/mod.rs`:
```diff
-mod intervals;
+pub mod intervals;
```

`src/lib.rs` (wygoda, nieobowiązkowe, ale ułatwia import):
```diff
+pub use physical_planner::intervals::{ColInterval, ColIntervals, parse as parse_intervals};
```

## Status: gotowe do zgłoszenia upstream

Ta zmiana nie modyfikuje żadnej logiki, tylko widoczność — bezpieczna, bezkonfliktowa,
łatwa do zaakceptowania jako osobny, mały PR do
[biodatageeks/datafusion-bio-functions](https://github.com/biodatageeks/datafusion-bio-functions).
Gdyby taki PR wylądował, ten katalog `vendor/` przestałby być potrzebny — wystarczyłoby
wrócić do zwykłej zależności `git`/przyszłej wersji na crates.io w `Cargo.toml`.

## Dlaczego wendorowane lokalnie, a nie jako fork na GitHubie

Zwendorowanie (kopia w repo, nie osobny fork na koncie użytkownika) było wybrane celowo:
nie wymaga zakładania nowego repozytorium/konta w imieniu użytkownika ani utrzymywania
zdalnego forka — cała zmiana mieści się w tym jednym pliku, więc jest w pełni
transparentna i łatwa do zweryfikowania w diffie commita.

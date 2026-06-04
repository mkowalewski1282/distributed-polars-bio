# Wnioski: mapowanie operacji polars-bio → Sail

## Ogólna ocena wykonalności

polars-bio i Sail mają wspólny fundament — oba opierają się na **DataFusion** (Rust).
polars-bio dodaje genomiczne operacje na wierzch Polars/DataFusion, a Sail implementuje
protokół Spark Connect właśnie przez DataFusion. Oznacza to, że "silnik pod spodem" jest
ten sam, co czyni migrację operacji realną.

## Mapowanie operacji

| Operacja polars-bio | Odpowiednik w Sail/SparkSQL | Trudność |
|---|---|---|
| `overlap(a, b)` | range join: `a.chrom == b.chrom AND a.start < b.end AND b.start < a.end` | średnia — da się wyrazić SQL-em |
| `merge(df)` | window functions + grupowanie | trudniejsza |
| `nearest(a, b)` | cross join + min odległości | trudna, kosztowna |
| `coverage(df)` | window functions lub explode | trudna |
| `subtract(a, b)` | left anti join z obsługą częściowych nakryć | najtrudniejsza |

## Weryfikacja empiryczna: `overlap`

Operacja `overlap` została przetestowana na syntetycznych danych (5 interwałów × 2 zbiory,
chromosomy chr1/chr2). Oba silniki zwróciły identyczne 8 par nakładających się interwałów,
co potwierdza poprawność semantyczną range joina w Sail względem `pb.overlap`.

Czasy na małych danych (polars-bio: ~0.45s, Sail: ~0.36s) nie są miarodajne —
dominuje overhead startowy, a nie czas rzeczywistego przetwarzania.

## Wnioski

- Migracja `overlap` do Sail jest **w pełni wykonalna** i daje identyczne wyniki.
- Sail uruchamia się in-process bez JVM, co upraszcza środowisko testowe.
- Operacje takie jak `merge`, `nearest` czy `subtract` wymagają bardziej złożonych
  zapytań SQL i są potencjalnie trudniejsze do zweryfikowania pod kątem poprawności.
- Rekomendowany kolejny krok: testy na rzeczywistych plikach BED w celu oceny
  wydajności na większych danych.
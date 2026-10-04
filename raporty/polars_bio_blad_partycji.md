# polars-bio: `merge` i `subtract` błędne przy `target_partitions > 1`

- **Data ustalenia:** 1 października 2026 (sonda przy pisaniu planu 3a)
- **Wersja w projekcie:** polars-bio 0.28.0, Python 3.10.12
- **Status upstream:** znany błąd — zgłoszenie
  [#372](https://github.com/biodatageeks/polars-bio/issues/372), „merge/complement/subtract
  return incorrect results when target_partitions > 1 on multi-partition inputs”, zamknięte
  jako naprawione 23.04.2026; dzień później wyszła wersja 0.29.0. Nie zgłaszamy ponownie.
- **Dlaczego mamy wersję z błędem:** od 0.29.0 polars-bio wymaga Pythona ≥ 3.11
  (`requires_python`), a system ma Pythona 3.10, więc pip instaluje 0.28.0. Najnowsza wersja
  w chwili ustalenia to 0.36.0 (21.09.2026; DataFusion 53 — ta sama wersja co w integracji
  z Ballistą). Poprawki nie sprawdzono lokalnie, bo wymagałoby to Pythona ≥ 3.11.

## Objaw

`pb.POLARS_BIO_MAX_THREADS` to `datafusion.execution.target_partitions`. Przy więcej niż jednej
partycji `merge` i `subtract` liczą każdą partycję osobno, więc przedziały chromosomu leżące
w różnych plikach nie są scalane ani odejmowane. Przy 1 partycji (domyślnej w polars-bio)
wynik jest poprawny. `overlap`, `nearest` i `coverage` są poprawne przy każdej liczbie
partycji. Według zgłoszenia upstream ten sam błąd dotyczy też `complement`.

## Minimalny przykład (sprawdzony na 0.28.0)

```python
import os, tempfile, warnings
os.environ.setdefault("MPLBACKEND", "Agg")  # bez tego import polars-bio czeka na serwer X
warnings.filterwarnings("ignore")
import polars as pl
import polars_bio as pb

d = tempfile.mkdtemp()
def write(name, rows):  # jeden przedział na plik -> przedziały chromosomu w RÓŻNYCH plikach
    os.makedirs(f"{d}/{name}")
    for i, (s, e) in enumerate(rows):
        pl.DataFrame({"chrom": ["chr1"], "start": [s], "end": [e]}).write_parquet(f"{d}/{name}/part-{i}.parquet")
    return f"{d}/{name}/*.parquet"

merge_in = write("merge_a", [(100, 200), (150, 300)])
sub_a = write("sub_a", [(100, 200), (300, 400)])
sub_b = write("sub_b", [(300, 350), (150, 180)])
pb.set_option("datafusion.bio.coordinate_system_zero_based", True)
for tp in (1, 2):
    pb.set_option(pb.POLARS_BIO_MAX_THREADS, tp)
    print(tp, sorted(pb.merge(merge_in, output_type="polars.DataFrame").rows()),
          sorted(pb.subtract(sub_a, sub_b, output_type="polars.DataFrame").rows()))
```

Wynik na polars-bio 0.28.0:

| `target_partitions` | `merge` | `subtract` |
|---|---|---|
| oczekiwany | `[('chr1', 100, 300, 2)]` | `[('chr1', 100, 150), ('chr1', 180, 200), ('chr1', 350, 400)]` |
| 1 | poprawny | poprawny |
| 2 | `[('chr1', 100, 200, 1), ('chr1', 150, 300, 1)]` | `[('chr1', 100, 200), ('chr1', 300, 400)]` |
| 4 | jak przy 2 | jak przy 2 |

## Skutki dla pracy

- Wynik wzorcowy narzędzia pomiarowego liczy polars-bio na 1 partycji (plan 3a).
- Punkt odniesienia w P1 (decyzja z 1.10.2026, opcja c): polars-bio na 1 partycji mierzony
  dodatkowo, a przebiegi A/B `merge` i `subtract` przy 2 i 2N partycjach zostają w wynikach
  jako nieważne — dokumentują błąd.
- Na prawdziwych danych (smoke na parze 1-2, 02.10.2026) błąd widać tylko w `subtract`: przy
  2, 4 i 6 partycjach 205 673, 202 854 i 201 506 wierszy zamiast 209 940. `merge` zbioru 1 jest
  poprawny, bo fBrain nie ma nakładających się przedziałów. Ujawnienie błędu zależy więc od
  danych — wykrywa go suma kontrolna narzędzia pomiarowego.
- Migracja do Pythona ≥ 3.11 i aktualnego polars-bio — ustalona 02.10.2026: przed planem 3b
  (uv albo globalnie — do wyboru). Testy oznaczone `xfail(strict=True)`
  (`tests/test_polars_bio_runner.py`) zgłoszą, gdy błąd zniknie.
- W Ballistcie te operacje liczą się poprawnie, bo `DistBioProvider` wymusza repartycję po
  chromosomie. Poprawne rozproszenie `merge` i `subtract` wymaga więc jawnego rozkładu danych
  po chromosomie — to argument do rozdziału o integracji.

## Stan w polars-bio 0.36.0 (plan 3b-1, 02.10.2026)

Po migracji na Pythona 3.12 i polars-bio 0.36.0 (`uv.lock`) błąd nie występuje:

- testy `test_two_partitions[merge]` i `[subtract]` w `tests/test_polars_bio_runner.py`, oznaczone
  wcześniej `xfail(strict=True)`, przeszły (XPASS) — oznaczenia usunięte;
- w teście integracyjnym orkiestratora (`tests/test_orchestrator_integration.py`) wszystkie
  przebiegi są ważne, także polars-bio A i B (2 i 4 partycje).

Wynik wzorcowy nadal liczy polars-bio na 1 partycji (wariant `polars_bio_ref`), ale osobny wariant
„polars-bio na 1 partycji” jako punkt odniesienia w P1 (decyzja z 01.10.2026) przestaje być
potrzebny.

Smoke na danych 1-2 (04.10.2026): `subtract` polars-bio A/B ma 209 940 wierszy przy 2, 4 i 6
partycjach — tyle co wzorzec; w planie 3a było 205 673, 202 854 i 201 506. Żaden przebieg smoke nie
ma błędnego wyniku.

# distributed-polars-bio

[![tests](https://github.com/mkowalewski1282/distributed-polars-bio/actions/workflows/tests.yml/badge.svg?branch=master)](https://github.com/mkowalewski1282/distributed-polars-bio/actions/workflows/tests.yml)

Kod pracy magisterskiej „Porównanie udostępnianych na otwartych licencjach rozproszonych silników
zapytań pod kątem analiz danych genomicznych” (promotor: dr inż. Marek Wiewiórka). Operacje na
przedziałach genomowych z [polars-bio](https://github.com/biodatageeks/polars-bio) (`overlap`,
`nearest`, `coverage`, `merge`, `subtract`) wykonywane w Apache Ballista (`ballista_genomics/`)
i w Sailu (`sail_bio.py`) oraz narzędzie pomiarowe (`bench/`).

## Środowisko

Wymagania: Linux x86_64 (narzędzie używa `taskset` i `/proc`), [uv](https://docs.astral.sh/uv/)
0.12.22, [rustup](https://rustup.rs/) i `protoc` (kompilator Protocol Buffers; potrzebuje go
skrypt budowania zależności `substrait` — lokalnie i w CI wersja 28.3).

```bash
curl -LsSf https://astral.sh/uv/0.12.22/install.sh | sh   # uv w ~/.local/bin
uv sync                                                   # Python 3.12 i biblioteki z uv.lock w .venv/
```

Polecenia uruchamia się przez `uv run …` albo po `source .venv/bin/activate`; w VS Code jako
interpreter trzeba wybrać `.venv/bin/python`. Pakiet `bench` nie działa pod innym Pythonem niż
3.12 ze środowiska projektu — systemowy `python3` ma inne wersje bibliotek.

## Budowanie Ballisty

```bash
cd ballista_genomics
CARGO_BUILD_JOBS=1 cargo build --bins     # wersja Rusta z rust-toolchain.toml
```

`CARGO_BUILD_JOBS=1` chroni maszynę z małą ilością pamięci (WSL, 5 GB) przed jej wyczerpaniem
przy kompilacji; na większej maszynie można je pominąć.

## Testy

```bash
uv run pytest -m "not real_data"          # cały pakiet poza danymi rzeczywistymi (jak w CI)
uv run python -m bench.data.download      # zbiór databio-8p w ~/bench_data (albo $BENCH_DATA_ROOT)
uv run pytest -m real_data                # testy na danych rzeczywistych
```

Testy przypinające procesy do emulowanych węzłów (`@pytest.mark.nodes(n)`) potrzebują CPU
0..2n+1; na maszynie z mniejszą liczbą CPU są pomijane z podaniem powodu.

CI (GitHub Actions, `.github/workflows/tests.yml`) buduje Ballistę i uruchamia pakiet bez danych
rzeczywistych. CI służy wyłącznie sprawdzaniu poprawności, nigdy pomiarom.

## Narzędzie pomiarowe

```bash
uv run python -m bench.orchestrator bench/conf/smoke.yaml --ballista-profile debug
```

Wyniki trafiają do `bench/results/<seria>/<czas>/runs.parquet`. Metodyka:
`docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md`.

## Dokumentacja

- `raporty/sprawozdanie_stanu_prac.md` — stan prac;
- `ballista_genomics/OPIS.md` — integracja z Ballistą;
- `ballista_genomics/vendor/PATCH.md` — łatki w zwendorowanej bibliotece algorytmów;
- `wnioski_claude.md` — obserwacje z implementacji;
- rysunki do raportów: `uv run --with matplotlib python raporty/img/generuj_rysunki.py`.

# Szkic architektury: rozszerzenie polars-bio o obliczenia rozproszone

## Kontekst

Celem jest umożliwienie wykonywania genomicznych operacji interwałowych (overlap, merge,
nearest, coverage, subtract) w środowisku rozproszonym, przy zachowaniu zoptymalizowanych
algorytmów z polars-bio (COITrees, SuperIntervals).

Wybrany silnik: **Apache Ballista** (rozproszony silnik zapytań oparty na DataFusion/Rust).

Odrzucone alternatywy:
- **Sail** — brak mechanizmu rozszerzeń (issue #1062, w fazie dyskusji)
- **Apache Comet** — brak zewnętrznego PhysicalOptimizerRule, wymaga JVM/Scali
- **Apache Gluten** — oparty na Velox (C++), niezgodny ze stosem Rust/DataFusion
- **Daft** — UDFy tylko przez Python, brak customowych reguł optymalizatora bez forka

---

## Stan obecny: polars-bio lokalnie

```
Użytkownik (Python)
    │
    │  pb.overlap(df_a, df_b)
    ▼
polars-bio API
    │
    ▼
DataFusion SessionContext (jedna maszyna)
    │
    │  LocalOverlapRule wykrywa warunek nakładania
    │  → podmienia na COITrees
    ▼
COITrees / SuperIntervals (Rust)
    │
    ▼
Polars DataFrame (wynik w RAM)
```

**Ograniczenie:** dane muszą mieścić się w pamięci RAM jednej maszyny.

---

## Stan docelowy: polars-bio + Ballista

```
Użytkownik (Python)
    │
    │  pb.overlap(df_a, df_b)   ← API bez zmian
    ▼
polars-bio API
    │
    ▼
Ballista Client
    │  serializuje plan zapytania → przesyła do schedulera
    ▼
Ballista Scheduler
    │
    ├── DistributedOverlapRule  [NOWE w polars-bio]
    │       wykrywa wzorzec overlap w planie logicznym
    │       generuje plan rozproszony:
    │         1. shuffle danych według chromosomu (przez Ballistę)
    │         2. na każdej partycji uruchom LocalOverlapRule
    │         3. zbierz i zwróć wyniki
    │
    └── rozdziela zadania do executorów
              │
              ├── Executor 1 (partycja: chr1)
              │       DataFusion + LocalOverlapRule → COITrees
              │
              ├── Executor 2 (partycja: chr2)
              │       DataFusion + LocalOverlapRule → COITrees
              │
              └── Executor N (partycja: chrN)
                      DataFusion + LocalOverlapRule → COITrees
```

---

## Konieczne zmiany w polars-bio

### 1. Nowa reguła optymalizatora dla trybu rozproszonego

Istniejąca reguła optymalizatora w polars-bio zakłada że wszystkie dane są dostępne
lokalnie i od razu podmienia plan na COITrees. Nowa reguła musi działać inaczej:
- wykryć ten sam wzorzec (warunek nakładania interwałów)
- wygenerować plan który **najpierw shuffluje dane według chromosomu** przez mechanizm
  Ballistry, a następnie uruchamia istniejącą lokalną regułę (COITrees) na każdej
  partycji niezależnie

Polars-bio miałoby więc **dwie reguły optymalizatora**: jedną dla trybu lokalnego
(istniejąca), drugą dla trybu rozproszonego (nowa).

### 2. Konfiguracja Ballistry

Kod który przy starcie schedulera i każdego executora:
- dostarcza Ballistce **rejestr funkcji genomicznych** (`override_function_registry`)
  — żeby SQL z `overlap()` był rozpoznawany jako znana funkcja
- dostarcza Ballistce **builder sesji z nową regułą optymalizatora** (`override_session_builder`)
  — żeby zapytania były wykonywane przez COITrees, a nie naiwny join

Konfiguracja musi być identyczna dla schedulera i każdego executora.

### 3. Serializacja planów wykonania

Ballista przesyła plany zapytań przez sieć w formacie protobuf. Niestandardowe
plany wykonania generowane przez polars-bio muszą umieć się serializować
i deserializować — wymaga to implementacji `PhysicalExtensionCodec` oraz
definicji odpowiednich komunikatów protobuf.

Jest to najtrudniejsza część integracji. Przykład dla prostego operatora dostępny
w projekcie [ballista_extensions](https://github.com/milenkovicm/ballista_extensions).

---

## Co pozostaje bez zmian

- **API użytkownika** (`pb.overlap`, `pb.merge` itd.) — bez żadnych zmian
- **Algorytmy COITrees / SuperIntervals** — działają na każdym executorze lokalnie,
  po tym jak dane zostaną podzielone według chromosomu
- **Obsługa formatów plików** (BED, VCF, BAM)

---

## Kolejność implementacji (propozycja)

1. `overlap` — najczęstsza operacja, najlepiej odwzorowuje się na shuffle-by-chromosome
2. `merge` — wymaga dodatkowego kroku łączenia wyników między partycjami
3. `nearest` — najtrudniejsza (dane z sąsiednich chromosomów mogą być potrzebne)
4. `coverage`, `subtract` — zależnie od postępów

---

## Otwarte pytania

1. Czy `DistributedOverlapRule` i `ballista_registry` mają być częścią głównego
   repozytorium polars-bio, czy osobnej biblioteki (np. `polars-bio-ballista`)?
2. Strategia partycjonowania: zawsze po chromosomie, czy dynamicznie na podstawie
   statystyk danych?
3. Zakres pracy: implementacja prototypu dla `overlap` + analiza architektoniczna
   pozostałych operacji, czy pełna implementacja wszystkich pięciu?

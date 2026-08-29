# Szkic architektury: rozszerzenie polars-bio o obliczenia rozproszone

*Zaktualizowano: sierpień 2026, po doprecyzowaniu wymagań i researchu nt. aktualnego
stanu Ballisty/Saila. Poprzednia wersja tego dokumentu zakładała odrzucenie Saila — to założenie
było błędne, patrz niżej.*

## Kontekst

Celem jest umożliwienie wykonywania genomicznych operacji interwałowych (overlap, merge,
nearest, coverage, subtract) w środowisku rozproszonym, przy zachowaniu zoptymalizowanych
algorytmów z polars-bio (COITrees, SuperIntervals — a konkretnie: crate'a
`datafusion-bio-function-ranges`, który polars-bio już dziś wykorzystuje jako silnik lokalny).

Cel projektu: **porównanie DWÓCH silników — Apache Ballista i Sail** — nie wybór
jednego. Oba mają zostać użyte jako **runtime extension, bez forkowania** (rejestracja UDF/UDTF
przez publiczne API silnika, nie łatanie jego źródeł).

## Dlaczego Sail nie jest odrzucony (korekta wcześniejszego założenia)

Wcześniejsza wersja tego dokumentu odrzucała Saila, powołując się na
[issue lakehq/sail#1062](https://github.com/lakehq/sail/issues/1062) (brak mechanizmu FFI dla
rozszerzeń). To niepełny obraz:

- Issue #1062 dotyczy **głębokiej integracji na poziomie planu zapytania** (logical/physical
  plan extensions, optimizer rules) — rzeczywiście niezaimplementowanej. Zamknięty 28.05.2026,
  kontynuacja w [dyskusji #2001](https://github.com/lakehq/sail/discussions/2001) (aktywna min.
  do 28.07.2026) — maintainerzy projektują `SailExtension`/FFI, ale to wciąż faza projektowa.
- Sail ma jednak od dawna (PR #1519, merged) **działający mechanizm Python UDTF**
  (Arrow-native, `spark.udtf.register(...)`), niezależny od powyższej dyskusji. To wystarcza,
  żeby wywołać polars-bio jako funkcję na każdej partycji danych — bez forka, bez czekania na
  FFI.
- Zespół SedonaDB rozwiązuje analogiczny problem (spatial join ≈ interval overlap) forkiem
  Saila (`james-willis/sail`, branch `sedona-integration`) — to potwierdza, że **głębsza
  integracja na poziomie planu wymaga dziś forka**, ale nie dyskwalifikuje UDTF jako
  wystarczającego, legalnego poziomu integracji dla tej pracy.

**Wniosek:** Sail zostaje jako pełnoprawny kandydat, na poziomie UDTF (nie na poziomie
optymalizatora planu — to poza zasięgiem bez forka).

## Dwa różne wzorce integracji — bo Sail i Ballista to różne rodziny silników

Ballista jest rozszerzeniem samego DataFusion (ta sama rodzina co polars-bio) — integracja może
być bezpośrednia, na poziomie planu DataFusion. Sail implementuje protokół Spark Connect na
DataFusion, ale jego jedyny dziś dostępny (bez forka) punkt rozszerzenia to Python UDTF — stąd
integracja idzie przez pętlę wywołań, nie przez współdzielony plan.

### Ballista — integracja bezpośrednia

```
distributed_overlap(df_a, df_b, engine="ballista")     [nasz kod, POZA polars-bio]
    │
    │  buduje plan z operatorem overlap() z crate'a datafusion-bio-function-ranges
    ▼
Klient Ballista
    │  wysyła plan do schedulera (wymaga LogicalExtensionCodec/PhysicalExtensionCodec
    │  dla operatora overlap — patrz sekcja "Problem serializacji planu")
    ▼
Ballista Scheduler
    │  repartycja danych wg chromosomu (RepartitionExec — standardowy, serializuje się "za darmo")
    ▼
    ├── Executor 1 (partycja: chr1) — overlap() z crate'a → COITrees lokalnie
    ├── Executor 2 (partycja: chr2) — overlap() z crate'a → COITrees lokalnie
    └── Executor N (partycja: chrN) — overlap() z crate'a → COITrees lokalnie
    │
    ▼
Wyniki zbierane i zwracane jako DataFrame
```

### Sail — pętla przez UDTF

```
distributed_overlap(df_a, df_b, engine="sail")     [nasz kod, POZA polars-bio]
    │
    │  łączy się z klastrem/serwerem Sail (Spark Connect)
    ▼
Sail
    │  repartycja danych wg chromosomu
    ▼
    ├── Partycja chr1 → UDTF (PR #1519) → WOŁA Z POWROTEM pb.overlap() (niezmieniona funkcja
    │                                       z zainstalowanego pakietu polars-bio)
    ├── Partycja chr2 → UDTF → pb.overlap()
    └── Partycja chrN → UDTF → pb.overlap()
    │
    ▼
Wyniki wracają przez Saila do naszego kodu → do użytkownika
```

Różnica kluczowa: w Ballistrze polars-bio (a właściwie jego silnik, `datafusion-bio-function-ranges`)
staje się **częścią planu DataFusion Ballisty** (widoczną dla optymalizatora Ballisty — przynajmniej
w teorii, po napisaniu kodeka). W Sailu polars-bio jest **czarną skrzynką wywoływaną z Pythona**
wewnątrz partycji — Sail nie "wie", co się dzieje w środku UDTF-a.

## Zakres na start: bez zmian w polars-bio

**Cała powyższa logika (`distributed_overlap`, wybór `engine=`) żyje w repo pracy magisterskiej,
nie w kodzie źródłowym polars-bio.** Wywołujemy wyłącznie publiczne, niezmienione `pb.overlap()`.
Prawdziwa zmiana wewnątrz polars-bio (nowa reguła optymalizatora `DistributedOverlapRule`,
rejestr silników) to opcjonalny, odłożony krok — patrz sekcja "Przyszły krok" niżej — do
uzgodnienia na dalszym etapie, dopiero po potwierdzeniu wykonalności.

## Problem serializacji planu (Ballista)

Ballista przesyła plany zapytań przez sieć w formacie protobuf. Standardowe operacje (joiny,
filtry, `RepartitionExec`) mają wbudowany kodek. Niestandardowy operator `overlap()` z
`datafusion-bio-function-ranges` — nie. Trzeba dostarczyć `LogicalExtensionCodec`/
`PhysicalExtensionCodec`, które nie serializują samego algorytmu (obie strony — scheduler i
executor — już mają go lokalnie, zarejestrowanego identycznie przy starcie przez
`override_function_registry`/`override_session_builder`), tylko informację "które customowe API
zostało wywołane, z jakimi argumentami" (nazwa + parametry — mały ładunek).

Punkt odniesienia: [ballista_extensions](https://github.com/milenkovicm/ballista_extensions)
(autor jest aktywnym committerem Ballisty) — przykład dodania customowego operatora bez forka.

Wcześniejszy prototyp (`ballista_genomics/src/main.rs`) unikał tego problemu, używając gołego
DataFusion zamiast prawdziwej Ballisty i naiwnego predykatu overlap (bez COITrees) zamiast
prawdziwego silnika polars-bio — do naprawienia w Fazie A planu.

## Co pozostaje bez zmian

- Publiczne API polars-bio (`pb.overlap`, `pb.merge` itd.) — niezmodyfikowane.
- Algorytmy COITrees / SuperIntervals — działają lokalnie na każdym executorze/partycji.
- Same pakiety Ballista/Sail — używane jako zależności, nie forkowane (poza opcjonalną,
  osobną ścieżką badawczą inspirowaną forkiem SedonaDB, nieplanowaną na start).

## Kolejność implementacji

1. `overlap` — najczęstsza operacja, najlepiej odwzorowuje się na shuffle-by-chromosome.
2. `merge` — wymaga dodatkowego kroku łączenia wyników między partycjami.
3. `nearest` — trudniejsza (może wymagać danych z sąsiednich partycji).
4. `coverage`, `subtract` — zależnie od postępów.

(Szczegółowa ocena trudności każdej operacji: `wnioski_claude.md`.)

## Przyszły krok (opcjonalny, odłożony): zmiany wewnątrz polars-bio

Docelowo — jeśli czas i wyniki Faz A–D na to pozwolą — integracja mogłaby zostać wbudowana w
samo polars-bio, tak by `pb.overlap(..., engine=...)` było częścią oficjalnego API:

- **Nowa reguła optymalizatora** `DistributedOverlapRule` obok istniejącej `LocalOverlapRule`
  (polars-bio dopuszcza posiadanie kilku reguł optymalizacyjnych).
- **Rejestr silników** (`ballista_registry`/`sail_registry`) — konfiguracja identyczna na
  wszystkich węzłach klastra.
- Serializacja planów jak opisano wyżej.

To jest osobna decyzja do podjęcia na dalszym etapie, nie punkt startowy tej pracy.

## Otwarte pytania

1. Natywne rozproszone obliczenia w samym Polars (Polars Cloud) — trzecia ścieżka warta zbadania?
2. Dostępność zasobów GCP na wydziale (budżet/projekt) — do ustalenia osobno.

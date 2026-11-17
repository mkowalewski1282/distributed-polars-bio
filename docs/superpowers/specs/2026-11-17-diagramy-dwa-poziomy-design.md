# Diagramy README w dwóch poziomach — projekt (krótki)

- **Data:** 17.11.2026
- **Status:** do akceptacji
- **Dotyczy:** etap 4 (`2026-11-15-trzy-repozytoria-i-procedura-1248-design.md`, §6.4); blokuje Task 14

## Cel i powód

Diagramy w README `ballista-genomics` i `sail-genomics` są nieczytelne. Query flow jest za
wysoki, a czcionka za mała, bo podgląd skaluje diagram do szerokości strony. Components
Ballisty (25 pudełek, 30 strzałek) nie da się czytać. Autor repozytoriów musi umieć
opowiedzieć każdy diagram własnymi słowami (prezentacja, praca pisemna). Wymóg „dokładnego”
diagramu (§6.4: każda strzałka i każde pudełko to nazwana funkcja, typ albo komunikat)
zostaje, ale przenosi się na poziom szczegółów.

- **Cele:**
  - każdy diagram czytelny w podglądzie VS Code i na GitHubie bez przybliżania;
  - przegląd zrozumiały bez znajomości kodu;
  - wszystkie nazwy z obecnych diagramów zachowane w szczegółach.
- **Nie-cele:**
  - zmiany kodu;
  - nowe nazwy, które wymagałyby ponownego sprawdzania z kodem i logami;
  - zmiany diagramu procesów i CPU w `genomics-engines-bench`.
- **Anty-cele:**
  - przegląd, którego autor nie umie opowiedzieć;
  - szczegóły tak gęste jak dotychczas, tylko podzielone na dwa pliki.

## Podejścia

- **A.** Przegląd plus obecny diagram zwinięty pod „szczegóły”. Odrzucone: szczegóły
  pozostają nieczytelne.
- **B. (wybrane przez użytkownika)** Przegląd plus szczegóły podzielone na fazy. Każdy
  krok lub grupa kroków przeglądu ma własny mały diagram z nazwami z kodu. Próbka (przegląd
  Query flow Ballisty i kroki 4–6) zaakceptowana 17.11.2026.
- **C.** Przegląd plus tabela „krok → nazwy w kodzie”. Odrzucone: użytkownik chce dwóch
  wersji diagramu.

## Ustalenia

**Układ w README.** Przy każdym diagramie:
1. Przegląd.
2. Lista „In words” (jedno zdanie na krok).
3. Wiersz z odnośnikami do faz.
4. Sekcja `### ... in detail` z diagramami faz.

Oba poziomy są w README. TECHNICAL.md bez zmian.

**Konwencje (jak w próbce):**
- Strzałka to wiadomość między procesami; prostokąt (`Note over`) to praca wewnątrz
  procesu.
- WIELKIE LITERY oznaczają serializację planu oraz przejście danych między procesami albo
  Rust↔Python.
- Kroki są numerowane ręcznie. Przegląd ma numery 1..n, szczegóły ten sam numer z literą
  (4a, 4b…).
- `mirrorActors: false`.
- Podpisy łamane `<br/>` do około 38 znaków w wierszu.
- Przegląd ma nie więcej niż około 8 kroków; diagram fazy nie więcej niż około 10
  elementów.

**ballista-genomics**
- *Query flow:*
  - przegląd w 8 krokach, jak w próbce;
  - fazy: 1–2 (klient i plan logiczny), 3 (planowanie w schedulerze: dekodowanie
    providera, `scan`, `EnsureRequirements`, cięcie na etapy), 4–6 (jak w próbce), 7–8
    (wynik przez Arrow Flight `do_get`);
  - akapit o trybie standalone zostaje tekstem.
- *Components:*
  - przegląd z 4 pudełkami (nasz kod / Ballista 55.0.0 / DataFusion 55.2.0 /
    bio-functions v0.25.0 vendored, patched) i kilkoma podpisanymi strzałkami;
  - szczegóły w dwóch diagramach: (a) budowanie i planowanie zapytania (binaria,
    konfiguracja sesji, `DistTableFunction` → `DistBioProvider` → providery i węzły
    vendored); (b) kodeki i serializacja (nasze kodeki, łańcuch delegacji, `codec_io`,
    datafusion-proto, Arrow IPC, kodeki Ballisty, elementy upublicznione łatkami 1 i 2).

**sail-genomics**
- *Query flow:*
  - przegląd w około 8 krokach: klient buduje zapytanie → plan przez Spark Connect →
    driver tnie na etapy → workery rozdzielają wiersze po chromosomie i zbierają je
    w paczki → RUST→PYTHON: nasza funkcja → polars-bio (kontekst z puli, wewnątrz
    Python→Rust) → RENAME i PYTHON→RUST → wynik do klienta;
  - fazy: klient i plan, etapy i shuffle w workerach, nasza funkcja i polars-bio, wynik;
  - lista „Notes on the diagram” zostaje.
- *Components:* przegląd z 4 pudełkami (nasz kod / PySpark / Sail / polars-bio).
  Obecny diagram (14 pudełek) staje się szczegółem; jeśli po próbie jest nieczytelny,
  dzielę go na dwa.

**Nazwy.** Szczegóły przenoszą nazwy już sprawdzone z kodem i z logami (§6.4); nie dochodzą
nowe. Sprawdzenie mechaniczne: każdy identyfikator kodu ze starych bloków `mermaid`
(nazwy z `::`, CamelCase, snake_case) występuje w nowych.

**Gdzie i jak.**
- Edycja w `tools/split/overlay/<repo>/README.md`.
- Ponowny eksport do `/home/milosz/repos-staging`: strażnik 0 trafień, brak polskich
  znaków, testy `test_split` przechodzą.
- Commit w monorepo (po polsku, bez polskich znaków, bez stopki).

**Weryfikacja wyglądu.**
- Podgląd offline (mermaid 11.4.1) bez błędów składni.
- Użytkownik ogląda w VS Code (markdown-mermaid 1.29.0).
- Ostateczne sprawdzenie w PR na GitHubie (Task 14).

## Zostawione wykonawcy

- Dokładne sformułowania podpisów i zdań „In words”.
- Przesunięcie granic faz, jeśli któraś przekroczy około 10 elementów.
- Kolejność strzałek w przeglądach Components.

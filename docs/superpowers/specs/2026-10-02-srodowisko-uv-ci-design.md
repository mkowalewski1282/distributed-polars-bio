# Etap 3b-1: środowisko uv, aktualne wersje i CI — projekt

- **Data:** 2 października 2026
- **Status:** projekt uzgodniony w rozmowie (brainstorming 02.10.2026); do przeglądu użytkownika
- **Zakres:** odtwarzalne środowisko Pythona (uv), aktualizacja bibliotek do najnowszych wersji
  etapami, wyrównanie wersji algorytmów przedziałowych między polars-bio i Ballistą, konwencja
  językowa kodu, CI na GitHub Actions
- **Powiązane:** `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` (metodyka),
  `docs/superpowers/plans/2026-10-01-narzedzie-pomiarowe-3a.md` (plan 3a),
  `raporty/polars_bio_blad_partycji.md` (błąd #372), `ballista_genomics/vendor/PATCH.md`

## 1. Kontekst i cel

Etap 3b (dokończenie narzędzia pomiarowego i serie pomiarowe) jest podzielony na trzy
podprojekty, każdy z własną specyfikacją, planem i wykonaniem:

1. **3b-1 (ten dokument):** środowisko, aktualne wersje i CI;
2. **3b-2:** narzędzie gotowe do pomiarów — build release, limity gRPC i `broadcast_bytes`, fazy,
   szybsza konwersja wyniku UDTF Saila, projekt przebiegu kontrolnego, reguła `pswpin`,
   `checksum_s`, liczba workerów Saila na przebieg, odłożone drobne uwagi z przeglądu planu 3a;
3. **3b-3:** kalibracja, konfiguracje `kalibracja/p1/p2/p3`, parametr `algorithm`.

Stan wyjściowy (po planie 3a):

- system ma Pythona 3.10.12, dlatego projekt używa polars-bio 0.28.0 — ostatniej wersji dla 3.10.
  Ta wersja ma błąd #372 (`merge`/`subtract` przy `target_partitions > 1`), naprawiony w 0.29.0;
- repozytorium **nie ma żadnej listy zależności** — biblioteki są zainstalowane w `~/.local`,
  więc środowiska nie da się odtworzyć na innej maszynie;
- polars-bio 0.36 używa algorytmów przedziałowych z `datafusion-bio-functions` **v0.22.2**,
  a integracja z Ballistą — zwendorowanej kopii **0.18.0** z dwiema łatkami widoczności.
  Po aktualizacji samego polars-bio oba silniki liczyłyby różnymi wersjami tych samych
  algorytmów i porównanie mieszałoby efekt silnika z efektem wersji biblioteki;
- testy uruchamia się tylko lokalnie.

**Cel:** środowisko odtwarzalne jednym poleceniem, z najnowszymi wersjami bibliotek, tą samą wersją
algorytmów w polars-bio i Ballistcie, kodem w spójnej konwencji językowej i testami uruchamianymi
automatycznie w CI — zanim ruszy jakakolwiek seria pomiarowa.

## 2. Decyzje (brainstorming 02.10.2026)

| Kwestia | Decyzja |
|---|---|
| Środowisko | **uv**: projektowe `.venv`, `pyproject.toml` + `uv.lock`, Python 3.12 pobierany przez uv; bez `sudo` |
| Wersje | **wszystko do najnowszych**: polars-bio 0.36, pysail 0.7.2, pyspark 4.2, numpy 2, pandas 3, pytest 9 itd. |
| Sposób migracji | **etapami, jedna zmiana naraz** — po każdym etapie zielony pakiet testów i osobny commit |
| Biblioteka algorytmów w Ballistcie | **wyrównanie do v0.22.2** (ta sama co w polars-bio 0.36) już w 3b-1 |
| #372 | jeśli zniknie: oznaczenia `xfail` usunięte, a osobny wariant „polars-bio na 1 partycji” w P1 (decyzja (c) z 01.10.2026) **przestaje być potrzebny**; wynik wzorcowy nadal liczony na 1 partycji |
| CI | **pełne**: budowanie Ballisty (Rust) i cały pakiet testów poza danymi rzeczywistymi |
| Konwencja językowa | **wszystko poza komentarzami po angielsku** (nazwy, znaczniki pytest, komunikaty, wartości w wynikach); komentarze, docstringi i dokumentacja `*.md` po polsku |
| README | nowe, krótkie: odtworzenie środowiska, budowanie, testy, odznaka CI |

## 3. Środowisko (uv)

- **Pliki w repozytorium:**
  - `pyproject.toml` — projekt „wirtualny” (`[tool.uv] package = false`): kod działa z katalogu
    repozytorium jak dotąd, bez instalowania pakietu;
  - `requires-python = ">=3.12,<3.13"` i `.python-version` = `3.12`;
  - `uv.lock` — dokładne wersje, w repozytorium;
  - `.venv/` w `.gitignore`.
- **Zależności:**
  - główne: polars-bio, polars, pyarrow, pysail, pyspark z dodatkiem do Spark Connect, pandas,
    numpy, PyYAML, requests;
  - grupa deweloperska: pytest.
- **Przypięcie silników:** silniki będące przedmiotem pomiarów mają w `pyproject.toml` wersję
  przypiętą do poziomu minor (`polars-bio==0.36.*`, `pysail==0.7.*`, `pyspark==4.2.*`), żeby
  przypadkowe `uv lock --upgrade` nie zmieniło ich w trakcie pomiarów. Pozostałe zależności mają
  tylko dolne granice; dokładne wersje są w `uv.lock`.
- **Instalacja uv:** oficjalny instalator do `~/.local/bin`, bez `sudo`. uv sam pobiera Pythona 3.12.
  Systemowy `python3` (3.10) i biblioteki w `~/.local` zostają nietknięte (zapas); projekt przestaje
  z nich korzystać.
- **Uruchamianie:** `uv run pytest`, `uv run python -m bench.orchestrator …` albo po
  `source .venv/bin/activate`. Orkiestrator uruchamia runnery przez `sys.executable`, więc
  automatycznie używa Pythona z `.venv`. VS Code: interpreter `.venv/bin/python`.
- **Rust:** `ballista_genomics/rust-toolchain.toml` przypina Rusta 1.95.0 (dzisiejsza wersja
  lokalna) — ta sama wersja lokalnie i w CI. Lokalnie nadal `CARGO_BUILD_JOBS=1`.

## 4. Etapy aktualizacji

Każdy etap kończy się pełnym pakietem testów (bez testów na danych rzeczywistych) i osobnym
commitem; zmiana wersji jest widoczna w diffie `uv.lock`.

1. **Mechanika uv.** Python 3.12 z dzisiejszymi wersjami bibliotek (polars-bio 0.28.0 ma pakiet
   `abi3`, działa na 3.12; pysail 0.5.3, pyspark 4.1.1, pandas 2.3, polars 1.39). Gdzie dzisiejsza
   wersja nie ma pakietu dla 3.12 albo na nim nie działa (np. numpy 1.24 → pierwsza 1.x z pakietem
   dla 3.12, czyli 1.26), bierzemy najstarszą działającą i zapisujemy to w komunikacie commitu.
   Wynik testów ma być taki sam jak na Pythonie 3.10: 343 passed, 4 xfailed.
2. **polars-bio 0.36** z wymaganymi przez niego wersjami polars, pyarrow (23–24) i datafusion.
   - Sprawdzenie zmian API: nazwy kolumn wyniku, opcje `datafusion.bio.*`,
     `POLARS_BIO_MAX_THREADS`, `output_type="datafusion.DataFrame"` i `execute_stream`, wyrocznie
     w testach (`tests/*_oracle.py`, `bench/ops.py`).
   - #372: testy `xfail(strict=True)` w `tests/test_polars_bio_runner.py` i zbiór
     `KNOWN_POLARS_BIO_BUG` w `tests/test_orchestrator_integration.py` pokażą, czy błąd zniknął.
     Jeśli tak — oznaczenia usunięte, zbiór usunięty (test integracyjny sprawdza wtedy, że wszystkie
     przebiegi są ważne), notatka `raporty/polars_bio_blad_partycji.md` uzupełniona o potwierdzenie, a w specyfikacji metodyki
     opis wyniku wzorcowego i decyzji (c) zaktualizowany (sekcja 2). Jeśli nie — wszystko zostaje
     jak w planie 3a, a obserwacja trafia do notatki.
3. **Biblioteka algorytmów w Ballistcie: 0.18.0 → v0.22.2.**
   - Źródło crate'a `datafusion-bio-function-ranges` z tagu `v0.22.2` repozytorium
     `biodatageeks/datafusion-bio-functions` zastępuje zawartość
     `ballista_genomics/vendor/datafusion-bio-function-ranges/`.
   - Łatki z `vendor/PATCH.md` nakładane ponownie tylko wtedy, gdy upstream wciąż nie udostępnia
     potrzebnych elementów; dalej wyłącznie `pub` i re-eksporty (reguła z `PATCH.md` sprawdzona po
     aktualizacji). `PATCH.md` opisuje stan dla v0.22.2.
   - Kod integracji (`ballista_genomics/src`) dostosowany do zmian API; przebudowa
     (`CARGO_BUILD_JOBS=1`) i pełny pakiet testów — testy porównują wyniki Ballisty z polars-bio,
     więc zgodność wersji algorytmów jest sprawdzana od razu.
4. **Sail 0.7.2 i PySpark 4.2.**
   - Sprawdzenie: zmienne `SAIL_*` (tryb `local-cluster`, liczba workerów, sloty, wygaszanie),
     rejestracja UDTF, strumień wyniku (`to_table_as_iterator`), test pamięci przy LATERAL.
   - Powtórzenie sond z planu 3a: pula workerów tworzona na sesję, dodatkowy worker przy N = 1,
     zawieszanie czwartej sesji otwartej w jednym procesie klienta.
   - W `wnioski_claude.md` — które ustalenia o Sailu obowiązują w 0.7.2, a które zniknęły.
5. **Pozostałe biblioteki:** numpy 2, pandas 3, pytest 9 i reszta (`uv lock --upgrade` dla
   pozostałych pakietów). Najbardziej narażone: suma kontrolna (`bench/checksum.py`, reguły
   promocji typów w numpy 2) i ramki pandas w UDTF-ach Saila (`sail_bio.py`, domyślny typ napisów
   w pandas 3).

**Zasada zatrzymania:** jeśli któryś etap odsłoni poważny problem (np. Sail 0.7 psuje UDTF albo
polars-bio 0.36 zmienia semantykę wyniku), wykonawca zatrzymuje się i pyta użytkownika, czy zostać
przy starszej wersji tej biblioteki, czy naprawiać.

## 5. Etap 6: konwencja językowa

- **Po angielsku:** identyfikatory, znaczniki pytest (`dane` → `real_data`), komunikaty błędów
  i pomoc CLI (Python i Rust: `bench/`, `tests/`, `sail_*.py`, `ballista_genomics/src/`), postęp
  wypisywany przez orkiestrator, wartości zapisywane w wynikach (np. `invalid_reason`:
  `"series interrupted"`, `"skipped: timeout"`), nazwy plików i katalogów tworzonych w testach.
- **Po polsku zostają:** komentarze, docstringi, dokumentacja `*.md`, komunikaty commitów (bez
  znaków diakrytycznych, jak dotąd).
- **Poza zakresem:** kod zwendorowany (`ballista_genomics/vendor/`) — należy do upstreamu.
- **Strażnik:** test (`tests/test_code_language.py`) przegląda pliki `.py` śledzone przez git (bez
  `ballista_genomics/vendor/`) i `.rs` w `ballista_genomics/src`, pomija komentarze i docstringi
  i nie przepuszcza polskich znaków diakrytycznych w pozostałym kodzie. Polskich słów bez diakrytyków nie wykryje, ale chroni
  przed nawrotem; działa także w CI.
- Testy porównujące komunikaty i przyczyny nieważności zmieniają się razem z kodem.
- Etap wykonywany po aktualizacjach (etapy 1–5), żeby ich diffy nie mieszały się z tłumaczeniem.

## 6. Etap 7: CI (GitHub Actions)

Repozytorium `mkowalewski1282/distributed-polars-bio` jest publiczne, więc standardowe runnery są
darmowe i bez limitu minut.

- **Plik:** `.github/workflows/tests.yml`.
- **Wyzwalacze:** push i pull request do `master` oraz ręcznie (`workflow_dispatch`); nowe
  uruchomienie na tej samej gałęzi przerywa poprzednie (`concurrency`).
- **Runner:** `ubuntu-24.04` (przypięty zamiast `ubuntu-latest`; 4 vCPU, 16 GB RAM, ~14 GB dysku);
  limit zadania 60 min.
- **Kroki:**
  1. checkout;
  2. Rust według `rust-toolchain.toml` (1.95.0);
  3. cache budowania Rusta (`Swatinem/rust-cache`, katalog `ballista_genomics`);
  4. `cargo build --bins` bez informacji do debugowania (`CARGO_PROFILE_DEV_DEBUG=0`) — lokalnie
     `target/debug` zajmuje 22 GB, głównie przez debuginfo; na zachowanie binarek nie wpływa;
  5. uv (`astral-sh/setup-uv` z cache), `uv sync --locked` — niezgodność `uv.lock`
     z `pyproject.toml` zatrzymuje CI;
  6. `uv run pytest -m "not real_data"`.
- **Testy zależne od liczby rdzeni:** znacznik `@pytest.mark.nodes(n)` — test, który przypina
  procesy do węzłów 1..n, jest pomijany z podanym powodem, gdy któregoś z CPU 0..2n+1 (rdzeń
  systemowy i węzły 1..n) nie ma w zbiorze CPU dostępnych dla procesu (`os.sched_getaffinity`);
  N = 2 wymaga CPU 0–5, N = 3 — CPU 0–7. Dotyczy testów klastra Ballisty i serwera Saila
  dla N = 2 oraz testu integracyjnego orkiestratora (N = 1 i 2). Lokalnie (12 wątków) uruchamia się
  wszystko.
- **Dane rzeczywiste:** testy `real_data` w CI są pomijane (brak danych); uruchamia je użytkownik
  lokalnie, po checkliście RAM.
- **Odznaka** stanu CI w `README.md`.
- CI służy wyłącznie sprawdzaniu poprawności — nigdy pomiarom (współdzielone maszyny wirtualne).

## 7. Weryfikacja końcowa i dokumentacja

- **Weryfikacja na danych rzeczywistych:** po checkliście RAM i potwierdzeniu użytkownika — testy
  `real_data` i smoke (`python -m bench.orchestrator bench/conf/smoke.yaml --ballista-profile
  debug`) na parze 1-2. Oczekiwane: przebiegi ważne z wyjątkiem dryfu przebiegu kontrolnego
  (sprawa planu 3b-2) oraz — jeśli #372 nie zniknął — `subtract` polars-bio A/B.
- **Dokumentacja:**
  - `README.md` (nowe);
  - specyfikacja metodyki: wersje (Python 3.12, polars-bio 0.36, Sail 0.7.2, PySpark 4.2,
    Rust 1.95.0, `datafusion-bio-functions` v0.22.2) i stan #372;
  - `raporty/polars_bio_blad_partycji.md`;
  - `ballista_genomics/OPIS.md` i `ballista_genomics/vendor/PATCH.md`;
  - `wnioski_claude.md` — sekcja o aktualizacji (co zmieniły nowe wersje);
  - pamięć projektu.

## 8. Kryteria ukończenia

1. `uv sync --locked` odtwarza środowisko na czystej maszynie (sprawdza to CI).
2. Pełny pakiet testów jest zielony lokalnie, łącznie z `real_data`; smoke daje oczekiwane wyniki.
3. CI na GitHubie jest zielone.
4. Ballista i polars-bio używają tej samej wersji `datafusion-bio-functions` (v0.22.2).
5. Stan #372 w polars-bio 0.36 jest ustalony i opisany.
6. Strażnik konwencji językowej przechodzi.

## 9. Ryzyka

| Ryzyko | Skutek | Środek zaradczy |
|---|---|---|
| Sail 0.7 zmienia API UDTF, zmienne `SAIL_*` albo zachowanie `local-cluster` | etap 4 się wydłuża | testy z planu 3a wskażą miejsce; zasada zatrzymania (sekcja 4) |
| polars-bio 0.36 zmienia nazwy kolumn lub opcje | etap 2 się wydłuża | wyrocznie i testy runnerów wskażą miejsce |
| zmiany API `datafusion-bio-function-ranges` 0.18 → 0.22.2 | etap 3 wymaga zmian w kodzie Rust integracji | wykonywany osobno; testy porównujące z polars-bio |
| pandas 3 (typ napisów, copy-on-write), numpy 2 (promocja typów) | błędy w UDTF-ach Saila albo sumie kontrolnej | etap 5 osobno; testy sumy kontrolnej na wartościach wzorcowych |
| 14 GB dysku runnera CI | budowanie przerwane | build bez debuginfo; w razie potrzeby zwolnienie miejsca przed budowaniem |
| testy uruchamiające klastry na obciążonych runnerach | niestabilne CI | ponawianie startu Ballisty (plan 3a); powtarzalne niepowodzenia badane jak błędy |

## 10. Poza zakresem

Wszystko z 3b-2 i 3b-3 (sekcja 1), obraz Dockera (powstanie z tego samego `uv.lock`, gdy będzie
potrzebny w chmurze albo na Kubernetesie) oraz tłumaczenie komentarzy na angielski.

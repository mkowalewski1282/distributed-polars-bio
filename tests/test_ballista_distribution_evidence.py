"""
Faza H: asercje na DOWODZIE dystrybucji, nie tylko na poprawności wyniku.

Motywacja: zapytanie może dać w 100% poprawny wynik i jednocześnie policzyć się
w jednym query stage'u, bez żadnej równoległości — objawu brak, a teza pracy
o "wykonaniu rozproszonym" byłaby wtedy pusta. Dokładnie to wykryto przy
overlapie w Fazie H (patrz `test_overlap_crosses_serialization_boundary` niżej).

Ballista implementuje `EXPLAIN ANALYZE` tak, że zwraca sekcje
`=========SuccessfulStage[stage_id=N, partitions=M]=========` z drzewem
operatorów i metrykami per stage. Binarka `dist_ops` zrzuca to do
`output/dist_<op>_explain.txt`, a ten plik parsujemy tutaj.

Ten moduł CELOWO nie importuje polars_bio — dzięki temu działa w kilka sekund
(import polars_bio trwa ~4.5 min) i daje szybką pętlę sprzężenia zwrotnego przy
pracy nad kodekami. Pełne testy poprawności: test_ballista_distributed_ops.py

Uruchomienie: pytest tests/test_ballista_distribution_evidence.py -v
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
OUTPUT_DIR = BALLISTA_DIR / "output"
OPIS = BALLISTA_DIR / "OPIS.md"
CLIENT_BINARY = BALLISTA_DIR / "target" / "debug" / "bench_client"

STAGE_RE = re.compile(r"SuccessfulStage\[stage_id=(\d+), partitions=(\d+)\]")
#: Czytnik shuffle po kluczu w Ballistcie 55: `ShuffleReaderExec: upstream_stage: N,
#: partitioning: Hash([chrom@0], 4)` (w 53 bez `upstream_stage`).
HASH_READER_RE = re.compile(r"ShuffleReaderExec: upstream_stage: \d+, partitioning: Hash\(\[chrom")


def _run_client(args: list[str], timeout: int) -> subprocess.CompletedProcess:
    """Uruchamia `bench_client` (tryb standalone); brak binarki to błąd, nie pominięcie."""
    if not CLIENT_BINARY.exists():
        pytest.fail(
            f"missing binary {CLIENT_BINARY.name} - build it: "
            "cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins"
        )
    return subprocess.run(
        [str(CLIENT_BINARY), *args],
        cwd=BALLISTA_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _explain(op: str) -> tuple[str, list[tuple[int, int]]]:
    """Zwraca (surowy tekst planu, [(stage_id, liczba_partycji), ...])."""
    path = OUTPUT_DIR / f"dist_{op}_explain.txt"
    if not path.exists():
        pytest.skip(
            f"no {path} - run first: "
            f"cd ballista_genomics && ./target/debug/dist_ops {op}"
        )
    txt = path.read_text()
    stages = [(int(a), int(b)) for a, b in STAGE_RE.findall(txt)]
    return txt, stages


# --------------------------------------------------------------------------
# merge — wzorzec hash-shuffle (najmocniejszy dowód dystrybucji)
# --------------------------------------------------------------------------


def test_merge_plan_is_split_into_stages():
    """Plan musi zostać pocięty na etapy — inaczej nie było żadnego shuffle."""
    txt, stages = _explain("merge")
    assert len(stages) >= 2, f"merge ran in a single stage:\n{txt}"


def test_merge_has_hash_shuffle_on_contig():
    """
    Shuffle musi być HASH-owy PO KOLUMNIE KONTIGU, nie round-robin.
    To warunek konieczny poprawności: wiersze tego samego chromosomu muszą
    trafić do tej samej partycji, żeby MergeExec mógł je scalić.
    """
    txt, _ = _explain("merge")
    assert re.search(r"ShuffleWriterExec: partitioning=Hash\(\[chrom", txt), txt


def test_merge_operator_reads_from_network():
    """
    MergeExec musi mieć ShuffleReaderExec jako źródło — czyli konsumować dane
    Z SIECI (z innego stage'a), a nie z lokalnego skanu pliku.
    """
    txt, _ = _explain("merge")
    assert "MergeExec" in txt, txt
    merge_pos = txt.index("MergeExec")
    after = txt[merge_pos:]
    assert "ShuffleReaderExec" in after, (
        f"MergeExec does not read from ShuffleReaderExec - no real shuffle:\n{txt}"
    )
    assert HASH_READER_RE.search(after), after


def test_merge_source_stage_is_parallel():
    """
    Stage źródłowy musi mieć >1 partycji, a skan musi widzieć oba pliki
    wejściowe jako osobne grupy. Bez tego test poprawności (nakładające się
    gene_A1/gene_A2 w różnych plikach) nie testowałby dystrybucji.
    """
    txt, stages = _explain("merge")
    assert stages[0][1] >= 2, f"source stage is not parallel:\n{txt}"
    assert re.search(r"file_groups=\{2 groups:", txt), (
        f"both parts_a/ files should be read as separate groups:\n{txt}"
    )


def test_merge_compute_stage_is_parallel():
    """Stage liczący MergeExec musi mieć >1 partycji."""
    txt, stages = _explain("merge")
    assert max(p for _, p in stages) > 1, f"all stages have a single partition:\n{txt}"


# --------------------------------------------------------------------------
# subtract — wzorzec DWUSTRONNEGO hash-shuffle (węzeł binarny)
# --------------------------------------------------------------------------


def test_subtract_has_two_shuffled_inputs():
    """
    SubtractExec jest węzłem binarnym: `execute(partition)` sięga po tę samą
    partycję z OBU stron. Obie muszą więc zostać ko-partycjonowane po `chrom`
    — w planie widać to jako dwa osobne stage'e źródłowe z hash-shuffle.
    """
    txt, stages = _explain("subtract")
    assert len(stages) >= 3, f"subtract needs >=3 stages:\n{txt}"
    assert txt.count("partitioning=Hash([chrom") >= 2, (
        f"both subtract sides should be hash-partitioned by chrom:\n{txt}"
    )


def test_subtract_operator_reads_both_sides_from_network():
    """SubtractExec musi mieć DWA ShuffleReaderExec jako dzieci."""
    txt, _ = _explain("subtract")
    assert "SubtractExec" in txt, txt
    after = txt[txt.index("SubtractExec"):]
    # obcinamy do konca tego stage'a, zeby nie liczyc czytnikow z nastepnych
    stage_end = after.find("=========SuccessfulStage")
    block = after if stage_end == -1 else after[:stage_end]
    readers = len(HASH_READER_RE.findall(block))
    assert readers == 2, (
        f"SubtractExec should read from 2 ShuffleReaderExec (Hash by chrom), "
        f"found {readers}:\n{block}"
    )


def test_subtract_both_source_stages_are_parallel():
    """Oba stage'e źródłowe (parts_a i parts_b) muszą być równoległe."""
    txt, stages = _explain("subtract")
    assert stages[0][1] >= 2 and stages[1][1] >= 2, (
        f"both source stages should have >=2 partitions:\n{txt}"
    )
    assert txt.count("file_groups={2 groups:") >= 2, txt


# --------------------------------------------------------------------------
# nearest — wzorzec BROADCAST (równoległość po prawej stronie, bez shuffle)
# --------------------------------------------------------------------------


def test_nearest_is_broadcast_not_shuffle():
    """
    NearestExec nie nadpisuje `required_input_distribution()`, więc hash-shuffle
    NIE wystąpi i nie powinien. Dowodem dystrybucji jest tu inny wzorzec:
    równoległość po stronie prawej tabeli plus lewa tabela przeniesiona
    w ładunku planu.
    """
    txt, stages = _explain("nearest")
    assert "NearestExec" in txt, txt
    assert not re.search(r"partitioning=Hash\(", txt), (
        f"nearest should not have a hash shuffle (broadcast pattern):\n{txt}"
    )


def test_nearest_compute_stage_is_parallel():
    """NearestExec musi liczyć się na >1 partycji (równoległość z prawej strony)."""
    txt, stages = _explain("nearest")
    assert max(p for _, p in stages) >= 2, (
        f"NearestExec does not run in parallel:\n{txt}"
    )
    assert re.search(r"file_groups=\{2 groups:", txt), (
        f"the right table should be read as 2 separate file groups:\n{txt}"
    )


def test_nearest_left_table_travels_in_plan_not_as_scan():
    """
    Dowód BROADCASTU: lewa (indeksowana) tabela NIE pojawia się w planie jako
    osobny DataSourceExec — jej dane pojechały wewnątrz ładunku planu
    fizycznego (Arrow IPC), a executor odbudował z nich indeks lokalnie.
    Lewą (broadcastowaną) jest B (parts_b), bo wynik ma wiersz na każdy wiersz
    PRAWEJ tabeli, a pb.nearest(A, B) — na każdy przedział A; w planie widać
    więc wyłącznie skan A (parts_a).
    """
    txt, _ = _explain("nearest")
    assert "parts_a" in txt, f"no scan of the right table (A):\n{txt}"
    assert "parts_b" not in txt, (
        "The left table appears in the plan as a scan - so it is NOT "
        f"broadcast in the plan payload:\n{txt}"
    )


# --------------------------------------------------------------------------
# coverage — broadcast + własny węzeł-nośnik
# --------------------------------------------------------------------------


def test_coverage_uses_carrier_node_with_broadcast():
    """
    W planie musi być NASZ węzeł-nośnik z widoczną liczbą broadcastowanych
    wierszy. `broadcast_rows` w DisplayAs jest celowe: to jednocześnie dowód,
    że lewa tabela pojechała w ładunku planu, i materiał do oceny, jak blisko
    jesteśmy limitu 16 MB na wiadomość gRPC.
    """
    txt, _ = _explain("coverage")
    assert re.search(r"DistCoverageExec: coverage=true, broadcast_rows=\d+", txt), txt


def test_coverage_compute_stage_is_parallel():
    """Węzeł coverage musi liczyć się na >1 partycji (równoległość ze źródła)."""
    txt, stages = _explain("coverage")
    assert max(p for _, p in stages) >= 2, txt
    assert re.search(r"file_groups=\{2 groups:", txt), txt


def test_coverage_roundrobin_is_absent():
    """
    ASERCJA DOKUMENTUJĄCA ZNALEZISKO: `CountOverlapsProvider::scan()` w vendorze
    wstawia RepartitionExec(RoundRobinBatch), a statyczny planista Ballisty 53
    usuwał z planu rozproszonego każdą repartycję inną niż hash
    (ballista-scheduler/src/planner.rs). Nasz DistCoverageProvider celowo jej
    więc nie wstawia — dzięki temu plan lokalny i rozproszony mają ten sam
    kształt, a równoległość bierze się z partycjonowania źródła.

    Ballista 55 z AQE takich repartycji nie usuwa; nie powstają, bo sesje mają
    `datafusion.optimizer.enable_round_robin_repartition = false`
    (`cluster::bio_ballista_config`: `upgrade_for_ballista()` przed setterami Ballisty;
    bez tego standalone dostawał RoundRobinBatch(4) przed DistCoverageExec).
    """
    txt, _ = _explain("coverage")
    assert "RoundRobinBatch" not in txt, (
        f"RoundRobinBatch appeared in the distributed plan - check whether "
        f"Ballista changed its behaviour:\n{txt}"
    )


# --------------------------------------------------------------------------
# overlap — asercja DOKUMENTUJĄCA znalezisko (Faza H)
# --------------------------------------------------------------------------


def test_overlap_crosses_serialization_boundary():
    """
    Overlap przechodzi przez granicę serializacji: IntervalJoinExec z algorytmem
    COITrees wykonuje się w etapie po stronie executora, który zapisuje wynik etapu
    (ShuffleWriterExec) — czyli LogicalExtensionCodec i PhysicalExtensionCodec
    faktycznie zadziałały. To jest to, co osiągnęły Fazy A.4 i A.5.

    Ballista 53 dzieliła ten plan w standalone na dwa etapy (osobny etap scalania
    posortowanego wyniku, czytany przez ShuffleReaderExec); Ballista 55 z AQE sortuje
    jedyną partycję w tym samym etapie, więc etap jest jeden. Dane CSV mają po jednej
    partycji, więc lewa strona spełnia już `SinglePartition` trybu CollectLeft i nie
    dostaje osobnego etapu (por. `test_overlap_indexes_the_whole_left_side_in_every_task`).

    Tryb złączenia to CollectLeft, rozstrzygnięty przy planowaniu
    (`dist_provider::collect_left_at_planning`), a nie Auto zamieniane dopiero w koderze.
    """
    txt, stages = _explain("overlap")
    assert len(stages) >= 1, txt
    first_stage = txt[txt.index("SuccessfulStage"):]
    assert "ShuffleWriterExec" in first_stage, txt
    assert "IntervalJoinExec: mode=CollectLeft" in first_stage, txt
    assert "alg=Coitrees" in first_stage, f"COITrees are not used on the executor side:\n{txt}"


def test_overlap_has_no_hash_partitioned_parallelism():
    """
    ASERCJA DOKUMENTUJĄCA ZNALEZISKO (Faza H), nie życzenie.

    Overlap — w odróżnieniu od merge — NIE uzyskuje równoległości
    hash-partycjonowanej. IntervalJoinExec powstaje w vendorze z
    `mode = PartitionMode::Auto` (`with_config_rt_bio()` usuwa regułę `join_selection`,
    która normalnie rozstrzygnęłaby Auto -> Partitioned/CollectLeft); my rozstrzygamy go
    przy planowaniu na CollectLeft (`dist_provider::collect_left_at_planning`, Ballista 55).
    `required_input_distribution()` zwraca wtedy `SinglePartition` dla lewej strony
    i `UnspecifiedDistribution` dla prawej — hash-repartycja nigdy nie jest zażądana.
    Równoległość overlapu bierze się z partycji prawej strony, a każde zadanie indeksuje
    całą lewą (na CSV obie strony mają po jednej partycji, stąd jedna partycja w etapie).

    Gdy ten test zacznie padać, to znaczy że ograniczenie zostało zdjęte
    (np. własną regułą optymalizatora wymuszającą Partitioned) — wtedy należy
    zaktualizować ten test i opis w OPIS.md, a nie "naprawiać" kod.
    """
    txt, stages = _explain("overlap")
    has_hash = bool(re.search(r"partitioning=Hash\(", txt))
    max_parts = max(p for _, p in stages)
    assert not has_hash and max_parts == 1, (
        "Overlap gained hash-partitioned parallelism - a CHANGE for the better "
        "compared with the documented state. Update this test and OPIS.md.\n"
        f"{txt}"
    )


def _metric(block: str, name: str) -> int:
    """Wartość licznika z metryk planu; zbiór testowy jest mały, więc liczby są całkowite
    (bez skrótów typu `1.75 M`)."""
    found = re.findall(rf"\b{name}=(\d+)(?=[,\]])", block)
    assert len(found) == 1, f"expected exactly one plain integer {name}, found {found}:\n{block}"
    return int(found[0])


def test_overlap_indexes_the_whole_left_side_in_every_task(parquet_dirs, tmp_path):
    """
    Overlap na wejściu z kilku partycji (zbiór testowy Parquet, Ballista 55): każde zadanie
    złączenia indeksuje CAŁĄ lewą stronę.

    Scheduler 55 zawęża liście planu do partycji zadania (`restrict_plan_to_partitions`),
    z wyjątkiem wejść, które muszą być scalone. Tryb CollectLeft, rozstrzygnięty przy
    planowaniu, żąda dla lewej strony `SinglePartition`: lewa strona jest osobnym etapem
    (skan → shuffle), a złączenie czyta ją przez `CoalescePartitionsExec` ←
    `ShuffleReaderExec` w całości. Dowód w metrykach: `build_input_rows` (suma po zadaniach)
    = liczba zadań złączenia × liczba wierszy lewej strony. Przy trybie Auto zamienianym
    dopiero w koderze suma była równa liczbie wierszy lewej strony (każde zadanie miało
    tylko swój wycinek), a overlap gubił pary.
    """
    left, right = parquet_dirs
    target = tmp_path / "overlap_explain.txt"
    r = _run_client(
        ["--op", "overlap", "--left", str(left), "--right", str(right),
         "--explain-analyze", str(target)],
        timeout=180,
    )
    assert r.returncode == 0, r.stderr[-3000:]
    txt = target.read_text()
    stages = [(int(a), int(b)) for a, b in STAGE_RE.findall(txt)]
    assert len(stages) == 2, f"expected the left-side stage and the join stage:\n{txt}"
    (left_stage, _), (_, join_tasks) = stages
    left_block, join_block = txt.split("=========SuccessfulStage")[1:]
    assert "IntervalJoinExec" not in left_block, txt
    assert join_tasks >= 2, f"the join stage is not parallel:\n{txt}"

    join_lines = [line.strip() for line in join_block.splitlines()]
    at = next(i for i, line in enumerate(join_lines) if line.startswith("IntervalJoinExec"))
    assert join_lines[at].startswith("IntervalJoinExec: mode=CollectLeft"), txt
    assert join_lines[at + 1].startswith("CoalescePartitionsExec"), txt
    assert join_lines[at + 2].startswith(f"ShuffleReaderExec: upstream_stage: {left_stage},"), txt

    left_rows = _metric(left_block.splitlines()[1], "output_rows")
    assert left_rows > 0, txt
    assert _metric(join_lines[at], "build_input_rows") == join_tasks * left_rows, txt


# --------------------------------------------------------------------------
# Ustawienia Ballisty 55 — jawne i zgodne z OPIS.md
# --------------------------------------------------------------------------


def _declared(key: str) -> str:
    """Wartość klucza Ballisty zadeklarowana w OPIS.md w postaci `klucz` = `wartość`
    (dokładnie jedna deklaracja)."""
    found = re.findall(rf"`{re.escape(key)}` = `([^`]+)`", OPIS.read_text())
    assert len(found) == 1, f"OPIS.md must declare `{key}` = `...` exactly once, found: {found}"
    return found[0]


def _version_info() -> dict:
    """Ustawienia Ballisty odczytane z konfiguracji klastra (`bench_client --version-info`)."""
    r = _run_client(["--version-info"], timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def test_aqe_setting_is_explicit():
    """
    AQE (planista adaptacyjny) Ballista 55 domyślnie włącza (#2315), a 53 domyślnie
    wyłączała — ustawienie jest więc jawne (`cluster::ADAPTIVE_QUERY_PLANNER`), a decyzja
    opisana w OPIS.md. Jedno źródło: wartość z konfiguracji klastra (`--version-info`)
    musi być tą, którą deklaruje OPIS.md, a plan musi pochodzić z tego planisty:
    AQE numeruje etapy od 0 (`AdaptivePlanner`: `stage_id_generator: 0`), planista
    statyczny od 1 (jak Ballista 53).
    """
    declared = _declared("ballista.planner.adaptive.enabled")
    assert declared in ("true", "false"), declared
    assert _version_info()["adaptive_query_planner"] == (declared == "true")
    _, stages = _explain("merge")
    first_stage = min(stage_id for stage_id, _ in stages)
    assert first_stage == (0 if declared == "true" else 1), stages


def test_scheduling_settings_are_explicit():
    """
    Ustawienia Ballisty 55 przywracające semantykę 53 — wartości z konfiguracji klastra
    muszą być tymi z OPIS.md:
    - `ballista.scheduler.max_partitions_per_task`: 55 domyślnie (0, bez limitu) pakuje
      do jednego zadania tyle partycji, ile executor ma wolnych slotów — przy 2 slotach etap
      dwóch partycji szedł jako jedno zadanie na jeden executor; 1 = partycja na zadanie;
    - `ballista.scheduler.share_file_statistics_cache`: wspólna dla sesji pamięć statystyk
      plików w schedulerze (nowa w 55) — wyłączona, jak w 53 (każda sesja z zimną).
    """
    info = _version_info()
    partitions = _declared("ballista.scheduler.max_partitions_per_task")
    assert partitions == "1", partitions
    assert info["max_partitions_per_task"] == int(partitions)
    shared = _declared("ballista.scheduler.share_file_statistics_cache")
    assert shared == "false", shared
    assert info["share_file_statistics_cache"] is False

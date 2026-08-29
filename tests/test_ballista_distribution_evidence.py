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

import re
from pathlib import Path

import pytest

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
OUTPUT_DIR = BALLISTA_DIR / "output"

STAGE_RE = re.compile(r"SuccessfulStage\[stage_id=(\d+), partitions=(\d+)\]")


def _explain(op: str) -> tuple[str, list[tuple[int, int]]]:
    """Zwraca (surowy tekst planu, [(stage_id, liczba_partycji), ...])."""
    path = OUTPUT_DIR / f"dist_{op}_explain.txt"
    if not path.exists():
        pytest.skip(
            f"brak {path} — uruchom najpierw: "
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
    assert len(stages) >= 2, f"merge policzył się w jednym stage'u:\n{txt}"


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
        f"MergeExec nie czyta z ShuffleReaderExec — brak realnego shuffle:\n{txt}"
    )
    assert re.search(r"ShuffleReaderExec: partitioning: Hash\(\[chrom", after), after


def test_merge_source_stage_is_parallel():
    """
    Stage źródłowy musi mieć >1 partycji, a skan musi widzieć oba pliki
    wejściowe jako osobne grupy. Bez tego test poprawności (nakładające się
    gene_A1/gene_A2 w różnych plikach) nie testowałby dystrybucji.
    """
    txt, stages = _explain("merge")
    assert stages[0][1] >= 2, f"stage źródłowy nie jest równoległy:\n{txt}"
    assert re.search(r"file_groups=\{2 groups:", txt), (
        f"oba pliki parts_a/ powinny być czytane jako osobne grupy:\n{txt}"
    )


def test_merge_compute_stage_is_parallel():
    """Stage liczący MergeExec musi mieć >1 partycji."""
    txt, stages = _explain("merge")
    assert max(p for _, p in stages) > 1, f"wszystkie stage'y jednopartycyjne:\n{txt}"


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
    assert len(stages) >= 3, f"subtract potrzebuje >=3 stage'ów:\n{txt}"
    assert txt.count("partitioning=Hash([chrom") >= 2, (
        f"obie strony subtract powinny być hash-partycjonowane po chrom:\n{txt}"
    )


def test_subtract_operator_reads_both_sides_from_network():
    """SubtractExec musi mieć DWA ShuffleReaderExec jako dzieci."""
    txt, _ = _explain("subtract")
    assert "SubtractExec" in txt, txt
    after = txt[txt.index("SubtractExec"):]
    # obcinamy do konca tego stage'a, zeby nie liczyc czytnikow z nastepnych
    stage_end = after.find("=========SuccessfulStage")
    block = after if stage_end == -1 else after[:stage_end]
    readers = len(re.findall(r"ShuffleReaderExec: partitioning: Hash\(\[chrom", block))
    assert readers == 2, (
        f"SubtractExec powinien czytać z 2 ShuffleReaderExec (Hash po chrom), "
        f"znalazłem {readers}:\n{block}"
    )


def test_subtract_both_source_stages_are_parallel():
    """Oba stage'e źródłowe (parts_a i parts_b) muszą być równoległe."""
    txt, stages = _explain("subtract")
    assert stages[0][1] >= 2 and stages[1][1] >= 2, (
        f"oba stage'e źródłowe powinny mieć >=2 partycji:\n{txt}"
    )
    assert txt.count("file_groups={2 groups:") >= 2, txt


# --------------------------------------------------------------------------
# overlap — asercja DOKUMENTUJĄCA znalezisko (Faza H)
# --------------------------------------------------------------------------


def test_overlap_crosses_serialization_boundary():
    """
    Overlap przechodzi przez granicę serializacji: plan jest pocięty na stage'e,
    a IntervalJoinExec z algorytmem COITrees wykonuje się po stronie executora
    (czyli LogicalExtensionCodec i PhysicalExtensionCodec faktycznie zadziałały).
    To jest to, co osiągnęły Fazy A.4 i A.5.
    """
    txt, stages = _explain("overlap")
    assert len(stages) >= 2, txt
    assert "IntervalJoinExec" in txt, txt
    assert "alg=Coitrees" in txt, f"COITrees nie są używane po stronie executora:\n{txt}"
    assert "ShuffleReaderExec" in txt, txt


def test_overlap_has_no_hash_partitioned_parallelism():
    """
    ASERCJA DOKUMENTUJĄCA ZNALEZISKO (Faza H), nie życzenie.

    Overlap — w odróżnieniu od merge — NIE uzyskuje równoległości
    hash-partycjonowanej. Przyczyna jest w vendorze: IntervalJoinExec powstaje
    z `mode = PartitionMode::Auto`, a `required_input_distribution()` zwraca dla
    trybu Auto `UnspecifiedDistribution` dla obu wejść — więc hash-repartycja
    nigdy nie jest zażądana i EnforceDistribution nie ma czego wstawić.

    Mode zostaje Auto, bo `with_config_rt_bio()` usuwa regułę `join_selection`,
    która normalnie rozstrzygnęłaby Auto -> Partitioned/CollectLeft. To ten sam
    korzeń co bug naprawiony w Fazie A.5 (tam objawiał się przy WYKONANIU:
    "unsupported PartitionMode Auto in execute()"), tylko widziany o etap
    wcześniej — przy PLANOWANIU.

    Gdy ten test zacznie padać, to znaczy że ograniczenie zostało zdjęte
    (np. własną regułą optymalizatora wymuszającą Partitioned) — wtedy należy
    zaktualizować ten test i opis w OPIS.md, a nie "naprawiać" kod.
    """
    txt, stages = _explain("overlap")
    has_hash = bool(re.search(r"partitioning=Hash\(", txt))
    max_parts = max(p for _, p in stages)
    assert not has_hash and max_parts == 1, (
        "Overlap uzyskał równoległość hash-partycjonowaną — to ZMIANA na lepsze "
        "względem udokumentowanego stanu. Zaktualizuj ten test i OPIS.md.\n"
        f"{txt}"
    )

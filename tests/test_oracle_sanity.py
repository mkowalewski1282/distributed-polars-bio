"""
Testy sanity samej wyroczni (lokalny pb.overlap()) — bez żadnego silnika
rozproszonego. Cel: potwierdzić, że tests/overlap_oracle.py poprawnie odzwierciedla
semantykę pb.overlap() (half-open, [start, end)) na ręcznie zweryfikowanych
przypadkach brzegowych, zanim użyjemy go jako wyroczni dla Saila/Ballisty.

Uruchomienie: pytest tests/test_oracle_sanity.py -v
"""

from tests.overlap_oracle import EDGE_CASES, generate_random_intervals, reference_overlap_pairs


def test_touching_not_overlapping():
    a, b = EDGE_CASES["touching_not_overlapping"]
    assert reference_overlap_pairs(a, b) == set()


def test_zero_length_interval():
    a, b = EDGE_CASES["zero_length_interval"]
    # Empirycznie zweryfikowane zachowanie pb.overlap() (nie założenie): mimo że
    # w czystym half-open [150,150) nie zawiera żadnej pozycji, polars-bio i tak
    # zgłasza to jako overlap z [100,200) — traktuje zero-length interval jako
    # punkt "150", nie jako pusty zbiór. Ważne do pamiętania przy generowaniu
    # losowych danych testowych (generate_random_intervals dopuszcza length=0).
    assert reference_overlap_pairs(a, b) == {("a1", "b1")}


def test_duplicate_intervals():
    a, b = EDGE_CASES["duplicate_intervals"]
    # dwa identyczne interwały w A, oba nakładają się z jedynym interwałem w B
    assert reference_overlap_pairs(a, b) == {("a1", "b1")}


def test_empty_a():
    a, b = EDGE_CASES["empty_a"]
    assert reference_overlap_pairs(a, b) == set()


def test_empty_b():
    a, b = EDGE_CASES["empty_b"]
    assert reference_overlap_pairs(a, b) == set()


def test_no_matching_chrom():
    a, b = EDGE_CASES["no_matching_chrom"]
    assert reference_overlap_pairs(a, b) == set()


def test_single_base_overlap():
    a, b = EDGE_CASES["single_base_overlap"]
    assert reference_overlap_pairs(a, b) == {("a1", "b1")}


def test_generator_is_deterministic_per_seed():
    a1, b1 = generate_random_intervals(seed=42)
    a2, b2 = generate_random_intervals(seed=42)
    assert a1 == a2
    assert b1 == b2


def test_generator_runs_and_oracle_does_not_crash():
    # Nie sprawdzamy konkretnego wyniku (losowe dane) — tylko że wyrocznia
    # nie wywala się na wielu różnych ziarnach, w tym na skrajnych rozmiarach.
    for seed in range(10):
        a, b = generate_random_intervals(seed=seed, n_a=15, n_b=15, n_chroms=2)
        pairs = reference_overlap_pairs(a, b)
        assert isinstance(pairs, set)

"""
Sanity tests of the oracle itself (the local pb.overlap()), without any
distributed engine. Goal: confirm that tests/overlap_oracle.py correctly reflects
the semantics of pb.overlap() (half-open, [start, end)) on manually verified
edge cases before it is used as the oracle for Sail/Ballista.

Run: pytest tests/test_oracle_sanity.py -v
"""

from tests.overlap_oracle import EDGE_CASES, generate_random_intervals, reference_overlap_pairs


def test_touching_not_overlapping():
    a, b = EDGE_CASES["touching_not_overlapping"]
    assert reference_overlap_pairs(a, b) == set()


def test_zero_length_interval():
    a, b = EDGE_CASES["zero_length_interval"]
    # Empirically verified behaviour of pb.overlap() (not an assumption): although
    # in pure half-open semantics [150,150) contains no position, polars-bio still
    # reports it as an overlap with [100,200); it treats a zero-length interval as
    # the point "150", not as an empty set. Worth remembering when generating
    # random test data (generate_random_intervals allows length=0).
    assert reference_overlap_pairs(a, b) == {("a1", "b1")}


def test_duplicate_intervals():
    a, b = EDGE_CASES["duplicate_intervals"]
    # two identical intervals in A, both overlap the only interval in B
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
    # We do not check a specific result (random data), only that the oracle
    # does not crash on many different seeds, including extreme sizes.
    for seed in range(10):
        a, b = generate_random_intervals(seed=seed, n_a=15, n_b=15, n_chroms=2)
        pairs = reference_overlap_pairs(a, b)
        assert isinstance(pairs, set)

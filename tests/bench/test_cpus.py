"""Tests of the CPU layout derived from the topology (fake sysfs)."""

from __future__ import annotations

import pytest

from bench.cpus import (
    NotEnoughCpus,
    TopologyError,
    control_cpus,
    cpus_for,
    max_parallelism,
    read_layout,
    uses_smt,
)
from tests.bench.fake_sysfs import make


def test_laptop_layout(tmp_path):  # 6 cores, core k = CPUs 2k, 2k+1
    lay = read_layout(make(tmp_path, [[2 * k, 2 * k + 1] for k in range(6)]), affinity=set(range(12)))
    assert lay.system == (0, 1)
    assert lay.order == (2, 4, 6, 8, 10, 3, 5, 7, 9, 11)
    assert cpus_for(lay, 1) == (2,) and cpus_for(lay, 2) == (2, 4)
    assert cpus_for(lay, 4) == (2, 4, 6, 8)
    assert cpus_for(lay, 8) == (2, 4, 6, 8, 10, 3, 5, 7)
    assert not uses_smt(lay, cpus_for(lay, 4)) and uses_smt(lay, cpus_for(lay, 8))
    assert control_cpus(lay) == (2, 3) and max_parallelism(lay) == 10


def test_layout_without_smt(tmp_path):
    lay = read_layout(make(tmp_path, [[0], [1], [2], [3]]), affinity={0, 1, 2, 3})
    assert lay.system == (0,) and lay.order == (1, 2, 3)
    assert control_cpus(lay) == (1, 2) and not uses_smt(lay, (1, 2, 3))
    with pytest.raises(NotEnoughCpus):
        cpus_for(lay, 4)


def test_layout_respects_affinity(tmp_path):
    lay = read_layout(make(tmp_path, [[2 * k, 2 * k + 1] for k in range(6)]), affinity={4, 5, 6, 7})
    assert lay.system == (4, 5) and lay.order == (6, 7) and uses_smt(lay, (6, 7))


def test_layout_interleaved_numbering(tmp_path):  # Intel: siblings i and i+4
    lay = read_layout(make(tmp_path, [[0, 4], [1, 5], [2, 6], [3, 7]]), affinity=set(range(8)))
    assert lay.system == (0, 4) and lay.order == (1, 2, 3, 5, 6, 7)


def test_unreadable_topology_raises(tmp_path):
    with pytest.raises(TopologyError):
        read_layout(tmp_path / "missing", affinity={0})

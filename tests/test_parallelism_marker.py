"""Znacznik parallelism(p) (plan 3b-1, etap 7; etap 4: semantyka P): test potrzebujący P CPU poza
rdzeniem systemowym jest pomijany, gdy układ CPU maszyny ich nie ma (bench/cpus.py) albo gdy
topologii nie da się odczytać — np. runner CI z 4 vCPU (2 rdzenie SMT) mieści tylko P ≤ 2."""

from __future__ import annotations

import pytest

from bench import cpus
from tests import conftest
from tests.bench.fake_sysfs import make


class _Item:
    """Minimalny zamiennik pytest.Item — hook używa tylko get_closest_marker."""

    def __init__(self, mark):
        self._mark = mark

    def get_closest_marker(self, name):
        return self._mark if self._mark is not None and self._mark.name == name else None


def _layout(monkeypatch, tmp_path, cores) -> None:
    layout = cpus.read_layout(make(tmp_path, cores), affinity={t for core in cores for t in core})
    monkeypatch.setattr(cpus, "read_layout", lambda *args, **kwargs: layout)


def test_parallelism_beyond_the_layout_is_skipped(monkeypatch, tmp_path):
    _layout(monkeypatch, tmp_path, [[0, 1], [2, 3]])  # 4 vCPU: rdzeń systemowy + 2 CPU
    with pytest.raises(pytest.skip.Exception, match=r"P = 4 .*2 CPUs outside the system core"):
        conftest.pytest_runtest_setup(_Item(pytest.mark.parallelism(4).mark))


def test_parallelism_within_the_layout_runs(monkeypatch, tmp_path):
    _layout(monkeypatch, tmp_path, [[0, 1], [2, 3]])
    conftest.pytest_runtest_setup(_Item(pytest.mark.parallelism(2).mark))


def test_unreadable_topology_skips_with_the_cause(monkeypatch):
    def broken(*args, **kwargs):
        raise cpus.TopologyError("cannot read /sys/devices/system/cpu/cpu3/topology/core_id")

    monkeypatch.setattr(cpus, "read_layout", broken)
    with pytest.raises(pytest.skip.Exception, match="CPU topology unreadable.*core_id"):
        conftest.pytest_runtest_setup(_Item(pytest.mark.parallelism(1).mark))


def test_unmarked_test_does_not_read_the_layout(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("layout read for an unmarked test")

    monkeypatch.setattr(cpus, "read_layout", unexpected)
    conftest.pytest_runtest_setup(_Item(None))

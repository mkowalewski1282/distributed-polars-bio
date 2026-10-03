"""Znacznik nodes(n) (plan 3b-1, etap 7): test przypinający procesy do węzłów 1..n jest pomijany,
gdy procesowi brakuje CPU rdzenia systemowego albo któregoś z węzłów — np. runner CI z 4 vCPU
przy N = 2 (specyfikacja metodyki, sekcja 3: węzeł k = CPU {2k, 2k+1})."""

from __future__ import annotations

import os

import pytest

from tests import conftest


class _Item:
    """Minimalny zamiennik pytest.Item — hook używa tylko get_closest_marker."""

    def __init__(self, mark):
        self._mark = mark

    def get_closest_marker(self, name):
        return self._mark if self._mark is not None and self._mark.name == name else None


def _cpus(monkeypatch, cpus) -> None:
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: set(cpus))


def test_two_nodes_are_skipped_on_four_cpus(monkeypatch):
    _cpus(monkeypatch, range(4))
    with pytest.raises(pytest.skip.Exception, match=r"CPUs 0-5.*\[4, 5\]"):
        conftest.pytest_runtest_setup(_Item(pytest.mark.nodes(2).mark))


def test_one_node_runs_on_four_cpus(monkeypatch):
    _cpus(monkeypatch, range(4))
    conftest.pytest_runtest_setup(_Item(pytest.mark.nodes(1).mark))


def test_unmarked_test_ignores_affinity(monkeypatch):
    _cpus(monkeypatch, [])
    conftest.pytest_runtest_setup(_Item(None))

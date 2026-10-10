"""Sprawdzenia przed serią (plan 3b-2, Zadanie 19; przegląd 3a, pozycja 4): CPU węzłów dostępne
dla procesu, pary SMT zgodne z tabelą węzłów, taskset i pysail, pusty swap."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench import preflight
from bench.config import ConfigError


def sysfs(tmp_path: Path, siblings: dict[int, str]) -> Path:
    for cpu, text in siblings.items():
        topology = tmp_path / f"cpu{cpu}" / "topology"
        topology.mkdir(parents=True)
        (topology / "thread_siblings_list").write_text(text + "\n")
    return tmp_path


SMT_PAIRS = {0: "0-1", 2: "2-3", 4: "4-5", 6: "6-7"}


def test_cpus_of_the_node_layout_pass(tmp_path):
    preflight.check_cpus(3, affinity=set(range(12)), sysfs=sysfs(tmp_path, SMT_PAIRS))


def test_missing_cpus_in_affinity_are_reported(tmp_path):
    with pytest.raises(ConfigError, match=r"CPUs \[6, 7\] are not available"):
        preflight.check_cpus(3, affinity=set(range(6)), sysfs=sysfs(tmp_path, SMT_PAIRS))


def test_layout_without_smt_pairs_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="CPU 2 and 3 are not SMT siblings"):
        preflight.check_cpus(2, affinity=set(range(12)), sysfs=sysfs(tmp_path, {0: "0-1", 2: "2,8", 4: "4-5"}))


def test_unreadable_topology_is_reported(tmp_path):
    with pytest.raises(ConfigError, match="cannot read SMT topology of CPU 0"):
        preflight.check_cpus(1, affinity=set(range(12)), sysfs=tmp_path)


def test_missing_taskset_is_reported(monkeypatch):
    monkeypatch.setattr(preflight.shutil, "which", lambda name: None)
    with pytest.raises(ConfigError, match="taskset"):
        preflight.check_tools(["polars_bio_a"])


def test_pysail_is_required_only_for_sail_variants(monkeypatch):
    monkeypatch.setattr(preflight, "_importable", lambda name: False)
    preflight.check_tools(["polars_bio_a", "ballista"])
    with pytest.raises(ConfigError, match="pysail"):
        preflight.check_tools(["sail_lock"])


def test_importable_runs_a_real_import():
    assert preflight._importable("json")
    assert not preflight._importable("no_such_module_3b2")


def test_used_swap_is_rejected():
    preflight.check_swap(0)
    with pytest.raises(ConfigError, match=r"swap is not empty \(2 MiB used\)"):
        preflight.check_swap(2 * 2**20)


def test_this_machine_has_the_node_layout():
    """Maszyna pomiarowa (Ryzen 5 4600H, WSL2): rdzeń k = wątki {2k, 2k+1} (lscpu -e)."""
    if not (preflight.SYSFS_CPU / "cpu7").exists():
        pytest.skip("fewer than 8 CPUs (CI runner)")
    preflight.check_cpus(3, affinity=set(range(8)))

"""Sprawdzenia przed serią (plan 3b-2, Zadanie 19; przegląd 3a, pozycja 4; etap 4: układ CPU
z topologii): czytelna topologia, miejsce co najmniej na P = 1 i przebieg kontrolny, za duże P
pominięte z ostrzeżeniem; taskset i pysail, pusty swap."""

from __future__ import annotations

import pytest

from bench import preflight
from bench.config import ConfigError, parse
from bench.cpus import read_layout
from tests.bench.fake_sysfs import make


def config(parallelism):
    return parse({
        "series": "test", "scenarios": [{"op": "overlap", "pair": "1-2"}],
        "variants": ["polars_bio"], "parallelism": parallelism, "seed": 1,
    })


def layout(tmp_path, cores):
    return read_layout(make(tmp_path, cores), affinity={t for core in cores for t in core})


def test_check_cpus_returns_skipped_parallelism(tmp_path):
    """3 rdzenie SMT: rdzeń systemowy + 4 CPU, więc P = 8 zostanie pominięte, reszta mieści się."""
    small = layout(tmp_path, [[0, 1], [2, 3], [4, 5]])
    assert preflight.check_cpus(config([1, 2, 4, 8]), small) == [8]
    assert preflight.check_cpus(config([1, 2, 4]), small) == []


def test_check_cpus_fails_without_control_cpus(tmp_path):
    """Jeden CPU poza rdzeniem systemowym: P = 1 by się zmieściło, ale przebieg kontrolny
    (2 wątki) nie ma gdzie działać."""
    with pytest.raises(ConfigError, match="control run needs 2 CPUs"):
        preflight.check_cpus(config([1]), layout(tmp_path, [[0], [1]]))


def test_check_cpus_fails_without_any_cpu_outside_the_system_core(tmp_path):
    with pytest.raises(ConfigError, match="at least P = 1"):
        preflight.check_cpus(config([1]), layout(tmp_path, [[0, 1]]))


def test_unreadable_topology_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError, match="cannot read the CPU topology.*sysfs directory not found"):
        preflight.cpu_layout(sysfs=tmp_path / "missing")


def test_preflight_warns_about_skipped_parallelism(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(preflight, "check_swap", lambda used: None)
    monkeypatch.setattr(preflight, "check_tools", lambda variants: None)
    small = layout(tmp_path, [[0, 1], [2, 3], [4, 5]])
    assert preflight.run(config([1, 8]), "debug", small) == [8]
    err = capsys.readouterr().err
    assert "P = 8" in err and "4 CPUs" in err and "skipped" in err


def test_missing_taskset_is_reported(monkeypatch):
    monkeypatch.setattr(preflight.shutil, "which", lambda name: None)
    with pytest.raises(ConfigError, match="taskset"):
        preflight.check_tools(["polars_bio"])


def test_pysail_is_required_only_for_sail_variants(monkeypatch):
    monkeypatch.setattr(preflight, "_importable", lambda name: False)
    preflight.check_tools(["polars_bio", "ballista"])
    with pytest.raises(ConfigError, match="pysail"):
        preflight.check_tools(["sail"])


def test_importable_runs_a_real_import():
    assert preflight._importable("json")
    assert not preflight._importable("no_such_module_3b2")


def test_used_swap_is_rejected():
    preflight.check_swap(0)
    with pytest.raises(ConfigError, match=r"swap is not empty \(2 MiB used\)"):
        preflight.check_swap(2 * 2**20)


def test_this_machine_fits_the_smallest_series():
    """Na maszynie testów (laptop albo CI) mieszczą się P = 1 i przebieg kontrolny."""
    assert preflight.check_cpus(config([1]), preflight.cpu_layout()) == []

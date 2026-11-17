"""Pre-series checks (CPU layout from the topology): readable topology, room for at least
P = 1 and the control run, too-large P skipped with a warning; taskset and pysail, empty swap."""

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
    """3 SMT cores: system core + 4 CPUs, so P = 8 is skipped and the rest fits."""
    small = layout(tmp_path, [[0, 1], [2, 3], [4, 5]])
    assert preflight.check_cpus(config([1, 2, 4, 8]), small) == [8]
    assert preflight.check_cpus(config([1, 2, 4]), small) == []


def test_check_cpus_fails_without_control_cpus(tmp_path):
    """One CPU outside the system core: P = 1 would fit, but the control run
    (2 threads) has nowhere to run."""
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
    assert not preflight._importable("no_such_module_for_preflight")


def test_used_swap_is_rejected():
    preflight.check_swap(0)
    with pytest.raises(ConfigError, match=r"swap is not empty \(2 MiB used\)") as info:
        preflight.check_swap(2 * 2**20)
    assert "wsl --shutdown" in str(info.value) and "sudo swapoff -a && sudo swapon -a" in str(info.value)


@pytest.mark.parallelism(2)
def test_this_machine_fits_the_smallest_series():
    """On the test machine (laptop or CI), P = 1 and the control run fit. The control run needs
    two CPUs outside the system core, so the test is skipped on smaller machines (for example a
    2-vCPU runner, where the series cannot run at all)."""
    assert preflight.check_cpus(config([1]), preflight.cpu_layout()) == []

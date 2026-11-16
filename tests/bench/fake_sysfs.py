"""Fake sysfs with a CPU topology for the core-layout tests."""

from __future__ import annotations

from pathlib import Path


def make(tmp_path: Path, cores: list[list[int]], package: int = 0) -> Path:
    """Creates cpuN/topology/{core_id,physical_package_id,thread_siblings_list}; core = list of threads."""
    root = tmp_path / "cpu"
    root.mkdir()
    # entries that are not CPUs, as in a real sysfs
    (root / "cpufreq").mkdir()
    (root / "online").write_text("0-%d\n" % (sum(len(c) for c in cores) - 1))
    for core_id, threads in enumerate(cores):
        siblings = ",".join(str(t) for t in sorted(threads))
        for t in threads:
            topo = root / f"cpu{t}" / "topology"
            topo.mkdir(parents=True)
            (topo / "core_id").write_text(f"{core_id}\n")
            (topo / "physical_package_id").write_text(f"{package}\n")
            (topo / "thread_siblings_list").write_text(siblings + "\n")
    return root

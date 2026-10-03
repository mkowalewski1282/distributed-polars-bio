"""Metryki z /proc (plan 3a, Zadanie 2; specyfikacja 5 i 6): parsowanie na plikach
wzorcowych oraz działanie na prawdziwym /proc."""

from __future__ import annotations

import os

import pytest

from bench import metrics

STATUS = """Name:\tpython3
Umask:\t0022
State:\tS (sleeping)
VmPeak:\t  812344 kB
VmSize:\t  812344 kB
VmHWM:\t  123456 kB
VmRSS:\t  100000 kB
Threads:\t14
Cpus_allowed:\t00c
Cpus_allowed_list:\t2-3
"""

MEMINFO = """MemTotal:        5000000 kB
MemFree:         3000000 kB
MemAvailable:    3900000 kB
Buffers:           10000 kB
"""

VMSTAT = """nr_free_pages 750000
pswpin 92346
pswpout 206037
pgfault 123
"""


@pytest.fixture
def fake_proc(tmp_path):
    (tmp_path / "4242").mkdir()
    (tmp_path / "4242" / "status").write_text(STATUS)
    (tmp_path / "4242" / "clear_refs").write_text("")
    (tmp_path / "meminfo").write_text(MEMINFO)
    (tmp_path / "vmstat").write_text(VMSTAT)
    return tmp_path


def test_peak_rss_reads_vmhwm_in_bytes(fake_proc):
    assert metrics.peak_rss(4242, fake_proc) == 123456 * 1024


def test_reset_peak_rss_writes_5_to_clear_refs(fake_proc):
    metrics.reset_peak_rss(4242, fake_proc)
    assert (fake_proc / "4242" / "clear_refs").read_text() == "5"


def test_cpus_allowed(fake_proc):
    assert metrics.cpus_allowed(4242, fake_proc) == "2-3"


def test_mem_available_in_bytes(fake_proc):
    assert metrics.mem_available(fake_proc) == 3900000 * 1024


def test_pswpout_not_pswpin(fake_proc):
    assert metrics.pswpout(fake_proc) == 206037


def test_pswpin(fake_proc):
    assert metrics.pswpin(fake_proc) == 92346


def test_missing_field_is_reported(tmp_path):
    (tmp_path / "1").mkdir()
    (tmp_path / "1" / "status").write_text("Name:\tzombie\nState:\tZ (zombie)\n")
    with pytest.raises(KeyError, match="VmHWM"):
        metrics.peak_rss(1, tmp_path)


def test_unexpected_unit_is_rejected(tmp_path):
    (tmp_path / "1").mkdir()
    (tmp_path / "1" / "status").write_text("VmHWM:\t  12 MB\n")
    with pytest.raises(ValueError, match="unexpected unit"):
        metrics.peak_rss(1, tmp_path)


def test_missing_pswpout_is_reported(tmp_path):
    (tmp_path / "vmstat").write_text("pswpin 1\n")
    with pytest.raises(KeyError, match="pswpout"):
        metrics.pswpout(tmp_path)


def test_reset_peak_rss_on_real_process():
    """200 MB zapisanych stron podnosi VmHWM; po zwolnieniu i wyzerowaniu licznik
    wraca do bieżącego RSS."""
    blob = b"x" * (200 << 20)
    del blob
    before = metrics.peak_rss()
    metrics.reset_peak_rss()
    assert metrics.peak_rss() < before - (100 << 20)


def test_real_proc_counters_are_readable():
    assert metrics.mem_available() > 0
    assert metrics.pswpout() >= 0
    assert metrics.cpus_allowed() != ""


def test_dir_size_counts_regular_files_recursively(tmp_path):
    (tmp_path / "a.arrow").write_bytes(b"x" * 10)
    (tmp_path / "stage" / "1").mkdir(parents=True)
    (tmp_path / "stage" / "1" / "b.arrow").write_bytes(b"y" * 20)
    big = tmp_path.parent / f"{tmp_path.name}_outside.bin"
    big.write_bytes(b"z" * 1000)
    os.symlink(big, tmp_path / "symlink")
    assert metrics.dir_size(tmp_path) == 30
    assert metrics.dir_size(tmp_path / "missing") == 0

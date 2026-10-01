"""Pomocnicze funkcje procesów silników (plan 3a, Zadanie 5)."""

from __future__ import annotations

import socket
import subprocess
import sys

import pytest

from bench.procutil import EngineError, free_port, wait_for_port


def test_free_port_is_bindable():
    port = free_port()
    with socket.socket() as s:
        s.bind(("127.0.0.1", port))


def test_wait_for_port_returns_when_process_listens(tmp_path):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_port(port, proc, name="http", log=tmp_path / "log", timeout=30)
    finally:
        proc.terminate()
        proc.wait()


def test_wait_for_port_reports_dead_process(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "raise SystemExit(3)"])
    proc.wait()
    with pytest.raises(EngineError, match=r"proces serwer_testowy zakończył się \(kod 3\)"):
        wait_for_port(free_port(), proc, name="serwer_testowy", log=tmp_path / "log", timeout=5)


def test_wait_for_port_times_out(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        with pytest.raises(EngineError, match="nie nasłuchuje"):
            wait_for_port(free_port(), proc, name="cichy", log=tmp_path / "log", timeout=1)
    finally:
        proc.kill()
        proc.wait()

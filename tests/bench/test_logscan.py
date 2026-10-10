"""Liczniki z logów procesów silników z czasu jednego przebiegu (plan 3b-2, Zadanie 11)."""

from __future__ import annotations

from bench import logscan

SAIL_LOG = """INFO creating session abc
INFO worker 1 server is ready
INFO worker 2 server is ready
INFO worker 1 server is ready
"""


def test_sail_workers_counts_distinct_ids():
    assert logscan.sail_workers(SAIL_LOG) == 2
    assert logscan.sail_workers("") == 0


def test_broadcast_bytes_sums_decoded_payloads():
    text = (
        "2026-10-09 INFO ballista_genomics::bio_phys_codec: broadcast payload decoded: 1000 bytes\n"
        "noise\n"
        "2026-10-09 INFO ballista_genomics::bio_phys_codec: broadcast payload decoded: 24 bytes\n"
    )
    assert logscan.broadcast_bytes(text) == 1024
    assert logscan.broadcast_bytes("") == 0


def test_text_since_reads_only_new_lines(tmp_path):
    log = tmp_path / "engine.log"
    log.write_text("before\n")
    missing = tmp_path / "later.log"
    mark = logscan.mark([log, missing])
    assert mark == {log: 7, missing: 0}
    with log.open("a") as f:
        f.write("after\n")
    missing.write_text("created\n")
    assert logscan.text_since(mark) == "after\ncreated\n"


def test_text_since_rereads_a_file_shorter_than_the_mark(tmp_path):
    """Plik zastąpiony albo skrócony (np. po restarcie silnika) — czytamy od początku."""
    log = tmp_path / "engine.log"
    log.write_text("a long line written before the mark\n")
    mark = logscan.mark([log])
    log.write_text("new\n")
    assert logscan.text_since(mark) == "new\n"

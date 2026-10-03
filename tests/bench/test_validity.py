"""Reguły ważności przebiegu i dryfu (plan 3a, Zadanie 8; specyfikacja 6.3–6.5 i 8.3)."""

from __future__ import annotations

import json

import pytest

from bench.validity import Expected, block_drifted, drift, invalid_reasons, parse_report, stderr_tail

GOOD = {
    "rows": 3, "checksum": "0x00000000000000aa", "t_total_s": 0.5, "phases": {},
    "extra": {"target_partitions": 2}, "peak_rss_bytes": 1234,
}
EXPECTED = Expected(3, "0x00000000000000aa")


def reasons(**overrides) -> list[str]:
    args = dict(
        returncode=0, killed=None, report=dict(GOOD), report_error=None, expected=EXPECTED,
        pswpout_delta=0, stderr="",
    )
    args.update(overrides)
    return invalid_reasons(**args)


def test_parse_report_accepts_protocol_line():
    assert parse_report("\n" + json.dumps(GOOD) + "\n") == GOOD


@pytest.mark.parametrize(
    "stdout, message",
    [
        ("", "one JSON line"),
        (json.dumps(GOOD) + "\n" + json.dumps(GOOD), "one JSON line"),
        ("not JSON", "invalid JSON"),
        ("[1, 2]", "a JSON object"),
        (json.dumps({k: v for k, v in GOOD.items() if k != "extra"}), "missing fields"),
        (json.dumps({**GOOD, "rows": -1}), "rows"),
        (json.dumps({**GOOD, "rows": True}), "rows"),
        (json.dumps({**GOOD, "checksum": "0xAA"}), "checksum"),
        (json.dumps({**GOOD, "t_total_s": "1"}), "t_total_s"),
        (json.dumps({**GOOD, "phases": []}), "phases"),
        (json.dumps({**GOOD, "peak_rss_bytes": 0}), "peak_rss_bytes"),
    ],
)
def test_parse_report_rejects_broken_lines(stdout, message):
    with pytest.raises(ValueError, match=message):
        parse_report(stdout)


def test_valid_run_has_no_reasons():
    assert reasons() == []


def test_killed_run_reports_why_it_was_killed_and_swap():
    assert reasons(killed="timeout", returncode=-9, report=None, pswpout_delta=5) == [
        "timeout", "swap: pswpout +5",
    ]


def test_engine_error_quotes_last_stderr_line():
    got = reasons(returncode=1, report=None, stderr="log\nbench_client: missing data: /x\n\n")
    assert got == ["exit code 1: bench_client: missing data: /x"]


def test_missing_report_uses_parse_error():
    got = reasons(report=None, report_error="runner stdout: invalid JSON")
    assert got == ["runner stdout: invalid JSON"]


def test_wrong_rows_and_checksum_are_both_reported():
    got = reasons(report={**GOOD, "rows": 4, "checksum": "0x00000000000000bb"})
    assert got == [
        "row count 4 ≠ reference 3",
        "checksum 0x00000000000000bb ≠ reference 0x00000000000000aa",
    ]


def test_swap_invalidates_otherwise_good_run():
    assert reasons(pswpout_delta=12) == ["swap: pswpout +12"]


def test_without_expected_value_only_execution_is_checked():
    assert reasons(expected=None, report={**GOOD, "rows": 99}) == []


def test_stderr_tail_is_shortened():
    assert stderr_tail("x" * 1000, limit=10) == "x" * 10
    assert stderr_tail("") == "(empty stderr)"


def test_drift_rule():
    assert drift(1.0, 1.2) == pytest.approx(0.2)
    assert not block_drifted(1.0, 1.05, 0.10)
    assert block_drifted(1.0, 1.2, 0.10)
    assert block_drifted(1.0, 0.8, 0.10)
    assert block_drifted(None, 1.0, 0.10) and block_drifted(1.0, None, 0.10)

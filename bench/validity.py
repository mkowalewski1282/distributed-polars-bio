"""Rules for the validity of a run and for the drift of control runs."""

from __future__ import annotations

import json
import re
import statistics
from dataclasses import dataclass

#: Fields of the runner's JSON line.
REPORT_KEYS = ("rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes")
_CHECKSUM = re.compile(r"0x[0-9a-f]{16}")


@dataclass(frozen=True)
class Expected:
    """Reference result of a scenario: row count and checksum."""

    rows: int
    checksum: str


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_report(stdout: str) -> dict:
    """A single non-empty JSON line conforming to the runner protocol; otherwise ValueError with a description."""
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise ValueError(f"runner stdout: expected one JSON line, got {len(lines)}")
    try:
        report = json.loads(lines[0])
    except json.JSONDecodeError as e:
        raise ValueError(f"runner stdout: invalid JSON ({e})") from e
    if not isinstance(report, dict):
        raise ValueError("runner stdout: expected a JSON object")
    missing = [k for k in REPORT_KEYS if k not in report]
    if missing:
        raise ValueError(f"runner stdout: missing fields {missing}")
    if not (_is_int(report["rows"]) and report["rows"] >= 0):
        raise ValueError(f"runner stdout: rows = {report['rows']!r}")
    if not (isinstance(report["checksum"], str) and _CHECKSUM.fullmatch(report["checksum"])):
        raise ValueError(f"runner stdout: checksum = {report['checksum']!r}")
    t = report["t_total_s"]
    if isinstance(t, bool) or not isinstance(t, (int, float)) or t < 0:
        raise ValueError(f"runner stdout: t_total_s = {t!r}")
    if not isinstance(report["phases"], dict) or not isinstance(report["extra"], dict):
        raise ValueError("runner stdout: phases and extra must be objects")
    if not (_is_int(report["peak_rss_bytes"]) and report["peak_rss_bytes"] > 0):
        raise ValueError(f"runner stdout: peak_rss_bytes = {report['peak_rss_bytes']!r}")
    return report


def stderr_tail(stderr: str, limit: int = 300) -> str:
    """The last non-empty stderr line (the runner's error message), truncated to `limit` characters."""
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    return lines[-1][-limit:] if lines else "(empty stderr)"


def invalid_reasons(
    *, returncode: int, killed: str | None, report: dict | None, report_error: str | None,
    expected: Expected | None, pswpout_delta: int | None, pswpin_delta: int | None, stderr: str,
) -> list[str]:
    """Reasons a run is invalid (an empty list = valid), in order:

    - killed by the orchestrator (timeout, memory guard) — the reason as is;
    - a non-zero exit code — with the last stderr line;
    - no valid JSON line;
    - a row count or checksum different from the reference;
    - an increase of pswpout or pswpin (a series starts from empty swap, so every swap-in
      comes from the measurement)."""
    reasons: list[str] = []
    if killed is not None:
        reasons.append(killed)
    elif returncode != 0:
        reasons.append(f"exit code {returncode}: {stderr_tail(stderr)}")
    elif report is None:
        reasons.append(report_error or "no runner report")
    elif expected is not None:
        if report["rows"] != expected.rows:
            reasons.append(f"row count {report['rows']} ≠ reference {expected.rows}")
        if report["checksum"] != expected.checksum:
            reasons.append(f"checksum {report['checksum']} ≠ reference {expected.checksum}")
    if pswpout_delta:
        reasons.append(f"swap: pswpout +{pswpout_delta}")
    if pswpin_delta:
        reasons.append(f"swap: pswpin +{pswpin_delta}")
    return reasons


def drift(t_start: float, t_end: float) -> float:
    """Relative change of the control-run time between the start and the end of a block."""
    return abs(t_end - t_start) / max(t_start, 1e-9)


def control_drift(start: list[float], end: list[float], *, repeats: int, tolerance: float) -> str | None:
    """The reason a block is invalidated after its control runs, or None.

    `start`, `end` — times of the VALID control runs from the start and the end of the block.
    Fewer than half valid at either end → "control run invalid"; a relative difference of the
    medians above the tolerance → "control drift N%". The median of several runs cuts off
    single outliers."""
    if not start or not end or 2 * len(start) < repeats or 2 * len(end) < repeats:
        return "control run invalid"
    d = drift(statistics.median(start), statistics.median(end))
    return f"control drift {d:.0%}" if d > tolerance else None

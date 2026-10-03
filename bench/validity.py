"""Reguły ważności przebiegu (specyfikacja 6.4–6.5) i dryfu przebiegu kontrolnego (6.3)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

#: Pola linii JSON runnera (specyfikacja 8.3).
REPORT_KEYS = ("rows", "checksum", "t_total_s", "phases", "extra", "peak_rss_bytes")
_CHECKSUM = re.compile(r"0x[0-9a-f]{16}")


@dataclass(frozen=True)
class Expected:
    """Wynik wzorcowy scenariusza: liczba wierszy i suma kontrolna."""

    rows: int
    checksum: str


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_report(stdout: str) -> dict:
    """Jedna niepusta linia JSON zgodna z protokołem 8.3; inaczej ValueError z opisem."""
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
    """Ostatnia niepusta linia stderr (komunikat błędu runnera), skrócona do `limit` znaków."""
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    return lines[-1][-limit:] if lines else "(empty stderr)"


def invalid_reasons(
    *, returncode: int, killed: str | None, report: dict | None, report_error: str | None,
    expected: Expected | None, pswpout_delta: int | None, stderr: str,
) -> list[str]:
    """Przyczyny nieważności przebiegu (pusta lista = ważny), w kolejności:

    - zabicie przez orkiestrator (timeout, strażnik pamięci) — przyczyna wprost;
    - niezerowy kod wyjścia — z ostatnią linią stderr;
    - brak poprawnej linii JSON;
    - liczba wierszy albo suma kontrolna różna od wzorca;
    - przyrost pswpout (specyfikacja 6.5: swap unieważnia czas i pamięć)."""
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
    return reasons


def drift(t_start: float, t_end: float) -> float:
    """Względna zmiana czasu przebiegu kontrolnego między początkiem a końcem bloku."""
    return abs(t_end - t_start) / max(t_start, 1e-9)


def block_drifted(t_start: float | None, t_end: float | None, tolerance: float) -> bool:
    """Blok do powtórzenia (specyfikacja 6.3): dryf ponad tolerancję albo nieważny przebieg
    kontrolny (brak czasu)."""
    if t_start is None or t_end is None:
        return True
    return drift(t_start, t_end) > tolerance

#!/usr/bin/env bash
# A measurement series on this machine: the smoke series (a short check that every engine works),
# the scaling series (P = 1, 2, 4, 8) and the report of the scaling series.
# Prerequisite: ./setup.sh
set -euo pipefail
cd "$(dirname "$0")"

cat <<'TEXT'
Before a measurement series:
  1. Close editors, IDEs and all background applications; leave the machine idle until the end.
  2. On WSL2: check the memory limit in .wslconfig (memory=...) and start from an empty swap
     (`free -h`: Swap used 0B; if not, run `wsl --shutdown` in Windows and start again).
     Runs that touch swap are marked invalid.
  3. The scaling series takes about 1.5 hours, up to about 3 hours when blocks are repeated after
     control-run drift (limit 4 hours).
Press Enter to start (Ctrl+C to cancel).
TEXT
read -r

if ! uv run python -m bench.orchestrator bench/conf/smoke.yaml; then
    echo "The smoke series failed - see its summary above; the scaling series was not started." >&2
    exit 1
fi

# Newest scaling series directory (bench/results/scaling/<YYYYmmdd-HHMMSS>), empty if none.
newest_series() {
    if [ -d bench/results/scaling ]; then
        find bench/results/scaling -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1
    fi
}

before=$(newest_series)
status=0
uv run python -m bench.orchestrator bench/conf/scaling.yaml || status=$?
series=$(newest_series)
if [ -z "$series" ] || [ "$series" = "$before" ]; then
    echo "The scaling series did not start - see the error above." >&2
    if [ "$status" -eq 0 ]; then status=1; fi
    exit "$status"
fi
uv run python -m bench.report "$series"
echo "Results and report: $series"
exit "$status"

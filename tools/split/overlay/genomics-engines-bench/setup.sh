#!/usr/bin/env bash
# One-time setup after `git clone --recursive`: engine submodules, the Python environment, the
# Ballista release binaries and the databio-8p dataset (in ~/bench_data or $BENCH_DATA_ROOT).
# Needs: uv, rustup, network access.
set -euo pipefail
cd "$(dirname "$0")"

git submodule update --init --recursive
uv sync --locked

# Linking one Ballista binary takes several GB of RAM: one build job by default (override with
# CARGO_BUILD_JOBS=N on a machine with more memory).
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
echo "Building the Ballista release binaries (engines/ballista-genomics, CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS)."
echo "A cold release build takes a long time: from about 15 minutes to over an hour, depending on the machine."
(cd engines/ballista-genomics && cargo build --release --bins --locked)

uv run python -m bench.data.download
echo "Setup done. Run a series with ./run_series.sh"

"""Ballista and polars-bio compute with the same version of the interval algorithms.

polars-bio is built from datafusion-bio-function-ranges from git (a tag in the polars-bio
Cargo.toml); Ballista uses a vendored copy of that crate with visibility patches (vendor/PATCH.md).
If the versions diverge, an engine comparison mixes the engine effect with the library-version
effect; for example, upstream fixed single-base intervals in nearest and coverage between 0.18.0
and 0.22.2."""

from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
CRATE = "datafusion-bio-function-ranges"

#: Version of the algorithms crate that a given polars-bio version was built with: the polars-bio
#: Cargo.toml at the release tag (0.36.0: tag = "v0.22.2"). A new polars-bio version means a new entry
#: here and, when the crate version changes, an update of the vendored copy per vendor/PATCH.md.
RANGES_IN_POLARS_BIO = {"0.36.0": "0.22.2", "0.37.0": "0.24.0"}

#: Exceptions to version equality: (expected per polars-bio, vendored version) -> reason for equivalence.
#: A pair is allowed only with a reason; other versions (e.g. 0.23.0) are not equivalent.
EQUIVALENT_RANGES: dict[tuple[str, str], str] = {
    ("0.24.0", "0.22.2"): "0.22.2 and 0.24.0 differ only in DataFusion version bumps; the interval algorithms are identical",
    ("0.24.0", "0.25.0"): "0.25.0 only moves to the DataFusion 55 API; the interval algorithms are identical to 0.24.0",
}


def test_ballista_uses_the_ranges_crate_version_of_polars_bio():
    polars_bio = version("polars-bio")
    assert polars_bio in RANGES_IN_POLARS_BIO, (
        f"polars-bio {polars_bio}: unknown {CRATE} version - read it from the polars-bio "
        f"Cargo.toml at the release tag, update RANGES_IN_POLARS_BIO and the vendored copy"
    )
    manifest = tomllib.loads((BALLISTA_DIR / "vendor" / CRATE / "Cargo.toml").read_text())
    lock = tomllib.loads((BALLISTA_DIR / "Cargo.lock").read_text())
    locked = {package["name"]: package["version"] for package in lock["package"]}
    expected = RANGES_IN_POLARS_BIO[polars_bio]
    for actual in (manifest["package"]["version"], locked[CRATE]):
        assert actual == expected or (expected, actual) in EQUIVALENT_RANGES, (
            f"{CRATE} {actual} is not {expected} and not a declared equivalent"
        )


def test_equivalence_exceptions_are_explicit():
    for pair, reason in EQUIVALENT_RANGES.items():
        assert reason.strip() and ("DataFusion" in reason or "identical" in reason), pair
    assert ("0.24.0", "0.23.0") not in EQUIVALENT_RANGES

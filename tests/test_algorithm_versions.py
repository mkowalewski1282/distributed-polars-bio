"""Ballista i polars-bio liczą tą samą wersją algorytmów przedziałowych (plan 3b-1, etap 3).

polars-bio jest budowany z datafusion-bio-function-ranges z gita (tag w Cargo.toml polars-bio);
Ballista używa zwendorowanej kopii tego crate'a z łatkami widoczności (vendor/PATCH.md). Gdy wersje
się rozjadą, porównanie silników miesza efekt silnika z efektem wersji biblioteki — np. upstream
poprawił między 0.18.0 a 0.22.2 przedziały jednozasadowe w nearest i coverage."""

from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
CRATE = "datafusion-bio-function-ranges"

#: Wersja crate'a algorytmów, z którą zbudowano daną wersję polars-bio — Cargo.toml polars-bio pod
#: tagiem wydania (0.36.0: tag = "v0.22.2"). Nowa wersja polars-bio = nowy wpis tutaj i, gdy wersja
#: crate'a się zmienia, aktualizacja vendora według vendor/PATCH.md.
RANGES_IN_POLARS_BIO = {"0.36.0": "0.22.2"}


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
    assert (manifest["package"]["version"], locked[CRATE]) == (expected, expected)

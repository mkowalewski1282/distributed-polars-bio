"""Pobranie i weryfikacja zbioru databio-8p (polars-bio-bench).

Źródło i układ katalogów jak w polars-bio-bench (`conf/common.yaml`,
`src/utils.py::prepare_datatests`): archiwum `databio-8p.zip` z Google Drive,
rozpakowane do `$BENCH_DATA_ROOT/databio-8p/<zbiór>/part-*.parquet`.
Świadome różnice względem tamtego mechanizmu:

- `requests` z bezpośrednim adresem pobierania zamiast `gdown` — bez nowej
  zależności. Gdy Google zwróci stronę HTML (limit pobrań), błąd z instrukcją
  pobrania ręcznego; archiwum położone ręcznie jako
  `$BENCH_DATA_ROOT/databio-8p.zip` jest używane bez ponownego pobierania.
- Rozpakowywane są TYLKO pliki danych. Archiwum zawiera też `__MACOSX/`
  z plikami `._part-*.parquet` (rozszerzenie .parquet, ale to nie Parquet),
  sumy `.crc` i znaczniki `_SUCCESS` Sparka.
- Rozpakowanie do katalogu tymczasowego i podmiana dopiero po sukcesie —
  przerwane rozpakowanie nie zostawia katalogu, który wygląda na kompletny,
  i nie niszczy poprzedniej kopii.
- Weryfikacja: 8 plików na zbiór, schemat, liczba wierszy zgodna ze
  specyfikacją (sekcja 4).

Użycie: python -m bench.data.download [--root KATALOG]
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import zipfile
from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from bench.data.datasets import DATASETS, EXPECTED_ROWS_K, data_dir

GDRIVE_ID = "1Sj7nTB5gCUq9nbeQOg4zzS4tKO37M5Nd"
ZIP_URL = (
    f"https://drive.usercontent.google.com/download?id={GDRIVE_ID}&export=download&confirm=t"
)
ZIP_BYTES = 1_208_716_701
PARTS_PER_DATASET = 8
EXPECTED_SCHEMA = pa.schema(
    [("contig", pa.string()), ("pos_start", pa.int32()), ("pos_end", pa.int32())]
)
ROW_TOLERANCE = 1000

_PART = re.compile(r"databio-8p/([^/]+)/(part-[^/]+\.parquet)")


def expected_rows() -> dict[str, int]:
    """Oczekiwane liczby wierszy (nazwa katalogu -> wiersze), ze specyfikacji."""
    return {DATASETS[i]: k * 1000 for i, k in EXPECTED_ROWS_K.items()}


def extract_parts(zip_path: Path, dest: Path) -> None:
    """Rozpakowuje wyłącznie `databio-8p/<zbiór>/part-*.parquet` do `dest/<zbiór>/`."""
    names = set(DATASETS.values())
    tmp = dest.with_name(dest.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for info in zf.infolist():
                m = _PART.fullmatch(info.filename)
                if m is None or m.group(1) not in names:
                    continue
                out = tmp / m.group(1) / m.group(2)
                out.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, open(out, "wb") as dst:
                    shutil.copyfileobj(src, dst, 1 << 20)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    shutil.rmtree(dest, ignore_errors=True)
    tmp.rename(dest)


def _schema_text(schema: pa.Schema) -> str:
    return ", ".join(f"{f.name}: {f.type}" for f in schema)


def verify(
    dest: Path, expected: Mapping[str, int], tolerance: int = ROW_TOLERANCE
) -> list[str]:
    """Lista problemów (pusta = zbiór kompletny). Czyta tylko stopki plików."""
    problems: list[str] = []
    for name, rows_expected in expected.items():
        d = dest / name
        parts = sorted(d.glob("part-*.parquet")) if d.is_dir() else []
        if len(parts) != PARTS_PER_DATASET:
            problems.append(
                f"{name}: {len(parts)} plików part-*.parquet zamiast {PARTS_PER_DATASET}"
            )
            continue
        rows = 0
        for p in parts:
            try:
                pf = pq.ParquetFile(p)
            except Exception as e:  # uszkodzony albo nie-Parquet
                problems.append(f"{name}/{p.name}: nieczytelny Parquet ({e})")
                break
            if not pf.schema_arrow.equals(EXPECTED_SCHEMA):
                problems.append(
                    f"{name}/{p.name}: schemat {_schema_text(pf.schema_arrow)} "
                    f"zamiast {_schema_text(EXPECTED_SCHEMA)}"
                )
                break
            rows += pf.metadata.num_rows
        else:
            if abs(rows - rows_expected) > tolerance:
                problems.append(
                    f"{name}: {rows} wierszy, oczekiwano {rows_expected} ± {tolerance}"
                )
    return problems


def summarize(dest: Path) -> list[tuple[int, str, int, int]]:
    """(id, zbiór, liczba plików, liczba wierszy) dla każdego zbioru."""
    out = []
    for idx, name in DATASETS.items():
        files = sorted((dest / name).glob("part-*.parquet"))
        rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
        out.append((idx, name, len(files), rows))
    return out


def download(url: str, target: Path, expected_bytes: int, timeout: float = 60) -> None:
    """Pobiera strumieniowo do `target.part`; nazwa docelowa dopiero po sprawdzeniu rozmiaru."""
    part = target.with_name(target.name + ".part")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        if r.headers.get("Content-Type", "").startswith("text/html"):
            raise RuntimeError(
                "Google Drive zwrócił stronę HTML zamiast archiwum (limit pobrań albo zmiana "
                f"adresu). Pobierz ręcznie https://drive.google.com/uc?id={GDRIVE_ID} "
                f"i zapisz jako {target}"
            )
        with open(part, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    size = part.stat().st_size
    if size != expected_bytes:
        part.unlink()
        raise RuntimeError(
            f"pobrano {size} B zamiast {expected_bytes} B — archiwum niekompletne"
        )
    part.rename(target)


def ensure_dataset(
    root: Path | None = None,
    *,
    url: str = ZIP_URL,
    expected_bytes: int = ZIP_BYTES,
    expected: Mapping[str, int] | None = None,
) -> Path:
    """Zapewnia kompletny zbiór w `data_dir(root)`; zwraca jego katalog."""
    expected = expected_rows() if expected is None else expected
    dest = data_dir(root)
    if dest.is_dir() and not verify(dest, expected):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    zip_path = dest.parent / f"{dest.name}.zip"
    if not (zip_path.is_file() and zip_path.stat().st_size == expected_bytes):
        download(url, zip_path, expected_bytes)
    extract_parts(zip_path, dest)
    problems = verify(dest, expected)
    if problems:
        raise RuntimeError(
            "zbiór po rozpakowaniu jest niepoprawny (archiwum zostaje do wglądu):\n"
            + "\n".join(problems)
        )
    zip_path.unlink()
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pobiera i weryfikuje zbiór databio-8p.")
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="katalog danych (domyślnie $BENCH_DATA_ROOT albo ~/bench_data)",
    )
    args = parser.parse_args(argv)
    try:
        dest = ensure_dataset(args.root)
    except (RuntimeError, OSError, requests.RequestException, zipfile.BadZipFile) as e:
        print(f"BŁĄD: {e}", file=sys.stderr)
        return 1
    print(f"{'id':>2}  {'zbiór':<18} {'pliki':>5} {'wiersze':>12}")
    for idx, name, files, rows in summarize(dest):
        print(f"{idx:>2}  {name:<18} {files:>5} {rows:>12}")
    print(f"Dane: {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

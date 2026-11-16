"""Download and verification of the databio-8p dataset (polars-bio-bench).

The source and directory layout follow polars-bio-bench (`conf/common.yaml`,
`src/utils.py::prepare_datatests`): the `databio-8p.zip` archive from Google Drive,
extracted to `$BENCH_DATA_ROOT/databio-8p/<dataset>/part-*.parquet`.
Deliberate differences from that mechanism:

- `requests` with a direct download URL instead of `gdown` — no new dependency. When Google
  returns an HTML page (download quota), the error carries manual download instructions; an
  archive placed by hand as `$BENCH_DATA_ROOT/databio-8p.zip` is used without downloading again.
- ONLY the data files are extracted. The archive also contains `__MACOSX/` with
  `._part-*.parquet` files (a .parquet extension, but not Parquet), `.crc` checksums and
  Spark's `_SUCCESS` markers.
- Extraction goes to a temporary directory and is swapped in only after success — an
  interrupted extraction does not leave a directory that looks complete, and does not destroy
  the previous copy.
- Verification: 8 files per dataset, the schema, and the row count matching the expected one
  (bench.data.datasets.EXPECTED_ROWS_K).

Usage: python -m bench.data.download [--root DIRECTORY]
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
    """Expected row counts (directory name -> rows), from EXPECTED_ROWS_K."""
    return {DATASETS[i]: k * 1000 for i, k in EXPECTED_ROWS_K.items()}


def extract_parts(zip_path: Path, dest: Path) -> None:
    """Extract only `databio-8p/<dataset>/part-*.parquet` to `dest/<dataset>/`."""
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
    """List of problems (empty = the dataset is complete). Reads only the file footers."""
    problems: list[str] = []
    for name, rows_expected in expected.items():
        d = dest / name
        parts = sorted(d.glob("part-*.parquet")) if d.is_dir() else []
        if len(parts) != PARTS_PER_DATASET:
            problems.append(
                f"{name}: {len(parts)} part-*.parquet files instead of {PARTS_PER_DATASET}"
            )
            continue
        rows = 0
        for p in parts:
            try:
                pf = pq.ParquetFile(p)
            except Exception as e:  # corrupted or not Parquet
                problems.append(f"{name}/{p.name}: unreadable Parquet ({e})")
                break
            if not pf.schema_arrow.equals(EXPECTED_SCHEMA):
                problems.append(
                    f"{name}/{p.name}: schema {_schema_text(pf.schema_arrow)} "
                    f"instead of {_schema_text(EXPECTED_SCHEMA)}"
                )
                break
            rows += pf.metadata.num_rows
        else:
            if abs(rows - rows_expected) > tolerance:
                problems.append(
                    f"{name}: {rows} rows, expected {rows_expected} ± {tolerance}"
                )
    return problems


def summarize(dest: Path) -> list[tuple[int, str, int, int]]:
    """(id, dataset, file count, row count) for every dataset."""
    out = []
    for idx, name in DATASETS.items():
        files = sorted((dest / name).glob("part-*.parquet"))
        rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
        out.append((idx, name, len(files), rows))
    return out


def download(url: str, target: Path, expected_bytes: int, timeout: float = 60) -> None:
    """Stream the download to `target.part`; the final name is set only after the size check."""
    part = target.with_name(target.name + ".part")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        if r.headers.get("Content-Type", "").startswith("text/html"):
            raise RuntimeError(
                "Google Drive returned an HTML page instead of the archive (download limit or a changed "
                f"address). Download it manually from https://drive.google.com/uc?id={GDRIVE_ID} "
                f"and save it as {target}"
            )
        with open(part, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    size = part.stat().st_size
    if size != expected_bytes:
        part.unlink()
        raise RuntimeError(
            f"downloaded {size} B instead of {expected_bytes} B - incomplete archive"
        )
    part.rename(target)


def ensure_dataset(
    root: Path | None = None,
    *,
    url: str = ZIP_URL,
    expected_bytes: int = ZIP_BYTES,
    expected: Mapping[str, int] | None = None,
) -> Path:
    """Ensure a complete dataset in `data_dir(root)`; return its directory."""
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
            "the extracted dataset is invalid (the archive is kept for inspection):\n"
            + "\n".join(problems)
        )
    zip_path.unlink()
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Downloads and verifies the databio-8p dataset.")
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="data directory (default: $BENCH_DATA_ROOT or ~/bench_data)",
    )
    args = parser.parse_args(argv)
    try:
        dest = ensure_dataset(args.root)
    except (RuntimeError, OSError, requests.RequestException, zipfile.BadZipFile) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    print(f"{'id':>2}  {'dataset':<18} {'files':>5} {'rows':>12}")
    for idx, name, files, rows in summarize(dest):
        print(f"{idx:>2}  {name:<18} {files:>5} {rows:>12}")
    print(f"Data: {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

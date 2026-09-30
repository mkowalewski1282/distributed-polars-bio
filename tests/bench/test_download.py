"""Pobieranie i weryfikacja databio-8p (plan 2, Zadanie 2) — na syntetycznym
archiwum o układzie databio-8p.zip i lokalnym serwerze HTTP, bez sieci."""

from __future__ import annotations

import threading
import zipfile
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bench.data import download as dl

NAMES = ("exons", "fBrain-DS14718")
ROWS_PER_PART = 2
EXPECTED = {name: ROWS_PER_PART * 8 for name in NAMES}


def _table(n: int, start: int, pos_type=pa.int32()) -> pa.Table:
    return pa.table(
        {
            "contig": pa.array([f"chr{i % 3 + 1}" for i in range(n)], pa.string()),
            "pos_start": pa.array(list(range(start, start + n)), pos_type),
            "pos_end": pa.array(list(range(start + 10, start + n + 10)), pos_type),
        }
    )


def _parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def _make_zip(path: Path, pos_type=pa.int32()) -> None:
    """Archiwum o układzie databio-8p.zip: dane i śmieci, które trzeba pominąć."""
    with zipfile.ZipFile(path, "w") as zf:
        for name in NAMES:
            for i in range(8):
                fname = f"part-{i:05d}-abc-c000.snappy.parquet"
                data = _parquet_bytes(_table(ROWS_PER_PART, i * 100, pos_type))
                zf.writestr(f"databio-8p/{name}/{fname}", data)
                zf.writestr(f"databio-8p/{name}/.{fname}.crc", b"crc")
                zf.writestr(f"__MACOSX/databio-8p/{name}/._{fname}", b"\x00\x05\x16\x07Mac")
            zf.writestr(f"databio-8p/{name}/_SUCCESS", b"")
            zf.writestr(f"databio-8p/{name}/._SUCCESS.crc", b"crc")


def _no_download(*args, **kwargs):
    raise AssertionError("pobieranie nie powinno być potrzebne")


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@pytest.fixture
def http_dir(tmp_path):
    www = tmp_path / "www"
    www.mkdir()
    server = HTTPServer(("127.0.0.1", 0), partial(_QuietHandler, directory=str(www)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield www, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


@pytest.fixture
def extracted(tmp_path) -> Path:
    zip_path = tmp_path / "databio-8p.zip"
    _make_zip(zip_path)
    dest = tmp_path / "databio-8p"
    dl.extract_parts(zip_path, dest)
    return dest


def test_extract_keeps_only_data_parts(extracted, tmp_path):
    assert sorted(p.name for p in extracted.iterdir()) == sorted(NAMES)
    for name in NAMES:
        files = sorted(p.name for p in (extracted / name).iterdir())
        assert files == [f"part-{i:05d}-abc-c000.snappy.parquet" for i in range(8)]
        for f in files:
            pq.ParquetFile(extracted / name / f)  # każdy plik to prawdziwy Parquet
    assert not (tmp_path / "databio-8p.tmp").exists()


def test_verify_accepts_complete_dataset(extracted):
    assert dl.verify(extracted, EXPECTED, tolerance=0) == []


def test_verify_reports_missing_part(extracted):
    next((extracted / "exons").glob("part-*.parquet")).unlink()
    problems = dl.verify(extracted, EXPECTED, tolerance=0)
    assert len(problems) == 1 and "exons" in problems[0] and "7 plików" in problems[0]


def test_verify_reports_missing_dataset(extracted):
    problems = dl.verify(extracted, {**EXPECTED, "ex-anno": 16}, tolerance=0)
    assert len(problems) == 1 and "ex-anno" in problems[0] and "0 plików" in problems[0]


def test_verify_reports_wrong_schema(tmp_path):
    zip_path = tmp_path / "databio-8p.zip"
    _make_zip(zip_path, pos_type=pa.int64())
    dest = tmp_path / "databio-8p"
    dl.extract_parts(zip_path, dest)
    problems = dl.verify(dest, EXPECTED, tolerance=0)
    assert len(problems) == 2 and all("schemat" in p for p in problems)


def test_verify_reports_wrong_row_count(extracted):
    problems = dl.verify(extracted, {**EXPECTED, "exons": 17}, tolerance=0)
    assert len(problems) == 1 and "exons" in problems[0] and "wierszy" in problems[0]


def test_failed_extraction_keeps_previous_dataset(tmp_path):
    zip_path = tmp_path / "databio-8p.zip"
    _make_zip(zip_path)
    data = bytearray(zip_path.read_bytes())
    data[data.index(b"PAR1") + 16] ^= 0xFF  # uszkodzenie wnętrza pierwszego pliku danych
    zip_path.write_bytes(bytes(data))
    dest = tmp_path / "databio-8p"
    dest.mkdir()
    (dest / "znacznik").write_text("poprzednia wersja")
    with pytest.raises(zipfile.BadZipFile):
        dl.extract_parts(zip_path, dest)
    assert (dest / "znacznik").read_text() == "poprzednia wersja"
    assert not (tmp_path / "databio-8p.tmp").exists()


def test_download_rejects_html_page(http_dir, tmp_path):
    www, base = http_dir
    (www / "strona.html").write_text("<html>Quota exceeded</html>")
    target = tmp_path / "x.zip"
    with pytest.raises(RuntimeError, match="HTML"):
        dl.download(f"{base}/strona.html", target, expected_bytes=10)
    assert not target.exists()
    assert not (tmp_path / "x.zip.part").exists()


def test_download_rejects_truncated_file(http_dir, tmp_path):
    www, base = http_dir
    (www / "dane.bin").write_bytes(b"x" * 100)
    target = tmp_path / "x.zip"
    with pytest.raises(RuntimeError, match="niekompletne"):
        dl.download(f"{base}/dane.bin", target, expected_bytes=200)
    assert not target.exists()
    assert not (tmp_path / "x.zip.part").exists()


def test_download_saves_complete_file(http_dir, tmp_path):
    www, base = http_dir
    (www / "dane.bin").write_bytes(b"x" * 100)
    target = tmp_path / "x.zip"
    dl.download(f"{base}/dane.bin", target, expected_bytes=100)
    assert target.read_bytes() == b"x" * 100


def test_ensure_dataset_downloads_extracts_and_removes_zip(http_dir, tmp_path):
    www, base = http_dir
    _make_zip(www / "databio-8p.zip")
    size = (www / "databio-8p.zip").stat().st_size
    root = tmp_path / "root"
    dest = dl.ensure_dataset(
        root, url=f"{base}/databio-8p.zip", expected_bytes=size, expected=EXPECTED
    )
    assert dest == root / "databio-8p"
    assert dl.verify(dest, EXPECTED, tolerance=0) == []
    assert not (root / "databio-8p.zip").exists()


def test_ensure_dataset_skips_download_when_complete(tmp_path, monkeypatch):
    zip_path = tmp_path / "src.zip"
    _make_zip(zip_path)
    root = tmp_path / "root"
    dl.extract_parts(zip_path, root / "databio-8p")
    monkeypatch.setattr(dl, "download", _no_download)
    assert dl.ensure_dataset(root, expected=EXPECTED) == root / "databio-8p"


def test_ensure_dataset_reuses_downloaded_zip(tmp_path, monkeypatch):
    """Archiwum pobrane ręcznie (np. przez przeglądarkę) wystarcza."""
    root = tmp_path / "root"
    root.mkdir()
    zip_path = root / "databio-8p.zip"
    _make_zip(zip_path)
    monkeypatch.setattr(dl, "download", _no_download)
    dest = dl.ensure_dataset(root, expected_bytes=zip_path.stat().st_size, expected=EXPECTED)
    assert dl.verify(dest, EXPECTED, tolerance=0) == []


def test_main_reports_failure_with_exit_code_1(monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise RuntimeError("symulowany błąd")

    monkeypatch.setattr(dl, "ensure_dataset", fail)
    assert dl.main(["--root", "/tmp/nieistotne"]) == 1
    assert "symulowany błąd" in capsys.readouterr().err

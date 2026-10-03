"""Runner polars-bio (specyfikacja 8.1, 8.3): jeden scenariusz w świeżym procesie.

polars-bio czyta pliki Parquet sam — ścieżka jako wzorzec `katalog/*.parquet` (gołego
katalogu nie przyjmuje) — a wynik dostaje jako `datafusion.DataFrame` i konsumuje
strumieniowo (`execute_stream`), licząc sumę kontrolną partia po partii.

Czas: od wywołania operacji do ostatniej partii; bez startu procesu, importów i ustawień.
Liczenie sumy kontrolnej jest jego częścią (specyfikacja 6.4); jego koszt podaje
`extra.checksum_s`.
Wątki: `--threads T` → `target_partitions = T` (pb.POLARS_BIO_MAX_THREADS; domyślnie
polars-bio liczy na 1 partycji). Orkiestrator ustawia też POLARS_MAX_THREADS = T
i przypina proces do rdzeni węzłów (taskset).

UWAGA (sonda 01.10.2026): przy target_partitions > 1 polars-bio 0.28 liczy merge
i subtract osobno w każdej partycji — wynik jest błędny, gdy przedziały chromosomu leżą
w różnych plikach (znany błąd polars-bio #372, naprawiony w 0.29+, która wymaga Pythona
≥ 3.11). Wynik wzorcowy serii liczy się dlatego na 1 partycji, a nieważność takich przebiegów
wykrywa suma kontrolna.
"""

from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

from bench.checksum import Checksum
from bench.metrics import peak_rss, reset_peak_rss
from bench.ops import UNARY_OPS, normalize_arrow
from bench.runners.common import main_guard, parse_scenario, report_line, require_paths, scenario_parser

PROG = "polars_bio_runner"


def source(path: Path) -> str:
    """Katalog plików Parquet → wzorzec `katalog/*.parquet`; plik → bez zmian."""
    return str(path / "*.parquet") if path.is_dir() else str(path)


def run(op: str, left: Path, right: Path | None, cols: tuple[str, str, str], threads: int) -> str:
    require_paths(left, right)
    import polars_bio as pb

    # Ścieżki nie niosą metadanych układu współrzędnych; dane databio-8p są 0-based.
    warnings.filterwarnings("ignore", message="Coordinate system metadata is missing")
    pb.set_option("datafusion.bio.coordinate_system_zero_based", True)
    pb.set_option(pb.POLARS_BIO_MAX_THREADS, threads)
    checksum = Checksum(op)
    reset_peak_rss()
    t0 = time.perf_counter()
    if op in UNARY_OPS:
        df = pb.merge(source(left), cols=list(cols), output_type="datafusion.DataFrame")
    else:
        df = getattr(pb, op)(
            source(left), source(right), cols1=list(cols), cols2=list(cols),
            output_type="datafusion.DataFrame",
        )
    checksum_s = 0.0
    for batch in df.execute_stream():
        data = normalize_arrow(op, batch.to_pyarrow(), cols)
        t = time.perf_counter()
        checksum.update(data)
        checksum_s += time.perf_counter() - t
    t_total_s = time.perf_counter() - t0
    return report_line(
        rows=checksum.rows,
        checksum=checksum.hex(),
        t_total_s=t_total_s,
        peak_rss_bytes=peak_rss(),
        extra={
            "target_partitions": int(pb.get_option(pb.POLARS_BIO_MAX_THREADS)),
            "checksum_s": checksum_s,
        },
    )


def main(argv: list[str] | None = None) -> int:
    parser = scenario_parser(PROG, "One scenario in polars-bio; result: one JSON line.")
    parser.add_argument("--threads", type=int, required=True, help="target_partitions polars-bio (≥ 1)")
    args = parse_scenario(parser, argv)
    if args.threads < 1:
        parser.error("--threads: expected an integer ≥ 1")
    return main_guard(PROG, lambda: run(args.op, args.left, args.right, args.cols, args.threads))


if __name__ == "__main__":
    sys.exit(main())

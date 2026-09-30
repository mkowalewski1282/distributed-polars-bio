"""Operacje benchmarku i wspólny schemat ich wyników (specyfikacja, sekcja 8.4).

Każdy silnik zwraca wynik pod innymi nazwami kolumn. Porównania (a w kolejnym
etapie suma kontrolna) działają na schemacie znormalizowanym poniżej, na
multizbiorze wierszy — niezależnie od kolejności wierszy i szerokości typów.
"""

from __future__ import annotations

from collections import Counter

import polars as pl

OPS = ("overlap", "nearest", "coverage", "merge", "subtract")
UNARY_OPS = frozenset({"merge"})

#: Kolumny wyniku w schemacie znormalizowanym, w tej kolejności.
OUTPUT_COLUMNS: dict[str, tuple[str, ...]] = {
    "overlap": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"),
    "nearest": ("chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2", "distance"),
    "coverage": ("chrom", "start", "end", "coverage"),
    "merge": ("chrom", "start", "end", "n_intervals"),
    "subtract": ("chrom", "start", "end"),
}

#: Kolumny wchodzące do porównania. `nearest` bez identyfikacji sąsiada: przy
#: remisach (kilku kandydatów w tej samej odległości) silniki wybierają różnie.
KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    **OUTPUT_COLUMNS,
    "nearest": ("chrom_1", "start_1", "end_1", "distance"),
}


def polars_bio_names(op: str, cols: tuple[str, str, str]) -> dict[str, str]:
    """Nazwy kolumn wyniku polars-bio 0.28 -> schemat znormalizowany."""
    c, s, e = cols
    if op in ("overlap", "nearest"):
        names = {
            f"{c}_1": "chrom_1", f"{s}_1": "start_1", f"{e}_1": "end_1",
            f"{c}_2": "chrom_2", f"{s}_2": "start_2", f"{e}_2": "end_2",
        }
        if op == "nearest":
            names["distance"] = "distance"
        return names
    names = {c: "chrom", s: "start", e: "end"}
    if op == "coverage":
        names["coverage"] = "coverage"
    elif op == "merge":
        names["n_intervals"] = "n_intervals"
    return names


def normalize_polars_bio(op: str, df: pl.DataFrame, cols: tuple[str, str, str]) -> pl.DataFrame:
    names = polars_bio_names(op, cols)
    missing = [src for src in names if src not in df.columns]
    if missing:
        raise KeyError(f"{op}: wynik polars-bio nie ma kolumn {missing}; ma {df.columns}")
    return df.select([pl.col(src).alias(dst) for src, dst in names.items()])


def row_multiset(op: str, df) -> Counter:
    """Multizbiór wierszy po kolumnach klucza (polars albo pandas; NaN = brak)."""
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df, nan_to_null=True)
    key = KEY_COLUMNS[op]
    missing = [k for k in key if k not in df.columns]
    if missing:
        raise KeyError(f"{op}: brak kolumn {missing}; są {df.columns}")
    exprs = [
        pl.col(k).cast(pl.Utf8) if k.startswith("chrom") else pl.col(k).cast(pl.Int64)
        for k in key
    ]
    return Counter(df.select(exprs).iter_rows())


def describe_diff(expected: Counter, actual: Counter, limit: int = 5) -> str:
    """Krótki opis różnicy dwóch multizbiorów — liczności i kilka przykładów."""
    missing = expected - actual
    extra = actual - expected
    return (
        f"brakuje {sum(missing.values())} wierszy, np. {list(missing)[:limit]}; "
        f"nadmiarowych {sum(extra.values())}, np. {list(extra)[:limit]}"
    )

"""Row multisets: results compared independently of row order and integer widths."""

from __future__ import annotations

from collections import Counter

import polars as pl

from sail_genomics.schema import OUTPUT_COLUMNS

#: Columns that enter the comparison. `nearest` without the neighbour's identity: on ties
#: (several candidates at the same distance) the choice of the neighbour is not defined.
KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    **OUTPUT_COLUMNS,
    "nearest": ("chrom_1", "start_1", "end_1", "distance"),
}


def row_multiset(op: str, data) -> Counter:
    """Multiset of rows over the key columns of a polars DataFrame or an Arrow table."""
    df = data if isinstance(data, pl.DataFrame) else pl.from_arrow(data)
    key = KEY_COLUMNS[op]
    exprs = [
        pl.col(k).cast(pl.Utf8) if k.startswith("chrom") else pl.col(k).cast(pl.Int64)
        for k in key
    ]
    return Counter(df.select(exprs).iter_rows())

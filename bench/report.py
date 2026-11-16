"""Series report: a table of medians and plots of time against P.

Usage: python -m bench.report <series directory>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display; must come before importing pyplot
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

from bench.results import SCHEMA, to_parquet  # noqa: E402

#: A fixed colour and marker shape per variant (the colour follows the entity).
VARIANT_STYLE = {
    "polars_bio": ("#2a78d6", "o"),
    "ballista": ("#eb6834", "s"),
    "sail": ("#1baf7a", "^"),
}
VARIANT_ORDER = list(VARIANT_STYLE)
SURFACE, INK, INK_MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"


def load_runs(series_dir: Path) -> pl.DataFrame:
    """Load runs.parquet; for an interrupted series (runs.jsonl only) convert it using the results schema."""
    parquet = series_dir / "runs.parquet"
    jsonl = series_dir / "runs.jsonl"
    if not parquet.exists():
        if not jsonl.exists():
            raise FileNotFoundError(f"no runs.parquet or runs.jsonl in {series_dir}")
        # Polars silently drops unknown fields, so we check for the old schema (n_nodes) ourselves.
        for line in jsonl.read_text(encoding="utf-8").splitlines():
            unknown = set(json.loads(line)) - set(SCHEMA) if line.strip() else set()
            if unknown:
                raise ValueError(
                    f"{jsonl}: does not match the current results schema (unknown fields {sorted(unknown)})"
                )
        to_parquet(jsonl, parquet)
    df = pl.read_parquet(parquet)
    if df.schema != pl.Schema(SCHEMA):
        raise ValueError(f"{parquet}: does not match the current results schema (old series?)")
    return df


def summarize(runs: pl.DataFrame) -> pl.DataFrame:
    """Medians of the valid measured runs per cell (op, pair, variant, P)."""
    measured = runs.filter(~pl.col("is_warmup") & ~pl.col("is_control") & ~pl.col("is_reference"))
    keys = ["op", "pair", "variant", "parallelism"]
    valid_t = pl.col("t_total_s").filter(pl.col("valid"))
    reasons = (
        measured.filter(~pl.col("valid") & pl.col("invalid_reason").is_not_null())
        .group_by(keys)
        .agg(pl.col("invalid_reason").mode().sort().first().alias("top_reason"))
    )
    cells = (
        measured.group_by(keys)
        .agg(
            pl.col("smt").drop_nulls().any().alias("smt"),
            pl.col("valid").sum().cast(pl.Int64).alias("n_valid"),
            valid_t.median().alias("median_s"),
            valid_t.min().alias("min_s"),
            valid_t.max().alias("max_s"),
        )
        .join(reasons, on=keys, how="left")
        .with_columns(
            pl.when(pl.col("n_valid") == 0).then(pl.col("top_reason")).otherwise(None).alias("missing_reason")
        )
        .drop("top_reason")
    )
    base_self = (
        cells.filter(pl.col("parallelism") == 1)
        .select("op", "pair", "variant", pl.col("median_s").alias("_base_self"))
    )
    base_pb = (
        cells.filter((pl.col("variant") == "polars_bio") & (pl.col("parallelism") == 1))
        .select("op", "pair", pl.col("median_s").alias("_base_pb"))
    )
    return (
        cells.join(base_self, on=["op", "pair", "variant"], how="left")
        .join(base_pb, on=["op", "pair"], how="left")
        .with_columns(
            (pl.col("_base_self") / pl.col("median_s")).alias("speedup_self"),
            (pl.col("_base_pb") / pl.col("median_s")).alias("speedup_vs_polars_bio_1"),
        )
        .select(
            "op", "pair", "variant", "parallelism", "smt", "n_valid", "median_s", "min_s", "max_s",
            "speedup_self", "speedup_vs_polars_bio_1", "missing_reason",
        )
        .sort(
            "op", "pair",
            pl.col("variant").replace_strict({v: i for i, v in enumerate(VARIANT_ORDER)}, default=99),
            "parallelism",
        )
    )


def _fmt(x: float | None, fmt_spec: str) -> str:
    return "-" if x is None else format(x, fmt_spec)


def _markdown(summary: pl.DataFrame) -> str:
    out = ["# Series summary", ""]
    out.append(
        "Times are medians of valid measured runs (`t_total_s`, seconds). "
        "A trailing `*` in the P column marks a point whose CPUs include SMT siblings "
        "(hollow marker in the plots). polars-bio at P = 1 is the single-threaded reference point."
    )
    for (op, pair), part in summary.group_by(["op", "pair"], maintain_order=True):
        out += ["", f"## {op} {pair}", ""]
        out.append("| variant | P | n valid | median [s] | min [s] | max [s] | speedup (self) | speedup vs polars-bio P=1 | note |")
        out.append("|---|---|---|---|---|---|---|---|---|")
        for r in part.iter_rows(named=True):
            p = f"{r['parallelism']}{'*' if r['smt'] else ''}"
            out.append(
                f"| {r['variant']} | {p} | {r['n_valid']} | {_fmt(r['median_s'], '.3f')} | "
                f"{_fmt(r['min_s'], '.3f')} | {_fmt(r['max_s'], '.3f')} | "
                f"{_fmt(r['speedup_self'], '.2f')} | {_fmt(r['speedup_vs_polars_bio_1'], '.2f')} | "
                f"{r['missing_reason'] or ''} |"
            )
    return "\n".join(out) + "\n"


def _plot(part: pl.DataFrame, op: str, pair: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6, 4), dpi=150, facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    for variant, vpart in part.group_by("variant", maintain_order=True):
        variant = variant[0]
        color, marker = VARIANT_STYLE.get(variant, ("#6250d6", "D"))
        pts = vpart.filter(pl.col("median_s").is_not_null()).sort("parallelism")
        if pts.is_empty():
            continue
        xs, ys = pts["parallelism"].to_list(), pts["median_s"].to_list()
        ax.plot(xs, ys, color=color, linewidth=2, zorder=2)
        for x, y, smt in zip(xs, ys, pts["smt"].to_list()):
            ax.plot(
                [x], [y], marker=marker, markersize=8, color=color, linestyle="none", zorder=3,
                markerfacecolor=SURFACE if smt else color, markeredgewidth=1.8,
            )
    handles = [
        Line2D([], [], color=c, marker=m, markersize=8, linewidth=2, label=v)
        for v, (c, m) in VARIANT_STYLE.items()
        if v in part["variant"].to_list()
    ]
    handles.append(
        Line2D([], [], color=INK_MUTED, marker="o", markersize=8, linestyle="none",
               markerfacecolor=SURFACE, label="SMT siblings used")
    )
    ax.legend(handles=handles, frameon=False, labelcolor=INK_MUTED, fontsize=9)
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8], labels=["1", "2", "4", "8"])
    ax.minorticks_off()
    ax.set_ylim(bottom=0)
    ax.set_xlabel("parallelism P", color=INK_MUTED)
    ax.set_ylabel("median t_total [s]", color=INK_MUTED)
    ax.set_title(f"{op} {pair}", color=INK, loc="left", fontsize=11)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_MUTED)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def write_report(series_dir: Path) -> list[Path]:
    """Write summary.md, summary.csv and plots/<op>_<pair>.png; return the list of files."""
    summary = summarize(load_runs(series_dir))
    md, csv = series_dir / "summary.md", series_dir / "summary.csv"
    md.write_text(_markdown(summary), encoding="utf-8")
    summary.write_csv(csv)
    plots = series_dir / "plots"
    plots.mkdir(exist_ok=True)
    written = [md, csv]
    for (op, pair), part in summary.group_by(["op", "pair"], maintain_order=True):
        path = plots / f"{op}_{pair}.png"
        _plot(part, op, pair, path)
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.report", description="Median table and time-vs-P plots for a series.")
    ap.add_argument("series_dir", type=Path, help="series directory (with runs.parquet or runs.jsonl)")
    args = ap.parse_args(argv)
    for path in write_report(args.series_dir):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

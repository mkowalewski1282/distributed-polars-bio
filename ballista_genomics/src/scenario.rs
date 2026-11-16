//! A benchmark scenario -> an SQL query with the result in the normalized schema
//! (the counterpart of `bench/ops.py`).
//!
//! The arguments of the `dist_*` functions mirror the per-operation SQL in `runner.rs` 1:1 - the
//! same strict/weak flags and the same reversed coverage convention - because those are
//! verified by tests against polars-bio. Differences: arbitrary paths and column
//! names, explicit output names, no ORDER BY (the result is consumed as a
//! stream, and comparisons do not depend on row order).

use crate::dist_payload::DistOp;

/// One operation on the given data; `right` only for binary operations.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub op: DistOp,
    pub left: String,
    pub right: Option<String>,
    /// Interval column names: contig, start, end.
    pub cols: [String; 3],
}

/// An SQL string literal (apostrophes doubled - paths can be arbitrary).
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A quoted SQL identifier: column names can be keywords (`end`), and
/// without quotes DataFusion would lowercase capital letters.
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn col(source: &str, alias: &str) -> String {
    format!("{} AS {}", ident(source), ident(alias))
}

impl Scenario {
    pub fn validate(&self) -> Result<(), String> {
        match (self.op, self.right.is_some()) {
            (DistOp::Merge, true) => Err("merge takes a single table (no --right)".to_string()),
            (DistOp::Merge, false) | (_, true) => Ok(()),
            (op, false) => Err(format!("{} requires --right", op.as_str())),
        }
    }

    pub fn sql(&self) -> String {
        let [c, s, e] = &self.cols;
        let cols = format!("{}, {}, {}", lit(c), lit(s), lit(e));
        let left = lit(&self.left);
        let right = lit(self.right.as_deref().unwrap_or_default());
        let side = |prefix: &str, n: u8| {
            [
                col(&format!("{prefix}_{c}"), &format!("chrom_{n}")),
                col(&format!("{prefix}_{s}"), &format!("start_{n}")),
                col(&format!("{prefix}_{e}"), &format!("end_{n}")),
            ]
            .join(", ")
        };
        let own = [col(c, "chrom"), col(s, "start"), col(e, "end")].join(", ");
        match self.op {
            DistOp::Overlap => format!(
                "SELECT {}, {} FROM dist_overlap('df1', {left}, 'df2', {right}, {cols}, 'strict')",
                side("left", 1),
                side("right", 2)
            ),
            // The provider's convention, as in coverage: the left table is indexed (and
            // broadcast), and the result has ONE row per row of the RIGHT table, with the
            // nearest neighbour from the left. pb.nearest(left, right) gives a row per
            // row of the left one - so the sides are swapped: 'df1' (our left) goes in
            // as the right (queried) side, and the _1 columns come from right_*, _2 from left_*.
            // 'strict' as in the other operations: the data is 0-based, half-open. Without it
            // NearestExec computes in the 1-based convention - since v0.22.2 the distance comes
            // out 1 smaller than in polars-bio.
            DistOp::Nearest => format!(
                "SELECT {}, {}, {} FROM dist_nearest('df2', {right}, 'df1', {left}, 1, true, true, {cols}, 'strict')",
                side("right", 1),
                side("left", 2),
                col("distance", "distance")
            ),
            // The reversed convention from the SQL in runner.rs: 'reads' = right, 'targets' = left,
            // so the result is the rows of the left table with their coverage by the right
            // one - like pb.coverage(left, right).
            DistOp::Coverage => format!(
                "SELECT {own}, {} FROM dist_coverage('df2', {right}, 'df1', {left}, {cols}, 'strict')",
                col("coverage", "coverage")
            ),
            DistOp::Merge => format!(
                "SELECT {own}, {} FROM dist_merge('df1', {left}, {cols}, 0, 'strict')",
                col("n_intervals", "n_intervals")
            ),
            DistOp::Subtract => format!(
                "SELECT {own} FROM dist_subtract('df1', {left}, 'df2', {right}, {cols}, 'strict')"
            ),
        }
    }
}

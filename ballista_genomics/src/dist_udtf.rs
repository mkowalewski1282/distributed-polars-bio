//! Rejestracja operacji jako funkcji tabelowych SQL (`dist_overlap(...)` itd.).
//!
//! Jeden typ z polem `op` zamiast typu per operacja — parsowanie argumentów
//! rozgałęzia się po `self.op`, reszta (budowa providera) jest wspólna.

use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

use crate::dist_payload::{Cols, DistOp, DistPayload, TableRef};
use crate::dist_provider::DistBioProvider;

#[derive(Debug)]
pub struct DistTableFunction {
    op: DistOp,
}

impl DistTableFunction {
    pub fn new(op: DistOp) -> Self {
        Self { op }
    }
}

pub fn expect_string_literal(args: &[Expr], idx: usize, fname: &str) -> Result<String> {
    match args.get(idx) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) => Ok(v.clone()),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fname}(): argument {idx} must be a string literal, got: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): missing argument {idx}"
        ))),
    }
}

/// Obowiązkowy argument konwencji współrzędnych: `'strict'` (0-based, półotwarte — dane
/// databio-8p) albo `'weak'` (1-based, zamknięte). Dawniej brak argumentu oznaczał `'weak'`, co
/// ukryło błąd w `nearest` aż do biblioteki v0.22.2 (plan 3b-1) — dlatego bez wartości domyślnej
/// (plan 3b-2).
pub fn expect_filter_mode(args: &[Expr], idx: usize, fname: &str) -> Result<bool> {
    match args.get(idx) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) if v.eq_ignore_ascii_case("strict") => Ok(true),
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) if v.eq_ignore_ascii_case("weak") => Ok(false),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fname}(): argument {idx} must be 'strict' (0-based, half-open) or 'weak' (1-based, \
             closed), got: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): missing argument {idx}: 'strict'|'weak'"
        ))),
    }
}

pub fn expect_i64_literal(args: &[Expr], idx: usize, fname: &str) -> Result<i64> {
    match args.get(idx) {
        Some(Expr::Literal(ScalarValue::Int64(Some(v)), _)) => Ok(*v),
        Some(Expr::Literal(ScalarValue::Int32(Some(v)), _)) => Ok(*v as i64),
        Some(Expr::Literal(ScalarValue::UInt64(Some(v)), _)) => Ok(*v as i64),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fname}(): argument {idx} must be an integer literal, got: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): missing argument {idx}"
        ))),
    }
}

fn parse_merge(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 7 {
        return Err(DataFusionError::Plan(
            "dist_merge() expects: table, csv_path, col_chrom, col_start, col_end, \
             min_dist, 'strict'|'weak'"
                .to_string(),
        ));
    }
    Ok(DistPayload::Merge {
        table: TableRef::new(
            expect_string_literal(args, 0, "dist_merge")?,
            expect_string_literal(args, 1, "dist_merge")?,
        ),
        cols: Cols(
            expect_string_literal(args, 2, "dist_merge")?,
            expect_string_literal(args, 3, "dist_merge")?,
            expect_string_literal(args, 4, "dist_merge")?,
        ),
        min_dist: expect_i64_literal(args, 5, "dist_merge")?,
        strict: expect_filter_mode(args, 6, "dist_merge")?,
    })
}

fn parse_subtract(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 8 {
        return Err(DataFusionError::Plan(
            "dist_subtract() expects: left_table, left_path, right_table, right_path, \
             col_chrom, col_start, col_end, 'strict'|'weak'"
                .to_string(),
        ));
    }
    let cols = Cols(
        expect_string_literal(args, 4, "dist_subtract")?,
        expect_string_literal(args, 5, "dist_subtract")?,
        expect_string_literal(args, 6, "dist_subtract")?,
    );
    Ok(DistPayload::Subtract {
        left: TableRef::new(
            expect_string_literal(args, 0, "dist_subtract")?,
            expect_string_literal(args, 1, "dist_subtract")?,
        ),
        right: TableRef::new(
            expect_string_literal(args, 2, "dist_subtract")?,
            expect_string_literal(args, 3, "dist_subtract")?,
        ),
        lcols: cols.clone(),
        rcols: cols,
        strict: expect_filter_mode(args, 7, "dist_subtract")?,
    })
}

fn expect_bool_literal(args: &[Expr], idx: usize, fname: &str) -> Result<bool> {
    match args.get(idx) {
        Some(Expr::Literal(ScalarValue::Boolean(Some(v)), _)) => Ok(*v),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fname}(): argument {idx} must be a boolean literal, got: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): missing argument {idx}"
        ))),
    }
}

fn parse_nearest(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 11 {
        return Err(DataFusionError::Plan(
            "dist_nearest() expects: left_table, left_path, right_table, right_path, \
             k, include_overlaps, compute_distance, col_chrom, col_start, col_end, \
             'strict'|'weak'"
                .to_string(),
        ));
    }
    let cols = Cols(
        expect_string_literal(args, 7, "dist_nearest")?,
        expect_string_literal(args, 8, "dist_nearest")?,
        expect_string_literal(args, 9, "dist_nearest")?,
    );
    Ok(DistPayload::Nearest {
        left: TableRef::new(
            expect_string_literal(args, 0, "dist_nearest")?,
            expect_string_literal(args, 1, "dist_nearest")?,
        ),
        right: TableRef::new(
            expect_string_literal(args, 2, "dist_nearest")?,
            expect_string_literal(args, 3, "dist_nearest")?,
        ),
        lcols: cols.clone(),
        rcols: cols,
        // Konwencja obowiązkowa (plan 3b-2): dane 0-based wymagają 'strict' — bez niego od v0.22.2
        // odległość wychodziła o 1 mniejsza niż w polars-bio (plan 3b-1).
        strict: expect_filter_mode(args, 10, "dist_nearest")?,
        k: expect_i64_literal(args, 4, "dist_nearest")? as u32,
        include_overlaps: expect_bool_literal(args, 5, "dist_nearest")?,
        compute_distance: expect_bool_literal(args, 6, "dist_nearest")?,
    })
}

fn parse_coverage(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 8 {
        return Err(DataFusionError::Plan(
            "dist_coverage() expects: reads_table, reads_path, targets_table, targets_path, \
             col_chrom, col_start, col_end, 'strict'|'weak'"
                .to_string(),
        ));
    }
    let cols = Cols(
        expect_string_literal(args, 4, "dist_coverage")?,
        expect_string_literal(args, 5, "dist_coverage")?,
        expect_string_literal(args, 6, "dist_coverage")?,
    );
    Ok(DistPayload::Coverage {
        left: TableRef::new(
            expect_string_literal(args, 0, "dist_coverage")?,
            expect_string_literal(args, 1, "dist_coverage")?,
        ),
        right: TableRef::new(
            expect_string_literal(args, 2, "dist_coverage")?,
            expect_string_literal(args, 3, "dist_coverage")?,
        ),
        lcols: cols.clone(),
        rcols: cols,
        strict: expect_filter_mode(args, 7, "dist_coverage")?,
        coverage: true,
    })
}

fn parse_overlap(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 8 {
        return Err(DataFusionError::Plan(
            "dist_overlap() expects: left_table, left_csv, right_table, right_csv, \
             col_chrom, col_start, col_end, 'strict'|'weak'"
                .to_string(),
        ));
    }
    Ok(DistPayload::Overlap {
        left: TableRef::new(
            expect_string_literal(args, 0, "dist_overlap")?,
            expect_string_literal(args, 1, "dist_overlap")?,
        ),
        right: TableRef::new(
            expect_string_literal(args, 2, "dist_overlap")?,
            expect_string_literal(args, 3, "dist_overlap")?,
        ),
        cols: Cols(
            expect_string_literal(args, 4, "dist_overlap")?,
            expect_string_literal(args, 5, "dist_overlap")?,
            expect_string_literal(args, 6, "dist_overlap")?,
        ),
        strict: expect_filter_mode(args, 7, "dist_overlap")?,
    })
}

impl TableFunctionImpl for DistTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let payload = match self.op {
            DistOp::Overlap => parse_overlap(args)?,
            DistOp::Merge => parse_merge(args)?,
            DistOp::Subtract => parse_subtract(args)?,
            DistOp::Nearest => parse_nearest(args)?,
            DistOp::Coverage => parse_coverage(args)?,
        };

        // TableFunctionImpl::call jest synchroniczne, a budowa providera wymaga
        // async — ten sam wzorzec, którego używa sam vendor (table_function.rs).
        let provider = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(DistBioProvider::build(payload))
        })?;

        Ok(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::lit;

    fn merge_args(mode: Option<&str>) -> Vec<Expr> {
        let mut args = vec![lit("t"), lit("p.parquet"), lit("c"), lit("s"), lit("e"), lit(0i64)];
        if let Some(m) = mode {
            args.push(lit(m));
        }
        args
    }

    #[test]
    fn coordinate_mode_is_required() {
        let err = parse_merge(&merge_args(None)).unwrap_err().to_string();
        assert!(err.contains("'strict'|'weak'"), "{err}");
    }

    #[test]
    fn strict_and_weak_are_parsed() {
        assert!(matches!(parse_merge(&merge_args(Some("strict"))).unwrap(), DistPayload::Merge { strict: true, .. }));
        assert!(matches!(parse_merge(&merge_args(Some("weak"))).unwrap(), DistPayload::Merge { strict: false, .. }));
    }

    #[test]
    fn unknown_mode_is_rejected() {
        let err = parse_merge(&merge_args(Some("loose"))).unwrap_err().to_string();
        assert!(err.contains("must be 'strict'"), "{err}");
    }
}

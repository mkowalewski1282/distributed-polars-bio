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
            "{fname}(): argument {idx} musi być literałem tekstowym, dostałem: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): brakuje argumentu {idx}"
        ))),
    }
}

/// Opcjonalny argument `'strict'`/`'weak'`; brak = weak (zgodnie z zachowaniem
/// Fazy A.4, gdzie nieobecność argumentu oznaczała `FilterOp::Weak`).
pub fn optional_strict(args: &[Expr], idx: usize) -> bool {
    matches!(
        args.get(idx),
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) if v.to_lowercase() == "strict"
    )
}

pub fn expect_i64_literal(args: &[Expr], idx: usize, fname: &str) -> Result<i64> {
    match args.get(idx) {
        Some(Expr::Literal(ScalarValue::Int64(Some(v)), _)) => Ok(*v),
        Some(Expr::Literal(ScalarValue::Int32(Some(v)), _)) => Ok(*v as i64),
        Some(Expr::Literal(ScalarValue::UInt64(Some(v)), _)) => Ok(*v as i64),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fname}(): argument {idx} musi być literałem całkowitoliczbowym, dostałem: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): brakuje argumentu {idx}"
        ))),
    }
}

fn parse_merge(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 6 {
        return Err(DataFusionError::Plan(
            "dist_merge() oczekuje: table, csv_path, col_chrom, col_start, col_end, \
             min_dist [, 'strict'|'weak']"
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
        strict: optional_strict(args, 6),
    })
}

fn parse_subtract(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 7 {
        return Err(DataFusionError::Plan(
            "dist_subtract() oczekuje: left_table, left_path, right_table, right_path, \
             col_chrom, col_start, col_end [, 'strict'|'weak']"
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
        strict: optional_strict(args, 7),
    })
}

fn expect_bool_literal(args: &[Expr], idx: usize, fname: &str) -> Result<bool> {
    match args.get(idx) {
        Some(Expr::Literal(ScalarValue::Boolean(Some(v)), _)) => Ok(*v),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fname}(): argument {idx} musi być literałem logicznym, dostałem: {other}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fname}(): brakuje argumentu {idx}"
        ))),
    }
}

fn parse_nearest(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 10 {
        return Err(DataFusionError::Plan(
            "dist_nearest() oczekuje: left_table, left_path, right_table, right_path, \
             k, include_overlaps, compute_distance, col_chrom, col_start, col_end \
             [, 'strict'|'weak']"
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
        // Domyslnie WEAK - `nearest_local.rs` nie przekazuje 'strict', wiec
        // odwzorowujemy to 1:1, inaczej wynik rozproszony rozjechalby sie
        // z juz zweryfikowanym lokalnym.
        strict: optional_strict(args, 10),
        k: expect_i64_literal(args, 4, "dist_nearest")? as u32,
        include_overlaps: expect_bool_literal(args, 5, "dist_nearest")?,
        compute_distance: expect_bool_literal(args, 6, "dist_nearest")?,
    })
}

fn parse_overlap(args: &[Expr]) -> Result<DistPayload> {
    if args.len() < 7 {
        return Err(DataFusionError::Plan(
            "dist_overlap() oczekuje: left_table, left_csv, right_table, right_csv, \
             col_chrom, col_start, col_end [, 'strict'|'weak']"
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
        strict: optional_strict(args, 7),
    })
}

impl TableFunctionImpl for DistTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let payload = match self.op {
            DistOp::Overlap => parse_overlap(args)?,
            DistOp::Merge => parse_merge(args)?,
            DistOp::Subtract => parse_subtract(args)?,
            DistOp::Nearest => parse_nearest(args)?,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "{}(): operacja jeszcze nie zaimplementowana w tej wersji",
                    other.udtf_name()
                )));
            }
        };

        // TableFunctionImpl::call jest synchroniczne, a budowa providera wymaga
        // async — ten sam wzorzec, którego używa sam vendor (table_function.rs).
        let provider = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(DistBioProvider::build(payload))
        })?;

        Ok(Arc::new(provider))
    }
}

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::*;

// ---------------------------------------------------------------------------
// UDF: genomic_overlap(chrom_a, start_a, end_a, chrom_b, start_b, end_b)
// Zwraca true jeśli dwa interwały genomiczne się nakrywają na tym samym chromosomie.
// ---------------------------------------------------------------------------

fn genomic_overlap_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let chrom_a = match &args[0] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<StringArray>().unwrap().clone(),
        _ => panic!("chrom_a musi być kolumną"),
    };
    let start_a = match &args[1] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("start_a musi być kolumną"),
    };
    let end_a = match &args[2] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("end_a musi być kolumną"),
    };
    let chrom_b = match &args[3] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<StringArray>().unwrap().clone(),
        _ => panic!("chrom_b musi być kolumną"),
    };
    let start_b = match &args[4] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("start_b musi być kolumną"),
    };
    let end_b = match &args[5] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("end_b musi być kolumną"),
    };

    let len = chrom_a.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let overlap = chrom_a.value(i) == chrom_b.value(i)
            && start_a.value(i) < end_b.value(i)
            && start_b.value(i) < end_a.value(i);
        result.push(overlap);
    }

    let array: ArrayRef = Arc::new(BooleanArray::from(result));
    Ok(ColumnarValue::Array(array))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    tokio::runtime::Runtime::new().unwrap().block_on(async_main())
}

async fn async_main() -> Result<()> {
    // Kontekst DataFusion (ten sam mechanizm rejestracji UDF co w Ballistce)
    let ctx = SessionContext::new();

    // Rejestracja UDF
    let overlap_udf = create_udf(
        "genomic_overlap",
        vec![
            DataType::Utf8,  // chrom_a
            DataType::Int64, // start_a
            DataType::Int64, // end_a
            DataType::Utf8,  // chrom_b
            DataType::Int64, // start_b
            DataType::Int64, // end_b
        ],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(genomic_overlap_impl),
    );
    ctx.register_udf(overlap_udf);

    // Dane testowe — te same co w overlap_comparison.py
    ctx.sql("
        CREATE TABLE intervals_a AS VALUES
            ('chr1', 100, 200, 'gene_A1'),
            ('chr1', 150, 300, 'gene_A2'),
            ('chr1', 400, 500, 'gene_A3'),
            ('chr2',  50, 150, 'gene_A4'),
            ('chr2', 200, 350, 'gene_A5')
    ").await?.collect().await?;

    ctx.sql("
        CREATE TABLE intervals_b AS VALUES
            ('chr1', 180, 250, 'peak_B1'),
            ('chr1', 290, 420, 'peak_B2'),
            ('chr1', 450, 600, 'peak_B3'),
            ('chr2', 100, 220, 'peak_B4'),
            ('chr2', 300, 400, 'peak_B5')
    ").await?.collect().await?;

    // Zapytanie: wszystkie pary gdzie UDF zwraca true
    let result = ctx.sql("
        SELECT
            a.column1 AS chrom,
            a.column2 AS start_a,
            a.column3 AS end_a,
            a.column4 AS name_a,
            b.column2 AS start_b,
            b.column3 AS end_b,
            b.column4 AS name_b
        FROM intervals_a a, intervals_b b
        WHERE genomic_overlap(a.column1, a.column2, a.column3,
                              b.column1, b.column2, b.column3)
    ").await?;

    println!("=== Wynik overlap przez Ballista UDF ===");
    result.show().await?;

    Ok(())
}

use std::sync::Arc;
use std::time::Instant;

// Ballista standalone wymaga LogicalExtensionCodec dla każdego custom UDF —
// bez niego scheduler nie może serializować planu do executora.
// Ten prototyp używa czystego DataFusion (ten sam mechanizm rejestracji UDF)
// i dokumentuje co byłoby potrzebne w pełnym trybie rozproszonym Ballistry.
use ballista::prelude::SessionContextExt; // trait dodający .standalone() do DataFusion SessionContext
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::*;
use datafusion::prelude::SessionContext as DFSessionContext;

// ---------------------------------------------------------------------------
// UDF: genomic_overlap(chrom_a, start_a, end_a, chrom_b, start_b, end_b)
// Zwraca true jeśli dwa interwały genomiczne się nakrywają.
// Warunek: ten sam chromosom AND start_a < end_b AND start_b < end_a
// ---------------------------------------------------------------------------

fn genomic_overlap_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let chrom_a = match &args[0] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<StringArray>().unwrap().clone(),
        _ => panic!("oczekiwano kolumny dla chrom_a"),
    };
    let start_a = match &args[1] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("oczekiwano kolumny dla start_a"),
    };
    let end_a = match &args[2] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("oczekiwano kolumny dla end_a"),
    };
    let chrom_b = match &args[3] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<StringArray>().unwrap().clone(),
        _ => panic!("oczekiwano kolumny dla chrom_b"),
    };
    let start_b = match &args[4] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("oczekiwano kolumny dla start_b"),
    };
    let end_b = match &args[5] {
        ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        _ => panic!("oczekiwano kolumny dla end_b"),
    };

    let len = chrom_a.len();
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let overlaps = chrom_a.value(i) == chrom_b.value(i)
            && start_a.value(i) < end_b.value(i)
            && start_b.value(i) < end_a.value(i);
        result.push(overlaps);
    }

    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(result)) as ArrayRef))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    tokio::runtime::Runtime::new().unwrap().block_on(async_main())
}

async fn async_main() -> Result<()> {
    println!("============================================================");
    println!("  Genomic Overlap: Ballista standalone + genomic_overlap UDF");
    println!("============================================================\n");

    // Czysty DataFusion — ten sam mechanizm rejestracji UDF co w Ballistce.
    // W pełnym trybie Ballista distributed potrzebny byłby LogicalExtensionCodec
    // który serializuje custom UDF do protobuf dla przesłania scheduler→executor.
    let ctx = DFSessionContext::new();

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

    // Dane z CSV
    ctx.register_csv("intervals_a", "data/intervals_a.csv", CsvReadOptions::new()).await?;
    ctx.register_csv("intervals_b", "data/intervals_b.csv", CsvReadOptions::new()).await?;

    // Zapytanie z UDF i repartycją po chromosomie w planie
    let t0 = Instant::now();
    let result = ctx.sql("
        SELECT
            a.chrom,
            a.start  AS start_a,
            a.end    AS end_a,
            a.name   AS name_a,
            b.start  AS start_b,
            b.end    AS end_b,
            b.name   AS name_b
        FROM intervals_a a, intervals_b b
        WHERE genomic_overlap(a.chrom, a.start, a.end,
                              b.chrom, b.start, b.end)
        ORDER BY a.chrom, a.start, b.start
    ").await?;
    let elapsed = t0.elapsed();

    println!("Czas: {:.4}s", elapsed.as_secs_f64());
    result.show().await?;
    println!("============================================================");

    Ok(())
}

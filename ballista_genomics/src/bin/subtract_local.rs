use std::time::Instant;

use datafusion::error::Result;
use datafusion::prelude::CsvReadOptions;
use datafusion_bio_function_ranges::create_bio_session;

// Faza C: operacja subtract, poziom lokalny — SubtractProvider też buduje
// customowy, prywatny SubtractExec bezpośrednio (ta sama granica dystrybucji
// co merge/nearest/coverage, patrz ballista_genomics/OPIS.md).

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("  Faza C: subtract (lokalnie, datafusion-bio-function-ranges)");
    println!("============================================================\n");

    let ctx = create_bio_session();

    ctx.register_csv("intervals_a", "data/intervals_a.csv", CsvReadOptions::new())
        .await?;
    ctx.register_csv("intervals_b", "data/intervals_b.csv", CsvReadOptions::new())
        .await?;

    let t0 = Instant::now();
    let df = ctx
        .sql(
            "SELECT * FROM subtract('intervals_a', 'intervals_b', 'chrom', 'start', 'end', 'strict') \
             ORDER BY 1, 2",
        )
        .await?;
    let result = df.collect().await?;
    let elapsed = t0.elapsed();

    println!("Czas: {:.4}s", elapsed.as_secs_f64());
    for batch in &result {
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch.clone()])?
        );
    }

    std::fs::create_dir_all("output")?;
    let out_file = std::fs::File::create("output/subtract_local_result.csv")?;
    let mut writer = datafusion::arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(out_file);
    for batch in &result {
        writer.write(batch)?;
    }
    println!("Wynik zapisany do output/subtract_local_result.csv");
    println!("============================================================");

    Ok(())
}

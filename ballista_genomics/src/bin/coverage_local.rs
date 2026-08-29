use std::time::Instant;

use datafusion::error::Result;
use datafusion::prelude::CsvReadOptions;
use datafusion_bio_function_ranges::create_bio_session;

// Faza C: operacja coverage, poziom lokalny (jak merge/nearest_local —
// RangeTableFunction/CountOverlapsProvider też buduje customowy, prywatny
// CountOverlapsExec bezpośrednio, ta sama granica dystrybucji).

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("  Faza C: coverage (lokalnie, datafusion-bio-function-ranges)");
    println!("============================================================\n");

    let ctx = create_bio_session();

    ctx.register_csv("intervals_a", "data/intervals_a.csv", CsvReadOptions::new())
        .await?;
    ctx.register_csv("intervals_b", "data/intervals_b.csv", CsvReadOptions::new())
        .await?;

    // UWAGA (nieoczywista różnica API, znaleziona empirycznie w tej sesji):
    // pb.coverage(a, b) z polars-bio i coverage(reads, targets) z
    // bio-function-ranges mają ODWRÓCONĄ konwencję argumentów. pb.coverage(a, b)
    // raportuje pokrycie KAŻDEGO interwału z `a` przez `b` (a = cel/target,
    // b = odczyty/reads pokrywające), podczas gdy dokumentacja SQL
    // "coverage('reads', 'targets', ...)" sugeruje odwrotnie. Żeby dostać ten
    // sam wynik co pb.coverage(intervals_a, intervals_b), trzeba wywołać
    // coverage('intervals_b', 'intervals_a', ...) — czyli B jako "reads", A
    // jako "targets" (raportowane interwały).
    let t0 = Instant::now();
    let df = ctx
        .sql(
            "SELECT * FROM coverage('intervals_b', 'intervals_a', 'chrom', 'start', 'end', 'strict') \
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
    let out_file = std::fs::File::create("output/coverage_local_result.csv")?;
    let mut writer = datafusion::arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(out_file);
    for batch in &result {
        writer.write(batch)?;
    }
    println!("Wynik zapisany do output/coverage_local_result.csv");
    println!("============================================================");

    Ok(())
}

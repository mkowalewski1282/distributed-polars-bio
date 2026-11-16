use std::time::Instant;

use datafusion::error::Result;
use datafusion::prelude::CsvReadOptions;
use datafusion_bio_function_ranges::create_bio_session;

// The coverage operation at the local level (like merge/nearest_local -
// RangeTableFunction/CountOverlapsProvider also builds a custom, private
// CountOverlapsExec directly, the same distribution boundary).

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("  Phase C: coverage (local, datafusion-bio-function-ranges)");
    println!("============================================================\n");

    let ctx = create_bio_session();

    ctx.register_csv("intervals_a", "data/intervals_a.csv", CsvReadOptions::new())
        .await?;
    ctx.register_csv("intervals_b", "data/intervals_b.csv", CsvReadOptions::new())
        .await?;

    // NOTE (a non-obvious API difference, found empirically):
    // pb.coverage(a, b) from polars-bio and coverage(reads, targets) from
    // bio-function-ranges use the OPPOSITE argument convention. pb.coverage(a, b)
    // reports the coverage of EACH interval of `a` by `b` (a = target,
    // b = the covering reads), whereas the SQL documentation
    // "coverage('reads', 'targets', ...)" suggests the reverse. To get the
    // same result as pb.coverage(intervals_a, intervals_b), call
    // coverage('intervals_b', 'intervals_a', ...) - i.e. B as the "reads", A
    // as the "targets" (the reported intervals).
    let t0 = Instant::now();
    let df = ctx
        .sql(
            "SELECT * FROM coverage('intervals_b', 'intervals_a', 'chrom', 'start', 'end', 'strict') \
             ORDER BY 1, 2",
        )
        .await?;
    let result = df.collect().await?;
    let elapsed = t0.elapsed();

    println!("Time: {:.4}s", elapsed.as_secs_f64());
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
    println!("Result written to output/coverage_local_result.csv");
    println!("============================================================");

    Ok(())
}

use std::time::Instant;

use datafusion::error::Result;
use datafusion::prelude::CsvReadOptions;
use datafusion_bio_function_ranges::create_bio_session;

// ---------------------------------------------------------------------------
// The merge operation at the "local in DataFusion" level (the counterpart of the local
// overlap check) - registers the real engine (MergeProvider from bio-function-ranges)
// and verifies correctness.
//
// IMPORTANT: unlike OverlapProvider, MergeProvider::scan() does NOT delegate to
// session.sql() - it builds its own custom, PRIVATE MergeExec directly. This means the
// fully distributed variant (as for overlap, with a LogicalExtensionCodec) does NOT have
// the same loophole here: the session cannot be "swapped" for a plain one to avoid the
// custom physical node - MergeExec always appears. Distributing merge fully needs a
// PhysicalExtensionCodec for MergeExec, which with private fields means either working
// with the crate's authors or reimplementing it from scratch (the vendored copy makes
// the fields public, see vendor/PATCH.md).
// This binary therefore verifies only LOCAL correctness (without Ballista).
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("  Phase C: merge (local, datafusion-bio-function-ranges)");
    println!("============================================================\n");

    let ctx = create_bio_session();

    ctx.register_csv(
        "intervals_a",
        "data/intervals_a.csv",
        CsvReadOptions::new(),
    )
    .await?;

    let t0 = Instant::now();
    let df = ctx
        .sql(
            "SELECT * FROM merge('intervals_a', 'chrom', 'start', 'end', 'strict') \
             ORDER BY chrom, start",
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
    let out_file = std::fs::File::create("output/merge_local_result.csv")?;
    let mut writer = datafusion::arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(out_file);
    for batch in &result {
        writer.write(batch)?;
    }
    println!("Result written to output/merge_local_result.csv");
    println!("============================================================");

    Ok(())
}

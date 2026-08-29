use std::time::Instant;

use datafusion::error::Result;
use datafusion::prelude::CsvReadOptions;
use datafusion_bio_function_ranges::create_bio_session;

// ---------------------------------------------------------------------------
// Faza C: operacja merge, poziom "lokalny w DataFusion" (odpowiednik Fazy A.2
// dla overlap) — rejestracja prawdziwego silnika (MergeProvider z
// bio-function-ranges) i weryfikacja poprawności.
//
// WAŻNE (odkrycie z tej sesji): w przeciwieństwie do OverlapProvider,
// MergeProvider::scan() NIE deleguje do session.sql() — buduje własny,
// customowy, PRYWATNY MergeExec bezpośrednio. To oznacza, że w pełni
// rozproszony wariant (jak Faza A.4 dla overlap, z LogicalExtensionCodec)
// NIE MA tu tej samej furtki: nie da się "podmienić" sesji na zwykłą, żeby
// uniknąć customowego węzła fizycznego — MergeExec pojawi się zawsze.
// Pełna dystrybucja merge wymagałaby PhysicalExtensionCodec dla MergeExec,
// co przy prywatnych polach oznacza współpracę z autorami crate'a albo
// reimplementację od zera (patrz ballista_genomics/OPIS.md).
// Ten binarz weryfikuje więc tylko poprawność LOKALNĄ (bez Ballisty).
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("  Faza C: merge (lokalnie, datafusion-bio-function-ranges)");
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

    println!("Czas: {:.4}s", elapsed.as_secs_f64());
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
    println!("Wynik zapisany do output/merge_local_result.csv");
    println!("============================================================");

    Ok(())
}

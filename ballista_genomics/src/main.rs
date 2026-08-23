use std::time::Instant;

use ballista::prelude::SessionContextExt;
use datafusion::error::Result;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::{BioSessionExt, register_ranges_functions};

// ---------------------------------------------------------------------------
// Faza A.3 (plan pracy magisterskiej): próba uruchomienia prawdziwej Ballisty
// (nie gołego DataFusion jak w Fazie A.2) z operatorem overlap() z
// datafusion-bio-function-ranges. Cel: udokumentować DOKŁADNY błąd
// LogicalExtensionCodec/PhysicalExtensionCodec na prawdziwej funkcji (nie na
// placeholderze jak w poprzednim prototypie z OPIS.md).
//
// Budujemy SessionState z regułami optymalizatora bio (BioSessionExt::new_with_bio),
// przekazujemy do Ballista::standalone_with_state() (prawdziwy klaster: scheduler +
// executor w jednym procesie, ale osobne komponenty Ballisty — nie plain DataFusion),
// rejestrujemy UDTF-y (overlap itd.) na już-ballistowym kontekście.
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("  Faza A.3: datafusion-bio-function-ranges + Ballista standalone");
    println!("============================================================\n");

    let bio_ctx = DFSessionContext::new_with_bio(SessionConfig::new());
    let bio_state = bio_ctx.state();

    println!("Łączenie z Ballista standalone (scheduler + executor in-proc)...");
    let ctx = DFSessionContext::standalone_with_state(bio_state).await?;
    println!("Klaster Ballista wystartował.\n");

    register_ranges_functions(&ctx);

    ctx.register_csv(
        "intervals_a",
        "data/intervals_a.csv",
        CsvReadOptions::new(),
    )
    .await?;
    ctx.register_csv(
        "intervals_b",
        "data/intervals_b.csv",
        CsvReadOptions::new(),
    )
    .await?;

    let t0 = Instant::now();
    let df = ctx
        .sql(
            "SELECT left_chrom AS chrom, \
                    left_start AS start_a, left_end AS end_a, left_name AS name_a, \
                    right_start AS start_b, right_end AS end_b, right_name AS name_b \
             FROM overlap('intervals_a', 'intervals_b', 'chrom', 'start', 'end', 'strict') \
             ORDER BY chrom, start_a, start_b",
        )
        .await?;
    let result = df.collect().await?;
    let elapsed = t0.elapsed();

    println!("Czas: {:.4}s", elapsed.as_secs_f64());
    for batch in &result {
        println!("{}", datafusion::arrow::util::pretty::pretty_format_batches(&[batch.clone()])?);
    }
    println!("============================================================");

    Ok(())
}

//! Wspólny „szkielet uruchomieniowy" dla wszystkich operacji rozproszonych:
//! jedna konfiguracja sesji, jedno miejsce z SQL-ami, jedno miejsce zapisujące
//! wynik i dowód dystrybucji.

use std::sync::Arc;
use std::time::Instant;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::{BioConfig, BioSessionExt};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::dist_payload::DistOp;
use crate::dist_udtf::DistTableFunction;
use crate::logical_codec::BioDistLogicalCodec;
use crate::bio_phys_codec::BioRangesPhysicalCodec;

/// Konfiguracja sesji używana ZARÓWNO przez sesję klienta/schedulera, JAK I
/// przez wewnętrzne sesje budowane w `DistBioProvider::build()`.
///
/// Dlaczego jedno miejsce: to wewnętrzna sesja buduje plany fizyczne dzieci
/// (`create_physical_plan()`), więc decyduje o liczbie partycji, na podstawie
/// której `EnforceDistribution` schedulera decyduje potem, czy wstawić shuffle.
/// Rozjazd między tymi konfiguracjami cicho psuł dystrybucję.
///
/// `with_target_partitions(4)` jest OBOWIĄZKOWE, nie kosmetyczne: przy
/// `target_partitions == 1` DataFusion w ogóle nie wstawia hash-repartycji
/// (`enforce_distribution.rs`, `add_hash_on_top`: `if n_target == 1 && count == 1
/// { return input }`), więc zapytanie policzyłoby się poprawnie, ale w JEDNYM
/// stage'u — a teza o dystrybucji byłaby pusta. Objawu brak; wykrywalne tylko
/// przez `EXPLAIN ANALYZE`.
///
/// `BioConfig::default()` rejestrujemy jawnie, mimo że `new_with_bio()` tego nie
/// wymaga — żeby `interval_join_algorithm = Coitrees` i
/// `interval_join_low_memory = false` były zapisane wprost, a nie brane
/// milcząco z domyślnych. `IntervalJoinPhysicalCodec` zakłada dokładnie te
/// wartości (nie da się ich odczytać z instancji węzła), więc lepiej, żeby
/// invariant był widoczny w kodzie.
pub fn bio_session_config() -> SessionConfig {
    SessionConfig::from(ConfigOptions::new())
        .with_option_extension(BioConfig::default())
        .with_target_partitions(4)
}

pub struct OpSpec {
    pub sql: String,
    pub output_csv: &'static str,
    pub explain_txt: &'static str,
    pub title: &'static str,
}

pub fn spec(op: DistOp) -> OpSpec {
    match op {
        DistOp::Overlap => OpSpec {
            sql: "SELECT left_chrom AS chrom, \
                         left_start AS start_a, left_end AS end_a, left_name AS name_a, \
                         right_start AS start_b, right_end AS end_b, right_name AS name_b \
                  FROM dist_overlap('intervals_a', 'data/intervals_a.csv', \
                                    'intervals_b', 'data/intervals_b.csv', \
                                    'chrom', 'start', 'end', 'strict') \
                  ORDER BY chrom, start_a, start_b"
                .to_string(),
            output_csv: "output/dist_overlap_result.csv",
            explain_txt: "output/dist_overlap_explain.txt",
            title: "dist_overlap + COITrees w pełni rozproszone",
        },
        DistOp::Merge => OpSpec {
            // Dane celowo rozbite na DWA pliki (data/parts_a/): nakładające się
            // gene_A1=[100,200) i gene_A2=[150,300) są w RÓŻNYCH plikach, więc
            // poprawny wynik [100,300) powstanie TYLKO jeśli hash-shuffle po
            // chrom faktycznie przeniósł wiersze między partycjami. Bez tego
            // MergeExec scaliłby je osobno i zwrócił dwa interwały zamiast
            // jednego — test poprawności zawali się głośno.
            sql: "SELECT * FROM dist_merge('intervals_a', 'data/parts_a', \
                                           'chrom', 'start', 'end', 0, 'strict') \
                  ORDER BY chrom, start"
                .to_string(),
            output_csv: "output/dist_merge_result.csv",
            explain_txt: "output/dist_merge_explain.txt",
            title: "dist_merge w pełni rozproszony (hash-shuffle po chrom)",
        },
        DistOp::Subtract => OpSpec {
            // Wezel BINARNY: obie strony musza byc ko-partycjonowane po chrom,
            // bo SubtractExec::execute(partition) siega po TE SAMA partycje z
            // lewej i prawej strony. DataFusion wymusza to przez needs_alignment
            // (gdy choc jedno dziecko wymaga hasha, wszystkie hash-owe dzieci
            // dostaja hash_necessary=true) — stad oczekiwane 2x Hash([chrom.
            sql: "SELECT * FROM dist_subtract('intervals_a', 'data/parts_a', \
                                              'intervals_b', 'data/parts_b', \
                                              'chrom', 'start', 'end', 'strict') \
                  ORDER BY chrom, start"
                .to_string(),
            output_csv: "output/dist_subtract_result.csv",
            explain_txt: "output/dist_subtract_explain.txt",
            title: "dist_subtract w pełni rozproszony (dwustronny hash-shuffle)",
        },
        DistOp::Nearest => OpSpec {
            // Wzorzec BROADCAST, nie hash-shuffle: NearestExec nie nadpisuje
            // required_input_distribution(), wiec nie zada repartycji. Lewa
            // (indeksowana) tabela jedzie w CALOSCI w ladunku planu do kazdego
            // executora, a rownoleglosc bierze sie z partycjonowania PRAWEJ
            // strony. Argumenty (k=1, include_overlaps, compute_distance, brak
            // 'strict') odwzorowuja nearest_local.rs 1:1.
            sql: "SELECT * FROM dist_nearest('intervals_a', 'data/parts_a', \
                                             'intervals_b', 'data/parts_b', \
                                             1, true, true, \
                                             'chrom', 'start', 'end') \
                  ORDER BY left_chrom, left_start"
                .to_string(),
            output_csv: "output/dist_nearest_result.csv",
            explain_txt: "output/dist_nearest_explain.txt",
            title: "dist_nearest w pełni rozproszony (broadcast lewej tabeli)",
        },
        other => panic!("runner::spec: brak specyfikacji dla operacji {other:?}"),
    }
}

pub async fn run(op: DistOp) -> Result<()> {
    let s = spec(op);

    println!("============================================================");
    println!("  {}", s.title);
    println!("============================================================\n");

    // Sesja SCHEDULERA/klienta MUSI być bio-owa (new_with_bio), żeby
    // IntervalJoinPhysicalOptimizationRule w ogóle zadziałała — reguły
    // fizycznego optymalizatora działają na CAŁYM drzewie planu, więc nawet
    // gdyby wewnętrzna sesja providera była "zwykła", to sesja schedulera
    // decyduje, czy join zostanie przepisany na IntervalJoinExec.
    let logical_codec: Arc<dyn LogicalExtensionCodec> = Arc::new(BioDistLogicalCodec::default());
    // BioRangesPhysicalCodec deleguje do IntervalJoinPhysicalCodec (Faza A.5),
    // a ten do domyślnego kodeka Ballisty — patrz bio_phys_codec.rs.
    let physical_codec: Arc<dyn PhysicalExtensionCodec> =
        Arc::new(BioRangesPhysicalCodec::default());
    let config = bio_session_config()
        .with_ballista_logical_extension_codec(logical_codec)
        .with_ballista_physical_extension_codec(physical_codec);
    let bio_ctx = DFSessionContext::new_with_bio(config);
    let state = bio_ctx.state();

    println!("Łączenie z Ballista standalone (scheduler + executor in-proc)...");
    let ctx = DFSessionContext::standalone_with_state(state).await?;
    println!("Klaster Ballista wystartował.\n");

    // Rejestrujemy wszystkie UDTF-y niezależnie od wybranej operacji — koszt
    // zerowy, a upraszcza dyspozytor.
    for o in DistOp::ALL {
        ctx.register_udtf(o.udtf_name(), Arc::new(DistTableFunction::new(o)));
    }

    let t0 = Instant::now();
    let result = ctx.sql(&s.sql).await?.collect().await?;
    let elapsed = t0.elapsed();

    println!("Czas: {:.4}s", elapsed.as_secs_f64());
    for batch in &result {
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch.clone()])?
        );
    }
    println!("============================================================");

    std::fs::create_dir_all("output")?;
    write_csv(&result, s.output_csv)?;
    println!("Wynik zapisany do {}", s.output_csv);

    // PUNKT KONTROLNY: zrzut planu ROZPROSZONEGO z podziałem na query stage'e.
    // Ballista implementuje EXPLAIN ANALYZE tak, że zwraca sekcje
    // `=========SuccessfulStage[stage_id=N, partitions=M]=========` z drzewem
    // operatorów i metrykami per stage — to jedyny maszynowo sprawdzalny dowód,
    // że zapytanie NAPRAWDĘ zostało pocięte na etapy i przeszło przez shuffle,
    // a nie tylko „nie wywaliło błędu". Asercje: tests/test_ballista_distribution_evidence.py
    match ctx.sql(&format!("EXPLAIN ANALYZE {}", s.sql)).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                std::fs::write(s.explain_txt, extract_text(&batches))?;
                println!("Plan rozproszony (EXPLAIN ANALYZE) zapisany do {}", s.explain_txt);
            }
            Err(e) => eprintln!("UWAGA: EXPLAIN ANALYZE nie wykonało się: {e}"),
        },
        Err(e) => eprintln!("UWAGA: EXPLAIN ANALYZE nie sparsowało się: {e}"),
    }

    Ok(())
}

fn write_csv(batches: &[RecordBatch], path: &str) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = datafusion::arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(file);
    for batch in batches {
        writer.write(batch)?;
    }
    Ok(())
}

/// Wyciąga surową treść tekstową z wyniku EXPLAIN (kolumny stringowe), zamiast
/// formatować przez `pretty_format_batches` — plan jest wielolinijkowy i
/// ramkowanie ASCII zniszczyłoby jego strukturę, a testy parsują go regexem.
fn extract_text(batches: &[RecordBatch]) -> String {
    let mut out = String::new();
    for batch in batches {
        for col in batch.columns() {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        out.push_str(arr.value(i));
                        out.push('\n');
                    }
                }
            }
        }
    }
    out
}

//! Wspólny „szkielet uruchomieniowy" dla wszystkich operacji rozproszonych:
//! jedna konfiguracja sesji, jedno miejsce z SQL-ami, jedno miejsce zapisujące
//! wynik i dowód dystrybucji.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use ballista::prelude::SessionContextExt;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::BioConfig;

use crate::cluster::{bio_ballista_config, bio_session_state};
use crate::dist_payload::DistOp;
use crate::dist_udtf::DistTableFunction;

/// Adres zewnętrznego schedulera (np. `df://localhost:50050`). Niepusta wartość
/// → klient łączy się z klastrem z osobnych procesów (`ballista_node`); pusta
/// albo brak → dotychczasowy tryb standalone (scheduler + executor in-proc).
pub const SCHEDULER_URL_ENV: &str = "BALLISTA_SCHEDULER_URL";

/// Katalog na wynik i plan EXPLAIN ANALYZE; pusty albo brak → `output`.
/// Testy klastra z osobnych procesów podają własny katalog, żeby nie nadpisywać
/// plików, na których opierają się testy trybu standalone.
pub const OUTPUT_DIR_ENV: &str = "DIST_OUTPUT_DIR";

fn output_dir() -> PathBuf {
    match std::env::var(OUTPUT_DIR_ENV) {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => PathBuf::from("output"),
    }
}

/// Adres zewnętrznego schedulera z `BALLISTA_SCHEDULER_URL`; pusta wartość = brak.
pub fn scheduler_url() -> Option<String> {
    std::env::var(SCHEDULER_URL_ENV).ok().filter(|url| !url.is_empty())
}

/// Liczba partycji docelowych (`target_partitions`) dla WSZYSTKICH sesji — klienta,
/// schedulera, executorów i wewnętrznych sesji providera. Orkiestrator pomiarów ustawia
/// ją na 2N (N węzłów po 2 sloty; specyfikacja, sekcja 3) w środowisku każdego procesu
/// klastra i klienta — rozjazd między procesami cicho psułby dystrybucję (patrz
/// `bio_session_config`). Brak albo pusta → 4 (wartość sprzed planu 3a).
pub const TARGET_PARTITIONS_ENV: &str = "BIO_TARGET_PARTITIONS";
pub const DEFAULT_TARGET_PARTITIONS: usize = 4;

/// Wartość z `BIO_TARGET_PARTITIONS`: liczba całkowita ≥ 2 (przy 1 DataFusion nie wstawia
/// hash-repartycji — patrz `bio_session_config`). Binarki sprawdzają ją przy starcie.
pub fn target_partitions() -> std::result::Result<usize, String> {
    match std::env::var(TARGET_PARTITIONS_ENV) {
        Ok(v) if !v.is_empty() => v
            .parse::<usize>()
            .ok()
            .filter(|n| *n >= 2)
            .ok_or_else(|| format!("{TARGET_PARTITIONS_ENV}: liczba całkowita ≥ 2, jest {v:?}")),
        _ => Ok(DEFAULT_TARGET_PARTITIONS),
    }
}

/// Bio-owa sesja klienta Ballisty z zarejestrowanymi funkcjami `dist_*`:
/// zewnętrzny scheduler, gdy podano adres, inaczej standalone in-proc.
/// Nic nie wypisuje na stdout — stdout `bench_client` to protokół.
pub async fn connect_from_env() -> Result<DFSessionContext> {
    // Sesja klienta MUSI być bio-owa (new_with_bio), a konfiguracja mieć oba
    // kodery — patrz cluster.rs. Ten sam stan dostaje scheduler standalone.
    let state = bio_session_state(bio_ballista_config())?;
    let ctx = match scheduler_url() {
        Some(url) => DFSessionContext::remote_with_state(&url, state).await?,
        None => DFSessionContext::standalone_with_state(state).await?,
    };
    for o in DistOp::ALL {
        ctx.register_udtf(o.udtf_name(), Arc::new(DistTableFunction::new(o)));
    }
    Ok(ctx)
}

/// Konfiguracja sesji używana ZARÓWNO przez sesję klienta/schedulera, JAK I
/// przez wewnętrzne sesje budowane w `DistBioProvider::build()`.
///
/// Dlaczego jedno miejsce: to wewnętrzna sesja buduje plany fizyczne dzieci
/// (`create_physical_plan()`), więc decyduje o liczbie partycji, na podstawie
/// której `EnforceDistribution` schedulera decyduje potem, czy wstawić shuffle.
/// Rozjazd między tymi konfiguracjami cicho psuł dystrybucję.
///
/// `target_partitions ≥ 2` jest OBOWIĄZKOWE, nie kosmetyczne: przy
/// `target_partitions == 1` DataFusion w ogóle nie wstawia hash-repartycji
/// (`enforce_distribution.rs`, `add_hash_on_top`: `if n_target == 1 && count == 1
/// { return input }`), więc zapytanie policzyłoby się poprawnie, ale w JEDNYM
/// stage'u — a teza o dystrybucji byłaby pusta. Objawu brak; wykrywalne tylko
/// przez `EXPLAIN ANALYZE`. Wartość: `BIO_TARGET_PARTITIONS` (domyślnie 4).
///
/// `BioConfig::default()` rejestrujemy jawnie, mimo że `new_with_bio()` tego nie
/// wymaga — żeby `interval_join_algorithm = Coitrees` i
/// `interval_join_low_memory = false` były zapisane wprost, a nie brane
/// milcząco z domyślnych. `IntervalJoinPhysicalCodec` zakłada dokładnie te
/// wartości (nie da się ich odczytać z instancji węzła), więc lepiej, żeby
/// invariant był widoczny w kodzie.
pub fn bio_session_config() -> SessionConfig {
    // Binarki sprawdzają zmienną przy starcie (błąd użycia, kod 2), więc niepoprawna
    // wartość w tym miejscu to błąd programisty.
    let partitions = target_partitions().unwrap_or_else(|e| panic!("{e}"));
    SessionConfig::from(ConfigOptions::new())
        .with_option_extension(BioConfig::default())
        .with_target_partitions(partitions)
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
            output_csv: "dist_overlap_result.csv",
            explain_txt: "dist_overlap_explain.txt",
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
            output_csv: "dist_merge_result.csv",
            explain_txt: "dist_merge_explain.txt",
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
            output_csv: "dist_subtract_result.csv",
            explain_txt: "dist_subtract_explain.txt",
            title: "dist_subtract w pełni rozproszony (dwustronny hash-shuffle)",
        },
        DistOp::Nearest => OpSpec {
            // Wzorzec BROADCAST, nie hash-shuffle: NearestExec nie nadpisuje
            // required_input_distribution(), wiec nie zada repartycji. Lewa
            // (indeksowana) tabela jedzie w CALOSCI w ladunku planu do kazdego
            // executora, a rownoleglosc bierze sie z partycjonowania PRAWEJ
            // strony. Argumenty (k=1, include_overlaps, compute_distance, 'strict')
            // odwzorowuja nearest_local.rs 1:1; 'strict' = dane 0-based (plan 3b-1).
            //
            // ORIENTACJA (poprawione w planie 2): wynik ma JEDEN wiersz na kazdy
            // wiersz PRAWEJ tabeli, z najblizszym sasiadem z lewej — odwrotnie niz
            // pb.nearest(A, B), ktore daje wiersz na kazdy przedzial A. Dlatego A
            // jest prawa (odpytywana), a B lewa (indeksowana, broadcastowana).
            // Wczesniej strony byly odwrotne, a test zgodnosci z pb przechodzil
            // przypadkiem (5 x 5 przedzialow, wszystkie odleglosci 0).
            sql: "SELECT * FROM dist_nearest('intervals_b', 'data/parts_b', \
                                             'intervals_a', 'data/parts_a', \
                                             1, true, true, \
                                             'chrom', 'start', 'end', 'strict') \
                  ORDER BY right_chrom, right_start"
                .to_string(),
            output_csv: "dist_nearest_result.csv",
            explain_txt: "dist_nearest_explain.txt",
            title: "dist_nearest w pełni rozproszony (broadcast lewej tabeli)",
        },
        DistOp::Coverage => OpSpec {
            // ODWROCONA KONWENCJA ARGUMENTOW, udokumentowana w Fazie C:
            // pb.coverage(a, b) raportuje pokrycie interwalow `a` przez `b`,
            // a SQL-owe coverage('reads','targets',...) odwrotnie. Zeby dostac
            // wynik identyczny z pb.coverage(INTERVALS_A, INTERVALS_B), wolamy
            // z intervals_b jako 'reads' i intervals_a jako 'targets' —
            // dokladnie tak jak coverage_local.rs.
            sql: "SELECT * FROM dist_coverage('intervals_b', 'data/parts_b', \
                                              'intervals_a', 'data/parts_a', \
                                              'chrom', 'start', 'end', 'strict') \
                  ORDER BY chrom, start"
                .to_string(),
            output_csv: "dist_coverage_result.csv",
            explain_txt: "dist_coverage_explain.txt",
            title: "dist_coverage w pełni rozproszony (broadcast + węzeł-nośnik)",
        },
    }
}

pub async fn run(op: DistOp) -> Result<()> {
    let s = spec(op);

    println!("============================================================");
    println!("  {}", s.title);
    println!("============================================================\n");

    match scheduler_url() {
        Some(url) => println!("Łączenie z zewnętrznym schedulerem Ballisty: {url}"),
        None => println!("Łączenie z Ballista standalone (scheduler + executor in-proc)..."),
    }
    let ctx = connect_from_env().await?;
    println!("Klaster Ballista gotowy.\n");

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

    let out_dir = output_dir();
    std::fs::create_dir_all(&out_dir)?;
    let output_csv = out_dir.join(s.output_csv);
    let explain_txt = out_dir.join(s.explain_txt);
    write_csv(&result, &output_csv)?;
    println!("Wynik zapisany do {}", output_csv.display());

    // PUNKT KONTROLNY: zrzut planu ROZPROSZONEGO z podziałem na query stage'e.
    // Ballista implementuje EXPLAIN ANALYZE tak, że zwraca sekcje
    // `=========SuccessfulStage[stage_id=N, partitions=M]=========` z drzewem
    // operatorów i metrykami per stage — to jedyny maszynowo sprawdzalny dowód,
    // że zapytanie NAPRAWDĘ zostało pocięte na etapy i przeszło przez shuffle,
    // a nie tylko „nie wywaliło błędu". Asercje: tests/test_ballista_distribution_evidence.py
    match ctx.sql(&format!("EXPLAIN ANALYZE {}", s.sql)).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                std::fs::write(&explain_txt, extract_text(&batches))?;
                println!(
                    "Plan rozproszony (EXPLAIN ANALYZE) zapisany do {}",
                    explain_txt.display()
                );
            }
            Err(e) => eprintln!("UWAGA: EXPLAIN ANALYZE nie wykonało się: {e}"),
        },
        Err(e) => eprintln!("UWAGA: EXPLAIN ANALYZE nie sparsowało się: {e}"),
    }

    Ok(())
}

fn write_csv(batches: &[RecordBatch], path: &Path) -> Result<()> {
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

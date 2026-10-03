//! Runner Ballisty dla narzędzia pomiarowego (specyfikacja, sekcje 8.1 i 8.3).
//!
//! Wykonuje JEDEN scenariusz na wskazanych danych — plik albo katalog plików
//! Parquet (zbiory databio-8p) lub CSV — i wypisuje na stdout JEDNĄ linię JSON:
//! `{"rows": N, "checksum": "0x…", "t_total_s": T, "phases": {}, "extra":
//! {"target_partitions": P, "checksum_s": C}, "peak_rss_bytes": M}`.
//!
//! - czas: od wysłania zapytania (`ctx.sql`) do skonsumowania ostatniej partii — bez
//!   startu procesu i połączenia z klastrem;
//! - suma kontrolna: `checksum.rs` (specyfikacja 8.4), liczona na bieżąco ze strumienia; jej
//!   czas jest częścią `t_total_s` i trafia osobno do `extra.checksum_s`;
//! - szczyt pamięci: VmHWM tego procesu, licznik zerowany tuż przed zapytaniem;
//! - `extra.target_partitions`: liczba partycji sesji (`BIO_TARGET_PARTITIONS`).
//!
//! Wynik jest konsumowany strumieniowo (bez zbierania w pamięci klienta), w schemacie
//! znormalizowanym (`scenario.rs`). Tryb klastra jak w `dist_ops`:
//! `BALLISTA_SCHEDULER_URL` → zewnętrzny scheduler, brak → standalone in-proc.
//! `--output` dodatkowo zapisuje wynik do Parquet (testy poprawności).
//!
//! `--checksum PLIK --op OP` liczy samą sumę kontrolną pliku Parquet w schemacie
//! znormalizowanym, bez silnika — test zgodności z `bench/checksum.py`.
//!
//! Kody wyjścia: 0 — sukces, 1 — błąd wykonania, 2 — błędne argumenty lub środowisko.

use std::fs::File;
use std::time::{Duration, Instant};

use ballista_genomics::checksum::Checksum;
use ballista_genomics::cli::{optional, parse_flags, required};
use ballista_genomics::runner::{connect_from_env, target_partitions};
use ballista_genomics::scenario::Scenario;
use ballista_genomics::DistOp;
use datafusion::error::Result;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::StreamExt;

const USAGE: &str = "usage:
  bench_client --op <overlap|nearest|coverage|merge|subtract> --left <PATH>
               [--right <PATH>] [--cols <contig,start,end>] [--output <FILE.parquet>]
  bench_client --op <OPERATION> --checksum <FILE.parquet>
  PATH: a Parquet (or CSV) file or a directory of them; --right for operations other than merge.
  Default --cols: contig,pos_start,pos_end (databio-8p datasets).
  --checksum: checksum of a file in the normalized schema, without running an operation.
  Environment: BALLISTA_SCHEDULER_URL (cluster), BIO_TARGET_PARTITIONS (≥ 2, default 4).";

const DEFAULT_COLS: &str = "contig,pos_start,pos_end";

enum Mode {
    /// Jeden scenariusz na silniku; opcjonalnie zapis wyniku do Parquet.
    Run { scenario: Scenario, output: Option<String> },
    /// Sama suma kontrolna pliku w schemacie znormalizowanym.
    Checksum { op: DistOp, file: String },
}

fn parse(args: &[String]) -> std::result::Result<Mode, String> {
    let flags = parse_flags(
        args,
        &["--op", "--left", "--right", "--cols", "--output", "--checksum"],
        &[],
    )?;
    let op_name: String = required(&flags, "--op")?;
    let op = DistOp::from_cli(&op_name).ok_or_else(|| format!("unknown operation: {op_name}"))?;
    if let Some(file) = optional::<String>(&flags, "--checksum")? {
        if let Some(other) = ["--left", "--right", "--cols", "--output"]
            .into_iter()
            .find(|f| flags.contains_key(*f))
        {
            return Err(format!("--checksum cannot be combined with {other}"));
        }
        return Ok(Mode::Checksum { op, file });
    }
    // Zmienna środowiskowa to też wejście: błąd = kod 2, zanim cokolwiek się uruchomi.
    target_partitions()?;
    let cols_raw: String =
        optional(&flags, "--cols")?.unwrap_or_else(|| DEFAULT_COLS.to_string());
    let cols: [String; 3] = cols_raw
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| format!("--cols: expected three comma-separated names, got: {cols_raw}"))?;
    if cols.iter().any(String::is_empty) {
        return Err(format!("--cols: empty column name in {cols_raw}"));
    }
    let scenario = Scenario {
        op,
        left: required(&flags, "--left")?,
        right: optional(&flags, "--right")?,
        cols,
    };
    scenario.validate()?;
    Ok(Mode::Run {
        scenario,
        output: optional(&flags, "--output")?,
    })
}

/// Logi Ballisty na stderr — tylko gdy ustawiono `RUST_LOG` (diagnostyka; stdout
/// to protokół, a domyślnie stderr ma zawierać wyłącznie komunikat błędu).
fn init_logging() {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .init();
    }
}

/// Zeruje licznik szczytu pamięci tego procesu (VmHWM := bieżący RSS).
fn reset_peak_rss() -> std::io::Result<()> {
    std::fs::write("/proc/self/clear_refs", "5")
}

/// VmHWM tego procesu w bajtach.
fn peak_rss_bytes() -> std::io::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().strip_suffix(" kB"))
        .and_then(|kb| kb.trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .ok_or_else(|| std::io::Error::other("no VmHWM in /proc/self/status"))
}

struct Report {
    rows: u64,
    checksum: String,
    t_total_s: f64,
    checksum_s: f64,
    target_partitions: usize,
    peak_rss_bytes: u64,
}

async fn run(scenario: &Scenario, output: Option<&str>) -> Result<Report> {
    let ctx = connect_from_env().await?;
    let target_partitions = ctx.state().config().target_partitions();
    let mut checksum = Checksum::new(scenario.op);
    reset_peak_rss()?;
    let t0 = Instant::now();
    let mut stream = ctx.sql(&scenario.sql()).await?.execute_stream().await?;
    let mut writer = match output {
        Some(path) => Some(ArrowWriter::try_new(File::create(path)?, stream.schema(), None)?),
        None => None,
    };
    let mut checksum_time = Duration::ZERO;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let t = Instant::now();
        checksum.update(&batch)?;
        checksum_time += t.elapsed();
        if let Some(w) = writer.as_mut() {
            w.write(&batch)?;
        }
    }
    if let Some(w) = writer {
        w.close()?;
    }
    let t_total_s = t0.elapsed().as_secs_f64();
    Ok(Report {
        rows: checksum.rows(),
        checksum: checksum.hex(),
        t_total_s,
        checksum_s: checksum_time.as_secs_f64(),
        target_partitions,
        peak_rss_bytes: peak_rss_bytes()?,
    })
}

async fn checksum_file(op: DistOp, file: &str) -> Result<Checksum> {
    let ctx = SessionContext::new();
    let mut stream = ctx
        .read_parquet(file, ParquetReadOptions::default())
        .await?
        .execute_stream()
        .await?;
    let mut checksum = Checksum::new(op);
    while let Some(batch) = stream.next().await {
        checksum.update(&batch?)?;
    }
    Ok(checksum)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Najpierw WYŁĄCZNIE parsowanie (kod 2), potem działanie (kod 1) — jak w ballista_node.
    let mode = parse(&args).unwrap_or_else(|msg| {
        eprintln!("error: {msg}\n{USAGE}");
        std::process::exit(2);
    });
    init_logging();
    let line = match mode {
        Mode::Checksum { op, file } => checksum_file(op, &file).await.map(|c| {
            format!("{{\"rows\": {}, \"checksum\": \"{}\"}}", c.rows(), c.hex())
        }),
        Mode::Run { scenario, output } => run(&scenario, output.as_deref()).await.map(|r| {
            format!(
                "{{\"rows\": {}, \"checksum\": \"{}\", \"t_total_s\": {:.6}, \"phases\": {{}}, \
                 \"extra\": {{\"target_partitions\": {}, \"checksum_s\": {:.9}}}, \
                 \"peak_rss_bytes\": {}}}",
                r.rows, r.checksum, r.t_total_s, r.target_partitions, r.checksum_s, r.peak_rss_bytes
            )
        }),
    };
    match line {
        Ok(line) => println!("{line}"),
        Err(e) => {
            eprintln!("bench_client: {e}");
            std::process::exit(1);
        }
    }
}

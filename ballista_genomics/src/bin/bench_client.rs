//! Runner Ballisty dla narzędzia pomiarowego (specyfikacja, sekcje 8.1 i 8.3).
//!
//! Wykonuje JEDEN scenariusz na wskazanych danych — plik albo katalog plików
//! Parquet (zbiory databio-8p) lub CSV — i wypisuje na stdout JEDNĄ linię JSON.
//! Na tym etapie: liczba wierszy i opcjonalny zapis wyniku do Parquet (testy
//! poprawności). Czas, fazy i suma kontrolna dochodzą w etapie narzędzia
//! pomiarowego.
//!
//! Wynik jest konsumowany strumieniowo (bez zbierania w pamięci klienta),
//! w schemacie znormalizowanym (`scenario.rs`). Tryb klastra jak w `dist_ops`:
//! `BALLISTA_SCHEDULER_URL` → zewnętrzny scheduler, brak → standalone in-proc.
//!
//! Kody wyjścia: 0 — sukces, 1 — błąd wykonania, 2 — błędne argumenty.

use std::fs::File;

use ballista_genomics::cli::{optional, parse_flags, required};
use ballista_genomics::runner::connect_from_env;
use ballista_genomics::scenario::Scenario;
use ballista_genomics::DistOp;
use datafusion::error::Result;
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;

const USAGE: &str = "użycie:
  bench_client --op <overlap|nearest|coverage|merge|subtract> --left <ŚCIEŻKA>
               [--right <ŚCIEŻKA>] [--cols <kontig,start,koniec>] [--output <PLIK.parquet>]
  ŚCIEŻKA: plik albo katalog plików Parquet (lub CSV); --right dla operacji innych niż merge.
  Domyślne --cols: contig,pos_start,pos_end (zbiory databio-8p).";

const DEFAULT_COLS: &str = "contig,pos_start,pos_end";

struct Opts {
    scenario: Scenario,
    output: Option<String>,
}

fn parse(args: &[String]) -> std::result::Result<Opts, String> {
    let flags = parse_flags(args, &["--op", "--left", "--right", "--cols", "--output"], &[])?;
    let op_name: String = required(&flags, "--op")?;
    let op = DistOp::from_cli(&op_name).ok_or_else(|| format!("nieznana operacja: {op_name}"))?;
    let cols_raw: String =
        optional(&flags, "--cols")?.unwrap_or_else(|| DEFAULT_COLS.to_string());
    let cols: [String; 3] = cols_raw
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| format!("--cols: trzy nazwy oddzielone przecinkami, dostałem: {cols_raw}"))?;
    if cols.iter().any(String::is_empty) {
        return Err(format!("--cols: pusta nazwa kolumny w {cols_raw}"));
    }
    let scenario = Scenario {
        op,
        left: required(&flags, "--left")?,
        right: optional(&flags, "--right")?,
        cols,
    };
    scenario.validate()?;
    Ok(Opts {
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

async fn run(opts: &Opts) -> Result<u64> {
    let ctx = connect_from_env().await?;
    let mut stream = ctx.sql(&opts.scenario.sql()).await?.execute_stream().await?;
    let mut writer = match &opts.output {
        Some(path) => Some(ArrowWriter::try_new(File::create(path)?, stream.schema(), None)?),
        None => None,
    };
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows() as u64;
        if let Some(w) = writer.as_mut() {
            w.write(&batch)?;
        }
    }
    if let Some(w) = writer {
        w.close()?;
    }
    Ok(rows)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Najpierw WYŁĄCZNIE parsowanie (kod 2), potem działanie (kod 1) — jak w ballista_node.
    let opts = parse(&args).unwrap_or_else(|msg| {
        eprintln!("błąd: {msg}\n{USAGE}");
        std::process::exit(2);
    });
    init_logging();
    match run(&opts).await {
        Ok(rows) => println!("{{\"rows\": {rows}}}"),
        Err(e) => {
            eprintln!("bench_client: {e}");
            std::process::exit(1);
        }
    }
}

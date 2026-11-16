//! Ballista runner for the benchmark tool.
//!
//! Runs ONE scenario on the given data - a file or a directory of Parquet files
//! (the databio-8p datasets) or CSV - and prints ONE JSON line to stdout:
//! `{"rows": N, "checksum": "0x…", "t_total_s": T, "phases": {"first_batch_s": F}, "extra":
//! {"target_partitions": P, "checksum_s": C}, "peak_rss_bytes": M}`.
//!
//! - time: from sending the query (`ctx.sql`) to receiving the last batch - excluding process
//!   start-up and the connection to the cluster; `phases.first_batch_s` - time to the first batch;
//! - checksum: `checksum.rs`, computed AFTER the clock is stopped, on the batches held in
//!   memory; its time is `extra.checksum_s`;
//! - peak memory: VmHWM of this process, the counter reset right before the query;
//! - `extra.target_partitions`: the number of session partitions (`BIO_TARGET_PARTITIONS`).
//!
//! The result is held in client memory (in the normalized schema, `scenario.rs`) until the
//! timing ends. Cluster mode as in `dist_ops`:
//! `BALLISTA_SCHEDULER_URL` -> an external scheduler, unset -> in-process standalone.
//! `--output` additionally writes the result to Parquet (correctness tests).
//!
//! `--checksum FILE --op OP` computes only the checksum of a Parquet file in the normalized
//! schema, without an engine - a test of agreement with `bench/checksum.py`.
//!
//! `--version-info` prints the Ballista and DataFusion versions and the Ballista settings read
//! from the cluster configuration (`cluster::bio_ballista_config`) - a single source for tests
//! and documentation.
//!
//! Exit codes: 0 - success, 1 - execution error, 2 - bad arguments or environment.

use std::fs::File;
use std::time::{Duration, Instant};

use ballista_core::extension::SessionConfigExt;
use ballista_genomics::checksum::Checksum;
use ballista_genomics::cli::{optional, parse_flags, required};
use ballista_genomics::cluster::bio_ballista_config;
use ballista_genomics::runner::{connect_from_env, extract_text, grpc_max_message_bytes, target_partitions};
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
  bench_client --op <OPERATION> --left <PATH> [--right <PATH>] [--cols <contig,start,end>]
               --explain-analyze <FILE.txt>
  bench_client --version-info
  PATH: a Parquet (or CSV) file or a directory of them; --right for operations other than merge.
  Default --cols: contig,pos_start,pos_end (databio-8p datasets).
  --checksum: checksum of a file in the normalized schema, without running an operation.
  --explain-analyze: write the EXPLAIN ANALYZE plan (with operator metrics) to a file instead of
  measuring; stdout: {\"explain_bytes\": N}.
  --version-info: Ballista and DataFusion versions and the Ballista settings of the cluster
  configuration, as one JSON line.
  Environment: BALLISTA_SCHEDULER_URL (cluster), BIO_TARGET_PARTITIONS (≥ 2, default 4),
  BALLISTA_GRPC_MAX_MESSAGE_BYTES (16 MiB .. 4 GiB - 1, default 512 MiB).";

const DEFAULT_COLS: &str = "contig,pos_start,pos_end";

enum Mode {
    /// One scenario on the engine; optionally writes the result to Parquet.
    Run { scenario: Scenario, output: Option<String> },
    /// Only the checksum of a file in the normalized schema.
    Checksum { op: DistOp, file: String },
    /// The scenario's EXPLAIN ANALYZE plan written to a file (an extra, unmeasured run).
    Explain { scenario: Scenario, file: String },
    /// Ballista versions and settings from the cluster configuration, without connecting to a cluster.
    VersionInfo,
}

fn parse(args: &[String]) -> std::result::Result<Mode, String> {
    let flags = parse_flags(
        args,
        &["--op", "--left", "--right", "--cols", "--output", "--checksum", "--explain-analyze"],
        &["--version-info"],
    )?;
    if flags.contains_key("--version-info") {
        if let Some(other) = flags.keys().find(|f| f.as_str() != "--version-info") {
            return Err(format!("--version-info cannot be combined with {other}"));
        }
        // The cluster configuration reads environment variables: an error = exit code 2, as in the other modes.
        target_partitions()?;
        grpc_max_message_bytes()?;
        return Ok(Mode::VersionInfo);
    }
    let op_name: String = required(&flags, "--op")?;
    let op = DistOp::from_cli(&op_name).ok_or_else(|| format!("unknown operation: {op_name}"))?;
    if let Some(file) = optional::<String>(&flags, "--checksum")? {
        if let Some(other) = ["--left", "--right", "--cols", "--output", "--explain-analyze"]
            .into_iter()
            .find(|f| flags.contains_key(*f))
        {
            return Err(format!("--checksum cannot be combined with {other}"));
        }
        return Ok(Mode::Checksum { op, file });
    }
    // An environment variable is also input: an error = exit code 2, before anything starts.
    target_partitions()?;
    grpc_max_message_bytes()?;
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
    let output = optional(&flags, "--output")?;
    match optional::<String>(&flags, "--explain-analyze")? {
        Some(_) if output.is_some() => Err("--explain-analyze cannot be combined with --output".to_string()),
        Some(file) => Ok(Mode::Explain { scenario, file }),
        None => Ok(Mode::Run { scenario, output }),
    }
}

/// Ballista logs on stderr - only when `RUST_LOG` is set (diagnostics; stdout is the
/// protocol, and by default stderr should contain only the error message).
fn init_logging() {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .init();
    }
}

/// Resets this process's peak-memory counter (VmHWM := current RSS).
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
    first_batch_s: f64,
    checksum_s: f64,
    target_partitions: usize,
    peak_rss_bytes: u64,
}

async fn run(scenario: &Scenario, output: Option<&str>) -> Result<Report> {
    let ctx = connect_from_env().await?;
    let target_partitions = ctx.state().config().target_partitions();
    reset_peak_rss()?;
    let t0 = Instant::now();
    let mut stream = ctx.sql(&scenario.sql()).await?.execute_stream().await?;
    let schema = stream.schema();
    let mut batches = Vec::new();
    let mut first_batch: Option<Duration> = None;
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
        first_batch.get_or_insert_with(|| t0.elapsed());
    }
    let total = t0.elapsed();
    let t = Instant::now();
    let mut checksum = Checksum::new(scenario.op);
    for batch in &batches {
        checksum.update(batch)?;
    }
    let checksum_s = t.elapsed().as_secs_f64();
    if let Some(path) = output {
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    Ok(Report {
        rows: checksum.rows(),
        checksum: checksum.hex(),
        t_total_s: total.as_secs_f64(),
        first_batch_s: first_batch.unwrap_or(total).as_secs_f64(),
        checksum_s,
        target_partitions,
        peak_rss_bytes: peak_rss_bytes()?,
    })
}

/// An extra, unmeasured run: the EXPLAIN ANALYZE plan with operator metrics, written to a
/// file. Returns the number of bytes written.
async fn explain(scenario: &Scenario, file: &str) -> Result<usize> {
    let ctx = connect_from_env().await?;
    let batches = ctx
        .sql(&format!("EXPLAIN ANALYZE {}", scenario.sql()))
        .await?
        .collect()
        .await?;
    let text = extract_text(&batches);
    std::fs::write(file, &text)?;
    Ok(text.len())
}

/// Ballista versions and settings read from the same configuration that the client,
/// scheduler and executors get (`bio_ballista_config`), not from constants - a test compares
/// them with the values documented in the technical description.
fn version_info() -> String {
    let config = bio_ballista_config();
    let ballista = config.ballista_config();
    format!(
        "{{\"ballista\": \"{}\", \"datafusion\": \"{}\", \"adaptive_query_planner\": {}, \
         \"max_partitions_per_task\": {}, \"share_file_statistics_cache\": {}, \
         \"grpc_client_max_message_size\": {}}}",
        ballista_core::BALLISTA_VERSION,
        datafusion::DATAFUSION_VERSION,
        config.ballista_adaptive_query_planner_enabled(),
        ballista.max_partitions_per_task(),
        ballista.share_file_statistics_cache(),
        config.ballista_grpc_client_max_message_size(),
    )
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
    // Parsing ONLY first (exit code 2), then running (exit code 1) - as in ballista_node.
    let mode = parse(&args).unwrap_or_else(|msg| {
        eprintln!("error: {msg}\n{USAGE}");
        std::process::exit(2);
    });
    init_logging();
    let line = match mode {
        Mode::Checksum { op, file } => checksum_file(op, &file).await.map(|c| {
            format!("{{\"rows\": {}, \"checksum\": \"{}\"}}", c.rows(), c.hex())
        }),
        Mode::Explain { scenario, file } => explain(&scenario, &file)
            .await
            .map(|bytes| format!("{{\"explain_bytes\": {bytes}}}")),
        Mode::VersionInfo => Ok(version_info()),
        Mode::Run { scenario, output } => run(&scenario, output.as_deref()).await.map(|r| {
            format!(
                "{{\"rows\": {}, \"checksum\": \"{}\", \"t_total_s\": {:.6}, \
                 \"phases\": {{\"first_batch_s\": {:.6}}}, \
                 \"extra\": {{\"target_partitions\": {}, \"checksum_s\": {:.9}}}, \
                 \"peak_rss_bytes\": {}}}",
                r.rows, r.checksum, r.t_total_s, r.first_batch_s, r.target_partitions,
                r.checksum_s, r.peak_rss_bytes
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

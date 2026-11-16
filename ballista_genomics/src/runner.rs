//! A common "run harness" for all distributed operations: one session
//! configuration, one place holding the SQL, one place that writes the
//! result and the proof of distribution.

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

/// Address of an external scheduler (e.g. `df://localhost:50050`). A non-empty value
/// -> the client connects to a cluster of separate processes (`ballista_node`); empty
/// or unset -> the standalone mode (scheduler + executor in-process).
pub const SCHEDULER_URL_ENV: &str = "BALLISTA_SCHEDULER_URL";

/// Directory for the result and the EXPLAIN ANALYZE plan; empty or unset -> `output`.
/// Tests of the separate-process cluster pass their own directory so as not to overwrite
/// the files that the standalone-mode tests rely on.
pub const OUTPUT_DIR_ENV: &str = "DIST_OUTPUT_DIR";

fn output_dir() -> PathBuf {
    match std::env::var(OUTPUT_DIR_ENV) {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => PathBuf::from("output"),
    }
}

/// Address of the external scheduler from `BALLISTA_SCHEDULER_URL`; an empty value = unset.
pub fn scheduler_url() -> Option<String> {
    std::env::var(SCHEDULER_URL_ENV).ok().filter(|url| !url.is_empty())
}

/// The target number of partitions (`target_partitions`) for ALL sessions - the client,
/// scheduler, executors and the providers' internal sessions. For parallelism P the benchmark
/// orchestrator sets it to `max(P, 2)` (one executor per CPU with one slot each; values below 2
/// are rejected because DataFusion skips hash repartitioning at 1 partition) in the environment
/// of every cluster process and of the client - a mismatch between processes would silently
/// break the distribution (see `bio_session_config`). Unset or empty -> 4 (the value used
/// before it became configurable).
pub const TARGET_PARTITIONS_ENV: &str = "BIO_TARGET_PARTITIONS";
pub const DEFAULT_TARGET_PARTITIONS: usize = 4;

/// The value of `BIO_TARGET_PARTITIONS`: an integer >= 2 (at 1 DataFusion does not insert
/// hash repartitioning - see `bio_session_config`). The binaries validate it at start-up.
pub fn target_partitions() -> std::result::Result<usize, String> {
    match std::env::var(TARGET_PARTITIONS_ENV) {
        Ok(v) if !v.is_empty() => v
            .parse::<usize>()
            .ok()
            .filter(|n| *n >= 2)
            .ok_or_else(|| format!("{TARGET_PARTITIONS_ENV}: expected an integer ≥ 2, got {v:?}")),
        _ => Ok(DEFAULT_TARGET_PARTITIONS),
    }
}

/// The gRPC message size limit (bytes) of the client, scheduler and executors.
/// The plan of a stage with a broadcast (nearest, coverage) carries the whole broadcast table.
/// Ballista 55 encodes the plan separately for each task and sends the tasks assigned to an
/// executor in one round as a single message (`launch_multi_task`), so the message carries one
/// copy of the table per such task - at most as many copies as the executor has slots (Ballista
/// 53: one copy per executor and stage). Ballista's default of 16 MiB does not fit the pairs
/// 2-7 and 7-0 (the query hung). The orchestrator passes the same value to all cluster
/// processes and to the client.
pub const GRPC_MAX_MESSAGE_ENV: &str = "BALLISTA_GRPC_MAX_MESSAGE_BYTES";
pub const DEFAULT_GRPC_MAX_MESSAGE_BYTES: usize = 512 * 1024 * 1024;
/// The smallest allowed value - Ballista's default limit; a lower one would break ordinary messages.
pub const MIN_GRPC_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// The value of `BALLISTA_GRPC_MAX_MESSAGE_BYTES`: an integer from 16 MiB to `u32::MAX` (the
/// configuration fields of Ballista's gRPC servers are u32); unset or empty -> 512 MiB. The
/// binaries validate it at start-up.
pub fn grpc_max_message_bytes() -> std::result::Result<usize, String> {
    match std::env::var(GRPC_MAX_MESSAGE_ENV) {
        Ok(v) if !v.is_empty() => v
            .parse::<usize>()
            .ok()
            .filter(|n| (MIN_GRPC_MAX_MESSAGE_BYTES..=u32::MAX as usize).contains(n))
            .ok_or_else(|| {
                format!(
                    "{GRPC_MAX_MESSAGE_ENV}: expected an integer from {MIN_GRPC_MAX_MESSAGE_BYTES} \
                     to {}, got {v:?}",
                    u32::MAX
                )
            }),
        _ => Ok(DEFAULT_GRPC_MAX_MESSAGE_BYTES),
    }
}

/// A bio-enabled Ballista client session with the `dist_*` functions registered:
/// an external scheduler when an address is given, otherwise in-process standalone.
/// Prints nothing to stdout - the stdout of `bench_client` is the protocol.
pub async fn connect_from_env() -> Result<DFSessionContext> {
    // The client session MUST be bio-enabled (new_with_bio), and the configuration must
    // have both codecs - see cluster.rs. The standalone scheduler gets the same state.
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

/// The session configuration used BOTH by the client/scheduler session AND by the
/// internal sessions built in `DistBioProvider::build()`.
///
/// Why a single place: the internal session builds the physical plans of the children
/// (`create_physical_plan()`), so it decides the number of partitions on which the
/// scheduler's `EnforceDistribution` later bases the decision whether to insert a shuffle.
/// A mismatch between these configurations silently broke the distribution.
///
/// `target_partitions >= 2` is REQUIRED, not cosmetic: with
/// `target_partitions == 1` DataFusion does not insert hash repartitioning at all
/// (`enforce_distribution.rs`, `add_hash_on_top`: `if n_target == 1 && count == 1
/// { return input }`), so the query would compute correctly but in ONE
/// stage - and the claim of distribution would be empty. There is no visible symptom;
/// it is detectable only through `EXPLAIN ANALYZE`. Value: `BIO_TARGET_PARTITIONS` (4 by default).
///
/// We register `BioConfig::default()` explicitly, even though `new_with_bio()` does not
/// require it - so that `interval_join_algorithm = Coitrees` and
/// `interval_join_low_memory = false` are written out rather than silently taken
/// from the defaults. `IntervalJoinPhysicalCodec` assumes exactly these values (they
/// cannot be read from a node instance), so the invariant is better visible in the code.
pub fn bio_session_config() -> SessionConfig {
    // The binaries validate the variable at start-up (a usage error, exit code 2), so an
    // invalid value here is a programmer error.
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

pub fn op_definition(op: DistOp) -> OpSpec {
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
            title: "dist_overlap + COITrees, fully distributed",
        },
        DistOp::Merge => OpSpec {
            // The data is deliberately split into TWO files (data/parts_a/): the overlapping
            // gene_A1=[100,200) and gene_A2=[150,300) are in DIFFERENT files, so the
            // correct result [100,300) appears ONLY if the hash shuffle by chrom
            // actually moved rows between partitions. Otherwise MergeExec would merge
            // them separately and return two intervals instead of one - the
            // correctness test would fail loudly.
            sql: "SELECT * FROM dist_merge('intervals_a', 'data/parts_a', \
                                           'chrom', 'start', 'end', 0, 'strict') \
                  ORDER BY chrom, start"
                .to_string(),
            output_csv: "dist_merge_result.csv",
            explain_txt: "dist_merge_explain.txt",
            title: "dist_merge, fully distributed (hash shuffle by chrom)",
        },
        DistOp::Subtract => OpSpec {
            // A BINARY node: both sides must be co-partitioned by chrom,
            // because SubtractExec::execute(partition) reads the SAME partition from
            // the left and the right side. DataFusion enforces this through needs_alignment
            // (when at least one child requires a hash, all hash-distributed children
            // get hash_necessary=true) - hence the expected 2x Hash([chrom ...) in the plan.
            sql: "SELECT * FROM dist_subtract('intervals_a', 'data/parts_a', \
                                              'intervals_b', 'data/parts_b', \
                                              'chrom', 'start', 'end', 'strict') \
                  ORDER BY chrom, start"
                .to_string(),
            output_csv: "dist_subtract_result.csv",
            explain_txt: "dist_subtract_explain.txt",
            title: "dist_subtract, fully distributed (two-sided hash shuffle)",
        },
        DistOp::Nearest => OpSpec {
            // The BROADCAST pattern, not a hash shuffle: NearestExec does not override
            // required_input_distribution(), so it does not request repartitioning. The left
            // (indexed) table travels IN FULL in the plan payload to every
            // executor, and parallelism comes from partitioning the RIGHT
            // side. The arguments (k=1, include_overlaps, compute_distance, 'strict')
            // mirror nearest_local.rs 1:1; 'strict' = 0-based data.
            //
            // ORIENTATION: the result has ONE row per row of the RIGHT table, with the
            // nearest neighbour from the left - the opposite of pb.nearest(A, B), which gives
            // a row per interval of A. Therefore A is the right (queried) side and B the left
            // (indexed, broadcast) one. The sides used to be swapped, and the agreement test
            // with pb passed by accident (5 x 5 intervals, all distances 0).
            sql: "SELECT * FROM dist_nearest('intervals_b', 'data/parts_b', \
                                             'intervals_a', 'data/parts_a', \
                                             1, true, true, \
                                             'chrom', 'start', 'end', 'strict') \
                  ORDER BY right_chrom, right_start"
                .to_string(),
            output_csv: "dist_nearest_result.csv",
            explain_txt: "dist_nearest_explain.txt",
            title: "dist_nearest, fully distributed (broadcast of the left table)",
        },
        DistOp::Coverage => OpSpec {
            // REVERSED ARGUMENT CONVENTION (see coverage_local.rs):
            // pb.coverage(a, b) reports the coverage of the intervals of `a` by `b`,
            // whereas the SQL coverage('reads','targets',...) does the reverse. To get a
            // result identical to pb.coverage(INTERVALS_A, INTERVALS_B), we call it
            // with intervals_b as 'reads' and intervals_a as 'targets' -
            // exactly like coverage_local.rs.
            sql: "SELECT * FROM dist_coverage('intervals_b', 'data/parts_b', \
                                              'intervals_a', 'data/parts_a', \
                                              'chrom', 'start', 'end', 'strict') \
                  ORDER BY chrom, start"
                .to_string(),
            output_csv: "dist_coverage_result.csv",
            explain_txt: "dist_coverage_explain.txt",
            title: "dist_coverage, fully distributed (broadcast + carrier node)",
        },
    }
}

pub async fn run(op: DistOp) -> Result<()> {
    let s = op_definition(op);

    println!("============================================================");
    println!("  {}", s.title);
    println!("============================================================\n");

    match scheduler_url() {
        Some(url) => println!("Connecting to external Ballista scheduler: {url}"),
        None => println!("Connecting to Ballista standalone (scheduler + executor in-proc)..."),
    }
    let ctx = connect_from_env().await?;
    println!("Ballista cluster ready.\n");

    let t0 = Instant::now();
    let result = ctx.sql(&s.sql).await?.collect().await?;
    let elapsed = t0.elapsed();

    println!("Time: {:.4}s", elapsed.as_secs_f64());
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
    println!("Result written to {}", output_csv.display());

    // CHECKPOINT: a dump of the DISTRIBUTED plan split into query stages.
    // Ballista implements EXPLAIN ANALYZE so that it returns sections
    // `=========SuccessfulStage[stage_id=N, partitions=M]=========` with the operator
    // tree and per-stage metrics - the only machine-checkable proof that the
    // query was REALLY cut into stages and went through a shuffle, and did not
    // merely "run without an error". Assertions: tests/test_ballista_distribution_evidence.py
    match ctx.sql(&format!("EXPLAIN ANALYZE {}", s.sql)).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                std::fs::write(&explain_txt, extract_text(&batches))?;
                println!(
                    "Distributed plan (EXPLAIN ANALYZE) written to {}",
                    explain_txt.display()
                );
            }
            Err(e) => eprintln!("WARNING: EXPLAIN ANALYZE failed: {e}"),
        },
        Err(e) => eprintln!("WARNING: EXPLAIN ANALYZE could not be parsed: {e}"),
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

/// Extracts the raw text from an EXPLAIN result (string columns) instead of
/// formatting it with `pretty_format_batches` - the plan is multi-line and
/// ASCII framing would destroy its structure, and the tests parse it with a regex.
pub fn extract_text(batches: &[RecordBatch]) -> String {
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

//! A Ballista cluster node as a SEPARATE process: a scheduler or an executor with the
//! genomic-operation codecs.
//!
//! One binary with two roles instead of two binaries: on the development machine the
//! dominant cost of an iteration is linking (see dist_ops.rs).
//!
//! All cluster processes and the client must run in the `ballista_genomics/` directory,
//! because the plan payloads carry RELATIVE PATHS to the data.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::extension::SessionConfigExt;
use ballista_core::ConfigProducer;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_genomics::cli::{parse_flags, required};
use ballista_genomics::cluster::{
    bio_ballista_config, bio_logical_codec, bio_physical_codec, bio_session_state,
};
use ballista_genomics::runner::{bio_session_config, grpc_max_message_bytes, target_partitions};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;

const USAGE: &str = "usage:
  ballista_node scheduler --port <P>
  ballista_node executor --scheduler-port <P> --port <F> --grpc-port <G> \\
                         --work-dir <DIR> --concurrent-tasks <N> [--no-codecs]";

struct ExecutorOpts {
    scheduler_port: u16,
    port: u16,
    grpc_port: u16,
    work_dir: String,
    concurrent_tasks: usize,
    /// `false` = negative control: an executor without the genomic-operation codecs.
    codecs: bool,
}

fn parse_executor(args: &[String]) -> Result<ExecutorOpts, String> {
    let flags = parse_flags(
        args,
        &["--scheduler-port", "--port", "--grpc-port", "--work-dir", "--concurrent-tasks"],
        &["--no-codecs"],
    )?;
    Ok(ExecutorOpts {
        scheduler_port: required(&flags, "--scheduler-port")?,
        port: required(&flags, "--port")?,
        grpc_port: required(&flags, "--grpc-port")?,
        work_dir: required(&flags, "--work-dir")?,
        concurrent_tasks: required(&flags, "--concurrent-tasks")?,
        codecs: !flags.contains_key("--no-codecs"),
    })
}

async fn run_scheduler(port: u16, grpc_limit: usize) -> Result<(), Box<dyn Error>> {
    let mut config = SchedulerConfig::default()
        .with_hostname("localhost")
        .with_port(port)
        // Push + round-robin: the scheduler hands out tasks to all executors in turn.
        // With pull (executors ask for work themselves) and millisecond-long tasks
        // one executor can grab everything, and any proof that "work ran on both
        // executors" would be a lottery.
        .with_scheduler_policy(TaskSchedulingPolicy::PushStaged)
        .with_task_distribution(TaskDistributionPolicy::RoundRobin)
        // 0 = do not tell executors to delete the data of finished tasks: the stage
        // files in the work directories show who computed what.
        .with_finished_job_data_clean_up_interval_seconds(0)
        // Equivalent of the session state that standalone mode takes from the client.
        // upgrade_for_ballista() adds the `ballista` namespace - without it the
        // scheduler silently rejects all `ballista.*` keys coming from the client.
        .with_override_config_producer(Arc::new(|| bio_ballista_config().upgrade_for_ballista()))
        .with_override_session_builder(Arc::new(bio_session_state))
        // gRPC message size limit of the scheduler server (the broadcast payload travels through it).
        .with_grpc_server_max_decoding_message_size(grpc_limit as u32)
        .with_grpc_server_max_encoding_message_size(grpc_limit as u32);
    config.bind_host = "127.0.0.1".into();
    config.override_logical_codec = Some(bio_logical_codec());
    config.override_physical_codec = Some(bio_physical_codec());

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let cluster = BallistaCluster::new_from_config(&config).await?;
    // Planning settings from the configuration the scheduler gets from `override_config_producer`.
    let planning = bio_ballista_config();
    println!(
        "ballista_node: scheduler listening on {addr}, grpc max message {grpc_limit} B, \
         adaptive query planner {}, max partitions per task {}",
        planning.ballista_adaptive_query_planner_enabled(),
        planning.ballista_config().max_partitions_per_task()
    );
    start_server(cluster, addr, Arc::new(config)).await?;
    Ok(())
}

async fn run_executor(o: ExecutorOpts, grpc_limit: usize) -> Result<(), Box<dyn Error>> {
    // The negative control removes the codecs from BOTH places - the overrides and the
    // session configuration - so the executor has nowhere to get them from.
    let config_producer: ConfigProducer = if o.codecs {
        Arc::new(|| bio_ballista_config().upgrade_for_ballista())
    } else {
        Arc::new(|| bio_session_config().upgrade_for_ballista())
    };
    let config = ExecutorProcessConfig {
        bind_host: "127.0.0.1".into(),
        external_host: Some("localhost".into()),
        port: o.port,
        grpc_port: o.grpc_port,
        scheduler_host: "localhost".into(),
        scheduler_port: o.scheduler_port,
        // Ballista 55: `concurrent_tasks` was renamed to `vcores` (the number of executor slots).
        vcores: o.concurrent_tasks,
        task_scheduling_policy: TaskSchedulingPolicy::PushStaged,
        work_dir: Some(o.work_dir.clone()),
        override_config_producer: Some(config_producer),
        override_logical_codec: o.codecs.then(bio_logical_codec),
        override_physical_codec: o.codecs.then(bio_physical_codec),
        // gRPC message size limit of the executor server - tasks with the broadcast payload arrive here.
        grpc_max_decoding_message_size: grpc_limit as u32,
        grpc_max_encoding_message_size: grpc_limit as u32,
        // No memory pool, as in Ballista 53. In 55 `None` means an automatic pool
        // (FairSpillPool = 70% of host/cgroup memory, divided among the vcores), and only
        // `Some(0)` gives an unbounded DataFusion pool (`MemoryBudget::Unbounded`). We keep
        // the 53 semantics; the measurement protocol handles execution memory separately.
        memory_pool_size: Some(0),
        // No session runtime-environment cache, as in Ballista 53 (a fresh `RuntimeEnv`
        // per task). The default of 16 in 55 shares the environment between the tasks and
        // queries of a session (object store clients, Parquet footer cache) - measurements
        // must not get "warm" caches that 53 did not have. 0 = disabled.
        session_runtime_cache_capacity: 0,
        ..ExecutorProcessConfig::default()
    };
    println!(
        "ballista_node: executor (flight {}, grpc {}, work dir {}, slots {}, codecs: {}), grpc max message {} B",
        o.port,
        o.grpc_port,
        o.work_dir,
        o.concurrent_tasks,
        if o.codecs { "yes" } else { "NO (negative control)" },
        grpc_limit
    );
    start_executor_process(Arc::new(config)).await?;
    Ok(())
}

fn init_logging() {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
}

fn usage_error(msg: &str) -> ! {
    eprintln!("error: {msg}\n{USAGE}");
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // An environment variable is also input: an invalid one is a usage error (exit code 2)
    // before the node starts anything (otherwise a panic in bio_session_config).
    if let Err(e) = target_partitions() {
        usage_error(&e);
    }
    let grpc_limit = grpc_max_message_bytes().unwrap_or_else(|e| usage_error(&e));
    let rest = args.get(1..).unwrap_or(&[]);
    // Parsing ONLY first (exit code 2), then running (exit code 1).
    let outcome = match args.first().map(String::as_str) {
        Some("scheduler") => {
            let port = parse_flags(rest, &["--port"], &[])
                .and_then(|f| required::<u16>(&f, "--port"))
                .unwrap_or_else(|e| usage_error(&e));
            init_logging();
            run_scheduler(port, grpc_limit).await
        }
        Some("executor") => {
            let opts = parse_executor(rest).unwrap_or_else(|e| usage_error(&e));
            init_logging();
            run_executor(opts, grpc_limit).await
        }
        _ => usage_error("role required: scheduler or executor"),
    };
    if let Err(e) = outcome {
        eprintln!("ballista_node: {e}");
        std::process::exit(1);
    }
}

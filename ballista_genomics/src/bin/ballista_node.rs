//! Zadanie P0: węzeł klastra Ballista jako OSOBNY proces — scheduler albo
//! executor z koderami operacji genomicznych.
//!
//! Jedna binarka z dwiema rolami zamiast dwóch binarek: na tej maszynie
//! dominującym kosztem iteracji jest linkowanie (patrz dist_ops.rs).
//!
//! Wszystkie procesy klastra i klient muszą działać w katalogu
//! `ballista_genomics/`, bo ładunki planu przenoszą ŚCIEŻKI WZGLĘDNE do danych.

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
use ballista_genomics::runner::bio_session_config;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;

const USAGE: &str = "użycie:
  ballista_node scheduler --port <P>
  ballista_node executor --scheduler-port <P> --port <F> --grpc-port <G> \\
                         --work-dir <DIR> --concurrent-tasks <N> [--no-codecs]";

struct ExecutorOpts {
    scheduler_port: u16,
    port: u16,
    grpc_port: u16,
    work_dir: String,
    concurrent_tasks: usize,
    /// `false` = kontrola negatywna: executor bez koderów operacji genomicznych.
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

async fn run_scheduler(port: u16) -> Result<(), Box<dyn Error>> {
    let mut config = SchedulerConfig::default()
        .with_hostname("localhost")
        .with_port(port)
        // Push + round-robin: scheduler sam rozdaje zadania po kolei na
        // wszystkie executory. Przy pull (executory same pytają o pracę)
        // i milisekundowych zadaniach jeden executor potrafi zgarnąć wszystko,
        // a dowód „praca na obu executorach” byłby loterią.
        .with_scheduler_policy(TaskSchedulingPolicy::PushStaged)
        .with_task_distribution(TaskDistributionPolicy::RoundRobin)
        // 0 = nie każ executorom kasować danych zakończonych zadań: pliki etapów
        // w katalogach roboczych są dowodem, kto co liczył.
        .with_finished_job_data_clean_up_interval_seconds(0)
        // Odpowiednik stanu sesji, który standalone przekazuje od klienta.
        // upgrade_for_ballista() dodaje przestrzeń nazw `ballista` — bez niej
        // scheduler cicho odrzuca wszystkie klucze `ballista.*` od klienta.
        .with_override_config_producer(Arc::new(|| bio_ballista_config().upgrade_for_ballista()))
        .with_override_session_builder(Arc::new(bio_session_state));
    config.bind_host = "127.0.0.1".into();
    config.override_logical_codec = Some(bio_logical_codec());
    config.override_physical_codec = Some(bio_physical_codec());

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let cluster = BallistaCluster::new_from_config(&config).await?;
    println!("ballista_node: scheduler nasłuchuje na {addr}");
    start_server(cluster, addr, Arc::new(config)).await?;
    Ok(())
}

async fn run_executor(o: ExecutorOpts) -> Result<(), Box<dyn Error>> {
    // Kontrola negatywna usuwa kodery z OBU miejsc — nadpisań i konfiguracji
    // sesji — żeby executor nie miał skąd ich wziąć.
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
        concurrent_tasks: o.concurrent_tasks,
        task_scheduling_policy: TaskSchedulingPolicy::PushStaged,
        work_dir: Some(o.work_dir.clone()),
        override_config_producer: Some(config_producer),
        override_logical_codec: o.codecs.then(bio_logical_codec),
        override_physical_codec: o.codecs.then(bio_physical_codec),
        ..ExecutorProcessConfig::default()
    };
    println!(
        "ballista_node: executor (flight {}, grpc {}, katalog {}, sloty {}, kodery: {})",
        o.port,
        o.grpc_port,
        o.work_dir,
        o.concurrent_tasks,
        if o.codecs { "tak" } else { "NIE (kontrola negatywna)" }
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
    eprintln!("błąd: {msg}\n{USAGE}");
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rest = args.get(1..).unwrap_or(&[]);
    // Najpierw WYŁĄCZNIE parsowanie (kod 2), potem działanie (kod 1).
    let outcome = match args.first().map(String::as_str) {
        Some("scheduler") => {
            let port = parse_flags(rest, &["--port"], &[])
                .and_then(|f| required::<u16>(&f, "--port"))
                .unwrap_or_else(|e| usage_error(&e));
            init_logging();
            run_scheduler(port).await
        }
        Some("executor") => {
            let opts = parse_executor(rest).unwrap_or_else(|e| usage_error(&e));
            init_logging();
            run_executor(opts).await
        }
        _ => usage_error("podaj rolę: scheduler albo executor"),
    };
    if let Err(e) = outcome {
        eprintln!("ballista_node: {e}");
        std::process::exit(1);
    }
}

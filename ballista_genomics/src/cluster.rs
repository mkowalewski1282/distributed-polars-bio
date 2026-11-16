//! A single source of configuration for the cluster nodes - client, scheduler and executor.
//!
//! In standalone mode the scheduler and executor get the client's READY-MADE session state
//! (`new_standalone_scheduler_from_state` / `new_standalone_executor_from_state`): the bio
//! planner, the optimizer rules and the codecs. A separate process has no access to the
//! client's memory, so it must build an identical state itself - hence these functions,
//! called by `runner` (the client) and `bin/ballista_node.rs` (the nodes).

use std::sync::Arc;

use ballista_core::config::{
    BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, BALLISTA_SCHEDULER_SHARE_FILE_STATISTICS_CACHE,
};
use ballista_core::extension::SessionConfigExt;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::BioSessionExt;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::bio_phys_codec::BioRangesPhysicalCodec;
use crate::logical_codec::BioDistLogicalCodec;
use crate::runner::{bio_session_config, grpc_max_message_bytes};

/// The logical-plan codec (client -> scheduler).
pub fn bio_logical_codec() -> Arc<dyn LogicalExtensionCodec> {
    Arc::new(BioDistLogicalCodec::default())
}

/// The physical-plan codec (scheduler -> executor). Delegates to
/// `IntervalJoinPhysicalCodec`, which delegates to Ballista's default codec.
pub fn bio_physical_codec() -> Arc<dyn PhysicalExtensionCodec> {
    Arc::new(BioRangesPhysicalCodec::default())
}

/// Ballista's adaptive query planner (AQE, `ballista.planner.adaptive.enabled`), set
/// explicitly rather than taken from the default (Ballista 53: off, 55: on, #2315). A single
/// source of truth: the value read from `bio_ballista_config()` is printed by
/// `bench_client --version-info`, and the test `test_aqe_setting_is_explicit` compares it with
/// the value declared in the technical description.
pub const ADAPTIVE_QUERY_PLANNER: bool = true;

/// At most this many input partitions per task (`ballista.scheduler.max_partitions_per_task`).
/// By default (0 = no limit) Ballista 55 packs as many partitions into one task as the executor
/// has free slots (vcores), so with 2 slots a whole two-partition stage went to ONE executor as
/// ONE task. 1 = one task per partition, as in Ballista 53 - tasks spread round-robin across the
/// executors. Does not apply to collecting stages (a single output partition), which Ballista
/// has to run as one task anyway.
pub const MAX_PARTITIONS_PER_TASK: usize = 1;

/// A file-statistics cache in the scheduler shared by all sessions
/// (`ballista.scheduler.share_file_statistics_cache`, new in 55, on by default): later queries
/// would reuse the Parquet footer statistics read by earlier ones. In 53 each session had its
/// own, cold one. Disabled, like the executor's session runtime-environment cache
/// (`ballista_node`): measurements must not get warm caches that 53 did not have.
/// As a safeguard, the scans of our operations are planned by the providers' internal sessions
/// (their own `RuntimeEnv`), which this cache does not cover anyway.
pub const SHARE_FILE_STATISTICS_CACHE: bool = false;

/// `bio_session_config()` with both codecs registered, the client's gRPC message size limit
/// (`ballista.client.grpc_max_message_size`) and explicit Ballista 55 settings (AQE, partitions
/// per task, shared file-statistics cache). This configuration goes to the client and, through
/// `override_config_producer`, to the scheduler (task planning, its gRPC client to the
/// executors) and to the executors.
pub fn bio_ballista_config() -> SessionConfig {
    // The binaries validate the variable at start-up (a usage error, exit code 2), so an
    // invalid value here is a programmer error.
    let limit = grpc_max_message_bytes().unwrap_or_else(|e| panic!("{e}"));
    bio_session_config()
        // Ballista 55 applies its DataFusion settings (`ballista_restricted_configuration`,
        // among them `enable_round_robin_repartition = false`) only on the FIRST
        // `upgrade_for_ballista()`, on a configuration without the `ballista` extension.
        // The setters below add that extension, so a later `upgrade_for_ballista()` (client,
        // scheduler, executors) would skip them - whereas Ballista 53 always applied them.
        // Without this call, standalone mode inserted RoundRobinBatch before the operation
        // nodes (seen in EXPLAIN ANALYZE).
        .upgrade_for_ballista()
        .with_ballista_logical_extension_codec(bio_logical_codec())
        .with_ballista_physical_extension_codec(bio_physical_codec())
        .with_ballista_grpc_client_max_message_size(limit)
        .with_ballista_adaptive_query_planner(ADAPTIVE_QUERY_PLANNER)
        .set_usize(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, MAX_PARTITIONS_PER_TASK)
        .set_bool(BALLISTA_SCHEDULER_SHARE_FILE_STATISTICS_CACHE, SHARE_FILE_STATISTICS_CACHE)
}

/// The bio session state for the given configuration. The scheduler MUST plan on such a
/// state for `IntervalJoinPhysicalOptimizationRule` to work at all.
pub fn bio_session_state(config: SessionConfig) -> Result<SessionState> {
    Ok(DFSessionContext::new_with_bio(config).state())
}

# Technical notes

`ballista_genomics` runs the polars-bio interval operations (overlap, nearest, coverage, merge,
subtract) on Apache Ballista **55.0.0** with DataFusion **55.2.0**. The operations themselves come
from `datafusion-bio-function-ranges` **v0.25.0**, the same algorithm crate polars-bio is built
on; it is vendored in `vendor/` with two visibility patches (`vendor/PATCH.md`). This crate adds
the parts Ballista needs to run them on a cluster: a table provider that keeps the operation in
the logical plan (`src/dist_provider.rs`), logical and physical extension codecs that ship the
operation nodes from the client to the scheduler and from the scheduler to the executors
(`src/logical_codec.rs`, `src/physical_codec.rs`, `src/bio_phys_codec.rs`), a coverage node that
carries its left table (`src/coverage_node.rs`) and the binaries in `src/bin/`.

## Building

- Rust from `rust-toolchain.toml`; `protoc` must be installed (the `substrait` crate, a
  dependency of the Ballista scheduler, needs it in its build script).
- `cargo build --bins` builds the debug binaries the tests use;
  `cargo build --release --bins` builds the binaries for measurements.
- **Memory limits.** Linking one of these binaries (the Ballista scheduler and executor with all
  of DataFusion) takes several GB of RAM. On a machine with little memory build with
  `CARGO_BUILD_JOBS=1`, so that only one binary is linked at a time; the `dev` profile has no
  debug info for the same reason (with full debug info the linker exhausted RAM and swap on the
  development laptop).

## Distribution strategy per operation

| Operation | Strategy |
|---|---|
| overlap | `IntervalJoinExec` in `CollectLeft` mode: every task indexes the whole left side (COITrees); the parallelism comes from the partitions of the right side. With several left partitions the left side is a separate stage, read in full by each join task through `CoalescePartitionsExec`. |
| merge | a hash shuffle on the chromosome column, then `MergeExec` per partition |
| subtract | both sides shuffled by a hash of the chromosome column, then `SubtractExec` per partition |
| nearest | the left table travels in the physical-plan payload (Arrow IPC) to every task; the parallelism comes from the partitions of the right side |
| coverage | as nearest, with our carrier node `DistCoverageExec` (the vendored `CountOverlapsExec` does not keep its left table) |

Overlap gets no hash-partitioned parallelism: the vendored code creates `IntervalJoinExec` in
`PartitionMode::Auto`, and the crate resolves it to `CollectLeft` at planning time
(`collect_left_at_planning`, `src/dist_provider.rs`). Ballista 55 restricts the leaves of a task's
plan to the task's partitions unless an input requires `SinglePartition`; resolving the mode
before the scheduler builds the tasks keeps the whole left side in every task.
`tests/test_ballista_distribution_evidence.py` checks the plan shape of every operation.

## Ballista 55 configuration

Settings whose defaults changed between Ballista 53 and 55 are set explicitly (`src/cluster.rs`,
`src/bin/ballista_node.rs`). `bench_client --version-info` prints the values read from the cluster
configuration; `test_aqe_setting_is_explicit` and `test_scheduling_settings_are_explicit`
(`tests/test_ballista_distribution_evidence.py`) compare them with the declarations below.

- `ballista.planner.adaptive.enabled` = `true` — the adaptive query planner (AQE) is on (the
  Ballista 55 default; 53 had it off). With AQE the strategies above are unchanged; only the plan
  shape differs: stages are numbered from 0, and the final ordered merge reads through
  `RangeShuffleReaderExec`.
- `ballista.scheduler.max_partitions_per_task` = `1` — one input partition per task. The default
  (0, no limit) packs as many partitions into one task as the executor has free slots, so with
  two slots a two-partition stage ran as a single task on a single executor.
- `ballista.scheduler.share_file_statistics_cache` = `false` — the scheduler's file-statistics
  cache shared across sessions (new in 55, on by default) is off, so every session starts cold.
- Ballista's DataFusion settings (`ballista_restricted_configuration`, among them
  `enable_round_robin_repartition = false`) are applied only by the first
  `upgrade_for_ballista()` call on a configuration without the `ballista` extension;
  `bio_ballista_config` therefore calls it before the Ballista setters. Without it, standalone
  mode inserted `RoundRobinBatch` repartitioning in front of the operation nodes.
- Executors (`ballista_node`): an unbounded memory pool (`memory_pool_size: Some(0)`) and no
  session runtime cache (`session_runtime_cache_capacity: 0`, a fresh `RuntimeEnv` per task).
- Push-based scheduling (`TaskSchedulingPolicy::PushStaged`), round-robin task distribution and a
  gRPC message limit of 512 MiB on the client, the scheduler and the executors (the variable
  `BALLISTA_GRPC_MAX_MESSAGE_BYTES` changes it): the nearest and coverage plans carry the whole
  left table.

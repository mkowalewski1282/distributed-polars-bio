# Technical description

`ballista_genomics` runs the polars-bio interval operations (overlap, nearest, coverage, merge,
subtract) on Apache Ballista **55.0.0** with DataFusion **55.2.0**. The operations themselves
come from `datafusion-bio-function-ranges` **v0.25.0**, the algorithm crate polars-bio is built on
(polars-bio 0.37 uses v0.24.0 with the same interval algorithms; v0.25.0 ports it to
DataFusion 55). The crate is vendored in `vendor/` with two visibility patches
(`vendor/PATCH.md`). Neither Ballista nor DataFusion is patched.

This document describes how the integration works: the processes and what each of them builds,
what is serialized where, the broadcast of nearest and coverage, how each operation executes,
the Ballista settings the plans depend on, and the known limitations. The README has the query
flow and component diagrams, build instructions and examples.

Contents:

1. [Processes and session state](#1-processes-and-session-state)
2. [From SQL to the logical plan](#2-from-sql-to-the-logical-plan)
3. [Serialization](#3-serialization)
4. [Physical planning and stages](#4-physical-planning-and-stages)
5. [Broadcast of the indexed table (nearest, coverage)](#5-broadcast-of-the-indexed-table-nearest-coverage)
6. [Per-operation execution](#6-per-operation-execution)
7. [Ballista 55 configuration](#7-ballista-55-configuration)
8. [Known limitations](#8-known-limitations)
9. [The vendored algorithm crate](#9-the-vendored-algorithm-crate)
10. [Building](#10-building)
11. [Reproducing the plans](#11-reproducing-the-plans)

## 1. Processes and session state

A query involves three kinds of processes:

| Process | Binary | Builds | Receives |
|---|---|---|---|
| client | `bench_client`, `dist_ops`, `ballista_genomics` | the logical plan | the result (Arrow Flight) |
| scheduler | `ballista_node scheduler` | the physical plan, its stages and the tasks | the logical plan (gRPC) |
| executor | `ballista_node executor` | nothing; runs the tasks it receives | the physical plan of each task (gRPC), shuffle partitions of earlier stages (Arrow Flight) |

In standalone mode (`BALLISTA_SCHEDULER_URL` unset) `SessionContext::standalone_with_state`
starts a scheduler and one executor inside the client process and hands them the client's
session state. Separate processes have no access to that state, so every process builds the same
state itself from one place, `src/cluster.rs`:

- `bio_session_config()` (`src/runner.rs`): a DataFusion `SessionConfig` with
  `BioConfig::default()` registered explicitly (`interval_join_algorithm = Coitrees`,
  `interval_join_low_memory = false`) and `target_partitions` from `BIO_TARGET_PARTITIONS`;
- `bio_ballista_config()`: the same configuration upgraded for Ballista
  (`upgrade_for_ballista()`), with both codecs (`with_ballista_logical_extension_codec`,
  `with_ballista_physical_extension_codec`), the client gRPC message limit and the explicit
  Ballista settings of [section 7](#7-ballista-55-configuration);
- `bio_session_state(config)`: `SessionContext::new_with_bio(config).state()`, a session with the
  vendored bio planner and `IntervalJoinPhysicalOptimizationRule`.

Where each process gets it:

- client: `runner::connect_from_env()` builds `bio_session_state(bio_ballista_config())` and
  passes it to `SessionContext::remote_with_state` (or `standalone_with_state`). Ballista
  replaces the query planner with `BallistaQueryPlanner`, so the client only builds logical plans;
  `connect_from_env` then registers the five `dist_*` table functions.
- scheduler (`run_scheduler` in `src/bin/ballista_node.rs`): `SchedulerConfig` with
  `override_config_producer` = `bio_ballista_config().upgrade_for_ballista()`,
  `override_session_builder` = `bio_session_state` (the scheduler must plan on a bio session),
  `override_logical_codec` and `override_physical_codec`, push scheduling
  (`TaskSchedulingPolicy::PushStaged`) with round-robin task distribution
  (`TaskDistributionPolicy::RoundRobin`), and the gRPC server limits.
- executor (`run_executor`): `ExecutorProcessConfig` with the same config producer and codecs,
  `vcores` = `--concurrent-tasks` (task slots), push scheduling, the gRPC server limits, an
  unbounded memory pool and no session runtime cache ([section 7](#7-ballista-55-configuration)).
  `--no-codecs` removes the codecs from both places: a negative control that cannot decode any
  plan containing our nodes.

The `ballista` extension namespace has to be present on the scheduler side
(`upgrade_for_ballista()` in the config producer); without it the scheduler silently drops every
`ballista.*` key the client sends with the query.

Module map:

| Module | Contents |
|---|---|
| `dist_udtf` | `DistTableFunction`: the `dist_*` SQL table functions, argument parsing |
| `dist_payload` | `DistOp` (operation tag), `DistPayload` (constructor arguments), `TableRef`, `Cols` |
| `dist_provider` | `DistBioProvider` (wrapper `TableProvider`), `collect_left_at_planning` |
| `coverage_node` | `DistCoverageProvider`, `DistCoverageExec`, `build_count_overlaps_exec` |
| `logical_codec` | `BioDistLogicalCodec` |
| `bio_phys_codec` | `BioRangesPhysicalCodec`, `placeholder_props`, `recompute_with_children` |
| `physical_codec` | `IntervalJoinPhysicalCodec`, `INTERVAL_JOIN_ALGORITHM`, `INTERVAL_JOIN_LOW_MEMORY` |
| `codec_io` | binary encoding primitives, schema and Arrow IPC helpers, `ipc_len` |
| `broadcast` | `check_broadcast_fits`, `MESSAGE_SIZE_MARGIN` |
| `cluster` | session configuration and state shared by all processes, explicit Ballista settings |
| `runner` | `connect_from_env`, environment variables, the `dist_ops` queries and output |
| `scenario` | `bench_client` scenario → SQL in a normalized output schema |
| `checksum` | row-order-independent checksum of a result (`bench_client`) |
| `cli` | strict flag parsing of the binaries |

## 2. From SQL to the logical plan

The client registers one `DistTableFunction` per operation. Calling it in SQL parses literal
arguments into a `DistPayload` and builds a `DistBioProvider` (`DistBioProvider::build`):

1. a new, private bio session with the same `bio_session_config()` as the query session;
2. every input table registered in it: a `*.parquet` file or a directory containing `*.parquet`
   files is Parquet, anything else is CSV; a missing path is a planning error naming it;
3. the vendored provider constructed with these tables (`OverlapProvider`, `MergeProvider`,
   `SubtractProvider`, `NearestProvider`), or our `DistCoverageProvider` for coverage.

`DistBioProvider` exposes the inner provider's schema and keeps the `DistPayload` beside it. The
private session must use the same configuration as the scheduler's session: it builds the
physical plans of the inputs, so its `target_partitions` decides how many partitions the scans
have, and the scheduler's distribution rules work from that number.

SQL signatures (`'strict'` = 0-based, half-open coordinates, as in polars-bio and databio-8p;
`'weak'` = 1-based, closed; the argument is mandatory):

```sql
dist_overlap (left_table, left_path, right_table, right_path, col_chrom, col_start, col_end, 'strict'|'weak')
dist_merge   (table, path, col_chrom, col_start, col_end, min_dist, 'strict'|'weak')
dist_subtract(left_table, left_path, right_table, right_path, col_chrom, col_start, col_end, 'strict'|'weak')
dist_nearest (left_table, left_path, right_table, right_path, k, include_overlaps, compute_distance,
              col_chrom, col_start, col_end, 'strict'|'weak')
dist_coverage(reads_table, reads_path, targets_table, targets_path, col_chrom, col_start, col_end, 'strict'|'weak')
```

The table names only name the tables inside the provider's private session; the paths are what
the other processes use. `bench_client` builds these calls from `--op/--left/--right/--cols`
(`src/scenario.rs`) and renames the output to a normalized schema (`chrom_1, start_1, end_1,
chrom_2, ...`).

## 3. Serialization

### 3.1 Logical plan: client → scheduler

On execution the client's `BallistaQueryPlanner` wraps the logical plan in
`DistributedQueryExec`, which serializes it as a datafusion-proto `LogicalPlanNode` and sends it in
`ExecuteQueryParams` (gRPC `execute_query_push`), together with the client's session settings. A
custom `TableProvider` becomes a `CustomTableScanNode` whose bytes come from the session's
`LogicalExtensionCodec`; without one the client fails with "LogicalExtensionCodec is not
provided".

`BioDistLogicalCodec::try_encode_table_provider` writes `DistPayload::encode()` for a
`DistBioProvider` and delegates anything else to Ballista's default codec. On the scheduler
`SchedulerServer::parse_plan` decodes the plan, and `try_decode_table_provider` calls
`DistPayload::decode()` and rebuilds the provider with `DistBioProvider::build`, which registers
the input files again in the scheduler. Nothing of the provider's state is serialized, only the
recipe for rebuilding it:

| Operation | `DistPayload` fields (after the magic `0xD1570004` and the operation tag) |
|---|---|
| overlap | left (name, path), right (name, path), columns, `strict` |
| merge | table (name, path), columns, `min_dist`, `strict` |
| subtract | left, right, left columns, right columns, `strict` |
| nearest | left, right, left columns, right columns, `strict`, `k`, `include_overlaps`, `compute_distance` |
| coverage | left (reads), right (targets), left columns, right columns, `strict`, `coverage` |

One codec serves all operations through the tag because a `SessionConfig` accepts exactly one
`LogicalExtensionCodec`.

### 3.2 Physical plan: scheduler → executors

The scheduler encodes the plan of every task separately (Ballista 55:
`TaskManager::prepare_multi_task_definition`, one `MultiTaskDefinition` per task, after
`restrict_plan_to_partitions` narrowed it to the task's partitions) with
`PhysicalPlanNode::try_from_physical_plan` and the session's physical codec, and sends the tasks
bound to an executor in one scheduling round as one `launch_multi_task` gRPC message
(`LaunchMultiTaskParams`). The executor decodes each task's plan in `get_task_definition_vec`
with `PhysicalPlanNode::try_into_physical_plan` and the same codec. Standard DataFusion nodes
(`DataSourceExec`, `ProjectionExec`, `SortExec`, ...) are protobuf messages of datafusion-proto;
Ballista's own nodes (`ShuffleWriterExec`, `ShuffleReaderExec`, ...) go through Ballista's default
physical codec; our nodes go through the chain

```text
BioRangesPhysicalCodec        magic 0xD1570003 + tag: MergeExec, SubtractExec, NearestExec, DistCoverageExec
  -> IntervalJoinPhysicalCodec   magic 0xD1570002: IntervalJoinExec
       -> BallistaPhysicalExtensionCodec (Ballista's default)
```

A node's children are serialized by datafusion-proto itself (`PhysicalExtensionNode { node,
inputs }`), so each codec writes only the node's own parameters:

| Node | Serialized | Rebuilt on the executor |
|---|---|---|
| `IntervalJoinExec` | `on` pairs and the filter expression as `PhysicalExprNode` (`serialize_physical_expr`), the filter's column indices and sides, `join_type`, `partition_mode`, `null_equals_null`, algorithm, low-memory flag | the filter's intermediate schema from the column indices and the input schemas; `ColIntervals` with `parse_intervals` (the function the optimizer rule uses); the node with `IntervalJoinExec::try_new` |
| `MergeExec` | `strict`, output schema, columns, `min_dist` | struct literal (patch 2) |
| `SubtractExec` | `strict`, output schema, left and right columns, `left_contig_col_idx`, `has_extra_cols` | struct literal (patch 2) |
| `NearestExec` | filter op, output schema, both column triples, `include_overlaps`, `k`, `compute_distance`, **the left table as Arrow IPC** | the index with `build_nearest_indexes` (the function `NearestProvider::scan` uses), then the struct literal |
| `DistCoverageExec` | filter op, output schema, both column triples, `coverage`, **the left table as Arrow IPC** | the inner `CountOverlapsExec` with `build_count_overlaps_exec` (`build_coitree_from_batches` or `build_count_index_from_batches`) |

Details that matter:

- `IntervalJoinExec` has no getters for its algorithm and low-memory mode. The codec writes the
  constants `INTERVAL_JOIN_ALGORITHM` (`Algorithm::Coitrees`) and `INTERVAL_JOIN_LOW_MEMORY`
  (`false`), the values of `BioConfig::default()` that `bio_session_config()` registers in every
  session. The partition mode is restored exactly as planned (`CollectLeft`, see
  [section 6.1](#61-overlap)); a node still in `PartitionMode::Auto` would be rejected by
  `IntervalJoinExec::execute()`. The node's embedded projection is not serialized (the plans here
  never have one; projections are separate `ProjectionExec` nodes).
- `PlanProperties` (`cache`) is not serialized. The decoder builds the node with a placeholder
  (`placeholder_props`) and immediately calls `recompute_with_children`
  (`replace_children(..., ChildrenPropertiesMode::Recompute)`), which ends in the vendored node's
  own `with_new_children` and recomputes partitioning and emission type exactly as the vendored
  code does.
- The output schema is always serialized (as a datafusion-proto `Schema`) rather than derived
  from the parameters, so the decoder does not duplicate each provider's schema logic.

### 3.3 Encoding format

`src/codec_io.rs`: little-endian integers, byte strings and strings prefixed with a `u32`
length, booleans as one byte, schemas as protobuf bytes, record batches as an Arrow IPC stream
(`StreamWriter`; the schema is written first, so an empty batch round-trips). Each payload starts
with a `u32` magic number, which is how a codec recognizes its own nodes and delegates everything
else. The format is not versioned: the client, the scheduler and the executors must run binaries
built from the same source.

## 4. Physical planning and stages

With the adaptive planner on, the scheduler plans a job in `TaskManager::submit_plan` →
`AdaptiveExecutionGraph::try_new` → `AdaptivePlanner::try_new`, which calls
`create_physical_plan` on the scheduler's bio session. For our table scan that is
`DistBioProvider::scan`:

1. the vendored provider's `scan()` (or `DistCoverageProvider::scan`) builds the operation's
   physical node over scans of the input files, planned by the provider's private session;
2. `collect_left_at_planning` rebuilds an `IntervalJoinExec` in `PartitionMode::Auto` as
   `CollectLeft` ([section 6.1](#61-overlap));
3. `check_broadcast_fits` rejects a broadcast table too large for the gRPC message limit
   ([section 5](#5-broadcast-of-the-indexed-table-nearest-coverage)).

The scheduler session's physical optimizer then enforces the input distributions the nodes
declare. In DataFusion 55.2 this is the `EnsureRequirements` rule: `MergeExec` and `SubtractExec`
require `KeyPartitioned` on the chromosome column, so a hash `RepartitionExec` is inserted over
each input with more than one partition; `IntervalJoinExec` in `CollectLeft` requires
`SinglePartition` for its left input, so a `CoalescePartitionsExec` is inserted there.
`NearestExec` and `CountOverlapsExec` declare no requirement. Ballista's adaptive planner
(`DistributedExchangeRule`) turns the hash repartitions and the coalesce into stage boundaries:
the producing stage ends in a `SortShuffleWriterExec` (hash) or `ShuffleWriterExec`, the
consuming stage starts with a `ShuffleReaderExec`. The final stage's output partitions are
fetched by the client over Arrow Flight (`fetch_partition`).

A stage has one task per input partition (`ballista.scheduler.max_partitions_per_task`, section 7).
Before sending a task, Ballista 55 narrows the leaves of its plan to the task's partitions
(`restrict_plan_to_partitions` in `ballista-scheduler/src/state/task_builder.rs`): a file scan
keeps only the task's file group, a shuffle reader only the task's partition. Leaves below a node
that collapses its input (`CoalescePartitionsExec`, `SortPreservingMergeExec`, an input that
requires `SinglePartition`) keep the full upstream. This is what the overlap strategy relies on.

## 5. Broadcast of the indexed table (nearest, coverage)

`NearestExec` and `CountOverlapsExec` hold their left (indexed) side in memory instead of having
it as an input: the vendored providers' `scan()` collects the left table, builds an index from it,
and puts only the right (queried) side under the node as a child. `NearestExec` keeps the table
and the index, `CountOverlapsExec` only the index. The index cannot be serialized (a COITree has
no serde support, and the index types have private fields), but it can be rebuilt from the table.
So the codec sends the table and the executor rebuilds the index:

1. **Planning (scheduler).** `NearestProvider::scan` (vendored) or `DistCoverageProvider::scan`
   materializes the left table as one `RecordBatch`. `CountOverlapsProvider::scan` would discard
   the table after building the index, so coverage uses our `DistCoverageProvider` and
   `DistCoverageExec`, a transparent decorator over the vendored `CountOverlapsExec` that also
   keeps the table, its column names and the coverage flag. `DistCoverageExec` delegates
   properties, distribution requirements, statistics, metrics and execution to the inner node.
   `DistCoverageProvider::scan` also deliberately leaves out the round-robin `RepartitionExec`
   the vendored coverage provider inserts, so the parallelism comes from the partitions of the
   scanned side, as for nearest.
2. **Guard (scheduler).** `check_broadcast_fits` encodes the table to Arrow IPC once to get its
   size (`ipc_len`) and fails planning with a `DataFusionError::Plan` when
   size × `MESSAGE_SIZE_MARGIN` (2) exceeds `BALLISTA_GRPC_MAX_MESSAGE_BYTES`:
   "broadcast table of N bytes (R rows) x 2 (safety margin) exceeds the gRPC message limit of L
   bytes". The error reaches the client as a query error. The margin is the number of table copies
   one `launch_multi_task` message can carry: one per task bound to the executor in a scheduling
   round, at most its number of slots; the rest of a plan is tens of kilobytes.
3. **Task launch (scheduler → executor).** Ballista encodes the plan once per task, so every task
   carries its own copy of the table, and the message to an executor carries one copy per task in
   it.
4. **Decoding (executor).** The codec reads the IPC stream back into one batch, logs
   `broadcast payload decoded: N bytes`, and rebuilds the index for the task.

Observed sizes (Ballista 55, release binaries): `exons` (438 694 rows, databio pair 1-2) gives a
payload of 10 544 588 B; `ex-anno` (1 194 285 rows, pair 2-7) gives 28 751 052 B per task. With two
executors of one slot each and two partitions, a nearest or coverage query decodes the payload
once on each executor. Costs: the scheduler materializes the table while planning, encodes it once
for the guard and once more for every task; every task decodes it and builds the index again.

gRPC limits. Ballista's default message limit is 16 MiB, too small for these payloads. One value,
`BALLISTA_GRPC_MAX_MESSAGE_BYTES` (16 MiB to 4 GiB − 1, default 512 MiB), sets it everywhere:

- client: `ballista.client.grpc_max_message_size` (`with_ballista_grpc_client_max_message_size`
  in `bio_ballista_config`); the scheduler's gRPC client to the executors takes it from the same
  configuration through `override_config_producer`;
- scheduler server: `with_grpc_server_max_decoding_message_size` and
  `with_grpc_server_max_encoding_message_size`;
- executor server: `grpc_max_decoding_message_size` and `grpc_max_encoding_message_size`.

All processes must get the same value: the guard runs in the scheduler and checks the scheduler's
value, and an executor with a smaller limit would hang the query
([section 8](#8-known-limitations)).

## 6. Per-operation execution

The plans below are `EXPLAIN ANALYZE` output of `bench_client --explain-analyze` on a cluster of a
scheduler and two executors with one slot each, `BIO_TARGET_PARTITIONS=2`, release binaries,
databio-8p pair 2-7 (first table `exons`, 438 694 intervals; second `ex-anno`, 1 194 285
intervals; merge on `ex-anno`). Metrics, file lists and the output projections are shortened.
Stage ids start at 0 (adaptive planner). The metrics of a stage are summed over its tasks.

### 6.1 overlap

`dist_overlap` delegates to the vendored `OverlapProvider`, whose `scan()` runs a SQL join in the
provider's private bio session; `IntervalJoinPhysicalOptimizationRule` turns it into an
`IntervalJoinExec` with the COITrees algorithm. The join's first (left, build) input is the
second table argument: the vendored join query reads `FROM <right table> AS b, <left table> AS a`.

```text
=========SuccessfulStage[stage_id=0, partitions=2]=========
ShuffleWriterExec: partitioning: UnknownPartitioning(2), metrics=[output_rows=1.19 M, ...]
  DataSourceExec: file_groups={2 groups: [[.../ex-anno/part-...], ...]}, file_type=parquet

=========SuccessfulStage[stage_id=1, partitions=2]=========
ShuffleWriterExec: partitioning: UnknownPartitioning(2), metrics=[output_rows=273.5 K, ...]
  ProjectionExec: expr=[contig@3 as chrom_1, ..., contig@0 as chrom_2, ...]
    IntervalJoinExec: mode=CollectLeft, join_type=Inner, on=[(contig@0, contig@0)],
        filter=pos_end@3 > pos_start@0 AND pos_start@2 < pos_end@1, alg=Coitrees,
        metrics=[output_rows=273.5 K, build_input_rows=2.39 M, input_rows=438.7 K, ...]
      CoalescePartitionsExec, metrics=[output_rows=2.39 M, ...]
        ShuffleReaderExec: upstream_stage: 0, partitioning: UnknownPartitioning(2)
      DataSourceExec: file_groups={2 groups: [[.../exons/part-...], ...]}, file_type=parquet
```

Every join task indexes the whole build side: `build_input_rows` summed over the two join tasks is
2 × 1 194 285. The probe side is split: `input_rows` = 438.7 K is the whole `exons` table, divided
between the two tasks by file group. There is no hash partitioning of the join (`IntervalJoinExec` would require
`KeyPartitioned` inputs only in `Partitioned` mode, which is not used).

Why the mode is fixed at planning time. The vendored session removes DataFusion's
`join_selection` rule (`with_config_rt_bio()`), which would normally resolve `Auto` to
`Partitioned` or `CollectLeft`, so the optimizer rule leaves the node in `PartitionMode::Auto`. In
`Auto` both inputs have an unspecified distribution, so `restrict_plan_to_partitions` would narrow
the build side to the task's partition as well: a mode chosen later, in the codec, would index
only a slice of the build side in each task and lose pairs without any error.
`collect_left_at_planning` (`src/dist_provider.rs`) therefore rebuilds the node as `CollectLeft`
inside `DistBioProvider::scan`, with the same parameters and the same algorithm. `CollectLeft`
requires `SinglePartition` for the build side; the inserted `CoalescePartitionsExec` keeps the
whole build side in every task, and with several build-side partitions it becomes a separate stage
(scan → shuffle). With one build-side partition (the single CSV files of `dist_ops`) the input
already satisfies `SinglePartition` and the plan has one stage. Ballista's adaptive planner may
also skip the separate stage for a small scan with exact size statistics
(`is_inlinable_scan_pipeline`, the `ballista.optimizer.broadcast_join_threshold_bytes` threshold);
every join task then scans the whole build side itself. The test
`test_overlap_indexes_the_whole_left_side_in_every_task` checks `build_input_rows` = join tasks ×
build-side rows on a Parquet test dataset with two partitions.

### 6.2 merge

`dist_merge` delegates to `MergeProvider`, whose `scan()` builds a `MergeExec` over the scan.
`MergeExec` requires `KeyPartitioned` on the chromosome, so the scan is hash-partitioned and every
chromosome is merged in one task:

```text
=========SuccessfulStage[stage_id=0, partitions=2]=========
SortShuffleWriterExec: partitioning=Hash([contig@0], 2), metrics=[output_rows=1.19 M, ...]
  DataSourceExec: file_groups={2 groups: [[.../ex-anno/part-...], ...]}, file_type=parquet

=========SuccessfulStage[stage_id=1, partitions=2]=========
ShuffleWriterExec: partitioning: UnknownPartitioning(2), metrics=[output_rows=308.9 K, ...]
  ProjectionExec: expr=[contig@0 as chrom, pos_start@1 as start, pos_end@2 as end, n_intervals@3 as n_intervals]
    MergeExec: min_dist=0, strict=true
      ShuffleReaderExec: upstream_stage: 0, partitioning: Hash([contig@0], 2)
```

### 6.3 subtract

`dist_subtract` delegates to `SubtractProvider`. `SubtractExec` has two inputs and its
`execute(i)` reads partition *i* of both, so both sides must be partitioned by the chromosome into
the same partitions; DataFusion aligns the two hash repartitions:

```text
=========SuccessfulStage[stage_id=0, partitions=2]=========
SortShuffleWriterExec: partitioning=Hash([contig@0], 2)
  DataSourceExec: file_groups={2 groups: [[.../exons/part-...], ...]}, file_type=parquet

=========SuccessfulStage[stage_id=1, partitions=2]=========
SortShuffleWriterExec: partitioning=Hash([contig@0], 2)
  DataSourceExec: file_groups={2 groups: [[.../ex-anno/part-...], ...]}, file_type=parquet

=========SuccessfulStage[stage_id=2, partitions=2]=========
ShuffleWriterExec: partitioning: UnknownPartitioning(2), metrics=[output_rows=423.6 K, ...]
  ProjectionExec: expr=[contig@0 as chrom, pos_start@1 as start, pos_end@2 as end]
    SubtractExec: strict=true
      ShuffleReaderExec: upstream_stage: 0, partitioning: Hash([contig@0], 2)
      ShuffleReaderExec: upstream_stage: 1, partitioning: Hash([contig@0], 2)
```

The two source stages can run at the same time, so the tasks are not always spread evenly over
the executors (2 and 4 of the 6 tasks in this run).

### 6.4 nearest

`dist_nearest(left, right, ...)` returns one row per row of the **right** (queried) table with its
nearest interval from the **left** (indexed, broadcast) table. `pb.nearest(df1, df2)` returns one
row per interval of `df1`, so `bench_client` passes `df2` as the left and `df1` as the right
table. `NearestExec` declares no input distribution, so there is no shuffle:

```text
=========SuccessfulStage[stage_id=0, partitions=2]=========
ShuffleWriterExec: partitioning: UnknownPartitioning(2), metrics=[output_rows=438.7 K, ...]
  ProjectionExec: expr=[right_contig@3 as chrom_1, ..., left_contig@0 as chrom_2, ..., distance@6 as distance]
    NearestExec: k=1, include_overlaps=true, compute_distance=true
      DataSourceExec: file_groups={2 groups: [[.../exons/part-...], ...]}, file_type=parquet
```

The left table (`ex-anno`) does not appear as a scan: it is inside `NearestExec`'s payload. Each
executor logged `broadcast payload decoded: 28751052 bytes` once.

### 6.5 coverage

`dist_coverage(reads, targets, ...)` returns the rows of **targets** with their coverage by
**reads**; `pb.coverage(df1, df2)` reports the coverage of `df1` by `df2`, so `bench_client`
passes `df2` as reads (broadcast) and `df1` as targets (scanned):

```text
=========SuccessfulStage[stage_id=0, partitions=2]=========
ShuffleWriterExec: partitioning: UnknownPartitioning(2), metrics=[output_rows=438.7 K, ...]
  ProjectionExec: expr=[contig@0 as chrom, pos_start@1 as start, pos_end@2 as end, coverage@3 as coverage]
    DistCoverageExec: coverage=true, broadcast_rows=1194285
      DataSourceExec: file_groups={2 groups: [[.../exons/part-...], ...]}, file_type=parquet
```

`broadcast_rows` in the node's display is deliberate: it shows in every plan that the table
travelled inside the node, and how large it is.

### 6.6 Sorted results (`dist_ops`)

The `dist_ops` queries end with `ORDER BY`. Each partition is sorted in the operation's stage
(`SortExec ... preserve_partitioning=[true]`), and a final single-partition stage merges them
(`SortPreservingMergeExec` ← `RangeShuffleReaderExec`). On the CSV data in `data/`, with
`BIO_TARGET_PARTITIONS=2`, the stage counts are: overlap 1, merge 3, subtract 4, nearest 2,
coverage 2.

## 7. Ballista 55 configuration

The settings below are set explicitly (`src/cluster.rs`, `src/bin/ballista_node.rs`) because the
plan shapes and the work distribution described above depend on them and their Ballista defaults
differ. `bench_client --version-info` prints the values read from the cluster configuration;
`test_aqe_setting_is_explicit` and `test_scheduling_settings_are_explicit`
(`tests/test_ballista_distribution_evidence.py`) compare them with the declarations below.

- `ballista.planner.adaptive.enabled` = `true` — the adaptive query planner (AQE) is on (also the
  Ballista 55 default; set explicitly so that a change of the default cannot change the plans
  silently). With AQE stages are numbered from 0 and the final ordered merge reads through
  `RangeShuffleReaderExec`; the strategies of section 6 are the same with the static planner
  (stages numbered from 1, `ShuffleReaderExec` in the final merge).
- `ballista.scheduler.max_partitions_per_task` = `1` — one input partition per task. The default
  (0, no limit) packs as many partitions into one task as the executor has free slots, so with
  two slots a two-partition stage ran as a single task on a single executor. With 1, a stage with
  P partitions becomes P tasks spread round-robin over the executors.
- `ballista.scheduler.share_file_statistics_cache` = `false` — the scheduler's file-statistics
  cache shared across sessions is off, so every session starts cold and a query is not sped up by
  the statistics an earlier one read. (The scans of our operations are planned by the providers'
  private sessions, which this cache does not cover anyway.)
- Ballista's DataFusion settings (`ballista_restricted_configuration`, among them
  `datafusion.optimizer.enable_round_robin_repartition = false`) are applied only by the first
  `upgrade_for_ballista()` call on a configuration without the `ballista` extension. Our Ballista
  setters add that extension, so `bio_ballista_config` calls `upgrade_for_ballista()` before them.
  Without it, standalone mode inserted `RepartitionExec: partitioning=RoundRobinBatch(4)` in front
  of the operation nodes.
- Executors (`ballista_node`): an unbounded memory pool (`memory_pool_size: Some(0)`; in Ballista
  55 `None` means an automatic `FairSpillPool` of 70% of the host memory divided among the slots)
  and no session runtime cache (`session_runtime_cache_capacity: 0`, a fresh `RuntimeEnv` per
  task), so tasks and queries do not share object-store clients or Parquet footer caches.
- Push-based scheduling (`TaskSchedulingPolicy::PushStaged`) with round-robin task distribution
  (`TaskDistributionPolicy::RoundRobin`): the scheduler hands tasks to all executors in turn. With
  pull scheduling and millisecond tasks one executor can take all the work.
- `with_finished_job_data_clean_up_interval_seconds(0)` on the scheduler: the delayed clean-up of
  a finished job's data is off, so the stage files stay in the executors' work directories
  (`work_dir/job_id/stage_id/...`); the multi-process tests read them to see which executor
  computed which stage. Long-running clusters need their work directories cleaned by hand.
- The gRPC message limit of 512 MiB on the client, the scheduler and the executors
  (`BALLISTA_GRPC_MAX_MESSAGE_BYTES`, [section 5](#5-broadcast-of-the-indexed-table-nearest-coverage)).
- `target_partitions` = `BIO_TARGET_PARTITIONS` (an integer ≥ 2, default 4) in every session of
  every process, including the providers' private sessions.

## 8. Known limitations

- **Standalone mode and the broadcast.** In standalone mode the in-process executor pulls its
  tasks through a scheduler client created by `SessionContext::standalone_with_state` with
  `SchedulerGrpcClient::connect`, that is with tonic's default 4 MiB decoding limit, which this
  crate cannot change. A poll response with nearest or coverage tasks on real data is larger (the
  broadcast of `exons` alone is 10.5 MB), the executor retries forever, and the query hangs instead
  of failing. Observed on pair 1-2 with Ballista 55. Overlap, merge and subtract work in standalone
  mode; for nearest and coverage on real data use a cluster of separate processes.
- **A message over an executor's limit hangs the query.** When a `launch_multi_task` message
  exceeds the executor's decoding limit, Ballista 55 in push mode does not fail the query: the
  scheduler logs "Failed to launch new task ... decoded message length too large", removes the
  executor, retries after about 60 s, and the client waits until its own timeout. Observed with
  executors limited to 16 MiB and a 21 113 832 B message (nearest on pair 1-2, two slots per
  executor). The broadcast guard prevents this when every process uses the same
  `BALLISTA_GRPC_MAX_MESSAGE_BYTES`.
- **Overlap indexes the whole build side in every task.** The join has no hash-partitioned
  parallelism: its parallelism comes from the probe side's partitions, and every join task
  fetches and indexes the whole build side ([section 6.1](#61-overlap)). Memory and index-building
  time per task grow with the build side, not with the number of tasks.
- **The broadcast table goes to every task.** Nearest and coverage send their indexed table once
  per task (one copy per task in the scheduler's messages and in executor memory), and the
  scheduler materializes it while planning; `NearestProvider::scan` also builds the nearest index
  once in the scheduler. The table must fit, with the margin, in the gRPC message limit.
- **`target_partitions` ≥ 2.** With 1 partition per scan DataFusion inserts no hash
  repartitioning (it is needed only for inputs with more than one partition), so merge and
  subtract would run in one stage on one executor and the query would be correct but not
  distributed. The binaries reject `BIO_TARGET_PARTITIONS` < 2 with exit code 2. A different value
  in different processes breaks the distribution silently.
- **Shared file system.** The logical payload carries the input paths as given; the client and
  the scheduler resolve them in their working directories when they register the tables, and the
  physical plan carries absolute file paths that every executor opens itself. All processes must
  see the same files at the same paths (on several machines: shared storage), and the client and
  the scheduler must run in the same directory when relative paths are used.
- **Same build everywhere.** The codec payloads are not versioned, and the codecs assume the
  algorithm and memory mode of `BioConfig::default()`; the client, the scheduler and the executors
  must run binaries built from the same source.
- **Executor start-up.** An executor connects to the scheduler once, at start-up
  (`scheduler_connect_timeout_seconds` = 0, the Ballista default): start the scheduler first.
- **Operations.** Only the five operations are exposed. The vendored `cluster` operation cannot
  be distributed: its `ClusterIdCoordinator` is an in-process barrier (`vendor/PATCH.md`).

## 9. The vendored algorithm crate

`vendor/datafusion-bio-function-ranges/` is `datafusion/bio-function-ranges` from tag v0.25.0 of
[biodatageeks/datafusion-bio-functions](https://github.com/biodatageeks/datafusion-bio-functions)
(Apache-2.0, BiodataGeeks Team) with two patches that change visibility only:

- **patch 1:** `pub mod intervals` and a re-export of `ColInterval`, `ColIntervals` and
  `parse_intervals`: `IntervalJoinExec::try_new` takes a `ColIntervals`, which could not be named
  from outside the crate;
- **patch 2:** `pub` on `MergeExec`, `SubtractExec`, `NearestExec`, `CountOverlapsExec` and their
  fields, on `CountOverlapsIndex` and `build_nearest_indexes`, and re-exports including
  `build_coitree_from_batches` and `build_count_index_from_batches`: these nodes have no
  constructors or getters, so a codec could neither read nor build them.

`vendor/PATCH.md` lists every change, the origin and licence of the copy, the command that checks
the copy against the clean upstream tag (only `pub`, re-exports and comments may differ), and how
to move the patches to a new upstream version. Both patches are candidates for upstream; once
they are released, the dependency can go back to the published crate. All construction of the
patched nodes is confined to the `build_*_exec` functions (`src/bio_phys_codec.rs`,
`src/coverage_node.rs`), so a different upstream API shape (constructors and getters instead of
public fields) changes only those functions.

## 10. Building

- Rust from `rust-toolchain.toml`; `protoc` must be installed (the `substrait` crate, a
  dependency of the Ballista scheduler, needs it in its build script; CI uses 28.3).
- `cargo build --bins` builds the debug binaries the tests use;
  `cargo build --release --bins` builds the binaries for measurements.
- **Memory limits.** Every binary links the Ballista scheduler and executor with all of
  DataFusion. On a machine with little memory build with `CARGO_BUILD_JOBS=1`, so that only one
  binary is linked at a time; the `dev` profile has no debug info for the same reason (with full
  debug info the linker exhausted RAM and swap on a laptop with 5 GB of RAM for Linux). With one
  job a cold release build takes on the order of an hour on a 6-core laptop.

## 11. Reproducing the plans

The plans of section 6 come from a cluster started as below (release binaries, repository root;
`$D` is the databio-8p directory). Every process gets the same `BIO_TARGET_PARTITIONS`:

```bash
export BIO_TARGET_PARTITIONS=2
./target/release/ballista_node scheduler --port 50050 &
mkdir -p /tmp/ex1 /tmp/ex2
./target/release/ballista_node executor --scheduler-port 50050 --port 50051 --grpc-port 50052 \
    --work-dir /tmp/ex1 --concurrent-tasks 1 &
./target/release/ballista_node executor --scheduler-port 50050 --port 50061 --grpc-port 50062 \
    --work-dir /tmp/ex2 --concurrent-tasks 1 &
export BALLISTA_SCHEDULER_URL=df://localhost:50050
./target/release/bench_client --op overlap --left $D/exons --right $D/ex-anno --explain-analyze overlap_2-7.txt
./target/release/bench_client --op merge --left $D/ex-anno --explain-analyze merge_7.txt
```

`dist_ops <operation>` writes the plan of its CSV query to `output/dist_<operation>_explain.txt`
(or to `$DIST_OUTPUT_DIR`); `tests/test_ballista_distribution_evidence.py` parses these files.
With `RUST_LOG=info` the executors log every received broadcast payload
(`broadcast payload decoded: N bytes`) and every task they run.

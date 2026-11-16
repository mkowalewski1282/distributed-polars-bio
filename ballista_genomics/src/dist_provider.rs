//! `DistBioProvider` - a single, generic `TableProvider` wrapping the actual
//! provider from datafusion-bio-function-ranges.
//!
//! Why wrap: the fields of the vendored providers are private, so they cannot be
//! read during serialization. `DistBioProvider` keeps next to them an explicit
//! `DistPayload` with the constructor parameters, and `scan()` delegates to the
//! real provider - so the algorithmic engine (COITrees) is still used and
//! nothing is reimplemented.
//!
//! One type instead of one type per operation. Adding another operation = one enum
//! variant + one `match` arm in `build()`.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::{
    BioSessionExt, FilterOp, IntervalJoinExec, MergeProvider, NearestProvider, OverlapProvider,
    SubtractProvider,
};

use crate::broadcast::check_broadcast_fits;
use crate::coverage_node::DistCoverageProvider;
use crate::dist_payload::{DistPayload, TableRef};
use crate::physical_codec::{INTERVAL_JOIN_ALGORITHM, INTERVAL_JOIN_LOW_MEMORY};
use crate::runner::{bio_session_config, grpc_max_message_bytes};

pub struct DistBioProvider {
    inner: Arc<dyn TableProvider>,
    payload: DistPayload,
}

impl fmt::Debug for DistBioProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DistBioProvider({:?})", self.payload)
    }
}

impl DistBioProvider {
    pub fn payload(&self) -> &DistPayload {
        &self.payload
    }

    /// Builds its own, one-off bio session (it does not share state with the query's
    /// session) - registers the data sources (Parquet or CSV, see
    /// `register_source`) and constructs the actual vendored provider.
    ///
    /// Building the session ourselves on every reconstruction is DELIBERATE: it is
    /// exactly what a real executor in a separate process would have to do
    /// (it has no access to the client's memory) - so the same code works both in
    /// in-process standalone mode and in a multi-process deployment.
    ///
    /// The session uses THE SAME `bio_session_config()` as the client/scheduler session.
    /// This matters: this session builds the physical plans of the children, so it decides
    /// the number of partitions on which the scheduler's `EnforceDistribution` later bases its
    /// decision to insert a shuffle. A configuration mismatch between these two places
    /// silently broke the distribution.
    pub async fn build(payload: DistPayload) -> Result<Self> {
        let session = Arc::new(DFSessionContext::new_with_bio(bio_session_config()));

        for t in payload.tables() {
            register_source(&session, t).await?;
        }

        let inner: Arc<dyn TableProvider> = match &payload {
            DistPayload::Overlap {
                left,
                right,
                cols,
                strict,
            } => {
                let left_schema = session.table(&left.name).await?.schema().as_arrow().clone();
                let right_schema = session.table(&right.name).await?.schema().as_arrow().clone();
                let filter_op = if *strict {
                    FilterOp::Strict
                } else {
                    FilterOp::Weak
                };
                Arc::new(OverlapProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    left_schema,
                    right_schema,
                    cols.as_vec(),
                    cols.as_vec(),
                    filter_op,
                ))
            }
            DistPayload::Merge {
                table,
                cols,
                min_dist,
                strict,
            } => Arc::new(MergeProvider::new(
                Arc::clone(&session),
                table.name.clone(),
                cols.as_tuple(),
                *min_dist,
                if *strict { FilterOp::Strict } else { FilterOp::Weak },
            )),
            DistPayload::Subtract {
                left,
                right,
                lcols,
                rcols,
                strict,
            } => {
                let left_schema = session.table(&left.name).await?.schema().as_arrow().clone();
                Arc::new(SubtractProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    lcols.as_tuple(),
                    rcols.as_tuple(),
                    if *strict { FilterOp::Strict } else { FilterOp::Weak },
                    left_schema,
                ))
            }
            DistPayload::Nearest {
                left,
                right,
                lcols,
                rcols,
                strict,
                k,
                include_overlaps,
                compute_distance,
            } => {
                let left_schema = session.table(&left.name).await?.schema().as_arrow().clone();
                let right_schema = session.table(&right.name).await?.schema().as_arrow().clone();
                Arc::new(NearestProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    left_schema,
                    right_schema,
                    lcols.as_vec(),
                    rcols.as_vec(),
                    if *strict { FilterOp::Strict } else { FilterOp::Weak },
                    *include_overlaps,
                    *k as usize,
                    *compute_distance,
                ))
            }
            DistPayload::Coverage {
                left,
                right,
                lcols,
                rcols,
                strict,
                coverage,
            } => {
                let right_schema = session.table(&right.name).await?.schema().as_arrow().clone();
                Arc::new(DistCoverageProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    right_schema,
                    lcols.as_tuple(),
                    rcols.as_tuple(),
                    if *strict { FilterOp::Strict } else { FilterOp::Weak },
                    *coverage,
                ))
            }
        };

        Ok(Self { inner, payload })
    }
}

/// Registers a table's data source in the receiver's session. The format is recognized from
/// the path: a `*.parquet` file or a directory containing `*.parquet` files ->
/// Parquet (the databio-8p datasets), anything else -> CSV (the small test data).
/// A path that does not exist is an error naming it - previously the registration
/// error was swallowed and the query failed later with a misleading "table not
/// found". A name that is already registered (the same table twice in one payload) is
/// skipped.
async fn register_source(session: &DFSessionContext, t: &TableRef) -> Result<()> {
    if session.table_exist(t.name.as_str())? {
        return Ok(());
    }
    let path = Path::new(&t.path);
    if !path.exists() {
        return Err(DataFusionError::Plan(format!(
            "no data for table {}: {}",
            t.name, t.path
        )));
    }
    if is_parquet_source(path) {
        session
            .register_parquet(&t.name, &t.path, ParquetReadOptions::default())
            .await
    } else {
        session
            .register_csv(&t.name, &t.path, CsvReadOptions::new())
            .await
    }
}

fn is_parquet_source(path: &Path) -> bool {
    let is_parquet = |p: &Path| p.extension().is_some_and(|ext| ext == "parquet");
    if path.is_dir() {
        std::fs::read_dir(path)
            .map(|entries| entries.flatten().any(|entry| is_parquet(&entry.path())))
            .unwrap_or(false)
    } else {
        is_parquet(path)
    }
}

/// Resolves the mode of `IntervalJoinExec` (overlap) from `Auto` to `CollectLeft` already
/// during physical planning in the scheduler.
///
/// A node produced by `IntervalJoinPhysicalOptimizationRule` has mode `Auto`
/// (`with_config_rt_bio()` removes the `join_selection` rule), and in it both inputs have an
/// unspecified distribution. Before sending a task, the Ballista 55 scheduler narrows the plan's
/// leaves to the partitions of that task (`restrict_plan_to_partitions`), sparing only the
/// inputs that must be merged - so with `Auto` it also narrowed the left (build) side, and the
/// codec, which turned `Auto` into `CollectLeft` only at decode time, built the index from a
/// slice of the left table (lost pairs). Ballista 53 sent every task the whole stage plan, so
/// that was enough there.
///
/// `CollectLeft` requires `SinglePartition` for the left side: the planner inserts a
/// `CoalescePartitionsExec` above it, which the scheduler does not narrow, and each join task
/// indexes the whole left side and shares the right side with the other tasks - the same
/// algorithm (COITrees) and the same mode as before, just fixed earlier. Cost: with several
/// partitions the left side becomes a separate stage (scan -> shuffle), read in full by every
/// join task. The algorithm and memory mode come from the same constants as in the codec (the
/// node has no getters).
fn collect_left_at_planning(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(join) = node.downcast_ref::<IntervalJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        if join.mode != PartitionMode::Auto {
            return Ok(Transformed::no(node));
        }
        let resolved: Arc<dyn ExecutionPlan> = Arc::new(IntervalJoinExec::try_new(
            Arc::clone(&join.left),
            Arc::clone(&join.right),
            join.on.clone(),
            join.filter.clone(),
            join.intervals.clone(),
            &join.join_type,
            join.projection.clone(),
            PartitionMode::CollectLeft,
            join.null_equals_null,
            INTERVAL_JOIN_ALGORITHM,
            INTERVAL_JOIN_LOW_MEMORY,
        )?);
        Ok(Transformed::yes(resolved))
    })
    .data()
}

#[async_trait]
impl TableProvider for DistBioProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        let plan = collect_left_at_planning(plan)?;
        // The broadcast guard: physical planning happens in the scheduler, so the error
        // reaches the client as a query error instead of hanging the query when tasks
        // are sent to the executors.
        let grpc_limit = grpc_max_message_bytes().map_err(DataFusionError::Configuration)?;
        check_broadcast_fits(&plan, grpc_limit)?;
        Ok(plan)
    }
}

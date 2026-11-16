//! Coverage - the only one of the four operations that requires its OWN
//! carrier node, and not just a visibility patch.
//!
//! The reason (verified in the source, count_overlaps.rs:122-141):
//! `CountOverlapsProvider::scan()` materializes the left table, MOVES it into the
//! index constructor and discards it. The resulting `CountOverlapsExec` has the fields
//! `schema, index, right, columns_2, filter_op, cache` - so it has NO:
//!   - data of the left table (it cannot be recovered),
//!   - `columns_1` (the left table's column names, needed to rebuild the index),
//!   - `coverage` flag (it is only an enum variant).
//! And the index itself cannot be read: `COITree` has no serde, and
//! `CountOverlapIndex` has private fields.
//!
//! The solution: `DistCoverageExec` - a TRANSPARENT DECORATOR that delegates
//! everything to the inner `CountOverlapsExec`, but additionally carries what
//! that one loses. For the optimizer and for Ballista's DistributedPlanner it
//! behaves identically to the inner node.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, InputDistributionRequirements,
    PlanProperties, StatisticsArgs,
};
use datafusion::prelude::SessionContext;
use datafusion_bio_function_ranges::{
    build_coitree_from_batches, build_count_index_from_batches, CountOverlapsExec,
    CountOverlapsIndex, CountOverlapsProvider, FilterOp,
};

/// Builds the vendored node from an explicitly given batch of the left table.
///
/// We deliberately never name the index types (`AHashMap`, `COITree`) -
/// inference matches them nominally from the public builder functions,
/// so our Cargo.toml needs neither `ahash` nor `coitrees`.
pub fn build_count_overlaps_exec(
    right: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    left_batch: &RecordBatch,
    columns_1: &(String, String, String),
    columns_2: (String, String, String),
    filter_op: FilterOp,
    coverage: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let cols = (
        columns_1.0.as_str(),
        columns_1.1.as_str(),
        columns_1.2.as_str(),
    );
    let batches = vec![left_batch.clone()];
    let index = if coverage {
        CountOverlapsIndex::Coverage(Arc::new(build_coitree_from_batches(batches, cols, true)?))
    } else {
        CountOverlapsIndex::Count(Arc::new(build_count_index_from_batches(batches, cols)?))
    };

    let exec = CountOverlapsExec {
        schema: Arc::clone(&schema),
        index,
        right: Arc::clone(&right),
        columns_2: Arc::new(columns_2),
        filter_op,
        // A dummy; `recompute_with_children` below recomputes the cache exactly as
        // the vendored code would (including EmissionType::Final).
        cache: crate::bio_phys_codec::placeholder_props(schema),
    };
    crate::bio_phys_codec::recompute_with_children(Arc::new(exec), vec![right])
}

/// A transparent decorator over `CountOverlapsExec`.
pub struct DistCoverageExec {
    pub left_batch: Arc<RecordBatch>,
    pub columns_1: Arc<(String, String, String)>,
    pub columns_2: Arc<(String, String, String)>,
    pub filter_op: FilterOp,
    pub coverage: bool,
    pub inner: Arc<dyn ExecutionPlan>,
}

impl Debug for DistCoverageExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DistCoverageExec(coverage={}, broadcast_rows={})",
            self.coverage,
            self.left_batch.num_rows()
        )
    }
}

impl DisplayAs for DistCoverageExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        // `broadcast_rows` is deliberately in the output: it appears in EXPLAIN ANALYZE
        // and becomes part of the evidence that the left table actually travelled
        // in the plan payload (and material for judging the 16 MB limit).
        write!(
            f,
            "DistCoverageExec: coverage={}, broadcast_rows={}",
            self.coverage,
            self.left_batch.num_rows()
        )
    }
}

impl ExecutionPlan for DistCoverageExec {
    fn name(&self) -> &str {
        "DistCoverageExec"
    }

    // DataFusion 55: required. The node has no expressions of its own (the inner
    // `CountOverlapsExec` has none either), so there is nothing to visit.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    // DataFusion 55: the successor of `required_input_distribution` (by default it wraps it).
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.inner.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            left_batch: Arc::clone(&self.left_batch),
            columns_1: Arc::clone(&self.columns_1),
            columns_2: Arc::clone(&self.columns_2),
            filter_op: self.filter_op.clone(),
            coverage: self.coverage,
            inner: crate::bio_phys_codec::recompute_with_children(Arc::clone(&self.inner), children)?,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    // DataFusion 55: statistics are computed through `StatisticsContext` by these two methods
    // (the successors of `partition_statistics`); the children are the inner node's
    // children, so delegating both keeps the decorator transparent.
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.inner.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }
}

/// Our provider for coverage - the only place where we do NOT delegate
/// `scan()` to the vendored crate (because that one loses the data needed for serialization).
pub struct DistCoverageProvider {
    session: Arc<SessionContext>,
    schema: SchemaRef,
    left_table: String,
    right_table: String,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
    filter_op: FilterOp,
    coverage: bool,
}

impl Debug for DistCoverageProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistCoverageProvider(coverage={})", self.coverage)
    }
}

impl DistCoverageProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Arc<SessionContext>,
        left_table: String,
        right_table: String,
        right_table_schema: Schema,
        columns_1: (String, String, String),
        columns_2: (String, String, String),
        filter_op: FilterOp,
        coverage: bool,
    ) -> Self {
        // We take the output schema from the vendored crate so that it is certainly identical
        // to the local variant (right_schema + the coverage/count column).
        let schema = CountOverlapsProvider::new(
            Arc::clone(&session),
            left_table.clone(),
            right_table.clone(),
            right_table_schema,
            vec![
                columns_1.0.clone(),
                columns_1.1.clone(),
                columns_1.2.clone(),
            ],
            vec![
                columns_2.0.clone(),
                columns_2.1.clone(),
                columns_2.2.clone(),
            ],
            filter_op.clone(),
            coverage,
        )
        .schema();

        Self {
            session,
            schema,
            left_table,
            right_table,
            columns_1,
            columns_2,
            filter_op,
            coverage,
        }
    }
}

#[async_trait]
impl TableProvider for DistCoverageProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Materialize the left (indexed) table - and KEEP the batch,
        //    unlike the vendored code, which discards it.
        let left_df = self
            .session
            .table(self.left_table.clone())
            .await?
            .select_columns(&[&self.columns_1.0, &self.columns_1.1, &self.columns_1.2])?;
        let left_schema: SchemaRef = Arc::new(left_df.schema().as_arrow().clone());
        let batches = left_df.collect().await?;
        let left_batch = Arc::new(concat_batches(&left_schema, &batches)?);

        // 2. The right side's plan. We DELIBERATELY do not insert the
        //    RepartitionExec(RoundRobinBatch) that the vendored code inserts: Ballista removes
        //    every non-hash repartitioning from the distributed plan anyway
        //    (ballista-scheduler/src/planner.rs), so inserting it here would only make the
        //    shape of the local and the distributed plan diverge. Parallelism comes from the
        //    source's partitioning.
        let right = self
            .session
            .table(self.right_table.clone())
            .await?
            .create_physical_plan()
            .await?;

        let inner = build_count_overlaps_exec(
            right,
            Arc::clone(&self.schema),
            left_batch.as_ref(),
            &self.columns_1,
            self.columns_2.clone(),
            self.filter_op.clone(),
            self.coverage,
        )?;

        Ok(Arc::new(DistCoverageExec {
            left_batch,
            columns_1: Arc::new(self.columns_1.clone()),
            columns_2: Arc::new(self.columns_2.clone()),
            filter_op: self.filter_op.clone(),
            coverage: self.coverage,
            inner,
        }))
    }
}

/// Helper: a `DataFusionError` with a message about a missing input.
pub fn missing_input(what: &str) -> DataFusionError {
    DataFusionError::Internal(format!("DistCoverageExec: missing {what}"))
}

//! Coverage — jedyna z czterech operacji Fazy H, ktora wymaga WLASNEGO
//! wezla-nosnika, a nie tylko latki widocznosci.
//!
//! Powod (zweryfikowany w zrodle, count_overlaps.rs:122-141):
//! `CountOverlapsProvider::scan()` materializuje lewa tabele, PRZENOSI ja do
//! konstruktora indeksu i porzuca. Powstaly `CountOverlapsExec` ma pola
//! `schema, index, right, columns_2, filter_op, cache` — czyli NIE MA:
//!   - danych lewej tabeli (nie da sie ich odzyskac),
//!   - `columns_1` (nazw kolumn lewej tabeli, potrzebnych do odbudowy indeksu),
//!   - flagi `coverage` (jest tylko wariantem enuma).
//! A samego indeksu nie da sie odczytac: `COITree` nie ma serde, a
//! `CountOverlapIndex` ma prywatne pola.
//!
//! Rozwiazanie: `DistCoverageExec` — TRANSPARENTNY DEKORATOR, ktory deleguje
//! wszystko do wewnetrznego `CountOverlapsExec`, ale dodatkowo przenosi to,
//! co tamten gubi. Dla optymalizatora i dla DistributedPlannera Ballisty
//! zachowuje sie identycznie jak wezel wewnetrzny.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::Distribution;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datafusion_bio_function_ranges::{
    build_coitree_from_batches, build_count_index_from_batches, CountOverlapsExec,
    CountOverlapsIndex, CountOverlapsProvider, FilterOp,
};

/// Buduje wezel vendora z jawnie podanego batcha lewej tabeli.
///
/// Typow indeksu (`AHashMap`, `COITree`) celowo nigdzie nie nazywamy —
/// inferencja dopasowuje je nominalnie z publicznych funkcji budujacych,
/// dzieki czemu nasz Cargo.toml nie potrzebuje ani `ahash`, ani `coitrees`.
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
        // Atrapa; `with_new_children` ponizej przelicza cache dokladnie tak,
        // jak zrobilby to vendor (wlacznie z EmissionType::Final).
        cache: crate::bio_phys_codec::placeholder_props(schema),
    };
    Arc::new(exec).with_new_children(vec![right])
}

/// Transparentny dekorator nad `CountOverlapsExec`.
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
        // `broadcast_rows` celowo w wydruku: pojawia sie w EXPLAIN ANALYZE
        // i staje sie czescia dowodu, ze lewa tabela faktycznie pojechala
        // w ladunku planu (oraz materialem do oceny limitu 16 MB).
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
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
            inner: Arc::clone(&self.inner).with_new_children(children)?,
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
    }
}

/// Nasz provider dla coverage — jedyne miejsce w Fazie H, gdzie NIE delegujemy
/// `scan()` do vendora (bo tamten gubi dane potrzebne do serializacji).
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
        // Schemat wyjsciowy bierzemy od vendora, zeby na pewno byl identyczny
        // z wariantem lokalnym (right_schema + kolumna coverage/count).
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        // 1. Materializacja lewej (indeksowanej) tabeli — i ZATRZYMUJEMY batch,
        //    w odroznieniu od vendora, ktory go porzuca.
        let left_df = self
            .session
            .table(self.left_table.clone())
            .await?
            .select_columns(&[&self.columns_1.0, &self.columns_1.1, &self.columns_1.2])?;
        let left_schema: SchemaRef = Arc::new(left_df.schema().as_arrow().clone());
        let batches = left_df.collect().await?;
        let left_batch = Arc::new(concat_batches(&left_schema, &batches)?);

        // 2. Plan prawej strony. CELOWO nie wstawiamy RepartitionExec(RoundRobinBatch),
        //    ktory wstawia vendor: Ballista i tak usuwa z planu rozproszonego kazda
        //    repartycje inna niz hash (ballista-scheduler/src/planner.rs), wiec
        //    wstawianie jej tutaj tylko rozjechaloby ksztalt planu lokalnego
        //    i rozproszonego. Rownoleglosc bierze sie z partycjonowania zrodla.
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

/// Pomocnicze: `DataFusionError` z komunikatem o brakujacym wejsciu.
pub fn missing_input(what: &str) -> DataFusionError {
    DataFusionError::Internal(format!("DistCoverageExec: brak {what}"))
}

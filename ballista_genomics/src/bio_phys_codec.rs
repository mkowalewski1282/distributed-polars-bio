//! Faza H: `PhysicalExtensionCodec` dla wezlow operacji zakresowych
//! (`MergeExec`, dalej: `SubtractExec`, `NearestExec`, coverage).
//!
//! Osobny kodek, a nie rozszerzenie `physical_codec.rs`, celowo: tamten jest
//! ZWERYFIKOWANYM artefaktem Fazy A.5 (wisi na nim test overlapa), wiec
//! ustawiamy sie PRZED nim w lancuchu delegacji i zostawiamy jego sciezke
//! bajt w bajt nietknieta:
//!
//! ```text
//! BioRangesPhysicalCodec   (MAGIC 0xD1570003 + tag operacji)
//!   -> nie moje? -> IntervalJoinPhysicalCodec (0xD1570002)
//!        -> nie moje? -> domyslny kodek Ballisty
//! ```
//!
//! W przeciwienstwie do `IntervalJoinExec`, zadna z tych operacji nie ma pol
//! typu `Arc<dyn PhysicalExpr>` - wiec kodek nie potrzebuje w ogole
//! `serialize_physical_expr`/`parse_physical_expr` i jest istotnie prostszy.
//!
//! Wezly buduje sie literalem struktury (dzieki latce widocznosci, patrz
//! vendor/PATCH.md, Latka 2), bo vendor nie daje dla nich zadnych konstruktorow.

use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_bio_function_ranges::{
    build_nearest_indexes, FilterOp, MergeExec, NearestExec, SubtractExec,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::codec_io::*;
use crate::coverage_node::{build_count_overlaps_exec, DistCoverageExec};
use crate::physical_codec::IntervalJoinPhysicalCodec;

pub const BIO_PHYS_MAGIC: u32 = 0xD157_0003;

const TAG_MERGE: u8 = 1;
const TAG_SUBTRACT: u8 = 2;
const TAG_NEAREST: u8 = 3;
const TAG_COVERAGE: u8 = 4;

#[derive(Debug)]
pub struct BioRangesPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl Default for BioRangesPhysicalCodec {
    fn default() -> Self {
        Self {
            inner: Arc::new(IntervalJoinPhysicalCodec::default()),
        }
    }
}

/// `PlanProperties` z atrapa partycjonowania. Nigdy nie trafia do gotowego
/// wezla: zaraz po zbudowaniu literalu wolamy `with_new_children()`, ktore
/// przelicza `cache` DOKLADNIE tak, jak zrobilby to vendor (wlacznie z
/// wlasciwym `EmissionType`). Dzieki temu nie musimy nawet wiedziec, czy dana
/// operacja jest `Incremental` czy `Final` - i eliminujemy cala klase bledow
/// typu "wezel zbudowany lokalnie ma stan, ktory rozproszony executor odrzuca"
/// (bug `PartitionMode::Auto` z Fazy A.5).
pub fn placeholder_props(schema: datafusion::arrow::datatypes::SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}

fn build_merge_exec(
    input: Arc<dyn ExecutionPlan>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    columns: (String, String, String),
    min_dist: i64,
    strict: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let exec = MergeExec {
        schema: Arc::clone(&schema),
        input: Arc::clone(&input),
        columns: Arc::new(columns),
        min_dist,
        strict,
        cache: placeholder_props(schema),
    };
    Arc::new(exec).with_new_children(vec![input])
}

/// Wezel BINARNY - pierwszy z dwojgiem dzieci. DataFusion serializuje oba
/// poddrzewa rekurencyjnie PRZED wywolaniem kodeka i podaje je w `inputs`,
/// wiec my zapisujemy wylacznie wlasne parametry wezla.
fn build_subtract_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    left_columns: (String, String, String),
    right_columns: (String, String, String),
    left_contig_col_idx: usize,
    strict: bool,
    has_extra_cols: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let exec = SubtractExec {
        schema: Arc::clone(&schema),
        left: Arc::clone(&left),
        right: Arc::clone(&right),
        left_columns: Arc::new(left_columns),
        right_columns: Arc::new(right_columns),
        left_contig_col_idx,
        strict,
        has_extra_cols,
        cache: placeholder_props(schema),
    };
    Arc::new(exec).with_new_children(vec![left, right])
}

fn encode_subtract(sx: &SubtractExec, buf: &mut Vec<u8>) -> Result<()> {
    write_magic(buf, BIO_PHYS_MAGIC);
    write_u8(buf, TAG_SUBTRACT);
    write_bool(buf, sx.strict);
    write_schema(buf, sx.schema.as_ref())?;
    write_cols(buf, sx.left_columns.as_ref());
    write_cols(buf, sx.right_columns.as_ref());
    write_u32(buf, sx.left_contig_col_idx as u32);
    // `has_extra_cols` zapisujemy JAWNIE, mimo ze dalo by sie je wyprowadzic ze
    // schematu (`fields().len() > 3`). Wyprowadzanie powielaloby logike
    // SubtractProvider::new i rozjechaloby sie przy tabeli o dokladnie trzech
    // kolumnach o innej semantyce.
    write_bool(buf, sx.has_extra_cols);
    Ok(())
}

fn decode_subtract(
    buf: &[u8],
    pos: &mut usize,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let strict = read_bool(buf, pos)?;
    let schema = read_schema(buf, pos)?;
    let left_columns = read_cols(buf, pos)?;
    let right_columns = read_cols(buf, pos)?;
    let left_contig_col_idx = read_u32(buf, pos)? as usize;
    let has_extra_cols = read_bool(buf, pos)?;
    let left = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("SubtractExec: brak lewego wejscia".into()))?
        .clone();
    let right = inputs
        .get(1)
        .ok_or_else(|| DataFusionError::Internal("SubtractExec: brak prawego wejscia".into()))?
        .clone();
    build_subtract_exec(
        left,
        right,
        schema,
        left_columns,
        right_columns,
        left_contig_col_idx,
        strict,
        has_extra_cols,
    )
}

fn filter_op_to_byte(f: &FilterOp) -> u8 {
    match f {
        FilterOp::Weak => 0,
        FilterOp::Strict => 1,
    }
}

fn byte_to_filter_op(b: u8) -> Result<FilterOp> {
    Ok(match b {
        0 => FilterOp::Weak,
        1 => FilterOp::Strict,
        other => {
            return Err(DataFusionError::Internal(format!(
                "nieprawidlowy bajt FilterOp: {other}"
            )));
        }
    })
}

/// Wzorzec BROADCAST: indeksu nie da sie zserializowac (COITree bez serde,
/// prywatne pola), ale jest w 100% ODTWARZALNY z `left_batch`. Wysylamy wiec
/// dane wejsciowe zamiast wyniku i przeliczamy indeks po stronie executora —
/// dokladnie ta sama sztuczka co `parse_intervals()` przy IntervalJoinExec.
///
/// `build_nearest_indexes` to TA SAMA funkcja, ktorej uzyl NearestProvider::scan(),
/// wiec `position` w IntervalRecord wskazuje ten sam wiersz tego samego batcha.
/// Typu indeksu celowo nigdzie nie nazywamy — inferencja dopasowuje go
/// nominalnie, dzieki czemu nasz Cargo.toml nie potrzebuje ahash ani coitrees.
#[allow(clippy::too_many_arguments)]
fn build_nearest_exec(
    right: Arc<dyn ExecutionPlan>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    left_batch: datafusion::arrow::record_batch::RecordBatch,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
    filter_op: FilterOp,
    include_overlaps: bool,
    k: usize,
    compute_distance: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let indexes = Arc::new(build_nearest_indexes(
        &left_batch,
        (&columns_1.0, &columns_1.1, &columns_1.2),
    )?);
    let exec = NearestExec {
        schema: Arc::clone(&schema),
        left_batch: Arc::new(left_batch),
        indexes,
        right: Arc::clone(&right),
        columns_1: Arc::new(columns_1),
        columns_2: Arc::new(columns_2),
        filter_op,
        include_overlaps,
        k,
        compute_distance,
        cache: placeholder_props(schema),
    };
    Arc::new(exec).with_new_children(vec![right])
}

fn encode_nearest(n: &NearestExec, buf: &mut Vec<u8>) -> Result<()> {
    write_magic(buf, BIO_PHYS_MAGIC);
    write_u8(buf, TAG_NEAREST);
    write_u8(buf, filter_op_to_byte(&n.filter_op));
    write_schema(buf, n.schema.as_ref())?;
    write_cols(buf, n.columns_1.as_ref());
    write_cols(buf, n.columns_2.as_ref());
    write_bool(buf, n.include_overlaps);
    write_u32(buf, n.k as u32);
    write_bool(buf, n.compute_distance);
    write_batch_ipc(buf, n.left_batch.as_ref())?;
    Ok(())
}

fn decode_nearest(
    buf: &[u8],
    pos: &mut usize,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let filter_op = byte_to_filter_op(read_u8(buf, pos)?)?;
    let schema = read_schema(buf, pos)?;
    let columns_1 = read_cols(buf, pos)?;
    let columns_2 = read_cols(buf, pos)?;
    let include_overlaps = read_bool(buf, pos)?;
    let k = read_u32(buf, pos)? as usize;
    let compute_distance = read_bool(buf, pos)?;
    let left_batch = read_batch_ipc(buf, pos)?;
    let right = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("NearestExec: brak wejscia".into()))?
        .clone();
    build_nearest_exec(
        right,
        schema,
        left_batch,
        columns_1,
        columns_2,
        filter_op,
        include_overlaps,
        k,
        compute_distance,
    )
}

fn encode_coverage(c: &DistCoverageExec, buf: &mut Vec<u8>) -> Result<()> {
    write_magic(buf, BIO_PHYS_MAGIC);
    write_u8(buf, TAG_COVERAGE);
    write_u8(buf, filter_op_to_byte(&c.filter_op));
    write_schema(buf, c.inner.schema().as_ref())?;
    // columns_1 i flaga coverage NIE ISTNIEJA w CountOverlapsExec - to wlasnie
    // po to jest nasz wezel-nosnik (patrz coverage_node.rs).
    write_cols(buf, c.columns_1.as_ref());
    write_cols(buf, c.columns_2.as_ref());
    write_bool(buf, c.coverage);
    write_batch_ipc(buf, c.left_batch.as_ref())?;
    Ok(())
}

fn decode_coverage(
    buf: &[u8],
    pos: &mut usize,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let filter_op = byte_to_filter_op(read_u8(buf, pos)?)?;
    let schema = read_schema(buf, pos)?;
    let columns_1 = read_cols(buf, pos)?;
    let columns_2 = read_cols(buf, pos)?;
    let coverage = read_bool(buf, pos)?;
    let left_batch = read_batch_ipc(buf, pos)?;
    let right = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("DistCoverageExec: brak wejscia".into()))?
        .clone();
    let inner = build_count_overlaps_exec(
        right,
        schema,
        &left_batch,
        &columns_1,
        columns_2.clone(),
        filter_op.clone(),
        coverage,
    )?;
    Ok(Arc::new(DistCoverageExec {
        left_batch: Arc::new(left_batch),
        columns_1: Arc::new(columns_1),
        columns_2: Arc::new(columns_2),
        filter_op,
        coverage,
        inner,
    }))
}

fn encode_merge(m: &MergeExec, buf: &mut Vec<u8>) -> Result<()> {
    write_magic(buf, BIO_PHYS_MAGIC);
    write_u8(buf, TAG_MERGE);
    write_bool(buf, m.strict);
    write_schema(buf, m.schema.as_ref())?;
    write_cols(buf, m.columns.as_ref());
    write_i64(buf, m.min_dist);
    Ok(())
}

fn decode_merge(
    buf: &[u8],
    pos: &mut usize,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let strict = read_bool(buf, pos)?;
    let schema = read_schema(buf, pos)?;
    let columns = read_cols(buf, pos)?;
    let min_dist = read_i64(buf, pos)?;
    let input = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("MergeExec: brak wejscia".into()))?
        .clone();
    build_merge_exec(input, schema, columns, min_dist, strict)
}

impl PhysicalExtensionCodec for BioRangesPhysicalCodec {
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(m) = node.as_any().downcast_ref::<MergeExec>() {
            return encode_merge(m, buf);
        }
        if let Some(sx) = node.as_any().downcast_ref::<SubtractExec>() {
            return encode_subtract(sx, buf);
        }
        if let Some(n) = node.as_any().downcast_ref::<NearestExec>() {
            return encode_nearest(n, buf);
        }
        if let Some(c) = node.as_any().downcast_ref::<DistCoverageExec>() {
            return encode_coverage(c, buf);
        }
        self.inner.try_encode(node, buf)
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !has_magic(buf, BIO_PHYS_MAGIC) {
            return self.inner.try_decode(buf, inputs, registry);
        }
        let mut pos = 4usize;
        let tag = read_u8(buf, &mut pos)?;
        match tag {
            TAG_MERGE => decode_merge(buf, &mut pos, inputs),
            TAG_SUBTRACT => decode_subtract(buf, &mut pos, inputs),
            TAG_NEAREST => decode_nearest(buf, &mut pos, inputs),
            TAG_COVERAGE => decode_coverage(buf, &mut pos, inputs),
            other => Err(DataFusionError::Internal(format!(
                "BioRangesPhysicalCodec: nieznany tag operacji {other}"
            ))),
        }
    }
}

//! A `PhysicalExtensionCodec` for the range-operation nodes
//! (`MergeExec`, `SubtractExec`, `NearestExec`, coverage).
//!
//! A separate codec, not an extension of `physical_codec.rs`, deliberately: that one is
//! a VERIFIED artifact (the overlap test depends on it), so
//! we sit BEFORE it in the delegation chain and leave its path
//! byte-for-byte untouched:
//!
//! ```text
//! BioRangesPhysicalCodec   (MAGIC 0xD1570003 + operation tag)
//!   -> not mine? -> IntervalJoinPhysicalCodec (0xD1570002)
//!        -> not mine? -> Ballista's default codec
//! ```
//!
//! Unlike `IntervalJoinExec`, none of these operations has fields of type
//! `Arc<dyn PhysicalExpr>` - so the codec does not need
//! `serialize_physical_expr`/`parse_physical_expr` at all and is much simpler.
//!
//! The nodes are built with a struct literal (thanks to the visibility patch, see
//! vendor/PATCH.md, Patch 2), because the vendored crate provides no constructors for them.

use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, PlanProperties, ReplaceChildrenOptions,
};
use datafusion_bio_function_ranges::{
    build_nearest_indexes, FilterOp, MergeExec, NearestExec, SubtractExec,
};
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};

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

/// `PlanProperties` with a dummy partitioning. It never ends up in a finished
/// node: right after building the literal we call `recompute_with_children()`, which
/// recomputes `cache` EXACTLY as the vendored code would (including the
/// correct `EmissionType`). Thanks to this we do not even need to know whether an
/// operation is `Incremental` or `Final` - and we eliminate a whole class of bugs of the
/// kind "a locally built node has a state that a distributed executor rejects"
/// (the `PartitionMode::Auto` bug in IntervalJoinExec).
pub fn placeholder_props(schema: datafusion::arrow::datatypes::SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}

/// Replacing the children with a recomputation of `cache` - the counterpart of the former
/// `with_new_children()` (deprecated in DataFusion 55): `replace_children` with
/// `ChildrenPropertiesMode::Recompute` ends up in the vendored nodes' `with_new_children()`
/// for them, so the result is the same.
pub fn recompute_with_children(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
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
    recompute_with_children(Arc::new(exec), vec![input])
}

/// A BINARY node - the first with two children. DataFusion serializes both
/// subtrees recursively BEFORE calling the codec and passes them in `inputs`,
/// so we write only the node's own parameters.
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
    recompute_with_children(Arc::new(exec), vec![left, right])
}

fn encode_subtract(sx: &SubtractExec, buf: &mut Vec<u8>) -> Result<()> {
    write_magic(buf, BIO_PHYS_MAGIC);
    write_u8(buf, TAG_SUBTRACT);
    write_bool(buf, sx.strict);
    write_schema(buf, sx.schema.as_ref())?;
    write_cols(buf, sx.left_columns.as_ref());
    write_cols(buf, sx.right_columns.as_ref());
    write_u32(buf, sx.left_contig_col_idx as u32);
    // We write `has_extra_cols` EXPLICITLY, even though it could be derived from the
    // schema (`fields().len() > 3`). Deriving it would duplicate the logic of
    // SubtractProvider::new and would drift apart for a table with exactly three
    // columns of different semantics.
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
        .ok_or_else(|| DataFusionError::Internal("SubtractExec: missing left input".into()))?
        .clone();
    let right = inputs
        .get(1)
        .ok_or_else(|| DataFusionError::Internal("SubtractExec: missing right input".into()))?
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
                "invalid FilterOp byte: {other}"
            )));
        }
    })
}

/// The BROADCAST pattern: the index cannot be serialized (a COITree without serde,
/// private fields), but it is 100% RECONSTRUCTIBLE from `left_batch`. So we send
/// the input data instead of the result and recompute the index on the executor side -
/// exactly the same trick as `parse_intervals()` for IntervalJoinExec.
///
/// `build_nearest_indexes` is THE SAME function that NearestProvider::scan() used,
/// so `position` in IntervalRecord points to the same row of the same batch.
/// We deliberately never name the index type - inference matches it
/// nominally, so our Cargo.toml needs neither ahash nor coitrees.
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
    recompute_with_children(Arc::new(exec), vec![right])
}

/// The size of the received broadcast payload: the orchestrator sums these lines from the
/// executor logs into the `broadcast_bytes` column (the pattern is in `bench/logscan.py`).
fn log_broadcast_decoded(bytes: usize) {
    tracing::info!("broadcast payload decoded: {bytes} bytes");
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
    let payload_start = *pos;
    let left_batch = read_batch_ipc(buf, pos)?;
    log_broadcast_decoded(*pos - payload_start);
    let right = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("NearestExec: missing input".into()))?
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
    // columns_1 and the coverage flag do NOT EXIST in CountOverlapsExec - that is
    // exactly why our carrier node exists (see coverage_node.rs).
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
    let payload_start = *pos;
    let left_batch = read_batch_ipc(buf, pos)?;
    log_broadcast_decoded(*pos - payload_start);
    let right = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("DistCoverageExec: missing input".into()))?
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
        .ok_or_else(|| DataFusionError::Internal("MergeExec: missing input".into()))?
        .clone();
    build_merge_exec(input, schema, columns, min_dist, strict)
}

impl PhysicalExtensionCodec for BioRangesPhysicalCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if let Some(m) = node.downcast_ref::<MergeExec>() {
            return encode_merge(m, buf);
        }
        if let Some(sx) = node.downcast_ref::<SubtractExec>() {
            return encode_subtract(sx, buf);
        }
        if let Some(n) = node.downcast_ref::<NearestExec>() {
            return encode_nearest(n, buf);
        }
        if let Some(c) = node.downcast_ref::<DistCoverageExec>() {
            return encode_coverage(c, buf);
        }
        self.inner.try_encode(node, buf, proto_converter)
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !has_magic(buf, BIO_PHYS_MAGIC) {
            return self.inner.try_decode(buf, inputs, registry, proto_converter);
        }
        let mut pos = 4usize;
        let tag = read_u8(buf, &mut pos)?;
        match tag {
            TAG_MERGE => decode_merge(buf, &mut pos, inputs),
            TAG_SUBTRACT => decode_subtract(buf, &mut pos, inputs),
            TAG_NEAREST => decode_nearest(buf, &mut pos, inputs),
            TAG_COVERAGE => decode_coverage(buf, &mut pos, inputs),
            other => Err(DataFusionError::Internal(format!(
                "BioRangesPhysicalCodec: unknown operation tag {other}"
            ))),
        }
    }
}

//! A PhysicalExtensionCodec for IntervalJoinExec - the last missing piece for a truly
//! distributed `overlap` with COITrees in Ballista.
//!
//! This required a local visibility patch in datafusion-bio-function-ranges
//! (see vendor/PATCH.md) - without it ColIntervals (a required argument of
//! IntervalJoinExec::try_new()) could not be named at all from outside the crate.
//!
//! The codec's strategy: we do NOT serialize the node's ready-made physical tree directly
//! (as DistOverlapProvider does for the logical side) - IntervalJoinExec is a BINARY
//! node (a join) whose children (left/right) DataFusion serializes itself,
//! recursively, before calling our codec (see
//! datafusion-proto::physical_plan::mod.rs - `PhysicalExtensionNode { node, inputs }`,
//! where `inputs` are already serialized/deserialized subtrees). We serialize
//! only the node's OWN parameters: `on`, `filter`, `join_type`, `partition_mode`,
//! `null_equals_null`, `algorithm`, `low_memory`.
//!
//! `on`/`filter.expression()` are `Arc<dyn PhysicalExpr>` - we use the public
//! functions of datafusion-proto (`serialize_physical_expr`/`parse_physical_expr`),
//! which already know how to serialize any standard physical expression
//! (Column, BinaryExpr, CastExpr, Literal - exactly what the overlap condition
//! is built from).
//!
//! The filter's schema is NOT serialized separately - we reconstruct it from
//! `column_indices` + the left/right schemas (exactly what
//! `column_indices` is for: a mapping back to the source columns).

use std::sync::Arc;

use ballista::prelude::SessionConfigExt;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DataFusionError, JoinSide, JoinType, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion_bio_function_ranges::{Algorithm, IntervalJoinExec, parse_intervals};
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;

use crate::codec_io::{has_magic, read_bytes, read_u32, write_bytes, write_u32};

/// `algorithm`/`low_memory` have NO public getters on IntervalJoinExec
/// (checked empirically) - they cannot be read from an existing instance.
/// But there is no need: they are values from the session's BioConfig (interval_join_algorithm/
/// interval_join_low_memory, `runner::bio_session_config`), and the SCHEDULER and EXECUTOR have
/// an identical configuration - so we KNOW them in advance, just like the session that built
/// this node. One place for this assumption, for the codec and for rebuilding the node during
/// planning (`dist_provider::collect_left_at_planning`).
pub(crate) const INTERVAL_JOIN_ALGORITHM: Algorithm = Algorithm::Coitrees;
pub(crate) const INTERVAL_JOIN_LOW_MEMORY: bool = false;

#[derive(Debug)]
pub struct IntervalJoinPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
    // See `INTERVAL_JOIN_ALGORITHM`/`INTERVAL_JOIN_LOW_MEMORY`.
    algorithm: Algorithm,
    low_memory: bool,
}

impl Default for IntervalJoinPhysicalCodec {
    fn default() -> Self {
        Self {
            inner: datafusion::prelude::SessionConfig::new().ballista_physical_extension_codec(),
            algorithm: INTERVAL_JOIN_ALGORITHM,
            low_memory: INTERVAL_JOIN_LOW_MEMORY,
        }
    }
}

// --- helpers, hand-written binary encoding (the same conventions as in codec_io.rs) ---

const MAGIC: u32 = 0xD157_0002;

fn encode_expr(codec: &dyn PhysicalExtensionCodec, expr: &Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> Result<Vec<u8>> {
    let proto: PhysicalExprNode = serialize_physical_expr(expr, codec)?;
    Ok(proto.encode_to_vec())
}

fn decode_expr(
    codec: &dyn PhysicalExtensionCodec,
    bytes: &[u8],
    ctx: &TaskContext,
    schema: &Schema,
) -> Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
    let proto = PhysicalExprNode::decode(bytes)
        .map_err(|e| DataFusionError::Internal(format!("PhysicalExprNode decode: {e}")))?;
    parse_physical_expr(&proto, ctx, schema, codec)
}

fn join_side_to_byte(side: JoinSide) -> u8 {
    match side {
        JoinSide::Left => 0,
        JoinSide::Right => 1,
        JoinSide::None => 2,
    }
}

fn byte_to_join_side(b: u8) -> Result<JoinSide> {
    match b {
        0 => Ok(JoinSide::Left),
        1 => Ok(JoinSide::Right),
        2 => Ok(JoinSide::None),
        other => Err(DataFusionError::Internal(format!("invalid JoinSide byte: {other}"))),
    }
}

fn join_type_to_byte(jt: &JoinType) -> u8 {
    match jt {
        JoinType::Inner => 0,
        JoinType::Left => 1,
        JoinType::Right => 2,
        JoinType::Full => 3,
        JoinType::LeftSemi => 4,
        JoinType::RightSemi => 5,
        JoinType::LeftAnti => 6,
        JoinType::RightAnti => 7,
        JoinType::LeftMark => 8,
        JoinType::RightMark => 9,
    }
}

fn byte_to_join_type(b: u8) -> Result<JoinType> {
    Ok(match b {
        0 => JoinType::Inner,
        1 => JoinType::Left,
        2 => JoinType::Right,
        3 => JoinType::Full,
        4 => JoinType::LeftSemi,
        5 => JoinType::RightSemi,
        6 => JoinType::LeftAnti,
        7 => JoinType::RightAnti,
        8 => JoinType::LeftMark,
        9 => JoinType::RightMark,
        other => return Err(DataFusionError::Internal(format!("invalid JoinType byte: {other}"))),
    })
}

fn partition_mode_to_byte(pm: &PartitionMode) -> u8 {
    match pm {
        PartitionMode::Partitioned => 0,
        PartitionMode::CollectLeft => 1,
        PartitionMode::Auto => 2,
    }
}

fn byte_to_partition_mode(b: u8) -> Result<PartitionMode> {
    Ok(match b {
        0 => PartitionMode::Partitioned,
        1 => PartitionMode::CollectLeft,
        2 => PartitionMode::Auto,
        other => return Err(DataFusionError::Internal(format!("invalid PartitionMode byte: {other}"))),
    })
}

fn algorithm_to_byte(a: &Algorithm) -> u8 {
    match a {
        Algorithm::Coitrees => 0,
        Algorithm::IntervalTree => 1,
        Algorithm::ArrayIntervalTree => 2,
        Algorithm::Lapper => 3,
        Algorithm::SuperIntervals => 4,
        Algorithm::CoitreesNearest => 5,
        Algorithm::CoitreesCountOverlaps => 6,
    }
}

fn byte_to_algorithm(b: u8) -> Result<Algorithm> {
    Ok(match b {
        0 => Algorithm::Coitrees,
        1 => Algorithm::IntervalTree,
        2 => Algorithm::ArrayIntervalTree,
        3 => Algorithm::Lapper,
        4 => Algorithm::SuperIntervals,
        5 => Algorithm::CoitreesNearest,
        6 => Algorithm::CoitreesCountOverlaps,
        other => return Err(DataFusionError::Internal(format!("invalid Algorithm byte: {other}"))),
    })
}

/// Reconstructs the JoinFilter's intermediate schema from column_indices + the left/right
/// schemas - exactly what column_indices is for (a mapping back to the source).
fn build_filter_schema(left: &Schema, right: &Schema, indices: &[ColumnIndex]) -> Schema {
    let fields: Vec<Arc<Field>> = indices
        .iter()
        .map(|ci| match ci.side {
            JoinSide::Left => Arc::new(left.field(ci.index).clone()),
            JoinSide::Right => Arc::new(right.field(ci.index).clone()),
            JoinSide::None => unreachable!("filter column index has no side"),
        })
        .collect();
    Schema::new(fields)
}

impl PhysicalExtensionCodec for IntervalJoinPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !has_magic(buf, MAGIC) {
            return self.inner.try_decode(buf, inputs, registry, proto_converter);
        }

        let left = inputs
            .first()
            .ok_or_else(|| DataFusionError::Internal("IntervalJoinExec: missing left input".into()))?
            .clone();
        let right = inputs
            .get(1)
            .ok_or_else(|| DataFusionError::Internal("IntervalJoinExec: missing right input".into()))?
            .clone();
        let left_schema = left.schema();
        let right_schema = right.schema();

        let mut pos = 4usize;

        let on_len = read_u32(buf, &mut pos)?;
        let mut on = Vec::with_capacity(on_len as usize);
        for _ in 0..on_len {
            let l_bytes = read_bytes(buf, &mut pos)?;
            let r_bytes = read_bytes(buf, &mut pos)?;
            let l = decode_expr(self.inner.as_ref(), l_bytes, registry, &left_schema)?;
            let r = decode_expr(self.inner.as_ref(), r_bytes, registry, &right_schema)?;
            on.push((l, r));
        }

        let has_filter = *buf
            .get(pos)
            .ok_or_else(|| DataFusionError::Internal("IntervalJoinPhysicalCodec: truncated (has_filter)".into()))?;
        pos += 1;

        let filter = if has_filter != 0 {
            let expr_bytes = read_bytes(buf, &mut pos)?;
            let n_idx = read_u32(buf, &mut pos)?;
            let mut indices = Vec::with_capacity(n_idx as usize);
            for _ in 0..n_idx {
                let idx = read_u32(buf, &mut pos)? as usize;
                let side_b = *buf.get(pos).ok_or_else(|| {
                    DataFusionError::Internal("IntervalJoinPhysicalCodec: truncated (side)".into())
                })?;
                pos += 1;
                indices.push(ColumnIndex {
                    index: idx,
                    side: byte_to_join_side(side_b)?,
                });
            }
            let filter_schema = build_filter_schema(&left_schema, &right_schema, &indices);
            let expr = decode_expr(self.inner.as_ref(), expr_bytes, registry, &filter_schema)?;
            Some(JoinFilter::new(expr, indices, Arc::new(filter_schema)))
        } else {
            None
        };

        let join_type_b = *buf
            .get(pos)
            .ok_or_else(|| DataFusionError::Internal("IntervalJoinPhysicalCodec: truncated (join_type)".into()))?;
        pos += 1;
        let join_type = byte_to_join_type(join_type_b)?;

        let partition_mode_b = *buf.get(pos).ok_or_else(|| {
            DataFusionError::Internal("IntervalJoinPhysicalCodec: truncated (partition_mode)".into())
        })?;
        pos += 1;
        // We reconstruct exactly the mode the scheduler planned. A node from
        // IntervalJoinPhysicalOptimizationRule has mode Auto (with_config_rt_bio() removes the
        // join_selection rule), and the executor rejects Auto in execute(). Formerly (first
        // Partitioned, then CollectLeft) the codec turned Auto into a concrete mode only here,
        // at decode time. In Ballista 55 that is too late: the scheduler narrows the plan's
        // leaves to the task's partitions (restrict_plan_to_partitions), and with Auto also the
        // left side, so a CollectLeft mode set here would index only a slice of the left table
        // and lose pairs without any error. Therefore Auto -> CollectLeft is now resolved during
        // planning (`dist_provider::collect_left_at_planning`), and there is NO conversion here:
        // if Auto did reach the executor, IntervalJoinExec::execute() rejects it with the error
        // "unsupported PartitionMode Auto" - loudly, instead of computing from a slice.
        let partition_mode = byte_to_partition_mode(partition_mode_b)?;

        let null_equals_null = *buf.get(pos).ok_or_else(|| {
            DataFusionError::Internal("IntervalJoinPhysicalCodec: truncated (null_equals_null)".into())
        })? != 0;
        pos += 1;

        let algorithm_b = *buf
            .get(pos)
            .ok_or_else(|| DataFusionError::Internal("IntervalJoinPhysicalCodec: truncated (algorithm)".into()))?;
        pos += 1;
        let algorithm = byte_to_algorithm(algorithm_b)?;

        let low_memory = *buf
            .get(pos)
            .ok_or_else(|| DataFusionError::Internal("IntervalJoinPhysicalCodec: truncated (low_memory)".into()))?
            != 0;
        let _ = pos; // the last field, pos is not used any further

        // ColIntervals is not serialized separately - it is reconstructed from the filter through
        // parse_intervals() (available thanks to the visibility patch, see vendor/PATCH.md).
        // This is EXACTLY the function IntervalJoinPhysicalOptimizationRule uses to build
        // ColIntervals the first time - so the result is identical whether computed on the
        // client side or on the executor.
        let intervals = parse_intervals(filter.as_ref()).ok_or_else(|| {
            DataFusionError::Internal(
                "IntervalJoinPhysicalCodec: could not rebuild ColIntervals from the filter".into(),
            )
        })?;

        let exec = IntervalJoinExec::try_new(
            left,
            right,
            on,
            filter,
            intervals,
            &join_type,
            None, // projection - unused in our queries (SELECT * with no projection on this node)
            partition_mode,
            null_equals_null,
            algorithm,
            low_memory,
        )?;

        Ok(Arc::new(exec))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(ij) = node.downcast_ref::<IntervalJoinExec>() else {
            return self.inner.try_encode(node, buf, proto_converter);
        };

        buf.extend_from_slice(&MAGIC.to_le_bytes());

        write_u32(buf, ij.on().len() as u32);
        for (l, r) in ij.on() {
            write_bytes(buf, &encode_expr(self.inner.as_ref(), l)?);
            write_bytes(buf, &encode_expr(self.inner.as_ref(), r)?);
        }

        match ij.filter() {
            Some(f) => {
                buf.push(1);
                write_bytes(buf, &encode_expr(self.inner.as_ref(), f.expression())?);
                write_u32(buf, f.column_indices().len() as u32);
                for ci in f.column_indices() {
                    write_u32(buf, ci.index as u32);
                    buf.push(join_side_to_byte(ci.side));
                }
            }
            None => buf.push(0),
        }

        buf.push(join_type_to_byte(ij.join_type()));
        buf.push(partition_mode_to_byte(ij.partition_mode()));
        buf.push(ij.null_equals_null() as u8);
        // IntervalJoinExec has no getters for algorithm/low_memory - we use the
        // values known from the codec's configuration (see the comment at the struct's
        // fields), not from the node instance.
        buf.push(algorithm_to_byte(&self.algorithm));
        buf.push(self.low_memory as u8);

        Ok(())
    }
}

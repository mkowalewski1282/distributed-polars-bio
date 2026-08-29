//! Faza A.5 (plan pracy magisterskiej): PhysicalExtensionCodec dla IntervalJoinExec —
//! ostatni brakujący element do prawdziwej dystrybucji `overlap` z COITrees w Ballistrze.
//!
//! Wymagało to lokalnej łatki widoczności w datafusion-bio-function-ranges
//! (patrz vendor/PATCH.md) — bez niej ColIntervals (wymagany argument
//! IntervalJoinExec::try_new()) był całkowicie nienazywalny spoza crate'a.
//!
//! Strategia kodeka: NIE serializujemy gotowego drzewa fizycznego węzła wprost
//! (jak przy DistOverlapProvider) — IntervalJoinExec to węzeł BINARNY
//! (join), którego dzieci (left/right) DataFusion serializuje samo,
//! rekurencyjnie, przed wywołaniem naszego kodeka (patrz
//! datafusion-proto::physical_plan::mod.rs — `PhysicalExtensionNode { node, inputs }`,
//! `inputs` to już zserializowane/zdeserializowane poddrzewa). My serializujemy
//! tylko WŁASNE parametry węzła: `on`, `filter`, `join_type`, `partition_mode`,
//! `null_equals_null`, `algorithm`, `low_memory`.
//!
//! `on`/`filter.expression()` to `Arc<dyn PhysicalExpr>` — używamy publicznych
//! funkcji datafusion-proto (`serialize_physical_expr`/`parse_physical_expr`),
//! które już umieją serializować dowolne standardowe wyrażenia fizyczne
//! (Column, BinaryExpr, CastExpr, Literal — dokładnie to, z czego zbudowany
//! jest warunek overlap, patrz Debug z Fazy A.4).
//!
//! `filter`'s schema NIE jest serializowany osobno — odtwarzamy go z
//! `column_indices` + schematów left/right (dokładnie to, do czego
//! `column_indices` służy: mapowanie z powrotem na kolumny źródłowe).

use std::sync::Arc;

use ballista::prelude::SessionConfigExt;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DataFusionError, JoinSide, JoinType, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion_bio_function_ranges::{Algorithm, IntervalJoinExec, parse_intervals};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;

use crate::codec_io::{has_magic, read_bytes, read_u32, write_bytes, write_u32};

#[derive(Debug)]
pub struct IntervalJoinPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
    // `algorithm`/`low_memory` NIE mają publicznych getterów na IntervalJoinExec
    // (sprawdzone empirycznie) — nie da się ich odczytać z istniejącej instancji.
    // Ale nie trzeba: to wartości pochodzące z BioConfig sesji (interval_join_algorithm/
    // interval_join_low_memory), a SCHEDULER i EXECUTOR mają identyczną konfigurację
    // (patrz main.rs) — więc kodek po prostu ZNA je z góry, tak samo jak sesja, która
    // zbudowała ten węzeł, zamiast je odczytywać.
    algorithm: Algorithm,
    low_memory: bool,
}

impl Default for IntervalJoinPhysicalCodec {
    fn default() -> Self {
        Self {
            inner: datafusion::prelude::SessionConfig::new().ballista_physical_extension_codec(),
            algorithm: Algorithm::Coitrees,
            low_memory: false,
        }
    }
}

// --- pomocnicze, ręczne kodowanie binarne (te same konwencje co Payload w main.rs) ---

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

/// Odtwarza schemat pośredni JoinFilter z column_indices + schematów left/right —
/// dokładnie to, do czego column_indices służy (mapowanie z powrotem na źródło).
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !has_magic(buf, MAGIC) {
            return self.inner.try_decode(buf, inputs, registry);
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
        let decoded_partition_mode = byte_to_partition_mode(partition_mode_b)?;
        // Znalezisko empiryczne: węzeł, jaki tworzy IntervalJoinPhysicalOptimizationRule,
        // ma partition_mode=Auto (bo with_config_rt_bio() usuwa standardową regułę
        // join_selection, która normalnie rozstrzyga Auto -> Partitioned/CollectLeft
        // przed wykonaniem). Lokalnie (jeden proces) to nie przeszkadza, ale prawdziwy,
        // rozproszony executor Ballisty odrzuca "Auto" w execute() błędem
        // "unsupported PartitionMode Auto". Wymuszamy Partitioned — dane i tak są
        // dzielone wg klucza joina między executory, to jedyny sensowny tryb tutaj.
        let partition_mode = if decoded_partition_mode == PartitionMode::Auto {
            PartitionMode::Partitioned
        } else {
            decoded_partition_mode
        };

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
        let _ = pos; // ostatnie pole, pos nie jest już dalej używane

        // ColIntervals nie jest serializowane osobno — odtwarzane z filter przez
        // parse_intervals() (dostępne dzięki łatce widoczności, patrz vendor/PATCH.md).
        // To DOKŁADNIE ta sama funkcja, której IntervalJoinPhysicalOptimizationRule
        // używa, żeby zbudować ColIntervals za pierwszym razem — więc wynik jest
        // identyczny niezależnie czy liczony po stronie klienta czy executora.
        let intervals = parse_intervals(filter.as_ref()).ok_or_else(|| {
            DataFusionError::Internal(
                "IntervalJoinPhysicalCodec: nie udało się odtworzyć ColIntervals z filter".into(),
            )
        })?;

        let exec = IntervalJoinExec::try_new(
            left,
            right,
            on,
            filter,
            intervals,
            &join_type,
            None, // projection — nieużywane w naszych zapytaniach (SELECT * bez projekcji na tym węźle)
            partition_mode,
            null_equals_null,
            algorithm,
            low_memory,
        )?;

        Ok(Arc::new(exec))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(ij) = node.as_any().downcast_ref::<IntervalJoinExec>() else {
            return self.inner.try_encode(node, buf);
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
        // Brak getterów dla algorithm/low_memory na IntervalJoinExec — używamy
        // wartości znanych z konfiguracji kodeka (patrz komentarz przy polach
        // struktury), nie z instancji węzła.
        buf.push(algorithm_to_byte(&self.algorithm));
        buf.push(self.low_memory as u8);

        Ok(())
    }
}

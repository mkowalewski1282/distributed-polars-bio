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
use datafusion_bio_function_ranges::MergeExec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::codec_io::*;
use crate::physical_codec::IntervalJoinPhysicalCodec;

pub const BIO_PHYS_MAGIC: u32 = 0xD157_0003;

const TAG_MERGE: u8 = 1;

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
fn placeholder_props(schema: datafusion::arrow::datatypes::SchemaRef) -> Arc<PlanProperties> {
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
            other => Err(DataFusionError::Internal(format!(
                "BioRangesPhysicalCodec: nieznany tag operacji {other}"
            ))),
        }
    }
}

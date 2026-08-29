//! `BioDistLogicalCodec` — to jest to, czego brakowało w Fazie A.3
//! (błąd `LogicalExtensionCodec is not provided`).
//!
//! Uogólniony w Fazie H z jednego typu per operacja na jeden kodek obsługujący
//! wszystkie operacje przez tag w ładunku. Powód techniczny, nie estetyczny:
//! `SessionConfig` przyjmuje dokładnie JEDEN `LogicalExtensionCodec`, więc
//! wariant per operacja wymuszałby łańcuch delegacji o tylu poziomach, ile
//! operacji, gdzie pomyłka (zapomniana delegacja jednej z czterech metod
//! traitu) byłaby cicha.

use std::sync::Arc;

use ballista::prelude::SessionConfigExt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::prelude::SessionConfig;
use datafusion::sql::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::dist_payload::DistPayload;
use crate::dist_provider::DistBioProvider;

#[derive(Debug)]
pub struct BioDistLogicalCodec {
    /// Domyślny kodek Ballisty (dla wszystkiego, co NIE jest naszym providerem)
    /// — pobrany jako trait object z pustego `SessionConfig`, żeby nie dodawać
    /// `ballista-core` jako osobnej zależności tylko po to, by nazwać konkretny
    /// typ `BallistaLogicalExtensionCodec`.
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl Default for BioDistLogicalCodec {
    fn default() -> Self {
        Self {
            inner: SessionConfig::new().ballista_logical_extension_codec(),
        }
    }
}

impl LogicalExtensionCodec for BioDistLogicalCodec {
    fn try_decode(&self, buf: &[u8], inputs: &[LogicalPlan], ctx: &TaskContext) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        match DistPayload::decode(buf) {
            Some(payload) => {
                // `try_decode_table_provider` jest synchroniczne, a budowa providera
                // wymaga async (rejestracja CSV, lookup schematu). Ten sam wzorzec
                // (block_in_place + block_on) jest już użyty wewnątrz samego
                // datafusion-bio-function-ranges (table_function.rs).
                let provider = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(DistBioProvider::build(payload))
                })?;
                Ok(Arc::new(provider))
            }
            None => self.inner.try_decode_table_provider(buf, table_ref, schema, ctx),
        }
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        match node.as_any().downcast_ref::<DistBioProvider>() {
            Some(p) => {
                buf.extend_from_slice(&p.payload().encode());
                Ok(())
            }
            None => self.inner.try_encode_table_provider(table_ref, node, buf),
        }
    }
}

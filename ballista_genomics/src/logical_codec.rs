//! `BioDistLogicalCodec` - the codec whose absence causes the error
//! `LogicalExtensionCodec is not provided`.
//!
//! One codec serves all operations through a tag in the payload, rather than one type per
//! operation. The reason is technical, not aesthetic: `SessionConfig` accepts exactly ONE
//! `LogicalExtensionCodec`, so a per-operation variant would force a delegation chain with as
//! many levels as there are operations, where a mistake (a forgotten delegation of one of the
//! four trait methods) would be silent.

use std::sync::Arc;

use ballista::prelude::SessionConfigExt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::prelude::SessionConfig;
use datafusion::common::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::dist_payload::DistPayload;
use crate::dist_provider::DistBioProvider;

#[derive(Debug)]
pub struct BioDistLogicalCodec {
    /// Ballista's default codec (for everything that is NOT our provider) -
    /// obtained as a trait object from an empty `SessionConfig`, so as not to add
    /// `ballista-core` as a separate dependency just to name the concrete
    /// `BallistaLogicalExtensionCodec` type.
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
                // `try_decode_table_provider` is synchronous, while building the provider
                // requires async (CSV registration, schema lookup). The same pattern
                // (block_in_place + block_on) is already used inside
                // datafusion-bio-function-ranges itself (table_function.rs).
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
        match node.downcast_ref::<DistBioProvider>() {
            Some(p) => {
                buf.extend_from_slice(&p.payload().encode());
                Ok(())
            }
            None => self.inner.try_encode_table_provider(table_ref, node, buf),
        }
    }
}

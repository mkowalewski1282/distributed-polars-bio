//! Binary for the fully distributed `overlap` with COITrees.
//!
//! A frozen reference point - `tests/test_ballista_overlap.py` runs exactly this
//! binary. All the logic lives in the library (`src/lib.rs` and its modules) so that
//! it can be shared with `src/bin/dist_ops.rs`; only the entry point is left here.

use ballista_genomics::{runner, DistOp};
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    runner::run(DistOp::Overlap).await
}

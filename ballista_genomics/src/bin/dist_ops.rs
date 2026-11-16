//! One binary for all distributed operations.
//!
//! One instead of four, because on the development machine (3.5 GB RAM,
//! CARGO_BUILD_JOBS=1) the dominant cost of an iteration is LINKING - four binaries
//! mean four links after every change to the library.
//!
//! Usage: `dist_ops <overlap|merge|subtract|nearest|coverage>`

use ballista_genomics::{runner, DistOp};
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1);
    let Some(op) = arg.as_deref().and_then(DistOp::from_cli) else {
        eprintln!(
            "usage: dist_ops <overlap|merge|subtract|nearest|coverage>, got: {:?}",
            arg
        );
        std::process::exit(2);
    };
    runner::run(op).await
}

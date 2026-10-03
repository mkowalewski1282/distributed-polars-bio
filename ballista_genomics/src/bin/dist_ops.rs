//! Faza H: jedna binarka dla wszystkich operacji rozproszonych.
//!
//! Jedna zamiast czterech, bo na tej maszynie (3.5 GB RAM, CARGO_BUILD_JOBS=1)
//! dominującym kosztem iteracji jest LINKOWANIE — cztery binarki to cztery
//! linkowania po każdej zmianie w bibliotece.
//!
//! Użycie: `dist_ops <overlap|merge|subtract|nearest|coverage>`

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

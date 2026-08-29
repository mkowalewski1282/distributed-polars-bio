//! Binarka Fazy A.4 + A.5: w pełni rozproszony `overlap` z COITrees.
//!
//! Zamrożony punkt odniesienia — `tests/test_ballista_overlap.py` uruchamia
//! dokładnie tę binarkę. Cała logika żyje w bibliotece (`src/lib.rs` i moduły),
//! żeby dzielić ją z `src/bin/dist_ops.rs`; tutaj został sam punkt wejścia.
//!
//! Historia i uzasadnienia poszczególnych decyzji: `OPIS.md`, sekcje Faza A.4 i A.5.

use ballista_genomics::{runner, DistOp};
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    runner::run(DistOp::Overlap).await
}

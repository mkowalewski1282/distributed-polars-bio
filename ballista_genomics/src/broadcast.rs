//! Ładunek broadcastu a limit wiadomości gRPC (specyfikacja 3b-2, 6.2).
//!
//! `nearest` i `coverage` rozsyłają całą tabelę indeksowaną w ładunku planu fizycznego KAŻDEGO
//! zadania (`bio_phys_codec.rs`). Gdy wiadomość do executora przekroczy limit gRPC, Ballista 53
//! w trybie push nie zgłasza błędu zapytania, tylko uznaje executor za utraconego i ponawia —
//! zapytanie wisi do timeoutu. Rozmiar sprawdzamy więc przy planowaniu fizycznym (w schedulerze),
//! gdzie tabela rozsyłana jest materializowana, i kończymy planowanie czytelnym błędem, który
//! dociera do klienta jako błąd zapytania.

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_bio_function_ranges::NearestExec;

use crate::codec_io::ipc_len;
use crate::coverage_node::DistCoverageExec;
use crate::runner::GRPC_MAX_MESSAGE_ENV;

/// Zadania jednego etapu w jednej wiadomości do executora — sloty zadań executora w narzędziu
/// pomiarowym (`bench/engines.py`, `SLOTS_PER_NODE = 2`).
pub const TASKS_PER_MESSAGE: usize = 2;

/// Tabela rozsyłana przez pierwszy węzeł z broadcastem w drzewie planu.
pub fn broadcast_batch(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<RecordBatch>> {
    let any = plan.as_any();
    if let Some(n) = any.downcast_ref::<NearestExec>() {
        return Some(Arc::clone(&n.left_batch));
    }
    if let Some(c) = any.downcast_ref::<DistCoverageExec>() {
        return Some(Arc::clone(&c.left_batch));
    }
    plan.children().into_iter().find_map(|child| broadcast_batch(child))
}

/// Błąd planowania, gdy ładunki broadcastu dla `TASKS_PER_MESSAGE` zadań nie mieszczą się
/// w limicie wiadomości gRPC `limit` (bajty).
pub fn check_broadcast_fits(plan: &Arc<dyn ExecutionPlan>, limit: usize) -> Result<()> {
    let Some(batch) = broadcast_batch(plan) else {
        return Ok(());
    };
    let size = ipc_len(&batch)?;
    if size.saturating_mul(TASKS_PER_MESSAGE) > limit {
        return Err(DataFusionError::Plan(format!(
            "broadcast table of {size} bytes ({} rows) x {TASKS_PER_MESSAGE} tasks per message \
             exceeds the gRPC message limit of {limit} bytes ({GRPC_MAX_MESSAGE_ENV})",
            batch.num_rows()
        )));
    }
    Ok(())
}

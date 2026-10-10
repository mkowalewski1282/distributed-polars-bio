//! Ładunek broadcastu a limit wiadomości gRPC (specyfikacja 3b-2, 6.2).
//!
//! `nearest` i `coverage` rozsyłają całą tabelę indeksowaną w ładunku planu fizycznego każdego
//! etapu wysyłanego do executora (`bio_phys_codec.rs`). Gdy wiadomość do executora przekroczy limit gRPC, Ballista 53
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

/// Margines bezpieczeństwa na rozmiar wiadomości. Ballista 53 wysyła plan etapu raz na executor
/// i etap (`MultiTaskDefinition`, bajty planu cache'owane per etap), więc wiadomość niesie tabelę
/// rozsyłaną raz plus resztę planu; czynnik 2 pokrywa ten narzut (specyfikacja 3b-2, 6.2
/// zakładała jedną kopię na slot zadań executora).
pub const MESSAGE_SIZE_MARGIN: usize = 2;

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

/// Błąd planowania, gdy ładunki broadcastu razy `MESSAGE_SIZE_MARGIN` nie mieszczą się
/// w limicie wiadomości gRPC `limit` (bajty).
pub fn check_broadcast_fits(plan: &Arc<dyn ExecutionPlan>, limit: usize) -> Result<()> {
    let Some(batch) = broadcast_batch(plan) else {
        return Ok(());
    };
    let size = ipc_len(&batch)?;
    if size.saturating_mul(MESSAGE_SIZE_MARGIN) > limit {
        return Err(DataFusionError::Plan(format!(
            "broadcast table of {size} bytes ({} rows) x {MESSAGE_SIZE_MARGIN} (safety margin) \
             exceeds the gRPC message limit of {limit} bytes ({GRPC_MAX_MESSAGE_ENV})",
            batch.num_rows()
        )));
    }
    Ok(())
}

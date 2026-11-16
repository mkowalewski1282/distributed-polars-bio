//! Ładunek broadcastu a limit wiadomości gRPC (specyfikacja 3b-2, 6.2).
//!
//! `nearest` i `coverage` rozsyłają całą tabelę indeksowaną w ładunku planu fizycznego
//! (`bio_phys_codec.rs`), który Ballista 55 koduje osobno dla każdego zadania. Gdy wiadomość
//! z zadaniami do executora przekroczy limit gRPC, Ballista w trybie push nie zgłasza błędu
//! zapytania, tylko uznaje executor za utraconego, a po jego ponownej rejestracji ponawia —
//! zapytanie wisi do timeoutu (tak w 53 i, sprawdzone, w 55: „decoded message length too large”
//! w logu schedulera, „Removing executor”, ponowienie po ok. 60 s). Rozmiar sprawdzamy więc przy
//! planowaniu fizycznym (w schedulerze), gdzie tabela rozsyłana jest materializowana, i kończymy
//! planowanie czytelnym błędem, który dociera do klienta jako błąd zapytania.

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_bio_function_ranges::NearestExec;

use crate::codec_io::ipc_len;
use crate::coverage_node::DistCoverageExec;
use crate::runner::GRPC_MAX_MESSAGE_ENV;

/// Margines bezpieczeństwa na rozmiar wiadomości = liczba kopii tabeli rozsyłanej w jednej
/// wiadomości do executora. Ballista 55 koduje plan osobno dla każdego zadania (jedna
/// `MultiTaskDefinition` na zadanie), a zadania przydzielone executorowi w jednej rundzie
/// planowania wysyła jedną wiadomością `launch_multi_task`. Przy jednej partycji na zadanie
/// (`cluster::MAX_PARTITIONS_PER_TASK`) i 2 slotach to do 2 kopii tabeli — zmierzone: wiadomość
/// 21 113 832 B = 2 × 10 544 588 B ładunku + ok. 25 KB reszty obu planów (nearest, para 1-2).
/// Czynnik 2 = sloty executora w pomiarach, czyli założenie specyfikacji 3b-2, 6.2 (kopia na
/// slot); reszty planu (dziesiątki KB) nie obejmuje — przy limicie 512 MiB i tabelach do 57 MB
/// bez znaczenia. Ballista 53 wysyłała plan raz na executor i etap, więc wtedy czynnik 2 był
/// z naddatkiem.
pub const MESSAGE_SIZE_MARGIN: usize = 2;

/// Tabela rozsyłana przez pierwszy węzeł z broadcastem w drzewie planu.
pub fn broadcast_batch(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<RecordBatch>> {
    if let Some(n) = plan.downcast_ref::<NearestExec>() {
        return Some(Arc::clone(&n.left_batch));
    }
    if let Some(c) = plan.downcast_ref::<DistCoverageExec>() {
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

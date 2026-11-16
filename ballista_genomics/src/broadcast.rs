//! The broadcast payload versus the gRPC message size limit.
//!
//! `nearest` and `coverage` broadcast the whole indexed table inside the physical-plan payload
//! (`bio_phys_codec.rs`), which Ballista 55 encodes separately for each task. When the message
//! carrying tasks to an executor exceeds the gRPC limit, Ballista in push mode does not report
//! a query error; it treats the executor as lost and retries after its re-registration - the
//! query hangs until a timeout (so in 53 and, verified, in 55: "decoded message length too
//! large" in the scheduler log, "Removing executor", a retry after about 60 s). We therefore
//! check the size during physical planning (in the scheduler), where the broadcast table is
//! materialized, and end planning with a readable error that reaches the client as a query
//! error.

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_bio_function_ranges::NearestExec;

use crate::codec_io::ipc_len;
use crate::coverage_node::DistCoverageExec;
use crate::runner::GRPC_MAX_MESSAGE_ENV;

/// Safety margin on the message size = the number of copies of the broadcast table in one
/// message to an executor. Ballista 55 encodes the plan separately for each task (one
/// `MultiTaskDefinition` per task), and sends the tasks assigned to an executor in one
/// scheduling round as a single `launch_multi_task` message. With one partition per task
/// (`cluster::MAX_PARTITIONS_PER_TASK`), the number of copies equals the number of tasks bound
/// in a round, which is at most the executor's slot count. Measured with 2 slots (nearest, pair
/// 1-2): a message of 21 113 832 B = 2 x 10 544 588 B of payload + about 25 KB for the rest of
/// both plans. The measurement executors now run with 1 slot (`--concurrent-tasks 1`), so the
/// factor 2 leaves one copy of slack. The rest of the plan (tens of KB) is not included - with a
/// 512 MiB limit and tables up to 57 MB it does not matter. Ballista 53 sent the plan once per
/// executor and stage, so the factor 2 was generous there.
pub const MESSAGE_SIZE_MARGIN: usize = 2;

/// The table broadcast by the first node with a broadcast in the plan tree.
pub fn broadcast_batch(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<RecordBatch>> {
    if let Some(n) = plan.downcast_ref::<NearestExec>() {
        return Some(Arc::clone(&n.left_batch));
    }
    if let Some(c) = plan.downcast_ref::<DistCoverageExec>() {
        return Some(Arc::clone(&c.left_batch));
    }
    plan.children().into_iter().find_map(|child| broadcast_batch(child))
}

/// A planning error when the broadcast payloads times `MESSAGE_SIZE_MARGIN` do not fit
/// in the gRPC message limit `limit` (bytes).
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

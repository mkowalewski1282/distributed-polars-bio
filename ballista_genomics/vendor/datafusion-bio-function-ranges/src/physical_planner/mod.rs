mod bio_physical_planner;
mod bio_query_planner;
// PATCH 1 (see ../../../PATCH.md): `mod` -> `pub mod`, so that
// ColIntervals/ColInterval and parse() are reachable from outside the crate.
// Without it IntervalJoinExec::try_new() cannot be called from outside (it takes
// ColIntervals as an argument, and the type cannot be named without this change).
pub mod intervals;
pub mod joins;

pub use bio_physical_planner::IntervalJoinPhysicalOptimizationRule;
pub use bio_query_planner::BioQueryPlanner;

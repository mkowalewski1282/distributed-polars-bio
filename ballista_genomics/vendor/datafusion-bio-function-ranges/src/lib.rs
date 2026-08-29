pub mod array_utils;
pub mod cluster;
pub mod complement;
pub mod count_overlaps;
pub mod filter_op;
pub mod grouped_stream;
pub mod interval_tree;
pub mod merge;
pub mod nearest;
pub mod nearest_index;
pub mod overlap;
pub mod physical_planner;
pub mod session_context;
pub mod subtract;
pub mod table_function;

// Re-export key types
pub use cluster::ClusterProvider;
pub use complement::ComplementProvider;
pub use count_overlaps::CountOverlapsProvider;
pub use filter_op::FilterOp;
pub use merge::{MergeExec, MergeProvider}; // PATCH: MergeExec, patrz ../PATCH.md (Latka 2)
pub use nearest::{NearestExec, NearestProvider, build_nearest_indexes}; // PATCH, patrz ../PATCH.md
pub use overlap::{OverlapOutputMode, OverlapProvider};
pub use physical_planner::BioQueryPlanner;
pub use physical_planner::IntervalJoinPhysicalOptimizationRule;
pub use physical_planner::intervals::{ColInterval, ColIntervals, parse as parse_intervals}; // PATCH, patrz ../PATCH.md
pub use physical_planner::joins::interval_join::IntervalJoinExec;
pub use session_context::{Algorithm, BioConfig, BioSessionExt, create_bio_session};
pub use subtract::{SubtractExec, SubtractProvider}; // PATCH: SubtractExec, patrz ../PATCH.md
pub use table_function::register_ranges_functions;

//! Jedno źródło konfiguracji węzłów klastra — klienta, schedulera i executora.
//!
//! W trybie standalone scheduler i executor dostają GOTOWY stan sesji klienta
//! (`new_standalone_scheduler_from_state` / `new_standalone_executor_from_state`):
//! bio-owego planistę, reguły optymalizatora i kodery. Osobny proces nie ma
//! dostępu do pamięci klienta, więc musi zbudować identyczny stan sam — stąd te
//! funkcje, wołane przez `runner` (klient) i `bin/ballista_node.rs` (węzły).

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::BioSessionExt;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::bio_phys_codec::BioRangesPhysicalCodec;
use crate::logical_codec::BioDistLogicalCodec;
use crate::runner::bio_session_config;

/// Koder planu logicznego (klient → scheduler).
pub fn bio_logical_codec() -> Arc<dyn LogicalExtensionCodec> {
    Arc::new(BioDistLogicalCodec::default())
}

/// Koder planu fizycznego (scheduler → executor). Deleguje do
/// `IntervalJoinPhysicalCodec`, a ten do domyślnego kodeka Ballisty.
pub fn bio_physical_codec() -> Arc<dyn PhysicalExtensionCodec> {
    Arc::new(BioRangesPhysicalCodec::default())
}

/// `bio_session_config()` z zarejestrowanymi oboma koderami.
pub fn bio_ballista_config() -> SessionConfig {
    bio_session_config()
        .with_ballista_logical_extension_codec(bio_logical_codec())
        .with_ballista_physical_extension_codec(bio_physical_codec())
}

/// Bio-owy stan sesji dla podanej konfiguracji. Scheduler MUSI planować na
/// takim stanie, żeby `IntervalJoinPhysicalOptimizationRule` w ogóle zadziałała.
pub fn bio_session_state(config: SessionConfig) -> Result<SessionState> {
    Ok(DFSessionContext::new_with_bio(config).state())
}

//! Jedno źródło konfiguracji węzłów klastra — klienta, schedulera i executora.
//!
//! W trybie standalone scheduler i executor dostają GOTOWY stan sesji klienta
//! (`new_standalone_scheduler_from_state` / `new_standalone_executor_from_state`):
//! bio-owego planistę, reguły optymalizatora i kodery. Osobny proces nie ma
//! dostępu do pamięci klienta, więc musi zbudować identyczny stan sam — stąd te
//! funkcje, wołane przez `runner` (klient) i `bin/ballista_node.rs` (węzły).

use std::sync::Arc;

use ballista_core::config::{
    BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, BALLISTA_SCHEDULER_SHARE_FILE_STATISTICS_CACHE,
};
use ballista_core::extension::SessionConfigExt;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::BioSessionExt;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::bio_phys_codec::BioRangesPhysicalCodec;
use crate::logical_codec::BioDistLogicalCodec;
use crate::runner::{bio_session_config, grpc_max_message_bytes};

/// Koder planu logicznego (klient → scheduler).
pub fn bio_logical_codec() -> Arc<dyn LogicalExtensionCodec> {
    Arc::new(BioDistLogicalCodec::default())
}

/// Koder planu fizycznego (scheduler → executor). Deleguje do
/// `IntervalJoinPhysicalCodec`, a ten do domyślnego kodeka Ballisty.
pub fn bio_physical_codec() -> Arc<dyn PhysicalExtensionCodec> {
    Arc::new(BioRangesPhysicalCodec::default())
}

/// Planista adaptacyjny Ballisty (AQE, `ballista.planner.adaptive.enabled`), ustawiany jawnie,
/// a nie brany z wartości domyślnej (Ballista 53: wyłączony, 55: włączony, #2315). Decyzja
/// i dowody z `EXPLAIN ANALYZE`: OPIS.md, sekcja „Ballista 55”. Jedno źródło prawdy: wartość
/// odczytaną z `bio_ballista_config()` drukuje `bench_client --version-info`, a test
/// `test_aqe_setting_is_explicit` porównuje ją z deklaracją w OPIS.md.
pub const ADAPTIVE_QUERY_PLANNER: bool = true;

/// Najwyżej tyle partycji wejściowych w jednym zadaniu (`ballista.scheduler.max_partitions_per_task`).
/// Ballista 55 domyślnie (0 = bez limitu) pakuje do jednego zadania tyle partycji, ile executor ma
/// wolnych slotów (vcores), więc przy 2 slotach cały etap dwóch partycji trafiał jako JEDNO zadanie
/// do JEDNEGO executora. 1 = jedno zadanie na partycję, jak w Ballistcie 53 — zadania rozchodzą się
/// round-robin po executorach. Nie dotyczy etapów zbierających (jedna partycja wyjściowa), które
/// Ballista i tak musi wykonać jako jedno zadanie.
pub const MAX_PARTITIONS_PER_TASK: usize = 1;

/// Wspólna dla wszystkich sesji pamięć podręczna statystyk plików w schedulerze
/// (`ballista.scheduler.share_file_statistics_cache`, nowość w 55, domyślnie włączona): kolejne
/// zapytania korzystałyby ze statystyk stopek Parquetu odczytanych przez wcześniejsze. W 53 każda
/// sesja miała własną, zimną. Wyłączone, jak pamięć podręczna środowiska sesji executora
/// (`ballista_node`): pomiary nie mogą dostawać ciepłych pamięci podręcznych, których 53 nie miała.
/// Zabezpieczenie: skany naszych operacji planują sesje wewnętrzne providerów (własne
/// `RuntimeEnv`), których ta pamięć i tak nie obejmuje.
pub const SHARE_FILE_STATISTICS_CACHE: bool = false;

/// `bio_session_config()` z zarejestrowanymi oboma koderami, limitem wiadomości gRPC klienta
/// (`ballista.client.grpc_max_message_size`) i jawnymi ustawieniami Ballisty 55 (AQE, partycje
/// na zadanie, wspólna pamięć statystyk plików). Ta konfiguracja trafia do klienta, a przez
/// `override_config_producer` — do schedulera (planowanie zadań, jego klient gRPC do executorów)
/// i executorów.
pub fn bio_ballista_config() -> SessionConfig {
    // Binarki sprawdzają zmienną przy starcie (błąd użycia, kod 2), więc niepoprawna wartość
    // w tym miejscu to błąd programisty.
    let limit = grpc_max_message_bytes().unwrap_or_else(|e| panic!("{e}"));
    bio_session_config()
        // Ustawienia DataFusion pod Ballistę (`ballista_restricted_configuration`, m.in.
        // `enable_round_robin_repartition = false`) Ballista 55 nakłada tylko przy PIERWSZYM
        // `upgrade_for_ballista()`, na konfiguracji bez rozszerzenia `ballista`.
        // Settery niżej to rozszerzenie dodają, więc późniejsze `upgrade_for_ballista()` (klient,
        // scheduler, executory) by je pomijały — a Ballista 53 nakładała je zawsze. Bez tego
        // wywołania standalone wstawiał RoundRobinBatch przed węzły operacji (EXPLAIN ANALYZE).
        .upgrade_for_ballista()
        .with_ballista_logical_extension_codec(bio_logical_codec())
        .with_ballista_physical_extension_codec(bio_physical_codec())
        .with_ballista_grpc_client_max_message_size(limit)
        .with_ballista_adaptive_query_planner(ADAPTIVE_QUERY_PLANNER)
        .set_usize(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, MAX_PARTITIONS_PER_TASK)
        .set_bool(BALLISTA_SCHEDULER_SHARE_FILE_STATISTICS_CACHE, SHARE_FILE_STATISTICS_CACHE)
}

/// Bio-owy stan sesji dla podanej konfiguracji. Scheduler MUSI planować na
/// takim stanie, żeby `IntervalJoinPhysicalOptimizationRule` w ogóle zadziałała.
pub fn bio_session_state(config: SessionConfig) -> Result<SessionState> {
    Ok(DFSessionContext::new_with_bio(config).state())
}

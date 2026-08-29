//! Praca magisterska: polars-bio w trybie rozproszonym (Apache Ballista).
//!
//! Target biblioteczny istnieje po to, żeby binarki w `src/bin/` mogły
//! współdzielić kod z `main.rs` — pliki w `src/bin/` to osobne crate rooty
//! i nie mogą importować niczego z `main.rs`.
//!
//! Mapa modułów:
//! - `codec_io`        — prymitywy kodowania binarnego wspólne dla kodeków
//! - `dist_payload`    — `DistOp`, `DistPayload`: parametry konstrukcyjne operacji
//! - `dist_provider`   — `DistBioProvider`: TableProvider owijający provider vendora
//! - `logical_codec`   — `BioDistLogicalCodec`: serializacja providera (klient → scheduler)
//! - `physical_codec`  — `IntervalJoinPhysicalCodec`: serializacja IntervalJoinExec (Faza A.5)
//! - `dist_udtf`       — rejestracja operacji jako funkcji tabelowych SQL
//! - `runner`          — konfiguracja sesji, SQL-e, uruchomienie, dowód dystrybucji

pub mod codec_io;
pub mod dist_payload;
pub mod dist_provider;
pub mod dist_udtf;
pub mod logical_codec;
pub mod physical_codec;
pub mod runner;

pub use dist_payload::{Cols, DistOp, DistPayload, TableRef};

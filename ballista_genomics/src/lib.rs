//! polars-bio genomic interval operations in distributed mode (Apache Ballista).
//!
//! The library target exists so that the binaries in `src/bin/` can share code with
//! `main.rs` - files in `src/bin/` are separate crate roots and cannot import
//! anything from `main.rs`.
//!
//! Module map:
//! - `codec_io`        - binary encoding primitives shared by the codecs
//! - `bio_phys_codec`  - `BioRangesPhysicalCodec`: serialization of the range-operation nodes
//! - `broadcast`       - guard of the broadcast payload size against the gRPC limit
//! - `checksum`        - row-order-independent checksum of a result
//! - `cli`             - strict flag parsing shared by the binaries
//! - `cluster`         - codecs and session state shared by the client and the nodes
//! - `coverage_node`   - `DistCoverageExec`/`DistCoverageProvider`
//! - `dist_payload`    - `DistOp`, `DistPayload`: constructor parameters of the operations
//! - `dist_provider`   - `DistBioProvider`: a TableProvider wrapping the vendored provider
//! - `logical_codec`   - `BioDistLogicalCodec`: provider serialization (client -> scheduler)
//! - `physical_codec`  - `IntervalJoinPhysicalCodec`: IntervalJoinExec serialization
//! - `dist_udtf`       - registration of the operations as SQL table functions
//! - `runner`          - session configuration, SQL, running, proof of distribution
//! - `scenario`        - a benchmark scenario -> SQL in the normalized schema

pub mod bio_phys_codec;
pub mod broadcast;
pub mod checksum;
pub mod cli;
pub mod cluster;
pub mod codec_io;
pub mod coverage_node;
pub mod dist_payload;
pub mod dist_provider;
pub mod dist_udtf;
pub mod logical_codec;
pub mod physical_codec;
pub mod runner;
pub mod scenario;

pub use dist_payload::{Cols, DistOp, DistPayload, TableRef};

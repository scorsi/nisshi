//! nisshi, the log store: the log schema and its validator (ADR 0004), the store of
//! docs/storage-format.md with its commit, seal, restore and hash chain (ADR 0005-0007), and
//! `read`. Shared by the binaries `nisshi` (data CLI) and `nisshi-store` (ADR 0016).

pub mod disk;
pub mod json;
pub mod log;
pub mod logschema;
pub mod read;
pub mod restore;
pub mod seal;
pub mod store;
pub mod time;

/// The ingestion API (ADR 0003), generated from `proto/nisshi/v1/ingest.proto` by build.rs.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/nisshi.v1.rs"));
}

#[cfg(test)]
mod store_tests;

//! nisshi, the log store: the log schema and its validator (ADR 0004). Shared by the binaries
//! `nisshi` (data CLI) and `nisshi-server`.

pub mod json;
pub mod logschema;
pub mod time;

/// The ingestion API (ADR 0003), generated from `proto/nisshi/v1/ingest.proto` by build.rs.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/nisshi.v1.rs"));
}

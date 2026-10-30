//! nisshi, the log store: shared by the binaries `nisshi` (data CLI) and `nisshi-server`.

/// The ingestion API (ADR 0003), generated from `proto/nisshi/v1/ingest.proto` by build.rs.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/nisshi.v1.rs"));
}

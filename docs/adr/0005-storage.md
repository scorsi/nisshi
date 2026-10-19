# ADR 0005 — Storage: NDJSON segments by receive day

Status: Accepted — 2026-10-19

## Context

Logs must read with `zstdcat | jq` without house tools, survive crashes mid-write, and
support filters by node, service and time range. A remote node may deliver weeks of buffered
data at once. Volume: a few MB a day after compression.

## Options considered

- **Parquet or another columnar format**: not readable with `jq`; the schema is open-ended
  (service fields).
- **Partition by event day**: a late batch reopens past days, breaking sealing and the chain
  (0007).
- **Partition by service as well**: dozens of tiny files per node per day.
- **Partition by node and receive day**: append-only, each day written once and sealed once;
  event-time pruning from the manifest's `min_ts`/`max_ts`.
- **An index (offsets, inverted index)**: not needed at a few MB a day.

## Decision

- Layout `<root>/<node>/<YYYY-MM-DD>.ndjson.zst`, day = UTC receive day.
- The open day is a sequence of complete zstd frames, one per commit: readable by `zstdcat` at
  any time; a crash leaves at most one partial trailing frame, cut at restart.
- At seal (0006) a manifest `<YYYY-MM-DD>.manifest.json` records counts, `min_ts`, `max_ts`,
  per-service counts, stream ranges and hashes (0007). It is the only index.
- Full specification: [storage-format](../storage-format.md).

## Consequences

- A query on an event range may open later receive days (late deliveries); manifests keep it
  cheap.
- The open day has no manifest: `nisshi read` scans it fully.

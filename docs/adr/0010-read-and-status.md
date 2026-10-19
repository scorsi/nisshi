# ADR 0010 — Reading and status

Status: Accepted — 2026-10-19

## Context

Exploitation (search, UI) comes later, but from day one the operator must be able to answer
"what did annai answer on this node between 14:00 and 14:05", and something must be able to
tell later whether a node went silent.

## Decision

- `nisshi read <root> --node <n> [--service <s>] [--event <e>] --from <ts> --to <ts>`: reads
  the store's files directly (no server needed, works on a backup copy), prunes days with the
  manifests, prints matching records as NDJSON sorted by `ts`. Times accept RFC 3339 or
  `-1h`-style offsets.
- `GET /v1/status` (identity `reader`): `StatusResponse` of
  [ingest.proto](../../proto/nisshi/v1/ingest.proto) as JSON: per stream the last receive time
  and newest record `ts`, the last agent status per node, free disk, chain heads.
- Own metrics on a loopback listener (`nisshi_*`, masu's metric convention):
  `nisshi_records_total{source_node}`, `nisshi_duplicate_records_total`,
  `nisshi_seq_gaps_total`, `nisshi_push_requests_total{code}`, `nisshi_disk_free_bytes`,
  `nisshi_seals_total{result}`, `nisshi_build_info{version}`.

## Consequences

- Silent-node detection is not implemented here; it is glue over `/v1/status` and the agent's
  heartbeat, built later on another component.

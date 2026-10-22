# ADR 0010 — Reading and status

Status: Accepted — 2026-10-19
Superseded in part by [0013](0013-forbidden-for-wrong-role.md) (403 for a valid token of the wrong role).

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

## Amendment — 2026-10-22 (accepted, owner decision)

Review finding: nisshi review §2.15 (the stream states grow with every new stream id).

- No cap on the number of streams per node: a stream's `hwm` is kept forever (forgetting it
  would let old duplicates in again), and a cap would need a refusal and an eviction policy for
  a case that only a broken or compromised agent creates. Instead nisshi exposes the gauge
  `nisshi_streams{source_node}`, the number of known streams owned by each node (ADR 0014), so
  an alert can catch a node that keeps creating streams.
- The full list of nisshi's own metrics is now: `nisshi_records_total{source_node}`,
  `nisshi_duplicate_records_total`, `nisshi_seq_gaps_total`,
  `nisshi_push_requests_total{code}`, `nisshi_seals_total{result}`,
  `nisshi_streams{source_node}`, `nisshi_disk_free_bytes`, `nisshi_disk_reserve_bytes`,
  `nisshi_build_info{version}`.

# ADR 0003 — Ingestion API and delivery semantics

Status: Accepted — 2026-10-19
Superseded in part by [0013](0013-forbidden-for-wrong-role.md) (403 for a valid token of the wrong role)
and [0014](0014-stream-owner.md) (403 `stream-owner`).

## Context

Agents push (the public edge node cannot be reached from inside). Nothing may be lost
silently, nothing may appear twice. The house contracts are protobuf.

## Options considered

- **OTLP/HTTP logs**: a large proto set; log bodies and attributes would be mapped to and from
  OTLP's model while the store keeps JSON lines; no sequence numbers to deduplicate on.
- **Loki or Elasticsearch push APIs**: tie the format to a product we do not run.
- **gRPC**: works, but a gRPC stack for one request/response.
- **House protobuf over HTTPS POST**: one endpoint, `curl`-debuggable, stdlib only.

## Decision

`POST /v1/push`, body = `PushRequest` ([ingest.proto](../../proto/nisshi/v1/ingest.proto)),
`Content-Type: application/x-protobuf`, `Content-Encoding: zstd`, HTTPS (HTTP/2 when
negotiated). At most 8 MiB compressed, 64 MiB decompressed.

**Streams.** A stream is identified by `stream_id`; each record has a `seq`, consecutive
within the stream. nisshi keeps per stream the highest committed `seq` (`hwm`), committed
atomically with the data (storage-format, "Commit"). Records are stored as received, with one
addition: nisshi appends `recv_ts`, the commit time, same format as `ts`, as the last member of
each object (storage-format, "Records"). The agent already wrote `node`, `stream`, `seq`.

| Request | nisshi does | Response |
|---|---|---|
| `first_seq > hwm + 1` | stores the records, writes a `seq-gap` record (`gap_stream`, `from_seq`, `to_seq`, `service:"nisshi"`) in that node's file, counts `nisshi_seq_gaps_total` | 200 |
| overlaps `hwm` | drops records with `seq <= hwm`, stores the rest | 200, `acked_seq` |
| all `seq <= hwm` | stores nothing, counts `nisshi_duplicate_records_total` | 200, `acked_seq = hwm` |
| no or bad credentials | nothing | 401 |
| a record invalid: not a JSON object, envelope invalid under the published rules (0004), a store field present (`recv_ts`), `node`/`stream`/`seq` not matching the request | nothing (whole batch) | 400, `PushRejected{record_index, reason}` of the first invalid record |
| the request itself invalid: `node` ≠ authenticated node, malformed `stream_id`, `first_seq` = 0 | nothing | 400, `PushRejected{reason}` without index |
| the stream was first pushed by another node, or is nisshi's reserved stream (0014) | nothing | 403, `PushRejected{reason: "stream-owner"}` without index |
| free space below `disk_reserve`, or a write error | rolls back to last commit | 507 |
| body too large | nothing | 413 |

A batch is all or nothing. Delivery is at least once on the wire, exactly once on disk.

**A record-level 400 must never block a stream.** The agent validates with the same rules
before sending (hikyaku ADR 0003), so this should not happen; when it does, `record_index`
lets the agent replace that one record and continue (hikyaku ADR 0005). A request-level 400
is a configuration or agent bug that no replacement can fix: the agent stops that stream and
says so loudly.

## Consequences

- Ordering is guaranteed within a stream only; readers sort by `ts`.
- A store restored from an older backup sees `first_seq > hwm + 1` and records the gap rather
  than hiding it.

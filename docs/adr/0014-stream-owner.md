# ADR 0014 — A stream belongs to the node that first pushed it

Status: Accepted — 2026-10-22 (owner decision)

Supersedes in part: [0003](0003-ingestion-api.md) (the response table gains a 403 row). Review
findings: nisshi review §2.5 and §2.10, cross review §2.3.

## Context

Tokens are per node (0008) and every record must carry the authenticated `node` (0003), but
the writer state is keyed by `stream_id` alone. A node token that learns another node's stream
id (from a backup, an export, `/v1/status`, a cloned spool) could push that stream as itself:
the records land in its own file, yet they advance the other node's `hwm`, and the owner's
next records are then dropped as duplicates and acknowledged. Stream ids are not secrets.
The reserved stream (32 zeros) that nisshi uses for its own `seq-gap` records was accepted from
agents too, so an agent could store `(000…000, 1)` before nisshi did, breaking `(stream, seq)`
uniqueness.

## Options considered

- **Key the state by `(node, stream)`**: two nodes could then share a stream id, which breaks
  the store-wide `(stream, seq)` uniqueness of the storage format.
- **400 without index**: hikyaku reads a request-level 400 as its own bug; this is an
  authorization refusal of a valid request.
- **403 with a `PushRejected` body**: the token is valid, the action is not allowed for it.

## Decision

- A stream belongs to the node that first committed a record to it. A push of an existing
  stream from another authenticated node is refused with **403** and the body
  `PushRejected{reason: "stream-owner"}` without `record_index`. Nothing is stored, no state
  changes.
- The reserved stream `00000000000000000000000000000000` belongs to nisshi: a push of it from
  any agent gets the same 403 `stream-owner`.
- Order: after authentication, the role (0013), the size and body checks and the request-level
  400s (`node-mismatch`, `stream-id-format`, `first-seq-zero`), before any record check. It is
  checked under the store lock before deduplication, so two racing first pushes of a new stream
  from two nodes give one 200 and one 403.
- The owner is the `node` of the stream's state, persisted in `state.json`; a store rebuilt
  from its sealed days takes it from the node directory the stream was found in (0005
  amendment).
- `nisshi_push_requests_total{code="403"}` counts it with the wrong-role 403s; nisshi logs a
  `stream-owner` warn event with the node and the stream id.
- Delivery scenario `fixtures/delivery/stream-owner.json` (mirrored with masu, 0012).

## Consequences

- hikyaku must read the `PushRejected` body of a 403 when present (a wrong-role 403 has none)
  and treat `stream-owner` like the other 403: stop that stream and say so.
- A node that legitimately took over another node's spool (a reinstall under another name)
  must start a new stream: the old one stays the old node's.
- masu implements the same rule (ADR 0003 mirror, proposed there).

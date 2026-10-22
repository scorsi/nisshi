# ADR 0007 — Integrity: hash chain from v0

Status: Accepted — 2026-10-19

## Context

The logs will feed a security analysis (kenbun). Tampering or silent corruption of past days
must be detectable. A chain added later would leave the first months, the baseline,
unverifiable.

## Options considered

- **A format that allows a chain, added later**: the early history stays unverifiable.
- **Chain per record**: a hash per line, heavy, and the open day would need it too.
- **Chain per sealed day**: one manifest per node and day with the content hash and the hash
  of the previous manifest.

## Decision

- Chain per sealed node-day, from v0, specified in [storage-format](../storage-format.md).
  Content hash on decompressed bytes: recompaction and backups do not touch it.
- `nisshi verify <root>`: every content hash (when the data file exists), every link,
  `(stream, seq)` uniqueness; non-zero exit on any mismatch. The same checks run with
  `zstdcat`, `sha256sum` and `jq` alone.
- Chain heads are exposed in `/v1/status` (`chain_heads`) so that something outside the
  store's host can record them later (anchoring).

## Consequences

- The open day is not covered until sealed: at most 24 hours of unchained data.
- Someone with write access to the store's host can rebuild a consistent chain; only heads
  recorded elsewhere detect it. Anchoring is an open question.

## Amendment — 2026-10-22 (accepted, owner decision: notes, no design change)

What a passing `nisshi verify` proves, and what it does not (nisshi review §3.1):

- It proves that the files present agree with the manifests present: each sealed day's
  decompressed content matches its manifest (hash, count, read strictly), each manifest links
  to the previous one present, and no `(stream, seq)` appears twice in the data present.
- It does not prove completeness: removing the newest sealed days of a node (data and
  manifests), or a whole node, leaves a consistent chain. Only chain heads recorded outside the
  host detect it, and only for the prefix they saw; anchoring stays open.
- It does not prove origin or truth: a stolen node token pushes valid records that are sealed
  like the others.
- A manifest without its data file is accepted as retention, whatever removed the data
  (ADR 0006 amendment); the open day is not covered (no manifest, `state.json` not compared).
- The unchained window is the open day plus any day whose seal failed or was deferred, and
  the time nisshi was down: watch `nisshi_seals_total{result="error"}` and the age of the
  newest manifest, not only the existence of a head.

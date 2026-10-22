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

## Amendment — 2026-10-22 (accepted, owner decision)

Review findings: nisshi review §2.2, §2.4, §2.11, §3.5; cross review §2.1.

- **Durability and recovery**: a new day file also syncs its node directory and the root.
  At start, a committed length whose file is missing or shorter, an unreadable directory, or an
  invalid `state.json` stops the writer before any change: acknowledged data was lost and an
  operator decides; nothing is "repaired" by extending a file. One writer per root, held by an
  exclusive `flock` on the root directory.
- **Restore**: `state.json` is not backed up. Backups hold the sealed days with their manifests
  and, twice a day (RPO about 12 hours), the committed prefix of each open day file as
  `<node>/<day>.ndjson.zst.partial`. The procedure (empty data directory, copy, rename each
  `.partial` to `.ndjson.zst`, start) is in [storage-format](../storage-format.md),
  "Restore". Without `state.json`, nisshi rebuilds each stream's `hwm` and owner (0014) and
  its own `seq` from the files, checking sealed days against their manifests and keeping open
  days open, so a restored store neither stores a backed-up record twice nor reuses one of its
  own `seq-gap` numbers. A day file without manifest and unknown to an existing `state.json`
  is still truncated to zero: restoring files under a stale state is not a restore.
- **Restore ends on a whole frame** (validation R2 residual): an open day file restored without
  `state.json` must end exactly on a complete zstd frame; a corrupt or incomplete tail stops the
  start and changes nothing, instead of being cut. A corrupt block size in a middle frame can
  run past the end of the file and look like a cut final frame, and cutting it would delete the
  valid frames after it. The backup copies whole frames (committed length, `.tmp` then rename),
  so a legitimate restore never ends mid-frame; an operator cuts a known-garbage tail by hand.
  Crash recovery with `state.json` (committed lengths known) is unchanged.

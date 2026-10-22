# ADR 0006 — Sealing, compaction and retention

Status: Accepted — 2026-10-19

## Context

The open day is one small zstd frame per commit, which compresses JSON logs about 5×; a whole
day recompressed reaches about 10×. History is the priority: deletion must be deliberate,
never a reaction to disk pressure.

## Decision

- **Seal** every day file of a past UTC day, at 00:10 UTC and at start-up (idempotent): scan
  the content (count, `min_ts`/`max_ts`, services, stream ranges, SHA-256), recompress into a
  temporary file (`SpeedBetterCompression`, 8 MiB window), decompress it again and compare the
  hash, rename over the original, write the manifest (temporary + rename), drop the file from
  `state.json`. A line that is not valid JSON stops the seal of that file
  (`nisshi_seals_total{result="error"}`, `error` log); the file stays as is.
- **Retention**: default **400 days** (a year of baseline plus a margin for year-on-year
  comparison), `0` keeps everything (TOML has no null). The daily job deletes the data files of sealed days
  older than that and **keeps their manifests**, so the chain stays verifiable.
- **Disk pressure never deletes data.** Below `disk_reserve` (default 2 GiB) pushes get 507;
  agents keep spooling. `nisshi_disk_free_bytes` and `/v1/status` show it.

## Consequences

- Disk estimates in [resources](../resources.md).
- Sealed files never change again: incremental backups stay cheap.

## Amendment — 2026-10-22 (accepted, owner decision)

Review findings on the seal path (nisshi review §2.1, §2.13):

- **Order of a seal**: the manifest is published (temporary + rename) *before* the recompressed
  file replaces the original. `content_sha256` is of the decompressed bytes, so both files match
  the manifest: a crash at any step leaves either an open day (no manifest, original file) or a
  sealed day whose file matches its manifest, never a recompressed file that recovery would cut
  to the committed length of the original. A recompressed temporary left by a crash is deleted at
  start. For a few milliseconds a sealed day may still hold its original frames; the content,
  and so `verify`, is the same.
- **A failed seal holds back its node**: when a day of a node fails to seal, the later days of
  that node are not sealed either (reported as errors) until it is fixed, since the chain would
  otherwise link past it and could not take it back.

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

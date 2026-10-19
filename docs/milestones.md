# nisshi — milestones

Each milestone ends with passing tests (ADR 0011). The stack-wide order (with hikyaku, masu
and the services) is in the deployment repository.

## M0 — Schema and validator

- Repository on Forgejo with GitHub mirror; flake (flake-parts, treefmt, `buildGoModule`,
  `CGO_ENABLED=0`); `go.mod` limited to ADR 0002; `proto/` with `buf generate`, generated Go
  committed and checked.
- `schema/log-v1.json`; `nisshi validate --logs` reading lines on stdin (raw service output or
  `journalctl -o cat`), failing with the line number of the first invalid line.
- Fixtures (ADR 0004, 0012): `fixtures/logs/service-lines.ndjson` and `records.ndjson` with
  their exact reasons (at least: missing `ts`, 6-digit `ts`, non-UTC offset, unknown `level`,
  an agent field written by a service, `recv_ts` present, a line over 1 MiB, not an object,
  invalid UTF-8; and every example of log-schema.md as valid), `fixtures/delivery/` with the
  initial scenario set of delivery-scenarios.md.
- Flake outputs: `packages.nisshi`, `lib.validateLogs.<system>` (path of the binary for other
  repos' tests), `lib.fixtures` (path of `fixtures/`), `nixosModules.default`.
- **Done when**: `nix flake check` green on darwin and Linux; `nisshi validate --logs` gives the
  expected verdict and reason on every fixture line. This unblocks the services'
  instrumentation, hikyaku's validator and masu's scenario tests.

## M1 — Store

- `nisshi serve`: config TOML, unknown keys rejected (`listen`, `listen_metrics`, `data_dir`, `tls.cert_file`,
  `tls.key_file`, `tokens` = map hex SHA-256 → identity, `retention_days` (400, `0` = keep),
  `disk_reserve`).
- `POST /v1/push` with the table of ADR 0003, `GET /v1/status`, own `/metrics` on loopback.
- Commit and recovery, seal and compaction at 00:10 UTC and at start, retention keeping
  manifests, `seq-gap` records.
- `nisshi read`, `nisshi verify`.
- NixOS module `services.nisshi` (config rendered with `pkgs.formats.toml`): `enable`, `package`, `listen` (required, list),
  `dataDir` (default `/var/lib/nisshi`, fixed path for persistence), `tls.{certFile, keyFile}`
  (read through `LoadCredential`), `tokens.<sha256 hex> = "node:<name>" | "reader"`,
  `retentionDays` (400; `null` rendered as `0`), `diskReserve` (2 GiB); static user `nisshi`, `MemoryMax=128M`,
  `GOMEMLIMIT=64MiB`, `Type=notify`, hardening as genkan; no firewall opening.
- **Tests (nixosTest with the test pusher)**:
  - every scenario of `fixtures/delivery/` (duplicates, overlap, gap, two streams, 401,
    request-level 400, indexed 400 with `record_index` and `reason`, `kill -9`, disk full);
  - every record fixture through the ingestion path: valid ones stored, invalid ones refused
    with the fixture's reason and index;
  - after the disk-full scenario: `nisshi verify` passes and `zstd -t` on every file;
  - every stored record ends with a `recv_ts` in the `ts` format whose day is the file's day,
    and is otherwise byte-identical to what was pushed;
  - seal with a faked clock: manifest, `content_sha256` and chain checked with `zstdcat`,
    `sha256sum` and `jq` only (no nisshi binary in that step);
  - `zstdcat | jq` on the open day and on a sealed day;
  - retention deletes data files and keeps manifests, `nisshi verify` still passes;
  - `nisshi read` returns the pushed `annai` `query` records of a given interval for a given
    node, sorted.
- **Done when**: all M1 tests pass and resources.md holds measured figures (24 h of real logs).

## M2 — mTLS (after hanko issues SVIDs)

- `auth.methods`, SPIFFE ID mapping (ADR 0008).
- **Done when**: nixosTest with a test CA standing in for hanko-ca: certificate accepted,
  expired or foreign certificate refused, token and certificate nodes side by side.

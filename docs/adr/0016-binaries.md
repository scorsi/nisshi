# ADR 0016 — Three binaries: data CLI, server, query daemon

Status: Accepted — 2026-10-30 (owner decision)

## Context

One `nisshi` binary validated log lines, read and verified stores, and ran the store server
from a configuration file. Other repositories call the validator in their tests
(`lib.validateLogs`), operators call `read` and `verify` on a store or a backup, and a query
daemon for yagura and kenbun will read the same files with its own configuration. A binary that
mixes data tools with a server's configuration grows options that only one of its uses needs.

## Options considered

- **One binary with subcommands**: one package, but the data tools carry the server's
  configuration and dependencies, and a third role makes it larger again.
- **One binary per role over one library**: the format, the log rules and the commit, seal and
  restore logic live once, in a library; each binary owns its configuration.

## Decision

A Cargo workspace:

- **`nisshi-core`** (library): the single implementation of the storage format, the log
  schema and its validator, the hash chain, commit, recovery, seal, restore, `verify` and
  `read`.
- **`nisshi`** (data CLI): `validate --logs`, `read`, `verify`, with the arguments, output and
  exit codes of before. It reads no configuration; it knows only data and its format.
  `lib.validateLogs` still names it.
- **`nisshi-store`** (datastore): the only writer of a store root: ingestion, sealing,
  retention, `/v1/status`, `/metrics`. `nisshi-store serve <config.toml>` serves,
  `nisshi-store check-config <config.toml>` checks a configuration and exits non-zero with a
  message when it is invalid, `nisshi-store --version` prints the version. The NixOS module
  runs `check-config` on the rendered file at build time.
- **`nisshi-query`** (planned, not built): a read-only query daemon over the files, with its
  own configuration and `check-config`. The name is reserved for it.

Each binary validates its own configuration. `nisshi-testpush` (ADR 0011) is a fourth crate,
for tests only, not shipped.

## Consequences

- `nisshi serve` is gone: `services.nisshi` runs `nisshi-store`, with the same options and
  defaults; the package holds both binaries.
- An invalid configuration fails the system build, not the service start.

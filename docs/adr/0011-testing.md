# ADR 0011 — Testing

Status: Accepted — 2026-10-19

## Decision

- **Go tests** (`nix flake check`): envelope validation (accept every example of log-schema.md,
  reject a fixture set), the response table of 0003, commit and recovery with a crash injected
  at every step, seal idempotence, manifest chain, retention keeping manifests.
- **nixosTests** are the definition of done, with standard tools as the oracle (`zstdcat`,
  `jq`, `sha256sum`, `curl`). nisshi does not depend on hikyaku: its tests push with a small
  test pusher (a Go command in the repository, not shipped), which runs every delivery scenario
  of `fixtures/delivery/` (0012). End-to-end tests with the agent live in hikyaku.
- **Fixtures** of `fixtures/logs/` (0004) run against `nisshi validate --logs` and the
  ingestion path, checking the exact reasons.
- A breakage seen on real hosts becomes a nixosTest before it is fixed.

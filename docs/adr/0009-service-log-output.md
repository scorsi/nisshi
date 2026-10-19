# ADR 0009 — How services write logs

Status: Accepted — 2026-10-19

## Context

House services are written in Go, Rust, Zig, Nim and Elixir and run under systemd on Linux and
launchd on macOS, where journald does not exist. annai prints `key=value` to stderr, kekkai
uses `tracing` to stderr, genkan `log/slog`.

## Options considered

- **journald native protocol with structured fields** (`ANNAI_QNAME=`): filterable with
  `journalctl ANNAI_QNAME=…`, but a client per language (none in the stdlib of Zig, Nim or
  Elixir), absent on macOS, and the agent would rebuild JSON from upper-case fields.
- **One JSON object per line on stderr**: no dependency anywhere (Go `slog.NewJSONHandler`, Rust
  `tracing-subscriber` `json` or `serde_json`, Zig `std.json`), identical under journald and
  in a launchd log file; `journalctl -u annai -o cat | jq` works.

## Decision

- **Services write one schema object per line on stderr.** No journald client.
- **Rate limiting**: services that log per request (annai, genkan) set
  `LogRateLimitIntervalSec=30s` and `LogRateLimitBurst=30000` as `mkDefault` in their NixOS
  module: 1000 lines/s sustained. Not disabled: an unbounded flood would fill the journal and
  evict other units' history. Suppressed lines are counted by hikyaku from journald's own
  message (hikyaku ADR 0003). Above 1000 lines/s, sampling is the service's job (annai ADR
  0011, v2).
- **Forward Secure Sealing: no.** It seals the local journal, a transit buffer here; the store
  has its own chain (0007).
- **Per language**: the tested reference code of [log-schema](../log-schema.md) (Go `log/slog`
  with a `ReplaceAttr` for UTC 9-digit `ts`; Rust with a hand-written timestamp and
  `serde_json`, or `tracing-subscriber` JSON with that timestamp, which for kekkai needs a
  dependency ADR in its repository; Zig 0.16 with `std.json.Stringify` and the caller's wall
  clock); Elixir and Nim hand-written when they need it. No shared library (0004).

## Consequences

- `journalctl` field filters do not work on service fields; `journalctl -o cat | jq` does.

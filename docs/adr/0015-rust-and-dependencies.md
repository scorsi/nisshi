# ADR 0015 — Language: Rust, and the dependency register

Status: Accepted — 2026-10-30 (owner decision)

Supersedes: [0002](0002-language-and-dependencies.md).

## Context

ADR 0002 chose Go for a store that only appends, seals and keeps. nisshi now has a future
beyond that: a query engine over its files, for a UI (yagura) and an anomaly analyser
(kenbun), and a deployment on a modest NAS where memory and CPU are scarce. Its sibling store,
masu, is written in Rust with the same delivery semantics and the same crash-safety approach
(masu ADR 0002); one language for both stores lets them share one dependency register and one
way of writing the same mechanisms.

## Options considered

| | For | Against |
|---|---|---|
| Keep Go | works, stdlib covers the store | a GC'd runtime on a small NAS for a scan-heavy query engine; a second language beside masu for the same mechanisms |
| Rust | control of memory and allocation for the query engine, no GC; masu's language and crates, so one register for both stores | more dependencies than Go's stdlib; the rewrite itself |

## Decision

Rust, built with crane, as a Cargo workspace (binaries: ADR 0016). The rewrite is like for
like: the ingestion API, HTTP behaviour, storage format, `state.json`, CLI and the module's
options do not change.

Direct dependencies, the same crates as masu:

| Crate | Why |
|---|---|
| `tokio` (`rt-multi-thread` with 2 workers, `net`, `fs`, `time`, `signal`, `macros`, `sync`) | runtime of `nisshi-store` |
| `hyper` (`server`, `http1`, `client`), `hyper-util` (`tokio`) | HTTP server, HTTP/1.1 only (ADR 0003); the client side for the test pusher only |
| `rustls` (`ring` provider, TLS 1.2 and 1.3), `tokio-rustls`, `rustls-pemfile` | TLS without a C toolchain for crypto |
| `prost` (`prost-build` at build time) | ingestion API; `PushRequest` is decoded by hand on prost's wire primitives so that records alias the body |
| `zstd` (vendored libzstd) | data files and push bodies; open-day frames at level 3, seals at level 7, 8 MiB window, content checksum |
| `serde` (`derive`), `serde_json` | reading `state.json`, manifests and fixtures; `/v1/status` |
| `toml` | configuration file, read with `#[serde(deny_unknown_fields)]` |
| `sha2` | content and manifest hashes, token hashes |

This is masu's register, crate for crate and feature for feature: no crate is added.

Written by hand, not taken from a crate:

- **JSON as the log rules define it.** The log schema's parser edges (log-schema.md) and the
  files written so far follow the JSON reading and writing of the first implementation (Go's
  `encoding/json`): lone surrogate escapes are valid strings, numbers are syntax-checked only,
  member names compare after unescaping, nesting is bounded. A small scanner reproduces that;
  `state.json` and manifests are written byte for byte as before. `serde_json` is used only
  where these corners cannot occur.
- **Time**: the `ts` format, calendar days, RFC 3339 and duration parsing of `nisshi read`
  and `NISSHI_FAKE_NOW`, with the accepted forms and messages of before.
- `statvfs`, `flock` and `sd_notify` through `std` and two `extern "C"` declarations (no
  `libc` crate), argument parsing, logging (ADR 0009) and the metrics exposition.

Any other crate needs a new ADR. A crate that is already a transitive dependency may become a
direct one, listed here.

## Consequences

- The storage format is untouched: a store written by one implementation is opened, sealed,
  verified and read by the other, `state.json` and manifests byte for byte. The compressed
  bytes of the frames differ (another encoder); the format never fixed them.
- The unit keeps `GOMEMLIMIT=64MiB` in its environment, without effect, so that the deployed
  unit and its acceptance tests do not change; `MemoryMax=128M` is the bound. The memory
  figures of [resources](../resources.md) were measured on the Go implementation and are to be
  measured again.
- One test pusher language for both stores (Rust); masu and nisshi still share no code.

# ADR 0002 — Language and dependencies

Status: Superseded by [0015](0015-rust-and-dependencies.md) — accepted 2026-10-19

## Context

nisshi receives a few MB of logs a day: an HTTPS server, protobuf, JSON validation, zstd,
SHA-256, careful file I/O. Its future (search for kenbun, a read API) is I/O and parsing,
not computation. It runs on a NixOS VM with memory to spare but not to waste.

## Options considered

| | For | Against |
|---|---|---|
| Go | stdlib HTTPS server with HTTP/2, `crypto/tls`, JSON, SHA-256; official protobuf; pure-Go zstd; genkan and hanko use it | GC, irrelevant here |
| Rust | memory, speed | a dependency tree (tokio, hyper, rustls, prost, serde_json, zstd, sha2) for an I/O-bound service |
| Elixir | concurrency, hanshou's language | a BEAM release for an append-only writer; zstd through a NIF |
| Zig | annai's language | no TLS server, no zstd compressor in std |

## Decision

Go, `CGO_ENABLED=0`, `buildGoModule`. External modules allowed:

| Need | Module | Why not stdlib |
|---|---|---|
| zstd | `github.com/klauspost/compress/zstd` | no zstd in stdlib; pure Go, standard frames |
| protobuf | `google.golang.org/protobuf` | ingestion API, status as protojson |
| TOML | `github.com/pelletier/go-toml/v2` | configuration file; `Decoder.DisallowUnknownFields()` turns a misspelled key into an error, as in genkan (genkan ADR 0005) |

Configuration is TOML, like the owner's other projects, rendered by the module with
`pkgs.formats.toml`. Internal files (state, manifests, segments) stay JSON and NDJSON.
JSON Schema validation of the envelope is hand-written (a dozen fixed fields), not a library;
`schema/log-v1.json` is published for other languages. Any other module needs a new ADR.

## Consequences

- klauspost's encoder levels (`SpeedDefault` ≈ zstd 3, `SpeedBetterCompression` ≈ zstd 7)
  replace zstd's numeric levels; the budget is re-measured with it (resources.md).

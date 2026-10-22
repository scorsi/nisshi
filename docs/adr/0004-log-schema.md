# ADR 0004 — Common log schema and its publication

Status: Accepted — 2026-10-19

## Context

Five languages write logs. The analysis layer needs the same field for the same meaning across
services and across years. annai already logs `level=… event=…` key=value lines.

## Options considered

- **Namespaced service fields** (`annai.qname`): collision-free but awkward in `jq` and in
  each language's logger.
- **Flat fields with a reserved envelope**: envelope names reserved, every other field belongs
  to the `(service, event)` pair that emits it.
- **Publishing in a separate contracts repository**: one more repository for a document whose
  only enforcer is nisshi.

Harmonisation across services:

- **A shared logging library per language** (Go, Rust, Zig, Elixir, Nim): five packages to
  version and release, a runtime dependency in every service, cross-repository pin updates on
  each change, for 20–50 lines of code each. Today the services to instrument are one per
  language (annai Zig, genkan Go, kekkai Rust): a library would have one user. And a library
  does not prove conformity: a service can bypass it or lag behind it.
- **Libraries in togi**: togi is development tooling (formatters, linters, hooks); runtime code
  there would make a linter update a runtime change for every repository.
- **Validation of the real output in each service's tests**, plus reference code to copy.

## Decision

- The schema is [log-schema.md](../log-schema.md): envelope written by the service (`v`, `ts`,
  `level`, `event`, optional `msg`, `err`), envelope added by the agent (`node`, `service`,
  `stream`, `seq`, `source`), flat `snake_case` service fields, units in names (`dur_us`,
  `bytes`). Never secrets, keys, tokens, request bodies or query strings.
- **Versioning**: `v` is the envelope's major version (1). Adding an event or a field is not a
  version change. Changing the meaning or type of an existing field of an event, or removing
  it, means a new event name (`query` → `query2`): the history stays interpretable without
  knowing release dates. `v` changes only if the envelope does.
- **Publication**: in this repository: the document, `schema/log-v1.json` (JSON Schema of the
  envelope, for other languages), and `nisshi validate --logs`, exported by the flake
  (`packages.nisshi`, `lib.validateLogs`). Each house service's nixosTest pipes its journal
  through it, with nisshi as a test-only input.
- **Fixtures are the executable rules.** `fixtures/logs/` holds two sets, exported by the flake
  (`lib.fixtures`):
  - `service-lines.ndjson`: what a service writes (used by `nisshi validate --logs` and by
    hikyaku before it assigns a `seq`);
  - `records.ndjson`: complete records as pushed (used by nisshi's ingestion and by hikyaku's
    record builder).

  Each fixture line is `{"input": "<raw line>", "valid": true|false, "reason": "<rule>"}`;
  `reason` is the exact string nisshi puts in `PushRejected.reason` (0003) and hikyaku in
  `unparsed.reason`. nisshi's and hikyaku's tests both run every fixture; a rule change is a
  fixture change first, so the two implementations cannot drift silently.
- Each service documents its events and fields in its own repository (`docs/logging.md`).
- **Harmonisation by validation, not by library.** Conformity is proven by `nisshi validate
  --logs` on each service's real output in its nixosTest. log-schema.md carries tested
  reference code per language (Go `slog`, Rust, Zig) to copy. When togi exists, it provides a
  reusable nixosTest helper: a service declares its units and gets the validation step without
  copying it. A shared library is written only when a second or third service in the same
  language rewrites the same code (Go is the likely first); it would live in its own repository
  or next to this schema, never in togi.

## Consequences

- nisshi rejects batches that violate the envelope (0003); service fields are not checked
  centrally.
- A service repository takes nisshi as an input for tests only: no runtime dependency.

## Amendment — 2026-10-22 (accepted, owner decision: notes, no design change)

What the fixtures do not fix yet (nisshi review §3.4): Go's `encoding/json` (nisshi) and
`serde_json` (hikyaku) differ outside the current fixtures, on JSON nesting depth (10 000
against about 128), on numbers out of `f64` range in service fields (`1e10000`: Go accepts
the syntax, Rust refuses), and on lone UTF-16 surrogates (`\ud800`). A line in one of these
corners can pass `nisshi validate --logs` and still be wrapped as `unparsed` by hikyaku: the
bytes travel, their interpretation changes. Free service fields named like envelope members
in another case (`TS`, `SEQ`) are accepted and never read as the envelope (storage-format).
Deciding these limits means fixtures first, in both implementations.

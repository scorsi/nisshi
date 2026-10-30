# ADR 0012 — Delivery scenarios as shared data

Status: Accepted — 2026-10-19

## Context

masu and nisshi implement the same delivery semantics (streams, `hwm`, deduplication, gaps,
indexed 400, 401, 507) in two languages and two repositories, with no shared code (masu ADR
0001, nisshi ADR 0001). Written twice, the test cases would drift exactly where the stores must
agree, since hikyaku's client logic assumes one behaviour.

## Options considered

- **Each store writes its own cases**: two lists that drift where the stores must agree.
- **A shared test pusher**: one binary in one language driving both stores; a cross-repository
  test dependency and a third program to maintain.
- **One copy, consumed by the other store as a test-only flake input**: a dependency between
  the two stores that exists only for tests.
- **Scenarios as data, mirrored in both repositories**: identical files in each, run by each
  store's own pusher; a written rule (each repository's `CLAUDE.md`) that a change on one side
  is made on the other in the same change.

## Decision

- The scenarios are JSON files in `fixtures/delivery/`, format in
  [delivery-scenarios.md](../delivery-scenarios.md). They are **mirrored**: masu holds
  byte-identical copies under the same names. Neither store depends on the other, not even for
  tests.
- They are signal-agnostic: they speak of record counts, sequence numbers, tokens and expected
  responses; each pusher generates valid records of its own signal and, where a scenario asks
  for it, an invalid one.
- nisshi's test pusher and masu's (both Rust since nisshi ADR 0015, no code shared) each run
  every scenario of their own copy.
- The mirror rule is written in both repositories' `CLAUDE.md`: a change to `fixtures/delivery/`
  or to `delivery-scenarios.md` is made in both repositories, with the implementation change it
  implies, before either is committed.

## Consequences

- The copies can drift if the rule is ignored; `sha256sum fixtures/delivery/*` in both
  repositories must print the same list, a manual check during review.
- hikyaku's end-to-end tests stay its own (they need the real agent), but use the same
  vocabulary.

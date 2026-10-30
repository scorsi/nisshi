# ADR 0001 — Scope and boundaries

Status: Accepted — 2026-10-19

## Context

Nothing is stored today and annai and genkan do not log each request yet. A security-oriented
analysis layer ([kenbun](../../../kenbun)) and alerting ([hansho](../../../hansho)) will come
later and must re-read the whole history. Metrics live in [masu](../../../masu), a separate
store with its own format: no code is shared between the two.

## Options considered

- **One store for logs and metrics**: one receiver, but two formats with different futures
  (masu becomes a TSDB) behind one release cycle.
- **A log store that indexes for search from day one**: a search engine to build before the
  first query exists.
- **A store that only appends, seals and keeps**: history first, exploitation later.

## Decision

nisshi owns:

- the **log schema** (0004) and its validator, used by every house service's tests;
- the **ingestion API** `nisshi.v1` (0003);
- the **storage format** (0005, [storage-format](../storage-format.md)), its sealing,
  retention (0006) and integrity chain (0007);
- `nisshi read`, `nisshi verify`, `nisshi validate` and `GET /v1/status` (0010).

It does not: search, aggregate, alert, display, or check that nodes are alive (it only exposes
what a checker needs).

## Consequences

- The storage format is a contract: kenbun, yagura or hansho read it with or without nisshi's
  code. Changing it means a new format version, never an in-place change.
- hikyaku depends on nisshi (protos, end-to-end tests); nisshi tests itself with a test
  pusher and depends on nothing in the stack.

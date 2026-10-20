# ADR 0013 — 403 for a valid token of the wrong role

Status: accepted (owner decision 2026-10-20)

Supersedes in part: [0003](0003-ingestion-api.md) (the 401 row of the response table),
[0008](0008-authentication.md) (what a token may do), [0010](0010-read-and-status.md)
(`GET /v1/status` refusals). Identical in masu.

## Context

ADRs 0003, 0008 and 0010 answer 401 to every request whose identity may not use the endpoint,
including a valid `reader` token on `POST /v1/push` and a valid `node:<x>` token on
`GET /v1/status`. A 401 tells the client to authenticate, which it already did: the operator
cannot tell a wrong token from a token used in the wrong place.

## Options considered

- **Keep 401 everywhere**: one code, but it hides a configuration mistake behind an
  authentication failure.
- **403 for an authenticated identity without the role**: HTTP's meaning of 401 (no valid
  credentials) and 403 (valid credentials, not allowed).

## Decision

| Request | Response |
|---|---|
| no `Authorization`, or a token that maps to no identity | 401 with `WWW-Authenticate: Bearer` (unchanged) |
| a valid `reader` token on `POST /v1/push` | 403, no body |
| a valid `node:<x>` token on `GET /v1/status` (and any later reader-only endpoint) | 403, no body |
| a `node:<x>` token pushing a request whose `node` is another node | 400 `node-mismatch` (unchanged) |

The role check comes right after authentication, before any request check. 403s on
`POST /v1/push` are counted in `nisshi_push_requests_total{code="403"}` like every other code.

## Consequences

- An agent configured with the reader token, or a dashboard with a node token, gets a
  distinct code and log line from a revoked or mistyped token.
- The delivery scenarios (`fixtures/delivery/auth.json`, mirrored with masu) keep only the 401
  cases; the 403 cases are asserted by the unit tests and the VM test script.

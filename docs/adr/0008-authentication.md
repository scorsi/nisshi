# ADR 0008 — Authentication before and after hanko

Status: Accepted — 2026-10-19
Superseded in part by [0013](0013-forbidden-for-wrong-role.md) (403 for a valid token of the wrong role).

## Context

nisshi must refuse unknown agents and know which node sent a batch (a node writes only its
own records). hanko will issue public certificates first, then internal X.509-SVIDs (hanko ADR
0004, SPIFFE model). kekkai authenticates peers but is part of the monitored stack.

## Options considered

- **kekkai identity** (trust the overlay source address): ties the observability of the
  network to the network itself. Rejected: nisshi must be reachable over any path.
- **mTLS with a private CA now**: a CA to run before hanko exists, which is hanko's job.
- **Per-node bearer token over TLS**: stdlib only, replaceable without changing the API.

## Decision

- **Now: TLS + per-node token.** `Authorization: Bearer <token>`, 32 random bytes,
  base64url. Config maps hex `sha256(token)` → identity (`node:<name>` or `reader`); the hashes
  are not secrets and live in plain Nix. Constant-time comparison. `node:<name>` may push
  records with `node = <name>` only; `reader` may only `GET /v1/status`.
- **Server certificate**: files given in config. Before hanko: self-signed, pinned by agents
  (`ca_file`), agents verify `server_name` so an IP address works without DNS.
- **Step 1, hanko public certificate**: nisshi's certificate comes from hanko, agents drop the
  pin. Tokens unchanged.
- **Step 2, hanko SVIDs**: `auth.methods = [ "token", "mtls" ]`; a client certificate with
  SPIFFE ID `spiffe://<trust-domain>/node/<name>` authenticates as `node:<name>`. Nodes switch
  one by one, then `token` is removed. No flag day.

## Consequences

- Tests cover: no token, unknown token, another node's token (400), and in step 2 an expired
  or foreign client certificate.
- A stolen token writes fake logs for one node until rotated; during rotation old and new
  hashes are both listed.

# nisshi (日誌)

> Log store of the sekkeizu observability stack: receives logs from [hikyaku](../hikyaku),
> keeps them losslessly as hash-chained NDJSON segments, owns the common log schema. Rust.

*日誌 (nisshi): the logbook, written day after day and never rewritten.*

**Status:** M1 (store: `nisshi-store`, `nisshi read`, `nisshi verify`, NixOS module `services.nisshi`);
mTLS is M2.

## Role

- **Ingestion API** (`nisshi.v1`, protobuf over HTTPS) with exactly-once storage.
- **Storage**: one zstd NDJSON file per node and receive day, sealed daily with a manifest
  and a hash chain; readable with `zstdcat | jq`.
- **The common log schema** every house service writes, with its validator.
- **`nisshi read`**: logs filtered by node, service, event and time range. **`nisshi
  verify`**: hashes and chain.
- **`GET /v1/status`**: last receive time per node, for whatever checks silent nodes later.

## Not its job

Search language, UI, alerting, analysis ([kenbun](../kenbun)), metrics ([masu](../masu)).

## Is every node still delivering?

Silent-node detection is deliberately outside the minimal base; it comes with the rest of the
observability stack. Until then, by hand, with a `reader` token:

```sh
curl -s --cacert nisshi-ca.pem -H "Authorization: Bearer $(cat reader-token)" \
  https://<store>:9473/v1/status \
  | jq -r '.streams[] | [.node, (.lastReceiveNs | tonumber / 1e9 | todate)] | @tsv'
```

A node whose last receive is minutes old while its agent should be pushing is silent; the
agent's `heartbeat` events (every 5 minutes) make that true for logs too. `.agents` shows each
agent's spool state (`paused`, oldest unacked record) **as of its last successful push**: it
is refreshed only by a push that nisshi accepted, and lost when nisshi restarts. Read it
together with the node's last receive time, never as a live probe of the agent. After a
restore, `lastReceiveNs` is 0 until the stream's next push.

## Documents

- [ADRs](docs/adr/README.md), [log schema](docs/log-schema.md),
  [storage format](docs/storage-format.md), [ingestion API](proto/nisshi/v1/ingest.proto),
  [milestones](docs/milestones.md), [resources](docs/resources.md)

## Relationships

| Direction | Project | What |
|---|---|---|
| fed by | hikyaku | `nisshi.v1` ingestion API |
| schema used by | every house service | log lines on stderr, `nisshi validate` in their tests |
| read by | kenbun, yagura, hansho (later) | the storage format, or a read API added then |

## Conventions

Nix flake, NixOS module, `nixosTest`. Docs in English. Public repository: no host names,
domains or addresses of any deployment.

## Testing

`nix flake check` runs the Rust tests, clippy, the fixtures and the module evaluation everywhere; the
nixosTests (`checks.<linux-system>.vm-delivery`, `vm-fixtures`, `vm-seal`) need a Linux
builder. They drive `nisshi.service` with `nisshi-testpush` (`crates/nisshi-testpush`, test-only,
not shipped) over `fixtures/delivery/` and `fixtures/logs/records.ndjson`. The environment
variable `NISSHI_FAKE_NOW` (RFC 3339) freezes the store's clock, for the seal and retention
tests only: `recv_ts`, the receive day, `sealed_at` and the retention cut-off all follow it.

# nisshi (日誌)

> Log store of the sekkeizu observability stack: receives logs from [hikyaku](../hikyaku),
> keeps them losslessly as hash-chained NDJSON segments, owns the common log schema. Go.

*日誌 (nisshi): the logbook, written day after day and never rewritten.*

**Status:** M0 (log schema, `nisshi validate --logs`, fixtures); the store itself is M1.

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
agent's spool state (`paused`, oldest unacked record).

## Documents

- [ADRs](docs/adr/README.md), [log schema](docs/log-schema.md),
  [storage format](docs/storage-format.md), [ingestion API](proto/nisshi/v1/ingest.proto),
  [milestones](docs/milestones.md), [resources](docs/resources.md)

## Relationships

| Direction | Project | What |
|---|---|---|
| fed by | hikyaku | `nisshi.v1` ingestion API |
| schema used by | every house service | log lines on stderr, `nisshi validate` in their tests |
| read by | kenbun, yagura, hanshou (later) | the storage format, or a read API added then |

## Conventions

Nix flake, NixOS module, `nixosTest`. Docs in English. Public repository: no host names,
domains or addresses of any deployment.

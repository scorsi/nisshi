# ADR 0017 — nisshi-query: records and counts over the query document

Status: Proposed — 2026-11-01

## Context

Reading nisshi today is `nisshi read` on the files (ADR 0010). yagura Y2 needs filtered
records with paging and a histogram of record counts per level over time, by HTTPS, for a
reader (`yagura/docs/store-query-needs.md`); kenbun and hansho will read the same. ADR 0016
reserves `nisshi-query`. The stores share one request contract, the query document (masu ADR
0013): a batch of named queries over one `[from, to)` range, answered in one pass, protobuf
or protojson over HTTPS POST, with common frames, errors and paging.

## Options considered

- **Routes of their own** (`/v1/records`, `/v1/histogram`): one request per tile and a shape
  that diverges from masu's and nenpyo's.
- **The query document with two nisshi operators**: chosen.

## Decision

`nisshi-query serve <config.toml>` is a separate read-only daemon over `nisshi-core` with its
own `check-config` (ADR 0016): root, HTTPS listeners, TLS files, token-hash-to-identity map.
It has read access only; it never recovers, seals, restores or writes. It serves
`POST /v1/query` as masu ADR 0013 specifies (HTTPS, HTTP/1.1 only, both encodings, identity
`reader`, 401/403 before decoding as ADRs 0008 and 0013, envelope messages and
`docs/query-document.md` mirrored byte-identical), with `proto/nisshi/v1/query.proto`:

```proto
message Query {
  oneof body {
    RecordsQuery records = 1;
    CountsQuery counts = 2;
  }
}
message RecordsQuery {
  repeated string node = 1;     // any of; empty = all
  repeated string service = 2;
  repeated string event = 3;
  repeated string level = 4;
  uint32 limit = 5;             // default 1000, at most 10 000
  string after = 6;             // cursor of a previous result
}
message CountsQuery {
  repeated string node = 1;
  repeated string service = 2;
  repeated string event = 3;
  repeated string level = 4;
  google.protobuf.Duration step = 5;  // at least 1 min; at most 10 000 buckets in the range
  repeated string by = 6;             // level, node, service, event
}
```

- **`records`**: a `TableFrame` whose rows are the stored record objects of ADR 0004, with
  `recv_ts`, unmodified; `from <= ts < to`; sorted by `ts`, then `node`, `stream`, `seq`;
  `cursor` while more remain, bound to the query's filters and range (masu ADR 0013).
- **`counts`**: a `CountsFrame`, one `CountGroup` per distinct combination of the `by` fields,
  `counts[i]` the records in bucket `i`.
- Selection by manifests' `min_ts`/`max_ts` and the record's `ts`, never by receive-day
  filename (late deliveries); open day files are read to their committed length from
  `state.json`, never an appending tail. The hash chain is not verified by a query: `nisshi
  verify` remains the integrity check.
- Times are RFC 3339 only in the API; the `-1h` offsets of `nisshi read` are resolved by the
  CLI. Initial budgets, until measured: 64 queries and 31 days per request, 512 MiB
  decompressed, 500 000 records decoded, 10 s wall time, 2 concurrent requests, 2 000 000
  rows and buckets accumulated across results, 16 MiB response; per query the `limit` and
  bucket ceilings above. A per-request scan budget hit is 422 `scan-limit`, an aggregate
  result one 422 `response-limit` counted while frames are built, a per-query one that
  query's `result-limit`. `step > 0`; the last bucket is cut at `to`.
- Own metrics on a loopback listener: `nisshi_query_requests_total{code}`,
  `nisshi_query_records_read_total`, `nisshi_build_info{version}`.
- `GET /v1/status` stays on `nisshi-store` (ADR 0010).

## Consequences

- yagura's log explorer and histogram are one request per refresh; the "live" view is polling
  with the cursor, as the needs state. yagura never reads nisshi's files.
- `prost-types` becomes a direct dependency (well-known types), recorded in ADR 0015 on
  acceptance; it is already transitive through `prost-build`.
- `nisshi read` is unchanged by this ADR; whether it becomes a client of the daemon, as masu
  proposes in its ADR 0015, is a separate decision.

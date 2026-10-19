# Storage format — `nisshi-store 1`

Enough to read, verify or re-ingest a nisshi store without nisshi's code. ADRs 0005–0007.

## Layout

```
<root>/
  FORMAT                           one line: "nisshi-store 1"
  state.json                       writer state (Commit); readers ignore it
  <node>/<YYYY-MM-DD>.ndjson.zst
  <node>/<YYYY-MM-DD>.manifest.json   present once the day is sealed
```

- `<node>` matches `[a-z0-9][a-z0-9-]{0,62}`.
- `<YYYY-MM-DD>` is the UTC day nisshi **received** the records, not the event day.
- A manifest without its data file: retention deleted the data and kept the manifest (0006).

## Data files

- A concatenation of complete zstd frames (RFC 8878), no dictionary, window ≤ 8 MiB:
  `zstdcat` decodes it with default limits.
- Decompressed: NDJSON, UTF-8, one record per line, each line ends with `\n`.
- Records follow [log-schema](log-schema.md). Readers ignore unknown fields and key order.
- Each record is the pushed bytes unchanged, with `,"recv_ts":"<commit time>"` inserted before
  the final `}`: the agent's JSON is never re-serialized. A record is at most 4 MiB.
- `(stream, seq)` is unique across the store.
- Records written by nisshi itself (`seq-gap`) use `stream` = 32 zeros and nisshi's own `seq`
  counter, and carry `recv_ts` like the others.
- The open day may end with a partial frame while a write is in progress; a reader stops at a
  truncated last frame.

## Manifest

```json
{
  "v": 1,
  "node": "node-a",
  "day": "2026-10-19",
  "records": 182311,
  "uncompressed_bytes": 51239110,
  "content_sha256": "3b0c…",
  "prev_day": "2026-10-18",
  "prev_manifest_sha256": "a91f…",
  "min_ts": "2026-10-18T23:59:58.120000000Z",
  "max_ts": "2026-10-19T23:59:59.990321000Z",
  "services": {"annai": 120311, "genkan": 40112, "sshd": 31},
  "streams": [{"stream": "9b1e…", "first_seq": 4112, "last_seq": 186422}],
  "sealed_at": "2026-10-20T00:10:02.000000000Z"
}
```

- `content_sha256`: SHA-256 of the **decompressed** bytes. Recompression does not change it.
- `prev_manifest_sha256`: SHA-256 of the bytes of the previous manifest file of the same
  node (`prev_day`); `null` for the first.

## Commit (writer)

1. Append the new frames to the day file, `fsync`.
2. Write `state.json.tmp` (committed length of each open day file, `hwm` and last times per
   stream, nisshi's own `seq`), `fsync`, rename over `state.json`, `fsync` the directory.
3. Only then answer 200.

At start, each open day file is truncated to its committed length; a day file absent from
`state.json` and without a manifest is truncated to zero.

## Reading and verifying with standard tools

```sh
zstdcat node-a/2026-10-19.ndjson.zst | jq -c 'select(.service == "annai" and .event == "query")'

zstdcat node-a/2026-10-19.ndjson.zst | sha256sum
jq -r .content_sha256 node-a/2026-10-19.manifest.json

sha256sum node-a/2026-10-18.manifest.json
jq -r .prev_manifest_sha256 node-a/2026-10-19.manifest.json

zstdcat node-a/*.ndjson.zst | jq -r '[.stream, .seq] | @tsv' | sort | uniq -d   # must print nothing
```

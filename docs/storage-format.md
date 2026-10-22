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
  Member names are case-sensitive: a service field `TS` or `SEQ` is not the envelope's `ts`
  or `seq`.
- Each record is the pushed bytes unchanged, with `,"recv_ts":"<commit time>"` inserted before
  the final `}`: the agent's JSON is never re-serialized. A record is at most 4 MiB.
- `(stream, seq)` is unique across the store.
- Records written by nisshi itself (`seq-gap`) use `stream` = 32 zeros and nisshi's own `seq`
  counter, and carry `recv_ts` like the others.
- The open day may end with a partial frame while a write is in progress; a reader stops at a
  truncated last frame. A sealed day has none: a partial frame or a last line without its
  `\n` in a sealed file is corruption, and so is a line over 4 MiB anywhere.

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

1. Append the new frames to the day file, `fsync` (a new day file: also `fsync` its node
   directory and the root).
2. Write `state.json.tmp` (committed length of each open day file, `hwm` and last times per
   stream, nisshi's own `seq`), `fsync`, rename over `state.json`, `fsync` the directory.
3. Only then answer 200.

At start, each open day file is truncated to its committed length; a day file absent from
`state.json` and without a manifest is truncated to zero. A committed length whose file is
missing or shorter is acknowledged data lost: the writer refuses to start and changes nothing.
One writer per root: it holds an exclusive `flock` on the root directory.

## Restore (writer)

A backup holds the sealed days with their manifests, and, taken twice a day by the deployment,
the committed prefix of each open day file (its length in `state.json`) as
`<node>/<YYYY-MM-DD>.ndjson.zst.partial`. `state.json` is never backed up: it may name open
data the backup does not hold.

1. Stop nisshi; start from an **empty** data directory.
2. Copy `FORMAT`, every `<node>/<day>.ndjson.zst` and `<node>/<day>.manifest.json`, and every
   `.partial` file of the backup into it. Leave out everything else the backup tree holds: the
   deployment's `LAST_RUN` marker at its root and any `*.tmp` file (a copy the backup did not
   finish).
3. Rename each `<day>.ndjson.zst.partial` to `<day>.ndjson.zst`.
4. Start nisshi.

The writer reads only `FORMAT`, `state.json` and the node directories (names matching
`<node>`): another file or directory at the root (`LAST_RUN`, `*.tmp`, `lost+found`) is ignored,
never read as node data. A `.partial` file still in a node directory stops the start: step 3
was skipped.

At start without `state.json`, the writer rebuilds its state from the files before accepting
pushes, and logs one `restore` event:

- each sealed day is checked against its manifest (content hash, record count, stream ranges)
  when its data file exists; a mismatch stops the start;
- each open day (no manifest) keeps its complete frames, a truncated last frame is cut, and the
  day stays open; a frame that is corrupt rather than cut short (bad magic, reserved block
  type, block over 128 KiB) stops the start, so that no valid frame after it is ever cut;
- each stream's `hwm` is its highest `seq` found, its owner the node directory it was found in
  (a stream in two node directories stops the start), nisshi's own `seq` the highest `seq` of
  the `seq-gap` records.

Records received after the backup are lost; agents whose spool still holds them resend them
(`first_seq > hwm + 1` is a reported gap, ADR 0003). Agents may resume as soon as nisshi is up.

## Reading and verifying with standard tools

```sh
zstdcat node-a/2026-10-19.ndjson.zst | jq -c 'select(.service == "annai" and .event == "query")'

zstdcat node-a/2026-10-19.ndjson.zst | sha256sum
jq -r .content_sha256 node-a/2026-10-19.manifest.json

sha256sum node-a/2026-10-18.manifest.json
jq -r .prev_manifest_sha256 node-a/2026-10-19.manifest.json

zstdcat node-a/*.ndjson.zst | jq -r '[.stream, .seq] | @tsv' | sort | uniq -d   # must print nothing
```

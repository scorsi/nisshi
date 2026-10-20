# Log schema v1

One log record = one JSON object on one line (UTF-8, no embedded newline). A service line is
at most 1 MiB; a stored record (envelope and `raw_b64` included) at most 4 MiB (4 194 304
bytes). The store inserts `,"recv_ts":"<ts>"` before the final `}`, always 43 bytes (`ts`
format), so a pushed record is at most 4 194 261 bytes. ADR 0004.

**The rules are the fixtures.** This document describes them; `fixtures/logs/` in this
repository is their executable definition, run by nisshi and by hikyaku (ADR 0004). A rule
added here without a fixture is not a rule.

## Envelope written by the service

| Field | Type | Required | Meaning |
|---|---|---|---|
| `v` | integer | yes | envelope major version, `1` |
| `ts` | string | yes | UTC, RFC 3339, **exactly 9 fractional digits**, `Z` suffix: `2026-10-19T08:14:03.512004117Z`. Taken by the service when the event happens. Fixed width, so it sorts as a string |
| `level` | string | yes | `debug`, `info`, `warn`, `error` |
| `event` | string | yes | kebab-case name of what happened (`query`, `request`, `upstream-timeout`) |
| `msg` | string | no | free text for humans; never parsed |
| `err` | string | no | error text; only when `level` is `warn` or `error` |

## Envelope added by the agent

A service must not write these: a service line carrying one is invalid, and the agent wraps
it as `unparsed` rather than guess which value is right.

| Field | Type | Meaning |
|---|---|---|
| `node` | string | node name, `[a-z0-9][a-z0-9-]{0,62}` |
| `service` | string | systemd unit without `.service`, or the name of a file input |
| `stream` | string | 32 lowercase hex chars, the agent stream |
| `seq` | integer | position in the stream, from 1 |
| `source` | string | only on wrapped lines: `journald` or `file` |

## Field added by the store

| Field | Type | Meaning |
|---|---|---|
| `recv_ts` | string | commit time at nisshi, same format as `ts`; appended as the last member of the object. Its UTC day is the file's day |

Reserved: all of the above, and any name starting with `_`. A service line carrying a reserved
field is invalid (the agent wraps it as `unparsed`); a pushed record carrying `recv_ts` is
refused.

## Validation and reasons

`nisshi validate --logs` checks service lines; the store checks pushed records with the same
code (ADR 0003). The checks run in this order and the first failure is the reason, the exact
string of `PushRejected.reason` and of an `unparsed` record's `reason`:

| # | Check | Reason |
|---|---|---|
| 1 | size: a service line over 1 MiB, a pushed record over 4 194 261 bytes (4 MiB stored, less the 43-byte `recv_ts` member) | `line-too-large`, `record-too-large` |
| 2 | UTF-8 | `not-utf8` |
| 3 | records only: the bytes start with `{`, end with `}` (no surrounding whitespace: the store inserts `recv_ts` before the last byte) and hold no CR or LF byte (the store keeps one record per line). A service line may have surrounding whitespace | `framing` |
| 4 | exactly one JSON value | `not-json` |
| 5 | that value is an object | `not-object` |
| 6 | no member name twice at the top level (first repeated name, after unescaping) | `duplicate-field:<name>` |
| 7 | no reserved name, first one in document order: in a service line the agent's and the store's fields and `_*`; in a record `recv_ts` and `_*` | `reserved-field:<name>` |
| 8 | fields in this order: `v`, `ts`, `level`, `event`, `msg`, `err`, then for records `node`, `service`, `stream`, `seq`, `source` | `missing-field:<name>` when required and absent, `<name>-format` when present and wrong |

Field formats: `v` is the literal `1` (not `1.0`, not `"1"`); `ts` as above and a real UTC date
and time; `level` one of the four strings; `event` matches `^[a-z][a-z0-9]*(-[a-z0-9]+)*$`;
`msg` a string; `err` a string, and only when `level` is `warn` or `error` (else `err-format`);
`node` as above; `service` a non-empty string; `stream` 32 lowercase hex chars; `seq` an
integer from 1 to 2^64-1 written in plain digits; `source` `journald` or `file`. Nested objects
and service fields are not checked. Fixture format: [fixtures/logs/README.md](../fixtures/logs/README.md).

### Ingestion reasons (M1)

`POST /v1/push` (ADR 0003) also refuses a request whose envelope does not match the request
itself. These reasons come from the store only, never from `nisshi validate --logs`, and have
no fixture line in `fixtures/logs/`:

| Level | Check | Reason |
|---|---|---|
| request, 400 without `record_index` | the `Content-Encoding` header is `zstd` (any case; absent or another value fails) | `content-encoding` |
| request, 400 without `record_index` | the body is a valid zstd stream | `body-zstd` |
| request, 400 without `record_index` | the decompressed body is a `PushRequest` | `body-protobuf` |
| request, 400 without `record_index` | the request's `node` is the authenticated node | `node-mismatch` |
| request, 400 without `record_index` | `stream_id` is 16 bytes (the 32 hex chars of `stream`) | `stream-id-format` |
| request, 400 without `record_index` | `first_seq` is not 0 | `first-seq-zero` |
| record, 400 with `record_index` | the record's `node` is the request's `node` | `node-mismatch` |
| record, 400 with `record_index` | the record's `stream` is the request's `stream_id` in hex | `stream-mismatch` |
| record, 400 with `record_index` | the record's `seq` is `first_seq` + its index | `seq-mismatch` |

The request checks run in table order before any record, after authentication and the role
check (401, 403: ADR 0013). A body over the size limits of ADR 0003 is 413 (a declared
`Content-Length` over 8 MiB before the `content-encoding` check). A record is first checked
against the rules above, then against the request: the first failure of the first invalid
record is the reason.

## Wrapped lines

Third-party units (sshd, forgejo, systemd) and house lines that fail to parse:

```json
{"v":1,"ts":"2026-10-19T08:14:03.512004000Z","level":"info","event":"line","msg":"Accepted publickey for root from 198.51.100.7 port 51122 ssh2","node":"node-a","service":"sshd","stream":"9b1e0c5d7a2f4e8b9c0d1e2f3a4b5c6d","seq":88120,"source":"journald"}
```

`event` is `line` (third party) or `unparsed` (a line that looked like JSON but fails the rules;
`reason` carries the rule that failed, as in the fixtures). `ts` comes from journald's
`__REALTIME_TIMESTAMP` (µs, padded to 9 digits), `level` from `PRIORITY` (0–3 `error`,
4 `warn`, 5–6 `info`, 7 `debug`). A non-UTF-8 message is stored as `msg_b64`. A line over
1 MiB (files only; journald splits long lines itself) is cut to 1 MiB with `truncated: true`.
A wrapped record is valid by construction: the agent never produces a record that its own
validation would refuse.

## Events of the stack itself

| Service | Event | Fields |
|---|---|---|
| hikyaku | `agent-start` | `version`, `fresh_spool` |
| hikyaku | `heartbeat` (every 5 min) | `spool_bytes`, `unacked` |
| hikyaku | `spool-full`, `spool-resumed` | `signal`, `spool_bytes` |
| hikyaku | `journald-suppressed` | `unit`, `count` |
| hikyaku | `journal-gap` | `from_ts`, `to_ts` |
| hikyaku | `file-truncated` | `path` |
| hikyaku | `rejected` (`level:"error"`, replaces a record the store refused, same `seq`) | `err` (store's reason), `raw_b64` (the refused record's bytes), `service` and `ts` of the refused record when readable, else `hikyaku` and the agent's time |
| nisshi | `seq-gap` (written in the node's file) | `gap_stream`, `from_seq`, `to_seq` |

## annai

```json
{"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"info","event":"query","proto":"udp","client":"203.0.113.9","qname":"example.org.","qtype":"SOA","rcode":"NOERROR","aa":true,"cache":"none","size":112,"dur_us":38}
{"v":1,"ts":"2026-10-19T08:14:03.910331870Z","level":"info","event":"query","proto":"udp","client":"192.0.2.1","qname":"forgejo.org.","qtype":"AAAA","rcode":"NOERROR","aa":false,"cache":"hit","size":96,"dur_us":11}
{"v":1,"ts":"2026-10-19T08:14:05.000118034Z","level":"warn","event":"upstream-timeout","upstream":"9.9.9.9:53","qname":"example.net.","qtype":"A"}
{"v":1,"ts":"2026-10-19T08:15:12.774019220Z","level":"info","event":"update","key":"hanko","owner":"_acme-challenge.git.example.org.","op":"add","records":1}
```

`query`, one per query including REFUSED and FORMERR: `proto` (`udp`/`tcp`), `client` (source
IP, no port), `qname` (lowercase, trailing dot; absent if unparsable), `qtype` (mnemonic or
`TYPEnnn`), `rcode`, `aa`, `cache` (`hit`, `miss`, `none` when authoritative), `size`
(response bytes), `dur_us`. Existing `key=value` events become JSON with the same names.

## genkan

```json
{"v":1,"ts":"2026-10-19T08:20:41.003991512Z","level":"info","event":"request","listener":"edge","route":"forgejo","client":"192.0.2.1","method":"GET","host":"git.example.org","path":"/owner/repo","proto":"HTTP/2.0","tls":"1.3","status":200,"bytes":51231,"dur_us":1830}
{"v":1,"ts":"2026-10-19T08:20:44.120004091Z","level":"warn","event":"upstream-error","listener":"edge","route":"forgejo","err":"dial tcp 127.0.0.1:3000: connection refused"}
```

`request`, one per request when the response completes: `path` without query string, `route`
= configured route name or `-`, `bytes` = response body bytes sent.

## kekkai

No requests; its info events (kekkai ADR 0019) take the schema:

```json
{"v":1,"ts":"2026-10-19T08:30:00.410020931Z","level":"info","event":"handshake-completed","peer":"node-b","endpoint":"198.51.100.20:4802","initiator":true}
{"v":1,"ts":"2026-10-19T08:31:12.000913112Z","level":"info","event":"path-change","peer":"node-b","from":"relayed","to":"direct","reason":"hole-punch-succeeded"}
```

`peer` = configured peer name. Never a key, a nonce or a payload.


## Reference code

Copy, do not depend: there is no shared logging library (ADR 0004). Each snippet was run on
2026-10-19 and its output checked against this schema. Whatever a service uses, its nixosTest
runs `nisshi validate --logs` on its real output.

### Go (`log/slog`)

slog's defaults break the schema twice: local time, and `RFC3339Nano` drops trailing zeros.

```go
func newLogger() *slog.Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				return slog.String("ts", a.Value.Time().UTC().Format("2006-01-02T15:04:05.000000000Z"))
			case slog.LevelKey:
				return slog.String("level", strings.ToLower(a.Value.String()))
			case slog.MessageKey:
				if a.Value.String() == "" {
					return slog.Attr{} // msg is optional in the schema
				}
			}
			return a
		},
	})
	return slog.New(h).With("v", 1)
}
```

Use: `log.Info("", "event", "request", "status", 200)`; the slog message is the schema's
optional `msg` (dropped when empty), `event` is an attribute.

### Rust (no date crate)

```rust
fn ts_now() -> String {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    ts_from(d.as_secs() as i64, d.subsec_nanos())
}

// civil_from_days (Howard Hinnant), no date crate needed.
fn ts_from(secs: i64, nanos: u32) -> String {
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if month <= 2 { 1 } else { 0 };
    format!("{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{nanos:09}Z", sod / 3600, sod % 3600 / 60, sod % 60)
}
```

A line: `serde_json::json!({"v": 1, "ts": ts_now(), "level": "info", "event": "handshake-completed", "peer": peer})`
written with `eprintln!("{line}")`. With `tracing`, the same `ts_now` goes into a custom
`FormatTime` of `tracing-subscriber`'s JSON formatter, whose field names must then be mapped.

### Zig 0.16

`std.time.nanoTimestamp` no longer exists: the caller passes the wall clock.

```zig
fn writeTs(w: *std.Io.Writer, ns: i128) !void {
    const secs: u64 = @intCast(@divFloor(ns, std.time.ns_per_s));
    const frac: u64 = @intCast(@mod(ns, std.time.ns_per_s));
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    try w.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        yd.year,               md.month.numeric(),       md.day_index + 1,
        ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
        frac,
    });
}

/// `now_ns`: wall clock in nanoseconds (clock_gettime REALTIME, or std.Io.Timestamp.now(io, .real)).
fn logQuery(now_ns: i128, qname: []const u8, rcode: []const u8, dur_us: u64) void {
    var buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    writeLine(&w, now_ns, qname, rcode, dur_us) catch return; // shortcut: a line over 1 KiB is dropped
    std.debug.print("{s}", .{w.buffered()});
}

fn writeLine(w: *std.Io.Writer, now_ns: i128, qname: []const u8, rcode: []const u8, dur_us: u64) !void {
    try w.writeAll("{\"v\":1,\"ts\":\"");
    try writeTs(w, now_ns);
    try w.writeAll("\",\"level\":\"info\",\"event\":\"query\",\"qname\":");
    try std.json.Stringify.value(qname, .{}, w); // escapes quotes and control bytes
    try w.print(",\"rcode\":\"{s}\",\"dur_us\":{d}}}\n", .{ rcode, dur_us });
}
```

Call: `clock_gettime(REALTIME)` as annai's `nowUnix`, then
`logQuery(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec, qname, rcode, dur_us)`.
`std.json.Stringify.value` escapes strings; never format a client-controlled string with `{s}`.

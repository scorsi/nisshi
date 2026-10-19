# Log fixtures

The executable rules of [log-schema.md](../../docs/log-schema.md) (ADR 0004), run by nisshi
(`nisshi validate --logs`, the ingestion path) and by hikyaku (validator, record builder).

- `service-lines.ndjson`: lines as a service writes them (no trailing newline in `input`).
- `records.ndjson`: records as an agent pushes them (`PushRequest.records` elements).

One JSON object per line:

| Field | Meaning |
|---|---|
| `input` | the bytes under test, as a JSON string (UTF-8) |
| `input_b64` | instead of `input`: the bytes, standard base64 with padding (used for invalid UTF-8) |
| `input_repeat` | instead of `input`: `{"prefix", "repeat", "count", "suffix"}`, the bytes are `prefix + repeat × count + suffix` (used for lines too large to keep in the file) |
| `valid` | `true` or `false` |
| `reason` | `""` when valid, else the exact reason (log-schema.md, "Validation and reasons") |

Exactly one of `input`, `input_b64`, `input_repeat` is present; no other field. Every example
of log-schema.md is a valid line of one of the two files (checked by nisshi's tests).

//! Checks service lines and pushed records against docs/log-schema.md (ADR 0004). The reasons
//! returned are the fixtures' reasons: `PushRejected.reason` at the store, `unparsed.reason` at
//! the agent.

use crate::json;

/// A service line.
pub const MAX_LINE_BYTES: usize = 1 << 20;
/// A pushed record: stored, with the 43-byte `,"recv_ts":"…"` member, at most 4 MiB.
pub const MAX_RECORD_BYTES: usize = (4 << 20) - 43;
/// The deepest JSON nesting accepted (log-schema.md, "Parser edges"); hikyaku applies the same.
const MAX_DEPTH: usize = 64;

/// One line written by a service, without its newline: `""` when valid.
pub fn line(b: &[u8]) -> String {
    check(b, false).err().unwrap_or_default()
}

/// One record as pushed by an agent: `""` when valid.
pub fn record(b: &[u8]) -> String {
    record_fields(b).err().unwrap_or_default()
}

/// The envelope members the store needs from a valid record.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Fields {
    pub ts: String,
    pub node: String,
    pub service: String,
    pub stream: String,
    pub seq: u64,
}

/// Checks one pushed record and returns its envelope fields, or the reason.
pub fn record_fields(b: &[u8]) -> Result<Fields, String> {
    let obj = check(b, true)?;
    let get = |k: &str| {
        obj.iter()
            .find(|(n, _)| n == k)
            .and_then(|(_, v)| str(v))
            .unwrap_or_default()
    };
    let seq = obj
        .iter()
        .find(|(n, _)| n == "seq")
        .and_then(|(_, v)| std::str::from_utf8(v).ok()?.parse().ok())
        .unwrap_or(0);
    Ok(Fields {
        ts: get("ts"),
        node: get("node"),
        service: get("service"),
        stream: get("stream"),
        seq,
    })
}

/// A JSON string value decoded, `None` for any other value.
fn str(raw: &[u8]) -> Option<String> {
    if raw.first() != Some(&b'"') {
        return None;
    }
    json::unquote(raw)
}

fn is_ts(raw: &[u8]) -> bool {
    let Some(s) = str(raw) else { return false };
    let b = s.as_bytes();
    let shape = b.len() == 30
        && b.iter().enumerate().all(|(i, &c)| match i {
            4 | 7 => c == b'-',
            10 => c == b'T',
            13 | 16 => c == b':',
            19 => c == b'.',
            29 => c == b'Z',
            _ => c.is_ascii_digit(),
        });
    shape && crate::time::parse_ts_shape(&s).is_some_and(crate::time::in_range)
}

/// `^[a-z][a-z0-9]*(-[a-z0-9]+)*$`
pub fn is_event(s: &str) -> bool {
    let b = s.as_bytes();
    b.first().is_some_and(u8::is_ascii_lowercase)
        && b.last() != Some(&b'-')
        && !s.contains("--")
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// `^[a-z0-9][a-z0-9-]{0,62}$`
pub fn is_node(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && b[0] != b'-'
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// `^[0-9a-f]{32}$`
pub fn is_stream(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_seq(raw: &[u8]) -> bool {
    raw.first().is_some_and(|c| (b'1'..=b'9').contains(c))
        && raw.iter().all(u8::is_ascii_digit)
        && std::str::from_utf8(raw).is_ok_and(|s| s.parse::<u64>().is_ok())
}

fn one_of(raw: &[u8], vals: &[&str]) -> bool {
    str(raw).is_some_and(|s| vals.contains(&s.as_str()))
}

const AGENT_FIELDS: [&str; 5] = ["node", "service", "stream", "seq", "source"];

/// `(name, required)` in check order (log-schema.md, "Validation and reasons").
const SERVICE_FIELDS: [(&str, bool); 6] = [
    ("v", true),
    ("ts", true),
    ("level", true),
    ("event", true),
    ("msg", false),
    ("err", false),
];
const RECORD_FIELDS: [(&str, bool); 5] = [
    ("node", true),
    ("service", true),
    ("stream", true),
    ("seq", true),
    ("source", false),
];

fn field_ok(name: &str, raw: &[u8], level: Option<&[u8]>) -> bool {
    match name {
        "v" => raw == b"1",
        "ts" => is_ts(raw),
        "level" => one_of(raw, &["debug", "info", "warn", "error"]),
        "event" => str(raw).is_some_and(|s| is_event(&s)),
        "msg" => str(raw).is_some(),
        "err" => str(raw).is_some() && level.is_some_and(|l| one_of(l, &["warn", "error"])),
        "node" => str(raw).is_some_and(|s| is_node(&s)),
        "service" => str(raw).is_some_and(|s| !s.is_empty()),
        "stream" => str(raw).is_some_and(|s| is_stream(&s)),
        "seq" => is_seq(raw),
        _ => one_of(raw, &["journald", "file"]),
    }
}

fn check(b: &[u8], record: bool) -> Result<Vec<(String, &[u8])>, String> {
    if record && b.len() > MAX_RECORD_BYTES {
        return Err("record-too-large".into());
    }
    if !record && b.len() > MAX_LINE_BYTES {
        return Err("line-too-large".into());
    }
    if std::str::from_utf8(b).is_err() {
        return Err("not-utf8".into());
    }
    // The store inserts recv_ts before the last byte and keeps one record per line on disk.
    if record
        && (b.len() < 2
            || b[0] != b'{'
            || b[b.len() - 1] != b'}'
            || b.iter().any(|&c| c == b'\r' || c == b'\n'))
    {
        return Err("framing".into());
    }
    if too_deep(b) || !json::valid(b) {
        return Err("not-json".into());
    }
    let obj = json::members(b).ok_or("not-object")?;
    for (i, (k, _)) in obj.iter().enumerate() {
        if obj[..i].iter().any(|(o, _)| o == k) {
            // Parsers disagree on which value wins.
            return Err(format!("duplicate-field:{k}"));
        }
    }
    for (k, _) in &obj {
        if k == "recv_ts" || k.starts_with('_') || (!record && AGENT_FIELDS.contains(&k.as_str())) {
            return Err(format!("reserved-field:{k}"));
        }
    }
    let get = |k: &str| obj.iter().find(|(n, _)| n == k).map(|(_, v)| *v);
    let extra: &[(&str, bool)] = if record { &RECORD_FIELDS } else { &[] };
    for (name, required) in SERVICE_FIELDS.iter().chain(extra) {
        match get(name) {
            None if *required => return Err(format!("missing-field:{name}")),
            None => {}
            Some(raw) if !field_ok(name, raw, get("level")) => {
                return Err(format!("{name}-format"));
            }
            Some(_) => {}
        }
    }
    Ok(obj)
}

/// Objects and arrays nested deeper than `MAX_DEPTH`: a byte scan before any decoding. On
/// invalid JSON the answer is irrelevant, the validity check rejects it anyway.
fn too_deep(b: &[u8]) -> bool {
    let (mut depth, mut in_str, mut i) = (0i64, false, 0);
    while i < b.len() {
        let c = b[i];
        if in_str {
            if c == b'\\' {
                i += 1;
            } else if c == b'"' {
                in_str = false;
            }
        } else if c == b'"' {
            in_str = true;
        } else if c == b'{' || c == b'[' {
            depth += 1;
            if depth > MAX_DEPTH as i64 {
                return true;
            }
        } else if c == b'}' || c == b']' {
            depth -= 1;
        }
        i += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regexes() {
        assert!(is_event("a") && is_event("upstream-timeout") && is_event("query2"));
        assert!(!is_event("a-") && !is_event("-a") && !is_event("a--b") && !is_event("1a"));
        assert!(!is_event("") && !is_event("A"));
        assert!(is_node("node-a") && is_node("0") && is_node(&"a".repeat(63)));
        assert!(!is_node("-a") && !is_node(&"a".repeat(64)) && !is_node("Node"));
        assert!(is_seq(b"1") && is_seq(b"18446744073709551615"));
        assert!(!is_seq(b"0") && !is_seq(b"01") && !is_seq(b"18446744073709551616"));
        assert!(!is_seq(b"1.0") && !is_seq(b"\"1\""));
    }

    #[test]
    fn escaped_values() {
        // Values compare after unescaping, as Go's encoding/json does.
        let l = br#"{"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"info","event":"q"}"#;
        assert_eq!(line(l), "");
        assert_eq!(
            line(br#"{"v":1,"v":1}"#),
            "duplicate-field:v",
            "names compare after unescaping"
        );
    }
}

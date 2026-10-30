//! JSON as Go's `encoding/json` reads and writes it, by hand: the log rules (log-schema.md,
//! "Parser edges") and the files written by the Go nisshi are defined by its behaviour, which
//! serde_json does not share on lone surrogates (`\ud800` is a valid string), on numbers of any
//! magnitude, and on escaping.
//!
//! The scanner works on bytes: like Go's, it does not check UTF-8 (callers that must, do).

/// Go's `encoding/json` nesting limit.
const MAX_DEPTH: usize = 10_000;

fn ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

fn string_end(b: &[u8], mut i: usize) -> Option<usize> {
    if b.get(i) != Some(&b'"') {
        return None;
    }
    i += 1;
    loop {
        match *b.get(i)? {
            b'"' => return Some(i + 1),
            b'\\' => match *b.get(i + 1)? {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => i += 2,
                b'u' => {
                    if !b.get(i + 2..i + 6)?.iter().all(u8::is_ascii_hexdigit) {
                        return None;
                    }
                    i += 6;
                }
                _ => return None,
            },
            c if c < 0x20 => return None,
            _ => i += 1,
        }
    }
}

fn digits(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    i
}

fn number_end(b: &[u8], mut i: usize) -> Option<usize> {
    if b.get(i) == Some(&b'-') {
        i += 1;
    }
    match b.get(i)? {
        b'0' => i += 1,
        b'1'..=b'9' => i = digits(b, i),
        _ => return None,
    }
    if b.get(i) == Some(&b'.') {
        let j = digits(b, i + 1);
        if j == i + 1 {
            return None;
        }
        i = j;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let j = digits(b, i);
        if j == i {
            return None;
        }
        i = j;
    }
    Some(i)
}

/// The end of the value starting at `i` (after whitespace), iteratively: no recursion, depth
/// bounded as Go's.
fn value_end(b: &[u8], i: usize) -> Option<usize> {
    let mut stack: Vec<u8> = Vec::new();
    let mut i = ws(b, i);
    loop {
        // A value at i.
        let mut closed = false;
        match *b.get(i)? {
            open @ (b'{' | b'[') => {
                if stack.len() >= MAX_DEPTH {
                    return None;
                }
                let j = ws(b, i + 1);
                let close = if open == b'{' { b'}' } else { b']' };
                if b.get(j) == Some(&close) {
                    i = j + 1;
                    closed = true;
                } else {
                    stack.push(open);
                    i = j;
                    if open == b'{' {
                        i = ws(b, string_end(b, i)?);
                        if b.get(i) != Some(&b':') {
                            return None;
                        }
                        i = ws(b, i + 1);
                    }
                }
            }
            b'"' => {
                i = string_end(b, i)?;
                closed = true;
            }
            b't' | b'f' | b'n' => {
                let lit: &[u8] = match b[i] {
                    b't' => b"true",
                    b'f' => b"false",
                    _ => b"null",
                };
                if !b[i..].starts_with(lit) {
                    return None;
                }
                i += lit.len();
                closed = true;
            }
            _ => {
                i = number_end(b, i)?;
                closed = true;
            }
        }
        if !closed {
            continue;
        }
        // After a value: close containers or move to the next element.
        loop {
            let Some(&top) = stack.last() else {
                return Some(i);
            };
            i = ws(b, i);
            match *b.get(i)? {
                b',' => {
                    i = ws(b, i + 1);
                    if top == b'{' {
                        i = ws(b, string_end(b, i)?);
                        if b.get(i) != Some(&b':') {
                            return None;
                        }
                        i = ws(b, i + 1);
                    }
                    break;
                }
                b'}' if top == b'{' => {
                    stack.pop();
                    i += 1;
                }
                b']' if top == b'[' => {
                    stack.pop();
                    i += 1;
                }
                _ => return None,
            }
        }
    }
}

/// Go's `json.Valid`: exactly one JSON value, surrounded by optional whitespace.
pub fn valid(b: &[u8]) -> bool {
    value_end(b, 0).is_some_and(|e| ws(b, e) == b.len())
}

/// The top-level members of a valid JSON text, names unescaped as Go does, values as raw
/// bytes, in document order; `None` when the value is not an object.
pub fn members(b: &[u8]) -> Option<Vec<(String, &[u8])>> {
    let mut i = ws(b, 0);
    if b.get(i) != Some(&b'{') {
        return None;
    }
    let mut out = Vec::new();
    i = ws(b, i + 1);
    if b.get(i) == Some(&b'}') {
        return Some(out);
    }
    loop {
        let k_end = string_end(b, i)?;
        let key = unquote(&b[i..k_end])?;
        i = ws(b, k_end);
        if b.get(i) != Some(&b':') {
            return None;
        }
        let start = ws(b, i + 1);
        let end = value_end(b, start)?;
        out.push((key, &b[start..end]));
        i = ws(b, end);
        match b.get(i)? {
            b',' => i = ws(b, i + 1),
            b'}' => return Some(out),
            _ => return None,
        }
    }
}

/// Top-level members: unescaped name, raw value.
pub type Members<'a> = Vec<(String, &'a [u8])>;

/// Parses a whole text with Go's `json.Unmarshal` into `map[string]json.RawMessage`: `Ok(None)`
/// for `null`, an error for invalid JSON or another type; a repeated name keeps its last value.
pub fn object_map(b: &[u8]) -> Result<Option<Members<'_>>, String> {
    if !valid(b) {
        return Err("invalid JSON".into());
    }
    let i = ws(b, 0);
    if b[i..].starts_with(b"null") {
        return Ok(None);
    }
    let Some(mut ms) = members(b) else {
        return Err(format!(
            "json: cannot unmarshal {} into Go value of type map[string]json.RawMessage",
            kind(&b[i..])
        ));
    };
    // Last one wins, at the position of the first.
    let mut out: Vec<(String, &[u8])> = Vec::with_capacity(ms.len());
    for (k, v) in ms.drain(..) {
        match out.iter_mut().find(|(o, _)| *o == k) {
            Some(slot) => slot.1 = v,
            None => out.push((k, v)),
        }
    }
    Ok(Some(out))
}

/// The Go type name of a raw value, for `cannot unmarshal` messages.
pub fn kind(raw: &[u8]) -> &'static str {
    match raw.first() {
        Some(b'"') => "string",
        Some(b'{') => "object",
        Some(b'[') => "array",
        Some(b't' | b'f') => "bool",
        _ => "number",
    }
}

fn hex4(b: &[u8]) -> Option<u32> {
    u32::from_str_radix(std::str::from_utf8(b.get(..4)?).ok()?, 16).ok()
}

/// A raw JSON string decoded as Go does: a lone surrogate escape, and each byte of invalid
/// UTF-8, becomes U+FFFD. `None` unless `raw` is exactly one valid JSON string.
pub fn unquote(raw: &[u8]) -> Option<String> {
    if string_end(raw, 0) != Some(raw.len()) {
        return None;
    }
    let s = &raw[1..raw.len() - 1];
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'\\' => {
                let c = s[i + 1];
                i += 2;
                out.push(match c {
                    b'b' => '\u{8}',
                    b'f' => '\u{c}',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'u' => {
                        let r = hex4(&s[i..])?;
                        i += 4;
                        if (0xd800..0xe000).contains(&r) {
                            // A pair, when a low surrogate escape follows a high one.
                            let lo = (s.get(i..i + 2) == Some(b"\\u"))
                                .then(|| hex4(&s[i + 2..]))
                                .flatten();
                            match lo {
                                Some(lo) if r < 0xdc00 && (0xdc00..0xe000).contains(&lo) => {
                                    i += 6;
                                    char::from_u32(0x10000 + ((r - 0xd800) << 10) + (lo - 0xdc00))?
                                }
                                _ => '\u{fffd}',
                            }
                        } else {
                            char::from_u32(r)?
                        }
                    }
                    c => c as char, // " \ /
                });
            }
            _ => {
                let start = i;
                while i < s.len() && s[i] != b'\\' {
                    i += 1;
                }
                push_lossy(&mut out, &s[start..i]);
            }
        }
    }
    Some(out)
}

/// Appends bytes as UTF-8, each byte of an invalid sequence as U+FFFD (Go's per-byte rule).
pub fn push_lossy(out: &mut String, mut b: &[u8]) {
    loop {
        match std::str::from_utf8(b) {
            Ok(s) => {
                out.push_str(s);
                return;
            }
            Err(e) => {
                let ok = e.valid_up_to();
                out.push_str(std::str::from_utf8(&b[..ok]).unwrap());
                out.push('\u{fffd}');
                b = &b[ok + 1..];
            }
        }
    }
}

/// Bytes as a Go string would hold them, per-byte U+FFFD for invalid UTF-8.
pub fn lossy(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len());
    push_lossy(&mut s, b);
    s
}

/// Which Go encoder to imitate.
#[derive(Clone, Copy, PartialEq)]
pub enum Style {
    /// `encoding/json` Marshal: HTML-safe, `\b` and `\f` short forms.
    Marshal,
    /// `log/slog`'s JSON handler.
    Slog,
}

/// A JSON string as Go writes it.
pub fn quote(s: &str, style: Style) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' if style == Style::Marshal => out.push_str("\\b"),
            '\u{c}' if style == Style::Marshal => out.push_str("\\f"),
            '<' | '>' | '&' if style == Style::Marshal => {
                out.push_str("\\u00");
                out.push(HEX[c as usize >> 4] as char);
                out.push(HEX[c as usize & 0xf] as char);
            }
            c if (c as u32) < 0x20 => {
                out.push_str("\\u00");
                out.push(HEX[c as usize >> 4] as char);
                out.push(HEX[c as usize & 0xf] as char);
            }
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity() {
        for ok in [
            "1",
            " {} ",
            "[]",
            "{\"a\":[1,2,{\"b\":null}],\"c\":\"\\ud800\"}",
            "-0.5e+10",
            "1e10000",
            "\"\\u00e9\"",
            "\n[true,false]\t",
        ] {
            assert!(valid(ok.as_bytes()), "{ok}");
        }
        for bad in [
            "",
            "{",
            "{\"a\"}",
            "[1,]",
            "01",
            "1.",
            "1e",
            "\"\\x\"",
            "\"\u{1}\"",
            "1 2",
            "nul",
            "{\"a\":1,}",
            "\"\\u12\"",
        ] {
            assert!(!valid(bad.as_bytes()), "{bad:?}");
        }
        assert!(valid(&[b'"', 0xff, b'"'])); // Go's scanner does not check UTF-8
        let deep = "[".repeat(10_001) + &"]".repeat(10_001);
        assert!(!valid(deep.as_bytes()));
        let ok = "[".repeat(10_000) + &"]".repeat(10_000);
        assert!(valid(ok.as_bytes()));
    }

    #[test]
    fn unescape() {
        assert_eq!(unquote(br#""a\u00e9\n""#).unwrap(), "a\u{e9}\n");
        assert_eq!(unquote(br#""\ud83d\ude00""#).unwrap(), "\u{1f600}");
        assert_eq!(unquote(br#""\ud800""#).unwrap(), "\u{fffd}");
        assert_eq!(unquote(br#""\ud800\u0041""#).unwrap(), "\u{fffd}A");
        assert_eq!(unquote(br#""\udc00x""#).unwrap(), "\u{fffd}x");
        assert_eq!(
            unquote(&[b'"', 0xe2, 0x82, b'"']).unwrap(),
            "\u{fffd}\u{fffd}"
        );
        assert!(unquote(b"1").is_none() && unquote(br#""a" "#).is_none());
    }

    #[test]
    fn object_members() {
        let m = members(br#" {"a" : 1, "b":{"c":[1]}, "\u0061":"x"} "#).unwrap();
        assert_eq!(m.len(), 3);
        assert_eq!(m[1], ("b".to_string(), &br#"{"c":[1]}"#[..]));
        assert_eq!(m[2].0, "a");
        assert!(members(b"[1]").is_none());
        let last = object_map(br#"{"a":1,"a":2}"#).unwrap().unwrap();
        assert_eq!(last, vec![("a".to_string(), &b"2"[..])]);
        assert_eq!(object_map(b"null").unwrap(), None);
        assert!(object_map(b"[1]").is_err() && object_map(b"x").is_err());
    }

    #[test]
    fn go_quoting() {
        assert_eq!(
            quote("a<b>&\"\\\u{8}\u{1}\u{2028}é", Style::Marshal),
            r#""a\u003cb\u003e\u0026\"\\\b\u0001\u2028é""#
        );
        assert_eq!(quote("a<b\u{8}\t", Style::Slog), r#""a<b\u0008\t""#);
    }
}

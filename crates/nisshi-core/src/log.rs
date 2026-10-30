//! Own logs (ADR 0009): one line of the log schema per event on stderr, written as the Go
//! nisshi's `log/slog` JSON handler did: `ts`, `level`, `v`, `event`, then the event's fields.
//! No field is ever named like a reserved member (`node`, `service`, `stream`, `seq`, `source`,
//! `recv_ts`, `_*`): `source_node`, `stream_id`.

use crate::json::{Style, quote};
use std::io::Write;

/// A field value.
pub enum Val<'a> {
    Str(&'a str),
    Int(i64),
    Strs(&'a [String]),
}

/// One schema line, without its newline.
pub fn line(ns: i64, level: &str, event: &str, fields: &[(&str, Val)]) -> String {
    let mut out = format!(
        "{{\"ts\":\"{}\",\"level\":\"{level}\",\"v\":1,\"event\":{}",
        crate::time::format_ts(ns),
        quote(event, Style::Slog)
    );
    for (k, v) in fields {
        out.push(',');
        out.push_str(&quote(k, Style::Slog));
        out.push(':');
        match v {
            Val::Str(s) => out.push_str(&quote(s, Style::Slog)),
            Val::Int(n) => out.push_str(&n.to_string()),
            Val::Strs(list) => {
                // slog marshals slices with encoding/json, HTML escaping off.
                let items: Vec<String> = list
                    .iter()
                    .map(|s| quote(s, crate::json::Style::Marshal))
                    .collect();
                out.push('[');
                out.push_str(&items.join(","));
                out.push(']');
            }
        }
    }
    out.push('}');
    out
}

/// Copies of the lines emitted while a test captures them.
static CAPTURED: std::sync::Mutex<Option<Vec<String>>> = std::sync::Mutex::new(None);

/// Tests: from now on, also keep a copy of every line emitted by this process.
pub fn capture() {
    CAPTURED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(Vec::new);
}

/// Tests: the lines emitted since `capture`.
pub fn captured() -> Vec<String> {
    CAPTURED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default()
}

fn emit(level: &str, event: &str, fields: &[(&str, Val)]) {
    let l = line(crate::time::real_now_ns(), level, event, fields);
    if let Some(c) = CAPTURED.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
        c.push(l.clone());
    }
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{l}");
}

pub fn info(event: &str, fields: &[(&str, Val)]) {
    emit("info", event, fields);
}

pub fn warn(event: &str, fields: &[(&str, Val)]) {
    emit("warn", event, fields);
}

pub fn error(event: &str, fields: &[(&str, Val)]) {
    emit("error", event, fields);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_lines() {
        let ns = 1_792_397_643_512_004_117;
        let listen = vec!["127.0.0.1:9473".to_string()];
        let l = line(
            ns,
            "info",
            "start",
            &[
                ("version", Val::Str("0.1.0-dev")),
                ("listen", Val::Strs(&listen)),
                ("streams", Val::Int(3)),
            ],
        );
        assert_eq!(
            l,
            r#"{"ts":"2026-10-19T08:14:03.512004117Z","level":"info","v":1,"event":"start","version":"0.1.0-dev","listen":["127.0.0.1:9473"],"streams":3}"#
        );
        assert_eq!(crate::logschema::line(l.as_bytes()), "");
        let e = line(
            ns,
            "error",
            "push-failed",
            &[("source_node", Val::Str("a")), ("err", Val::Str("x\n<"))],
        );
        assert_eq!(crate::logschema::line(e.as_bytes()), "");
        assert!(e.contains(r#""err":"x\n<""#));
    }
}

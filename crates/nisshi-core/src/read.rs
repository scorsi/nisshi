//! `nisshi read` (ADR 0010): records of a node filtered by service, event and time range,
//! sorted by `ts`, read from the store's files (no server; works on a backup copy).

use crate::store::{data_files, parse_envelope, read_lines};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

/// Selects records for `read`; `service` and `event` are optional (empty).
pub struct Query {
    pub node: String,
    pub service: String,
    pub event: String,
    /// Unix nanoseconds, within the ts range.
    pub from: i64,
    pub to: i64,
}

/// Prints the matching records as NDJSON sorted by `ts`, pruning sealed days with their
/// manifests.
pub fn read(root: &Path, q: &Query, w: &mut dyn Write) -> Result<(), String> {
    let from = crate::time::format_ts(q.from);
    let to = crate::time::format_ts(q.to);
    let by_day: BTreeMap<String, crate::seal::Manifest> =
        crate::seal::manifests(&root.join(&q.node))?
            .into_iter()
            .map(|m| (m.day.clone(), m))
            .collect();
    let mut recs: Vec<(String, Vec<u8>)> = Vec::new();
    for f in data_files(root)? {
        if f.node != q.node {
            continue;
        }
        if let Some(m) = by_day.get(&f.day)
            && (m.max_ts < from || m.min_ts > to)
        {
            continue;
        }
        read_lines(&f.path, false, |line| {
            let e = parse_envelope(line)?;
            if e.ts >= from
                && e.ts <= to
                && (q.service.is_empty() || e.service == q.service)
                && (q.event.is_empty() || e.event == q.event)
            {
                recs.push((e.ts, line.to_vec()));
            }
            Ok(())
        })?;
    }
    // ts is fixed width: a string sort is a time sort.
    recs.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, line) in recs {
        w.write_all(&line)
            .and_then(|()| w.write_all(b"\n"))
            .map_err(|e| crate::store::errno_text(&e))?;
    }
    Ok(())
}

/// The schema's ts format, RFC 3339, or an offset such as `-1h` from `now`, within the ts
/// range; Go's error texts.
pub fn parse_time(s: &str, now: i64) -> Result<i64, String> {
    let t = if s.starts_with('-') || s.starts_with('+') {
        i128::from(now) + i128::from(crate::time::parse_duration(s)?)
    } else {
        crate::time::parse_rfc3339(s)?
    };
    if !crate::time::in_range(t) {
        return Err(format!(
            "{s}: outside the ts range {} to {}",
            crate::time::format_ts(i64::MIN),
            crate::time::format_ts(i64::MAX)
        ));
    }
    Ok(t as i64)
}

//! The hand-written exposition of ADR 0010, in masu's metric convention: `nisshi_*`, counters
//! `_total`, base units, closed label sets only. Text identical to the Go nisshi's.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;

/// `(key, name, label, help)`, in exposition order.
const FAMILIES: [(&str, &str, &str, &str); 5] = [
    (
        "records",
        "nisshi_records_total",
        "source_node",
        "Records stored, by pushing node.",
    ),
    (
        "duplicates",
        "nisshi_duplicate_records_total",
        "",
        "Records dropped because their seq was already committed.",
    ),
    (
        "gaps",
        "nisshi_seq_gaps_total",
        "",
        "seq-gap records written.",
    ),
    (
        "pushes",
        "nisshi_push_requests_total",
        "code",
        "POST /v1/push requests, by HTTP status code.",
    ),
    (
        "seals",
        "nisshi_seals_total",
        "result",
        "Day files sealed, by result.",
    ),
];

pub struct Metrics {
    /// family → label value → count
    counters: Mutex<BTreeMap<&'static str, BTreeMap<String, u64>>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Metrics {
        let mut c: BTreeMap<&'static str, BTreeMap<String, u64>> =
            FAMILIES.iter().map(|f| (f.0, BTreeMap::new())).collect();
        for (fam, label) in [
            ("duplicates", ""),
            ("gaps", ""),
            ("seals", "ok"),
            ("seals", "error"),
        ] {
            c.get_mut(fam).unwrap().insert(label.into(), 0);
        }
        Metrics {
            counters: Mutex::new(c),
        }
    }

    pub fn add(&self, family: &str, label: &str, n: u64) {
        let mut c = self.counters.lock().unwrap();
        *c.get_mut(family)
            .expect("known family")
            .entry(label.into())
            .or_insert(0) += n;
    }

    /// The exposition; `streams` is the number of known streams per owning node.
    pub fn render(
        &self,
        streams: &BTreeMap<String, u64>,
        free: u64,
        reserve: u64,
        version: &str,
    ) -> String {
        let mut b = String::new();
        {
            let c = self.counters.lock().unwrap();
            for (key, name, label, help) in FAMILIES {
                let _ = write!(b, "# HELP {name} {help}\n# TYPE {name} counter\n");
                for (k, v) in &c[key] {
                    if label.is_empty() {
                        let _ = writeln!(b, "{name} {v}");
                    } else {
                        let _ = writeln!(b, "{name}{{{label}={k:?}}} {v}");
                    }
                }
            }
        }
        b.push_str("# HELP nisshi_streams Known streams (hwm kept forever), by owning node.\n# TYPE nisshi_streams gauge\n");
        for (n, count) in streams {
            let _ = writeln!(b, "nisshi_streams{{source_node={n:?}}} {count}");
        }
        let _ = write!(
            b,
            "# HELP nisshi_disk_free_bytes Free space on the store's filesystem.\n# TYPE nisshi_disk_free_bytes gauge\nnisshi_disk_free_bytes {free}\n"
        );
        let _ = write!(
            b,
            "# HELP nisshi_disk_reserve_bytes Free space below which pushes get 507.\n# TYPE nisshi_disk_reserve_bytes gauge\nnisshi_disk_reserve_bytes {reserve}\n"
        );
        let _ = write!(
            b,
            "# HELP nisshi_build_info Build information.\n# TYPE nisshi_build_info gauge\nnisshi_build_info{{version={version:?}}} 1\n"
        );
        b
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn exposition() {
        let m = super::Metrics::new();
        m.add("records", "node-a", 21);
        m.add("pushes", "200", 1);
        let streams = [("node-a".to_string(), 1)].into_iter().collect();
        let text = m.render(&streams, 5, 6, "0.1.0-dev");
        for want in [
            "# HELP nisshi_records_total Records stored, by pushing node.\n# TYPE nisshi_records_total counter\nnisshi_records_total{source_node=\"node-a\"} 21\n",
            "nisshi_duplicate_records_total 0\n",
            "nisshi_seq_gaps_total 0\n",
            "nisshi_push_requests_total{code=\"200\"} 1\n",
            "nisshi_seals_total{result=\"error\"} 0\nnisshi_seals_total{result=\"ok\"} 0\n",
            "nisshi_streams{source_node=\"node-a\"} 1\n",
            "nisshi_disk_free_bytes 5\n",
            "nisshi_disk_reserve_bytes 6\n",
            "nisshi_build_info{version=\"0.1.0-dev\"} 1\n",
        ] {
            assert!(text.contains(want), "{want} in\n{text}");
        }
    }
}

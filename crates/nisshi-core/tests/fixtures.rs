//! Every line of fixtures/logs/ through the validator, verdict and exact reason, and every
//! example of docs/log-schema.md a valid fixture (ADR 0004, 0011).

use nisshi_core::logschema;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Repeat {
    prefix: String,
    repeat: String,
    count: usize,
    suffix: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    input: Option<String>,
    input_b64: Option<String>,
    input_repeat: Option<Repeat>,
    valid: bool,
    reason: String,
}

fn b64(s: &str) -> Vec<u8> {
    let val = |c: u8| match c {
        b'A'..=b'Z' => c - b'A',
        b'a'..=b'z' => c - b'a' + 26,
        b'0'..=b'9' => c - b'0' + 52,
        b'+' => 62,
        b'/' => 63,
        _ => panic!("not base64: {c}"),
    };
    let mut out = Vec::new();
    for chunk in s.as_bytes().chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        let n = chunk
            .iter()
            .map(|&c| if c == b'=' { 0 } else { u32::from(val(c)) })
            .fold(0u32, |n, v| n << 6 | v);
        out.extend_from_slice(&n.to_be_bytes()[1..4 - pad]);
    }
    out
}

impl Fixture {
    fn bytes(&self) -> Vec<u8> {
        match (&self.input, &self.input_b64, &self.input_repeat) {
            (Some(s), None, None) => s.clone().into_bytes(),
            (None, Some(b), None) => b64(b),
            (None, None, Some(r)) => {
                format!("{}{}{}", r.prefix, r.repeat.repeat(r.count), r.suffix).into_bytes()
            }
            _ => panic!("exactly one of input, input_b64, input_repeat"),
        }
    }
}

fn load(name: &str) -> Vec<Fixture> {
    let path = format!("{}/../../fixtures/logs/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .enumerate()
        .map(|(i, l)| {
            let f: Fixture =
                serde_json::from_str(l).unwrap_or_else(|e| panic!("{name}:{}: {e}", i + 1));
            assert_eq!(f.valid, f.reason.is_empty(), "{name}:{}", i + 1);
            f
        })
        .collect()
}

#[test]
fn fixtures() {
    for (name, check) in [
        (
            "service-lines.ndjson",
            logschema::line as fn(&[u8]) -> String,
        ),
        ("records.ndjson", logschema::record),
    ] {
        for (i, f) in load(name).iter().enumerate() {
            let b = f.bytes();
            if name == "service-lines.ndjson" {
                assert!(
                    !b.contains(&b'\n'),
                    "{name}:{}: a service line holds no newline",
                    i + 1
                );
            }
            assert_eq!(check(&b), f.reason, "{name}:{}", i + 1);
        }
    }
}

#[test]
fn schema_examples_are_fixtures() {
    let doc = std::fs::read_to_string(format!(
        "{}/../../docs/log-schema.md",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let valid: Vec<String> = ["service-lines.ndjson", "records.ndjson"]
        .iter()
        .flat_map(|n| load(n))
        .filter(|f| f.valid)
        .filter_map(|f| f.input)
        .collect();
    let examples: Vec<&str> = doc.lines().filter(|l| l.starts_with("{\"v\":1")).collect();
    assert!(!examples.is_empty(), "no example found in log-schema.md");
    for e in examples {
        assert!(
            valid.iter().any(|v| v == e),
            "log-schema.md example is not a valid fixture: {e}"
        );
    }
}

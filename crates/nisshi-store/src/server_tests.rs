//! The serve tests of the Go nisshi, ported: the response table of ADR 0003 over HTTP,
//! status and metrics, protobuf decoding as `proto.Unmarshal`, admission and deadlines.

use super::*;
use nisshi_core::pb::PushRequest;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

const STREAM: &str = "9b1e0c5d7a2f4e8b9c0d1e2f3a4b5c6d";

fn sha(s: &str) -> String {
    nisshi_core::store::hex(&Sha256::digest(s.as_bytes()))
}

fn tempdir() -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let p = std::env::temp_dir().join(format!(
        "nisshi-store-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn day0() -> i64 {
    nisshi_core::time::parse_ts_shape("2026-10-19T12:00:00.000000000Z").unwrap() as i64
}

fn new_app(deadlines: Deadlines) -> Arc<App> {
    let root = tempdir();
    let cfg = Config::parse(&format!(
        "listen = [\"x\"]\ndata_dir = \"{}\"\ndisk_reserve = 0\n[tls]\ncert_file = \"c\"\nkey_file = \"k\"\n[tokens]\n\"{}\" = \"node:node-a\"\n\"{}\" = \"node:node-b\"\n\"{}\" = \"reader\"\n",
        root.display(),
        sha("tok-a"),
        sha("tok-b"),
        sha("tok-r")
    ))
    .unwrap();
    let store = Store::open(&root, 0, Box::new(day0)).unwrap();
    let mut app = App::new(store, cfg);
    app.deadlines = deadlines;
    Arc::new(app)
}

async fn start(app: Arc<App>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            tokio::spawn(serve_connection(tcp, app.clone()));
        }
    });
    addr
}

/// One HTTP/1.1 exchange on a fresh connection: status, head, body.
async fn http(addr: SocketAddr, head: String, body: Vec<u8>) -> (u16, String, Vec<u8>) {
    tokio::task::spawn_blocking(move || {
        let mut c = std::net::TcpStream::connect(addr).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c.write_all(head.as_bytes()).unwrap();
        let _ = c.write_all(&body);
        let mut out = Vec::new();
        let _ = c.read_to_end(&mut out);
        let split = out
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("a response");
        let head = String::from_utf8_lossy(&out[..split]).into_owned();
        let code = head[9..12].parse().unwrap();
        (code, head, out[split + 4..].to_vec())
    })
    .await
    .unwrap()
}

fn record(node: &str, seq: u64, extra: &str) -> Bytes {
    Bytes::from(format!(
        r#"{{"v":1,"ts":"2026-10-19T08:00:{:02}.000000000Z","level":"info","event":"query","node":"{node}","service":"annai","stream":"{STREAM}","seq":{seq}{extra}}}"#,
        seq % 60
    ))
}

fn records(node: &str, first: u64, n: u64) -> Vec<Bytes> {
    (first..first + n).map(|s| record(node, s, "")).collect()
}

struct Push {
    token: &'static str,
    node: &'static str,
    encoding: &'static str,
    stream_id: Vec<u8>,
    first: u64,
    records: Vec<Bytes>,
    raw: Option<Vec<u8>>,
}

impl Default for Push {
    fn default() -> Push {
        Push {
            token: "tok-a",
            node: "node-a",
            encoding: "zstd",
            stream_id: unhex(STREAM),
            first: 0,
            records: Vec::new(),
            raw: None,
        }
    }
}

async fn push(addr: SocketAddr, p: Push) -> (u16, Option<PushResponse>, Option<PushRejected>) {
    let mut body = p.raw.clone().unwrap_or_else(|| {
        PushRequest {
            node: p.node.into(),
            stream_id: p.stream_id.clone().into(),
            first_seq: p.first,
            records: p.records.clone(),
            status: Some(AgentStatus {
                version: "t".into(),
                ..Default::default()
            }),
        }
        .encode_to_vec()
    });
    if p.encoding.eq_ignore_ascii_case("zstd") {
        body = zstd::encode_all(&body[..], 3).unwrap();
    }
    let mut head = format!(
        "POST /v1/push HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\n",
        body.len()
    );
    match p.encoding {
        "none" => {}
        "raw-zstd" => head.push_str("Content-Encoding: zstd\r\n"),
        e => head.push_str(&format!("Content-Encoding: {e}\r\n")),
    }
    if p.token != "none" {
        head.push_str(&format!("Authorization: Bearer {}\r\n", p.token));
    }
    head.push_str("\r\n");
    let (code, _, b) = http(addr, head, body).await;
    match code {
        200 => (200, Some(PushResponse::decode(&b[..]).unwrap()), None),
        400 | 403 => (code, None, Some(PushRejected::decode(&b[..]).unwrap())),
        _ => (code, None, None),
    }
}

async fn get(addr: SocketAddr, path: &str, auth: &str) -> (u16, String, Vec<u8>) {
    let auth = if auth.is_empty() {
        String::new()
    } else {
        format!("Authorization: {auth}\r\n")
    };
    http(
        addr,
        format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n{auth}\r\n"),
        Vec::new(),
    )
    .await
}

#[tokio::test]
async fn response_table() {
    log::capture();
    let app = new_app(DEADLINES);
    let addr = start(app.clone()).await;
    let gap0 = record("node-a", 1, "").to_vec();
    let reserved = String::from_utf8(gap0)
        .unwrap()
        .replace(STREAM, &"0".repeat(32));
    type Case = (&'static str, Push, u16, u64, Option<u32>, &'static str);
    let cases: Vec<Case> = vec![
        (
            "no token",
            Push {
                token: "none",
                first: 1,
                records: records("node-a", 1, 2),
                ..Default::default()
            },
            401,
            0,
            None,
            "",
        ),
        (
            "unknown token",
            Push {
                token: "nope",
                first: 1,
                records: records("node-a", 1, 2),
                ..Default::default()
            },
            401,
            0,
            None,
            "",
        ),
        (
            "reader token",
            Push {
                token: "tok-r",
                first: 1,
                records: records("node-a", 1, 2),
                ..Default::default()
            },
            403,
            0,
            None,
            "",
        ),
        (
            "reader token, bad body",
            Push {
                token: "tok-r",
                raw: Some(b"x".to_vec()),
                encoding: "none",
                ..Default::default()
            },
            403,
            0,
            None,
            "",
        ),
        (
            "other node's token",
            Push {
                token: "tok-b",
                first: 1,
                records: records("node-a", 1, 2),
                ..Default::default()
            },
            400,
            0,
            None,
            "node-mismatch",
        ),
        (
            "request node",
            Push {
                node: "node-b",
                first: 1,
                records: records("node-b", 1, 2),
                ..Default::default()
            },
            400,
            0,
            None,
            "node-mismatch",
        ),
        (
            "stream id",
            Push {
                stream_id: vec![1, 2],
                first: 1,
                records: records("node-a", 1, 2),
                ..Default::default()
            },
            400,
            0,
            None,
            "stream-id-format",
        ),
        (
            "first seq",
            Push {
                first: 0,
                records: records("node-a", 0, 2),
                ..Default::default()
            },
            400,
            0,
            None,
            "first-seq-zero",
        ),
        (
            "no encoding",
            Push {
                first: 1,
                records: records("node-a", 1, 2),
                encoding: "none",
                ..Default::default()
            },
            400,
            0,
            None,
            "content-encoding",
        ),
        (
            "identity encoding",
            Push {
                first: 1,
                records: records("node-a", 1, 2),
                encoding: "identity",
                ..Default::default()
            },
            400,
            0,
            None,
            "content-encoding",
        ),
        (
            "encoding before node",
            Push {
                node: "node-b",
                first: 0,
                records: records("node-b", 1, 2),
                encoding: "gzip",
                ..Default::default()
            },
            400,
            0,
            None,
            "content-encoding",
        ),
        (
            "not zstd",
            Push {
                raw: Some(b"not zstd".to_vec()),
                encoding: "raw-zstd",
                ..Default::default()
            },
            400,
            0,
            None,
            "body-zstd",
        ),
        (
            "garbage body",
            Push {
                raw: Some(b"not protobuf at all".to_vec()),
                ..Default::default()
            },
            400,
            0,
            None,
            "body-protobuf",
        ),
        (
            "invalid record",
            Push {
                first: 1,
                records: vec![record("node-a", 1, ""), Bytes::from_static(br#"{"v":1}"#)],
                ..Default::default()
            },
            400,
            0,
            Some(1),
            "missing-field:ts",
        ),
        (
            "recv_ts in record",
            Push {
                first: 1,
                records: vec![record("node-a", 1, r#","recv_ts":"x""#)],
                ..Default::default()
            },
            400,
            0,
            Some(0),
            "reserved-field:recv_ts",
        ),
        (
            "record node",
            Push {
                first: 1,
                records: vec![record("node-b", 1, "")],
                ..Default::default()
            },
            400,
            0,
            Some(0),
            "node-mismatch",
        ),
        (
            "record seq",
            Push {
                first: 1,
                records: vec![record("node-a", 1, ""), record("node-a", 3, "")],
                ..Default::default()
            },
            400,
            0,
            Some(1),
            "seq-mismatch",
        ),
        (
            "in order",
            Push {
                first: 1,
                records: records("node-a", 1, 10),
                ..Default::default()
            },
            200,
            10,
            None,
            "",
        ),
        (
            "exact duplicate",
            Push {
                first: 1,
                records: records("node-a", 1, 10),
                ..Default::default()
            },
            200,
            10,
            None,
            "",
        ),
        (
            "overlap",
            Push {
                first: 6,
                records: records("node-a", 6, 10),
                ..Default::default()
            },
            200,
            15,
            None,
            "",
        ),
        (
            "gap",
            Push {
                first: 21,
                records: records("node-a", 21, 5),
                ..Default::default()
            },
            200,
            25,
            None,
            "",
        ),
        (
            "upper-case encoding",
            Push {
                first: 26,
                records: records("node-a", 26, 1),
                encoding: "ZSTD",
                ..Default::default()
            },
            200,
            26,
            None,
            "",
        ),
        // ADR 0014: the stream is node-a's; request checks first, then the owner, then records.
        (
            "another node's stream",
            Push {
                token: "tok-b",
                node: "node-b",
                first: 27,
                records: records("node-b", 27, 1),
                ..Default::default()
            },
            403,
            0,
            None,
            "stream-owner",
        ),
        (
            "owner before records",
            Push {
                token: "tok-b",
                node: "node-b",
                first: 27,
                records: vec![Bytes::from_static(b"x")],
                ..Default::default()
            },
            403,
            0,
            None,
            "stream-owner",
        ),
        (
            "request before owner",
            Push {
                token: "tok-b",
                node: "node-b",
                first: 0,
                records: records("node-b", 27, 1),
                ..Default::default()
            },
            400,
            0,
            None,
            "first-seq-zero",
        ),
        (
            "reserved stream",
            Push {
                stream_id: vec![0; 16],
                first: 1,
                records: vec![Bytes::from(reserved)],
                ..Default::default()
            },
            403,
            0,
            None,
            "stream-owner",
        ),
    ];
    for (name, p, code, acked, index, reason) in cases {
        let (got, pr, rj) = push(addr, p).await;
        assert_eq!(got, code, "{name}");
        if let Some(pr) = pr {
            assert_eq!(pr.acked_seq, acked, "{name}");
        }
        if let Some(rj) = rj {
            assert_eq!(
                (rj.record_index, rj.reason.as_str()),
                (index, reason),
                "{name}"
            );
        }
    }
    // Stream mismatch: a record of another stream.
    let rec = String::from_utf8(record("node-a", 27, "").to_vec())
        .unwrap()
        .replace(STREAM, &"ab".repeat(16));
    let (code, _, rj) = push(
        addr,
        Push {
            first: 27,
            records: vec![Bytes::from(rec)],
            ..Default::default()
        },
    )
    .await;
    let rj = rj.unwrap();
    assert_eq!(
        (code, rj.reason.as_str(), rj.record_index),
        (400, "stream-mismatch", Some(0))
    );
    let (code, _, _) = push(
        addr,
        Push {
            raw: Some(vec![0; MAX_COMPRESSED + 1]),
            encoding: "none",
            ..Default::default()
        },
    )
    .await;
    assert_eq!(code, 413, "too large");
    // A frame whose decompressed size exceeds the limit.
    let (code, _, _) = push(
        addr,
        Push {
            raw: Some(vec![0; MAX_DECOMPRESSED + 1]),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(code, 413, "too large decompressed");

    // Status: reader only, protojson.
    assert_eq!(get(addr, "/v1/status", "Bearer tok-a").await.0, 403);
    assert_eq!(get(addr, "/v1/status", "").await.0, 401);
    let (code, head, _) = get(addr, "/v1/status", "Bearer nope").await;
    assert!(
        code == 401 && head.to_lowercase().contains("www-authenticate: bearer"),
        "{head}"
    );
    let (code, _, b) = get(addr, "/v1/status", "Bearer tok-r").await;
    assert_eq!(code, 200);
    let s: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(s["streams"].as_array().unwrap().len(), 1, "{s}");
    assert_eq!(s["streams"][0]["ackedSeq"], "26");
    assert_eq!(s["streams"][0]["node"], "node-a");
    assert_eq!(s["streams"][0]["streamId"], base64(&unhex(STREAM)));
    let last = nisshi_core::time::parse_ts_shape("2026-10-19T08:00:26.000000000Z").unwrap();
    assert_eq!(s["streams"][0]["lastRecordNs"], last.to_string());
    assert_eq!(s["agents"]["node-a"]["version"], "t");
    assert_eq!(s["agents"]["node-a"]["spoolBytes"], "0");
    assert_eq!(s["diskReserveBytes"], "0");
    assert_eq!(s["chainHeads"], serde_json::json!({}));
    assert!(b.ends_with(b"}\n"));

    // Routing as Go's mux.
    assert_eq!(get(addr, "/nope", "").await.0, 404);
    assert_eq!(get(addr, "/v1/push", "").await.0, 405);

    // Metrics: the names of ADR 0010, from the handler.
    let (code, head, m) = get(start_metrics(app.clone()).await, "/metrics", "").await;
    let m = String::from_utf8(m).unwrap();
    assert_eq!(code, 200);
    assert!(
        head.to_lowercase()
            .contains("content-type: text/plain; version=0.0.4; charset=utf-8"),
        "{head}"
    );
    assert!(m.ends_with('\n'), "exposition format");
    let build = format!(r#"nisshi_build_info{{version="{VERSION}"}} 1"#);
    for want in [
        "nisshi_disk_free_bytes ",
        &build,
        r#"nisshi_records_total{source_node="node-a"} 21"#,
        "nisshi_duplicate_records_total 15",
        "nisshi_seq_gaps_total 1",
        r#"nisshi_push_requests_total{code="200"} 5"#,
        r#"nisshi_push_requests_total{code="401"} 2"#,
        r#"nisshi_push_requests_total{code="403"} 5"#,
        r#"nisshi_seals_total{result="ok"} 0"#,
        r#"nisshi_streams{source_node="node-a"} 1"#,
    ] {
        assert!(m.contains(want), "metrics missing {want} in\n{m}");
    }

    // Own logs validate against the schema: every line this process emitted.
    log::info("test", &[]);
    log::error("test-error", &[("err", Val::Str("boom"))]);
    let lines = log::captured();
    assert!(
        lines
            .iter()
            .any(|l| l.contains(r#""event":"stream-owner""#)),
        "{lines:?}"
    );
    for (i, l) in lines.iter().enumerate() {
        let reason = nisshi_core::logschema::line(l.as_bytes());
        assert_eq!(reason, "", "log line {}: {l}", i + 1);
    }
}

async fn start_metrics(app: Arc<App>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(accept_loop(listener, move |tcp, _| {
        tokio::spawn(serve_metrics(tcp, app.clone()));
    }));
    addr
}

#[test]
fn fake_clock() {
    // SAFETY: tests touching the environment run in this one test only.
    unsafe { std::env::set_var("NISSHI_FAKE_NOW", "2026-10-20T00:00:00Z") };
    assert_eq!(
        clock()
            .unwrap()
            .map(nisshi_core::time::format_ts)
            .as_deref(),
        Some("2026-10-20T00:00:00.000000000Z")
    );
    unsafe { std::env::set_var("NISSHI_FAKE_NOW", "tomorrow") };
    assert!(clock().is_err());
    unsafe { std::env::remove_var("NISSHI_FAKE_NOW") };
}

#[test]
fn notify_socket() {
    let path = tempdir().with_extension("sock");
    let sock = std::os::unix::net::UnixDatagram::bind(&path).unwrap();
    unsafe { std::env::set_var("NOTIFY_SOCKET", &path) };
    notify("READY=1").unwrap();
    let mut buf = [0u8; 64];
    let n = sock.recv(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"READY=1");
    unsafe { std::env::set_var("NOTIFY_SOCKET", "") };
    notify("READY=1").unwrap();
    unsafe { std::env::remove_var("NOTIFY_SOCKET") };
    let _ = std::fs::remove_file(path);
}

/// A push the store's clock puts on a sealed day: 503, the agent keeps its spool.
#[tokio::test]
async fn sealed_day_503() {
    log::capture();
    let app = new_app(DEADLINES);
    let dir = app.store.lock().unwrap().root.join("node-a");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("2026-10-19.manifest.json"), "{}").unwrap();
    let addr = start(app).await;
    let (code, _, _) = push(
        addr,
        Push {
            first: 1,
            records: records("node-a", 1, 2),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(code, 503);
    let lines = log::captured();
    assert!(
        lines.iter().any(|l| l.contains(r#""level":"error""#)
            && l.contains("receive day already sealed: 2026-10-19")),
        "no error event: {lines:?}"
    );
}

fn tag(num: u32, wt: u8) -> Vec<u8> {
    let mut v = Vec::new();
    prost::encoding::encode_varint(u64::from(num) << 3 | u64::from(wt), &mut v);
    v
}

/// decode_push answers as protobuf decoding does, records aside: it keeps MAX_RECORDS+1.
#[test]
fn decode_push_as_protobuf() {
    let good = PushRequest {
        node: "node-a".into(),
        stream_id: vec![1, 2].into(),
        first_seq: 7,
        records: vec![Bytes::from_static(b"a"), Bytes::new()],
        status: Some(AgentStatus {
            version: "v".into(),
            paused: true,
            ..Default::default()
        }),
    }
    .encode_to_vec();
    let with = |extra: &[u8]| [&good[..], extra].concat();
    let every = AgentStatus {
        version: "é".into(),
        spool_bytes: 1,
        spool_limit_bytes: 2,
        paused: true,
        oldest_unacked_ns: -3,
    }
    .encode_to_vec();
    let over = [tag((1 << 29) - 1 + 1, 0), vec![1]].concat();
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("good", good.clone()),
        ("unknown field", with(&[0x30, 0x01])),
        ("wrong wire type", with(&[0x18 | 5, 1, 2, 3, 4])),
        ("status twice", with(&[0x2a, 2, 0x20, 0x00])),
        ("bad utf8 node", vec![0x0a, 1, 0xff]),
        ("truncated", good[..good.len() - 1].to_vec()),
        ("field number 0", vec![0x00, 0x01]),
        ("bad status", vec![0x2a, 2, 0x0a, 5]),
        ("empty", vec![]),
        ("field number over the maximum", with(&over)),
        (
            "status, field number over",
            [vec![0x2a, over.len() as u8], over.clone()].concat(),
        ),
        (
            "status, every field",
            [vec![0x2a, every.len() as u8], every.clone()].concat(),
        ),
        ("status, bad utf8 version", vec![0x2a, 3, 0x0a, 1, 0xff]),
        (
            "status, wrong wire types",
            vec![0x2a, 7, 0x0d, 1, 2, 3, 4, 0x20, 0x01],
        ),
        (
            "status, unknown field",
            vec![0x2a, 4, 0x78, 0x01, 0x20, 0x02],
        ),
    ];
    for (name, b) in cases {
        // Go's proto.Unmarshal reads a known field with another wire type as an unknown field;
        // prost refuses it. The Go behaviour is the contract: the oracle drops that field.
        let want = match name {
            "wrong wire type" => PushRequest::decode(&good[..]),
            "status, wrong wire types" => Ok(PushRequest {
                status: Some(AgentStatus {
                    paused: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            _ => PushRequest::decode(&b[..]),
        };
        let got = decode_push(Bytes::from(b));
        assert_eq!(got.is_ok(), want.is_ok(), "{name}: {got:?} {want:?}");
        if let (Ok(g), Ok(w)) = (got, want) {
            assert_eq!(
                (g.node, g.stream_id, g.first_seq, g.records, g.status),
                (w.node, w.stream_id, w.first_seq, w.records, w.status),
                "{name}"
            );
        }
    }
    // An absurd count of empty records: a bounded vector, and the indexed 400 of the first.
    let head = PushRequest {
        node: "node-a".into(),
        stream_id: vec![7; 16].into(),
        first_seq: 1,
        ..Default::default()
    }
    .encode_to_vec();
    let raw = [head, [0x22u8, 0].repeat(1 << 20)].concat();
    assert_eq!(
        decode_push(Bytes::from(raw)).unwrap().records.len(),
        MAX_RECORDS + 1
    );
    // The smallest valid record is longer than the bound assumes.
    let r = format!(
        r#"{{"v":1,"ts":"2026-10-19T08:00:00.000000000Z","level":"info","event":"a","node":"a","service":"a","stream":"{}","seq":1}}"#,
        "0".repeat(32)
    );
    assert!(
        nisshi_core::logschema::record(r.as_bytes()).is_empty() && r.len() + 2 >= MIN_RECORD_WIRE
    );
}

#[tokio::test]
async fn absurd_record_count_is_indexed_400() {
    let app = new_app(DEADLINES);
    let addr = start(app).await;
    let head = PushRequest {
        node: "node-a".into(),
        stream_id: vec![7; 16].into(),
        first_seq: 1,
        ..Default::default()
    }
    .encode_to_vec();
    let raw = [head, [0x22u8, 0].repeat(1 << 20)].concat();
    let (code, _, rj) = push(
        addr,
        Push {
            raw: Some(raw),
            ..Default::default()
        },
    )
    .await;
    let rj = rj.unwrap();
    assert_eq!(
        (code, rj.reason.as_str(), rj.record_index),
        (400, "framing", Some(0))
    );
}

/// A push waits while another holds the slot, gets 503 when the wait outlasts the admission
/// deadline, and goes through once the slot is free; an agent's status is kept bounded.
#[tokio::test]
async fn push_admission_and_status() {
    let app = new_app(Deadlines {
        admit: Duration::from_millis(300),
        ..DEADLINES
    });
    let addr = start(app.clone()).await;
    let slot = app.pushing.clone().acquire_owned().await.unwrap();
    let (code, _, _) = push(
        addr,
        Push {
            first: 1,
            records: records("node-a", 1, 1),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(code, 503, "slot held past the admission deadline");
    // A declared body that never arrives does not delay the 503 (validation N1).
    let t = std::time::Instant::now();
    let (code, head, _) = http(
        addr,
        "POST /v1/push HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-a\r\nContent-Encoding: zstd\r\nContent-Length: 100\r\n\r\n".into(),
        Vec::new(),
    )
    .await;
    assert!(
        code == 503 && t.elapsed() < Duration::from_secs(3),
        "{head}"
    );
    assert!(head.to_lowercase().contains("connection: close"), "{head}");
    drop(slot);

    let app = new_app(Deadlines {
        admit: Duration::from_secs(5),
        ..DEADLINES
    });
    let addr = start(app.clone()).await;
    let slot = app.pushing.clone().acquire_owned().await.unwrap();
    let waiting = tokio::spawn(push(
        addr,
        Push {
            first: 1,
            records: records("node-a", 1, 1),
            ..Default::default()
        },
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished(), "admitted while the slot is held");
    drop(slot);
    assert_eq!(waiting.await.unwrap().0, 200, "after the slot is freed");

    let mut body = PushRequest {
        node: "node-a".into(),
        stream_id: vec![1; 16].into(),
        first_seq: 1,
        status: Some(AgentStatus {
            version: "v".repeat(10_000),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    body.extend([0x2a, 2, 0x78, 0x01]); // status with an unknown field 15
    assert_eq!(
        push(
            addr,
            Push {
                raw: Some(body),
                ..Default::default()
            }
        )
        .await
        .0,
        200
    );
    assert_eq!(app.agents()["node-a"].version.len(), MAX_VERSION);
    // The cut falls inside a two-byte character: it is left out whole, status still serialises.
    let version = "v".repeat(MAX_VERSION - 1) + "é";
    let body = PushRequest {
        node: "node-a".into(),
        stream_id: vec![1; 16].into(),
        first_seq: 1,
        status: Some(AgentStatus {
            version: version.clone(),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        push(
            addr,
            Push {
                raw: Some(body),
                ..Default::default()
            }
        )
        .await
        .0,
        200
    );
    let (code, _, b) = get(addr, "/v1/status", "Bearer tok-r").await;
    let s: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        (code, s["agents"]["node-a"]["version"].as_str().unwrap()),
        (200, &version[..MAX_VERSION - 1])
    );
}

/// Deadlines reclaim a connection whose headers or body never come.
#[tokio::test]
async fn deadlines() {
    let short = Duration::from_millis(300);
    let app = new_app(Deadlines {
        header: short,
        read: short,
        write: Duration::from_secs(1),
        admit: DEADLINES.admit,
    });
    let addr = start(app).await;
    for (name, req) in [
        (
            "body never sent, token",
            "POST /v1/push HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-a\r\nContent-Encoding: zstd\r\nContent-Length: 100\r\n\r\n",
        ),
        (
            "body never sent, no token",
            "POST /v1/push HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n",
        ),
        (
            "chunked, stalled",
            "POST /v1/push HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-a\r\nContent-Encoding: zstd\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab",
        ),
        (
            "idle keep-alive",
            "GET /v1/status HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-r\r\n\r\n",
        ),
        ("headers never end", "POST /v1/push HTTP/1.1\r\nHost: x\r\n"),
    ] {
        let t = std::time::Instant::now();
        let req = req.to_string();
        tokio::task::spawn_blocking(move || {
            let mut c = std::net::TcpStream::connect(addr).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            c.write_all(req.as_bytes()).unwrap();
            let _ = c.read_to_end(&mut Vec::new());
        })
        .await
        .unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(3),
            "{name}: open for {:?}",
            t.elapsed()
        );
    }
}

#[test]
fn seal_schedule() {
    let ns = |s: &str| nisshi_core::time::parse_ts_shape(s).unwrap() as i64;
    let s = 1_000_000_000;
    assert_eq!(until_seal(ns("2026-10-19T00:00:00.000000000Z")), 600 * s);
    assert_eq!(until_seal(ns("2026-10-19T00:10:00.000000000Z")), 86_400 * s);
    assert_eq!(until_seal(ns("2026-10-19T23:59:50.000000000Z")), 610 * s);
}

#[test]
fn base64_encodes() {
    assert_eq!(base64(&[0xff]), "/w==");
    assert_eq!(base64(b"abc"), "YWJj");
    assert_eq!(base64(&[0xaa; 16]), "qqqqqqqqqqqqqqqqqqqqqg==");
}

/// A declared `Content-Length` over 8 MiB is 413 from the headers alone: before the push slot
/// (held here), before the body, which is never sent. A body without length, or longer than
/// declared, is cut at the limit while reading (`response_table`: 413 too).
#[tokio::test]
async fn content_length_over_limit_is_413_at_once() {
    let app = new_app(Deadlines {
        admit: Duration::from_secs(5),
        ..DEADLINES
    });
    let addr = start(app.clone()).await;
    let _slot = app.pushing.clone().acquire_owned().await.unwrap();
    let t = std::time::Instant::now();
    let (code, _, _) = http(
        addr,
        format!(
            "POST /v1/push HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-a\r\nContent-Encoding: zstd\r\nContent-Length: {}\r\n\r\n",
            MAX_COMPRESSED + 1
        ),
        Vec::new(),
    )
    .await;
    assert_eq!(code, 413);
    assert!(
        t.elapsed() < Duration::from_secs(2),
        "waited {:?}",
        t.elapsed()
    );
    // Exactly the limit is not refused from the header: it waits for the slot like any push.
    let waiting = tokio::spawn(http(
        addr,
        format!(
            "POST /v1/push HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-a\r\nContent-Encoding: zstd\r\nContent-Length: {MAX_COMPRESSED}\r\n\r\n"
        ),
        Vec::new(),
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiting.is_finished(),
        "a body at the limit was refused from its header"
    );
    waiting.abort();
}

/// HTTP/1.1 only (ADR 0003): ALPN offers no h2, and an HTTP/2 preface, whole or cut, does not
/// hold a connection past the header deadline.
#[tokio::test]
async fn http2_preface_is_refused() {
    let short = Duration::from_millis(300);
    let app = new_app(Deadlines {
        header: short,
        ..DEADLINES
    });
    let addr = start(app).await;
    for preface in ["PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", "PRI * HTTP/2.0\r\n"] {
        let t = std::time::Instant::now();
        let out = tokio::task::spawn_blocking(move || {
            let mut c = std::net::TcpStream::connect(addr).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            c.write_all(preface.as_bytes()).unwrap();
            let mut out = Vec::new();
            let _ = c.read_to_end(&mut out);
            String::from_utf8_lossy(&out).into_owned()
        })
        .await
        .unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(3),
            "{preface:?}: {:?}",
            t.elapsed()
        );
        assert!(!out.starts_with("HTTP/1.1 2"), "{out}");
    }
}

#[derive(Debug)]
struct NoCert;

impl rustls::server::ResolvesServerCert for NoCert {
    fn resolve(
        &self,
        _: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        None
    }
}

/// A TLS handshake error (a port probe that closes at once) is a schema line, the Go
/// server's `http-error` event (TestErrorLogIsSchema).
#[tokio::test]
async fn tls_error_log_is_schema() {
    log::capture();
    let cfg =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(NoCert));
    let tls = TlsAcceptor::from(Arc::new(cfg));
    let app = new_app(DEADLINES);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(accept_loop(listener, move |tcp, peer| {
        tokio::spawn(serve_tls(tls.clone(), tcp, peer, app.clone()));
    }));
    let probe = TcpStream::connect(addr).await.unwrap();
    let want = format!(
        "http: TLS handshake error from {}: ",
        probe.local_addr().unwrap()
    );
    drop(probe);
    let t = std::time::Instant::now();
    let line = loop {
        if let Some(l) = log::captured().into_iter().find(|l| l.contains(&want)) {
            break l;
        }
        assert!(t.elapsed() < Duration::from_secs(5), "no log line");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(nisshi_core::logschema::line(line.as_bytes()), "", "{line}");
    assert!(
        line.contains(r#""event":"http-error","err":"http: TLS handshake error"#),
        "{line}"
    );
}

/// The write deadline covers writing the response: a reader that asks for a large status and
/// stops reading loses its connection, the response is cut (validation P2).
#[tokio::test]
async fn write_deadline_bounds_the_response() {
    let app = new_app(Deadlines {
        write: Duration::from_secs(2),
        ..DEADLINES
    });
    {
        let mut s = app.store.lock().unwrap();
        for i in 0..100_000u64 {
            let st = nisshi_core::store::Stream {
                node: "node-a".into(),
                hwm: i,
                ..Default::default()
            };
            s.st.streams.insert(format!("{i:032x}"), st);
        }
    }
    let addr = start(app).await;
    // Read at once, the same response is whole.
    let (code, _, b) = get(addr, "/v1/status", "Bearer tok-r").await;
    assert_eq!(code, 200);
    let full = b.len();
    assert!(full > 8 << 20, "{full} bytes: not past the socket buffers");
    let got = tokio::task::spawn_blocking(move || {
        let mut c = std::net::TcpStream::connect(addr).unwrap();
        c.write_all(
            b"GET /v1/status HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-r\r\nConnection: close\r\n\r\n",
        )
        .unwrap();
        std::thread::sleep(Duration::from_secs(4));
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut out = Vec::new();
        let _ = c.read_to_end(&mut out);
        out.len()
    })
    .await
    .unwrap();
    assert!(got < full, "{got} bytes of {full}: response not cut");
}

/// A zstd frame declaring more than 64 MiB is 413 from its header, as Go's decoder answers,
/// a single-segment one too (its window is its size); a later frame declaring more than the
/// budget left as well (validation P2).
#[tokio::test]
async fn declared_oversize_zstd_is_413() {
    let frame = |fhd: u8, size: &[u8]| {
        let mut f = vec![0x28, 0xb5, 0x2f, 0xfd, fhd];
        f.extend_from_slice(size);
        f.extend([1, 0, 0]); // the last block: raw, empty
        f
    };
    let single = frame(0xe0, &((MAX_DECOMPRESSED + 1) as u64).to_le_bytes());
    assert_eq!(decompress(&single), Err(None));
    let mut two = zstd::bulk::compress(&vec![0; MAX_DECOMPRESSED - 10], 1).unwrap();
    let first = two.len();
    two.extend(frame(0x20, &[11]));
    assert_eq!(decompress(&two), Err(None), "past the budget left");
    two.truncate(first);
    two.extend(frame(0x20, &[10]));
    assert_eq!(
        decompress(&two),
        Err(Some(())),
        "within it, but no 10 bytes"
    );
    let ok = zstd::encode_all(&b"abc"[..], 3).unwrap();
    assert_eq!(
        decompress(&[ok.clone(), ok].concat()),
        Ok(b"abcabc".to_vec())
    );
    assert_eq!(decompress(b""), Ok(Vec::new()));

    let addr = start(new_app(DEADLINES)).await;
    let (code, _, _) = push(
        addr,
        Push {
            raw: Some(single),
            encoding: "raw-zstd",
            ..Default::default()
        },
    )
    .await;
    assert_eq!(code, 413);
}

/// Sequence numbers up to u64::MAX over the API: the hwm stays at the top (validation P1).
#[tokio::test]
async fn hwm_at_u64_max() {
    let addr = start(new_app(DEADLINES)).await;
    let top = |first: u64, n: u64| (0..n).map(|i| record("node-a", first + i, "")).collect();
    for (first, records) in [(u64::MAX - 1, top(u64::MAX - 1, 2)), (1, top(1, 2))] {
        let (code, ok, _) = push(
            addr,
            Push {
                first,
                records,
                ..Default::default()
            },
        )
        .await;
        assert_eq!((code, ok.unwrap().acked_seq), (200, u64::MAX), "{first}");
    }
}

#[test]
fn unhex_never_panics() {
    assert_eq!(unhex("aé"), vec![0]);
    assert_eq!(unhex("é0ab"), vec![0, 0x0a]);
    assert_eq!(unhex("ff00"), vec![0xff, 0]);
}

/// Accept errors: EMFILE and the like are retried, anything else ends the server.
#[tokio::test]
async fn accept_errors_and_bind() {
    for (e, retry) in [
        (std::io::Error::from_raw_os_error(24), true), // EMFILE
        (std::io::Error::from_raw_os_error(23), true), // ENFILE
        (std::io::ErrorKind::ConnectionAborted.into(), true),
        (std::io::Error::from_raw_os_error(9), false), // EBADF
        (std::io::ErrorKind::InvalidInput.into(), false),
    ] {
        assert_eq!(accept_retryable(&e), retry, "{e}");
    }
    let l = bind(":0").await.unwrap();
    assert_ne!(l.local_addr().unwrap().port(), 0);
}

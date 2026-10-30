//! nisshi-testpush drives a running nisshi-store through the delivery scenarios of
//! fixtures/delivery/ (docs/delivery-scenarios.md, ADR 0012) or pushes every record fixture of
//! fixtures/logs/records.ndjson (ADR 0011). Test-only: not shipped. It reads the store back with
//! zstdcat, so that the oracle is a standard tool, and checks that every stored record is the
//! pushed bytes plus a recv_ts of the file's day.
//!
//! ```text
//! nisshi-testpush --url https://127.0.0.1:9473 --ca <pem> --node node-a --other-node node-b \
//!   --token-valid <t> --token-other-node <t> --root /var/lib/nisshi [--zstdcat zstdcat] \
//!   [--restart <cmd>] [--fill <cmd>] [--free <cmd>] (--fixtures <records.ndjson> | <scenario.json>...)
//! ```

use hyper::body::Body as _;
use hyper::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HOST};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use nisshi_core::json;
use nisshi_core::pb::{PushRejected, PushRequest, PushResponse};
use prost::Message;
use prost::bytes::Bytes;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::pin::Pin;
use std::process::{Command, ExitCode};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

type Res<T> = Result<T, String>;

struct Opts {
    url: String,
    ca: String,
    node: String,
    other_node: String,
    tok_valid: String,
    tok_other: String,
    root: String,
    zstdcat: String,
    restart: String,
    fill: String,
    free: String,
    fixtures: String,
    files: Vec<String>,
}

fn parse_args() -> Res<Opts> {
    let mut o = Opts {
        url: "https://127.0.0.1:9473".into(),
        ca: String::new(),
        node: "node-a".into(),
        other_node: "node-b".into(),
        tok_valid: String::new(),
        tok_other: String::new(),
        root: "/var/lib/nisshi".into(),
        zstdcat: "zstdcat".into(),
        restart: String::new(),
        fill: String::new(),
        free: String::new(),
        fixtures: String::new(),
        files: Vec::new(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        let Some(body) = a.strip_prefix("--").or_else(|| a.strip_prefix('-')) else {
            break;
        };
        if a == "--" {
            i += 1;
            break;
        }
        let (name, inline) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (body, None),
        };
        let value = match inline {
            Some(v) => v,
            None => {
                i += 1;
                args.get(i)
                    .cloned()
                    .ok_or(format!("flag needs an argument: -{name}"))?
            }
        };
        let slot = match name {
            "url" => &mut o.url,
            "ca" => &mut o.ca,
            "node" => &mut o.node,
            "other-node" => &mut o.other_node,
            "token-valid" => &mut o.tok_valid,
            "token-other-node" => &mut o.tok_other,
            "root" => &mut o.root,
            "zstdcat" => &mut o.zstdcat,
            "restart" => &mut o.restart,
            "fill" => &mut o.fill,
            "free" => &mut o.free,
            "fixtures" => &mut o.fixtures,
            _ => return Err(format!("flag provided but not defined: -{name}")),
        };
        *slot = value;
        i += 1;
    }
    o.files = args[i..].to_vec();
    Ok(o)
}

/// A one-chunk request body.
struct Full(Option<Bytes>);

impl hyper::body::Body for Full {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        Poll::Ready(
            self.get_mut()
                .0
                .take()
                .map(|b| Ok(hyper::body::Frame::data(b))),
        )
    }
}

struct Client {
    host: String,
    port: u16,
    tls: TlsConnector,
}

impl Client {
    fn new(url: &str, ca: &str) -> Res<Client> {
        let rest = url
            .strip_prefix("https://")
            .ok_or("--url must be https://host:port")?;
        let rest = rest.trim_end_matches('/');
        let (host, port) = rest
            .rsplit_once(':')
            .ok_or("--url must be https://host:port")?;
        let mut roots = RootCertStore::empty();
        if !ca.is_empty() {
            let f = std::fs::File::open(ca).map_err(|e| format!("{ca}: {e}"))?;
            for cert in rustls_pemfile::certs(&mut std::io::BufReader::new(f)) {
                roots
                    .add(cert.map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            }
        }
        let mut cfg =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| e.to_string())?
                .with_root_certificates(roots)
                .with_no_client_auth();
        // HTTP/1.1, the only protocol the store serves (ADR 0003).
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Client {
            host: host.trim_start_matches('[').trim_end_matches(']').into(),
            port: port.parse().map_err(|_| "--url: bad port")?,
            tls: TlsConnector::from(Arc::new(cfg)),
        })
    }

    async fn send(&self, req: Request<Full>) -> Res<(u16, Vec<u8>)> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| e.to_string())?;
        let name = ServerName::try_from(self.host.clone()).map_err(|e| e.to_string())?;
        let tls = self
            .tls
            .connect(name, tcp)
            .await
            .map_err(|e| e.to_string())?;
        let (mut s, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(conn);
        let resp: Response<hyper::body::Incoming> =
            s.send_request(req).await.map_err(|e| e.to_string())?;
        let code = resp.status().as_u16();
        let mut body = resp.into_body();
        let mut out = Vec::new();
        while let Some(f) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
            if let Ok(d) = f.map_err(|e| e.to_string())?.into_data() {
                out.extend_from_slice(&d);
            }
        }
        Ok((code, out))
    }

    async fn request(&self, req: Request<Full>) -> Res<(u16, Vec<u8>)> {
        tokio::time::timeout(Duration::from_secs(30), self.send(req))
            .await
            .map_err(|_| "request timeout".to_string())?
    }
}

#[derive(Deserialize)]
struct PushStep {
    first_seq: u64,
    count: u64,
    #[serde(default)]
    token: String,
    #[serde(default)]
    node: String,
    invalid_at: Option<u64>,
    #[serde(default)]
    stream: String,
}

#[derive(Deserialize)]
struct CheckStep {
    stored_seqs: Option<String>,
    duplicates: Option<usize>,
    gaps: Option<usize>,
}

#[derive(Deserialize)]
struct Step {
    push: Option<PushStep>,
    #[serde(default)]
    expect: BTreeMap<String, serde_json::Value>,
    check: Option<CheckStep>,
    restart: Option<serde_json::Value>,
    disk: Option<BTreeMap<String, bool>>,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    steps: Vec<Step>,
}

struct Stored {
    /// Without recv_ts.
    line: Vec<u8>,
    stream: String,
    seq: u64,
    /// gap_stream of a seq-gap record.
    gap: String,
}

struct Pusher {
    o: Opts,
    client: Client,
    streams: HashMap<&'static str, String>,
    /// "<stream>/<seq>" → pushed bytes
    pushed: HashMap<String, Vec<u8>>,
}

fn random_stream() -> String {
    use std::io::Read;
    let mut b = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .expect("/dev/urandom");
    nisshi_core::store::hex(&b)
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap_or(0))
        .collect()
}

/// A valid annai query record; ts = base + seq seconds, stream b one hour earlier, so that
/// `nisshi read` output order differs from file order. `invalid` makes ts 6-digit.
fn record(node: &str, stream: &str, seq: u64, hour_offset: i64, invalid: bool) -> Vec<u8> {
    let base = nisshi_core::time::parse_ts_shape("2026-10-19T08:00:00.000000000Z").unwrap() as i64;
    let mut ts = nisshi_core::time::format_ts(
        base + hour_offset * 3_600_000_000_000 + (seq as i64).wrapping_mul(1_000_000_000),
    );
    if invalid {
        ts = format!("{}Z", &ts[..26]);
    }
    format!(
        r#"{{"v":1,"ts":"{ts}","level":"info","event":"query","node":"{node}","service":"annai","stream":"{stream}","seq":{seq},"proto":"udp","client":"192.0.2.1","qname":"example.org.","qtype":"A","rcode":"NOERROR","dur_us":38}}"#
    )
    .into_bytes()
}

fn sh(cmd: &str) -> Res<()> {
    if cmd.is_empty() {
        return Err("no command configured for this step".into());
    }
    let st = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .status()
        .map_err(|e| e.to_string())?;
    if !st.success() {
        return Err(format!("{cmd}: {st}"));
    }
    Ok(())
}

/// A member of a record as Go's `json.Unmarshal` into a map reads it.
fn members(line: &[u8]) -> Option<Vec<(String, &[u8])>> {
    json::object_map(line).ok().flatten()
}

fn get<'a>(m: &[(String, &'a [u8])], k: &str) -> Option<&'a [u8]> {
    m.iter().find(|(n, _)| n == k).map(|(_, v)| *v)
}

/// The string member `k`, `""` when absent or not a string.
fn str_of(m: &[(String, &[u8])], k: &str) -> String {
    get(m, k).and_then(json::unquote).unwrap_or_default()
}

fn seq_of(m: &[(String, &[u8])]) -> Option<u64> {
    std::str::from_utf8(get(m, "seq")?).ok()?.parse().ok()
}

/// Sorted seqs as "1-10,21-30".
fn ranges(mut seqs: Vec<u64>) -> String {
    seqs.sort_unstable();
    let mut parts = Vec::new();
    let mut i = 0;
    while i < seqs.len() {
        let mut j = i;
        while j + 1 < seqs.len() && seqs[j + 1] == seqs[j] + 1 {
            j += 1;
        }
        parts.push(if i == j {
            seqs[i].to_string()
        } else {
            format!("{}-{}", seqs[i], seqs[j])
        });
        i = j + 1;
    }
    parts.join(",")
}

impl Pusher {
    async fn push(
        &self,
        token: &str,
        req: &PushRequest,
    ) -> Res<(u16, Option<PushResponse>, Option<PushRejected>)> {
        let body = zstd::encode_all(&req.encode_to_vec()[..], 3).map_err(|e| e.to_string())?;
        let mut r = Request::builder()
            .method("POST")
            .uri(format!("{}/v1/push", self.o.url.trim_end_matches('/')))
            .header(HOST, &self.client.host)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "zstd");
        if !token.is_empty() {
            r = r.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let (code, b) = self
            .client
            .request(r.body(Full(Some(Bytes::from(body)))).unwrap())
            .await?;
        Ok(match code {
            200 => (
                200,
                Some(PushResponse::decode(&b[..]).map_err(|e| format!("200 body: {e}"))?),
                None,
            ),
            // a 403 for the wrong role has no body, a 403 stream-owner has one (ADR 0014)
            400 | 403 => (
                code,
                None,
                Some(PushRejected::decode(&b[..]).map_err(|e| format!("{code} body: {e}"))?),
            ),
            _ => (code, None, None),
        })
    }

    async fn wait_up(&self) -> Res<()> {
        for _ in 0..300 {
            let r = Request::builder()
                .uri(format!("{}/v1/status", self.o.url.trim_end_matches('/')))
                .header(HOST, &self.client.host)
                .body(Full(None))
                .unwrap();
            if self.client.request(r).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err("nisshi not back after restart".into())
    }

    async fn run(&mut self, sc: &Scenario) -> Res<()> {
        self.streams = HashMap::from([("a", random_stream()), ("b", random_stream())]);
        self.pushed.clear();
        for (i, st) in sc.steps.iter().enumerate() {
            self.step(st)
                .await
                .map_err(|e| format!("step {}: {e}", i + 1))?;
        }
        Ok(())
    }

    async fn step(&mut self, st: &Step) -> Res<()> {
        if let Some(ps) = &st.push {
            let name = if ps.stream == "b" { "b" } else { "a" };
            let stream = self.streams[name].clone();
            let req_node = if ps.node == "other" {
                self.o.other_node.clone()
            } else {
                self.o.node.clone()
            };
            let token = match ps.token.as_str() {
                "none" => String::new(),
                "unknown" => format!("unknown-token-{}", random_stream()),
                "other-node" => self.o.tok_other.clone(),
                _ => self.o.tok_valid.clone(),
            };
            let hour = if name == "b" { -1 } else { 0 };
            let mut recs = Vec::new();
            for i in 0..ps.count {
                let seq = ps.first_seq + i;
                let rec = record(&req_node, &stream, seq, hour, ps.invalid_at == Some(i));
                if ps.invalid_at.is_none() && req_node == self.o.node && ps.token != "other-node" {
                    self.pushed.insert(format!("{stream}/{seq}"), rec.clone());
                }
                recs.push(Bytes::from(rec));
            }
            let req = PushRequest {
                node: req_node,
                stream_id: unhex(&stream).into(),
                first_seq: ps.first_seq,
                records: recs,
                status: None,
            };
            let (code, resp, rej) = self.push(&token, &req).await?;
            return expect(&st.expect, code, resp.as_ref(), rej.as_ref());
        }
        if let Some(c) = &st.check {
            let rows = self.read_store()?;
            let ours = [self.streams["a"].clone(), self.streams["b"].clone()];
            let mut seen: HashMap<String, usize> = HashMap::new();
            let (mut seqs_a, mut gaps, mut dups) = (Vec::new(), 0, 0);
            for r in rows {
                if !r.gap.is_empty() {
                    gaps += usize::from(ours.contains(&r.gap));
                    continue;
                }
                if !ours.contains(&r.stream) {
                    continue;
                }
                let key = format!("{}/{}", r.stream, r.seq);
                let n = seen.entry(key.clone()).or_insert(0);
                *n += 1;
                if *n > 1 {
                    dups += 1;
                }
                let want = self.pushed.get(&key).cloned().unwrap_or_default();
                if want != r.line {
                    return Err(format!(
                        "stored record {key} differs from the pushed bytes:\n{}\n{}",
                        String::from_utf8_lossy(&r.line),
                        String::from_utf8_lossy(&want)
                    ));
                }
                if r.stream == self.streams["a"] {
                    seqs_a.push(r.seq);
                }
            }
            if let Some(want) = &c.stored_seqs {
                let got = ranges(seqs_a);
                if &got != want {
                    return Err(format!("stored_seqs {got:?}, want {want:?}"));
                }
            }
            if let Some(want) = c.duplicates
                && dups != want
            {
                return Err(format!("duplicates {dups}, want {want}"));
            }
            if let Some(want) = c.gaps
                && gaps != want
            {
                return Err(format!("gaps {gaps}, want {want}"));
            }
            return Ok(());
        }
        if st.restart.is_some() {
            sh(&self.o.restart)?;
            return self.wait_up().await;
        }
        if let Some(d) = &st.disk {
            return sh(if d.get("fill") == Some(&true) {
                &self.o.fill
            } else {
                &self.o.free
            });
        }
        Err("unknown step".into())
    }

    /// Decodes every data file of the node with zstdcat and checks the recv_ts member of each
    /// line against the file's day.
    fn read_store(&self) -> Res<Vec<Stored>> {
        let dir = PathBuf::from(&self.o.root).join(&self.o.node);
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .map(|rd| rd.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        files.retain(|p| p.to_string_lossy().ends_with(".ndjson.zst"));
        files.sort();
        let mut out = Vec::new();
        for f in files {
            let name = f.file_name().unwrap().to_string_lossy().into_owned();
            let day = name.trim_end_matches(".ndjson.zst").to_string();
            let o = Command::new(&self.o.zstdcat)
                .arg(&f)
                .output()
                .map_err(|e| format!("{} {}: {e}", self.o.zstdcat, f.display()))?;
            if !o.status.success() {
                return Err(format!("{} {}: {}", self.o.zstdcat, f.display(), o.status));
            }
            let data = o.stdout;
            if data.is_empty() {
                continue;
            }
            let data = data.strip_suffix(b"\n").unwrap_or(&data);
            for line in data.split(|&c| c == b'\n') {
                // ,"recv_ts":"<ts>"} at the end, 43 bytes plus the brace.
                let bad = || {
                    format!(
                        "{}: no recv_ts member: {}",
                        f.display(),
                        String::from_utf8_lossy(line)
                    )
                };
                if line.len() < 44 {
                    return Err(bad());
                }
                let tail = &line[line.len() - 44..];
                if !tail.starts_with(br#","recv_ts":""#) || !tail.ends_with(br#""}"#) {
                    return Err(bad());
                }
                let recv = std::str::from_utf8(&tail[12..42]).map_err(|_| bad())?;
                let shape_ok = nisshi_core::logschema::record(
                    format!(r#"{{"v":1,"ts":"{recv}","level":"info","event":"x","node":"a","service":"a","stream":"{}","seq":1}}"#, "0".repeat(32)).as_bytes(),
                )
                .is_empty();
                if !shape_ok || !recv.starts_with(&day) {
                    return Err(format!(
                        "{}: recv_ts {recv} not a ts of day {day}",
                        f.display()
                    ));
                }
                let m = members(line).ok_or_else(|| {
                    format!(
                        "{}: not a JSON object: {}",
                        f.display(),
                        String::from_utf8_lossy(line)
                    )
                })?;
                let mut s = Stored {
                    line: [&line[..line.len() - 44], b"}"].concat(),
                    stream: str_of(&m, "stream"),
                    seq: seq_of(&m).unwrap_or(0),
                    gap: String::new(),
                };
                if str_of(&m, "event") == "seq-gap" && str_of(&m, "service") == "nisshi" {
                    s.gap = str_of(&m, "gap_stream");
                }
                out.push(s);
            }
        }
        Ok(out)
    }

    /// Pushes every record fixture as a one-record batch whose stream_id and first_seq come from
    /// the record itself (defaults when unreadable; the node is always the pusher's): invalid
    /// ones get 400 with index 0 and the fixture's reason, valid ones 200. A model of the hwm
    /// rules predicts the stored set, compared byte for byte with the store.
    async fn run_fixtures(&self, path: &str) -> Res<()> {
        #[derive(Deserialize)]
        struct Repeat {
            prefix: String,
            repeat: String,
            count: usize,
            suffix: String,
        }
        #[derive(Deserialize)]
        struct Fx {
            input: Option<String>,
            input_b64: Option<String>,
            input_repeat: Option<Repeat>,
            valid: bool,
            reason: String,
        }
        let data = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        let mut hwm: HashMap<String, u64> = HashMap::new();
        let mut expected: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut gaps = 0;
        let default_stream = random_stream();
        for (i, line) in data.lines().enumerate() {
            let at = format!("{path}:{}", i + 1);
            let fx: Fx = serde_json::from_str(line).map_err(|e| format!("{at}: {e}"))?;
            let rec = match (fx.input, fx.input_b64, fx.input_repeat) {
                (Some(s), _, _) => s.into_bytes(),
                (_, Some(b), _) => b64(&b),
                (_, _, Some(r)) => {
                    format!("{}{}{}", r.prefix, r.repeat.repeat(r.count), r.suffix).into_bytes()
                }
                _ => return Err(format!("{at}: no input")),
            };
            let (mut stream, mut seq) = (default_stream.clone(), 1u64);
            if let Some(m) = members(&rec) {
                let st = str_of(&m, "stream");
                if st.len() == 32 && st.bytes().all(|c| c.is_ascii_hexdigit()) {
                    stream = st;
                }
                if let Some(n) = seq_of(&m).filter(|&n| n > 0) {
                    seq = n;
                }
            }
            let req = PushRequest {
                node: self.o.node.clone(),
                stream_id: unhex(&stream).into(),
                first_seq: seq,
                records: vec![Bytes::from(rec.clone())],
                status: None,
            };
            let (code, resp, rej) = self
                .push(&self.o.tok_valid, &req)
                .await
                .map_err(|e| format!("{at}: {e}"))?;
            let key = stream.clone();
            let h = hwm.get(&key).copied().unwrap_or(0);
            if fx.valid {
                let acked = resp.as_ref().map(|r| r.acked_seq);
                if code != 200 || acked != Some(h.max(seq)) {
                    return Err(format!(
                        "{at}: valid record got {code} {resp:?} {rej:?}, want 200 acked {}",
                        h.max(seq)
                    ));
                }
                if seq > h {
                    if seq > h + 1 {
                        gaps += 1;
                    }
                    hwm.insert(key, seq);
                    *expected.entry(rec).or_insert(0) += 1;
                }
            } else if code != 400
                || rej.as_ref().and_then(|r| r.record_index) != Some(0)
                || rej.as_ref().map(|r| r.reason.as_str()) != Some(fx.reason.as_str())
            {
                return Err(format!(
                    "{at}: invalid record got {code} {rej:?}, want 400 index 0 reason {}",
                    fx.reason
                ));
            }
        }
        let rows = self.read_store()?;
        let mut got: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut got_gaps = 0;
        for r in &rows {
            if !r.gap.is_empty() {
                got_gaps += 1;
                continue;
            }
            *got.entry(r.line.clone()).or_insert(0) += 1;
        }
        if got_gaps != gaps {
            return Err(format!("{got_gaps} gap records, want {gaps}"));
        }
        if got.len() != expected.len() {
            return Err(format!(
                "{} distinct stored records, want {}",
                got.len(),
                expected.len()
            ));
        }
        for (k, n) in &expected {
            if got.get(k) != Some(n) {
                let s = String::from_utf8_lossy(k);
                return Err(format!(
                    "record stored {} times, want {n}: {:.80}",
                    got.get(k).unwrap_or(&0),
                    s
                ));
            }
        }
        println!(
            "fixtures {path}: {} records stored, {got_gaps} gap records",
            rows.len() - got_gaps
        );
        Ok(())
    }
}

fn b64(s: &str) -> Vec<u8> {
    let val = |c: u8| -> u32 {
        match c {
            b'A'..=b'Z' => u32::from(c - b'A'),
            b'a'..=b'z' => u32::from(c - b'a') + 26,
            b'0'..=b'9' => u32::from(c - b'0') + 52,
            b'+' => 62,
            b'/' => 63,
            _ => 0,
        }
    };
    let mut out = Vec::new();
    for chunk in s.as_bytes().chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        let n = chunk
            .iter()
            .fold(0u32, |n, &c| n << 6 | if c == b'=' { 0 } else { val(c) });
        out.extend_from_slice(&n.to_be_bytes()[1..4 - pad]);
    }
    out
}

/// Checks the response: absent fields are not checked, `null` means absent.
fn expect(
    ex: &BTreeMap<String, serde_json::Value>,
    code: u16,
    resp: Option<&PushResponse>,
    rej: Option<&PushRejected>,
) -> Res<()> {
    for (k, want) in ex {
        let got: Option<String> = match k.as_str() {
            "status" => Some(code.to_string()),
            "acked_seq" => resp.map(|r| r.acked_seq.to_string()),
            "record_index" => rej.and_then(|r| r.record_index).map(|i| i.to_string()),
            "reason" => rej.map(|r| r.reason.clone()),
            _ => return Err(format!("unknown expect field {k}")),
        };
        if want.is_null() {
            if let Some(g) = got {
                return Err(format!("{k}: present ({g}), want absent"));
            }
            continue;
        }
        let want_s = match want {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        if got.as_deref() != Some(want_s.as_str()) {
            return Err(format!(
                "{k}: got {got:?}, want {want} (status {code}, response {resp:?} {rej:?})"
            ));
        }
    }
    Ok(())
}

async fn main_async() -> Res<()> {
    let o = parse_args()?;
    let client = Client::new(&o.url, &o.ca)?;
    let mut p = Pusher {
        o,
        client,
        streams: HashMap::new(),
        pushed: HashMap::new(),
    };
    if !p.o.fixtures.is_empty() {
        let f = p.o.fixtures.clone();
        return p.run_fixtures(&f).await;
    }
    for f in p.o.files.clone() {
        let b = std::fs::read(&f).map_err(|e| format!("{f}: {e}"))?;
        let sc: Scenario = serde_json::from_slice(&b).map_err(|e| format!("{f}: {e}"))?;
        p.run(&sc)
            .await
            .map_err(|e| format!("scenario {}: {e}", sc.name))?;
        println!("scenario {}: ok", sc.name);
    }
    Ok(())
}

fn main() -> ExitCode {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    match rt.block_on(main_async()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("nisshi-testpush: {e}");
            ExitCode::FAILURE
        }
    }
}

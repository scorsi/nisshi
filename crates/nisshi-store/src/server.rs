//! The HTTPS ingestion API (ADR 0003, 0013, 0014), token authentication (ADR 0008),
//! `GET /v1/status` and the loopback `/metrics` listener (ADR 0010), the seal schedule
//! (ADR 0006), over hyper and rustls (ADR 0015). HTTP/1.1 only (ADR 0003): hyper bounds the
//! time of HTTP/1 headers, not of an HTTP/2 preface or HEADERS frame.

use crate::config::{Config, Identity, identity};
use crate::metrics::Metrics;
use hyper::body::{Body, Bytes, Frame, Incoming, SizeHint};
use hyper::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use nisshi_core::log::{self, Val};
use nisshi_core::pb::{AgentStatus, PushRejected, PushResponse};
use nisshi_core::store::{Batch, Kind, Store};
use prost::Message;
use prost::bytes::Buf;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const MAX_COMPRESSED: usize = 8 << 20;
const MAX_DECOMPRESSED: usize = 64 << 20;
/// Below the wire size of the smallest valid record (149 bytes of JSON plus its 2-byte field
/// header), so a body of `MAX_DECOMPRESSED` bytes holds at most `MAX_RECORDS` valid records:
/// with more, one of the first `MAX_RECORDS + 1` is invalid.
const MIN_RECORD_WIRE: usize = 128;
const MAX_RECORDS: usize = MAX_DECOMPRESSED / MIN_RECORD_WIRE;
/// `AgentStatus.version` kept for /v1/status.
const MAX_VERSION: usize = 256;
/// Push bodies: a window up to the decompressed limit (64 MiB).
const WINDOW_LOG_MAX: u32 = 26;

/// HTTP deadlines (ADR 0003 amendment), calibrated on hikyaku's 30 s request timeout: a client
/// has given up long before them, they only reclaim connections a client left hanging.
#[derive(Clone, Copy)]
pub struct Deadlines {
    /// Headers (and the TLS handshake).
    pub header: Duration,
    /// From the request's start to the end of its body.
    pub read: Duration,
    /// From the end of the headers to the end of the response.
    pub write: Duration,
    /// The wait for the push slot (a long seal or a stuck fsync holds it).
    pub admit: Duration,
}

pub const DEADLINES: Deadlines = Deadlines {
    header: Duration::from_secs(10),
    read: Duration::from_secs(60),
    write: Duration::from_secs(120),
    admit: Duration::from_secs(25),
};

/// A one-chunk response body.
pub struct Full(Option<Bytes>);

impl From<Vec<u8>> for Full {
    fn from(v: Vec<u8>) -> Full {
        Full((!v.is_empty()).then(|| Bytes::from(v)))
    }
}

impl Body for Full {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.get_mut().0.take().map(|b| Ok(Frame::data(b))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.as_ref().map_or(0, |b| b.len() as u64))
    }
}

/// Collects a request body, `Err(())` past `limit` bytes or on a transport error.
pub async fn read_body(mut body: Incoming, limit: usize) -> Result<Vec<u8>, ()> {
    let mut out = Vec::new();
    while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        if let Ok(data) = frame.map_err(|_| ())?.into_data() {
            if out.len() + data.len() > limit {
                return Err(());
            }
            out.extend_from_slice(&data);
        }
    }
    Ok(out)
}

pub struct App {
    pub store: Arc<Mutex<Store>>,
    pub metrics: Metrics,
    pub cfg: Config,
    root: PathBuf,
    /// One push at a time from body read to commit: the memory of a 64 MiB batch is budgeted
    /// once (MemoryMax=128M); the others wait, unread, in their connection.
    pub pushing: Arc<Semaphore>,
    /// Last status of each node's agent, kept from accepted pushes only.
    agents: Mutex<BTreeMap<String, AgentStatus>>,
    pub deadlines: Deadlines,
}

impl App {
    pub fn new(store: Store, cfg: Config) -> App {
        App {
            root: store.root.clone(),
            store: Arc::new(Mutex::new(store)),
            metrics: Metrics::new(),
            cfg,
            pushing: Arc::new(Semaphore::new(1)),
            agents: Mutex::new(BTreeMap::new()),
            deadlines: DEADLINES,
        }
    }

    /// The identity of `Authorization: Bearer <token>`: the hex of its SHA-256 compared in
    /// constant time with every configured hash.
    fn identity<B>(&self, req: &Request<B>) -> Option<Identity> {
        let token = req
            .headers()
            .get(AUTHORIZATION)?
            .as_bytes()
            .strip_prefix(b"Bearer ")?;
        let h = nisshi_core::store::hex(&Sha256::digest(token));
        let mut found = None;
        for (k, v) in &self.cfg.tokens {
            if k.len() == h.len()
                && k.bytes().zip(h.bytes()).fold(0u8, |d, (a, b)| d | (a ^ b)) == 0
            {
                found = identity(v);
            }
        }
        found
    }

    fn agents(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, AgentStatus>> {
        self.agents.lock().unwrap()
    }
}

fn respond(
    status: StatusCode,
    content_type: Option<&'static str>,
    body: Vec<u8>,
) -> Response<Full> {
    let mut b = Response::builder().status(status);
    if let Some(ct) = content_type {
        b = b.header(CONTENT_TYPE, ct);
    }
    b.body(Full::from(body)).unwrap()
}

fn empty(status: StatusCode) -> Response<Full> {
    respond(status, None, Vec::new())
}

/// `http.Error`: a text body with a newline.
fn text(status: StatusCode, msg: &str) -> Response<Full> {
    let mut r = respond(
        status,
        Some("text/plain; charset=utf-8"),
        format!("{msg}\n").into_bytes(),
    );
    r.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    r
}

fn not_allowed(allow: &'static str) -> Response<Full> {
    let mut r = text(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed");
    r.headers_mut()
        .insert("allow", HeaderValue::from_static(allow));
    r
}

fn refuse(status: StatusCode, index: Option<u32>, reason: &str) -> Response<Full> {
    let body = PushRejected {
        record_index: index,
        reason: reason.into(),
    };
    respond(status, Some("application/x-protobuf"), body.encode_to_vec())
}

fn reject(index: Option<u32>, reason: &str) -> Response<Full> {
    refuse(StatusCode::BAD_REQUEST, index, reason)
}

/// 401 without a known token, 403 for a valid token of another role (ADR 0013).
fn deny(id: Option<Identity>) -> Response<Full> {
    match id {
        Some(_) => empty(StatusCode::FORBIDDEN),
        None => {
            let mut r = empty(StatusCode::UNAUTHORIZED);
            r.headers_mut()
                .insert("www-authenticate", HeaderValue::from_static("Bearer"));
            r
        }
    }
}

/// A push to a stream the node does not own (ADR 0014).
fn forbid_stream(node: &str, stream: &str) -> Response<Full> {
    log::warn(
        "stream-owner",
        &[
            ("source_node", Val::Str(node)),
            ("stream_id", Val::Str(stream)),
        ],
    );
    refuse(StatusCode::FORBIDDEN, None, "stream-owner")
}

pub async fn handle(app: Arc<App>, req: Request<Incoming>) -> Result<Response<Full>, Infallible> {
    let start = Instant::now();
    Ok(match req.uri().path() {
        "/v1/push" if req.method() == Method::POST => {
            let r = push(&app, req, start).await;
            app.metrics.add("pushes", r.status().as_str(), 1);
            r
        }
        "/v1/push" => not_allowed("POST"),
        "/v1/status" if req.method() == Method::GET || req.method() == Method::HEAD => {
            status(&app, &req).await
        }
        "/v1/status" => not_allowed("GET, HEAD"),
        _ => text(StatusCode::NOT_FOUND, "404 page not found"),
    })
}

fn content_length<B>(req: &Request<B>) -> Option<u64> {
    req.headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse().ok())
}

/// The response table of ADR 0003.
async fn push(app: &Arc<App>, req: Request<Incoming>, start: Instant) -> Response<Full> {
    let node = match app.identity(&req) {
        Some(Identity::Node(n)) => n,
        id => return deny(id),
    };
    if content_length(&req).is_some_and(|n| n > MAX_COMPRESSED as u64) {
        return empty(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let zstd = req
        .headers()
        .get(CONTENT_ENCODING)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"zstd"));
    if !zstd {
        return reject(None, "content-encoding");
    }
    // Nothing is read before the slot: a push refused here holds no body.
    let Ok(permit) =
        tokio::time::timeout(app.deadlines.admit, app.pushing.clone().acquire_owned()).await
    else {
        // The body is unread: close rather than wait for it (validation N1).
        let mut r = empty(StatusCode::SERVICE_UNAVAILABLE);
        r.headers_mut()
            .insert("connection", HeaderValue::from_static("close"));
        return r;
    };
    let permit = permit.expect("semaphore never closed");
    // Any read error is 413, a deadline too: the agent splits the batch, which also gets a slow
    // link through.
    let body = match tokio::time::timeout_at(
        start + app.deadlines.read,
        read_body(req.into_body(), MAX_COMPRESSED),
    )
    .await
    {
        Ok(Ok(b)) => b,
        _ => return empty(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let app2 = app.clone();
    // The slot goes to the blocking task: it is held until the commit ends, even if this
    // connection goes away.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        commit(&app2, &node, body)
    })
    .await
    .unwrap_or_else(|e| text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))
}

/// The body decoded, checked and committed.
fn commit(app: &App, node: &str, body: Vec<u8>) -> Response<Full> {
    let raw = match decompress(&body) {
        Ok(raw) => raw,
        Err(None) => return empty(StatusCode::PAYLOAD_TOO_LARGE),
        Err(Some(())) => return reject(None, "body-zstd"),
    };
    drop(body);
    let Ok(req) = decode_push(Bytes::from(raw)) else {
        return reject(None, "body-protobuf");
    };
    if req.node != node {
        return reject(None, "node-mismatch");
    }
    if req.stream_id.len() != 16 {
        return reject(None, "stream-id-format");
    }
    if req.first_seq == 0 {
        return reject(None, "first-seq-zero");
    }
    let stream = nisshi_core::store::hex(&req.stream_id);
    if app
        .store
        .lock()
        .unwrap()
        .check_owner(node, &stream)
        .is_err()
    {
        return forbid_stream(node, &stream);
    }
    let mut last_ts = String::new();
    for (i, rec) in req.records.iter().enumerate() {
        let reason = match nisshi_core::logschema::record_fields(rec) {
            Err(r) => r,
            Ok(f) if f.node != node => "node-mismatch".into(),
            Ok(f) if f.stream != stream => "stream-mismatch".into(),
            Ok(f) if Some(f.seq) != req.first_seq.checked_add(i as u64) => "seq-mismatch".into(),
            Ok(f) => {
                if f.ts > last_ts {
                    last_ts = f.ts;
                }
                continue;
            }
        };
        return reject(Some(i as u32), &reason);
    }
    let batch = Batch {
        node,
        stream: &stream,
        first_seq: req.first_seq,
        records: &req.records,
        last_ts,
    };
    let res = app.store.lock().unwrap().push(&batch);
    let res = match res {
        Ok(r) => r,
        Err(e) if e.kind == Kind::StreamOwner => return forbid_stream(node, &stream),
        Err(e) => {
            log::error(
                "push-failed",
                &[("source_node", Val::Str(node)), ("err", Val::Str(&e.msg))],
            );
            return empty(match e.kind {
                Kind::Disk => StatusCode::INSUFFICIENT_STORAGE,
                Kind::SealedDay => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            });
        }
    };
    app.metrics.add("records", node, res.stored as u64);
    app.metrics.add("duplicates", "", res.duplicates as u64);
    if res.gap {
        app.metrics.add("gaps", "", 1);
    }
    if let Some(st) = req.status {
        app.agents().insert(node.into(), st);
    }
    respond(
        StatusCode::OK,
        Some("application/x-protobuf"),
        PushResponse {
            acked_seq: res.acked,
        }
        .encode_to_vec(),
    )
}

/// The transport frames decoded: `Err(None)` past 64 MiB (413), `Err(Some(()))` not zstd. As
/// Go's decoder, frame by frame: one whose header declares more than the budget left (a
/// single-segment frame's window too) is 413 before it is decoded; no frame at all is empty.
fn decompress(body: &[u8]) -> Result<Vec<u8>, Option<()>> {
    use zstd::zstd_safe::{find_frame_compressed_size, get_frame_content_size};
    let mut raw = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let left = MAX_DECOMPRESSED - raw.len();
        if let Ok(Some(n)) = get_frame_content_size(rest)
            && n > left as u64
        {
            return Err(None);
        }
        // A frame whose end is not found is decoded with what follows: it fails there, or
        // first runs past the limit (413), as Go's.
        let (frame, next) = rest.split_at(find_frame_compressed_size(rest).unwrap_or(rest.len()));
        let decoded = zstd::stream::read::Decoder::with_buffer(frame).and_then(|d| {
            let mut d = d.single_frame();
            d.window_log_max(WINDOW_LOG_MAX)?;
            d.take(left as u64 + 1).read_to_end(&mut raw)
        });
        if raw.len() > MAX_DECOMPRESSED {
            return Err(None);
        }
        decoded.map_err(|_| Some(()))?;
        rest = next;
    }
    Ok(raw)
}

/// A decoded `PushRequest`, records aliasing the body.
#[derive(Debug, Default, PartialEq)]
pub struct PushReq {
    pub node: String,
    pub stream_id: Bytes,
    pub first_seq: u64,
    pub records: Vec<Bytes>,
    pub status: Option<AgentStatus>,
}

/// A `PushRequest` decoded as protobuf decoding does, unknown fields dropped, keeping at most
/// `MAX_RECORDS + 1` records: an absurd count of tiny records cannot allocate past the memory
/// budget, and the first invalid record is among those kept.
pub fn decode_push(mut b: Bytes) -> Result<PushReq, ()> {
    use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
    let mut req = PushReq::default();
    while b.has_remaining() {
        let (num, wt) = decode_key(&mut b).map_err(|_| ())?;
        match (num, wt) {
            (1 | 2 | 4 | 5, WireType::LengthDelimited) => {
                let len = decode_varint(&mut b).map_err(|_| ())?;
                if len > b.remaining() as u64 {
                    return Err(());
                }
                let v = b.split_to(len as usize);
                match num {
                    1 => req.node = String::from_utf8(v.to_vec()).map_err(|_| ())?,
                    2 => req.stream_id = v,
                    4 => {
                        if req.records.len() <= MAX_RECORDS {
                            req.records.push(v);
                        }
                    }
                    _ => decode_status(&v, req.status.get_or_insert_with(AgentStatus::default))?,
                }
            }
            (3, WireType::Varint) => req.first_seq = decode_varint(&mut b).map_err(|_| ())?,
            _ => skip_field(wt, num, &mut b, DecodeContext::default()).map_err(|_| ())?,
        }
    }
    Ok(req)
}

/// Merges an `AgentStatus` into `st`, unknown fields dropped. Only the first `MAX_VERSION`
/// bytes of version are kept, cut on a character boundary.
fn decode_status(mut b: &[u8], st: &mut AgentStatus) -> Result<(), ()> {
    use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
    while b.has_remaining() {
        let (num, wt) = decode_key(&mut b).map_err(|_| ())?;
        match (num, wt) {
            (1, WireType::LengthDelimited) => {
                let len = decode_varint(&mut b).map_err(|_| ())?;
                if len > b.len() as u64 {
                    return Err(());
                }
                let v = &b[..len as usize];
                b = &b[len as usize..];
                std::str::from_utf8(v).map_err(|_| ())?;
                let mut k = v.len().min(MAX_VERSION);
                while k < v.len() && v[k] & 0xc0 == 0x80 {
                    k -= 1;
                }
                st.version = String::from_utf8(v[..k].to_vec()).unwrap();
            }
            (2..=5, WireType::Varint) => {
                let x = decode_varint(&mut b).map_err(|_| ())?;
                match num {
                    2 => st.spool_bytes = x,
                    3 => st.spool_limit_bytes = x,
                    4 => st.paused = x != 0,
                    _ => st.oldest_unacked_ns = x as i64,
                }
            }
            _ => skip_field(wt, num, &mut b, DecodeContext::default()).map_err(|_| ())?,
        }
    }
    Ok(())
}

/// `StatusResponse` in the protojson mapping with every field emitted: lowerCamelCase, 64-bit
/// integers as strings, bytes as base64.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusJson {
    streams: Vec<StreamJson>,
    agents: BTreeMap<String, AgentJson>,
    disk_free_bytes: String,
    disk_reserve_bytes: String,
    chain_heads: BTreeMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamJson {
    node: String,
    stream_id: String,
    acked_seq: String,
    last_receive_ns: String,
    last_record_ns: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentJson {
    version: String,
    spool_bytes: String,
    spool_limit_bytes: String,
    paused: bool,
    oldest_unacked_ns: String,
}

async fn status(app: &Arc<App>, req: &Request<Incoming>) -> Response<Full> {
    match app.identity(req) {
        Some(Identity::Reader) => {}
        id => return deny(id),
    }
    // Off the async workers: the store lock may wait for a commit's fsync or a seal.
    let app = app.clone();
    match tokio::task::spawn_blocking(move || status_body(&app)).await {
        Ok(Ok(body)) => respond(StatusCode::OK, Some("application/json"), body),
        Ok(Err(e)) => text(StatusCode::INTERNAL_SERVER_ERROR, &e),
        Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn status_body(app: &App) -> Result<Vec<u8>, String> {
    let streams = app.store.lock().unwrap().streams();
    let agents = app.agents().clone();
    let free = nisshi_core::disk::free_bytes(&app.root).unwrap_or(0);
    let heads = nisshi_core::store::chain_heads(&app.root)?;
    let body = StatusJson {
        streams: streams
            .iter()
            .map(|(hex, st)| StreamJson {
                node: st.node.clone(),
                stream_id: base64(&unhex(hex)),
                acked_seq: st.hwm.to_string(),
                last_receive_ns: st.last_receive_ns.to_string(),
                last_record_ns: nisshi_core::time::parse_ts_shape(&st.last_record_ts)
                    .filter(|&ns| nisshi_core::time::in_range(ns))
                    .unwrap_or(0)
                    .to_string(),
            })
            .collect(),
        agents: agents
            .into_iter()
            .map(|(node, a)| {
                (
                    node,
                    AgentJson {
                        version: a.version,
                        spool_bytes: a.spool_bytes.to_string(),
                        spool_limit_bytes: a.spool_limit_bytes.to_string(),
                        paused: a.paused,
                        oldest_unacked_ns: a.oldest_unacked_ns.to_string(),
                    },
                )
            })
            .collect(),
        disk_free_bytes: free.to_string(),
        disk_reserve_bytes: app.cfg.disk_reserve.to_string(),
        chain_heads: heads,
    };
    let mut out = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
    out.push(b'\n');
    Ok(out)
}

/// On bytes: a key of `state.json` is not always ASCII, slicing the `str` could panic.
fn unhex(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|p| {
            std::str::from_utf8(p)
                .ok()
                .and_then(|p| u8::from_str_radix(p, 16).ok())
                .unwrap_or(0)
        })
        .collect()
}

/// Standard base64 with padding, as protojson encodes `bytes`.
pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | u32::from(b) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

async fn metrics_handle(
    app: Arc<App>,
    req: Request<Incoming>,
) -> Result<Response<Full>, Infallible> {
    Ok(match req.uri().path() {
        "/metrics" if req.method() == Method::GET || req.method() == Method::HEAD => {
            let app2 = app.clone();
            let text = tokio::task::spawn_blocking(move || {
                let mut per_node: BTreeMap<String, u64> = BTreeMap::new();
                for st in app2.store.lock().unwrap().streams().values() {
                    *per_node.entry(st.node.clone()).or_insert(0) += 1;
                }
                let free = nisshi_core::disk::free_bytes(&app2.root).unwrap_or(0);
                app2.metrics
                    .render(&per_node, free, app2.cfg.disk_reserve, VERSION)
            })
            .await
            .unwrap_or_default();
            respond(
                StatusCode::OK,
                Some("text/plain; version=0.0.4; charset=utf-8"),
                text.into_bytes(),
            )
        }
        "/metrics" => not_allowed("GET, HEAD"),
        _ => text(StatusCode::NOT_FOUND, "404 page not found"),
    })
}

pub fn tls_config(cfg: &Config) -> Result<Arc<ServerConfig>, String> {
    let open = |p: &str| {
        std::fs::File::open(p)
            .map(std::io::BufReader::new)
            .map_err(|e| nisshi_core::store::ioerr("open", std::path::Path::new(p), &e))
    };
    let certs = rustls_pemfile::certs(&mut open(&cfg.tls.cert_file)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("tls: failed to read the certificate: {e}"))?;
    if certs.is_empty() {
        return Err("tls: failed to find any PEM data in certificate input".into());
    }
    let key = rustls_pemfile::private_key(&mut open(&cfg.tls.key_file)?)
        .map_err(|e| format!("tls: failed to read the key: {e}"))?
        .ok_or("tls: failed to find any PEM data in key input")?;
    let mut tls =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("tls: {e}"))?;
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(tls))
}

/// HTTP/1.1 with its header deadline, which also closes an idle connection and one that sends
/// an HTTP/2 preface; 64 KiB of headers.
fn builder(deadlines: Deadlines) -> hyper::server::conn::http1::Builder {
    let mut b = hyper::server::conn::http1::Builder::new();
    b.timer(TokioTimer::new())
        .header_read_timeout(deadlines.header)
        .max_buf_size(64 << 10 | 4096);
    b
}

/// One connection, HTTP/1.1. The write deadline runs from the end of a request's headers to
/// the end of its response, writing included: past it the connection is dropped, as Go's socket
/// deadline does. A commit under way completes in its blocking task all the same.
pub async fn serve_connection<I>(io: I, app: Arc<App>)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let write = app.deadlines.write;
    let deadline = Arc::new(Mutex::new(None::<Instant>));
    let set = deadline.clone();
    let conn = builder(app.deadlines).serve_connection(
        TokioIo::new(io),
        service_fn(move |req| {
            *set.lock().unwrap() = Some(Instant::now() + write);
            handle(app.clone(), req)
        }),
    );
    tokio::pin!(conn);
    loop {
        // Between requests the header deadline closes an idle connection.
        let until = deadline
            .lock()
            .unwrap()
            .unwrap_or_else(|| Instant::now() + write);
        tokio::select! {
            _ = &mut conn => return,
            _ = tokio::time::sleep_until(until) => {
                if deadline.lock().unwrap().is_some_and(|d| d <= Instant::now()) {
                    return;
                }
            }
        }
    }
}

/// One ingestion connection: the TLS handshake under the header deadline, then HTTP. A failed
/// handshake is a schema line, as the Go server's `ErrorLog` wrote it.
async fn serve_tls(tls: TlsAcceptor, tcp: TcpStream, peer: SocketAddr, app: Arc<App>) {
    let err = match tokio::time::timeout(app.deadlines.header, tls.accept(tcp)).await {
        Ok(Ok(stream)) => return serve_connection(stream, app).await,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => "EOF".to_string(),
        Ok(Err(e)) => e.to_string(),
        Err(_) => "i/o timeout".to_string(),
    };
    log::warn(
        "http-error",
        &[(
            "err",
            Val::Str(&format!("http: TLS handshake error from {peer}: {err}")),
        )],
    );
}

/// One `/metrics` connection, plain HTTP on loopback.
async fn serve_metrics(tcp: TcpStream, app: Arc<App>) {
    let _ = builder(app.deadlines)
        .serve_connection(
            TokioIo::new(tcp),
            service_fn(move |req| metrics_handle(app.clone(), req)),
        )
        .await;
}

/// Go's `Temporary` accept errors, which net/http retries: ENFILE and EMFILE (the same numbers
/// on Linux and the BSDs), an interrupted call, a connection reset or aborted before accept.
fn accept_retryable(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::{
        ConnectionAborted, ConnectionReset, Interrupted, TimedOut, WouldBlock,
    };
    matches!(
        e.kind(),
        Interrupted | WouldBlock | ConnectionReset | ConnectionAborted | TimedOut
    ) || matches!(e.raw_os_error(), Some(23 | 24))
}

/// Accepts connections until a permanent error, which it returns. A transient one is logged
/// and retried after a backoff, 5 ms doubling up to 1 s, as net/http does.
async fn accept_loop(listener: TcpListener, conn: impl Fn(TcpStream, SocketAddr)) -> String {
    let mut delay = Duration::ZERO;
    loop {
        match listener.accept().await {
            Ok((tcp, peer)) => {
                delay = Duration::ZERO;
                conn(tcp, peer);
            }
            Err(e) if accept_retryable(&e) => {
                delay = (delay * 2).clamp(Duration::from_millis(5), Duration::from_secs(1));
                let msg = format!(
                    "http: Accept error: {}; retrying in {delay:?}",
                    nisshi_core::store::errno_text(&e)
                );
                log::warn("http-error", &[("err", Val::Str(&msg))]);
                tokio::time::sleep(delay).await;
            }
            Err(e) => {
                let addr = listener
                    .local_addr()
                    .map_or(String::new(), |a| a.to_string());
                return format!("accept tcp {addr}: {}", nisshi_core::store::errno_text(&e));
            }
        }
    }
}

/// `sd_notify` by hand over `NOTIFY_SOCKET` (`Type=notify`); a no-op outside such a unit.
pub fn notify(state: &str) -> std::io::Result<()> {
    let Ok(path) = std::env::var("NOTIFY_SOCKET") else {
        return Ok(());
    };
    if path.is_empty() {
        return Ok(());
    }
    let sock = std::os::unix::net::UnixDatagram::unbound()?;
    match path.strip_prefix('@') {
        #[cfg(target_os = "linux")]
        Some(name) => {
            use std::os::linux::net::SocketAddrExt;
            let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())?;
            sock.send_to_addr(state.as_bytes(), &addr)?;
        }
        #[cfg(not(target_os = "linux"))]
        Some(_) => return Err(std::io::Error::other("abstract socket")),
        None => {
            sock.send_to(state.as_bytes(), &path)?;
        }
    }
    Ok(())
}

/// The store's clock: `NISSHI_FAKE_NOW` (RFC 3339, tests only) freezes it.
pub fn clock() -> Result<Option<i64>, String> {
    match std::env::var("NISSHI_FAKE_NOW") {
        Ok(v) if !v.is_empty() => {
            let t = nisshi_core::time::parse_rfc3339(&v)
                .map_err(|e| format!("NISSHI_FAKE_NOW: {e}"))?;
            if !nisshi_core::time::in_range(t) {
                return Err("NISSHI_FAKE_NOW: outside the ts range".into());
            }
            Ok(Some(t as i64))
        }
        _ => Ok(None),
    }
}

/// Nanoseconds until the next 00:10 UTC.
fn until_seal(now_ns: i64) -> u64 {
    const NS: i64 = 1_000_000_000;
    let day = 86_400 * NS;
    let target = 600 * NS;
    let tod = now_ns.rem_euclid(day);
    (if tod < target {
        target - tod
    } else {
        day - tod + target
    }) as u64
}

/// Seals past days and applies retention (ADR 0006), logging each result.
pub fn seal_and_retain(app: &App) {
    let mut s = app.store.lock().unwrap();
    let (sealed, errs) = s.seal_past();
    for d in sealed {
        app.metrics.add("seals", "ok", 1);
        log::info("seal", &[("day", Val::Str(&d))]);
    }
    for e in errs {
        app.metrics.add("seals", "error", 1);
        log::error("seal-failed", &[("err", Val::Str(&e))]);
    }
    let (deleted, err) = s.retain(app.cfg.retention_days);
    for d in deleted {
        log::info("retention", &[("day", Val::Str(&d))]);
    }
    if let Some(e) = err {
        log::error("retention-failed", &[("err", Val::Str(&e))]);
    }
}

/// Binds a listener; `:port` (no host) listens on every address, as Go does: IPv6 and IPv4
/// on `[::]`, IPv4 alone on a host without IPv6.
async fn bind(addr: &str) -> Result<TcpListener, String> {
    let bound = match addr.strip_prefix(':') {
        Some(port) => match TcpListener::bind(format!("[::]:{port}")).await {
            Err(e) => TcpListener::bind(format!("0.0.0.0:{port}"))
                .await
                .map_err(|_| e),
            ok => ok,
        },
        None => TcpListener::bind(addr).await,
    };
    bound.map_err(|e| format!("listen tcp {addr}: {}", nisshi_core::store::errno_text(&e)))
}

/// Serves until SIGTERM or SIGINT; notifies systemd once the listeners are bound.
pub fn run(cfg: Config) -> Result<(), String> {
    let fake = clock()?;
    let now: nisshi_core::store::Clock = match fake {
        Some(t) => Box::new(move || t),
        None => Box::new(nisshi_core::time::real_now_ns),
    };
    let store = Store::open(std::path::Path::new(&cfg.data_dir), cfg.disk_reserve, now)?;
    if let Some(n) = store.rebuilt() {
        log::info("restore", &[("streams", Val::Int(n))]);
    }
    let app = Arc::new(App::new(store, cfg.clone()));
    seal_and_retain(&app);
    let tls = TlsAcceptor::from(tls_config(&cfg)?);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let result = rt.block_on(async {
        let mut listeners = Vec::new();
        for addr in &cfg.listen {
            listeners.push(bind(addr).await?);
        }
        // A permanent accept error ends the process, as Go's `ServeTLS` returning does.
        let mut accepting = tokio::task::JoinSet::new();
        if !cfg.listen_metrics.is_empty() {
            let listener = bind(&cfg.listen_metrics).await?;
            let app = app.clone();
            accepting.spawn(accept_loop(listener, move |tcp, _| {
                tokio::spawn(serve_metrics(tcp, app.clone()));
            }));
        }
        for listener in listeners {
            let (tls, app) = (tls.clone(), app.clone());
            accepting.spawn(accept_loop(listener, move |tcp, peer| {
                tokio::spawn(serve_tls(tls.clone(), tcp, peer, app.clone()));
            }));
        }
        log::info(
            "start",
            &[
                ("version", Val::Str(VERSION)),
                ("listen", Val::Strs(&cfg.listen)),
                ("data_dir", Val::Str(&cfg.data_dir)),
            ],
        );
        if let Err(e) = notify("READY=1") {
            log::warn(
                "notify-failed",
                &[("err", Val::Str(&nisshi_core::store::errno_text(&e)))],
            );
        }
        let sched = app.clone();
        tokio::spawn(async move {
            loop {
                // Off the async workers: the store lock may wait for a commit's fsync or a seal.
                let s = sched.clone();
                let now = tokio::task::spawn_blocking(move || (s.store.lock().unwrap().now)())
                    .await
                    .unwrap_or_else(|_| nisshi_core::time::real_now_ns());
                tokio::time::sleep(Duration::from_nanos(until_seal(now))).await;
                let app = sched.clone();
                let _ = tokio::task::spawn_blocking(move || seal_and_retain(&app)).await;
            }
        });
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| e.to_string())?;
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
            Some(Ok(e)) = accepting.join_next() => return Err(e),
        }
        Ok::<(), String>(())
    });
    let _ = notify("STOPPING=1");
    rt.shutdown_background();
    // An error (binding, or a permanent accept error) is the `exit` event, not a stop.
    result?;
    log::info("stop", &[]);
    Ok(())
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;

//! The store's tests (ADR 0011), ported from the Go nisshi case by case: the response table,
//! commit and recovery with a crash injected at every step, seal crash and idempotence, the
//! manifest chain, retention keeping manifests, restore, verify.

use crate::read::{Query, parse_time, read};
use crate::seal::{manifests, verify};
use crate::store::*;
use prost::bytes::Bytes;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

type Damage = Box<dyn Fn(&Path)>;

const STREAM_A: &str = "9b1e0c5d7a2f4e8b9c0d1e2f3a4b5c6d";
const HOUR: i64 = 3_600_000_000_000;
const DAY: i64 = 24 * HOUR;

fn day0() -> i64 {
    crate::time::parse_ts_shape("2026-10-19T12:00:00.000000000Z").unwrap() as i64
}

pub fn tempdir(name: &str) -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let p = std::env::temp_dir().join(format!(
        "nisshi-test-{}-{name}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn record(seq: u64) -> Bytes {
    Bytes::from(format!(
        r#"{{"v":1,"ts":"2026-10-19T08:00:{:02}.000000000Z","level":"info","event":"query","node":"node-a","service":"annai","stream":"{STREAM_A}","seq":{seq},"qname":"example.org."}}"#,
        seq % 60
    ))
}

struct OwnedBatch {
    node: String,
    stream: String,
    first: u64,
    records: Vec<Bytes>,
}

impl OwnedBatch {
    fn b(&self) -> Batch<'_> {
        Batch {
            node: &self.node,
            stream: &self.stream,
            first_seq: self.first,
            records: &self.records,
            last_ts: "2026-10-19T08:00:59.000000000Z".into(),
        }
    }

    /// The same batch for another node or stream, records rewritten to match.
    fn moved(mut self, node: &str, stream: &str) -> OwnedBatch {
        for r in &mut self.records {
            let s = String::from_utf8(r.to_vec()).unwrap();
            *r = Bytes::from(s.replace(STREAM_A, stream).replace("node-a", node));
        }
        self.node = node.into();
        self.stream = stream.into();
        self
    }
}

fn batch(first: u64, n: u64) -> OwnedBatch {
    OwnedBatch {
        node: "node-a".into(),
        stream: STREAM_A.into(),
        first,
        records: (0..n).map(|i| record(first + i)).collect(),
    }
}

#[derive(Clone)]
struct FakeClock(Arc<AtomicI64>);

impl FakeClock {
    fn new() -> FakeClock {
        FakeClock(Arc::new(AtomicI64::new(day0())))
    }
    fn set(&self, ns: i64) {
        self.0.store(ns, Ordering::Relaxed);
    }
    fn clock(&self) -> Clock {
        let c = self.0.clone();
        Box::new(move || c.load(Ordering::Relaxed))
    }
}

fn open(root: &Path, c: &FakeClock) -> Store {
    Store::open(root, 0, c.clock()).unwrap()
}

/// Every stored line of a node in file order, with its file's day.
fn lines(root: &Path, node: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for f in data_files(root).unwrap() {
        if f.node != node {
            continue;
        }
        read_lines(&f.path, false, |l| {
            out.push((String::from_utf8(l.to_vec()).unwrap(), f.day.clone()));
            Ok(())
        })
        .unwrap();
    }
    out
}

fn seqs(root: &Path, node: &str) -> Vec<u64> {
    lines(root, node)
        .iter()
        .map(|(l, _)| parse_envelope(l.as_bytes()).unwrap())
        .filter(|e| e.stream == STREAM_A)
        .map(|e| e.seq().unwrap())
        .collect()
}

fn verify_ok(root: &Path) -> Result<(), String> {
    let mut out = Vec::new();
    verify(root, &mut out).map_err(|e| format!("{e}: {}", String::from_utf8_lossy(&out)))
}

#[test]
fn push_table() {
    let root = tempdir("table");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    for (i, (b, acked, stored, dups, gap)) in [
        (batch(1, 10), 10, 10, 0, false),
        (batch(1, 10), 10, 0, 10, false), // exact duplicate
        (batch(6, 10), 15, 5, 5, false),  // overlap
        (batch(26, 5), 30, 5, 0, true),   // gap 16-25
        (batch(31, 0), 30, 0, 0, false),  // empty batch
    ]
    .into_iter()
    .enumerate()
    {
        let r = s.push(&b.b()).unwrap();
        let want = PushResult {
            acked,
            stored,
            duplicates: dups,
            gap,
        };
        assert_eq!(r, want, "push {i}");
    }
    assert_eq!(
        seqs(&root, "node-a"),
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 26, 27, 28, 29, 30
        ]
    );
    let mut gaps = 0;
    for (i, (l, day)) in lines(&root, "node-a").iter().enumerate() {
        let m = crate::json::object_map(l.as_bytes()).unwrap().unwrap();
        let get = |k: &str| {
            m.iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| String::from_utf8(v.to_vec()).unwrap())
                .unwrap_or_default()
        };
        let recv = get("recv_ts");
        let recv = recv.trim_matches('"');
        // The stored record is the pushed one plus recv_ts, whose day is the file's day.
        let suffix = format!(r#","recv_ts":"{recv}"}}"#);
        assert!(
            l.ends_with(&suffix) && recv.starts_with(day.as_str()) && suffix.len() - 1 == 43,
            "line {i}: {l}"
        );
        let pushed = format!("{}}}", l.strip_suffix(&suffix).unwrap());
        if get("event") == "\"seq-gap\"" {
            gaps += 1;
            assert_eq!(get("service"), "\"nisshi\"");
            assert_eq!(get("stream"), format!("\"{GAP_STREAM}\""));
            assert_eq!(get("gap_stream"), format!("\"{STREAM_A}\""));
            assert_eq!((get("from_seq"), get("to_seq")), ("16".into(), "25".into()));
            assert_eq!(get("node"), "\"node-a\"");
            assert_eq!(
                crate::logschema::record(pushed.as_bytes()),
                "",
                "gap record invalid"
            );
            continue;
        }
        let seq: u64 = get("seq").parse().unwrap();
        assert_eq!(
            record(seq),
            pushed.as_bytes(),
            "line {i}: not the pushed bytes"
        );
    }
    assert_eq!(gaps, 1);
    let st = &s.streams()[STREAM_A];
    assert_eq!(
        (
            st.hwm,
            st.node.as_str(),
            st.last_record_ts.as_str(),
            st.last_receive_ns
        ),
        (30, "node-a", "2026-10-19T08:00:59.000000000Z", day0())
    );
}

#[test]
fn disk_reserve() {
    let root = tempdir("reserve");
    let c = FakeClock::new();
    let mut s = Store::open(&root, 1 << 62, c.clock()).unwrap();
    let e = s.push(&batch(1, 1).b()).unwrap_err();
    assert_eq!(e.kind, Kind::Disk, "{e}");
    assert!(seqs(&root, "node-a").is_empty());
}

/// A crash or a failure after any commit step leaves the store either without the batch or
/// with all of it, and a resend after restart stores exactly once. "rename": the state is
/// published but its directory sync failed; "truncate": the rollback failed too. Both stop
/// the writer until a restart.
#[test]
fn commit_crash() {
    for (steps, hwm, stopped) in [
        ("append", 10, false),
        ("tmp", 10, false),
        ("rename", 20, true),
        ("tmp truncate", 10, true),
    ] {
        let root = tempdir("crash");
        let c = FakeClock::new();
        let mut s = open(&root, &c);
        s.push(&batch(1, 10).b()).unwrap();
        s.crash = Some(Box::new(move |at| steps.contains(at)));
        assert!(
            s.push(&batch(11, 10).b()).is_err(),
            "{steps}: crash not injected"
        );
        s.crash = None;
        if stopped {
            assert!(
                s.push(&batch(11, 10).b()).is_err(),
                "{steps}: writer not stopped"
            );
        }
        // A failed rollback leaves the frame past the committed length, for recovery to cut.
        let size = fs::metadata(data_path(&root, "node-a", "2026-10-19"))
            .unwrap()
            .len() as i64;
        if steps == "tmp truncate" {
            assert!(
                size > s.st.days["node-a/2026-10-19"],
                "{steps}: tail already cut"
            );
        }
        drop(s);
        let mut s = open(&root, &c);
        let got = s.streams()[STREAM_A].hwm;
        assert_eq!(got, hwm, "{steps}: hwm after crash");
        assert_eq!(seqs(&root, "node-a").len() as u64, hwm, "{steps}");
        let r = s.push(&batch(11, 10).b()).unwrap();
        assert_eq!(
            (r.acked, r.stored as u64),
            (20, 20 - hwm),
            "{steps}: resend"
        );
        assert_eq!(seqs(&root, "node-a").len(), 20, "{steps}");
        verify_ok(&root).unwrap();
    }
}

fn rewrite_state(root: &Path, key: &str, value: &str) {
    let p = root.join("state.json");
    let mut m: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&fs::read(&p).unwrap()).unwrap();
    m.insert(key.into(), serde_json::from_str(value).unwrap());
    fs::write(&p, serde_json::to_vec(&m).unwrap()).unwrap();
}

fn is_root() -> bool {
    fs::read_to_string("/proc/self/status")
        .map(|s| {
            s.lines()
                .any(|l| l.starts_with("Uid:") && l.split_whitespace().nth(1) == Some("0"))
        })
        .unwrap_or(false)
        || std::env::var("USER").is_ok_and(|u| u == "root")
}

/// Recovery refuses a state that does not match the files, and changes nothing.
#[test]
fn recover_refuses() {
    use std::os::unix::fs::PermissionsExt;
    const DAY_FILE: &str = "node-a/2026-10-19.ndjson.zst";
    let damages: Vec<(&str, Damage)> = vec![
        (
            "missing file",
            Box::new(|r| fs::remove_file(r.join(DAY_FILE)).unwrap()),
        ),
        (
            "short file",
            Box::new(|r| truncate(&r.join(DAY_FILE), 5).unwrap()),
        ),
        ("null days", Box::new(|r| rewrite_state(r, "days", "null"))),
        (
            "null stream",
            Box::new(|r| rewrite_state(r, "streams", &format!(r#"{{"{STREAM_A}":null}}"#))),
        ),
        ("version", Box::new(|r| rewrite_state(r, "v", "2"))),
        (
            "empty object",
            Box::new(|r| fs::write(r.join("state.json"), "{}").unwrap()),
        ),
        (
            "null",
            Box::new(|r| fs::write(r.join("state.json"), "null").unwrap()),
        ),
        (
            "no days",
            Box::new(|r| {
                fs::write(r.join("state.json"), r#"{"v":1,"streams":{},"own_seq":0}"#).unwrap()
            }),
        ),
        (
            "unreadable",
            Box::new(|r| {
                if is_root() {
                    fs::remove_file(r.join(DAY_FILE)).unwrap(); // root reads anything
                }
                fs::set_permissions(r.join("node-a"), fs::Permissions::from_mode(0o000)).unwrap();
            }),
        ),
    ];
    for (name, damage) in damages {
        let root = tempdir("refuse");
        let c = FakeClock::new();
        let mut s = open(&root, &c);
        s.push(&batch(1, 10).b()).unwrap();
        drop(s);
        damage(&root);
        let before = fs::read(root.join("state.json")).unwrap();
        assert!(Store::open(&root, 0, c.clock()).is_err(), "{name}: opened");
        fs::set_permissions(root.join("node-a"), fs::Permissions::from_mode(0o750)).unwrap();
        assert_eq!(
            fs::read(root.join("state.json")).unwrap(),
            before,
            "{name}: state.json changed"
        );
        if name == "short file" {
            assert_eq!(
                fs::metadata(root.join(DAY_FILE)).unwrap().len(),
                5,
                "{name}: file changed"
            );
        }
    }
}

#[test]
fn one_writer() {
    let root = tempdir("writer");
    let c = FakeClock::new();
    let s = open(&root, &c);
    assert!(
        Store::open(&root, 0, c.clock()).is_err(),
        "a second writer opened the store"
    );
    drop(s);
    Store::open(&root, 0, c.clock()).unwrap();
}

/// A seal stopped at any step leaves the day sealed or open, never a file that recovery cuts
/// to the length of its other representation. The process goes on without a restart: once the
/// manifest is published the day refuses pushes although it is still in the state's open days,
/// and the next seal and push work.
#[test]
fn seal_crash() {
    for step in ["tmp", "rename", "seal-rename"] {
        let root = tempdir("sealcrash");
        let c = FakeClock::new();
        let mut s = open(&root, &c);
        for i in 0..5 {
            s.push(&batch(1 + 10 * i, 10).b()).unwrap();
        }
        c.set(day0() + DAY);
        s.crash = Some(Box::new(move |at| at == step));
        let (sealed, errs) = s.seal_past();
        assert!(
            sealed.is_empty() && errs.len() == 1,
            "{step}: crash not injected: {sealed:?} {errs:?}"
        );
        s.crash = None;
        let published = step != "tmp";
        c.set(day0());
        let r = s.push(&batch(51, 5).b());
        assert_eq!(
            published,
            r.is_err_and(|e| e.kind == Kind::SealedDay),
            "{step}: push on the day just sealed"
        );
        let want = if published { 50 } else { 55 };
        c.set(day0() + DAY);
        let (sealed, errs) = s.seal_past();
        assert!(
            errs.is_empty() && published != (sealed.len() == 1),
            "{step}: seal again: {sealed:?} {errs:?}"
        );
        let r = s.push(&batch(want + 1, 5).b()).unwrap();
        assert_eq!(r.acked, want + 5, "{step}: push the next day");
        verify_ok(&root).unwrap();
        drop(s);
        let s = open(&root, &c);
        assert!(
            !s.st.days.contains_key("node-a/2026-10-19"),
            "{step}: sealed day still open"
        );
        let got = seqs(&root, "node-a");
        assert_eq!(
            (got.len() as u64, *got.last().unwrap()),
            (want + 5, want + 5),
            "{step}"
        );
        verify_ok(&root).unwrap();
        let tmps: Vec<_> = fs::read_dir(root.join("node-a"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(tmps.is_empty(), "{step}: left {tmps:?}");
    }
}

fn frame(data: &[u8]) -> Vec<u8> {
    zstd::encode_all(data, 3).unwrap()
}

/// A day that fails to seal holds back the later days of its node, so the chain never skips it.
#[test]
fn seal_defers_later_days() {
    let root = tempdir("defer");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 5).b()).unwrap();
    c.set(day0() + DAY);
    s.push(&batch(6, 5).b()).unwrap();
    let p = data_path(&root, "node-a", "2026-10-19");
    let good = fs::read(&p).unwrap();
    let bad = frame(b"not json\n");
    fs::write(&p, &bad).unwrap();
    s.st.days
        .insert("node-a/2026-10-19".into(), bad.len() as i64);
    c.set(day0() + 2 * DAY);
    let (sealed, errs) = s.seal_past();
    assert!(sealed.is_empty() && errs.len() == 2, "{sealed:?} {errs:?}");
    fs::write(&p, &good).unwrap();
    s.st.days
        .insert("node-a/2026-10-19".into(), good.len() as i64);
    let (sealed, errs) = s.seal_past();
    assert!(
        sealed.len() == 2 && errs.is_empty(),
        "after repair: {sealed:?} {errs:?}"
    );
    verify_ok(&root).unwrap();
}

#[test]
fn recover_unknown_file() {
    let root = tempdir("unknown");
    let c = FakeClock::new();
    drop(open(&root, &c));
    fs::create_dir_all(root.join("node-b")).unwrap();
    let p = root.join("node-b/2026-10-19.ndjson.zst");
    fs::write(&p, "garbage").unwrap();
    drop(open(&root, &c));
    assert_eq!(
        fs::metadata(&p).unwrap().len(),
        0,
        "unknown day file not truncated to zero"
    );
}

#[test]
fn seal_chain_retention_read() {
    let root = tempdir("chain");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 10).b()).unwrap();
    let (sealed, errs) = s.seal_past();
    assert!(sealed.is_empty() && errs.is_empty(), "sealed the open day");
    c.set(day0() + DAY);
    drop(s);
    let mut s = open(&root, &c);
    s.push(&batch(11, 5).b()).unwrap();
    for i in 0..2 {
        // idempotent
        let (sealed, errs) = s.seal_past();
        assert!(
            errs.is_empty() && sealed.len() == if i == 0 { 1 } else { 0 },
            "seal {i}"
        );
    }
    let ms = manifests(&root.join("node-a")).unwrap();
    assert_eq!(ms.len(), 1);
    let m = &ms[0];
    assert_eq!(
        (
            m.day.as_str(),
            m.records,
            m.prev_day.is_none(),
            m.services["annai"]
        ),
        ("2026-10-19", 10, true, 10)
    );
    let st = m.streams.as_ref().unwrap();
    assert_eq!((st.len(), st[0].last_seq), (1, 10));
    assert_eq!(
        (m.min_ts.as_str(), m.max_ts.as_str()),
        (
            "2026-10-19T08:00:01.000000000Z",
            "2026-10-19T08:00:10.000000000Z"
        )
    );
    let (sha, n) = read_lines(&data_path(&root, "node-a", "2026-10-19"), true, |_| Ok(())).unwrap();
    assert_eq!((sha.as_str(), n), (m.content_sha256.as_str(), 10));
    assert_eq!(chain_heads(&root).unwrap()["node-a"], m.sha256);
    c.set(day0() + 2 * DAY);
    drop(s);
    let mut s = open(&root, &c);
    s.seal_past();
    let ms = manifests(&root.join("node-a")).unwrap();
    assert_eq!(ms.len(), 2);
    assert_eq!(ms[1].prev_day.as_deref(), Some("2026-10-19"));
    assert_eq!(ms[1].prev_manifest_sha256.as_ref(), Some(&ms[0].sha256));
    verify_ok(&root).unwrap();
    // Retention deletes data, keeps manifests; verify still passes.
    let (deleted, err) = s.retain(1);
    assert_eq!(
        (deleted, err),
        (vec!["node-a/2026-10-19".to_string()], None)
    );
    assert!(
        !data_path(&root, "node-a", "2026-10-19").exists(),
        "data file kept"
    );
    assert!(
        manifest_path(&root, "node-a", "2026-10-19").exists(),
        "manifest deleted"
    );
    verify_ok(&root).unwrap();
    assert!(s.retain(0).0.is_empty(), "retention 0 deleted data");
    // Read: pruned day (deleted anyway), sealed day 2 (seqs 11-15, ts 08:00:11-15), sorted.
    let now = day0() + 2 * DAY;
    let q = Query {
        node: "node-a".into(),
        service: "annai".into(),
        event: "query".into(),
        from: parse_time("2026-10-19T08:00:12Z", now).unwrap(),
        to: parse_time("2026-10-19T08:00:14Z", now).unwrap(),
    };
    let mut out = Vec::new();
    read(&root, &q, &mut out).unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(
        out.matches('\n').count() == 3 && out.contains(r#""seq":13"#),
        "read: {out}"
    );
    // Tampering is detected.
    fs::write(manifest_path(&root, "node-a", "2026-10-19"), r#"{"v":1}"#).unwrap();
    assert!(
        verify(&root, &mut Vec::new()).is_err(),
        "verify passed on a tampered manifest"
    );
}

#[test]
fn seal_stops_on_bad_line() {
    let root = tempdir("badline");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 1).b()).unwrap();
    // Corrupt the committed content: a frame holding a non-JSON line.
    let p = data_path(&root, "node-a", "2026-10-19");
    let bad = frame(b"not json\n");
    fs::write(&p, &bad).unwrap();
    s.st.days
        .insert("node-a/2026-10-19".into(), bad.len() as i64);
    c.set(day0() + DAY);
    let (sealed, errs) = s.seal_past();
    assert!(sealed.is_empty() && errs.len() == 1, "{sealed:?} {errs:?}");
    assert!(
        !manifest_path(&root, "node-a", "2026-10-19").exists(),
        "manifest written for a bad file"
    );
}

#[test]
fn parse_time_bounds() {
    let now = day0();
    assert_eq!(parse_time("-1h", now), Ok(now - HOUR));
    assert_eq!(
        parse_time("2026-10-19T08:00:00.123456789Z", now).unwrap() % 1_000_000_000,
        123_456_789
    );
    assert!(parse_time("yesterday", now).is_err());
    for s in [
        "1677-09-21T00:12:43.145224191Z",
        "2262-04-11T23:47:16.854775808Z",
        "+2562047h",
    ] {
        assert!(
            parse_time(s, now).is_err(),
            "{s}: accepted outside the ts range"
        );
    }
    assert!(parse_time("2262-04-11T23:47:16.854775807Z", now).is_ok());
}

/// Free service fields named like envelope members (`TS`, `SEQ`, …) never change the envelope
/// a seal, a read or verify sees.
#[test]
fn envelope_exact_names() {
    let root = tempdir("names");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    let mut b = batch(1, 2);
    let r1 = String::from_utf8(b.records[1].to_vec()).unwrap().replacen(
        r#","qname""#,
        &format!(
            r#","TS":"2099-01-01T00:00:00.000000000Z","SEQ":{{}},"SERVICE":"x","STREAM":"{}","qname""#,
            "f".repeat(32)
        ),
        1,
    );
    assert_eq!(crate::logschema::record(r1.as_bytes()), "");
    b.records[1] = Bytes::from(r1);
    s.push(&b.b()).unwrap();
    c.set(day0() + DAY);
    assert!(s.seal_past().1.is_empty());
    let m = &manifests(&root.join("node-a")).unwrap()[0];
    assert_eq!(m.max_ts, "2026-10-19T08:00:02.000000000Z");
    assert_eq!(m.services.len(), 1);
    assert_eq!(m.services["annai"], 2);
    let st = m.streams.as_ref().unwrap();
    assert_eq!(
        (
            st.len(),
            st[0].stream.as_str(),
            st[0].first_seq,
            st[0].last_seq
        ),
        (1, STREAM_A, 1, 2)
    );
    let q = Query {
        node: "node-a".into(),
        service: "annai".into(),
        event: String::new(),
        from: day0() - 5 * HOUR,
        to: day0(),
    };
    let mut out = Vec::new();
    read(&root, &q, &mut out).unwrap();
    assert_eq!(out.iter().filter(|&&c| c == b'\n').count(), 2);
    verify_ok(&root).unwrap();
}

/// Verify reads sealed days strictly: a trailing partial frame, a manifest whose day is not
/// its file's, an unreadable node directory all fail it. A line over 4 MiB is an error.
#[test]
fn verify_strict() {
    use std::os::unix::fs::PermissionsExt;
    let damages: Vec<(&str, Damage)> = vec![
        (
            "partial frame",
            Box::new(|r| {
                use std::io::Write;
                let mut f = fs::OpenOptions::new()
                    .append(true)
                    .open(data_path(r, "node-a", "2026-10-19"))
                    .unwrap();
                f.write_all(&[0x28, 0xb5]).unwrap();
            }),
        ),
        (
            "manifest day",
            Box::new(|r| {
                let p = manifest_path(r, "node-a", "2026-10-19");
                let b = fs::read_to_string(&p).unwrap();
                fs::write(
                    &p,
                    b.replacen(r#""day":"2026-10-19""#, r#""day":"2026-10-18""#, 1),
                )
                .unwrap();
            }),
        ),
        (
            "unreadable",
            Box::new(|r| {
                if !is_root() {
                    fs::set_permissions(r.join("node-a"), fs::Permissions::from_mode(0o000))
                        .unwrap();
                } else {
                    fs::write(manifest_path(r, "node-a", "2026-10-19"), "x").unwrap();
                }
            }),
        ),
        (
            "not a calendar day",
            Box::new(|r| {
                let p = manifest_path(r, "node-a", "2026-10-19");
                let b = fs::read_to_string(&p).unwrap();
                fs::write(
                    manifest_path(r, "node-a", "2026-02-30"),
                    b.replacen(r#""day":"2026-10-19""#, r#""day":"2026-02-30""#, 1),
                )
                .unwrap();
                fs::rename(
                    data_path(r, "node-a", "2026-10-19"),
                    data_path(r, "node-a", "2026-02-30"),
                )
                .unwrap();
                fs::remove_file(&p).unwrap();
            }),
        ),
        (
            "long record",
            Box::new(|r| {
                // valid JSON, unique seq, over the 4 MiB a stored line may have
                let rec = String::from_utf8(record(99).to_vec()).unwrap();
                let rec = format!(
                    "{},\"pad\":\"{}\"}}\n",
                    rec.strip_suffix('}').unwrap(),
                    "a".repeat(MAX_LINE)
                );
                fs::write(data_path(r, "node-a", "2026-10-20"), frame(rec.as_bytes())).unwrap();
            }),
        ),
        (
            "long line",
            Box::new(|r| {
                fs::write(
                    data_path(r, "node-a", "2026-10-20"),
                    frame(&vec![b'a'; MAX_LINE + 1]),
                )
                .unwrap();
            }),
        ),
    ];
    for (name, damage) in damages {
        let root = tempdir("strict");
        let c = FakeClock::new();
        let mut s = open(&root, &c);
        s.push(&batch(1, 5).b()).unwrap();
        c.set(day0() + DAY);
        s.seal_past();
        verify_ok(&root).unwrap();
        damage(&root);
        assert!(
            verify(&root, &mut Vec::new()).is_err(),
            "{name}: verify passed"
        );
        fs::set_permissions(root.join("node-a"), fs::Permissions::from_mode(0o750)).unwrap();
    }
}

/// A seal source whose committed content ends with a partial frame is not sealed, and stays.
#[test]
fn seal_strict_source() {
    use std::io::Write;
    let root = tempdir("strictsrc");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 5).b()).unwrap();
    let p = data_path(&root, "node-a", "2026-10-19");
    let f = frame(&record(6));
    fs::OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(&f[..f.len() - 3])
        .unwrap();
    let size = fs::metadata(&p).unwrap().len();
    s.st.days.insert("node-a/2026-10-19".into(), size as i64);
    let q = Query {
        node: "node-a".into(),
        service: String::new(),
        event: String::new(),
        from: day0() - 5 * HOUR,
        to: day0(),
    };
    let mut out = Vec::new();
    read(&root, &q, &mut out).unwrap();
    assert_eq!(
        out.iter().filter(|&&c| c == b'\n').count(),
        5,
        "open day not readable"
    );
    c.set(day0() + DAY);
    let (sealed, errs) = s.seal_past();
    assert!(sealed.is_empty() && errs.len() == 1, "{sealed:?} {errs:?}");
    assert_eq!(fs::metadata(&p).unwrap().len(), size, "file changed");
}

/// A stream belongs to the node that first pushed it, across restarts; nisshi's own stream
/// to no agent (ADR 0014). Two racing first pushes: one wins.
#[test]
fn stream_owner() {
    let root = tempdir("owner");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 10).b()).unwrap();
    let other = batch(11, 10).moved("node-b", STREAM_A);
    let gap = batch(1, 1).moved("node-a", GAP_STREAM);
    for b in [&other, &gap] {
        assert_eq!(
            s.push(&b.b()).unwrap_err().kind,
            Kind::StreamOwner,
            "{}/{}",
            b.node,
            b.stream
        );
    }
    drop(s);
    let s = Arc::new(std::sync::Mutex::new(open(&root, &c)));
    assert_eq!(
        s.lock().unwrap().push(&other.b()).unwrap_err().kind,
        Kind::StreamOwner
    );
    let r = s.lock().unwrap().push(&batch(11, 10).b()).unwrap();
    assert_eq!((r.stored, r.acked), (10, 20));
    assert_eq!(seqs(&root, "node-a").len(), 20);
    let cc = "c".repeat(32);
    let handles: Vec<_> = ["node-a", "node-b"]
        .into_iter()
        .map(|node| {
            let (s, cc) = (s.clone(), cc.clone());
            std::thread::spawn(move || {
                let b = batch(1, 1).moved(node, &cc);
                s.lock().unwrap().push(&b.b()).map(|_| ())
            })
        })
        .collect();
    let mut results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    results.sort_by_key(|r| r.is_err());
    assert!(
        results[0].is_ok()
            && results[1]
                .as_ref()
                .is_err_and(|e| e.kind == Kind::StreamOwner)
    );
    assert_eq!(s.lock().unwrap().streams()[&cc].hwm, 1);
    for node in ["node-a", "node-b"] {
        for (l, _) in lines(&root, node) {
            let e = parse_envelope(l.as_bytes()).unwrap();
            assert!(
                e.stream != cc || e.node == node,
                "{node} holds a record of {}",
                e.node
            );
        }
    }
}

/// The store's clock back on a sealed day, or before the newest one: the push fails, nothing
/// changes on disk, and pushes succeed again once the clock is right (ADR 0006 amendment).
#[test]
fn clock_back_on_sealed_day() {
    let root = tempdir("clockback");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 10).b()).unwrap();
    c.set(day0() + DAY);
    s.seal_past();
    let data = fs::read(data_path(&root, "node-a", "2026-10-19")).unwrap();
    let manifest = fs::read(manifest_path(&root, "node-a", "2026-10-19")).unwrap();
    for back in [day0() + 11 * HOUR, day0() - 3 * DAY] {
        c.set(back);
        assert_eq!(s.push(&batch(11, 5).b()).unwrap_err().kind, Kind::SealedDay);
    }
    assert_eq!(
        fs::read(data_path(&root, "node-a", "2026-10-19")).unwrap(),
        data
    );
    assert_eq!(
        fs::read(manifest_path(&root, "node-a", "2026-10-19")).unwrap(),
        manifest
    );
    assert_eq!(s.streams()[STREAM_A].hwm, 10);
    assert_eq!(fs::read_dir(root.join("node-a")).unwrap().count(), 2);
    c.set(day0() + DAY);
    assert_eq!(s.push(&batch(11, 5).b()).unwrap().acked, 15);
    verify_ok(&root).unwrap();
}

fn copy_tree(src: &Path, dst: &Path, days: &std::collections::BTreeMap<String, i64>) {
    for node in fs::read_dir(src).unwrap().flatten() {
        let name = node.file_name().to_string_lossy().into_owned();
        if name == "FORMAT" {
            fs::copy(node.path(), dst.join("FORMAT")).unwrap();
        }
        if !node.path().is_dir() {
            continue;
        }
        fs::create_dir_all(dst.join(&name)).unwrap();
        for f in fs::read_dir(node.path()).unwrap().flatten() {
            let fname = f.file_name().to_string_lossy().into_owned();
            let mut data = fs::read(f.path()).unwrap();
            if let Some(day) = fname.strip_suffix(".ndjson.zst")
                && let Some(&n) = days.get(&format!("{name}/{day}"))
            {
                data.truncate(n as usize); // the .partial copy, renamed back
            }
            fs::write(dst.join(&name).join(&fname), data).unwrap();
        }
    }
}

/// A store restored from its backup (sealed days with their manifests, the committed prefix
/// of the open day as `.partial` renamed, no state.json) rebuilds hwm, owners and its own seq,
/// and keeps the open day open (storage-format, "Restore").
#[test]
fn restore() {
    let src = tempdir("restore-src");
    let c = FakeClock::new();
    let mut s = open(&src, &c);
    s.push(&batch(1, 10).b()).unwrap();
    s.push(&batch(21, 5).b()).unwrap(); // gap 11-20: nisshi's seq 1
    c.set(day0() + DAY);
    s.push(&batch(26, 5).b()).unwrap();
    s.push(&batch(1, 3).moved("node-b", &"b".repeat(32)).b())
        .unwrap();
    s.seal_past();
    let days = s.st.days.clone();
    let backup = || {
        let dst = tempdir("restore");
        copy_tree(&src, &dst, &days);
        dst
    };
    {
        let root = backup();
        // Backup artefacts copied by mistake are not store files: the deployment's LAST_RUN
        // marker, stray *.tmp files, and a directory that is no node (ext4's lost+found).
        fs::write(root.join("LAST_RUN"), "2026-10-20T12:00:00Z\n").unwrap();
        fs::write(root.join("state.json.tmp"), "{").unwrap();
        fs::write(root.join("node-a.ndjson.zst.tmp"), "x").unwrap();
        fs::write(root.join("node-a/2026-10-19.manifest.json.tmp"), "{").unwrap();
        fs::create_dir(root.join("lost+found")).unwrap();
        let junk = root.join("lost+found/2026-10-19.ndjson.zst");
        fs::write(&junk, "not a store file").unwrap();
        let mut r = open(&root, &c);
        assert_eq!(fs::read_to_string(&junk).unwrap(), "not a store file");
        assert_eq!(r.rebuilt(), Some(2));
        let st = &r.streams()[STREAM_A];
        assert_eq!((st.hwm, st.node.as_str(), r.st.own_seq), (30, "node-a", 1));
        assert!(
            r.st.days.contains_key("node-a/2026-10-20"),
            "open day not kept open"
        );
        assert_eq!(
            r.push(&batch(26, 5).b()).unwrap().stored,
            0,
            "resend stored again"
        );
        assert!(r.push(&batch(41, 1).b()).unwrap().gap);
        assert_eq!(
            r.push(&batch(42, 1).moved("node-b", STREAM_A).b())
                .unwrap_err()
                .kind,
            Kind::StreamOwner
        );
        c.set(day0() + 2 * DAY);
        assert!(r.seal_past().1.is_empty());
        verify_ok(&root).unwrap(); // no duplicate (stream, seq), own seq included
        c.set(day0() + DAY);
    }
    // A .partial left unrenamed is an incomplete restore: it stops the start.
    let root = backup();
    fs::rename(
        root.join("node-a/2026-10-20.ndjson.zst"),
        root.join("node-a/2026-10-20.ndjson.zst.partial"),
    )
    .unwrap();
    let e = Store::open(&root, 0, c.clock()).err().unwrap();
    assert!(e.contains(".partial"), "{e}");
    // An open day must end on a complete frame: a corrupt frame, a corrupt block size running
    // past the end, or a final frame cut short stops the start and changes nothing; the same
    // frames intact are all kept (validation R2).
    for (name, want) in [
        ("intact", ""),
        ("corrupt magic", "corrupt zstd frame"),
        ("corrupt block size", "incomplete zstd frame"),
        ("cut final frame", "incomplete zstd frame"),
    ] {
        let root = backup();
        let p = root.join("node-a/2026-10-20.ndjson.zst");
        let mut data = fs::read(&p).unwrap();
        let at = data.len();
        for seq in [31, 32] {
            let mut r = record(seq).to_vec();
            r.push(b'\n');
            data.extend(frame(&r));
        }
        match name {
            "corrupt magic" => data[at] ^= 0xff,
            "corrupt block size" => {
                // first block header of the middle frame: 64 KiB, past the end
                let fhd = data[at + 4];
                let single = fhd & 0x20 != 0;
                let dict = [0, 1, 2, 4][usize::from(fhd & 3)];
                let fcs = match fhd >> 6 {
                    0 => usize::from(single),
                    v => 1 << v,
                };
                let o = at + 5 + usize::from(!single) + dict + fcs;
                let bh = (usize::from(data[o]) & 7) | (64 << 10) << 3;
                data[o] = bh as u8;
                data[o + 1] = (bh >> 8) as u8;
                data[o + 2] = (bh >> 16) as u8;
            }
            "cut final frame" => data.truncate(data.len() - 5),
            _ => {}
        }
        fs::write(&p, &data).unwrap();
        let r = Store::open(&root, 0, c.clock());
        if want.is_empty() {
            assert_eq!(r.unwrap().streams()[STREAM_A].hwm, 32, "intact frames");
            continue;
        }
        let e = r.err().unwrap_or_else(|| panic!("{name}: opened"));
        assert!(e.contains(want), "{name}: {e}");
        assert_eq!(fs::read(&p).unwrap(), data, "{name}: open day changed");
        assert!(
            !root.join("state.json").exists(),
            "{name}: state.json written"
        );
    }
    // A manifest whose stream ranges do not match its data stops the start.
    let root = backup();
    let p = root.join("node-a/2026-10-19.manifest.json");
    let m = fs::read_to_string(&p).unwrap();
    fs::write(&p, m.replacen(r#""last_seq":25"#, r#""last_seq":24"#, 1)).unwrap();
    assert!(
        Store::open(&root, 0, c.clock()).is_err(),
        "opened with a tampered manifest"
    );
}

/// An empty day file (truncated to zero, then sealed) seals to `"streams":null`, as Go did.
#[test]
fn empty_day_seals() {
    let root = tempdir("empty");
    let c = FakeClock::new();
    drop(open(&root, &c));
    fs::create_dir_all(root.join("node-b")).unwrap();
    fs::write(root.join("node-b/2026-10-18.ndjson.zst"), "garbage").unwrap();
    let mut s = open(&root, &c);
    let (sealed, errs) = s.seal_past();
    assert_eq!((sealed.len(), errs.len()), (1, 0), "{errs:?}");
    let m = fs::read_to_string(manifest_path(&root, "node-b", "2026-10-18")).unwrap();
    assert!(
        m.contains(r#""records":0,"#)
            && m.contains(r#""streams":null,"#)
            && m.contains(r#""min_ts":"","#),
        "{m}"
    );
    verify_ok(&root).unwrap();
}

/// Sequence numbers at the top of the range (validation P1): the first push may end at
/// u64::MAX, after which every batch is a duplicate and the hwm never goes back; a batch that
/// would run past u64::MAX stores nothing. No arithmetic overflow, in debug builds too.
#[test]
fn hwm_at_u64_max() {
    let root = tempdir("u64max");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    let r = s.push(&batch(u64::MAX - 2, 3).b()).unwrap();
    assert_eq!((r.acked, r.stored, r.gap), (u64::MAX, 3, true));
    for first in [1, u64::MAX - 1, u64::MAX] {
        let r = s.push(&batch(first, 1).b()).unwrap();
        assert_eq!(
            (r.acked, r.stored, r.duplicates),
            (u64::MAX, 0, 1),
            "{first}"
        );
    }
    assert_eq!(s.streams()[STREAM_A].hwm, u64::MAX);
    let root = tempdir("u64max-past");
    let mut s = open(&root, &c);
    let mut b = batch(u64::MAX, 1);
    b.records.push(record(0));
    assert_eq!(s.push(&b.b()).unwrap_err().kind, Kind::Other);
    assert!(seqs(&root, "node-a").is_empty());
    assert!(s.streams().is_empty());
}

/// A sealed day relabelled under another node, manifest and path both saying node-b while
/// its records say node-a (content hash unchanged), is refused at restore, nothing changed.
/// Go nisshi accepted it (validation P2, masu `check_sealed`).
#[test]
fn restore_refuses_relabelled_day() {
    let root = tempdir("relabel");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 5).b()).unwrap();
    c.set(day0() + DAY);
    assert!(s.seal_past().1.is_empty());
    drop(s);
    fs::rename(root.join("node-a"), root.join("node-b")).unwrap();
    let mp = root.join("node-b/2026-10-19.manifest.json");
    let m = fs::read_to_string(&mp).unwrap();
    fs::write(&mp, m.replace(r#""node":"node-a""#, r#""node":"node-b""#)).unwrap();
    fs::remove_file(root.join("state.json")).unwrap();
    let data = root.join("node-b/2026-10-19.ndjson.zst");
    let before = (fs::read(&data).unwrap(), fs::read(&mp).unwrap());
    let e = Store::open(&root, 0, c.clock()).err().unwrap();
    assert!(e.contains("node-a"), "{e}");
    assert_eq!((fs::read(&data).unwrap(), fs::read(&mp).unwrap()), before);
    assert!(!root.join("state.json").exists());
}

/// A seal interrupted after its manifest is checked at open, not trusted: a data file that no
/// longer matches its manifest stops the start (masu recovery).
#[test]
fn interrupted_seal_is_verified() {
    let root = tempdir("sealverify");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 5).b()).unwrap();
    c.set(day0() + DAY);
    s.crash = Some(Box::new(|at| at == "seal-rename"));
    assert_eq!(s.seal_past().1.len(), 1);
    drop(s);
    let data = data_path(&root, "node-a", "2026-10-19");
    let good = fs::read(&data).unwrap();
    let mut other = record(9).to_vec();
    other.push(b'\n');
    fs::write(&data, frame(&other)).unwrap();
    let e = Store::open(&root, 0, c.clock()).err().unwrap();
    assert!(e.contains("interrupted seal"), "{e}");
    fs::write(&data, &good).unwrap();
    let s = open(&root, &c);
    assert!(!s.st.days.contains_key("node-a/2026-10-19"));
}

/// A stopped writer seals nothing and deletes nothing until a restart (masu).
#[test]
fn broken_writer_skips_maintenance() {
    let root = tempdir("brokenseal");
    let c = FakeClock::new();
    let mut s = open(&root, &c);
    s.push(&batch(1, 5).b()).unwrap();
    s.crash = Some(Box::new(|at| at == "rename"));
    assert!(s.push(&batch(6, 5).b()).is_err());
    s.crash = None;
    assert!(s.is_broken());
    c.set(day0() + DAY);
    let (sealed, errs) = s.seal_past();
    assert!(
        sealed.is_empty() && errs[0].contains("writer stopped"),
        "{errs:?}"
    );
    assert!(s.retain(1).1.is_some_and(|e| e.contains("writer stopped")));
    assert!(!manifest_path(&root, "node-a", "2026-10-19").exists());
}

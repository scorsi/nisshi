//! The on-disk log store of docs/storage-format.md (`nisshi-store 1`): NDJSON records in zstd
//! frames by node and UTC receive day, atomic commits through `state.json` (ADR 0005).
//! Sealing, retention and verification are in `seal`, restore in `restore`.

use crate::json::{Style, quote};
use prost::bytes::Bytes;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub const FORMAT_LINE: &[u8] = b"nisshi-store 1\n";
/// The `stream` of records nisshi writes itself.
pub const GAP_STREAM: &str = "00000000000000000000000000000000";
/// zstd window of the data files: 8 MiB.
pub const WINDOW_LOG: u32 = 23;
/// The longest stored line without its newline: a record is at most 4 MiB.
pub const MAX_LINE: usize = 4 << 20;
/// Level of the open day's frames (Go nisshi: klauspost `SpeedDefault`).
const OPEN_LEVEL: i32 = 3;

/// A test hook: aborts a commit or a seal after the named step (ADR 0011).
pub type Crash = Box<dyn Fn(&str) -> bool + Send>;
/// The store's clock, unix nanoseconds (fake in tests and under `NISSHI_FAKE_NOW`).
pub type Clock = Box<dyn Fn() -> i64 + Send>;

/// An I/O error as Go's `os` package prints it: `open /x: no such file or directory`.
pub fn ioerr(op: &str, path: &Path, e: &io::Error) -> String {
    format!("{op} {}: {}", path.display(), errno_text(e))
}

/// The OS error text without Rust's ` (os error N)` suffix, lower-cased as Go prints it.
pub fn errno_text(e: &io::Error) -> String {
    let s = e.to_string();
    let s = match s.find(" (os error ") {
        Some(i) => s[..i].to_string(),
        None => s,
    };
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
        None => s,
    }
}

/// Why a push failed (HTTP code in `serve`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
    /// 403 `stream-owner` (ADR 0014).
    StreamOwner,
    /// 507: free space below the reserve or a write error; the batch was rolled back.
    Disk,
    /// 503: the store's clock puts the batch on a sealed day (ADR 0006 amendment).
    SealedDay,
    /// 500.
    Other,
}

#[derive(Debug, Clone)]
pub struct PushErr {
    pub kind: Kind,
    pub msg: String,
}

impl std::fmt::Display for PushErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

fn err(kind: Kind, msg: String) -> PushErr {
    PushErr { kind, msg }
}

/// The writer state of one agent stream.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stream {
    pub node: String,
    pub hwm: u64,
    pub last_receive_ns: i64,
    pub last_record_ts: String,
}

#[derive(Debug, Clone, Default)]
pub struct State {
    /// `<node>/<day>` → committed length of the open day file.
    pub days: BTreeMap<String, i64>,
    pub streams: BTreeMap<String, Stream>,
    pub own_seq: u64,
}

impl State {
    /// `state.json` as Go's `json.Marshal` wrote it, byte for byte.
    pub fn to_json(&self) -> String {
        let q = |s: &str| quote(s, Style::Marshal);
        let days: Vec<String> = self
            .days
            .iter()
            .map(|(k, v)| format!("{}:{v}", q(k)))
            .collect();
        let streams: Vec<String> = self
            .streams
            .iter()
            .map(|(k, s)| {
                format!(
                    "{}:{{\"node\":{},\"hwm\":{},\"last_receive_ns\":{},\"last_record_ts\":{}}}",
                    q(k),
                    q(&s.node),
                    s.hwm,
                    s.last_receive_ns,
                    q(&s.last_record_ts)
                )
            })
            .collect();
        format!(
            "{{\"v\":1,\"days\":{{{}}},\"streams\":{{{}}},\"own_seq\":{}}}",
            days.join(","),
            streams.join(","),
            self.own_seq
        )
    }

    /// Reads `state.json`: every member required, so that a member left out never inherits
    /// the empty maps of a new store, whose recovery would cut every open day to zero.
    pub fn parse(b: &[u8]) -> Result<State, String> {
        #[derive(Deserialize)]
        struct StreamIn {
            node: Option<String>,
            hwm: Option<u64>,
            last_receive_ns: Option<i64>,
            last_record_ts: Option<String>,
        }
        #[derive(Deserialize)]
        struct StateIn {
            v: Option<i64>,
            days: Option<BTreeMap<String, Option<i64>>>,
            streams: Option<BTreeMap<String, Option<StreamIn>>>,
            own_seq: Option<u64>,
        }
        let value: serde_json::Value =
            serde_json::from_slice(b).map_err(|e| format!("state.json: {e}"))?;
        match &value {
            serde_json::Value::Null => return Err("state.json: v missing".into()),
            serde_json::Value::Object(m) => {
                for k in ["v", "days", "streams", "own_seq"] {
                    if !m.contains_key(k) {
                        return Err(format!("state.json: {k} missing"));
                    }
                }
            }
            other => {
                return Err(format!(
                    "state.json: json: cannot unmarshal {} into Go value of type map[string]json.RawMessage",
                    json_kind(other)
                ));
            }
        }
        let st: StateIn = serde_json::from_value(value).map_err(|e| format!("state.json: {e}"))?;
        let v = st.v.unwrap_or(0);
        let (Some(days), Some(streams)) = (st.days, st.streams) else {
            return Err(format!("state.json: version {v} or days/streams null"));
        };
        if v != 1 {
            return Err(format!("state.json: version {v} or days/streams null"));
        }
        let mut out = State {
            days: days.into_iter().map(|(k, v)| (k, v.unwrap_or(0))).collect(),
            streams: BTreeMap::new(),
            own_seq: st.own_seq.unwrap_or(0),
        };
        for (k, s) in streams {
            let Some(s) = s else {
                return Err(format!("state.json: stream {k} is null"));
            };
            out.streams.insert(
                k,
                Stream {
                    node: s.node.unwrap_or_default(),
                    hwm: s.hwm.unwrap_or(0),
                    last_receive_ns: s.last_receive_ns.unwrap_or(0),
                    last_record_ts: s.last_record_ts.unwrap_or_default(),
                },
            );
        }
        Ok(out)
    }
}

fn json_kind(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Array(_) => "array",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Bool(_) => "bool",
        _ => "number",
    }
}

/// One accepted push: records already validated against the request.
pub struct Batch<'a> {
    pub node: &'a str,
    /// 32 hex chars.
    pub stream: &'a str,
    pub first_seq: u64,
    pub records: &'a [Bytes],
    /// Newest record `ts` of the batch.
    pub last_ts: String,
}

/// What a push did.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct PushResult {
    pub acked: u64,
    pub stored: usize,
    pub duplicates: usize,
    pub gap: bool,
}

/// An open store root; one process writes it at a time (an exclusive flock on the root).
pub struct Store {
    pub root: PathBuf,
    reserve: u64,
    pub now: Clock,
    _lock: File,
    pub st: State,
    /// Streams rebuilt from the files at open (restore), -1 when `state.json` existed.
    pub(crate) rebuilt: i64,
    /// Stops every push once the files may no longer match the state in memory (a state
    /// write that may have been published, a failed rollback); a restart recovers from disk.
    broken: Option<String>,
    pub crash: Option<Crash>,
}

impl Store {
    /// Creates or opens a store root, recovers the open day files to their committed length,
    /// and returns the writer.
    pub fn open(root: &Path, reserve: u64, now: Clock) -> Result<Store, String> {
        mkdir_all(root).map_err(|e| ioerr("mkdir", root, &e))?;
        let lock = File::open(root).map_err(|e| ioerr("open", root, &e))?;
        // Before any recovery or maintenance: a second writer must not touch the files.
        crate::disk::lock_exclusive(&lock).map_err(|e| {
            format!(
                "{}: another writer holds the store: {}",
                root.display(),
                errno_text(&e)
            )
        })?;
        let mut s = Store {
            root: root.to_path_buf(),
            reserve,
            now,
            _lock: lock,
            st: State::default(),
            rebuilt: -1,
            broken: None,
            crash: None,
        };
        s.load()?;
        Ok(s)
    }

    /// The number of streams rebuilt from the files at open, when a restore happened.
    pub fn rebuilt(&self) -> Option<i64> {
        (self.rebuilt >= 0).then_some(self.rebuilt)
    }

    fn crashes(&self, step: &str) -> bool {
        self.crash.as_ref().is_some_and(|c| c(step))
    }

    fn load(&mut self) -> Result<(), String> {
        let fp = self.root.join("FORMAT");
        match fs::read(&fp) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                write_atomic(&fp, FORMAT_LINE, None).map_err(|e| e.msg)?;
            }
            Err(e) => return Err(ioerr("open", &fp, &e)),
            Ok(b) if b != FORMAT_LINE => {
                return Err(format!(
                    "{}: unknown format {}",
                    fp.display(),
                    go_quote_bytes(b.trim_ascii())
                ));
            }
            Ok(_) => {}
        }
        self.rebuilt = -1;
        let sp = self.root.join("state.json");
        match fs::read(&sp) {
            Ok(b) => self.st = State::parse(&b)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                crate::restore::rebuild(self).map_err(|e| format!("restore: {e}"))?;
            }
            Err(e) => return Err(ioerr("open", &sp, &e)),
        }
        self.recover()
    }

    /// Truncates every open day file to its committed length; a day file without state and
    /// without manifest is truncated to zero (storage-format, "Commit"). A committed length
    /// whose file is missing or shorter is lost acknowledged data: open fails, nothing changes.
    fn recover(&mut self) -> Result<(), String> {
        let files = data_files(&self.root)?;
        let by_key: BTreeMap<String, &DataFile> = files.iter().map(|f| (f.key(), f)).collect();
        for (key, &n) in &self.st.days {
            let f = by_key.get(key);
            if f.is_some_and(|f| f.sealed) {
                continue;
            }
            let (node, day) = key.split_once('/').unwrap_or((key, ""));
            let Some(f) = f else {
                match exists(&manifest_path(&self.root, node, day)) {
                    Err(_) | Ok(true) => continue, // an error: the loop below fails on it
                    Ok(false) if n == 0 => continue,
                    Ok(false) => {
                        return Err(format!(
                            "{}: missing, {n} bytes committed",
                            data_path(&self.root, node, day).display()
                        ));
                    }
                }
            };
            let md = fs::metadata(&f.path).map_err(|e| ioerr("stat", &f.path, &e))?;
            if !md.is_file() || (md.len() as i64) < n {
                return Err(format!(
                    "{}: {} bytes, {n} committed",
                    f.path.display(),
                    md.len()
                ));
            }
        }
        for f in &files {
            let key = f.key();
            if f.sealed {
                self.st.days.remove(&key);
                continue;
            }
            let n = self.st.days.get(&key).copied().unwrap_or(0); // 0 for a file the state does not know
            truncate(&f.path, n)?;
            self.st.days.insert(key, n);
        }
        self.st.days.retain(|k, _| by_key.contains_key(k));
        // Recompressed files of a seal interrupted before its rename.
        for dir in fs::read_dir(&self.root).into_iter().flatten().flatten() {
            let dir = dir.path();
            if !dir.is_dir() {
                continue;
            }
            for e in fs::read_dir(&dir).into_iter().flatten().flatten() {
                if e.file_name().to_string_lossy().ends_with(".ndjson.zst.tmp") {
                    fs::remove_file(e.path()).map_err(|err| ioerr("remove", &e.path(), &err))?;
                }
            }
        }
        self.write_state().map_err(|e| e.msg)
    }

    /// The free space of the store's filesystem.
    pub fn free_bytes(&self) -> Result<u64, String> {
        crate::disk::free_bytes(&self.root).map_err(|e| ioerr("statfs", &self.root, &e))
    }

    /// Applies the hwm rules of ADR 0003 to a validated batch and commits what is new, with a
    /// `seq-gap` record when `first_seq > hwm + 1`. `Kind::Disk` means nothing was stored.
    pub fn push(&mut self, b: &Batch) -> Result<PushResult, PushErr> {
        if let Some(broken) = &self.broken {
            return Err(err(
                Kind::Other,
                format!("writer stopped until restart: {broken}"),
            ));
        }
        self.check_owner(b.node, b.stream)?;
        let old = self.st.streams.get(b.stream).cloned();
        let st = old.clone().unwrap_or_else(|| Stream {
            node: b.node.into(),
            ..Stream::default()
        });
        let n = b.records.len() as u64;
        let mut r = PushResult {
            acked: st.hwm,
            ..PushResult::default()
        };
        // seqs are consecutive: the duplicates are a prefix of the batch.
        if st.hwm >= b.first_seq {
            r.duplicates = (st.hwm - b.first_seq + 1).min(n) as usize;
        }
        r.stored = b.records.len() - r.duplicates;
        if r.stored == 0 {
            return Ok(r);
        }
        let now = (self.now)();
        let ts = crate::time::format_ts(now);
        let recv = format!(",\"recv_ts\":\"{ts}\"}}\n");
        let mut own_seq = self.st.own_seq;
        let mut gap = String::new();
        if b.first_seq > st.hwm.wrapping_add(1) {
            r.gap = true;
            own_seq += 1;
            gap = format!(
                "{{\"v\":1,\"ts\":\"{ts}\",\"level\":\"warn\",\"event\":\"seq-gap\",\"node\":{},\"service\":\"nisshi\",\"stream\":\"{GAP_STREAM}\",\"seq\":{own_seq},\"gap_stream\":{},\"from_seq\":{},\"to_seq\":{}{recv}",
                quote(b.node, Style::Marshal),
                quote(b.stream, Style::Marshal),
                st.hwm.wrapping_add(1),
                b.first_seq - 1
            );
        }
        let day = crate::time::day_of(now);
        let key = format!("{}/{day}", b.node);
        let committed = self.st.days.get(&key).copied().unwrap_or(0);
        // Even for a day still in days: a seal that failed after publishing its manifest
        // leaves it there.
        let last = sealed_through(&self.root, b.node).map_err(|e| err(Kind::Other, e))?;
        if day <= last {
            return Err(err(
                Kind::SealedDay,
                format!(
                    "receive day already sealed: {day}, {} sealed through {last}: store clock behind?",
                    b.node
                ),
            ));
        }
        let free = self.free_bytes().map_err(|e| err(Kind::Other, e))?;
        if free < self.reserve {
            return Err(err(
                Kind::Disk,
                format!("disk: {free} bytes free, reserve {}", self.reserve),
            ));
        }
        let path = data_path(&self.root, b.node, &day);
        let records = &b.records[r.duplicates..];
        let frame_len = match self.append_frame(b.node, &path, committed, |w| {
            for rec in records {
                w.write_all(&rec[..rec.len() - 1])?;
                w.write_all(recv.as_bytes())?;
            }
            w.write_all(gap.as_bytes())
        }) {
            Ok(n) => n,
            Err(e) => return Err(self.rollback(&path, committed, &e)),
        };
        if self.crashes("append") {
            return Err(err(Kind::Other, "crash after append".into()));
        }
        // Mutate a copy of the stream so a failed state write leaves memory as on disk.
        let mut ns = st.clone();
        ns.hwm = b.first_seq + n - 1;
        ns.last_receive_ns = now;
        if b.last_ts > ns.last_record_ts {
            ns.last_record_ts = b.last_ts.clone();
        }
        let prev_len = self.st.days.get(&key).copied();
        let prev_own = self.st.own_seq;
        self.st.days.insert(key.clone(), committed + frame_len);
        self.st.streams.insert(b.stream.into(), ns.clone());
        self.st.own_seq = own_seq;
        if let Err(e) = self.write_state() {
            if e.published {
                // state.json may already name the frame: keep both, accept nothing more until
                // a restart recovers from whichever state survived (old: frame cut; new: kept).
                self.broken = Some(e.msg.clone());
                return Err(err(Kind::Other, e.msg));
            }
            match prev_len {
                Some(n) => self.st.days.insert(key, n),
                None => self.st.days.remove(&key),
            };
            match old {
                Some(o) => self.st.streams.insert(b.stream.into(), o),
                None => self.st.streams.remove(b.stream),
            };
            self.st.own_seq = prev_own;
            return Err(self.rollback(&path, committed, &e.msg));
        }
        r.acked = ns.hwm;
        Ok(r)
    }

    /// Refuses a stream that another node pushed first, and nisshi's own stream (ADR 0014).
    pub fn check_owner(&self, node: &str, stream: &str) -> Result<(), PushErr> {
        if stream == GAP_STREAM || self.st.streams.get(stream).is_some_and(|s| s.node != node) {
            return Err(err(Kind::StreamOwner, "stream-owner".into()));
        }
        Ok(())
    }

    /// Cuts a day file back to its committed length after a failed commit. If that fails too,
    /// the file's tail is unknown: the writer stops until a restart recovers it.
    fn rollback(&mut self, path: &Path, committed: i64, cause: &str) -> PushErr {
        let res = if self.crashes("truncate") {
            Err("truncate failed".to_string()) // before the truncate: the uncommitted tail stays
        } else {
            match OpenOptions::new().write(true).open(path) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                r => r
                    .and_then(|f| f.set_len(committed as u64))
                    .map_err(|e| ioerr("truncate", path, &e)),
            }
        };
        if let Err(e) = res {
            let broken = format!("rollback of {}: {e}", path.display());
            self.broken = Some(broken.clone());
            return err(Kind::Disk, format!("disk: {cause}; {broken}"));
        }
        err(Kind::Disk, format!("disk: {cause}"))
    }

    /// Writes one zstd frame of what `write` writes at the committed length of the day file,
    /// fsyncs it and returns the frame's length.
    fn append_frame(
        &self,
        node: &str,
        path: &Path,
        committed: i64,
        write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
    ) -> Result<i64, String> {
        let dir = self.root.join(node);
        mkdir_all(&dir).map_err(|e| ioerr("mkdir", &dir, &e))?;
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o640)
            .open(path)
            .map_err(|e| ioerr("open", path, &e))?;
        let mut w = OffsetWriter {
            file: &f,
            off: committed as u64,
            n: 0,
        };
        let wrote = (|| {
            let mut enc = zstd::stream::write::Encoder::new(&mut w, OPEN_LEVEL)?;
            enc.include_checksum(true)?;
            write(&mut enc)?;
            enc.finish().map(|_| ())
        })();
        wrote.map_err(|e| ioerr("write", path, &e))?;
        let n = w.n;
        f.sync_all().map_err(|e| ioerr("sync", path, &e))?;
        if committed == 0 {
            // Maybe a new file in a new directory: make both names durable.
            sync_dir(&dir)?;
            sync_dir(&self.root)?;
        }
        Ok(n as i64)
    }

    /// Commits `state.json`: tmp, fsync, rename, fsync dir.
    pub(crate) fn write_state(&self) -> Result<(), AtomicErr> {
        let crash = self.crash.as_deref();
        write_atomic(
            &self.root.join("state.json"),
            self.st.to_json().as_bytes(),
            crash,
        )
    }

    /// The stream states, keyed by hex stream id.
    pub fn streams(&self) -> BTreeMap<String, Stream> {
        self.st.streams.clone()
    }

    pub fn is_broken(&self) -> bool {
        self.broken.is_some()
    }
}

/// `node → hex SHA-256 of its newest manifest`, the chain heads (ADR 0007).
pub fn chain_heads(root: &Path) -> Result<BTreeMap<String, String>, String> {
    let mut heads = BTreeMap::new();
    for node in nodes(root)? {
        let ms = crate::seal::manifests(&root.join(&node))?;
        if let Some(m) = ms.last() {
            heads.insert(node, m.sha256.clone());
        }
    }
    Ok(heads)
}

struct OffsetWriter<'a> {
    file: &'a File,
    off: u64,
    n: u64,
}

impl Write for OffsetWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let k = self.file.write_at(buf, self.off + self.n)?;
        self.n += k as u64;
        Ok(k)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn mkdir_all(dir: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o750)
        .create(dir)
}

pub(crate) fn truncate(path: &Path, n: i64) -> Result<(), String> {
    let r = if n < 0 {
        Err(io::Error::from(io::ErrorKind::InvalidInput))
    } else {
        OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_len(n as u64))
    };
    r.map_err(|e| ioerr("truncate", path, &e))
}

pub(crate) fn sync_dir(dir: &Path) -> Result<(), String> {
    File::open(dir)
        .map_err(|e| ioerr("open", dir, &e))?
        .sync_all()
        .map_err(|e| ioerr("sync", dir, &e))
}

/// An atomic write's failure; `published` when the rename happened and only the directory
/// sync after it failed: the new file may or may not survive a power loss.
#[derive(Debug)]
pub struct AtomicErr {
    pub published: bool,
    pub msg: String,
}

pub(crate) fn write_atomic(
    path: &Path,
    b: &[u8],
    crash: Option<&(dyn Fn(&str) -> bool + Send)>,
) -> Result<(), AtomicErr> {
    let fail = |msg| AtomicErr {
        published: false,
        msg,
    };
    let published = |msg: String| AtomicErr {
        published: true,
        msg: format!("published, directory sync failed: {msg}"),
    };
    let crashes = |step: &str| crash.is_some_and(|c| c(step));
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o640)
        .open(&tmp)
        .map_err(|e| fail(ioerr("open", &tmp, &e)))?;
    f.write_all(b).map_err(|e| fail(ioerr("write", &tmp, &e)))?;
    f.sync_all().map_err(|e| fail(ioerr("sync", &tmp, &e)))?;
    drop(f);
    if crashes("tmp") {
        return Err(fail("crash after tmp".into()));
    }
    fs::rename(&tmp, path).map_err(|e| {
        fail(format!(
            "rename {} {}: {}",
            tmp.display(),
            path.display(),
            errno_text(&e)
        ))
    })?;
    if crashes("rename") {
        return Err(published("crash after rename".into()));
    }
    sync_dir(path.parent().unwrap_or(Path::new("."))).map_err(published)
}

pub fn data_path(root: &Path, node: &str, day: &str) -> PathBuf {
    root.join(node).join(format!("{day}.ndjson.zst"))
}

pub fn manifest_path(root: &Path, node: &str, day: &str) -> PathBuf {
    root.join(node).join(format!("{day}.manifest.json"))
}

pub(crate) fn exists(path: &Path) -> Result<bool, String> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(ioerr("stat", path, &e)),
    }
}

/// A directory's entries sorted by name, as Go's `os.ReadDir`: `(name, is a directory)`.
pub(crate) fn read_dir_sorted(dir: &Path) -> Result<Vec<(String, bool)>, String> {
    let rd = fs::read_dir(dir).map_err(|e| ioerr("open", dir, &e))?;
    let mut out = Vec::new();
    for e in rd {
        let e = e.map_err(|e| ioerr("readdirent", dir, &e))?;
        let is_dir = e.file_type().is_ok_and(|t| t.is_dir());
        out.push((e.file_name().to_string_lossy().into_owned(), is_dir));
    }
    out.sort();
    Ok(out)
}

/// The node directories of a root (storage-format, "Layout"). Anything else at the root
/// (FORMAT, state.json, a backup's LAST_RUN or *.tmp, lost+found) is not node data.
pub fn nodes(root: &Path) -> Result<Vec<String>, String> {
    Ok(read_dir_sorted(root)?
        .into_iter()
        .filter(|(n, d)| *d && crate::logschema::is_node(n))
        .map(|(n, _)| n)
        .collect())
}

#[derive(Debug, Clone)]
pub struct DataFile {
    pub node: String,
    pub day: String,
    pub path: PathBuf,
    /// Its manifest exists.
    pub sealed: bool,
}

impl DataFile {
    pub fn key(&self) -> String {
        format!("{}/{}", self.node, self.day)
    }
}

/// Every data file, sorted by node then day.
pub fn data_files(root: &Path) -> Result<Vec<DataFile>, String> {
    let mut out = Vec::new();
    for node in nodes(root)? {
        for (name, _) in read_dir_sorted(&root.join(&node))? {
            if let Some(day) = name.strip_suffix(".ndjson.zst") {
                let sealed = exists(&manifest_path(root, &node, day))?;
                out.push(DataFile {
                    path: data_path(root, &node, day),
                    node: node.clone(),
                    day: day.into(),
                    sealed,
                });
            }
        }
    }
    Ok(out)
}

/// The newest sealed day of a node, `""` when none.
fn sealed_through(root: &Path, node: &str) -> Result<String, String> {
    let dir = root.join(node);
    match fs::read_dir(&dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(String::new()),
        Err(e) => return Err(ioerr("open", &dir, &e)),
        Ok(_) => {}
    }
    Ok(read_dir_sorted(&dir)?
        .into_iter()
        .filter_map(|(n, _)| n.strip_suffix(".manifest.json").map(str::to_string))
        .max()
        .unwrap_or_default())
}

/// Go's `%q` of bytes (error messages).
pub fn go_quote_bytes(b: &[u8]) -> String {
    let mut out = String::from("\"");
    let mut rest = b;
    while !rest.is_empty() {
        let (c, len) = match std::str::from_utf8(&rest[..rest.len().min(4)]) {
            Ok(s) => (s.chars().next(), s.chars().next().map_or(1, char::len_utf8)),
            Err(e) if e.valid_up_to() > 0 => {
                let s = std::str::from_utf8(&rest[..e.valid_up_to()]).unwrap();
                let c = s.chars().next().unwrap();
                (Some(c), c.len_utf8())
            }
            Err(_) => (None, 1),
        };
        match c {
            Some('"') => out.push_str("\\\""),
            Some('\\') => out.push_str("\\\\"),
            Some('\u{7}') => out.push_str("\\a"),
            Some('\u{8}') => out.push_str("\\b"),
            Some('\u{c}') => out.push_str("\\f"),
            Some('\n') => out.push_str("\\n"),
            Some('\r') => out.push_str("\\r"),
            Some('\t') => out.push_str("\\t"),
            Some('\u{b}') => out.push_str("\\v"),
            Some(c) if (c as u32) < 0x20 || c == '\u{7f}' => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            Some(c) => out.push(c),
            None => out.push_str(&format!("\\x{:02x}", rest[0])),
        }
        rest = &rest[len..];
    }
    out.push('"');
    out
}

/// SHA-256 in lowercase hex.
pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Decodes a data file (a concatenation of zstd frames) and calls `f` on each line without
/// its newline; returns the SHA-256 of the decompressed bytes and the line count. Lenient (an
/// open day being written): a truncated last frame ends the read (storage-format, "Data
/// files"). Strict (a seal, a sealed day): it is an error, and so is a last line without its
/// newline.
pub fn read_lines(
    path: &Path,
    strict: bool,
    f: impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(String, usize), String> {
    let file = File::open(path).map_err(|e| ioerr("open", path, &e))?;
    read_from(file, strict, f)
}

pub fn read_from(
    r: impl Read,
    strict: bool,
    mut f: impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(String, usize), String> {
    let mut h = Sha256::new();
    let mut br = BufReader::with_capacity(1 << 16, r);
    // libzstd reads an empty input as an incomplete frame; Go's decoder as no frame at all.
    if br.fill_buf().map_err(|e| errno_text(&e))?.is_empty() {
        return Ok((hex(&h.finalize()), 0));
    }
    let mut dec = zstd::stream::read::Decoder::with_buffer(br).map_err(|e| e.to_string())?;
    dec.window_log_max(WINDOW_LOG).map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; 1 << 20];
    let mut rest: Vec<u8> = Vec::new();
    let mut n = 0usize;
    loop {
        let (k, eof) = match dec.read(&mut buf) {
            Ok(0) => (0, true),
            Ok(k) => (k, false),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && !strict => (0, true),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err("unexpected EOF".into());
            }
            Err(e) => return Err(e.to_string()),
        };
        h.update(&buf[..k]);
        rest.extend_from_slice(&buf[..k]);
        let mut start = 0;
        while let Some(i) = rest[start..].iter().position(|&c| c == b'\n') {
            if i > MAX_LINE {
                return Err(format!("line {}: over {MAX_LINE} bytes", n + 1));
            }
            f(&rest[start..start + i])?;
            n += 1;
            start += i + 1;
        }
        rest.drain(..start);
        if rest.len() > MAX_LINE {
            return Err(format!(
                "line {}: over {MAX_LINE} bytes without a newline",
                n + 1
            ));
        }
        if eof {
            break;
        }
    }
    if strict && !rest.is_empty() {
        return Err(format!("line {}: no final newline", n + 1));
    }
    Ok((hex(&h.finalize()), n))
}

/// What the store reads back from a stored record.
#[derive(Debug, Default, Clone)]
pub struct Envelope {
    pub ts: String,
    pub event: String,
    pub service: String,
    pub stream: String,
    pub node: String,
    pub seq: Option<Vec<u8>>,
}

impl Envelope {
    /// Go's `strconv.ParseUint(seq, 10, 64)`.
    pub fn seq(&self) -> Result<u64, String> {
        let raw = self.seq.as_deref().unwrap_or_default();
        let q = quote(&crate::json::lossy(raw), Style::Slog);
        if raw.is_empty() || !raw.iter().all(u8::is_ascii_digit) {
            return Err(format!("strconv.ParseUint: parsing {q}: invalid syntax"));
        }
        std::str::from_utf8(raw)
            .unwrap()
            .parse()
            .map_err(|_| format!("strconv.ParseUint: parsing {q}: value out of range"))
    }
}

/// The envelope members of a stored line, by their exact names: `TS` or `Seq` are free
/// service fields (log-schema). Read as Go's `json.Unmarshal` into a map would.
pub fn parse_envelope(line: &[u8]) -> Result<Envelope, String> {
    let mut e = Envelope::default();
    let Some(m) = crate::json::object_map(line)? else {
        return Ok(e);
    };
    for (k, v) in m {
        let slot = match k.as_str() {
            "ts" => &mut e.ts,
            "event" => &mut e.event,
            "service" => &mut e.service,
            "stream" => &mut e.stream,
            "node" => &mut e.node,
            "seq" => {
                e.seq = Some(v.to_vec());
                continue;
            }
            _ => continue,
        };
        if v == b"null" {
            continue;
        }
        *slot = crate::json::unquote(v).ok_or_else(|| {
            format!(
                "{k}: json: cannot unmarshal {} into Go value of type string",
                crate::json::kind(v)
            )
        })?;
    }
    Ok(e)
}

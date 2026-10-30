//! Sealing, compaction and retention (ADR 0006), manifests and the hash chain (ADR 0007,
//! storage-format "Manifest"), `nisshi verify`.

use crate::json::{Style, quote};
use crate::store::{
    DataFile, Store, data_files, errno_text, ioerr, manifest_path, nodes, parse_envelope,
    read_dir_sorted, read_lines, write_atomic,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// Go nisshi: klauspost `SpeedBetterCompression`.
const SEAL_LEVEL: i32 = 7;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StreamRange {
    #[serde(default)]
    pub stream: String,
    #[serde(default)]
    pub first_seq: u64,
    #[serde(default)]
    pub last_seq: u64,
}

/// `<day>.manifest.json`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Manifest {
    pub v: i64,
    pub node: String,
    pub day: String,
    pub records: i64,
    pub uncompressed_bytes: i64,
    pub content_sha256: String,
    pub prev_day: Option<String>,
    pub prev_manifest_sha256: Option<String>,
    pub min_ts: String,
    pub max_ts: String,
    pub services: BTreeMap<String, i64>,
    /// `null` for a day without records.
    pub streams: Option<Vec<StreamRange>>,
    pub sealed_at: String,
    /// Of the file's bytes.
    pub sha256: String,
    pub path: PathBuf,
}

impl Manifest {
    /// As Go's `json.Marshal` wrote it, byte for byte.
    pub fn to_json(&self) -> String {
        let q = |s: &str| quote(s, Style::Marshal);
        let opt = |s: &Option<String>| s.as_deref().map_or("null".to_string(), q);
        let services: Vec<String> = self
            .services
            .iter()
            .map(|(k, v)| format!("{}:{v}", q(k)))
            .collect();
        let streams = match &self.streams {
            None => "null".to_string(),
            Some(list) => format!(
                "[{}]",
                list.iter()
                    .map(|r| format!(
                        "{{\"stream\":{},\"first_seq\":{},\"last_seq\":{}}}",
                        q(&r.stream),
                        r.first_seq,
                        r.last_seq
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        };
        format!(
            "{{\"v\":{},\"node\":{},\"day\":{},\"records\":{},\"uncompressed_bytes\":{},\"content_sha256\":{},\"prev_day\":{},\"prev_manifest_sha256\":{},\"min_ts\":{},\"max_ts\":{},\"services\":{{{}}},\"streams\":{streams},\"sealed_at\":{}}}",
            self.v,
            q(&self.node),
            q(&self.day),
            self.records,
            self.uncompressed_bytes,
            q(&self.content_sha256),
            opt(&self.prev_day),
            opt(&self.prev_manifest_sha256),
            q(&self.min_ts),
            q(&self.max_ts),
            services.join(","),
            q(&self.sealed_at)
        )
    }

    fn parse(b: &[u8], path: &Path) -> Result<Manifest, String> {
        #[derive(Deserialize)]
        struct In {
            v: Option<i64>,
            node: Option<String>,
            day: Option<String>,
            records: Option<i64>,
            uncompressed_bytes: Option<i64>,
            content_sha256: Option<String>,
            prev_day: Option<String>,
            prev_manifest_sha256: Option<String>,
            min_ts: Option<String>,
            max_ts: Option<String>,
            services: Option<BTreeMap<String, Option<i64>>>,
            streams: Option<Vec<StreamRange>>,
            sealed_at: Option<String>,
        }
        let m: In = serde_json::from_slice(b).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Manifest {
            v: m.v.unwrap_or(0),
            node: m.node.unwrap_or_default(),
            day: m.day.unwrap_or_default(),
            records: m.records.unwrap_or(0),
            uncompressed_bytes: m.uncompressed_bytes.unwrap_or(0),
            content_sha256: m.content_sha256.unwrap_or_default(),
            prev_day: m.prev_day,
            prev_manifest_sha256: m.prev_manifest_sha256,
            min_ts: m.min_ts.unwrap_or_default(),
            max_ts: m.max_ts.unwrap_or_default(),
            services: m
                .services
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or(0)))
                .collect(),
            streams: m.streams,
            sealed_at: m.sealed_at.unwrap_or_default(),
            sha256: crate::store::hex(&Sha256::digest(b)),
            path: path.to_path_buf(),
        })
    }
}

/// A node directory's manifests sorted by day.
pub fn manifests(dir: &Path) -> Result<Vec<Manifest>, String> {
    let mut out = Vec::new();
    for (name, _) in read_dir_sorted(dir)? {
        if !name.ends_with(".manifest.json") {
            continue;
        }
        let p = dir.join(&name);
        let b = fs::read(&p).map_err(|e| ioerr("open", &p, &e))?;
        out.push(Manifest::parse(&b, &p)?);
    }
    out.sort_by(|a, b| a.day.cmp(&b.day));
    Ok(out)
}

impl Store {
    /// Seals every open day file of a past UTC day (ADR 0006), oldest first so the chain links
    /// in order. Returns the days sealed and the errors met; a file whose seal fails stays as
    /// it is, and so do the later days of its node: sealing them would link past it.
    pub fn seal_past(&mut self) -> (Vec<String>, Vec<String>) {
        let today = crate::time::day_of((self.now)());
        let files = match data_files(&self.root) {
            Ok(f) => f,
            Err(e) => return (Vec::new(), vec![e]),
        };
        let (mut sealed, mut errs) = (Vec::new(), Vec::new());
        let mut failed: Vec<String> = Vec::new();
        for f in files {
            if f.sealed || f.day >= today {
                continue;
            }
            if failed.contains(&f.node) {
                errs.push(format!(
                    "{}/{}: deferred, an earlier day of the node failed",
                    f.node, f.day
                ));
                continue;
            }
            match self.seal(&f) {
                Ok(()) => sealed.push(f.key()),
                Err(e) => {
                    errs.push(format!("{}/{}: {e}", f.node, f.day));
                    failed.push(f.node.clone());
                }
            }
        }
        (sealed, errs)
    }

    fn seal(&mut self, f: &DataFile) -> Result<(), String> {
        let key = f.key();
        if let Some(&n) = self.st.days.get(&key) {
            crate::store::truncate(&f.path, n)?; // never seal an uncommitted tail
        }
        let mut tmp = f.path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let _cleanup = RemoveOnDrop(&tmp);
        let out = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o640)
            .open(&tmp)
            .map_err(|e| ioerr("open", &tmp, &e))?;
        let mut enc = zstd::stream::write::Encoder::new(out, SEAL_LEVEL)
            .and_then(|mut e| {
                e.include_checksum(true)?;
                e.window_log(crate::store::WINDOW_LOG)?;
                Ok(e)
            })
            .map_err(|e| e.to_string())?;
        let mut m = Manifest {
            v: 1,
            node: f.node.clone(),
            day: f.day.clone(),
            ..Manifest::default()
        };
        let mut ranges: BTreeMap<String, StreamRange> = BTreeMap::new();
        let (sha, n) = read_lines(&f.path, true, |line| {
            let e = parse_envelope(line).map_err(|e| format!("record {}: {e}", m.records + 1))?;
            let seq = e
                .seq()
                .map_err(|e| format!("record {}: seq: {e}", m.records + 1))?;
            m.records += 1;
            m.uncompressed_bytes += line.len() as i64 + 1;
            if m.min_ts.is_empty() || e.ts < m.min_ts {
                m.min_ts = e.ts.clone();
            }
            if e.ts > m.max_ts {
                m.max_ts = e.ts.clone();
            }
            *m.services.entry(e.service.clone()).or_insert(0) += 1;
            ranges
                .entry(e.stream.clone())
                .and_modify(|r| {
                    r.first_seq = r.first_seq.min(seq);
                    r.last_seq = r.last_seq.max(seq);
                })
                .or_insert(StreamRange {
                    stream: e.stream,
                    first_seq: seq,
                    last_seq: seq,
                });
            enc.write_all(line)
                .and_then(|()| enc.write_all(b"\n"))
                .map_err(|e| ioerr("write", &tmp, &e))
        })?;
        if n as i64 != m.records {
            return Err("record count mismatch".into());
        }
        let out = enc.finish().map_err(|e| ioerr("write", &tmp, &e))?;
        out.sync_all().map_err(|e| ioerr("sync", &tmp, &e))?;
        drop(out);
        // Decompress the recompressed file and compare before replacing the original.
        let (sha2, _) = read_lines(&tmp, true, |_| Ok(()))?;
        if sha2 != sha {
            return Err(format!("recompressed content hash {sha2} != {sha}"));
        }
        m.content_sha256 = sha;
        if !ranges.is_empty() {
            m.streams = Some(ranges.into_values().collect());
        }
        m.sealed_at = crate::time::format_ts((self.now)());
        let prev = manifests(&self.root.join(&f.node))?;
        if let Some(p) = prev.iter().rev().find(|p| p.day < f.day) {
            m.prev_day = Some(p.day.clone());
            m.prev_manifest_sha256 = Some(p.sha256.clone());
        }
        // The manifest first: content_sha256 is of the decompressed bytes, so the original file
        // matches it as well as the recompressed one, and a crash at any step below leaves a
        // sealed day whose file matches its manifest. Recovery never cuts it.
        write_atomic(
            &manifest_path(&self.root, &f.node, &f.day),
            m.to_json().as_bytes(),
            self.crash.as_deref(),
        )
        .map_err(|e| e.msg)?;
        fs::rename(&tmp, &f.path).map_err(|e| {
            format!(
                "rename {} {}: {}",
                tmp.display(),
                f.path.display(),
                errno_text(&e)
            )
        })?;
        if self.crash.as_ref().is_some_and(|c| c("seal-rename")) {
            return Err("crash after rename".into());
        }
        crate::store::sync_dir(f.path.parent().unwrap())?;
        self.st.days.remove(&key);
        self.write_state().map_err(|e| e.msg)
    }

    /// Deletes the data files of sealed days older than `days` (0 keeps everything), keeping
    /// their manifests (ADR 0006). Returns the days deleted.
    pub fn retain(&mut self, days: i64) -> (Vec<String>, Option<String>) {
        let mut deleted = Vec::new();
        if days <= 0 {
            return (deleted, None);
        }
        let today = crate::time::day_number((self.now)());
        // A cut-off before year 0 keeps everything, as Go's (string) comparison does.
        let cutoff = crate::time::format_day(today - days.min(1 << 40));
        let files = match data_files(&self.root) {
            Ok(f) => f,
            Err(e) => return (deleted, Some(e)),
        };
        for f in files {
            if !f.sealed || f.day >= cutoff {
                continue;
            }
            if let Err(e) = fs::remove_file(&f.path) {
                return (deleted, Some(ioerr("remove", &f.path, &e)));
            }
            deleted.push(f.key());
        }
        (deleted, None)
    }
}

struct RemoveOnDrop<'a>(&'a Path);

impl Drop for RemoveOnDrop<'_> {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0);
    }
}

/// Checks every manifest's content hash (when its data file exists), every chain link and
/// `(stream, seq)` uniqueness across the store (ADR 0007). Problems are written to `w`; the
/// error is set when any was found.
pub fn verify(root: &Path, w: &mut dyn Write) -> Result<(), String> {
    let mut problems = 0;
    let mut bad = |msg: String| {
        problems += 1;
        let _ = writeln!(w, "{msg}");
    };
    if fs::read(root.join("FORMAT")).ok().as_deref() != Some(crate::store::FORMAT_LINE) {
        bad(format!("{}: FORMAT missing or unknown", root.display()));
    }
    let mut seqs: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    // data path → what one read of it found
    let mut contents: BTreeMap<PathBuf, Result<(String, usize), String>> = BTreeMap::new();
    let files = data_files(root).unwrap_or_else(|e| {
        bad(e);
        Vec::new()
    });
    for f in &files {
        let r = read_lines(&f.path, f.sealed, |line| {
            let e = parse_envelope(line)?;
            let seq = e.seq()?;
            seqs.entry(e.stream).or_default().push(seq);
            Ok(())
        });
        if let Err(e) = &r {
            bad(format!("{}: {e}", f.path.display()));
        }
        contents.insert(f.path.clone(), r);
    }
    for (stream, list) in &mut seqs {
        list.sort_unstable();
        for i in 1..list.len() {
            if list[i] == list[i - 1] {
                bad(format!("stream {stream}: seq {} stored twice", list[i]));
            }
        }
    }
    let nodes = nodes(root).unwrap_or_else(|e| {
        bad(e);
        Vec::new()
    });
    for node in nodes {
        let ms = match manifests(&root.join(&node)) {
            Ok(ms) => ms,
            Err(e) => {
                bad(e);
                continue;
            }
        };
        for (i, m) in ms.iter().enumerate() {
            let file = m.path.file_name().unwrap().to_string_lossy().into_owned();
            let day = file.strip_suffix(".manifest.json").unwrap_or(&file);
            if m.node != node || m.v != 1 || m.day != day || !crate::time::valid_day(day) {
                bad(format!(
                    "{}: node, version or day mismatch, or not a calendar day",
                    m.path.display()
                ));
            }
            // A missing data file is retention (storage-format); any other error is a problem.
            let data = crate::store::data_path(root, &node, day);
            match contents.get(&data) {
                Some(Ok((sha, _))) if *sha != m.content_sha256 => bad(format!(
                    "{}: content_sha256 {}, data file {sha}",
                    m.path.display(),
                    m.content_sha256
                )),
                Some(Ok((_, n))) if *n as i64 != m.records => bad(format!(
                    "{}: records {}, data file {n}",
                    m.path.display(),
                    m.records
                )),
                Some(_) => {}
                None => {
                    if let Err(e) = crate::store::exists(&data) {
                        bad(e);
                    }
                }
            }
            if i == 0 {
                if m.prev_day.is_some() || m.prev_manifest_sha256.is_some() {
                    bad(format!(
                        "{}: first manifest with a prev link",
                        m.path.display()
                    ));
                }
                continue;
            }
            let p = &ms[i - 1];
            if m.prev_day.as_deref() != Some(p.day.as_str())
                || m.prev_manifest_sha256.as_deref() != Some(p.sha256.as_str())
            {
                bad(format!(
                    "{}: chain link does not match {}",
                    m.path.display(),
                    p.path.display()
                ));
            }
        }
    }
    if problems > 0 {
        return Err(format!("verify: {problems} problem(s)"));
    }
    Ok(())
}

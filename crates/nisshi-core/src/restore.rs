//! Restore at open when `state.json` is absent (storage-format, "Restore"; ADR 0005
//! amendment): each stream's hwm is its highest seq, its owner the node directory it was found
//! in (ADR 0014), nisshi's own seq the highest `seq-gap` seq. Sealed days are checked against
//! their manifests; an open day must end on a complete frame and stays open.

use crate::seal::StreamRange;
use crate::store::{
    GAP_STREAM, Store, Stream, data_path, exists, ioerr, manifest_path, parse_envelope,
    read_dir_sorted, read_from, read_lines,
};
use std::collections::BTreeMap;

type Ranges = BTreeMap<String, StreamRange>;

pub(crate) fn rebuild(s: &mut Store) -> Result<(), String> {
    let root = s.root.clone();
    let mut found = false;
    let mut owner: BTreeMap<String, String> = BTreeMap::new();
    let mut add = |s: &mut Store, node: &str, ranges: &Ranges| -> Result<(), String> {
        for (stream, r) in ranges {
            if stream == GAP_STREAM {
                s.st.own_seq = s.st.own_seq.max(r.last_seq);
                continue;
            }
            if let Some(o) = owner.get(stream)
                && o != node
            {
                return Err(format!("stream {stream} found in {o} and {node}"));
            }
            owner.insert(stream.clone(), node.into());
            let st =
                s.st.streams
                    .entry(stream.clone())
                    .or_insert_with(|| Stream {
                        node: node.into(),
                        ..Stream::default()
                    });
            st.hwm = st.hwm.max(r.last_seq);
        }
        Ok(())
    };
    for node in crate::store::nodes(&root)? {
        let dir = root.join(&node);
        for m in crate::seal::manifests(&dir)? {
            found = true;
            let file = m.path.file_name().unwrap().to_string_lossy().into_owned();
            let day = file.strip_suffix(".manifest.json").unwrap_or(&file);
            if m.day != day || m.node != node || !crate::time::valid_day(day) {
                return Err(format!(
                    "{}: day or node mismatch, or not a calendar day",
                    m.path.display()
                ));
            }
            let mut ranges = Ranges::new();
            for r in m.streams.iter().flatten() {
                ranges.insert(r.stream.clone(), r.clone());
            }
            let data = data_path(&root, &node, day);
            if exists(&data)? {
                let mut got = Ranges::new();
                let (sha, n) = read_lines(&data, true, scan_ranges(&mut got))
                    .map_err(|e| format!("{}: {e}", data.display()))?;
                if sha != m.content_sha256 || n as i64 != m.records || got != ranges {
                    return Err(format!(
                        "{}: data does not match its manifest",
                        data.display()
                    ));
                }
            }
            add(s, &node, &ranges)?;
        }
        for (name, _) in read_dir_sorted(&dir)? {
            let path = dir.join(&name);
            if name.ends_with(".ndjson.zst.partial") {
                return Err(format!(
                    "{}: rename each .partial to .ndjson.zst before starting (storage-format, \"Restore\")",
                    path.display()
                ));
            }
            let Some(day) = name.strip_suffix(".ndjson.zst") else {
                continue;
            };
            if exists(&manifest_path(&root, &node, day))? {
                continue;
            }
            found = true;
            let b = std::fs::read(&path).map_err(|e| ioerr("open", &path, &e))?;
            whole_frames(&b).map_err(|e| format!("{}: {e}", path.display()))?;
            let mut ranges = Ranges::new();
            read_from(&b[..], true, scan_ranges(&mut ranges))
                .map_err(|e| format!("{}: {e}", path.display()))?;
            add(s, &node, &ranges)?;
            s.st.days.insert(format!("{node}/{day}"), b.len() as i64);
        }
    }
    if found {
        s.rebuilt = s.st.streams.len() as i64;
    }
    Ok(())
}

/// Collects the seq range of each stream of the lines it is called on.
pub(crate) fn scan_ranges(ranges: &mut Ranges) -> impl FnMut(&[u8]) -> Result<(), String> + '_ {
    |line| {
        let e = parse_envelope(line)?;
        let seq = e.seq().map_err(|err| format!("seq: {err}"))?;
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
        Ok(())
    }
}

/// Checks that `b` is made of whole zstd frames only (RFC 8878 frame and block headers,
/// skippable frames included). A tail cut short is refused like any corruption: a corrupt
/// block size can run past the end too, and cutting there would drop the valid frames after
/// it. The backup copies whole frames, so a legitimate restore never ends mid-frame.
pub(crate) fn whole_frames(b: &[u8]) -> Result<(), String> {
    let mut off = 0;
    while off < b.len() {
        match frame_len(&b[off..]) {
            Ok(n) => off += n,
            Err(FrameErr::Short) => {
                return Err(format!(
                    "incomplete zstd frame at byte {off}, {} bytes after it: the file must end on a complete frame (storage-format, \"Restore\")",
                    b.len() - off
                ));
            }
            Err(FrameErr::Corrupt(e)) => {
                return Err(format!(
                    "corrupt zstd frame at byte {off}, {} bytes after it: {e}",
                    b.len() - off
                ));
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum FrameErr {
    Short,
    Corrupt(String),
}

/// The length of the frame at the start of `b`, from its headers alone.
pub(crate) fn frame_len(b: &[u8]) -> Result<usize, FrameErr> {
    use FrameErr::{Corrupt, Short};
    if b.len() < 4 {
        return Err(Short);
    }
    if b[..4] != [0x28, 0xb5, 0x2f, 0xfd] {
        if b[1..4] != [0x2a, 0x4d, 0x18] || b[0] & 0xf0 != 0x50 {
            return Err(Corrupt("invalid input: magic number mismatch".into()));
        }
        if b.len() < 8 {
            return Err(Short);
        }
        let size = u32::from_le_bytes([b[4], b[5], b[6], b[7]]) as usize;
        return if 8 + size <= b.len() {
            Ok(8 + size)
        } else {
            Err(Short)
        };
    }
    let fhd = *b.get(4).ok_or(Short)?;
    let single = fhd & (1 << 5) != 0;
    let checksum = fhd & (1 << 2) != 0;
    if fhd & (1 << 3) != 0 {
        return Err(Corrupt("reserved bit set on frame header".into()));
    }
    let dict = match fhd & 3 {
        3 => 4,
        n => n as usize,
    };
    let fcs = match fhd >> 6 {
        0 => usize::from(single),
        v => 1 << v,
    };
    let mut off = 5 + usize::from(!single) + dict + fcs;
    if off > b.len() {
        return Err(Short);
    }
    loop {
        if off + 3 > b.len() {
            return Err(Short);
        }
        let bh = usize::from(b[off]) | usize::from(b[off + 1]) << 8 | usize::from(b[off + 2]) << 16;
        let mut size = bh >> 3;
        match bh >> 1 & 3 {
            3 => return Err(Corrupt("reserved block type".into())),
            _ if size > 128 << 10 => return Err(Corrupt(format!("block of {size} bytes"))),
            1 => size = 1, // RLE: one byte repeated
            _ => {}
        }
        off += 3 + size;
        if bh & 1 == 1 {
            break;
        }
    }
    if checksum {
        off += 4;
    }
    if off > b.len() {
        return Err(Short);
    }
    Ok(off)
}

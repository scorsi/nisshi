package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// Rebuilt reports a restore at Open: the number of streams rebuilt from the files, ok false
// when state.json existed or the root held no data.
func (s *Store) Rebuilt() (streams int, ok bool) { return s.rebuilt, s.rebuilt >= 0 }

// rebuild runs at Open when state.json is absent: a store restored from a backup of its sealed
// days and the committed prefix of its open days (storage-format, "Restore"). Each stream's hwm
// is its highest seq, its owner the node directory it was found in (ADR 0014), nisshi's own seq
// the highest seq-gap seq. Sealed days are checked against their manifests; an open day keeps
// its complete frames (a truncated tail is cut by recover) and stays open.
func (s *Store) rebuild() error {
	nodes, err := s.nodes()
	if err != nil {
		return err
	}
	found := false
	owner := map[string]string{}
	add := func(node string, ranges map[string]*StreamRange) error {
		for stream, r := range ranges {
			if stream == GapStream {
				s.st.OwnSeq = max(s.st.OwnSeq, r.LastSeq)
				continue
			}
			if o, ok := owner[stream]; ok && o != node {
				return fmt.Errorf("stream %s found in %s and %s", stream, o, node)
			}
			owner[stream] = node
			st := s.st.Streams[stream]
			if st == nil {
				st = &Stream{Node: node}
				s.st.Streams[stream] = st
			}
			st.Hwm = max(st.Hwm, r.LastSeq)
		}
		return nil
	}
	for _, node := range nodes {
		dir := filepath.Join(s.root, node)
		ms, err := manifests(dir)
		if err != nil {
			return err
		}
		for _, m := range ms {
			found = true
			day := strings.TrimSuffix(filepath.Base(m.path), ".manifest.json")
			if m.Day != day || m.Node != node {
				return fmt.Errorf("%s: day or node mismatch", m.path)
			}
			ranges := map[string]*StreamRange{}
			for _, r := range m.Streams {
				ranges[r.Stream] = &StreamRange{r.Stream, r.FirstSeq, r.LastSeq}
			}
			if ok, err := exists(s.dataPath(node, day)); err != nil {
				return err
			} else if ok {
				got := map[string]*StreamRange{}
				sha, n, err := readLines(s.dataPath(node, day), true, scanRanges(got))
				if err != nil {
					return fmt.Errorf("%s: %w", s.dataPath(node, day), err)
				}
				if sha != m.ContentSha256 || n != m.Records || !sameRanges(got, ranges) {
					return fmt.Errorf("%s: data does not match its manifest", s.dataPath(node, day))
				}
			}
			if err := add(node, ranges); err != nil {
				return err
			}
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".ndjson.zst.partial") {
				return fmt.Errorf("%s: rename each .partial to .ndjson.zst before starting (storage-format, \"Restore\")", filepath.Join(dir, e.Name()))
			}
			day, ok := strings.CutSuffix(e.Name(), ".ndjson.zst")
			if !ok {
				continue
			}
			if sealed, err := exists(s.manifestPath(node, day)); err != nil || sealed {
				if err != nil {
					return err
				}
				continue
			}
			found = true
			b, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				return err
			}
			n := completeFrames(b)
			ranges := map[string]*StreamRange{}
			if _, _, err := readFrom(bytes.NewReader(b[:n]), true, scanRanges(ranges)); err != nil {
				return fmt.Errorf("%s: %w", filepath.Join(dir, e.Name()), err)
			}
			if err := add(node, ranges); err != nil {
				return err
			}
			s.st.Days[node+"/"+day] = n
		}
	}
	if found {
		s.rebuilt = len(s.st.Streams)
	}
	return nil
}

// scanRanges collects the seq range of each stream of the lines it is called on.
func scanRanges(ranges map[string]*StreamRange) func([]byte) error {
	return func(line []byte) error {
		e, err := parseEnvelope(line)
		if err != nil {
			return err
		}
		seq, err := e.seq()
		if err != nil {
			return fmt.Errorf("seq: %w", err)
		}
		if r := ranges[e.Stream]; r == nil {
			ranges[e.Stream] = &StreamRange{e.Stream, seq, seq}
		} else {
			r.FirstSeq, r.LastSeq = min(r.FirstSeq, seq), max(r.LastSeq, seq)
		}
		return nil
	}
}

func sameRanges(a, b map[string]*StreamRange) bool {
	if len(a) != len(b) {
		return false
	}
	for k, r := range a {
		if o := b[k]; o == nil || *o != *r {
			return false
		}
	}
	return true
}

// completeFrames is the length of the longest prefix of b made of whole zstd frames (RFC 8878
// frame and block headers; skippable frames included).
func completeFrames(b []byte) int64 {
	off := 0
	for off < len(b) {
		n, err := frameLen(b[off:])
		if err != nil {
			break
		}
		off += n
	}
	return int64(off)
}

var errShort = errors.New("truncated frame")

func frameLen(b []byte) (int, error) {
	var h zstd.Header
	if err := h.Decode(b); err != nil {
		return 0, err
	}
	if h.Skippable {
		if n := h.HeaderSize + int(h.SkippableSize); n <= len(b) {
			return n, nil
		}
		return 0, errShort
	}
	off := h.HeaderSize
	for {
		if off+3 > len(b) {
			return 0, errShort
		}
		bh := int(b[off]) | int(b[off+1])<<8 | int(b[off+2])<<16
		size := bh >> 3
		if bh>>1&3 == 1 { // RLE: one byte repeated
			size = 1
		}
		off += 3 + size
		if bh&1 == 1 {
			break
		}
	}
	if h.HasCheckSum {
		off += 4
	}
	if off > len(b) {
		return 0, errShort
	}
	return off, nil
}

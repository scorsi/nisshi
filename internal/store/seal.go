package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// Manifest is `<day>.manifest.json` (storage-format, "Manifest").
type Manifest struct {
	V                 int            `json:"v"`
	Node              string         `json:"node"`
	Day               string         `json:"day"`
	Records           int            `json:"records"`
	UncompressedBytes int64          `json:"uncompressed_bytes"`
	ContentSha256     string         `json:"content_sha256"`
	PrevDay           *string        `json:"prev_day"`
	PrevManifestSha   *string        `json:"prev_manifest_sha256"`
	MinTs             string         `json:"min_ts"`
	MaxTs             string         `json:"max_ts"`
	Services          map[string]int `json:"services"`
	Streams           []StreamRange  `json:"streams"`
	SealedAt          string         `json:"sealed_at"`
	sha256            string         // of the file bytes
	path              string
}

// StreamRange is one entry of Manifest.Streams.
type StreamRange struct {
	Stream   string `json:"stream"`
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
}

type hasher struct{ hash.Hash }

func newHash() hasher        { return hasher{sha256.New()} }
func (h hasher) hex() string { return hex.EncodeToString(h.Sum(nil)) }

// manifests reads a node directory's manifests sorted by day.
func manifests(dir string) ([]Manifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Manifest
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".manifest.json") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var m Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		sum := sha256.Sum256(b)
		m.sha256 = hex.EncodeToString(sum[:])
		m.path = p
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Day < out[j].Day })
	return out, nil
}

// SealPast seals every open day file of a past UTC day (ADR 0006), oldest first so the chain
// links in order. It returns the days sealed and the errors met; a file whose seal fails
// stays as it is, and so do the later days of its node: sealing them would link past it.
func (s *Store) SealPast() (sealed []string, errs []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	today := s.now().UTC().Format("2006-01-02")
	files, err := s.dataFiles()
	if err != nil {
		return nil, []error{err}
	}
	failed := map[string]bool{}
	for _, f := range files {
		if f.sealed || f.day >= today {
			continue
		}
		if failed[f.node] {
			errs = append(errs, fmt.Errorf("%s/%s: deferred, an earlier day of the node failed", f.node, f.day))
			continue
		}
		if err := s.seal(f); err != nil {
			errs = append(errs, fmt.Errorf("%s/%s: %w", f.node, f.day, err))
			failed[f.node] = true
			continue
		}
		sealed = append(sealed, f.node+"/"+f.day)
	}
	return sealed, errs
}

func (s *Store) seal(f dataFile) error {
	key := f.node + "/" + f.day
	if n, ok := s.st.Days[key]; ok {
		if err := os.Truncate(f.path, n); err != nil { // never seal an uncommitted tail
			return err
		}
	}
	m := Manifest{V: 1, Node: f.node, Day: f.day, Services: map[string]int{}}
	ranges := map[string]*StreamRange{}
	tmp := f.path + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	enc, err := zstd.NewWriter(out, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(windowSize))
	if err != nil {
		out.Close()
		return err
	}
	sha, n, err := readLines(f.path, true, func(line []byte) error {
		e, err := parseEnvelope(line)
		if err != nil {
			return fmt.Errorf("record %d: %w", m.Records+1, err)
		}
		seq, err := e.seq()
		if err != nil {
			return fmt.Errorf("record %d: seq: %w", m.Records+1, err)
		}
		m.Records++
		m.UncompressedBytes += int64(len(line)) + 1
		if m.MinTs == "" || e.Ts < m.MinTs {
			m.MinTs = e.Ts
		}
		if e.Ts > m.MaxTs {
			m.MaxTs = e.Ts
		}
		m.Services[e.Service]++
		if r := ranges[e.Stream]; r == nil {
			ranges[e.Stream] = &StreamRange{e.Stream, seq, seq}
		} else {
			r.FirstSeq, r.LastSeq = min(r.FirstSeq, seq), max(r.LastSeq, seq)
		}
		_, err = enc.Write(append(line, '\n'))
		return err
	})
	if err != nil {
		enc.Close()
		out.Close()
		return err
	}
	if n != m.Records {
		return fmt.Errorf("record count mismatch")
	}
	if err := enc.Close(); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	// Decompress the recompressed file and compare before replacing the original.
	if sha2, _, err := readLines(tmp, true, func([]byte) error { return nil }); err != nil {
		return err
	} else if sha2 != sha {
		return fmt.Errorf("recompressed content hash %s != %s", sha2, sha)
	}
	m.ContentSha256 = sha
	for _, k := range sortedKeys(ranges) {
		m.Streams = append(m.Streams, *ranges[k])
	}
	m.SealedAt = s.now().UTC().Format(TsLayout)
	prev, err := manifests(filepath.Join(s.root, f.node))
	if err != nil {
		return err
	}
	for i := len(prev) - 1; i >= 0; i-- {
		if prev[i].Day < f.day {
			m.PrevDay, m.PrevManifestSha = &prev[i].Day, &prev[i].sha256
			break
		}
	}
	mb, err := json.Marshal(m)
	if err != nil {
		return err
	}
	// The manifest first: content_sha256 is of the decompressed bytes, so the original file
	// matches it as well as the recompressed one, and a crash at any step below leaves a sealed
	// day whose file matches its manifest. Recovery never applies a committed length to it.
	if err := writeAtomic(s.manifestPath(f.node, f.day), mb, s.crash); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	if s.crash != nil && s.crash("seal-rename") {
		return errors.New("crash after rename")
	}
	if err := syncDir(filepath.Dir(f.path)); err != nil {
		return err
	}
	delete(s.st.Days, key)
	return s.writeState()
}

// Retain deletes the data files of sealed days older than days (0 keeps everything) and keeps
// their manifests (ADR 0006). It returns the days deleted.
func (s *Store) Retain(days int) ([]string, error) {
	if days <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().UTC().AddDate(0, 0, -days).Format("2006-01-02")
	var deleted []string
	files, err := s.dataFiles()
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if !f.sealed || f.day >= cutoff {
			continue
		}
		if err := os.Remove(f.path); err != nil {
			return deleted, err
		}
		deleted = append(deleted, f.node+"/"+f.day)
	}
	return deleted, nil
}

// Verify checks every manifest's content hash (when its data file exists), every chain link
// and (stream, seq) uniqueness across the store (ADR 0007). Problems are written to w; the
// error is non-nil when any was found.
func Verify(root string, w io.Writer) error {
	s := &Store{root: root}
	problems := 0
	bad := func(format string, args ...any) { problems++; fmt.Fprintf(w, format+"\n", args...) }
	if b, err := os.ReadFile(filepath.Join(root, "FORMAT")); err != nil || string(b) != formatLine {
		bad("%s: FORMAT missing or unknown", root)
	}
	seqs := map[string][]uint64{}
	type content struct {
		sha string
		n   int
		err error
	}
	contents := map[string]content{} // data path -> what one read of it found
	files, err := s.dataFiles()
	if err != nil {
		bad("%v", err)
	}
	for _, f := range files {
		sha, n, err := readLines(f.path, f.sealed, func(line []byte) error {
			e, err := parseEnvelope(line)
			if err != nil {
				return err
			}
			seq, err := e.seq()
			if err != nil {
				return err
			}
			seqs[e.Stream] = append(seqs[e.Stream], seq)
			return nil
		})
		if err != nil {
			bad("%s: %v", f.path, err)
		}
		contents[f.path] = content{sha, n, err}
	}
	for stream, list := range seqs {
		sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })
		for i := 1; i < len(list); i++ {
			if list[i] == list[i-1] {
				bad("stream %s: seq %d stored twice", stream, list[i])
			}
		}
	}
	nodes, err := s.nodes()
	if err != nil {
		bad("%v", err)
	}
	for _, node := range nodes {
		ms, err := manifests(filepath.Join(root, node))
		if err != nil {
			bad("%v", err)
			continue
		}
		for i, m := range ms {
			day := strings.TrimSuffix(filepath.Base(m.path), ".manifest.json")
			if m.Node != node || m.V != 1 || m.Day != day {
				bad("%s: node, version or day mismatch", m.path)
			}
			// A missing data file is retention (storage-format); any other error is a problem.
			if c, ok := contents[s.dataPath(node, day)]; ok {
				if c.err == nil && c.sha != m.ContentSha256 {
					bad("%s: content_sha256 %s, data file %s", m.path, m.ContentSha256, c.sha)
				} else if c.err == nil && c.n != m.Records {
					bad("%s: records %d, data file %d", m.path, m.Records, c.n)
				}
			} else if _, err := exists(s.dataPath(node, day)); err != nil {
				bad("%v", err)
			}
			if i == 0 {
				if m.PrevDay != nil || m.PrevManifestSha != nil {
					bad("%s: first manifest with a prev link", m.path)
				}
				continue
			}
			p := ms[i-1]
			if m.PrevDay == nil || *m.PrevDay != p.Day || m.PrevManifestSha == nil || *m.PrevManifestSha != p.sha256 {
				bad("%s: chain link does not match %s", m.path, p.path)
			}
		}
	}
	if problems > 0 {
		return errors.New("verify: " + fmt.Sprint(problems) + " problem(s)")
	}
	return nil
}

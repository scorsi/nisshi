// Package store is the on-disk log store of docs/storage-format.md (`nisshi-store 1`): NDJSON
// records in zstd frames by node and UTC receive day, atomic commits through state.json,
// sealing with manifests and a hash chain, retention (ADR 0005, 0006, 0007).
package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	formatLine = "nisshi-store 1\n"
	// TsLayout is the schema's ts format: UTC, 9 fractional digits.
	TsLayout = "2006-01-02T15:04:05.000000000Z"
	// GapStream is the `stream` of records nisshi writes itself.
	GapStream  = "00000000000000000000000000000000"
	windowSize = 8 << 20
)

// ErrDisk is returned by Push when free space is below the reserve or a write failed: the
// batch was rolled back (HTTP 507).
var ErrDisk = errors.New("disk")

// Stream is the writer state of one agent stream.
type Stream struct {
	Node          string `json:"node"`
	Hwm           uint64 `json:"hwm"`
	LastReceiveNs int64  `json:"last_receive_ns"`
	LastRecordTs  string `json:"last_record_ts"`
}

type state struct {
	V       int                `json:"v"`
	Days    map[string]int64   `json:"days"` // "<node>/<day>" -> committed length of the open day file
	Streams map[string]*Stream `json:"streams"`
	OwnSeq  uint64             `json:"own_seq"`
}

// Store is an open store root; one process writes it at a time.
type Store struct {
	root    string
	reserve uint64
	now     func() time.Time
	mu      sync.Mutex
	st      state
	enc     *zstd.Encoder
	// crash, set by tests, aborts a commit after the named step (ADR 0011).
	crash func(step string) bool
}

// Batch is one accepted push: records already validated against the request.
type Batch struct {
	Node     string
	Stream   string // 32 hex chars
	FirstSeq uint64
	Records  [][]byte
	LastTs   string // newest record ts of the batch
}

// Result is what a push did.
type Result struct {
	Acked      uint64
	Stored     int
	Duplicates int
	Gap        bool
}

// Open creates or opens a store root, recovers the open day files to their committed length,
// and returns the writer. now is the store's clock (fake in tests).
func Open(root string, reserve uint64, now func() time.Time) (*Store, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	fp := filepath.Join(root, "FORMAT")
	if b, err := os.ReadFile(fp); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(fp, []byte(formatLine), 0o640); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if string(b) != formatLine {
		return nil, fmt.Errorf("%s: unknown format %q", fp, bytes.TrimSpace(b))
	}
	s := &Store{root: root, reserve: reserve, now: now, st: state{V: 1, Days: map[string]int64{}, Streams: map[string]*Stream{}}}
	if b, err := os.ReadFile(filepath.Join(root, "state.json")); err == nil {
		if err := json.Unmarshal(b, &s.st); err != nil {
			return nil, fmt.Errorf("state.json: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var err error
	if s.enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(windowSize)); err != nil {
		return nil, err
	}
	return s, s.recover()
}

// recover truncates every open day file to its committed length; a day file without state and
// without manifest is truncated to zero (storage-format, "Commit").
func (s *Store) recover() error {
	seen := map[string]bool{}
	for _, f := range s.dataFiles() {
		key := f.node + "/" + f.day
		seen[key] = true
		if f.sealed {
			delete(s.st.Days, key)
			continue
		}
		if err := os.Truncate(f.path, s.st.Days[key]); err != nil {
			return err
		}
		if _, ok := s.st.Days[key]; !ok {
			s.st.Days[key] = 0
		}
	}
	for key := range s.st.Days {
		if !seen[key] {
			delete(s.st.Days, key)
		}
	}
	return s.writeState()
}

type dataFile struct {
	node, day, path string
	sealed          bool // its manifest exists
}

// dataFiles lists every data file, sorted by node then day.
func (s *Store) dataFiles() []dataFile {
	var out []dataFile
	for _, node := range s.nodes() {
		entries, _ := os.ReadDir(filepath.Join(s.root, node))
		for _, e := range entries {
			if day, ok := strings.CutSuffix(e.Name(), ".ndjson.zst"); ok {
				_, err := os.Stat(s.manifestPath(node, day))
				out = append(out, dataFile{node, day, s.dataPath(node, day), err == nil})
			}
		}
	}
	return out
}

func (s *Store) nodes() []string {
	entries, _ := os.ReadDir(s.root)
	var nodes []string
	for _, e := range entries {
		if e.IsDir() {
			nodes = append(nodes, e.Name())
		}
	}
	return nodes
}

func (s *Store) dataPath(node, day string) string {
	return filepath.Join(s.root, node, day+".ndjson.zst")
}

func (s *Store) manifestPath(node, day string) string {
	return filepath.Join(s.root, node, day+".manifest.json")
}

// FreeBytes is the free space of the store's filesystem.
func (s *Store) FreeBytes() (uint64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(s.root, &fs); err != nil {
		return 0, err
	}
	return uint64(fs.Bavail) * uint64(fs.Bsize), nil
}

// Push applies the hwm rules of ADR 0003 to a validated batch and commits what is new, with a
// seq-gap record when first_seq > hwm + 1. ErrDisk means nothing was stored.
func (s *Store) Push(b Batch) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.st.Streams[b.Stream]
	if st == nil {
		st = &Stream{Node: b.Node}
	}
	var r Result
	r.Acked = st.Hwm
	var lines bytes.Buffer
	now := s.now().UTC()
	ts := now.Format(TsLayout)
	recv := []byte(`,"recv_ts":"` + ts + `"}` + "\n")
	for i, rec := range b.Records {
		if b.FirstSeq+uint64(i) <= st.Hwm {
			r.Duplicates++
			continue
		}
		lines.Write(rec[:len(rec)-1])
		lines.Write(recv)
		r.Stored++
	}
	if r.Stored == 0 {
		return r, nil
	}
	if b.FirstSeq > st.Hwm+1 {
		r.Gap = true
		s.st.OwnSeq++
		fmt.Fprintf(&lines, `{"v":1,"ts":%q,"level":"warn","event":"seq-gap","node":%q,"service":"nisshi","stream":%q,"seq":%d,"gap_stream":%q,"from_seq":%d,"to_seq":%d%s`,
			ts, b.Node, GapStream, s.st.OwnSeq, b.Stream, st.Hwm+1, b.FirstSeq-1, recv)
	}
	free, err := s.FreeBytes()
	if err != nil {
		return r, err
	}
	if free < s.reserve {
		return r, fmt.Errorf("%w: %d bytes free, reserve %d", ErrDisk, free, s.reserve)
	}
	day := now.Format("2006-01-02")
	key := b.Node + "/" + day
	committed := s.st.Days[key]
	frameLen, err := s.appendFrame(b.Node, day, committed, lines.Bytes())
	if err != nil {
		os.Truncate(s.dataPath(b.Node, day), committed)
		return r, fmt.Errorf("%w: %v", ErrDisk, err)
	}
	if s.crash != nil && s.crash("append") {
		return r, errors.New("crash after append")
	}
	// Mutate a copy of the stream so a failed state write leaves memory as on disk.
	ns := *st
	ns.Hwm = b.FirstSeq + uint64(len(b.Records)) - 1
	ns.LastReceiveNs = now.UnixNano()
	if b.LastTs > ns.LastRecordTs {
		ns.LastRecordTs = b.LastTs
	}
	prevLen, hadDay := s.st.Days[key]
	s.st.Days[key] = committed + frameLen
	s.st.Streams[b.Stream] = &ns
	if err := s.writeState(); err != nil {
		if hadDay {
			s.st.Days[key] = prevLen
		} else {
			delete(s.st.Days, key)
		}
		if st.LastReceiveNs == 0 {
			delete(s.st.Streams, b.Stream)
		} else {
			s.st.Streams[b.Stream] = st
		}
		os.Truncate(s.dataPath(b.Node, day), committed)
		return r, fmt.Errorf("%w: %v", ErrDisk, err)
	}
	r.Acked = ns.Hwm
	return r, nil
}

// appendFrame writes one zstd frame at the committed length of the day file, fsyncs it and
// returns the frame's length.
func (s *Store) appendFrame(node, day string, committed int64, lines []byte) (int64, error) {
	if err := os.MkdirAll(filepath.Join(s.root, node), 0o750); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(s.dataPath(node, day), os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	frame := s.enc.EncodeAll(lines, nil)
	if _, err := f.WriteAt(frame, committed); err != nil {
		return 0, err
	}
	return int64(len(frame)), f.Sync()
}

// writeState commits state.json: tmp, fsync, rename, fsync dir.
func (s *Store) writeState() error {
	b, err := json.Marshal(s.st)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(s.root, "state.json"), b, s.crash)
}

func writeAtomic(path string, b []byte, crash func(string) bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if crash != nil && crash("tmp") {
		return errors.New("crash after tmp")
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Streams is a snapshot of the stream states, keyed by hex stream id.
func (s *Store) Streams() map[string]Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Stream, len(s.st.Streams))
	for k, v := range s.st.Streams {
		out[k] = *v
	}
	return out
}

// ChainHeads is node -> hex SHA-256 of its newest manifest (ADR 0007).
func (s *Store) ChainHeads() (map[string]string, error) {
	heads := map[string]string{}
	for _, node := range s.nodes() {
		ms, err := manifests(filepath.Join(s.root, node))
		if err != nil {
			return nil, err
		}
		if len(ms) > 0 {
			heads[node] = ms[len(ms)-1].sha256
		}
	}
	return heads, nil
}

// readLines decodes a data file (a concatenation of zstd frames) and calls fn on each line
// without its newline. A truncated last frame ends the read (storage-format, "Data files").
func readLines(path string, fn func(line []byte) error) (sha string, n int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(windowSize))
	if err != nil {
		return "", 0, err
	}
	defer dec.Close()
	h := newHash()
	var rest []byte
	buf := make([]byte, 1<<20)
	for {
		k, rerr := dec.Read(buf)
		h.Write(buf[:k])
		rest = append(rest, buf[:k]...)
		for {
			i := bytes.IndexByte(rest, '\n')
			if i < 0 {
				break
			}
			if err := fn(rest[:i]); err != nil {
				return "", n, err
			}
			n++
			rest = rest[i+1:]
		}
		if rerr == io.EOF || errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
		if rerr != nil {
			return "", n, rerr
		}
	}
	return h.hex(), n, nil
}

// envelope is what the store reads back from a stored record.
type envelope struct {
	Ts      string          `json:"ts"`
	Event   string          `json:"event"`
	Service string          `json:"service"`
	Stream  string          `json:"stream"`
	Seq     json.RawMessage `json:"seq"`
	Node    string          `json:"node"`
}

func (e envelope) seq() (uint64, error) { return strconv.ParseUint(string(e.Seq), 10, 64) }

// ParseTime accepts the schema's ts format, RFC 3339, or an offset such as -1h from now.
func ParseTime(s string, now time.Time) (time.Time, error) {
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		d, err := time.ParseDuration(s)
		if err != nil {
			return time.Time{}, err
		}
		return now.Add(d), nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

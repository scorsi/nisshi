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

// ErrStreamOwner is returned by Push and CheckOwner for a stream that another node pushed first,
// or for nisshi's own stream (ADR 0014, HTTP 403 `stream-owner`).
var ErrStreamOwner = errors.New("stream-owner")

// ErrSealedDay is returned by Push when the store's clock puts the batch on a day of the node
// that is sealed, or before its newest sealed day: the chain is closed there (ADR 0006
// amendment, HTTP 503). Nothing is written; the agent keeps its spool and retries.
var ErrSealedDay = errors.New("receive day already sealed")

// errPublished: the rename of an atomic write happened, the directory sync after it failed. The
// new file may or may not survive a power loss; the old one is gone from the namespace.
var errPublished = errors.New("published, directory sync failed")

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

// Store is an open store root; one process writes it at a time (an exclusive flock on the root).
type Store struct {
	root    string
	reserve uint64
	now     func() time.Time
	lock    *os.File
	mu      sync.Mutex
	st      state
	enc     *zstd.Encoder
	// broken stops every push once the files may no longer match the state in memory (a state
	// write that may have been published, a failed rollback); a restart recovers from disk.
	broken error
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
	lock, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	// Before any recovery or maintenance: a second writer must not touch the files.
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("%s: another writer holds the store: %w", root, err)
	}
	s := &Store{root: root, reserve: reserve, now: now, lock: lock, st: state{V: 1, Days: map[string]int64{}, Streams: map[string]*Stream{}}}
	if err := s.load(); err != nil {
		lock.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the writer lock.
func (s *Store) Close() error { return s.lock.Close() }

func (s *Store) load() error {
	fp := filepath.Join(s.root, "FORMAT")
	if b, err := os.ReadFile(fp); errors.Is(err, os.ErrNotExist) {
		if err := writeAtomic(fp, []byte(formatLine), nil); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if string(b) != formatLine {
		return fmt.Errorf("%s: unknown format %q", fp, bytes.TrimSpace(b))
	}
	if b, err := os.ReadFile(filepath.Join(s.root, "state.json")); err == nil {
		if err := json.Unmarshal(b, &s.st); err != nil {
			return fmt.Errorf("state.json: %w", err)
		}
		if s.st.V != 1 || s.st.Days == nil || s.st.Streams == nil {
			return fmt.Errorf("state.json: version %d or days/streams missing", s.st.V)
		}
		for k, v := range s.st.Streams {
			if v == nil {
				return fmt.Errorf("state.json: stream %s is null", k)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var err error
	if s.enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(windowSize)); err != nil {
		return err
	}
	return s.recover()
}

// recover truncates every open day file to its committed length; a day file without state and
// without manifest is truncated to zero (storage-format, "Commit"). A committed length whose file
// is missing or shorter is lost acknowledged data: Open fails and changes nothing.
func (s *Store) recover() error {
	files, err := s.dataFiles()
	if err != nil {
		return err
	}
	byKey := map[string]dataFile{}
	for _, f := range files {
		byKey[f.node+"/"+f.day] = f
	}
	for key, n := range s.st.Days {
		f, ok := byKey[key]
		if ok && f.sealed {
			continue
		}
		node, day, _ := strings.Cut(key, "/")
		if !ok {
			if sealed, err := exists(s.manifestPath(node, day)); err != nil || n == 0 || sealed {
				continue // err: the loop below fails on it
			}
			return fmt.Errorf("%s: missing, %d bytes committed", s.dataPath(node, day), n)
		}
		fi, err := os.Stat(f.path)
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() || fi.Size() < n {
			return fmt.Errorf("%s: %d bytes, %d committed", f.path, fi.Size(), n)
		}
	}
	for _, f := range files {
		key := f.node + "/" + f.day
		if f.sealed {
			delete(s.st.Days, key)
			continue
		}
		n := s.st.Days[key] // 0 for a file the state does not know
		if err := os.Truncate(f.path, n); err != nil {
			return err
		}
		s.st.Days[key] = n
	}
	for key := range s.st.Days {
		if _, ok := byKey[key]; !ok {
			delete(s.st.Days, key)
		}
	}
	// Recompressed files of a seal interrupted before its rename.
	tmps, _ := filepath.Glob(filepath.Join(s.root, "*", "*.ndjson.zst.tmp"))
	for _, p := range tmps {
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return s.writeState()
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

type dataFile struct {
	node, day, path string
	sealed          bool // its manifest exists
}

// dataFiles lists every data file, sorted by node then day.
func (s *Store) dataFiles() ([]dataFile, error) {
	nodes, err := s.nodes()
	if err != nil {
		return nil, err
	}
	var out []dataFile
	for _, node := range nodes {
		entries, err := os.ReadDir(filepath.Join(s.root, node))
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if day, ok := strings.CutSuffix(e.Name(), ".ndjson.zst"); ok {
				sealed, err := exists(s.manifestPath(node, day))
				if err != nil {
					return nil, err
				}
				out = append(out, dataFile{node, day, s.dataPath(node, day), sealed})
			}
		}
	}
	return out, nil
}

func (s *Store) nodes() ([]string, error) {
	entries, err := os.ReadDir(s.root)
	var nodes []string
	for _, e := range entries {
		if e.IsDir() {
			nodes = append(nodes, e.Name())
		}
	}
	return nodes, err
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
	if s.broken != nil {
		return Result{}, fmt.Errorf("writer stopped until restart: %w", s.broken)
	}
	if err := s.checkOwner(b.Node, b.Stream); err != nil {
		return Result{}, err
	}
	old := s.st.Streams[b.Stream]
	st := old
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
	ownSeq := s.st.OwnSeq
	if b.FirstSeq > st.Hwm+1 {
		r.Gap = true
		ownSeq++
		fmt.Fprintf(&lines, `{"v":1,"ts":%q,"level":"warn","event":"seq-gap","node":%q,"service":"nisshi","stream":%q,"seq":%d,"gap_stream":%q,"from_seq":%d,"to_seq":%d%s`,
			ts, b.Node, GapStream, ownSeq, b.Stream, st.Hwm+1, b.FirstSeq-1, recv)
	}
	day := now.Format("2006-01-02")
	key := b.Node + "/" + day
	committed, open := s.st.Days[key]
	if !open {
		last, err := s.sealedThrough(b.Node)
		if err != nil {
			return r, err
		}
		if day <= last {
			return r, fmt.Errorf("%w: %s, %s sealed through %s: store clock behind?", ErrSealedDay, day, b.Node, last)
		}
	}
	free, err := s.FreeBytes()
	if err != nil {
		return r, err
	}
	if free < s.reserve {
		return r, fmt.Errorf("%w: %d bytes free, reserve %d", ErrDisk, free, s.reserve)
	}
	frameLen, err := s.appendFrame(b.Node, day, committed, lines.Bytes())
	if err != nil {
		return r, s.rollback(s.dataPath(b.Node, day), committed, err)
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
	prevOwn := s.st.OwnSeq
	s.st.Days[key] = committed + frameLen
	s.st.Streams[b.Stream] = &ns
	s.st.OwnSeq = ownSeq
	if err := s.writeState(); err != nil {
		if errors.Is(err, errPublished) {
			// state.json may already name the frame: keep both, accept nothing more until a
			// restart recovers from whichever state survived (old: frame cut; new: frame kept).
			s.broken = err
			return r, err
		}
		if hadDay {
			s.st.Days[key] = prevLen
		} else {
			delete(s.st.Days, key)
		}
		if old == nil {
			delete(s.st.Streams, b.Stream)
		} else {
			s.st.Streams[b.Stream] = old
		}
		s.st.OwnSeq = prevOwn
		return r, s.rollback(s.dataPath(b.Node, day), committed, err)
	}
	r.Acked = ns.Hwm
	return r, nil
}

// sealedThrough is the newest sealed day of a node, "" when none.
func (s *Store) sealedThrough(node string) (string, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, node))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	last := ""
	for _, e := range entries {
		if d, ok := strings.CutSuffix(e.Name(), ".manifest.json"); ok && d > last {
			last = d
		}
	}
	return last, err
}

// CheckOwner refuses a stream that another node pushed first, and nisshi's own stream (ADR
// 0014). Push checks again under the same lock: two first pushes may race.
func (s *Store) CheckOwner(node, stream string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkOwner(node, stream)
}

func (s *Store) checkOwner(node, stream string) error {
	if st := s.st.Streams[stream]; stream == GapStream || st != nil && st.Node != node {
		return ErrStreamOwner
	}
	return nil
}

// rollback cuts a day file back to its committed length after a failed commit. If that fails
// too, the file's tail is unknown: the writer stops until a restart recovers it.
func (s *Store) rollback(path string, committed int64, cause error) error {
	err := os.Truncate(path, committed)
	if s.crash != nil && s.crash("truncate") {
		err = errors.New("truncate failed")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.broken = fmt.Errorf("rollback of %s: %w", path, err)
		return fmt.Errorf("%w: %v; %v", ErrDisk, cause, s.broken)
	}
	return fmt.Errorf("%w: %v", ErrDisk, cause)
}

// appendFrame writes one zstd frame at the committed length of the day file, fsyncs it and
// returns the frame's length.
func (s *Store) appendFrame(node, day string, committed int64, lines []byte) (int64, error) {
	dir := filepath.Join(s.root, node)
	if err := os.MkdirAll(dir, 0o750); err != nil {
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
	if err := f.Sync(); err != nil {
		return 0, err
	}
	if committed == 0 { // maybe a new file in a new directory: make both names durable
		if err := syncDir(dir); err != nil {
			return 0, err
		}
		if err := syncDir(s.root); err != nil {
			return 0, err
		}
	}
	return int64(len(frame)), nil
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
	if crash != nil && crash("rename") {
		return fmt.Errorf("%w: crash after rename", errPublished)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("%w: %v", errPublished, err)
	}
	return nil
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
	nodes, err := s.nodes()
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
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

// maxLine is the longest stored line without its newline: a record is at most 4 MiB.
const maxLine = 4 << 20

// readLines decodes a data file (a concatenation of zstd frames) and calls fn on each line
// without its newline. Lenient (an open day being written): a truncated last frame ends the
// read (storage-format, "Data files"). Strict (a seal, a sealed day): it is an error, and so is
// a last line without its newline.
func readLines(path string, strict bool, fn func(line []byte) error) (sha string, n int, err error) {
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
		if len(rest) > maxLine {
			return "", n, fmt.Errorf("line %d: over %d bytes without a newline", n+1, maxLine)
		}
		if rerr == io.EOF || !strict && errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
		if rerr != nil {
			return "", n, rerr
		}
	}
	if strict && len(rest) > 0 {
		return "", n, fmt.Errorf("line %d: no final newline", n+1)
	}
	return h.hex(), n, nil
}

// envelope is what the store reads back from a stored record (parseEnvelope).
type envelope struct {
	Ts      string          `json:"ts"`
	Event   string          `json:"event"`
	Service string          `json:"service"`
	Stream  string          `json:"stream"`
	Seq     json.RawMessage `json:"seq"`
	Node    string          `json:"node"`
}

func (e envelope) seq() (uint64, error) { return strconv.ParseUint(string(e.Seq), 10, 64) }

// parseEnvelope reads the envelope members by their exact names: encoding/json's struct
// decoding also matches `TS` or `Seq`, which are free service fields (log-schema).
func parseEnvelope(line []byte) (e envelope, err error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		return e, err
	}
	for k, p := range map[string]*string{"ts": &e.Ts, "event": &e.Event, "service": &e.Service, "stream": &e.Stream, "node": &e.Node} {
		if v, ok := m[k]; ok {
			if err := json.Unmarshal(v, p); err != nil {
				return e, fmt.Errorf("%s: %w", k, err)
			}
		}
	}
	e.Seq = m["seq"]
	return e, nil
}

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

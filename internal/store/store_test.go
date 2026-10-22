package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"nisshi/internal/logschema"
)

const streamA = "9b1e0c5d7a2f4e8b9c0d1e2f3a4b5c6d"

var day0 = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)

func record(seq uint64) []byte {
	return []byte(fmt.Sprintf(`{"v":1,"ts":"2026-10-19T08:00:%02d.000000000Z","level":"info","event":"query","node":"node-a","service":"annai","stream":"%s","seq":%d,"qname":"example.org."}`, seq%60, streamA, seq))
}

func batch(first uint64, n int) Batch {
	b := Batch{Node: "node-a", Stream: streamA, FirstSeq: first}
	for i := 0; i < n; i++ {
		b.Records = append(b.Records, record(first+uint64(i)))
	}
	b.LastTs = "2026-10-19T08:00:59.000000000Z"
	return b
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

var opened = map[string]*Store{}

// open opens root, closing the store a previous call opened there (one writer per root).
func open(t *testing.T, root string, c *clock) *Store {
	if old := opened[root]; old != nil {
		old.Close()
	}
	s, err := Open(root, 0, c.now)
	if err != nil {
		t.Fatal(err)
	}
	opened[root] = s
	return s
}

// lines returns every stored line of a node, in file order, with the file's day.
func lines(t *testing.T, root, node string) (out []string, days []string) {
	s := &Store{root: root}
	files, err := s.dataFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.node != node {
			continue
		}
		if _, _, err := readLines(f.path, func(l []byte) error { out = append(out, string(l)); days = append(days, f.day); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	return out, days
}

func seqs(t *testing.T, root, node string) []uint64 {
	var out []uint64
	ls, _ := lines(t, root, node)
	for _, l := range ls {
		var e envelope
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			t.Fatal(err)
		}
		if e.Stream == streamA {
			seq, _ := e.seq()
			out = append(out, seq)
		}
	}
	return out
}

func TestPushTable(t *testing.T) {
	root := t.TempDir()
	c := &clock{day0}
	s := open(t, root, c)
	for i, tc := range []struct {
		b     Batch
		acked uint64
		res   Result
	}{
		{batch(1, 10), 10, Result{Stored: 10}},
		{batch(1, 10), 10, Result{Duplicates: 10}},           // exact duplicate
		{batch(6, 10), 15, Result{Stored: 5, Duplicates: 5}}, // overlap
		{batch(26, 5), 30, Result{Stored: 5, Gap: true}},     // gap 16-25
		{batch(31, 0), 30, Result{}},                         // empty batch
	} {
		r, err := s.Push(tc.b)
		if err != nil {
			t.Fatal(err)
		}
		tc.res.Acked = tc.acked
		if r != tc.res {
			t.Errorf("push %d: got %+v, want %+v", i, r, tc.res)
		}
	}
	got := seqs(t, root, "node-a")
	want := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 26, 27, 28, 29, 30}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("stored seqs %v, want %v", got, want)
	}
	ls, days := lines(t, root, "node-a")
	gaps := 0
	for i, l := range ls {
		var e struct {
			envelope
			RecvTs    string `json:"recv_ts"`
			GapStream string `json:"gap_stream"`
			FromSeq   uint64 `json:"from_seq"`
			ToSeq     uint64 `json:"to_seq"`
		}
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			t.Fatal(err)
		}
		// The stored record is the pushed one plus recv_ts, whose day is the file's day.
		suffix := `,"recv_ts":"` + e.RecvTs + `"}`
		if !strings.HasSuffix(l, suffix) || !strings.HasPrefix(e.RecvTs, days[i]) || len(suffix)-1 != 43 {
			t.Errorf("line %d: bad recv_ts member in %s", i, l)
		}
		if e.Event == "seq-gap" {
			gaps++
			if e.Service != "nisshi" || e.Stream != GapStream || e.GapStream != streamA || e.FromSeq != 16 || e.ToSeq != 25 || e.Node != "node-a" {
				t.Errorf("gap record: %s", l)
			}
			if reason := logschema.Record([]byte(strings.TrimSuffix(l, suffix) + "}")); reason != "" {
				t.Errorf("gap record invalid: %s", reason)
			}
			continue
		}
		seq, _ := e.seq()
		if string(record(seq)) != strings.TrimSuffix(l, suffix)+"}" {
			t.Errorf("line %d: not the pushed bytes", i)
		}
	}
	if gaps != 1 {
		t.Errorf("%d gap records, want 1", gaps)
	}
	if st := s.Streams()[streamA]; st.Hwm != 30 || st.Node != "node-a" || st.LastRecordTs != "2026-10-19T08:00:59.000000000Z" || st.LastReceiveNs != day0.UnixNano() {
		t.Errorf("stream state %+v", st)
	}
}

func TestDiskReserve(t *testing.T) {
	s, err := Open(t.TempDir(), 1<<62, func() time.Time { return day0 })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Push(batch(1, 1)); err == nil || !strings.Contains(err.Error(), "disk") {
		t.Errorf("got %v, want ErrDisk", err)
	}
	if got := seqs(t, s.root, "node-a"); len(got) != 0 {
		t.Errorf("stored %v after 507", got)
	}
}

// A crash or a failure after any commit step leaves the store either without the batch or with
// all of it, and a resend after restart stores exactly once (ADR 0011). "rename": the state is
// published but its directory sync failed; "truncate": the rollback failed too. Both stop the
// writer until a restart.
func TestCommitCrash(t *testing.T) {
	for _, tc := range []struct {
		steps   string
		hwm     uint64 // after the restart
		stopped bool
	}{{"append", 10, false}, {"tmp", 10, false}, {"rename", 20, true}, {"tmp truncate", 10, true}} {
		step := tc.steps
		root := t.TempDir()
		c := &clock{day0}
		s := open(t, root, c)
		if _, err := s.Push(batch(1, 10)); err != nil {
			t.Fatal(err)
		}
		s.crash = func(at string) bool { return strings.Contains(step, at) }
		if _, err := s.Push(batch(11, 10)); err == nil {
			t.Fatalf("%s: crash not injected", step)
		}
		s.crash = nil
		if tc.stopped {
			if _, err := s.Push(batch(11, 10)); err == nil {
				t.Errorf("%s: writer not stopped", step)
			}
		}
		s = open(t, root, c)
		hwm := s.Streams()[streamA].Hwm
		if hwm != tc.hwm {
			t.Errorf("%s: hwm %d after crash, want %d", step, hwm, tc.hwm)
		}
		if got := seqs(t, root, "node-a"); uint64(len(got)) != hwm {
			t.Errorf("%s: %d records after recovery, hwm %d", step, len(got), hwm)
		}
		r, err := s.Push(batch(11, 10))
		if err != nil || r.Acked != 20 || uint64(r.Stored) != 20-hwm {
			t.Fatalf("%s: resend: %+v %v", step, r, err)
		}
		if got := seqs(t, root, "node-a"); len(got) != 20 {
			t.Errorf("%s: %d records, want 20", step, len(got))
		}
		if err := Verify(root, os.Stderr); err != nil {
			t.Error(err)
		}
	}
}

// Recovery refuses a state that does not match the files, and changes nothing.
func TestRecoverRefuses(t *testing.T) {
	day := "node-a/2026-10-19.ndjson.zst"
	for name, damage := range map[string]func(root string){
		"missing file": func(root string) { os.Remove(filepath.Join(root, day)) },
		"short file":   func(root string) { os.Truncate(filepath.Join(root, day), 5) },
		"null days":    func(root string) { rewriteState(root, `"days":null`) },
		"null stream":  func(root string) { rewriteState(root, `"streams":{"`+streamA+`":null}`) },
		"version":      func(root string) { rewriteState(root, `"v":2`) },
		"unreadable": func(root string) {
			if os.Geteuid() == 0 {
				os.Remove(filepath.Join(root, day)) // root reads anything: fall back to a missing file
			}
			os.Chmod(filepath.Join(root, "node-a"), 0)
		},
	} {
		root := t.TempDir()
		c := &clock{day0}
		s := open(t, root, c)
		s.Push(batch(1, 10))
		s.Close()
		delete(opened, root)
		damage(root)
		before, _ := os.ReadFile(filepath.Join(root, "state.json"))
		if s, err := Open(root, 0, c.now); err == nil {
			s.Close()
			t.Errorf("%s: opened", name)
		}
		os.Chmod(filepath.Join(root, "node-a"), 0o750)
		after, _ := os.ReadFile(filepath.Join(root, "state.json"))
		if !bytes.Equal(before, after) {
			t.Errorf("%s: state.json changed", name)
		}
		if fi, err := os.Stat(filepath.Join(root, day)); name == "short file" && (err != nil || fi.Size() != 5) {
			t.Errorf("%s: file changed", name)
		}
	}
}

// rewriteState replaces one top-level member of state.json.
func rewriteState(root, member string) {
	p := filepath.Join(root, "state.json")
	var m map[string]json.RawMessage
	b, _ := os.ReadFile(p)
	json.Unmarshal(b, &m)
	var one map[string]json.RawMessage
	json.Unmarshal([]byte("{"+member+"}"), &one)
	for k, v := range one {
		m[k] = v
	}
	b, _ = json.Marshal(m)
	os.WriteFile(p, b, 0o640)
}

func TestOneWriter(t *testing.T) {
	root := t.TempDir()
	c := &clock{day0}
	s := open(t, root, c)
	if s2, err := Open(root, 0, c.now); err == nil {
		s2.Close()
		t.Fatal("a second writer opened the store")
	}
	s.Close()
	if s2, err := Open(root, 0, c.now); err != nil {
		t.Fatal(err)
	} else {
		s2.Close()
	}
	delete(opened, root)
}

// A seal stopped at any step leaves the day sealed or open, never a file that recovery cuts to
// the length of its other representation (several frames make the two lengths differ).
func TestSealCrash(t *testing.T) {
	for _, step := range []string{"tmp", "rename", "seal-rename"} {
		root := t.TempDir()
		c := &clock{day0}
		s := open(t, root, c)
		for i := uint64(0); i < 5; i++ {
			s.Push(batch(1+10*i, 10))
		}
		c.t = day0.AddDate(0, 0, 1)
		s.crash = func(at string) bool { return at == step }
		if sealed, errs := s.SealPast(); len(sealed) != 0 || len(errs) != 1 {
			t.Fatalf("%s: crash not injected: %v %v", step, sealed, errs)
		}
		s = open(t, root, c)
		if sealed, errs := s.SealPast(); len(errs) != 0 || (step == "tmp") != (len(sealed) == 1) {
			t.Errorf("%s: reseal: %v %v", step, sealed, errs)
		}
		if _, ok := s.st.Days["node-a/2026-10-19"]; ok {
			t.Errorf("%s: sealed day still open in the state", step)
		}
		if got := seqs(t, root, "node-a"); len(got) != 50 || got[49] != 50 {
			t.Errorf("%s: %d records after the seal", step, len(got))
		}
		if err := Verify(root, os.Stderr); err != nil {
			t.Errorf("%s: %v", step, err)
		}
		if tmps, _ := filepath.Glob(filepath.Join(root, "*", "*.tmp")); len(tmps) != 0 {
			t.Errorf("%s: left %v", step, tmps)
		}
	}
}

// A day that fails to seal holds back the later days of its node, so the chain never skips it.
func TestSealDefersLaterDays(t *testing.T) {
	root := t.TempDir()
	c := &clock{day0}
	s := open(t, root, c)
	s.Push(batch(1, 5))
	c.t = day0.AddDate(0, 0, 1)
	s.Push(batch(6, 5))
	p := s.dataPath("node-a", "2026-10-19")
	good, _ := os.ReadFile(p)
	os.WriteFile(p, s.enc.EncodeAll([]byte("not json\n"), nil), 0o640)
	fi, _ := os.Stat(p)
	s.st.Days["node-a/2026-10-19"] = fi.Size()
	c.t = day0.AddDate(0, 0, 2)
	if sealed, errs := s.SealPast(); len(sealed) != 0 || len(errs) != 2 {
		t.Fatalf("got %v %v", sealed, errs)
	}
	os.WriteFile(p, good, 0o640)
	s.st.Days["node-a/2026-10-19"] = int64(len(good))
	if sealed, errs := s.SealPast(); len(sealed) != 2 || len(errs) != 0 {
		t.Fatalf("after repair: %v %v", sealed, errs)
	}
	if err := Verify(root, os.Stderr); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverUnknownFile(t *testing.T) {
	root := t.TempDir()
	c := &clock{day0}
	open(t, root, c)
	os.MkdirAll(filepath.Join(root, "node-b"), 0o750)
	p := filepath.Join(root, "node-b", "2026-10-19.ndjson.zst")
	os.WriteFile(p, []byte("garbage"), 0o640)
	open(t, root, c)
	if fi, _ := os.Stat(p); fi.Size() != 0 {
		t.Errorf("unknown day file not truncated to zero")
	}
}

func TestSealChainRetentionRead(t *testing.T) {
	root := t.TempDir()
	c := &clock{day0}
	s := open(t, root, c)
	if _, err := s.Push(batch(1, 10)); err != nil {
		t.Fatal(err)
	}
	if sealed, errs := s.SealPast(); len(sealed) != 0 || len(errs) != 0 {
		t.Errorf("sealed the open day: %v %v", sealed, errs)
	}
	c.t = day0.AddDate(0, 0, 1)
	s = open(t, root, c)
	if _, err := s.Push(batch(11, 5)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // idempotent
		sealed, errs := s.SealPast()
		if len(errs) != 0 || (i == 0 && len(sealed) != 1) || (i == 1 && len(sealed) != 0) {
			t.Fatalf("seal %d: %v %v", i, sealed, errs)
		}
	}
	ms, err := manifests(filepath.Join(root, "node-a"))
	if err != nil || len(ms) != 1 {
		t.Fatal(ms, err)
	}
	m := ms[0]
	if m.Day != "2026-10-19" || m.Records != 10 || m.PrevDay != nil || m.Services["annai"] != 10 || len(m.Streams) != 1 || m.Streams[0].LastSeq != 10 || m.MinTs != "2026-10-19T08:00:01.000000000Z" || m.MaxTs != "2026-10-19T08:00:10.000000000Z" {
		t.Errorf("manifest %+v", m)
	}
	sha, n, _ := readLines(s.dataPath("node-a", "2026-10-19"), func([]byte) error { return nil })
	if sha != m.ContentSha256 || n != 10 {
		t.Errorf("content hash %s/%d vs manifest %s/%d", sha, n, m.ContentSha256, m.Records)
	}
	if heads, _ := s.ChainHeads(); heads["node-a"] != m.sha256 {
		t.Errorf("chain head %v", heads)
	}
	c.t = day0.AddDate(0, 0, 2)
	s = open(t, root, c)
	s.SealPast()
	ms, _ = manifests(filepath.Join(root, "node-a"))
	if len(ms) != 2 || *ms[1].PrevDay != "2026-10-19" || *ms[1].PrevManifestSha != ms[0].sha256 {
		t.Fatalf("chain: %+v", ms)
	}
	if err := Verify(root, os.Stderr); err != nil {
		t.Fatal(err)
	}
	// Retention deletes data, keeps manifests; verify still passes.
	if deleted, err := s.Retain(1); err != nil || fmt.Sprint(deleted) != "[node-a/2026-10-19]" {
		t.Fatalf("retain: %v %v", deleted, err)
	}
	if _, err := os.Stat(s.dataPath("node-a", "2026-10-19")); !os.IsNotExist(err) {
		t.Error("data file kept")
	}
	if _, err := os.Stat(s.manifestPath("node-a", "2026-10-19")); err != nil {
		t.Error("manifest deleted")
	}
	if err := Verify(root, os.Stderr); err != nil {
		t.Fatal(err)
	}
	if deleted, _ := s.Retain(0); len(deleted) != 0 {
		t.Error("retention 0 deleted data")
	}
	// Read: pruned day (deleted anyway), sealed day 2 (seqs 11-15, ts 08:00:11-15), sorted.
	var out bytes.Buffer
	from, _ := ParseTime("2026-10-19T08:00:12Z", c.t)
	to, _ := ParseTime("2026-10-19T08:00:14Z", c.t)
	if err := Read(root, Query{Node: "node-a", Service: "annai", Event: "query", From: from, To: to}, &out); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(out.String(), "\n"); got != 3 || !strings.Contains(out.String(), `"seq":13`) {
		t.Errorf("read: %s", out.String())
	}
	// Tampering is detected.
	os.WriteFile(s.manifestPath("node-a", "2026-10-19"), []byte(`{"v":1}`), 0o640)
	if err := Verify(root, &bytes.Buffer{}); err == nil {
		t.Error("verify passed on a tampered manifest")
	}
}

func TestSealStopsOnBadLine(t *testing.T) {
	root := t.TempDir()
	c := &clock{day0}
	s := open(t, root, c)
	s.Push(batch(1, 1))
	// Corrupt the committed content: a frame holding a non-JSON line.
	p := s.dataPath("node-a", "2026-10-19")
	os.WriteFile(p, s.enc.EncodeAll([]byte("not json\n"), nil), 0o640)
	fi, _ := os.Stat(p)
	s.st.Days["node-a/2026-10-19"] = fi.Size()
	c.t = day0.AddDate(0, 0, 1)
	if sealed, errs := s.SealPast(); len(sealed) != 0 || len(errs) != 1 {
		t.Errorf("got %v %v", sealed, errs)
	}
	if _, err := os.Stat(s.manifestPath("node-a", "2026-10-19")); err == nil {
		t.Error("manifest written for a bad file")
	}
}

func TestParseTime(t *testing.T) {
	now := day0
	if got, _ := ParseTime("-1h", now); got != now.Add(-time.Hour) {
		t.Error(got)
	}
	if got, _ := ParseTime("2026-10-19T08:00:00.123456789Z", now); got.Nanosecond() != 123456789 {
		t.Error(got)
	}
	if _, err := ParseTime("yesterday", now); err == nil {
		t.Error("accepted garbage")
	}
}

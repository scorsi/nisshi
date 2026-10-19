package serve

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	nisshiv1 "nisshi/gen/nisshi/v1"
	"nisshi/internal/logschema"
)

const streamHex = "9b1e0c5d7a2f4e8b9c0d1e2f3a4b5c6d"

func sha(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

func newTestServer(t *testing.T, reserve uint64) (*Server, *httptest.Server, *bytes.Buffer) {
	cfg := Config{DataDir: t.TempDir(), DiskReserve: reserve, Tokens: map[string]string{sha("tok-a"): "node:node-a", sha("tok-b"): "node:node-b", sha("tok-r"): "reader"}}
	var logs bytes.Buffer
	s, err := newServer(cfg, NewLogger(&logs), func() time.Time { return time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.handler())
	t.Cleanup(ts.Close)
	return s, ts, &logs
}

func record(node string, seq uint64, extra string) []byte {
	return []byte(fmt.Sprintf(`{"v":1,"ts":"2026-10-19T08:00:%02d.000000000Z","level":"info","event":"query","node":%q,"service":"annai","stream":"%s","seq":%d%s}`, seq%60, node, streamHex, seq, extra))
}

type pushOpts struct {
	token, node, encoding string
	streamID              []byte
	first                 uint64
	records               [][]byte
	raw                   []byte
}

func push(t *testing.T, url string, o pushOpts) (int, *nisshiv1.PushResponse, *nisshiv1.PushRejected) {
	if o.node == "" {
		o.node = "node-a"
	}
	if o.streamID == nil {
		o.streamID, _ = hex.DecodeString(streamHex)
	}
	body := o.raw
	if body == nil {
		body, _ = proto.Marshal(&nisshiv1.PushRequest{Node: o.node, StreamId: o.streamID, FirstSeq: o.first, Records: o.records, Status: &nisshiv1.AgentStatus{Version: "t"}})
	}
	if o.encoding == "" {
		o.encoding = "zstd"
	}
	if o.encoding == "zstd" {
		enc, _ := zstd.NewWriter(nil)
		body = enc.EncodeAll(body, nil)
	}
	req, _ := http.NewRequest("POST", url+"/v1/push", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	if o.encoding != "none" {
		req.Header.Set("Content-Encoding", o.encoding)
	}
	if o.token != "none" {
		if o.token == "" {
			o.token = "tok-a"
		}
		req.Header.Set("Authorization", "Bearer "+o.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case 200:
		var pr nisshiv1.PushResponse
		proto.Unmarshal(b, &pr)
		return 200, &pr, nil
	case 400:
		var rj nisshiv1.PushRejected
		if err := proto.Unmarshal(b, &rj); err != nil {
			t.Fatal(err)
		}
		return 400, nil, &rj
	}
	return resp.StatusCode, nil, nil
}

func records(node string, first uint64, n int) [][]byte {
	var out [][]byte
	for i := 0; i < n; i++ {
		out = append(out, record(node, first+uint64(i), ""))
	}
	return out
}

func TestResponseTable(t *testing.T) {
	s, ts, logs := newTestServer(t, 0)
	idx := func(i uint32) *uint32 { return &i }
	for _, tc := range []struct {
		name   string
		o      pushOpts
		code   int
		acked  uint64
		index  *uint32
		reason string
	}{
		{"no token", pushOpts{token: "none", first: 1, records: records("node-a", 1, 2)}, 401, 0, nil, ""},
		{"unknown token", pushOpts{token: "nope", first: 1, records: records("node-a", 1, 2)}, 401, 0, nil, ""},
		{"reader token", pushOpts{token: "tok-r", first: 1, records: records("node-a", 1, 2)}, 401, 0, nil, ""},
		{"other node's token", pushOpts{token: "tok-b", first: 1, records: records("node-a", 1, 2)}, 400, 0, nil, "node-mismatch"},
		{"request node", pushOpts{node: "node-b", first: 1, records: records("node-b", 1, 2)}, 400, 0, nil, "node-mismatch"},
		{"stream id", pushOpts{streamID: []byte{1, 2}, first: 1, records: records("node-a", 1, 2)}, 400, 0, nil, "stream-id-format"},
		{"first seq", pushOpts{first: 0, records: records("node-a", 0, 2)}, 400, 0, nil, "first-seq-zero"},
		{"garbage body", pushOpts{raw: []byte("not protobuf at all"), encoding: "none"}, 400, 0, nil, "body-format"},
		{"not zstd", pushOpts{raw: []byte("not zstd"), encoding: "identity"}, 400, 0, nil, "body-format"},
		{"invalid record", pushOpts{first: 1, records: [][]byte{record("node-a", 1, ""), []byte(`{"v":1}`)}}, 400, 0, idx(1), "missing-field:ts"},
		{"recv_ts in record", pushOpts{first: 1, records: [][]byte{record("node-a", 1, `,"recv_ts":"x"`)}}, 400, 0, idx(0), "reserved-field:recv_ts"},
		{"record node", pushOpts{first: 1, records: [][]byte{record("node-b", 1, "")}}, 400, 0, idx(0), "node-mismatch"},
		{"record seq", pushOpts{first: 1, records: [][]byte{record("node-a", 1, ""), record("node-a", 3, "")}}, 400, 0, idx(1), "seq-mismatch"},
		{"in order", pushOpts{first: 1, records: records("node-a", 1, 10)}, 200, 10, nil, ""},
		{"exact duplicate", pushOpts{first: 1, records: records("node-a", 1, 10)}, 200, 10, nil, ""},
		{"overlap", pushOpts{first: 6, records: records("node-a", 6, 10)}, 200, 15, nil, ""},
		{"gap", pushOpts{first: 21, records: records("node-a", 21, 5)}, 200, 25, nil, ""},
		{"plain body", pushOpts{first: 26, records: records("node-a", 26, 1), encoding: "none"}, 200, 26, nil, ""},
	} {
		code, pr, rj := push(t, ts.URL, tc.o)
		if code != tc.code {
			t.Errorf("%s: code %d, want %d", tc.name, code, tc.code)
			continue
		}
		if pr != nil && pr.AckedSeq != tc.acked {
			t.Errorf("%s: acked %d, want %d", tc.name, pr.AckedSeq, tc.acked)
		}
		if rj != nil && (rj.Reason != tc.reason || (rj.RecordIndex == nil) != (tc.index == nil) || (tc.index != nil && *rj.RecordIndex != *tc.index)) {
			t.Errorf("%s: rejected %v, want index %v reason %s", tc.name, rj, tc.index, tc.reason)
		}
	}
	// Stream mismatch: a record of another stream.
	other := strings.Repeat("ab", 16)
	rec := bytes.Replace(record("node-a", 27, ""), []byte(streamHex), []byte(other), 1)
	if code, _, rj := push(t, ts.URL, pushOpts{first: 27, records: [][]byte{rec}}); code != 400 || rj.Reason != "stream-mismatch" || *rj.RecordIndex != 0 {
		t.Errorf("stream mismatch: %d %v", code, rj)
	}
	if code, _, _ := push(t, ts.URL, pushOpts{raw: bytes.Repeat([]byte{0}, maxCompressed+1), encoding: "none"}); code != 413 {
		t.Errorf("too large: %d", code)
	}
	// A frame whose decompressed size exceeds the limit.
	if code, _, _ := push(t, ts.URL, pushOpts{raw: bytes.Repeat([]byte{0}, maxDecompressed+1)}); code != 413 {
		t.Errorf("too large decompressed: %d", code)
	}

	// Status: reader only, protojson.
	req, _ := http.NewRequest("GET", ts.URL+"/v1/status", nil)
	req.Header.Set("Authorization", "Bearer tok-a")
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 401 {
		t.Errorf("status with a node token: %d", resp.StatusCode)
	}
	req.Header.Set("Authorization", "Bearer tok-r")
	resp, _ := http.DefaultClient.Do(req)
	b, _ := io.ReadAll(resp.Body)
	var status struct {
		Streams []struct {
			Node, AckedSeq, LastReceiveNs string
		}
		Agents           map[string]struct{ Version string }
		DiskReserveBytes string
		ChainHeads       map[string]string
	}
	if err := json.Unmarshal(b, &status); err != nil || resp.StatusCode != 200 {
		t.Fatalf("status: %d %v %s", resp.StatusCode, err, b)
	}
	if len(status.Streams) != 1 || status.Streams[0].AckedSeq != "26" || status.Streams[0].Node != "node-a" || status.Agents["node-a"].Version != "t" || status.DiskReserveBytes != "0" {
		t.Errorf("status: %s", b)
	}

	// Metrics: the names of ADR 0010.
	rec2 := httptest.NewRecorder()
	s.serveMetrics(rec2, httptest.NewRequest("GET", "/metrics", nil))
	m := rec2.Body.String()
	for _, want := range []string{`nisshi_records_total{source_node="node-a"} 21`, "nisshi_duplicate_records_total 15", "nisshi_seq_gaps_total 1", `nisshi_push_requests_total{code="200"} 5`, `nisshi_push_requests_total{code="401"} 3`, `nisshi_seals_total{result="ok"} 0`, "nisshi_disk_free_bytes ", `nisshi_build_info{version="dev"} 1`} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q in\n%s", want, m)
		}
	}
	if !strings.HasSuffix(m, "\n") || rec2.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Error("exposition format")
	}

	// Own logs validate against the schema.
	s.log.Info("", "event", "test")
	s.log.Error("", "event", "test-error", "err", "boom")
	for i, line := range bytes.Split(bytes.TrimSuffix(logs.Bytes(), []byte("\n")), []byte("\n")) {
		if reason := logschema.Line(line); reason != "" {
			t.Errorf("log line %d: %s: %s", i+1, reason, line)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "c.toml")
		os.WriteFile(p, []byte(s), 0o600)
		return p
	}
	good := "listen = [\"127.0.0.1:9473\"]\ndata_dir = \"/var/lib/nisshi\"\n[tls]\ncert_file = \"c\"\nkey_file = \"k\"\n[tokens]\n\"" + sha("x") + "\" = \"node:node-a\"\n"
	cfg, err := LoadConfig(write(good))
	if err != nil || cfg.RetentionDays != 400 || cfg.DiskReserve != 2<<30 || cfg.Tokens[sha("x")] != "node:node-a" {
		t.Fatalf("%+v %v", cfg, err)
	}
	for name, bad := range map[string]string{
		"unknown key":   good + "listne_metrics = \"x\"\n",
		"bad identity":  strings.Replace(good, "node:node-a", "admin", 1),
		"bad hash":      strings.Replace(good, sha("x"), "abc", 1),
		"no listen":     strings.Replace(good, "listen = [\"127.0.0.1:9473\"]", "listen = []", 1),
		"negative days": good + "retention_days = -1\n",
	} {
		if _, err := LoadConfig(write(bad)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestClock(t *testing.T) {
	t.Setenv("NISSHI_FAKE_NOW", "2026-10-20T00:00:00Z")
	now, err := Clock()
	if err != nil || now().Format(time.RFC3339) != "2026-10-20T00:00:00Z" {
		t.Error(err)
	}
	t.Setenv("NISSHI_FAKE_NOW", "tomorrow")
	if _, err := Clock(); err == nil {
		t.Error("accepted garbage")
	}
}

func TestNotify(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.sock")
	sock, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	t.Setenv("NOTIFY_SOCKET", path)
	if err := notify("READY=1"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, _ := sock.Read(buf)
	if string(buf[:n]) != "READY=1" {
		t.Errorf("got %q", buf[:n])
	}
	t.Setenv("NOTIFY_SOCKET", "")
	if err := notify("READY=1"); err != nil {
		t.Errorf("without NOTIFY_SOCKET: %v", err)
	}
}

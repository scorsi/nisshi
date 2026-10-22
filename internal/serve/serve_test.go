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
	"google.golang.org/protobuf/encoding/protowire"
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
	if strings.EqualFold(o.encoding, "zstd") {
		enc, _ := zstd.NewWriter(nil)
		body = enc.EncodeAll(body, nil)
	}
	req, _ := http.NewRequest("POST", url+"/v1/push", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	switch o.encoding {
	case "none":
	case "raw-zstd": // the header without compressing raw
		req.Header.Set("Content-Encoding", "zstd")
	default:
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
	case 400, 403:
		var rj nisshiv1.PushRejected
		if err := proto.Unmarshal(b, &rj); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, nil, &rj
	}
	return resp.StatusCode, nil, nil
}

func mustMarshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
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
		{"reader token", pushOpts{token: "tok-r", first: 1, records: records("node-a", 1, 2)}, 403, 0, nil, ""},
		{"reader token, bad body", pushOpts{token: "tok-r", raw: []byte("x"), encoding: "none"}, 403, 0, nil, ""},
		{"other node's token", pushOpts{token: "tok-b", first: 1, records: records("node-a", 1, 2)}, 400, 0, nil, "node-mismatch"},
		{"request node", pushOpts{node: "node-b", first: 1, records: records("node-b", 1, 2)}, 400, 0, nil, "node-mismatch"},
		{"stream id", pushOpts{streamID: []byte{1, 2}, first: 1, records: records("node-a", 1, 2)}, 400, 0, nil, "stream-id-format"},
		{"first seq", pushOpts{first: 0, records: records("node-a", 0, 2)}, 400, 0, nil, "first-seq-zero"},
		{"no encoding", pushOpts{first: 1, records: records("node-a", 1, 2), encoding: "none"}, 400, 0, nil, "content-encoding"},
		{"identity encoding", pushOpts{first: 1, records: records("node-a", 1, 2), encoding: "identity"}, 400, 0, nil, "content-encoding"},
		{"encoding before node", pushOpts{node: "node-b", first: 0, records: records("node-b", 1, 2), encoding: "gzip"}, 400, 0, nil, "content-encoding"},
		{"not zstd", pushOpts{raw: []byte("not zstd"), encoding: "raw-zstd"}, 400, 0, nil, "body-zstd"},
		{"garbage body", pushOpts{raw: []byte("not protobuf at all")}, 400, 0, nil, "body-protobuf"},
		{"invalid record", pushOpts{first: 1, records: [][]byte{record("node-a", 1, ""), []byte(`{"v":1}`)}}, 400, 0, idx(1), "missing-field:ts"},
		{"recv_ts in record", pushOpts{first: 1, records: [][]byte{record("node-a", 1, `,"recv_ts":"x"`)}}, 400, 0, idx(0), "reserved-field:recv_ts"},
		{"record node", pushOpts{first: 1, records: [][]byte{record("node-b", 1, "")}}, 400, 0, idx(0), "node-mismatch"},
		{"record seq", pushOpts{first: 1, records: [][]byte{record("node-a", 1, ""), record("node-a", 3, "")}}, 400, 0, idx(1), "seq-mismatch"},
		{"in order", pushOpts{first: 1, records: records("node-a", 1, 10)}, 200, 10, nil, ""},
		{"exact duplicate", pushOpts{first: 1, records: records("node-a", 1, 10)}, 200, 10, nil, ""},
		{"overlap", pushOpts{first: 6, records: records("node-a", 6, 10)}, 200, 15, nil, ""},
		{"gap", pushOpts{first: 21, records: records("node-a", 21, 5)}, 200, 25, nil, ""},
		{"upper-case encoding", pushOpts{first: 26, records: records("node-a", 26, 1), encoding: "ZSTD"}, 200, 26, nil, ""},
		// ADR 0014: the stream is node-a's; request checks first, then the owner, then records.
		{"another node's stream", pushOpts{token: "tok-b", node: "node-b", first: 27, records: records("node-b", 27, 1)}, 403, 0, nil, "stream-owner"},
		{"owner before records", pushOpts{token: "tok-b", node: "node-b", first: 27, records: [][]byte{[]byte("x")}}, 403, 0, nil, "stream-owner"},
		{"request before owner", pushOpts{token: "tok-b", node: "node-b", first: 0, records: records("node-b", 27, 1)}, 400, 0, nil, "first-seq-zero"},
		{"reserved stream", pushOpts{streamID: make([]byte, 16), first: 1, records: [][]byte{bytes.ReplaceAll(record("node-a", 1, ""), []byte(streamHex), []byte(strings.Repeat("0", 32)))}}, 403, 0, nil, "stream-owner"},
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
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 403 {
		t.Errorf("status with a node token: %d", resp.StatusCode)
	}
	for tok, want := range map[string]int{"none": 401, "Bearer nope": 401} {
		r2, _ := http.NewRequest("GET", ts.URL+"/v1/status", nil)
		if tok != "none" {
			r2.Header.Set("Authorization", tok)
		}
		if resp, _ := http.DefaultClient.Do(r2); resp.StatusCode != want {
			t.Errorf("status with %s: %d, want %d", tok, resp.StatusCode, want)
		}
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
	for _, want := range []string{`nisshi_records_total{source_node="node-a"} 21`, "nisshi_duplicate_records_total 15", "nisshi_seq_gaps_total 1", `nisshi_push_requests_total{code="200"} 5`, `nisshi_push_requests_total{code="401"} 2`, `nisshi_push_requests_total{code="403"} 5`, `nisshi_seals_total{result="ok"} 0`, `nisshi_streams{source_node="node-a"} 1`, "nisshi_disk_free_bytes ", `nisshi_build_info{version="dev"} 1`} {
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

type chanWriter chan []byte

func (c chanWriter) Write(p []byte) (int, error) { c <- bytes.Clone(p); return len(p), nil }

// A TLS handshake error (a port probe that closes at once) is a schema line, not net/http text.
func TestErrorLogIsSchema(t *testing.T) {
	lines := make(chanWriter, 4)
	ts := httptest.NewUnstartedServer(http.NotFoundHandler())
	ts.Config.ErrorLog = errorLog(NewLogger(lines))
	ts.StartTLS()
	defer ts.Close()
	c, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	select {
	case line := <-lines:
		line = bytes.TrimSuffix(line, []byte("\n"))
		if reason := logschema.Line(line); reason != "" || !bytes.Contains(line, []byte(`"event":"http-error","err":"http: TLS handshake error`)) {
			t.Errorf("%s: %s", reason, line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no log line")
	}
}

// A push the store's clock puts on a sealed day: 503, so the agent keeps its spool and retries.
func TestSealedDay503(t *testing.T) {
	s, ts, logs := newTestServer(t, 0)
	os.MkdirAll(filepath.Join(s.cfg.DataDir, "node-a"), 0o750)
	os.WriteFile(filepath.Join(s.cfg.DataDir, "node-a", "2026-10-19.manifest.json"), []byte("{}"), 0o640)
	if code, _, _ := push(t, ts.URL, pushOpts{first: 1, records: records("node-a", 1, 2)}); code != 503 {
		t.Errorf("code %d", code)
	}
	if !strings.Contains(logs.String(), `"level":"error"`) || !strings.Contains(logs.String(), "sealed") {
		t.Errorf("no error event: %s", logs)
	}
}

// decodePush answers as proto.Unmarshal does, records aside: it keeps maxRecords+1 of them.
func TestDecodePush(t *testing.T) {
	good, _ := proto.Marshal(&nisshiv1.PushRequest{Node: "node-a", StreamId: []byte{1, 2}, FirstSeq: 7, Records: [][]byte{[]byte("a"), {}}, Status: &nisshiv1.AgentStatus{Version: "v", Paused: true}})
	for name, b := range map[string][]byte{
		"good":                          good,
		"unknown field":                 append(bytes.Clone(good), 0x30, 0x01),
		"wrong wire type":               append(bytes.Clone(good), 0x18|5, 1, 2, 3, 4),
		"status twice":                  append(bytes.Clone(good), 0x2a, 2, 0x20, 0x00),
		"bad utf8 node":                 {0x0a, 1, 0xff},
		"truncated":                     good[:len(good)-1],
		"field number 0":                {0x00, 0x01},
		"bad status":                    {0x2a, 2, 0x0a, 5},
		"empty":                         {},
		"field number over the maximum": append(protowire.AppendTag(bytes.Clone(good), protowire.MaxValidNumber+1, protowire.VarintType), 1),
		"status, field number over":     append([]byte{0x2a, 6}, append(protowire.AppendTag(nil, protowire.MaxValidNumber+1, protowire.VarintType), 1)...),
		"status, every field":           protowire.AppendBytes([]byte{0x2a}, mustMarshal(&nisshiv1.AgentStatus{Version: "é", SpoolBytes: 1, SpoolLimitBytes: 2, Paused: true, OldestUnackedNs: -3})),
		"status, bad utf8 version":      {0x2a, 3, 0x0a, 1, 0xff},
		"status, wrong wire types":      {0x2a, 7, 0x0d, 1, 2, 3, 4, 0x20, 0x01},
		"status, unknown field":         {0x2a, 4, 0x78, 0x01, 0x20, 0x02},
	} {
		var want nisshiv1.PushRequest
		werr := proto.Unmarshal(b, &want)
		want.ProtoReflect().SetUnknown(nil) // dropped: nothing reads them
		if want.Status != nil {
			want.Status.ProtoReflect().SetUnknown(nil)
		}
		got, err := decodePush(b)
		if (err != nil) != (werr != nil) || err == nil && !proto.Equal(got, &want) {
			t.Errorf("%s: got %v %v, want %v %v", name, got, err, &want, werr)
		}
	}
	// An absurd count of empty records: a bounded slice, and the indexed 400 of the first one.
	head, _ := proto.Marshal(&nisshiv1.PushRequest{Node: "node-a", StreamId: bytes.Repeat([]byte{7}, 16), FirstSeq: 1})
	raw := append(head, bytes.Repeat([]byte{0x22, 0}, 1<<20)...)
	if req, _ := decodePush(raw); len(req.Records) != maxRecords+1 {
		t.Errorf("%d records kept", len(req.Records))
	}
	_, ts, _ := newTestServer(t, 0)
	if code, _, rj := push(t, ts.URL, pushOpts{raw: raw}); code != 400 || rj.Reason != "framing" || rj.RecordIndex == nil || *rj.RecordIndex != 0 {
		t.Errorf("%d %v", code, rj)
	}
	// The smallest valid record is longer than the bound assumes.
	if r := `{"v":1,"ts":"2026-10-19T08:00:00.000000000Z","level":"info","event":"a","node":"a","service":"a","stream":"` + strings.Repeat("0", 32) + `","seq":1}`; logschema.Record([]byte(r)) != "" || len(r)+2 < minRecordWire {
		t.Error("minRecordWire")
	}
}

// A push waits while another holds the admission slot, gets 503 when the wait outlasts
// admitTimeout, and goes through once the slot is free; an agent's status is kept bounded.
func TestPushAdmissionAndStatus(t *testing.T) {
	defer func(d time.Duration) { admitTimeout = d }(admitTimeout)
	admitTimeout = 300 * time.Millisecond
	s, ts, _ := newTestServer(t, 0)
	s.pushing <- struct{}{} // a push in progress
	if code, _, _ := push(t, ts.URL, pushOpts{first: 1, records: records("node-a", 1, 1)}); code != 503 {
		t.Errorf("slot held past admitTimeout: %d", code)
	}
	admitTimeout = 5 * time.Second
	done := make(chan int, 1)
	go func() {
		code, _, _ := push(t, ts.URL, pushOpts{first: 1, records: records("node-a", 1, 1)})
		done <- code
	}()
	select {
	case code := <-done:
		t.Fatalf("admitted while the slot is held: %d", code)
	case <-time.After(200 * time.Millisecond):
	}
	<-s.pushing
	if code := <-done; code != 200 {
		t.Errorf("after the slot is freed: %d", code)
	}
	body, _ := proto.Marshal(&nisshiv1.PushRequest{Node: "node-a", StreamId: bytes.Repeat([]byte{1}, 16), FirstSeq: 1, Status: &nisshiv1.AgentStatus{Version: strings.Repeat("v", 10000)}})
	body = append(body, 0x2a, 2, 0x78, 0x01) // status with an unknown field 15
	if code, _, _ := push(t, ts.URL, pushOpts{raw: body}); code != 200 {
		t.Fatalf("code %d", code)
	}
	st := s.agents["node-a"]
	if len(st.Version) != maxVersion || len(st.ProtoReflect().GetUnknown()) != 0 {
		t.Errorf("status kept: %d bytes of version, unknown %v", len(st.Version), st.ProtoReflect().GetUnknown())
	}
	// The cut falls inside a two-byte rune: it keeps the rune whole out, and /v1/status still
	// serialises (validation R3).
	version := strings.Repeat("v", maxVersion-1) + "é"
	body, _ = proto.Marshal(&nisshiv1.PushRequest{Node: "node-a", StreamId: bytes.Repeat([]byte{1}, 16), FirstSeq: 1, Status: &nisshiv1.AgentStatus{Version: version}})
	if code, _, _ := push(t, ts.URL, pushOpts{raw: body}); code != 200 {
		t.Fatalf("code %d", code)
	}
	req, _ := http.NewRequest("GET", ts.URL+"/v1/status", nil)
	req.Header.Set("Authorization", "Bearer tok-r")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got struct {
		Agents map[string]struct{ Version string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); resp.StatusCode != 200 || err != nil || got.Agents["node-a"].Version != version[:maxVersion-1] {
		t.Errorf("status: %d %v %+v", resp.StatusCode, err, got)
	}
}

// Deadlines reclaim a connection whose body never comes, with or without a token, and an idle
// keep-alive connection.
func TestDeadlines(t *testing.T) {
	defer func(r, w, i time.Duration) { readTimeout, writeTimeout, idleTimeout = r, w, i }(readTimeout, writeTimeout, idleTimeout)
	readTimeout, writeTimeout, idleTimeout = 300*time.Millisecond, time.Second, 300*time.Millisecond
	s, _, _ := newTestServer(t, 0)
	ts := httptest.NewUnstartedServer(nil)
	ts.Config = httpServer(s.handler(), s.log)
	ts.Start()
	defer ts.Close()
	for name, req := range map[string]string{
		"body never sent, token":    "POST /v1/push HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-a\r\nContent-Encoding: zstd\r\nContent-Length: 100\r\n\r\n",
		"body never sent, no token": "POST /v1/push HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n",
		"chunked, stalled":          "POST /v1/push HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-a\r\nContent-Encoding: zstd\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab",
		"idle keep-alive":           "GET /v1/status HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok-r\r\n\r\n",
	} {
		c, err := net.Dial("tcp", ts.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		c.Write([]byte(req))
		start := time.Now()
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		io.Copy(io.Discard, c) // until the server closes
		if d := time.Since(start); d > 3*time.Second {
			t.Errorf("%s: open for %v", name, d)
		}
		c.Close()
	}
}

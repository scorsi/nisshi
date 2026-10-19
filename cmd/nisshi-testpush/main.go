// nisshi-testpush drives a running nisshi through the delivery scenarios of fixtures/delivery/
// (docs/delivery-scenarios.md, ADR 0012) or pushes every record fixture of
// fixtures/logs/records.ndjson (ADR 0011). Test-only: not shipped. It reads the store back with
// zstdcat, so that the oracle is a standard tool, and checks that every stored record is the
// pushed bytes plus a recv_ts of the file's day.
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	nisshiv1 "nisshi/gen/nisshi/v1"
)

var (
	url       = flag.String("url", "https://127.0.0.1:9473", "nisshi base URL")
	caFile    = flag.String("ca", "", "PEM CA of the server certificate")
	node      = flag.String("node", "node-a", "the pusher's node (identity of --token-valid)")
	otherNode = flag.String("other-node", "node-b", "another node (identity of --token-other-node)")
	tokValid  = flag.String("token-valid", "", "token of --node")
	tokOther  = flag.String("token-other-node", "", "token of --other-node")
	root      = flag.String("root", "/var/lib/nisshi", "store root, read with zstdcat")
	zstdcat   = flag.String("zstdcat", "zstdcat", "decompressor command")
	restart   = flag.String("restart", "", "shell command for the restart step (kill -9 and start)")
	fill      = flag.String("fill", "", "shell command for disk fill")
	free      = flag.String("free", "", "shell command for disk free")
	fixtures  = flag.String("fixtures", "", "push every record of this fixtures file instead of scenarios")
)

const tsLayout = "2006-01-02T15:04:05.000000000Z"

var recvRe = regexp.MustCompile(`,"recv_ts":"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z)"}$`)

type step struct {
	Push *struct {
		FirstSeq  uint64 `json:"first_seq"`
		Count     int    `json:"count"`
		Token     string `json:"token"`
		Node      string `json:"node"`
		InvalidAt *int   `json:"invalid_at"`
		Stream    string `json:"stream"`
	} `json:"push"`
	Expect map[string]json.RawMessage `json:"expect"`
	Check  *struct {
		StoredSeqs *string `json:"stored_seqs"`
		Duplicates *int    `json:"duplicates"`
		Gaps       *int    `json:"gaps"`
	} `json:"check"`
	Restart *struct{ Kill bool }       `json:"restart"`
	Disk    *struct{ Fill, Free bool } `json:"disk"`
}

type scenario struct {
	Name  string `json:"name"`
	Steps []step `json:"steps"`
}

type pusher struct {
	client  *http.Client
	enc     *zstd.Encoder
	streams map[string]string // "a"/"b" -> hex stream id
	pushed  map[string][]byte // "<stream>/<seq>" -> pushed bytes
}

type stored struct {
	line   []byte // without recv_ts
	stream string
	seq    uint64
	gap    string // gap_stream for seq-gap records
}

func main() {
	flag.Parse()
	p, err := newPusher()
	if err != nil {
		fatal(err)
	}
	if *fixtures != "" {
		if err := p.runFixtures(*fixtures); err != nil {
			fatal(err)
		}
		return
	}
	for _, f := range flag.Args() {
		b, err := os.ReadFile(f)
		if err != nil {
			fatal(err)
		}
		var sc scenario
		if err := json.Unmarshal(b, &sc); err != nil {
			fatal(fmt.Errorf("%s: %w", f, err))
		}
		if err := p.run(sc); err != nil {
			fatal(fmt.Errorf("scenario %s: %w", sc.Name, err))
		}
		fmt.Printf("scenario %s: ok\n", sc.Name)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "nisshi-testpush:", err)
	os.Exit(1)
}

func newPusher() (*pusher, error) {
	tc := &tls.Config{}
	if *caFile != "" {
		pem, err := os.ReadFile(*caFile)
		if err != nil {
			return nil, err
		}
		tc.RootCAs = x509.NewCertPool()
		tc.RootCAs.AppendCertsFromPEM(pem)
	}
	enc, _ := zstd.NewWriter(nil)
	return &pusher{client: &http.Client{Transport: &http.Transport{TLSClientConfig: tc, ForceAttemptHTTP2: true}, Timeout: 30 * time.Second}, enc: enc}, nil
}

func (p *pusher) run(sc scenario) error {
	p.streams = map[string]string{"a": randomStream(), "b": randomStream()}
	p.pushed = map[string][]byte{}
	for i, st := range sc.Steps {
		if err := p.step(st); err != nil {
			return fmt.Errorf("step %d: %w", i+1, err)
		}
	}
	return nil
}

func randomStream() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// record is a valid annai query record; ts = base + seq seconds, stream b one hour earlier,
// so that `nisshi read` output order differs from file order. invalid makes ts 6-digit.
func record(nodeName, stream string, seq uint64, hourOffset int, invalid bool) []byte {
	ts := time.Date(2026, 10, 19, 8+hourOffset, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Second).Format(tsLayout)
	if invalid {
		ts = ts[:26] + "Z"
	}
	return []byte(fmt.Sprintf(`{"v":1,"ts":%q,"level":"info","event":"query","node":%q,"service":"annai","stream":%q,"seq":%d,"proto":"udp","client":"192.0.2.1","qname":"example.org.","qtype":"A","rcode":"NOERROR","dur_us":38}`, ts, nodeName, stream, seq))
}

func (p *pusher) step(st step) error {
	switch {
	case st.Push != nil:
		ps := st.Push
		streamName := ps.Stream
		if streamName == "" {
			streamName = "a"
		}
		stream := p.streams[streamName]
		reqNode, token := *node, *tokValid
		if ps.Node == "other" {
			reqNode = *otherNode
		}
		switch ps.Token {
		case "none":
			token = ""
		case "unknown":
			token = "unknown-token-" + randomStream()
		case "other-node":
			token = *tokOther
		}
		hour := 0
		if streamName == "b" {
			hour = -1
		}
		var recs [][]byte
		for i := 0; i < ps.Count; i++ {
			seq := ps.FirstSeq + uint64(i)
			rec := record(reqNode, stream, seq, hour, ps.InvalidAt != nil && *ps.InvalidAt == i)
			recs = append(recs, rec)
			if ps.InvalidAt == nil && reqNode == *node && ps.Token != "other-node" {
				p.pushed[stream+"/"+strconv.FormatUint(seq, 10)] = rec
			}
		}
		sid, _ := hex.DecodeString(stream)
		code, resp, rej, err := p.push(token, &nisshiv1.PushRequest{Node: reqNode, StreamId: sid, FirstSeq: ps.FirstSeq, Records: recs})
		if err != nil {
			return err
		}
		return expect(st.Expect, code, resp, rej)
	case st.Check != nil:
		rows, err := p.readStore()
		if err != nil {
			return err
		}
		ours := map[string]bool{p.streams["a"]: true, p.streams["b"]: true}
		seen := map[string]int{}
		var seqsA []uint64
		gaps, dups := 0, 0
		for _, r := range rows {
			if r.gap != "" {
				if ours[r.gap] {
					gaps++
				}
				continue
			}
			if !ours[r.stream] {
				continue
			}
			key := r.stream + "/" + strconv.FormatUint(r.seq, 10)
			seen[key]++
			if seen[key] > 1 {
				dups++
			}
			if want := p.pushed[key]; !bytes.Equal(want, r.line) {
				return fmt.Errorf("stored record %s differs from the pushed bytes:\n%s\n%s", key, r.line, want)
			}
			if r.stream == p.streams["a"] {
				seqsA = append(seqsA, r.seq)
			}
		}
		c := st.Check
		if c.StoredSeqs != nil {
			if got := ranges(seqsA); got != *c.StoredSeqs {
				return fmt.Errorf("stored_seqs %q, want %q", got, *c.StoredSeqs)
			}
		}
		if c.Duplicates != nil && dups != *c.Duplicates {
			return fmt.Errorf("duplicates %d, want %d", dups, *c.Duplicates)
		}
		if c.Gaps != nil && gaps != *c.Gaps {
			return fmt.Errorf("gaps %d, want %d", gaps, *c.Gaps)
		}
		return nil
	case st.Restart != nil:
		if err := sh(*restart); err != nil {
			return err
		}
		return p.waitUp()
	case st.Disk != nil:
		if st.Disk.Fill {
			return sh(*fill)
		}
		return sh(*free)
	}
	return fmt.Errorf("unknown step")
}

func sh(cmd string) error {
	if cmd == "" {
		return fmt.Errorf("no command configured for this step")
	}
	c := exec.Command("sh", "-c", cmd)
	c.Stdout, c.Stderr = os.Stdout, os.Stderr
	return c.Run()
}

func (p *pusher) waitUp() error {
	for i := 0; i < 300; i++ {
		if resp, err := p.client.Get(*url + "/v1/status"); err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("nisshi not back after restart")
}

func (p *pusher) push(token string, req *nisshiv1.PushRequest) (int, *nisshiv1.PushResponse, *nisshiv1.PushRejected, error) {
	body, err := proto.Marshal(req)
	if err != nil {
		return 0, nil, nil, err
	}
	r, _ := http.NewRequest("POST", *url+"/v1/push", bytes.NewReader(p.enc.EncodeAll(body, nil)))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Content-Encoding", "zstd")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := p.client.Do(r)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case 200:
		var pr nisshiv1.PushResponse
		if err := proto.Unmarshal(b, &pr); err != nil {
			return 0, nil, nil, fmt.Errorf("200 body: %w", err)
		}
		return 200, &pr, nil, nil
	case 400:
		var rj nisshiv1.PushRejected
		if err := proto.Unmarshal(b, &rj); err != nil {
			return 0, nil, nil, fmt.Errorf("400 body: %w", err)
		}
		return 400, nil, &rj, nil
	}
	return resp.StatusCode, nil, nil, nil
}

// expect checks the response: absent fields are not checked, null means absent.
func expect(ex map[string]json.RawMessage, code int, resp *nisshiv1.PushResponse, rej *nisshiv1.PushRejected) error {
	for k, raw := range ex {
		var got any
		present := false
		switch k {
		case "status":
			got, present = code, true
		case "acked_seq":
			if resp != nil {
				got, present = resp.AckedSeq, true
			}
		case "record_index":
			if rej != nil && rej.RecordIndex != nil {
				got, present = *rej.RecordIndex, true
			}
		case "reason":
			if rej != nil {
				got, present = rej.Reason, true
			}
		default:
			return fmt.Errorf("unknown expect field %s", k)
		}
		if string(raw) == "null" {
			if present {
				return fmt.Errorf("%s: present (%v), want absent", k, got)
			}
			continue
		}
		if !present || fmt.Sprint(got) != strings.Trim(string(raw), `"`) {
			return fmt.Errorf("%s: got %v (present %v), want %s (status %d, response %v %v)", k, got, present, raw, code, resp, rej)
		}
	}
	return nil
}

// readStore decodes every data file of the node with zstdcat and checks the recv_ts member of
// each line against the file's day.
func (p *pusher) readStore() ([]stored, error) {
	files, err := filepath.Glob(filepath.Join(*root, *node, "*.ndjson.zst"))
	if err != nil {
		return nil, err
	}
	var out []stored
	for _, f := range files {
		day := strings.TrimSuffix(filepath.Base(f), ".ndjson.zst")
		data, err := exec.Command(*zstdcat, f).Output()
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", *zstdcat, f, err)
		}
		if len(data) == 0 {
			continue
		}
		for _, line := range bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
			m := recvRe.FindSubmatch(line)
			if m == nil {
				return nil, fmt.Errorf("%s: no recv_ts member: %s", f, line)
			}
			if _, err := time.Parse(tsLayout, string(m[1])); err != nil || !strings.HasPrefix(string(m[1]), day) {
				return nil, fmt.Errorf("%s: recv_ts %s not a ts of day %s", f, m[1], day)
			}
			var e struct {
				Event     string `json:"event"`
				Service   string `json:"service"`
				Stream    string `json:"stream"`
				GapStream string `json:"gap_stream"`
				Seq       uint64 `json:"seq"`
			}
			if err := json.Unmarshal(line, &e); err != nil {
				return nil, fmt.Errorf("%s: %w: %s", f, err, line)
			}
			s := stored{line: append(line[:len(line)-len(m[0])], '}'), stream: e.Stream, seq: e.Seq}
			if e.Event == "seq-gap" && e.Service == "nisshi" {
				s.gap = e.GapStream
			}
			out = append(out, s)
		}
	}
	return out, nil
}

// ranges prints sorted seqs as "1-10,21-30".
func ranges(seqs []uint64) string {
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	var parts []string
	for i := 0; i < len(seqs); {
		j := i
		for j+1 < len(seqs) && seqs[j+1] == seqs[j]+1 {
			j++
		}
		if i == j {
			parts = append(parts, strconv.FormatUint(seqs[i], 10))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", seqs[i], seqs[j]))
		}
		i = j + 1
	}
	return strings.Join(parts, ",")
}

// runFixtures pushes every record fixture as a one-record batch whose stream_id and first_seq
// come from the record itself (defaults when unreadable; the node is always the pusher's):
// invalid ones get 400 with index 0 and the fixture's reason, valid ones 200. A model of the hwm rules predicts the stored set, compared
// byte for byte with the store.
func (p *pusher) runFixtures(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	hwm := map[string]uint64{}
	expected := map[string]int{} // stored line -> count
	gaps := 0
	defaultStream := randomStream()
	for i, line := range bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
		var fx struct {
			Input       *string `json:"input"`
			InputB64    *string `json:"input_b64"`
			InputRepeat *struct {
				Prefix, Repeat, Suffix string
				Count                  int
			} `json:"input_repeat"`
			Valid  bool   `json:"valid"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(line, &fx); err != nil {
			return fmt.Errorf("%s:%d: %w", path, i+1, err)
		}
		var rec []byte
		switch {
		case fx.Input != nil:
			rec = []byte(*fx.Input)
		case fx.InputB64 != nil:
			rec, _ = base64.StdEncoding.DecodeString(*fx.InputB64)
		default:
			r := fx.InputRepeat
			rec = []byte(r.Prefix + strings.Repeat(r.Repeat, r.Count) + r.Suffix)
		}
		stream, seq := defaultStream, uint64(1)
		var env struct {
			Stream string
			Seq    json.Number
		}
		if json.Unmarshal(rec, &env) == nil {
			if len(env.Stream) == 32 {
				if _, err := hex.DecodeString(env.Stream); err == nil {
					stream = env.Stream
				}
			}
			if n, err := strconv.ParseUint(env.Seq.String(), 10, 64); err == nil && n > 0 {
				seq = n
			}
		}
		sid, _ := hex.DecodeString(stream)
		code, resp, rej, err := p.push(*tokValid, &nisshiv1.PushRequest{Node: *node, StreamId: sid, FirstSeq: seq, Records: [][]byte{rec}})
		if err != nil {
			return fmt.Errorf("%s:%d: %w", path, i+1, err)
		}
		if fx.Valid {
			if code != 200 || resp.AckedSeq != max(hwm[stream], seq) {
				return fmt.Errorf("%s:%d: valid record got %d %v %v, want 200 acked %d", path, i+1, code, resp, rej, max(hwm[stream], seq))
			}
			if seq > hwm[stream] {
				if seq > hwm[stream]+1 {
					gaps++
				}
				hwm[stream] = seq
				expected[string(rec)]++
			}
		} else if code != 400 || rej.RecordIndex == nil || *rej.RecordIndex != 0 || rej.Reason != fx.Reason {
			return fmt.Errorf("%s:%d: invalid record got %d %v, want 400 index 0 reason %s", path, i+1, code, rej, fx.Reason)
		}
	}
	rows, err := p.readStore()
	if err != nil {
		return err
	}
	got := map[string]int{}
	gotGaps := 0
	for _, r := range rows {
		if r.gap != "" {
			gotGaps++
			continue
		}
		got[string(r.line)]++
	}
	if gotGaps != gaps {
		return fmt.Errorf("%d gap records, want %d", gotGaps, gaps)
	}
	if len(got) != len(expected) {
		return fmt.Errorf("%d distinct stored records, want %d", len(got), len(expected))
	}
	for k, n := range expected {
		if got[k] != n {
			return fmt.Errorf("record stored %d times, want %d: %.80s", got[k], n, k)
		}
	}
	fmt.Printf("fixtures %s: %d records stored, %d gap records\n", path, len(rows)-gotGaps, gotGaps)
	return nil
}

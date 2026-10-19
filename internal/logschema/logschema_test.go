package logschema

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// fixture is one line of fixtures/logs/*.ndjson; the format is in fixtures/logs/README.md.
type fixture struct {
	Input       *string `json:"input"`
	InputB64    *string `json:"input_b64"`
	InputRepeat *struct {
		Prefix string `json:"prefix"`
		Repeat string `json:"repeat"`
		Count  int    `json:"count"`
		Suffix string `json:"suffix"`
	} `json:"input_repeat"`
	Valid  bool   `json:"valid"`
	Reason string `json:"reason"`
}

func (f fixture) bytes(t *testing.T) []byte {
	switch {
	case f.Input != nil && f.InputB64 == nil && f.InputRepeat == nil:
		return []byte(*f.Input)
	case f.Input == nil && f.InputB64 != nil && f.InputRepeat == nil:
		b, err := base64.StdEncoding.DecodeString(*f.InputB64)
		if err != nil {
			t.Fatal(err)
		}
		return b
	case f.Input == nil && f.InputB64 == nil && f.InputRepeat != nil:
		r := f.InputRepeat
		return []byte(r.Prefix + strings.Repeat(r.Repeat, r.Count) + r.Suffix)
	}
	t.Fatal("exactly one of input, input_b64, input_repeat")
	return nil
}

func loadFixtures(t *testing.T, name string) []fixture {
	data, err := os.ReadFile("../../fixtures/logs/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var out []fixture
	for i, line := range bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
		var f fixture
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&f); err != nil {
			t.Fatalf("%s:%d: %v", name, i+1, err)
		}
		if f.Valid != (f.Reason == "") {
			t.Fatalf("%s:%d: a valid fixture has an empty reason, an invalid one a reason", name, i+1)
		}
		out = append(out, f)
	}
	return out
}

func TestFixtures(t *testing.T) {
	for name, check := range map[string]func([]byte) string{"service-lines.ndjson": Line, "records.ndjson": Record} {
		for i, f := range loadFixtures(t, name) {
			b := f.bytes(t)
			if name == "service-lines.ndjson" && bytes.IndexByte(b, '\n') >= 0 {
				t.Errorf("%s:%d: a service line holds no newline", name, i+1)
			}
			if got := check(b); got != f.Reason {
				t.Errorf("%s:%d: got %q, want %q", name, i+1, got, f.Reason)
			}
		}
	}
}

// Every example of log-schema.md is a valid fixture: service lines in service-lines.ndjson,
// complete records (with the agent's envelope) in records.ndjson.
func TestSchemaExamplesAreFixtures(t *testing.T) {
	doc, err := os.ReadFile("../../docs/log-schema.md")
	if err != nil {
		t.Fatal(err)
	}
	valid := map[string]bool{}
	for _, name := range []string{"service-lines.ndjson", "records.ndjson"} {
		for _, f := range loadFixtures(t, name) {
			if f.Valid && f.Input != nil {
				valid[*f.Input] = true
			}
		}
	}
	n := 0
	for sc := bufio.NewScanner(bytes.NewReader(doc)); sc.Scan(); {
		if line := sc.Text(); strings.HasPrefix(line, `{"v":1`) {
			n++
			if !valid[line] {
				t.Errorf("log-schema.md example is not a valid fixture: %s", line)
			}
		}
	}
	if n == 0 {
		t.Fatal("no example found in log-schema.md")
	}
}

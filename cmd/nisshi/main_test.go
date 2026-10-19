package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestValidateLogs(t *testing.T) {
	ok := `{"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"info","event":"query"}`
	big := `{"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"info","event":"big","pad":"` + strings.Repeat("x", 2<<20) + `"}`
	for _, tc := range []struct {
		name, in string
		code     int
		stderr   string
	}{
		{"empty", "", 0, ""},
		{"valid", ok + "\n" + ok + "\n", 0, ""},
		{"no final newline", ok + "\n" + ok, 0, ""},
		{"first invalid line", ok + "\n" + ok + "\nlevel=info event=query\n{}\n", 1, "line 3: not-json\n"},
		{"line over the scanner buffer", ok + "\n" + big + "\n" + ok + "\n", 1, "line 2: line-too-large\n"},
	} {
		var stderr bytes.Buffer
		if code := run([]string{"validate", "--logs"}, strings.NewReader(tc.in), &stderr); code != tc.code || stderr.String() != tc.stderr {
			t.Errorf("%s: got %d %q, want %d %q", tc.name, code, stderr.String(), tc.code, tc.stderr)
		}
	}
	if code := run([]string{"validate"}, strings.NewReader(""), &bytes.Buffer{}); code != 2 {
		t.Errorf("usage error: got %d, want 2", code)
	}
}

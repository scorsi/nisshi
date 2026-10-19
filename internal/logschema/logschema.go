// Package logschema checks service lines and pushed records against docs/log-schema.md
// (ADR 0004). The reasons it returns are the fixtures' reasons: PushRejected.reason at the
// store, unparsed.reason at the agent.
package logschema

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxLineBytes   = 1 << 20    // a service line
	MaxRecordBytes = 4<<20 - 43 // a pushed record: stored, with the 43-byte `,"recv_ts":"…"` member, at most 4 MiB
)

// Line checks one line written by a service, without its newline. It returns "" when valid.
func Line(b []byte) string { return check(b, false) }

// Record checks one record as pushed by an agent. It returns "" when valid.
func Record(b []byte) string { return check(b, true) }

var (
	tsRe     = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z$`)
	eventRe  = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)
	nodeRe   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	streamRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
	seqRe    = regexp.MustCompile(`^[1-9][0-9]*$`)
)

type field struct {
	name     string
	required bool
	ok       func(raw []byte) bool
}

// Checked in this order; the first failure is the reason (log-schema.md, "Validation order").
var serviceFields = []field{
	{"v", true, func(r []byte) bool { return string(r) == "1" }},
	{"ts", true, func(r []byte) bool {
		s, ok := str(r)
		if !ok || !tsRe.MatchString(s) {
			return false
		}
		_, err := time.Parse("2006-01-02T15:04:05.000000000Z", s)
		return err == nil
	}},
	{"level", true, oneOf("debug", "info", "warn", "error")},
	{"event", true, matches(eventRe)},
	{"msg", false, isString},
	{"err", false, isString},
}

var agentFields = []field{
	{"node", true, matches(nodeRe)},
	{"service", true, func(r []byte) bool { s, ok := str(r); return ok && s != "" }},
	{"stream", true, matches(streamRe)},
	{"seq", true, func(r []byte) bool {
		if !seqRe.Match(r) {
			return false
		}
		_, err := strconv.ParseUint(string(r), 10, 64)
		return err == nil
	}},
	{"source", false, oneOf("journald", "file")},
}

func check(b []byte, record bool) string {
	if record && len(b) > MaxRecordBytes {
		return "record-too-long"
	}
	if !record && len(b) > MaxLineBytes {
		return "line-too-long"
	}
	if !utf8.Valid(b) {
		return "not-utf8" // encoding/json would silently replace the bytes with U+FFFD
	}
	if record && bytes.IndexByte(b, '\n') >= 0 {
		return "embedded-newline" // JSON whitespace, but one record is one line on disk
	}
	obj, order, reason := parseObject(b)
	if reason != "" {
		return reason
	}
	for _, k := range order {
		if k == "recv_ts" || strings.HasPrefix(k, "_") || (!record && slices.ContainsFunc(agentFields, func(f field) bool { return f.name == k })) {
			return "reserved-field:" + k
		}
	}
	fields := serviceFields
	if record {
		fields = append(slices.Clip(serviceFields), agentFields...)
	}
	for _, f := range fields {
		raw, ok := obj[f.name]
		if !ok {
			if f.required {
				return "missing-field:" + f.name
			}
			continue
		}
		if !f.ok(raw) {
			return f.name + "-format"
		}
	}
	return ""
}

// parseObject returns the top-level members of a single JSON object, keys in document order.
func parseObject(b []byte) (map[string][]byte, []string, string) {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, nil, "not-object"
	}
	obj := map[string][]byte{}
	var order []string
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, nil, "not-object"
		}
		k := t.(string) // inside an object, the decoder only yields string keys
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, nil, "not-object"
		}
		if _, dup := obj[k]; dup {
			return nil, nil, "duplicate-field:" + k // parsers disagree on which value wins
		}
		obj[k] = v
		order = append(order, k)
	}
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return nil, nil, "not-object"
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, nil, "not-object" // trailing data after the object
	}
	return obj, order, ""
}

func str(r []byte) (string, bool) {
	var s string
	if len(r) == 0 || r[0] != '"' || json.Unmarshal(r, &s) != nil {
		return "", false
	}
	return s, true
}

func isString(r []byte) bool { _, ok := str(r); return ok }

func matches(re *regexp.Regexp) func([]byte) bool {
	return func(r []byte) bool { s, ok := str(r); return ok && re.MatchString(s) }
}

func oneOf(vals ...string) func([]byte) bool {
	return func(r []byte) bool { s, ok := str(r); return ok && slices.Contains(vals, s) }
}

package store

import (
	"encoding/json"
	"io"
	"path/filepath"
	"sort"
	"time"
)

// Query selects records for Read (ADR 0010). Service and Event are optional.
type Query struct {
	Node, Service, Event string
	From, To             time.Time
}

// Read prints the matching records of a store as NDJSON sorted by ts, pruning sealed days with
// their manifests. It needs no server and works on a copy of the store.
func Read(root string, q Query, w io.Writer) error {
	s := &Store{root: root}
	from, to := q.From.UTC().Format(TsLayout), q.To.UTC().Format(TsLayout)
	ms, err := manifests(filepath.Join(root, q.Node))
	if err != nil {
		return err
	}
	byDay := map[string]Manifest{}
	for _, m := range ms {
		byDay[m.Day] = m
	}
	type rec struct{ ts, line string }
	var recs []rec
	for _, f := range s.dataFiles() {
		if f.node != q.Node {
			continue
		}
		if m, ok := byDay[f.day]; ok && (m.MaxTs < from || m.MinTs > to) {
			continue
		}
		if _, _, err := readLines(f.path, func(line []byte) error {
			var e envelope
			if err := json.Unmarshal(line, &e); err != nil {
				return err
			}
			if e.Ts >= from && e.Ts <= to && (q.Service == "" || e.Service == q.Service) && (q.Event == "" || e.Event == q.Event) {
				recs = append(recs, rec{e.Ts, string(line)})
			}
			return nil
		}); err != nil {
			return err
		}
	}
	// ts is fixed width: a string sort is a time sort.
	sort.SliceStable(recs, func(i, j int) bool { return recs[i].ts < recs[j].ts })
	for _, r := range recs {
		if _, err := io.WriteString(w, r.line+"\n"); err != nil {
			return err
		}
	}
	return nil
}

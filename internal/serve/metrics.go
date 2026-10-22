package serve

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// metrics is the hand-written exposition of ADR 0010, in masu's metric convention: nisshi_*,
// counters _total, base units, closed label sets only.
type metrics struct {
	mu       sync.Mutex
	counters map[string]map[string]uint64 // family -> label value -> count
}

var families = []struct{ key, name, label, help string }{
	{"records", "nisshi_records_total", "source_node", "Records stored, by pushing node."},
	{"duplicates", "nisshi_duplicate_records_total", "", "Records dropped because their seq was already committed."},
	{"gaps", "nisshi_seq_gaps_total", "", "seq-gap records written."},
	{"pushes", "nisshi_push_requests_total", "code", "POST /v1/push requests, by HTTP status code."},
	{"seals", "nisshi_seals_total", "result", "Day files sealed, by result."},
}

func (m *metrics) init() {
	m.counters = map[string]map[string]uint64{}
	for _, f := range families {
		m.counters[f.key] = map[string]uint64{}
	}
	m.counters["duplicates"][""] = 0
	m.counters["gaps"][""] = 0
	m.counters["seals"]["ok"] = 0
	m.counters["seals"]["error"] = 0
}

func (m *metrics) add(family, label string, n uint64) {
	m.mu.Lock()
	m.counters[family][label] += n
	m.mu.Unlock()
}

func (s *Server) serveMetrics(w http.ResponseWriter, _ *http.Request) {
	var b strings.Builder
	s.metrics.mu.Lock()
	for _, f := range families {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n", f.name, f.help, f.name)
		vals := s.metrics.counters[f.key]
		keys := make([]string, 0, len(vals))
		for k := range vals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if f.label == "" {
				fmt.Fprintf(&b, "%s %d\n", f.name, vals[k])
			} else {
				fmt.Fprintf(&b, "%s{%s=%q} %d\n", f.name, f.label, k, vals[k])
			}
		}
	}
	s.metrics.mu.Unlock()
	perNode := map[string]int{}
	for _, st := range s.st.Streams() {
		perNode[st.Node]++
	}
	fmt.Fprintf(&b, "# HELP nisshi_streams Known streams (hwm kept forever), by owning node.\n# TYPE nisshi_streams gauge\n")
	for _, n := range sortedKeys(perNode) {
		fmt.Fprintf(&b, "nisshi_streams{source_node=%q} %d\n", n, perNode[n])
	}
	free, _ := s.st.FreeBytes()
	fmt.Fprintf(&b, "# HELP nisshi_disk_free_bytes Free space on the store's filesystem.\n# TYPE nisshi_disk_free_bytes gauge\nnisshi_disk_free_bytes %d\n", free)
	fmt.Fprintf(&b, "# HELP nisshi_disk_reserve_bytes Free space below which pushes get 507.\n# TYPE nisshi_disk_reserve_bytes gauge\nnisshi_disk_reserve_bytes %d\n", s.cfg.DiskReserve)
	fmt.Fprintf(&b, "# HELP nisshi_build_info Build information.\n# TYPE nisshi_build_info gauge\nnisshi_build_info{version=%q} 1\n", Version)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(b.String()))
}

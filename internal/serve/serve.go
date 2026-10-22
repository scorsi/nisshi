package serve

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	nisshiv1 "nisshi/gen/nisshi/v1"
	"nisshi/internal/logschema"
	"nisshi/internal/store"
)

// Version is set by the build (-X nisshi/internal/serve.Version=...).
var Version = "dev"

const (
	maxCompressed   = 8 << 20
	maxDecompressed = 64 << 20
)

// Server serves one store.
type Server struct {
	cfg     Config
	st      *store.Store
	log     *slog.Logger
	now     func() time.Time
	dec     *zstd.Decoder
	metrics metrics
	mu      sync.Mutex
	agents  map[string]*nisshiv1.AgentStatus
}

// Clock returns the process clock: NISSHI_FAKE_NOW (RFC 3339, tests only) freezes it.
func Clock() (func() time.Time, error) {
	if v := os.Getenv("NISSHI_FAKE_NOW"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return nil, fmt.Errorf("NISSHI_FAKE_NOW: %w", err)
		}
		return func() time.Time { return t }, nil
	}
	return time.Now, nil
}

// NewLogger writes the schema's JSON lines to w (log-schema.md, "Reference code").
func NewLogger(w io.Writer) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				return slog.String("ts", a.Value.Time().UTC().Format(store.TsLayout))
			case slog.LevelKey:
				return slog.String("level", strings.ToLower(a.Value.String()))
			case slog.MessageKey:
				if a.Value.String() == "" {
					return slog.Attr{}
				}
			}
			return a
		},
	})
	return slog.New(h).With("v", 1)
}

// logWriter turns each line of a stdlib logger into a schema line.
type logWriter struct{ log *slog.Logger }

func (w logWriter) Write(p []byte) (int, error) {
	w.log.Warn("", "event", "http-error", "err", strings.TrimSpace(string(p)))
	return len(p), nil
}

// errorLog is http.Server.ErrorLog: net/http otherwise writes plain text (TLS handshake errors,
// handler panics) to stderr, outside the log schema.
func errorLog(l *slog.Logger) *log.Logger { return log.New(logWriter{l}, "", 0) }

// Run serves until SIGTERM or SIGINT. It notifies systemd when the listeners are bound.
func Run(cfg Config, log *slog.Logger) error {
	now, err := Clock()
	if err != nil {
		return err
	}
	s, err := newServer(cfg, log, now)
	if err != nil {
		return err
	}
	s.sealAndRetain()

	cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.handler(), TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, ReadHeaderTimeout: 10 * time.Second, ErrorLog: errorLog(log)}
	var servers []*http.Server
	var listeners []net.Listener
	for _, addr := range cfg.Listen {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		listeners = append(listeners, ln)
	}
	if cfg.ListenMetrics != "" {
		ln, err := net.Listen("tcp", cfg.ListenMetrics)
		if err != nil {
			return err
		}
		mm := http.NewServeMux()
		mm.HandleFunc("GET /metrics", s.serveMetrics)
		msrv := &http.Server{Handler: mm, ReadHeaderTimeout: 10 * time.Second, ErrorLog: errorLog(log)}
		servers = append(servers, msrv)
		go msrv.Serve(ln)
	}
	errc := make(chan error, len(listeners))
	for _, ln := range listeners {
		go func() {
			if err := srv.ServeTLS(ln, "", ""); !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}()
	}
	servers = append(servers, srv)
	log.Info("", "event", "start", "version", Version, "listen", cfg.Listen, "data_dir", cfg.DataDir)
	if err := notify("READY=1"); err != nil {
		log.Warn("", "event", "notify-failed", "err", err.Error())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go s.schedule(ctx)
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	notify("STOPPING=1")
	for _, h := range servers {
		h.Close()
	}
	log.Info("", "event", "stop")
	return err
}

func newServer(cfg Config, log *slog.Logger, now func() time.Time) (*Server, error) {
	st, err := store.Open(cfg.DataDir, cfg.DiskReserve, now)
	if err != nil {
		return nil, err
	}
	if n, ok := st.Rebuilt(); ok {
		log.Info("", "event", "restore", "streams", n)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecompressed))
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, st: st, log: log, now: now, dec: dec, agents: map[string]*nisshiv1.AgentStatus{}}
	s.metrics.init()
	return s, nil
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/push", s.push)
	mux.HandleFunc("GET /v1/status", s.status)
	return mux
}

// schedule seals and applies retention at 00:10 UTC every day (ADR 0006).
func (s *Server) schedule(ctx context.Context) {
	for {
		now := s.now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day(), 0, 10, 0, 0, time.UTC)
		if !next.After(now) {
			next = next.AddDate(0, 0, 1)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(next.Sub(now)):
			s.sealAndRetain()
		}
	}
}

func (s *Server) sealAndRetain() {
	sealed, errs := s.st.SealPast()
	for _, d := range sealed {
		s.metrics.add("seals", "ok", 1)
		s.log.Info("", "event", "seal", "day", d)
	}
	for _, err := range errs {
		s.metrics.add("seals", "error", 1)
		s.log.Error("", "event", "seal-failed", "err", err.Error())
	}
	deleted, err := s.st.Retain(s.cfg.RetentionDays)
	for _, d := range deleted {
		s.log.Info("", "event", "retention", "day", d)
	}
	if err != nil {
		s.log.Error("", "event", "retention-failed", "err", err.Error())
	}
}

// identity authenticates the Bearer token: "" when unknown.
func (s *Server) identity(r *http.Request) string {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return ""
	}
	sum := sha256.Sum256([]byte(tok))
	h := []byte(hex.EncodeToString(sum[:]))
	id := ""
	for k, v := range s.cfg.Tokens {
		if subtle.ConstantTimeCompare(h, []byte(k)) == 1 {
			id = v
		}
	}
	return id
}

func (s *Server) push(w http.ResponseWriter, r *http.Request) {
	code := s.doPush(w, r)
	s.metrics.add("pushes", fmt.Sprint(code), 1)
}

func reject(w http.ResponseWriter, index *uint32, reason string) int {
	return refuse(w, http.StatusBadRequest, index, reason)
}

func refuse(w http.ResponseWriter, code int, index *uint32, reason string) int {
	b, _ := proto.Marshal(&nisshiv1.PushRejected{RecordIndex: index, Reason: reason})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(code)
	w.Write(b)
	return code
}

// forbidStream answers a push to a stream the node does not own (ADR 0014).
func (s *Server) forbidStream(w http.ResponseWriter, node, stream string) int {
	s.log.Warn("", "event", "stream-owner", "source_node", node, "stream_id", stream)
	return refuse(w, http.StatusForbidden, nil, "stream-owner")
}

// deny answers a request whose identity lacks the endpoint's role: 401 without a known token,
// 403 for a valid token of another role (ADR 0013).
func deny(w http.ResponseWriter, id string) int {
	if id != "" {
		w.WriteHeader(http.StatusForbidden)
		return http.StatusForbidden
	}
	w.Header().Set("WWW-Authenticate", "Bearer")
	w.WriteHeader(http.StatusUnauthorized)
	return http.StatusUnauthorized
}

// doPush implements the response table of ADR 0003 and returns the status code sent.
func (s *Server) doPush(w http.ResponseWriter, r *http.Request) int {
	id := s.identity(r)
	node, ok := strings.CutPrefix(id, "node:")
	if !ok {
		return deny(w, id)
	}
	if r.ContentLength > maxCompressed {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return http.StatusRequestEntityTooLarge
	}
	if !strings.EqualFold(r.Header.Get("Content-Encoding"), "zstd") {
		return reject(w, nil, "content-encoding")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCompressed))
	if err != nil {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return http.StatusRequestEntityTooLarge
	}
	if body, err = s.dec.DecodeAll(body, nil); err != nil {
		if errors.Is(err, zstd.ErrDecoderSizeExceeded) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return http.StatusRequestEntityTooLarge
		}
		return reject(w, nil, "body-zstd")
	}
	var req nisshiv1.PushRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return reject(w, nil, "body-protobuf")
	}
	switch {
	case req.Node != node:
		return reject(w, nil, "node-mismatch")
	case len(req.StreamId) != 16:
		return reject(w, nil, "stream-id-format")
	case req.FirstSeq == 0:
		return reject(w, nil, "first-seq-zero")
	}
	stream := hex.EncodeToString(req.StreamId)
	if err := s.st.CheckOwner(node, stream); err != nil {
		return s.forbidStream(w, node, stream)
	}
	b := store.Batch{Node: node, Stream: stream, FirstSeq: req.FirstSeq, Records: req.Records}
	for i, rec := range req.Records {
		f, reason := logschema.RecordFields(rec)
		switch {
		case reason != "":
		case f.Node != node:
			reason = "node-mismatch"
		case f.Stream != stream:
			reason = "stream-mismatch"
		case f.Seq != req.FirstSeq+uint64(i):
			reason = "seq-mismatch"
		}
		if reason != "" {
			idx := uint32(i)
			return reject(w, &idx, reason)
		}
		if f.Ts > b.LastTs {
			b.LastTs = f.Ts
		}
	}
	res, err := s.st.Push(b)
	if errors.Is(err, store.ErrStreamOwner) {
		return s.forbidStream(w, node, stream)
	}
	if err != nil {
		s.log.Error("", "event", "push-failed", "source_node", node, "err", err.Error())
		if errors.Is(err, store.ErrDisk) {
			w.WriteHeader(http.StatusInsufficientStorage)
			return http.StatusInsufficientStorage
		}
		if errors.Is(err, store.ErrSealedDay) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return http.StatusServiceUnavailable
		}
		w.WriteHeader(http.StatusInternalServerError)
		return http.StatusInternalServerError
	}
	s.metrics.add("records", node, uint64(res.Stored))
	s.metrics.add("duplicates", "", uint64(res.Duplicates))
	if res.Gap {
		s.metrics.add("gaps", "", 1)
	}
	if req.Status != nil {
		s.mu.Lock()
		s.agents[node] = req.Status
		s.mu.Unlock()
	}
	out, _ := proto.Marshal(&nisshiv1.PushResponse{AckedSeq: res.Acked})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(out)
	return http.StatusOK
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if id := s.identity(r); id != "reader" {
		deny(w, id)
		return
	}
	resp := &nisshiv1.StatusResponse{Agents: map[string]*nisshiv1.AgentStatus{}, DiskReserveBytes: s.cfg.DiskReserve}
	streams := s.st.Streams()
	for _, id := range sortedKeys(streams) {
		st := streams[id]
		sid, _ := hex.DecodeString(id)
		var recNs int64
		if t, err := time.Parse(store.TsLayout, st.LastRecordTs); err == nil {
			recNs = t.UnixNano()
		}
		resp.Streams = append(resp.Streams, &nisshiv1.StatusResponse_Stream{Node: st.Node, StreamId: sid, AckedSeq: st.Hwm, LastReceiveNs: st.LastReceiveNs, LastRecordNs: recNs})
	}
	s.mu.Lock()
	for k, v := range s.agents {
		resp.Agents[k] = v
	}
	s.mu.Unlock()
	resp.DiskFreeBytes, _ = s.st.FreeBytes()
	heads, err := s.st.ChainHeads()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.ChainHeads = heads
	b, _ := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(append(b, '\n'))
}

// notify writes state to systemd's NOTIFY_SOCKET; a no-op outside a Type=notify unit. Go maps
// a leading '@' to Linux's abstract namespace.
func notify(state string) error {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return nil
	}
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.Write([]byte(state))
	return err
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

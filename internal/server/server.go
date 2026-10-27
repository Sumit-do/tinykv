// Package server exposes a tinykv.DB over HTTP with JSON errors.
//
// Routes:
//
//	GET    /healthz             liveness probe
//	GET    /stats               store statistics
//	POST   /compact             run compaction
//	POST   /batch               apply a JSON array of operations atomically
//	GET    /kv?prefix=p         list keys with prefix (sorted)
//	GET    /kv/{key}            read a value (raw bytes)
//	PUT    /kv/{key}[?ttl=30s]  write a value (request body = value)
//	DELETE /kv/{key}            delete a key
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sumit-do/tinykv"
)

// MaxValueBytes caps the size of a PUT body or batch request body.
const MaxValueBytes = 32 << 20

// Server is an http.Handler serving a tinykv.DB.
type Server struct {
	db  *tinykv.DB
	log *slog.Logger
	mux *http.ServeMux
}

// New builds a Server. A nil logger falls back to slog.Default().
func New(db *tinykv.DB, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{db: db, log: logger, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", methods{http.MethodGet: s.handleHealth}.handler)
	s.mux.HandleFunc("/stats", methods{http.MethodGet: s.handleStats}.handler)
	s.mux.HandleFunc("/compact", methods{http.MethodPost: s.handleCompact}.handler)
	s.mux.HandleFunc("/batch", methods{http.MethodPost: s.handleBatch}.handler)
	s.mux.HandleFunc("/kv", methods{http.MethodGet: s.handleList}.handler)
	s.mux.HandleFunc("/kv/{key...}", methods{
		http.MethodGet:    s.handleGet,
		http.MethodPut:    s.handlePut,
		http.MethodDelete: s.handleDelete,
	}.handler)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "no such route")
	})
}

// methods dispatches on HTTP method and answers 405 as JSON otherwise.
type methods map[string]http.HandlerFunc

func (m methods) handler(w http.ResponseWriter, r *http.Request) {
	if h, ok := m[r.Method]; ok {
		h(w, r)
		return
	}
	allowed := make([]string, 0, len(m))
	for k := range m {
		allowed = append(allowed, k)
	}
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

// ServeHTTP implements http.Handler with panic recovery and request logging.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		if rec := recover(); rec != nil {
			s.log.Error("panic serving request", "method", r.Method, "path", r.URL.Path, "panic", rec)
			if !rw.wrote {
				writeError(rw, http.StatusInternalServerError, "internal error")
			}
		}
		s.log.Debug("request", "method", r.Method, "path", r.URL.Path,
			"status", rw.status, "dur", time.Since(start))
	}()
	s.mux.ServeHTTP(rw, r)
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// dbError maps engine errors to HTTP responses.
func dbError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, tinykv.ErrNotFound):
		writeError(w, http.StatusNotFound, "key not found")
	case errors.Is(err, tinykv.ErrEmptyKey), errors.Is(err, tinykv.ErrKeyTooLarge),
		errors.Is(err, tinykv.ErrValueTooLarge), errors.Is(err, tinykv.ErrInvalidOp):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, tinykv.ErrClosed):
		writeError(w, http.StatusServiceUnavailable, "database is closed")
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	st, err := s.db.Stats()
	if err != nil {
		dbError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleCompact(w http.ResponseWriter, _ *http.Request) {
	before, err := s.db.Stats()
	if err != nil {
		dbError(w, err)
		return
	}
	start := time.Now()
	if err := s.db.Compact(); err != nil {
		dbError(w, err)
		return
	}
	after, err := s.db.Stats()
	if err != nil {
		dbError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"before":      before,
		"after":       after,
		"duration_ms": time.Since(start).Milliseconds(),
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	keys, err := s.db.Keys(r.URL.Query().Get("prefix"))
	if err != nil {
		dbError(w, err)
		return
	}
	if keys == nil {
		keys = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys, "count": len(keys)})
}

func keyFrom(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key must not be empty")
		return "", false
	}
	return key, true
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key, ok := keyFrom(w, r)
	if !ok {
		return
	}
	val, err := s.db.Get(key)
	if err != nil {
		dbError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(len(val)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(val)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key, ok := keyFrom(w, r)
	if !ok {
		return
	}
	var ttl time.Duration
	if raw := r.URL.Query().Get("ttl"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			writeError(w, http.StatusBadRequest, "invalid ttl: use a Go duration such as 30s or 5m")
			return
		}
		ttl = d
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxValueBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "value too large")
		return
	}
	if err := s.db.SetTTL(key, body, ttl); err != nil {
		dbError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key, ok := keyFrom(w, r)
	if !ok {
		return
	}
	if err := s.db.Delete(key); err != nil {
		dbError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// BatchOp is one element of a POST /batch request body.
type BatchOp struct {
	Op    string `json:"op"`              // "set" or "delete"
	Key   string `json:"key"`             //
	Value string `json:"value,omitempty"` // set only
	TTL   string `json:"ttl,omitempty"`   // set only; Go duration, e.g. "30s"
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	var reqOps []BatchOp
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxValueBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&reqOps); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(reqOps) == 0 {
		writeError(w, http.StatusBadRequest, "batch must contain at least one operation")
		return
	}
	ops := make([]tinykv.Op, 0, len(reqOps))
	for i, o := range reqOps {
		op := tinykv.Op{Key: o.Key}
		switch strings.ToLower(o.Op) {
		case "set":
			op.Type = tinykv.OpSet
			op.Value = []byte(o.Value)
			if o.TTL != "" {
				d, err := time.ParseDuration(o.TTL)
				if err != nil || d < 0 {
					writeError(w, http.StatusBadRequest, fmt.Sprintf("op %d: invalid ttl %q", i, o.TTL))
					return
				}
				op.TTL = d
			}
		case "delete", "del":
			op.Type = tinykv.OpDelete
		default:
			writeError(w, http.StatusBadRequest, fmt.Sprintf("op %d: unknown op %q (want set or delete)", i, o.Op))
			return
		}
		ops = append(ops, op)
	}
	if err := s.db.Batch(ops); err != nil {
		dbError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"applied": len(ops)})
}

// ListenAndServe serves on addr until ctx is cancelled, then shuts down
// gracefully, letting in-flight requests finish (up to 10 seconds).
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve is ListenAndServe on an existing listener.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	s.log.Info("tinykv listening", "addr", ln.Addr().String())

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.log.Info("shutting down")
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		<-errCh
		return nil
	}
}

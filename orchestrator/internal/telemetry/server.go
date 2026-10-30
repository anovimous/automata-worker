package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net"
	"net/http"
	"time"
)

// source is what the handlers read; *Tracker is one.
type source interface {
	Snapshot() Status
	Health() (ok bool, progressAge time.Duration, phase Phase)
}

// Server serves GET /healthz and /status. It is read-only and unauthenticated: it binds to localhost or a private
// Docker network and its documents hold no secret.
type Server struct {
	addr string
	srv  *http.Server
	ln   net.Listener
}

func NewServer(addr string, t *Tracker) *Server {
	return newServer(addr, t)
}

func newServer(addr string, src source) *Server {
	return &Server{addr: addr, srv: &http.Server{
		Handler:           handler(src),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}}
}

// Start binds synchronously (a bind error stops the start of the orchestrator) and serves in a goroutine.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen for telemetry on %s: %w", s.addr, err)
	}
	s.ln = ln
	go func() {
		if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("the telemetry server stopped", "error", err)
		}
	}()
	return nil
}

// Addr is the address the server listens on, once started.
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.addr
	}
	return s.ln.Addr().String()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// handler routes GET (and HEAD) /healthz and /status; the mux answers 405 to the other methods and 404 elsewhere.
func handler(src source) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ok, age, phase := src.Health()
		if ok {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprint(w, "ok")
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"reason":             "STUCK",
			"phase":              phase,
			"progressAgeSeconds": int(age.Seconds()),
		})
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, src.Snapshot())
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Warn("failed to write a telemetry answer", "error", err)
	}
}

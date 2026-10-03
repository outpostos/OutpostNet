package observation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	// SnapshotPath and EventsPath are the Range observation API paths.
	SnapshotPath = "/outpostrange/observation/v1alpha1/snapshot"
	EventsPath   = "/outpostrange/observation/v1alpha1/events"
)

// Server is the localhost-only snapshot and SSE endpoint for one Reporter.
type Server struct {
	addr     string
	reporter *Reporter
	logger   *slog.Logger

	mu   sync.Mutex
	ln   net.Listener
	http *http.Server
}

// NewServer validates that addr is a loopback address.
func NewServer(addr string, reporter *Reporter, logger *slog.Logger) (*Server, error) {
	if err := validateLoopbackAddr(addr); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{addr: addr, reporter: reporter, logger: logger}, nil
}

// Start binds the listener and serves in the background until ctx is done.
// Every failure (invalid or taken address, serve error) is logged and
// returned for the host to log; it never stops the host.
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("observation: listen %s: %w", s.addr, err)
	}
	if tcp, ok := ln.Addr().(*net.TCPAddr); ok && !tcp.IP.IsLoopback() {
		ln.Close()
		return fmt.Errorf("observation: listener is not loopback-only: %s", ln.Addr())
	}
	mux := http.NewServeMux()
	mux.HandleFunc(SnapshotPath, s.handleSnapshot)
	mux.HandleFunc(EventsPath, s.handleEvents)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	s.mu.Lock()
	s.ln = ln
	s.http = srv
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			srv.Close()
		}
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("observation endpoint stopped; product keeps running without it", "error", err)
		}
	}()
	s.logger.Info("observation endpoint listening", "addr", ln.Addr().String())
	return nil
}

// Addr is the bound address, or nil before a successful Start.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := protojson.Marshal(s.reporter.Snapshot())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	var afterSeq uint64
	if raw := strings.TrimSpace(r.URL.Query().Get("after_seq")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid after_seq %q", raw), http.StatusBadRequest)
			return
		}
		afterSeq = parsed
	}
	backlog, ch, cancel, err := s.reporter.Log().SubscribeAfter(afterSeq)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrEventGap) {
			status = http.StatusGone
		}
		http.Error(w, err.Error(), status)
		return
	}
	defer cancel()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	for _, event := range backlog {
		if err := writeSSE(w, event); err != nil {
			return
		}
	}
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			if err := writeSSE(w, event); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event *observationv1alpha1.Event) error {
	body, err := protojson.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", event.GetSeq(), body)
	return err
}

func validateLoopbackAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("observation: listen addr %q: %w", addr, err)
	}
	if port == "" {
		return fmt.Errorf("observation: listen addr %q has no port", addr)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("observation: listen addr must be loopback-only, got %q", addr)
	}
	return nil
}

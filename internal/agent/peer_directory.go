package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel"
)

type Peer struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

type PeerStatus struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
	Status string `json:"status"`
}

type peerEntry struct {
	Peer
	clientRole bool
}

type PeerDirectory struct {
	nodeID  string
	tunnels *TunnelManager
	logger  *slog.Logger

	mu    sync.RWMutex
	peers map[string]peerEntry
}

func NewPeerDirectory(nodeID string, tunnels *TunnelManager, logger *slog.Logger) *PeerDirectory {
	if logger == nil {
		logger = slog.Default()
	}
	return &PeerDirectory{
		nodeID:  nodeID,
		tunnels: tunnels,
		logger:  logger,
		peers:   make(map[string]peerEntry),
	}
}

func (pd *PeerDirectory) Replace(ctx context.Context, peers []Peer) error {
	next := make(map[string]Peer, len(peers))
	for _, p := range peers {
		if p.NodeID == "" {
			return fmt.Errorf("node_id is required")
		}
		if p.Addr == "" {
			return fmt.Errorf("addr is required for node_id %q", p.NodeID)
		}
		if p.NodeID == pd.nodeID {
			continue
		}
		next[p.NodeID] = p
	}

	type upsert struct {
		peer       Peer
		clientRole bool
	}
	var upserts []upsert
	var removes []peerEntry

	pd.mu.Lock()
	for nodeID, old := range pd.peers {
		if _, ok := next[nodeID]; !ok {
			removes = append(removes, old)
			delete(pd.peers, nodeID)
		}
	}
	for nodeID, p := range next {
		// Deterministic one-tunnel-per-node-pair rule: the lexicographically
		// smaller node_id acts as QUIC client, so both sides make the same role
		// decision without coordination.
		clientRole := pd.nodeID < nodeID
		old, exists := pd.peers[nodeID]
		if !exists || old.Addr != p.Addr || old.clientRole != clientRole {
			pd.peers[nodeID] = peerEntry{Peer: p, clientRole: clientRole}
			upserts = append(upserts, upsert{peer: p, clientRole: clientRole})
		}
	}
	pd.mu.Unlock()

	for _, old := range removes {
		if old.clientRole {
			pd.tunnels.RemovePeerClient(old.NodeID)
		}
		pd.logger.Info("peer removed", "node_id", old.NodeID, "addr", old.Addr)
	}
	for _, u := range upserts {
		if !u.clientRole {
			pd.logger.Info("peer accepted in server role", "node_id", u.peer.NodeID, "addr", u.peer.Addr)
			continue
		}
		if err := pd.tunnels.AddOrUpdatePeerClient(ctx, u.peer.NodeID, u.peer.Addr); err != nil {
			return err
		}
		pd.logger.Info("peer client ensured", "node_id", u.peer.NodeID, "addr", u.peer.Addr)
	}

	return nil
}

func (pd *PeerDirectory) Statuses() []PeerStatus {
	pd.mu.RLock()
	entries := make([]peerEntry, 0, len(pd.peers))
	for _, p := range pd.peers {
		entries = append(entries, p)
	}
	pd.mu.RUnlock()

	out := make([]PeerStatus, 0, len(entries))
	for _, p := range entries {
		status := "pending"
		if p.clientRole {
			if state, ok := pd.tunnels.DynamicClientState(p.NodeID); ok {
				status = statusFromConnState(state)
			}
		} else {
			status = "pending"
		}
		out = append(out, PeerStatus{
			NodeID: p.NodeID,
			Addr:   p.Addr,
			Status: status,
		})
	}
	return out
}

func statusFromConnState(state tunnel.ConnState) string {
	switch state {
	case tunnel.StateConnected:
		return "connected"
	case tunnel.StateConnecting, tunnel.StateReconnecting:
		return "reconnecting"
	default:
		return "pending"
	}
}

type ManagementServer struct {
	socketPath string
	directory  *PeerDirectory
	logger     *slog.Logger
	server     *http.Server
	listener   net.Listener
}

func NewManagementServer(socketPath string, directory *PeerDirectory, logger *slog.Logger) *ManagementServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ManagementServer{
		socketPath: socketPath,
		directory:  directory,
		logger:     logger,
	}
}

func (ms *ManagementServer) Start(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(ms.socketPath), 0755); err != nil {
		return fmt.Errorf("create management socket dir: %w", err)
	}
	if err := os.RemoveAll(ms.socketPath); err != nil {
		return fmt.Errorf("remove stale management socket: %w", err)
	}

	ln, err := net.Listen("unix", ms.socketPath)
	if err != nil {
		return fmt.Errorf("listen unix management socket: %w", err)
	}
	ms.listener = ln

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/peers", ms.handlePeers(ctx))
	ms.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		_ = ms.Stop()
	}()
	go func() {
		if err := ms.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			ms.logger.Error("management server stopped", "error", err)
		}
	}()
	ms.logger.Info("management server started", "socket_path", ms.socketPath)
	return nil
}

func (ms *ManagementServer) Stop() error {
	if ms.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ms.server.Shutdown(ctx)
	}
	if ms.listener != nil {
		_ = ms.listener.Close()
	}
	if ms.socketPath != "" {
		_ = os.Remove(ms.socketPath)
	}
	return nil
}

func (ms *ManagementServer) handlePeers(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
				http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
				return
			}
			defer r.Body.Close()
			var peers []Peer
			if err := json.NewDecoder(r.Body).Decode(&peers); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := ms.directory.Replace(ctx, peers); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(ms.directory.Statuses()); err != nil {
				ms.logger.Error("encode peers response", "error", err)
			}
		default:
			w.Header().Set("Allow", "GET, PUT")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

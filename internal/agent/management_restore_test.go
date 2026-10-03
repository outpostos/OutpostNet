package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
)

// The peer directory is kept in memory only, so a restarted agent starts with
// an empty directory until its feeder (OutpostLet) pushes again. The
// migration/rollback runbook (docs/service-migration.md) restores it by
// saving `GET /v1/peers` before the restart and sending that body unchanged
// to `PUT /v1/peers` afterwards. This pins that the GET output (which adds
// "status") is accepted by PUT and restores the same node_id -> addr table.
func TestPeerDirectoryGetOutputRestoresThroughPut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := os.MkdirTemp("/tmp", "onetmgmt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	oldSock := filepath.Join(dir, "old.sock")
	newSock := filepath.Join(dir, "new.sock")
	startManagement(ctx, t, oldSock)
	startManagement(ctx, t, newSock)

	seed := `[{"node_id":"node-b","addr":"192.0.2.12:5252"},{"node_id":"node-c","addr":"192.0.2.13:5252"}]`
	if code := doPeers(t, oldSock, http.MethodPut, []byte(seed)); code != http.StatusAccepted {
		t.Fatalf("seed PUT: status %d", code)
	}
	saved := getPeers(t, oldSock)

	if got := getPeers(t, newSock); !bytes.Equal(bytes.TrimSpace(got), []byte("[]")) {
		t.Fatalf("restarted directory should start empty, got %s", got)
	}
	if code := doPeers(t, newSock, http.MethodPut, saved); code != http.StatusAccepted {
		t.Fatalf("PUT of saved GET output: status %d", code)
	}

	if want, got := peerTable(t, saved), peerTable(t, getPeers(t, newSock)); len(want) != 2 || !equalTables(want, got) {
		t.Fatalf("restored table %v, want %v", got, want)
	}
}

func startManagement(ctx context.Context, t *testing.T, sock string) {
	t.Helper()
	tm := NewTunnelManager(&agentconfig.Config{TLS: agentconfig.TLSConfig{Insecure: true}}, nil, nil)
	tm.connectPeerClient = func(ctx context.Context, mc *tunnel.ManagedClient) error { return nil }
	tm.connectPeerClientUntil = func(ctx context.Context, mc *tunnel.ManagedClient) {}
	ms := NewManagementServer(sock, NewPeerDirectory("node-a", tm, nil), nil)
	if err := ms.Start(ctx); err != nil {
		t.Fatalf("start management %s: %v", sock, err)
	}
	t.Cleanup(func() { ms.Stop() })
}

func doPeers(t *testing.T, sock, method string, body []byte) int {
	t.Helper()
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	req, err := http.NewRequest(method, "http://unix/v1/peers", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s /v1/peers on %s: %v", method, sock, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func getPeers(t *testing.T, sock string) []byte {
	t.Helper()
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	resp, err := c.Get("http://unix/v1/peers")
	if err != nil {
		t.Fatalf("GET /v1/peers on %s: %v", sock, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/peers on %s: status %d, err %v", sock, resp.StatusCode, err)
	}
	return body
}

func peerTable(t *testing.T, body []byte) map[string]string {
	t.Helper()
	var entries []PeerStatus
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		out[e.NodeID] = e.Addr
	}
	return out
}

func equalTables(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

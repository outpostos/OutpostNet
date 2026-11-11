package servicehost_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/internal/servicehost"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

const token = "four-layer-09-token"

// startApp starts a real TCP application instance that tags every reply.
func startApp(t *testing.T, tag string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if _, err := io.WriteString(c, tag+":"+line); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// sockDir returns a short directory for Unix sockets (sun_path is limited to
// ~108 bytes, which deep test temp dirs can exceed).
func sockDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "osvc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type runningHost struct {
	h      *servicehost.Host
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returns
	err    error
}

func (r *runningHost) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(15 * time.Second):
		t.Error("host did not stop")
	}
}

func startHost(t *testing.T, cfg *service.Config) *runningHost {
	t.Helper()
	return startHostWithLogger(t, cfg, nil)
}

func startHostWithLogger(t *testing.T, cfg *service.Config, logger *slog.Logger) *runningHost {
	t.Helper()
	h := servicehost.New(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningHost{h: h, cancel: cancel, done: make(chan struct{})}
	go func() { r.err = h.Run(ctx); close(r.done) }()
	t.Cleanup(func() { r.stop(t) })
	deadline := time.Now().Add(5 * time.Second)
	for !h.Ready() {
		select {
		case <-r.done:
			t.Fatalf("host exited: %v", r.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("host not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return r
}

// exportingNode is a node that serves the "echo" service from appAddr on a
// real QUIC tunnel listener.
func exportingNode(t *testing.T, nodeID, appAddr, tok string) *service.Config {
	t.Helper()
	return &service.Config{Config: agentconfig.Config{
		NodeID:     nodeID,
		HealthAddr: "127.0.0.1:0",
		Tunnels:    []agentconfig.TunnelConfig{{Role: "server", ListenAddr: "127.0.0.1:0"}},
		Exports:    []agentconfig.ExportConfig{{Name: "echo", LocalAddr: appAddr, Protocol: "tcp"}},
		TLS:        agentconfig.TLSConfig{Insecure: true},
		Auth:       agentconfig.AuthConfig{Token: tok},
		Management: agentconfig.ManagementConfig{SocketPath: filepath.Join(sockDir(t), nodeID+".sock")},
	}}
}

// consumingNode is a node whose application reaches echo@node-b through a
// stable local entry.
func consumingNode(t *testing.T, nodeID, entryAddr, peerAddr, tok string) *service.Config {
	t.Helper()
	return &service.Config{
		Config: agentconfig.Config{
			NodeID:     nodeID,
			HealthAddr: "127.0.0.1:0",
			Imports:    []agentconfig.ImportConfig{{Service: "echo@node-b", Name: "echo", RemoteRegion: "node-b", ListenAddr: entryAddr, Protocol: "tcp"}},
			TLS:        agentconfig.TLSConfig{Insecure: true},
			Auth:       agentconfig.AuthConfig{Token: tok},
			Management: agentconfig.ManagementConfig{SocketPath: filepath.Join(sockDir(t), nodeID+".sock")},
		},
		Peers: []service.Peer{{NodeID: "node-b", Addr: peerAddr}},
	}
}

func tunnelAddr(t *testing.T, r *runningHost) string {
	t.Helper()
	addrs := r.h.TunnelAddrs()
	if len(addrs) != 1 {
		t.Fatalf("tunnel addrs = %v", addrs)
	}
	return addrs[0].String()
}

type session struct {
	c net.Conn
	r *bufio.Reader
}

func dialEntry(addr string) (*session, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &session{c: c, r: bufio.NewReader(c)}, nil
}

func (s *session) ask(msg string) (string, error) {
	_ = s.c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(s.c, msg+"\n"); err != nil {
		return "", err
	}
	line, err := s.r.ReadString('\n')
	return strings.TrimSpace(line), err
}

// askEventually opens fresh connections to the same entry until the reply
// matches want (tunnel reconnect uses the managed 1s backoff).
func askEventually(t *testing.T, entry, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last string
	var lastErr error
	for time.Now().Before(deadline) {
		s, err := dialEntry(entry)
		if err == nil {
			last, lastErr = s.ask("hello")
			s.c.Close()
			if lastErr == nil && last == want {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("entry %s never answered %q (last=%q err=%v)", entry, want, last, lastErr)
}

func unixHTTP(socket string) *http.Client {
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}
}

// putPeers drives the existing management input (PUT /v1/peers).
func putPeers(t *testing.T, socket string, peers []map[string]string) {
	t.Helper()
	body, _ := json.Marshal(peers)
	req, _ := http.NewRequest(http.MethodPut, "http://unix/v1/peers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := unixHTTP(socket).Do(req)
	if err != nil {
		t.Fatalf("PUT /v1/peers: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT /v1/peers status %d: %s", resp.StatusCode, b)
	}
}

func expectClosed(t *testing.T, s *session, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := s.ask("still-there")
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: in-flight connection was expected to be closed", what)
}

// TestStableEntryFollowsReplacedInstance is the acceptance test for ticket
// 09 item 3: two real nodes (separate Host instances, real QUIC tunnel
// listeners, real TCP application connections, token auth on). The
// application on node-a only knows the stable entry for echo@node-b.
func TestStableEntryFollowsReplacedInstance(t *testing.T) {
	appV1 := startApp(t, "v1")
	appV2 := startApp(t, "v2")
	appV3 := startApp(t, "v3")

	nodeB1 := startHost(t, exportingNode(t, "node-b", appV1, token))
	entry := freeAddr(t)
	nodeACfg := consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB1), token)
	nodeA := startHost(t, nodeACfg)

	askEventually(t, entry, "v1:hello")

	// An application connection established before the switch.
	inflight, err := dialEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer inflight.c.Close()
	if got, err := inflight.ask("before"); err != nil || got != "v1:before" {
		t.Fatalf("in-flight warmup = %q, %v", got, err)
	}

	// Replace the instance: node-b comes back as a new process-equivalent on
	// a different tunnel port serving a different application instance.
	nodeB2 := startHost(t, exportingNode(t, "node-b", appV2, token))
	newAddr := tunnelAddr(t, nodeB2)
	putPeers(t, nodeACfg.Management.SocketPath, []map[string]string{{"node_id": "node-b", "addr": newAddr}})

	// Existing contract: changing the peer address closes the old QUIC
	// connection, so in-flight streams end even though B1 is still alive.
	expectClosed(t, inflight, "peer address change")

	// Same entry, new instance.
	askEventually(t, entry, "v2:hello")
	if got := nodeA.h.EntryAddrs()[0].String(); got != entry {
		t.Fatalf("entry address moved: %s -> %s", entry, got)
	}
	nodeB1.stop(t)
	askEventually(t, entry, "v2:hello")

	// Same node, instance moves to another local port: export re-pointed in
	// place. Established streams keep their original backend.
	kept, err := dialEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer kept.c.Close()
	if got, err := kept.ask("before"); err != nil || got != "v2:before" {
		t.Fatalf("kept warmup = %q, %v", got, err)
	}
	ch := nodeB2.h.ApplyExports([]agentconfig.ExportConfig{{Name: "echo", LocalAddr: appV3, Protocol: "tcp"}})
	if len(ch.Updated) != 1 || ch.Updated[0] != "echo" {
		t.Fatalf("ApplyExports changes = %+v", ch)
	}
	askEventually(t, entry, "v3:hello")
	if got, err := kept.ask("after"); err != nil || got != "v2:after" {
		t.Fatalf("established stream after export move = %q, %v (want v2:after)", got, err)
	}
}

// ApplyExports applies routes on every tunnel server but must report each
// affected name once, not once per server.
func TestApplyExportsMergesChangesAcrossServers(t *testing.T) {
	appV1 := startApp(t, "v1")
	appV2 := startApp(t, "v2")
	cfg := exportingNode(t, "node-multi", appV1, token)
	cfg.Tunnels = []agentconfig.TunnelConfig{
		{Role: "server", ListenAddr: "127.0.0.1:0"},
		{Role: "server", ListenAddr: "127.0.0.1:0"},
	}
	r := startHost(t, cfg)
	if got := len(r.h.TunnelAddrs()); got != 2 {
		t.Fatalf("tunnel listeners = %d, want 2", got)
	}

	ch := r.h.ApplyExports([]agentconfig.ExportConfig{
		{Name: "echo", LocalAddr: appV2, Protocol: "tcp"},
		{Name: "extra", LocalAddr: appV1, Protocol: "tcp"},
	})
	if !reflect.DeepEqual(ch.Updated, []string{"echo"}) ||
		!reflect.DeepEqual(ch.Added, []string{"extra"}) ||
		len(ch.Removed) != 0 {
		t.Fatalf("apply with two servers = %+v, want updated=[echo] added=[extra]", ch)
	}

	ch = r.h.ApplyExports([]agentconfig.ExportConfig{
		{Name: "extra", LocalAddr: appV1, Protocol: "tcp"},
	})
	if !reflect.DeepEqual(ch.Removed, []string{"echo"}) ||
		len(ch.Added) != 0 || len(ch.Updated) != 0 {
		t.Fatalf("apply after removing echo = %+v, want removed=[echo]", ch)
	}
}

// logBuffer captures a host's log output so tests can wait for specific
// events (for example the tunnel server rejecting a token).
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *logBuffer) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// peerStatus returns the GET /v1/peers status of nodeID ("" if absent).
func peerStatus(t *testing.T, socket, nodeID string) string {
	t.Helper()
	resp, err := unixHTTP(socket).Get("http://unix/v1/peers")
	if err != nil {
		t.Fatalf("GET /v1/peers: %v", err)
	}
	defer resp.Body.Close()
	var st []struct {
		NodeID string `json:"node_id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	for _, p := range st {
		if p.NodeID == nodeID {
			return p.Status
		}
	}
	return ""
}

// TestStableEntryRejectedWithoutValidToken keeps the existing tunnel auth.
// Same topology twice against one exporting node: the consumer with the
// right token reaches the service (positive control); the consumer with a
// wrong token is rejected by the tunnel server's auth, its peer never
// becomes connected, and its entry closes every application connection
// without a reply.
func TestStableEntryRejectedWithoutValidToken(t *testing.T) {
	app := startApp(t, "v1")
	var bLog, badLog logBuffer
	nodeB := startHostWithLogger(t, exportingNode(t, "node-b", app, token), bLog.logger())
	peerAddr := tunnelAddr(t, nodeB)

	// Positive control: right token, same exporting node.
	goodEntry := freeAddr(t)
	startHost(t, consumingNode(t, "node-a", goodEntry, peerAddr, token))
	askEventually(t, goodEntry, "v1:hello")

	// Wrong token. node-a2 sorts before node-b, so it is the dialing side.
	badEntry := freeAddr(t)
	badCfg := consumingNode(t, "node-a2", badEntry, peerAddr, "wrong-token")
	startHostWithLogger(t, badCfg, badLog.logger())

	defer func() {
		if t.Failed() {
			t.Logf("node-b log:\n%s\nnode-a2 log:\n%s", bLog.String(), badLog.String())
		}
	}()
	waitFor(t, 15*time.Second, "tunnel server to reject the wrong token", func() bool {
		// Server side: the tunnel auth handshake rejected the token.
		// Client side: node-a2's peer tunnel saw the auth failure, either
		// as the server's authFail reply or as the server's "authentication
		// failed" connection close, whichever reaches the client first.
		client := badLog.String()
		return strings.Contains(bLog.String(), `error="token mismatch"`) &&
			(strings.Contains(client, "tunnel auth: server rejected authentication") ||
				strings.Contains(client, "(remote): authentication failed"))
	})
	if st := peerStatus(t, badCfg.Management.SocketPath, "node-b"); st == "connected" || st == "" {
		t.Fatalf("wrong-token peer status = %q, want a non-connected directory entry", st)
	}

	for i := 0; i < 3; i++ {
		s, err := dialEntry(badEntry)
		if err != nil {
			t.Fatalf("entry should still listen: %v", err)
		}
		got, err := s.ask("hello")
		s.c.Close()
		if err == nil {
			t.Fatalf("unauthenticated node reached the service: %q", got)
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatalf("entry left the connection hanging instead of closing it: %v", err)
		}
	}
	if st := peerStatus(t, badCfg.Management.SocketPath, "node-b"); st == "connected" {
		t.Fatal("wrong-token peer became connected")
	}
	// The control consumer is unaffected.
	askEventually(t, goodEntry, "v1:hello")
}

// TestManagementStatusReportsPeer checks the reused GET /v1/peers input.
func TestManagementStatusReportsPeer(t *testing.T) {
	app := startApp(t, "v1")
	nodeB := startHost(t, exportingNode(t, "node-b", app, token))
	entry := freeAddr(t)
	cfg := consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB), token)
	startHost(t, cfg)
	askEventually(t, entry, "v1:hello")

	resp, err := unixHTTP(cfg.Management.SocketPath).Get("http://unix/v1/peers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st []struct {
		NodeID string `json:"node_id"`
		Addr   string `json:"addr"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if len(st) != 1 || st[0].NodeID != "node-b" || st[0].Status != "connected" {
		t.Fatalf("peer status = %+v", st)
	}
}

// TestConfigFileExportMoveAppliesWithoutRestart drives the "instance moved
// to another local port" case through the real config file and the reused
// ConfigWatcher, across a real two-node tunnel.
func TestConfigFileExportMoveAppliesWithoutRestart(t *testing.T) {
	appV1 := startApp(t, "v1")
	appV2 := startApp(t, "v2")
	dir := t.TempDir()
	path := filepath.Join(dir, "node-b.yaml")
	write := func(appAddr string) {
		y := "node_id: node-b\nhealth_addr: 127.0.0.1:0\n" +
			"management:\n  socket_path: " + filepath.Join(sockDir(t), "b.sock") + "\n" +
			"auth:\n  token: " + token + "\n" +
			"tls:\n  insecure: true\n" +
			"tunnels:\n  - role: server\n    listen_addr: 127.0.0.1:0\n" +
			"exports:\n  - name: echo\n    local_addr: " + appAddr + "\n    protocol: tcp\n"
		if err := os.WriteFile(path, []byte(y), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(appV1)
	cfgB, err := service.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	nodeB := startHost(t, cfgB)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go nodeB.h.WatchConfig(ctx, path, 50*time.Millisecond, nil)

	entry := freeAddr(t)
	startHost(t, consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB), token))
	askEventually(t, entry, "v1:hello")

	time.Sleep(100 * time.Millisecond) // let the watcher record the initial stat
	write(appV2)
	askEventually(t, entry, "v2:hello")
}

// entryGoroutines counts goroutines currently inside service.Entry code.
func entryGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "OutpostNet/pkg/service.(*Entry)")
}

// assertEntriesReleased checks, right after Run returned, that no entry
// goroutine is left and the entry address can be bound again.
func assertEntriesReleased(t *testing.T, entry string) {
	t.Helper()
	if n := entryGoroutines(); n != 0 {
		t.Errorf("%d service entry goroutine frame(s) still running after Run returned", n)
	}
	ln, err := net.Listen("tcp", entry)
	if err != nil {
		t.Errorf("entry address %s still bound after Run returned: %v", entry, err)
		return
	}
	ln.Close()
}

// TestRunStopsEntriesBeforeReturning: on normal shutdown Run cancels the
// entries and waits for them (including an in-flight application
// connection) before stopping the tunnels, so nothing outlives Run.
func TestRunStopsEntriesBeforeReturning(t *testing.T) {
	app := startApp(t, "v1")
	nodeB := startHost(t, exportingNode(t, "node-b", app, token))
	entry := freeAddr(t)
	nodeA := startHost(t, consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB), token))
	askEventually(t, entry, "v1:hello")

	inflight, err := dialEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer inflight.c.Close()
	if got, err := inflight.ask("before"); err != nil || got != "v1:before" {
		t.Fatalf("in-flight warmup = %q, %v", got, err)
	}

	nodeA.cancel()
	select {
	case <-nodeA.done:
	case <-time.After(15 * time.Second):
		t.Fatal("host did not stop")
	}
	if nodeA.err != nil {
		t.Fatalf("Run returned %v on normal shutdown", nodeA.err)
	}
	assertEntriesReleased(t, entry)
	expectClosed(t, inflight, "host shutdown")
}

// TestRunReleasesEntriesOnStartupError: when a later startup step fails
// (health address already in use), Run returns the error promptly and no
// entry is left listening.
func TestRunReleasesEntriesOnStartupError(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	app := startApp(t, "v1")
	nodeB := startHost(t, exportingNode(t, "node-b", app, token))
	entry := freeAddr(t)
	cfg := consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB), token)
	cfg.HealthAddr = busy.Addr().String()

	errc := make(chan error, 1)
	go func() { errc <- servicehost.New(cfg, nil).Run(context.Background()) }()
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "health") {
			t.Fatalf("Run error = %v, want health server error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the health server failed to start")
	}
	assertEntriesReleased(t, entry)
}

// TestRunReportsEntryBindError: an entry whose listen address is taken
// fails startup with the real bind error, promptly, and nothing started
// for the other entries is left behind.
func TestRunReportsEntryBindError(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	app := startApp(t, "v1")
	nodeB := startHost(t, exportingNode(t, "node-b", app, token))
	okEntry := freeAddr(t)
	cfg := consumingNode(t, "node-a", okEntry, tunnelAddr(t, nodeB), token)
	cfg.Imports = append(cfg.Imports, agentconfig.ImportConfig{
		Service: "echo@node-b", Name: "echo", RemoteRegion: "node-b", ListenAddr: busy.Addr().String(), Protocol: "tcp",
	})

	start := time.Now()
	errc := make(chan error, 1)
	go func() { errc <- servicehost.New(cfg, nil).Run(context.Background()) }()
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "address already in use") || !strings.Contains(err.Error(), busy.Addr().String()) {
			t.Fatalf("Run error = %v, want the bind error for %s", err, busy.Addr())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after an entry failed to bind")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("bind error reported after %v; want it immediately", d)
	}
	assertEntriesReleased(t, okEntry)
}

// writeServiceFile writes a node-b Service config with the given export
// address and peers section.
func writeServiceFile(t *testing.T, path, sock, healthAddr, appAddr, peers string) {
	t.Helper()
	y := "node_id: node-b\nhealth_addr: " + healthAddr + "\n" +
		"management:\n  socket_path: " + sock + "\n" +
		"auth:\n  token: " + token + "\n" +
		"tls:\n  insecure: true\n" +
		"tunnels:\n  - role: server\n    listen_addr: 127.0.0.1:0\n" +
		"exports:\n  - name: echo\n    local_addr: " + appAddr + "\n    protocol: tcp\n" + peers
	if err := os.WriteFile(path, []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
}

const restartWarning = "config change outside exports ignored until restart"

// TestReloadWarnsOnPeersChange: peers are seeded only at startup, so a
// peers edit in the file must be reported as needing a restart
// (docs/service-layer.md §7).
func TestReloadWarnsOnPeersChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node-b.yaml")
	sock := filepath.Join(sockDir(t), "b.sock")
	writeServiceFile(t, path, sock, "127.0.0.1:0", "127.0.0.1:1", "")
	cfg, err := service.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	var logs logBuffer
	r := startHostWithLogger(t, cfg, logs.logger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.h.WatchConfig(ctx, path, 20*time.Millisecond, nil)
	time.Sleep(100 * time.Millisecond) // let the watcher record the initial stat

	writeServiceFile(t, path, sock, "127.0.0.1:0", "127.0.0.1:2",
		"peers:\n  - node_id: node-c\n    addr: 127.0.0.1:7443\n")
	defer func() {
		if t.Failed() {
			t.Logf("log:\n%s", logs.String())
		}
	}()
	waitFor(t, 5*time.Second, "export change applied and peers change reported", func() bool {
		l := logs.String()
		return strings.Contains(l, "exports applied") && strings.Contains(l, restartWarning)
	})
}

// TestReloadIgnoresCommandLineOverrides: the command overrides node_id and
// health_addr after loading the file. An export-only edit must not be
// reported as a restart-scope change just because the file differs from
// those overrides.
func TestReloadIgnoresCommandLineOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node-b.yaml")
	sock := filepath.Join(sockDir(t), "b.sock")
	writeServiceFile(t, path, sock, "127.0.0.1:1", "127.0.0.1:1", "")
	cfg, err := service.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	override := func(c *service.Config) { c.HealthAddr = "127.0.0.1:0" } // as --health-addr does
	override(cfg)
	var logs logBuffer
	r := startHostWithLogger(t, cfg, logs.logger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.h.WatchConfig(ctx, path, 20*time.Millisecond, override)
	time.Sleep(100 * time.Millisecond)

	writeServiceFile(t, path, sock, "127.0.0.1:1", "127.0.0.1:2", "")
	waitFor(t, 5*time.Second, "export change applied", func() bool {
		return strings.Count(logs.String(), "exports applied") == 1
	})
	// A second export-only edit: once it is applied, the watcher goroutine
	// has finished the first reload, including any restart-scope warning.
	time.Sleep(50 * time.Millisecond) // distinct mtime/size for the poller
	writeServiceFile(t, path, sock, "127.0.0.1:1", "127.0.0.1:33", "")
	waitFor(t, 5*time.Second, "second export change applied", func() bool {
		return strings.Count(logs.String(), "exports applied") == 2
	})
	if strings.Contains(logs.String(), restartWarning) {
		t.Fatalf("export-only change reported as restart scope; log:\n%s", logs.String())
	}
}

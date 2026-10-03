package servicehost_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	h := servicehost.New(cfg, nil)
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

// TestStableEntryRejectedWithoutValidToken keeps the existing tunnel auth:
// an entry on a node with the wrong token never reaches the service.
func TestStableEntryRejectedWithoutValidToken(t *testing.T) {
	app := startApp(t, "v1")
	nodeB := startHost(t, exportingNode(t, "node-b", app, token))
	entry := freeAddr(t)
	startHost(t, consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB), "wrong-token"))

	time.Sleep(1500 * time.Millisecond) // allow connect attempts to fail
	for i := 0; i < 3; i++ {
		s, err := dialEntry(entry)
		if err != nil {
			t.Fatalf("entry should still listen: %v", err)
		}
		got, err := s.ask("hello")
		s.c.Close()
		if err == nil {
			t.Fatalf("unauthenticated node reached the service: %q", got)
		}
	}
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
	go nodeB.h.WatchConfig(ctx, path, 50*time.Millisecond)

	entry := freeAddr(t)
	startHost(t, consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB), token))
	askEventually(t, entry, "v1:hello")

	time.Sleep(100 * time.Millisecond) // let the watcher record the initial stat
	write(appV2)
	askEventually(t, entry, "v2:hello")
}

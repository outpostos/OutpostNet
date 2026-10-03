package agent

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func startTaggedLineServer(t *testing.T, tag string) net.Listener {
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
	return ln
}

func askOverStream(t *testing.T, ctx context.Context, c *tunnel.Client, svc, msg string) string {
	t.Helper()
	s, err := c.OpenServiceStream(ctx, svc)
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(s, msg+"\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(s).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return strings.TrimSpace(line)
}

// TestReloadAgentUpdatesChangedExportAddress covers the "same service, new
// local port" case: before ticket 09 ReloadAgent only added new names and
// removed deleted ones, so a changed local_addr kept routing to the old port.
func TestReloadAgentUpdatesChangedExportAddress(t *testing.T) {
	v1 := startTaggedLineServer(t, "v1")
	v2 := startTaggedLineServer(t, "v2")

	cfg := &agentconfig.Config{
		Tunnels: []agentconfig.TunnelConfig{{Role: "server", ListenAddr: "127.0.0.1:0"}},
		Exports: []agentconfig.ExportConfig{{Name: "echo", LocalAddr: v1.Addr().String(), Protocol: "tcp"}},
		TLS:     agentconfig.TLSConfig{Insecure: true},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	a := New(cfg, nil)
	tm := NewTunnelManager(cfg, nil, nil)
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tm.Stop()
	a.tunnels = tm

	client := tunnel.NewClient(tunnel.ClientConfig{
		ServerAddr: tm.Servers()[0].Addr().String(),
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	if got := askOverStream(t, ctx, client, "echo", "ping"); got != "v1:ping" {
		t.Fatalf("before reload got %q, want v1:ping", got)
	}

	next := *cfg
	next.Exports = []agentconfig.ExportConfig{{Name: "echo", LocalAddr: v2.Addr().String(), Protocol: "tcp"}}
	// Exports apply in place. ReloadAgent always reports that tunnel changes
	// need a restart (no tunnel hot-reload); any other error is a failure.
	if err := ReloadAgent(a, &next); err == nil || !strings.Contains(err.Error(), "restart required for tunnel changes") {
		t.Fatalf("ReloadAgent error = %v, want only the restart-required notice", err)
	}

	if got := askOverStream(t, ctx, client, "echo", "ping"); got != "v2:ping" {
		t.Fatalf("after reload got %q, want v2:ping", got)
	}
}

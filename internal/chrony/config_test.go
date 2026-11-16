package chrony_test

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/outpostos/OutpostNet/internal/chrony"
)

func TestRenderGroupNTSGolden(t *testing.T) {
	cfg := chrony.Config{NodeID: "node-a", CAFile: "/etc/outpostnet/group.pem", CertFile: "/etc/outpostnet/node.pem", KeyFile: "/etc/outpostnet/node.key"}
	peers := []chrony.Peer{{NodeID: "node-c", Addr: "[2001:db8::3]:4242"}, {NodeID: "node-a", Addr: "192.0.2.1:4242"}, {NodeID: "node-b", Addr: "node-b.example.net:4242"}}
	resolve := func(context.Context, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("192.0.2.2"), net.ParseIP("2001:db8::2")}, nil
	}
	for _, tc := range []struct {
		name string
		gnss chrony.GNSS
	}{
		{"group", chrony.GNSS{}},
		{"shm-pps", chrony.GNSS{Driver: "SHM", Parameter: "0", PPS: "/dev/pps0"}},
		{"sock-pps", chrony.GNSS{Driver: "SOCK", Parameter: "/run/chrony.clk.ttyS0.sock", PPS: "/dev/pps0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg.GNSS = tc.gnss
			got, err := chrony.Render(context.Background(), cfg, peers, resolve)
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile("testdata/" + tc.name + ".conf")
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("config differs:\n%s", got)
			}
		})
	}
}

func TestRenderRejectsUnsafeInput(t *testing.T) {
	base := chrony.Config{NodeID: "a", CAFile: "/etc/group.pem", CertFile: "/etc/node.pem", KeyFile: "/etc/node.key"}
	for _, addr := range []string{"0.0.0.0:4242", "[::]:4242", "192.0.2.0/24:4242", "evil\nallow all:4242", "-x:4242", "192.0.2.1:0", "missing-port"} {
		if _, err := chrony.Render(context.Background(), base, []chrony.Peer{{NodeID: "b", Addr: addr}}, nil); err == nil {
			t.Fatalf("accepted %q", addr)
		}
	}
	for _, gnss := range []chrony.GNSS{{PPS: "/dev/pps0"}, {Driver: "SHM", Parameter: "-1"}, {Driver: "SOCK", Parameter: "/run/a\nserver evil"}, {Driver: "NTP", Parameter: "evil"}} {
		cfg := base
		cfg.GNSS = gnss
		if _, err := chrony.Render(context.Background(), cfg, nil, nil); err == nil {
			t.Fatal("accepted unsafe GNSS")
		}
	}
	base.KeyFile = "/etc/key\nallow all"
	if _, err := chrony.Render(context.Background(), base, nil, nil); err == nil {
		t.Fatal("accepted injected path")
	}
}

func TestRenderOnlyGroupSourcesAndDeterministic(t *testing.T) {
	cfg := chrony.Config{NodeID: "a", CAFile: "/etc/group.pem", CertFile: "/etc/node.pem", KeyFile: "/etc/node.key"}
	peers := []chrony.Peer{{NodeID: "c", Addr: "192.0.2.3:4242"}, {NodeID: "b", Addr: "192.0.2.2:4242"}}
	a, err := chrony.Render(context.Background(), cfg, peers, nil)
	if err != nil {
		t.Fatal(err)
	}
	peers[0], peers[1] = peers[1], peers[0]
	b, err := chrony.Render(context.Background(), cfg, peers, nil)
	if err != nil || string(a) != string(b) {
		t.Fatal("unstable render", err)
	}
	for _, line := range strings.Split(string(a), "\n") {
		if strings.HasPrefix(line, "server ") && !strings.Contains(line, " nts ") {
			t.Fatal("unauthenticated source", line)
		}
	}
	peers = append(peers, peers[0])
	if _, err := chrony.Render(context.Background(), cfg, peers, nil); err == nil {
		t.Fatal("duplicate node accepted")
	}
}

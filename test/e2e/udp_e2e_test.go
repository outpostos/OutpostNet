package e2e

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/internal/server"
	"github.com/outpostos/OutpostNet/internal/sidecar"
	"github.com/outpostos/OutpostNet/pkg/protocol"
)

// TestUDPEndToEnd verifies the complete UDP data flow:
//
//	App A → Sidecar A (import UDP :importPort) → Server → Sidecar B (export) → App B (UDP :appBPort)
//
// App B is a simple UDP echo server.
func TestUDPEndToEnd(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "server.sock")

	// --- Start App B (UDP echo server) ---
	appBConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start app B: %v", err)
	}
	defer appBConn.Close()
	appBPort := appBConn.LocalAddr().(*net.UDPAddr).Port

	go func() {
		buf := make([]byte, 65535)
		for {
			n, addr, err := appBConn.ReadFrom(buf)
			if err != nil {
				return
			}
			// Echo back.
			appBConn.WriteTo(buf[:n], addr)
		}
	}()

	// --- Start Server ---
	srv := server.New(server.Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 30 * time.Second,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// --- Start Sidecar B (exporter) ---
	sidecarB := sidecar.New(sidecar.Config{
		ServerSocket:      sockPath,
		SidecarID:         "sidecar-b-udp",
		HeartbeatInterval: 10 * time.Second,
		Exports: []sidecar.ExportEntry{
			{Name: "udp-echo", Protocol: protocol.ProtoUDP, Port: uint16(appBPort)},
		},
	})
	go sidecarB.Run()
	defer sidecarB.Stop()
	time.Sleep(100 * time.Millisecond)

	// --- Start Sidecar A (importer) ---
	tmpLn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	importPort := tmpLn.LocalAddr().(*net.UDPAddr).Port
	tmpLn.Close()

	sidecarA := sidecar.New(sidecar.Config{
		ServerSocket:      sockPath,
		SidecarID:         "sidecar-a-udp",
		HeartbeatInterval: 10 * time.Second,
		Imports: []sidecar.ImportEntry{
			{Name: "udp-echo", Protocol: protocol.ProtoUDP, ListenPort: uint16(importPort)},
		},
	})
	go sidecarA.Run()
	defer sidecarA.Stop()
	time.Sleep(200 * time.Millisecond)

	// --- App A: send UDP datagram to import port ---
	conn, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", importPort))
	if err != nil {
		t.Fatalf("app A dial: %v", err)
	}
	defer conn.Close()

	testMsg := "UDP hello!"
	_, err = conn.Write([]byte(testMsg))
	if err != nil {
		t.Fatalf("app A write: %v", err)
	}

	// Read echo response.
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("app A read: %v", err)
	}

	got := string(buf[:n])
	if got != testMsg {
		t.Errorf("echo mismatch: got %q, want %q", got, testMsg)
	}

	t.Logf("UDP end-to-end OK: sent %q, received %q", testMsg, got)
}

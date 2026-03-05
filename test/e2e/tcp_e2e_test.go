package e2e

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/internal/server"
	"github.com/outpostos/OutpostNet/internal/sidecar"
	"github.com/outpostos/OutpostNet/pkg/protocol"
)

// TestTCPEndToEnd verifies the complete data flow:
//
//	App A → Sidecar A (import :importPort) → Server → Sidecar B (export) → App B (:appBPort)
//
// App B is a simple echo TCP server.
func TestTCPEndToEnd(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "server.sock")

	// --- Start App B (echo server) ---
	appBListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start app B: %v", err)
	}
	defer appBListener.Close()
	appBPort := appBListener.Addr().(*net.TCPAddr).Port

	go func() {
		for {
			conn, err := appBListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // echo
			}(conn)
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
		SidecarID:         "sidecar-b",
		HeartbeatInterval: 10 * time.Second,
		Exports: []sidecar.ExportEntry{
			{Name: "echo-service", Protocol: protocol.ProtoTCP, Port: uint16(appBPort)},
		},
	})
	go sidecarB.Run()
	defer sidecarB.Stop()
	time.Sleep(100 * time.Millisecond)

	// --- Start Sidecar A (importer) ---
	// Find a free port for the import listener.
	importListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	importPort := importListener.Addr().(*net.TCPAddr).Port
	importListener.Close() // release it so sidecar can bind

	sidecarA := sidecar.New(sidecar.Config{
		ServerSocket:      sockPath,
		SidecarID:         "sidecar-a",
		HeartbeatInterval: 10 * time.Second,
		Imports: []sidecar.ImportEntry{
			{Name: "echo-service", Protocol: protocol.ProtoTCP, ListenPort: uint16(importPort)},
		},
	})
	go sidecarA.Run()
	defer sidecarA.Stop()
	time.Sleep(200 * time.Millisecond)

	// --- App A: connect to import port and send data ---
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 5*time.Second)
	if err != nil {
		t.Fatalf("app A connect: %v", err)
	}
	defer conn.Close()

	testMsg := "Hello from App A!"
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

	t.Logf("TCP end-to-end OK: sent %q, received %q", testMsg, got)
}

// TestTCPMultipleConnections verifies multiple concurrent TCP connections
// through the same import/export service.
func TestTCPMultipleConnections(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "server.sock")

	// Echo server.
	appBListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start app B: %v", err)
	}
	defer appBListener.Close()
	appBPort := appBListener.Addr().(*net.TCPAddr).Port

	go func() {
		for {
			conn, err := appBListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()

	// Server.
	srv := server.New(server.Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 30 * time.Second,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Sidecar B (exporter).
	sidecarB := sidecar.New(sidecar.Config{
		ServerSocket:      sockPath,
		SidecarID:         "sidecar-b",
		HeartbeatInterval: 10 * time.Second,
		Exports: []sidecar.ExportEntry{
			{Name: "multi-svc", Protocol: protocol.ProtoTCP, Port: uint16(appBPort)},
		},
	})
	go sidecarB.Run()
	defer sidecarB.Stop()
	time.Sleep(100 * time.Millisecond)

	// Sidecar A (importer).
	tmpLn, _ := net.Listen("tcp", "127.0.0.1:0")
	importPort := tmpLn.Addr().(*net.TCPAddr).Port
	tmpLn.Close()

	sidecarA := sidecar.New(sidecar.Config{
		ServerSocket:      sockPath,
		SidecarID:         "sidecar-a",
		HeartbeatInterval: 10 * time.Second,
		Imports: []sidecar.ImportEntry{
			{Name: "multi-svc", Protocol: protocol.ProtoTCP, ListenPort: uint16(importPort)},
		},
	})
	go sidecarA.Run()
	defer sidecarA.Stop()
	time.Sleep(200 * time.Millisecond)

	// Send 5 concurrent connections.
	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 5*time.Second)
			if err != nil {
				errors <- fmt.Errorf("conn %d dial: %w", id, err)
				return
			}
			defer conn.Close()

			msg := fmt.Sprintf("message-%d", id)
			if _, err := conn.Write([]byte(msg)); err != nil {
				errors <- fmt.Errorf("conn %d write: %w", id, err)
				return
			}

			buf := make([]byte, 1024)
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				errors <- fmt.Errorf("conn %d read: %w", id, err)
				return
			}
			if string(buf[:n]) != msg {
				errors <- fmt.Errorf("conn %d: got %q, want %q", id, string(buf[:n]), msg)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

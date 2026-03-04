package server

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
)

func tempSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.sock")
}

func TestHandshakeAndRegister(t *testing.T) {
	sockPath := tempSocketPath(t)
	cfg := Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	}
	srv := New(cfg)

	go srv.Run()
	defer srv.Stop()

	// Wait for server to start.
	time.Sleep(50 * time.Millisecond)

	// Connect as sidecar.
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec(conn)

	// Handshake.
	err = codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshake,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeHandshake(&protocol.HandshakePayload{
			ProtocolVersion: protocol.ProtocolVersionV1,
			SidecarID:       "test-sidecar-1",
		}),
	})
	if err != nil {
		t.Fatalf("write handshake: %v", err)
	}

	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read handshake ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeHandshakeAck {
		t.Fatalf("expected HandshakeAck, got 0x%02x", frame.Type)
	}
	ack, err := protocol.DecodeHandshakeAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ack.Status != protocol.StatusOK {
		t.Fatalf("handshake status=%d: %s", ack.Status, ack.Message)
	}

	// Register a service.
	err = codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegister,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeRegister(&protocol.RegisterPayload{
			ServiceName: "my-web",
			Protocol:    protocol.ProtoTCP,
			LocalPort:   8080,
		}),
	})
	if err != nil {
		t.Fatalf("write register: %v", err)
	}

	frame, err = codec.ReadFrame()
	if err != nil {
		t.Fatalf("read register ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeRegisterAck {
		t.Fatalf("expected RegisterAck, got 0x%02x", frame.Type)
	}
	regAck, err := protocol.DecodeRegisterAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode register ack: %v", err)
	}
	if regAck.Status != protocol.StatusOK {
		t.Fatalf("register status=%d: %s", regAck.Status, regAck.Message)
	}
	if regAck.ServiceName != "my-web" {
		t.Errorf("service name: got %q, want %q", regAck.ServiceName, "my-web")
	}

	// Verify service is in registry.
	srv.mu.RLock()
	svc, ok := srv.services["my-web"]
	srv.mu.RUnlock()
	if !ok {
		t.Fatal("service not found in registry")
	}
	if svc.sidecarID != "test-sidecar-1" {
		t.Errorf("sidecar ID: got %q, want %q", svc.sidecarID, "test-sidecar-1")
	}
}

func TestHeartbeat(t *testing.T) {
	sockPath := tempSocketPath(t)
	cfg := Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	}
	srv := New(cfg)

	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec(conn)

	// Handshake.
	codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshake,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeHandshake(&protocol.HandshakePayload{
			ProtocolVersion: protocol.ProtocolVersionV1,
			SidecarID:       "hb-test",
		}),
	})
	codec.ReadFrame() // read ack

	// Send heartbeat.
	ts := uint64(time.Now().UnixMilli())
	codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHeartbeat,
		StreamID: protocol.StreamIDControl,
		Payload:  protocol.EncodeHeartbeat(&protocol.HeartbeatPayload{Timestamp: ts}),
	})

	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read heartbeat ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeHeartbeatAck {
		t.Fatalf("expected HeartbeatAck, got 0x%02x", frame.Type)
	}

	hbAck, err := protocol.DecodeHeartbeat(frame.Payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hbAck.Timestamp != ts {
		t.Errorf("timestamp: got %d, want %d", hbAck.Timestamp, ts)
	}
}

func TestDisconnectCleanup(t *testing.T) {
	sockPath := tempSocketPath(t)
	cfg := Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	}
	srv := New(cfg)

	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	codec := protocol.NewCodec(conn)

	// Handshake + Register.
	codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshake,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeHandshake(&protocol.HandshakePayload{
			ProtocolVersion: protocol.ProtocolVersionV1,
			SidecarID:       "dc-test",
		}),
	})
	codec.ReadFrame() // ack

	codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegister,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeRegister(&protocol.RegisterPayload{
			ServiceName: "ephemeral-svc",
			Protocol:    protocol.ProtoTCP,
			LocalPort:   9090,
		}),
	})
	codec.ReadFrame() // ack

	// Verify registered.
	srv.mu.RLock()
	_, ok := srv.services["ephemeral-svc"]
	srv.mu.RUnlock()
	if !ok {
		t.Fatal("service not registered")
	}

	// Disconnect.
	conn.Close()
	time.Sleep(200 * time.Millisecond)

	// Verify cleaned up.
	srv.mu.RLock()
	_, ok = srv.services["ephemeral-svc"]
	_, scOk := srv.sidecars["dc-test"]
	srv.mu.RUnlock()

	if ok {
		t.Error("service still in registry after disconnect")
	}
	if scOk {
		t.Error("sidecar still tracked after disconnect")
	}

	// Verify socket file is still there (server still running).
	if _, err := os.Stat(sockPath); os.IsNotExist(err) {
		t.Error("socket file removed while server is still running")
	}
}

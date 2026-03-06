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

// --- Helper ---

func connectSidecar(t *testing.T, sockPath, sidecarID string) *protocol.Codec {
	t.Helper()
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	codec := protocol.NewCodec(conn)
	if err := codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshake,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeHandshake(&protocol.HandshakePayload{
			ProtocolVersion: protocol.ProtocolVersionV1,
			SidecarID:       sidecarID,
		}),
	}); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read handshake ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeHandshakeAck {
		t.Fatalf("expected HandshakeAck, got 0x%02x", frame.Type)
	}
	return codec
}

func registerService(t *testing.T, codec *protocol.Codec, serviceName string, port uint16) {
	t.Helper()
	if err := codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegister,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeRegister(&protocol.RegisterPayload{
			ServiceName: serviceName,
			Protocol:    protocol.ProtoTCP,
			LocalPort:   port,
		}),
	}); err != nil {
		t.Fatalf("write register: %v", err)
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read register ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeRegisterAck {
		t.Fatalf("expected RegisterAck, got 0x%02x", frame.Type)
	}
	ack, err := protocol.DecodeRegisterAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode register ack: %v", err)
	}
	if ack.Status != protocol.StatusOK {
		t.Fatalf("register status=%d: %s", ack.Status, ack.Message)
	}
}

// --- New Tests ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SocketPath != "/var/run/outpostnet/server.sock" {
		t.Errorf("SocketPath: got %q, want %q", cfg.SocketPath, "/var/run/outpostnet/server.sock")
	}
	if cfg.HeartbeatTimeout != 30*time.Second {
		t.Errorf("HeartbeatTimeout: got %v, want %v", cfg.HeartbeatTimeout, 30*time.Second)
	}
}

func TestConnectServiceNotFound(t *testing.T) {
	sockPath := tempSocketPath(t)
	srv := New(Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Connect two sidecars; neither registers the target service.
	codecA := connectSidecar(t, sockPath, "sidecar-A")
	defer codecA.Close()
	_ = connectSidecar(t, sockPath, "sidecar-B")

	// Sidecar A tries to connect to a non-existent service.
	if err := codecA.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnect,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnect(&protocol.ConnectPayload{
			ServiceName: "no-such-service",
			Protocol:    protocol.ProtoTCP,
		}),
	}); err != nil {
		t.Fatalf("write connect: %v", err)
	}

	frame, err := codecA.ReadFrame()
	if err != nil {
		t.Fatalf("read connect ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeConnectAck {
		t.Fatalf("expected ConnectAck, got 0x%02x", frame.Type)
	}
	ack, err := protocol.DecodeConnectAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode connect ack: %v", err)
	}
	if ack.Status != protocol.StatusNotFound {
		t.Errorf("status: got %d, want %d (StatusNotFound)", ack.Status, protocol.StatusNotFound)
	}
	if ack.ServiceName != "no-such-service" {
		t.Errorf("service name: got %q, want %q", ack.ServiceName, "no-such-service")
	}
}

func TestConnectAndDataFlow(t *testing.T) {
	sockPath := tempSocketPath(t)
	srv := New(Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Sidecar B registers "echo-svc".
	codecB := connectSidecar(t, sockPath, "sidecar-B")
	defer codecB.Close()
	registerService(t, codecB, "echo-svc", 8080)

	// Sidecar A connects to "echo-svc".
	codecA := connectSidecar(t, sockPath, "sidecar-A")
	defer codecA.Close()

	if err := codecA.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnect,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnect(&protocol.ConnectPayload{
			ServiceName: "echo-svc",
			Protocol:    protocol.ProtoTCP,
		}),
	}); err != nil {
		t.Fatalf("write connect: %v", err)
	}

	// Sidecar A should receive ConnectAck with StatusOK.
	frame, err := codecA.ReadFrame()
	if err != nil {
		t.Fatalf("read connect ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeConnectAck {
		t.Fatalf("expected ConnectAck, got 0x%02x", frame.Type)
	}
	connAck, err := protocol.DecodeConnectAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode connect ack: %v", err)
	}
	if connAck.Status != protocol.StatusOK {
		t.Fatalf("connect status: got %d, want %d (StatusOK)", connAck.Status, protocol.StatusOK)
	}
	streamID := connAck.StreamID
	if streamID == 0 {
		t.Fatal("streamID should be non-zero")
	}

	// Sidecar B should receive OpenStream from the server.
	frame, err = codecB.ReadFrame()
	if err != nil {
		t.Fatalf("read open stream: %v", err)
	}
	if frame.Type != protocol.MsgTypeOpenStream {
		t.Fatalf("expected OpenStream, got 0x%02x", frame.Type)
	}
	os, err := protocol.DecodeOpenStream(frame.Payload)
	if err != nil {
		t.Fatalf("decode open stream: %v", err)
	}
	if os.StreamID != streamID {
		t.Errorf("open stream streamID: got %d, want %d", os.StreamID, streamID)
	}
	if os.ServiceName != "echo-svc" {
		t.Errorf("open stream service: got %q, want %q", os.ServiceName, "echo-svc")
	}

	// Sidecar A sends Data on the streamID.
	dataPayload := []byte("hello from importer")
	if err := codecA.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeData,
		StreamID: streamID,
		Payload:  dataPayload,
	}); err != nil {
		t.Fatalf("write data A->B: %v", err)
	}

	// Sidecar B should receive the Data frame.
	frame, err = codecB.ReadFrame()
	if err != nil {
		t.Fatalf("read data on B: %v", err)
	}
	if frame.Type != protocol.MsgTypeData {
		t.Fatalf("expected Data, got 0x%02x", frame.Type)
	}
	if frame.StreamID != streamID {
		t.Errorf("data streamID: got %d, want %d", frame.StreamID, streamID)
	}
	if string(frame.Payload) != "hello from importer" {
		t.Errorf("data payload: got %q, want %q", string(frame.Payload), "hello from importer")
	}

	// Sidecar B sends Data back on the same streamID.
	replyPayload := []byte("hello from exporter")
	if err := codecB.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeData,
		StreamID: streamID,
		Payload:  replyPayload,
	}); err != nil {
		t.Fatalf("write data B->A: %v", err)
	}

	// Sidecar A should receive the reply.
	frame, err = codecA.ReadFrame()
	if err != nil {
		t.Fatalf("read data on A: %v", err)
	}
	if frame.Type != protocol.MsgTypeData {
		t.Fatalf("expected Data, got 0x%02x", frame.Type)
	}
	if frame.StreamID != streamID {
		t.Errorf("reply streamID: got %d, want %d", frame.StreamID, streamID)
	}
	if string(frame.Payload) != "hello from exporter" {
		t.Errorf("reply payload: got %q, want %q", string(frame.Payload), "hello from exporter")
	}

	// Verify route exists in server.
	srv.mu.RLock()
	route, ok := srv.routes[streamID]
	srv.mu.RUnlock()
	if !ok {
		t.Fatal("route not found in server")
	}
	if route.importerID != "sidecar-A" {
		t.Errorf("route importerID: got %q, want %q", route.importerID, "sidecar-A")
	}
	if route.exporterID != "sidecar-B" {
		t.Errorf("route exporterID: got %q, want %q", route.exporterID, "sidecar-B")
	}
}

func TestStreamClose(t *testing.T) {
	sockPath := tempSocketPath(t)
	srv := New(Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Sidecar B registers "close-svc".
	codecB := connectSidecar(t, sockPath, "sc-close-B")
	defer codecB.Close()
	registerService(t, codecB, "close-svc", 9090)

	// Sidecar A connects.
	codecA := connectSidecar(t, sockPath, "sc-close-A")
	defer codecA.Close()

	codecA.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnect,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnect(&protocol.ConnectPayload{
			ServiceName: "close-svc",
			Protocol:    protocol.ProtoTCP,
		}),
	})

	// Read ConnectAck on A.
	frame, err := codecA.ReadFrame()
	if err != nil {
		t.Fatalf("read connect ack: %v", err)
	}
	connAck, err := protocol.DecodeConnectAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode connect ack: %v", err)
	}
	if connAck.Status != protocol.StatusOK {
		t.Fatalf("connect status=%d, want OK", connAck.Status)
	}
	streamID := connAck.StreamID

	// Consume the OpenStream on B.
	frame, err = codecB.ReadFrame()
	if err != nil {
		t.Fatalf("read open stream: %v", err)
	}
	if frame.Type != protocol.MsgTypeOpenStream {
		t.Fatalf("expected OpenStream, got 0x%02x", frame.Type)
	}

	// Verify route exists before close.
	srv.mu.RLock()
	_, routeExists := srv.routes[streamID]
	srv.mu.RUnlock()
	if !routeExists {
		t.Fatal("route should exist before StreamClose")
	}

	// Sidecar A sends StreamClose.
	if err := codecA.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeStreamClose,
		StreamID: streamID,
		Payload:  protocol.EncodeStreamClose(&protocol.StreamClosePayload{Reason: protocol.CloseNormal}),
	}); err != nil {
		t.Fatalf("write stream close: %v", err)
	}

	// Sidecar B should receive the StreamClose.
	frame, err = codecB.ReadFrame()
	if err != nil {
		t.Fatalf("read stream close on B: %v", err)
	}
	if frame.Type != protocol.MsgTypeStreamClose {
		t.Fatalf("expected StreamClose, got 0x%02x", frame.Type)
	}
	if frame.StreamID != streamID {
		t.Errorf("close streamID: got %d, want %d", frame.StreamID, streamID)
	}
	sc, err := protocol.DecodeStreamClose(frame.Payload)
	if err != nil {
		t.Fatalf("decode stream close: %v", err)
	}
	if sc.Reason != protocol.CloseNormal {
		t.Errorf("close reason: got %d, want %d (CloseNormal)", sc.Reason, protocol.CloseNormal)
	}

	// Give server time to clean up route.
	time.Sleep(50 * time.Millisecond)

	// Verify route is removed.
	srv.mu.RLock()
	_, routeExists = srv.routes[streamID]
	srv.mu.RUnlock()
	if routeExists {
		t.Error("route should be removed after StreamClose")
	}
}

func TestOpenStreamAck(t *testing.T) {
	sockPath := tempSocketPath(t)
	srv := New(Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Sidecar B registers a service and connects.
	codecB := connectSidecar(t, sockPath, "sc-osa-B")
	defer codecB.Close()
	registerService(t, codecB, "osa-svc", 7070)

	// Sidecar A connects to the service.
	codecA := connectSidecar(t, sockPath, "sc-osa-A")
	defer codecA.Close()

	codecA.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnect,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnect(&protocol.ConnectPayload{
			ServiceName: "osa-svc",
			Protocol:    protocol.ProtoTCP,
		}),
	})

	// Read ConnectAck on A.
	frame, err := codecA.ReadFrame()
	if err != nil {
		t.Fatalf("read connect ack: %v", err)
	}
	connAck, err := protocol.DecodeConnectAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode connect ack: %v", err)
	}
	streamID := connAck.StreamID

	// Consume OpenStream on B.
	frame, err = codecB.ReadFrame()
	if err != nil {
		t.Fatalf("read open stream: %v", err)
	}
	if frame.Type != protocol.MsgTypeOpenStream {
		t.Fatalf("expected OpenStream, got 0x%02x", frame.Type)
	}

	// Sidecar B (exporter) sends OpenStreamAck with OK status.
	if err := codecB.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeOpenStreamAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeOpenStreamAck(&protocol.OpenStreamAckPayload{
			StreamID: streamID,
			Status:   protocol.StatusOK,
		}),
	}); err != nil {
		t.Fatalf("write open stream ack: %v", err)
	}

	// Give server time to process.
	time.Sleep(50 * time.Millisecond)

	// Verify server is still running and the route still exists (no crash).
	srv.mu.RLock()
	_, routeOK := srv.routes[streamID]
	_, scAOK := srv.sidecars["sc-osa-A"]
	_, scBOK := srv.sidecars["sc-osa-B"]
	srv.mu.RUnlock()

	if !routeOK {
		t.Error("route should still exist after OpenStreamAck")
	}
	if !scAOK {
		t.Error("sidecar A should still be connected")
	}
	if !scBOK {
		t.Error("sidecar B should still be connected")
	}

	// Also send an OpenStreamAck with non-OK status to exercise that code path.
	if err := codecB.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeOpenStreamAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeOpenStreamAck(&protocol.OpenStreamAckPayload{
			StreamID: streamID,
			Status:   protocol.StatusConnectionFailed,
			Message:  "connection refused",
		}),
	}); err != nil {
		t.Fatalf("write non-OK open stream ack: %v", err)
	}

	// Give server time to process (should just log, no crash).
	time.Sleep(50 * time.Millisecond)

	// Verify server is still healthy.
	srv.mu.RLock()
	_, scBStillOK := srv.sidecars["sc-osa-B"]
	srv.mu.RUnlock()
	if !scBStillOK {
		t.Error("sidecar B should still be connected after non-OK OpenStreamAck")
	}
}

func TestHeartbeatTimeout(t *testing.T) {
	sockPath := tempSocketPath(t)
	srv := New(Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 200 * time.Millisecond,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Connect and register a service.
	codec := connectSidecar(t, sockPath, "timeout-sc")
	defer codec.Close()
	registerService(t, codec, "timeout-svc", 5555)

	// Verify sidecar and service are registered.
	srv.mu.RLock()
	_, scOK := srv.sidecars["timeout-sc"]
	_, svcOK := srv.services["timeout-svc"]
	srv.mu.RUnlock()
	if !scOK {
		t.Fatal("sidecar should be registered")
	}
	if !svcOK {
		t.Fatal("service should be registered")
	}

	// Do NOT send any heartbeats. Wait for timeout + monitor interval.
	// HeartbeatTimeout=200ms, monitor runs every 100ms, so wait enough time.
	time.Sleep(500 * time.Millisecond)

	// Verify sidecar and service are cleaned up.
	srv.mu.RLock()
	_, scOK = srv.sidecars["timeout-sc"]
	_, svcOK = srv.services["timeout-svc"]
	srv.mu.RUnlock()

	if scOK {
		t.Error("sidecar should be evicted after heartbeat timeout")
	}
	if svcOK {
		t.Error("service should be removed after sidecar eviction")
	}
}

func TestConnectExporterDisconnected(t *testing.T) {
	sockPath := tempSocketPath(t)
	srv := New(Config{
		SocketPath:       sockPath,
		HeartbeatTimeout: 5 * time.Second,
	})
	go srv.Run()
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Sidecar B registers "disc-svc" then disconnects.
	codecB := connectSidecar(t, sockPath, "disc-B")
	registerService(t, codecB, "disc-svc", 6060)

	// Verify the service is registered.
	srv.mu.RLock()
	_, svcOK := srv.services["disc-svc"]
	srv.mu.RUnlock()
	if !svcOK {
		t.Fatal("disc-svc should be registered")
	}

	// Disconnect sidecar B. The service entry remains because we manually
	// re-add it after cleanup to simulate a stale service record scenario.
	codecB.Close()
	time.Sleep(200 * time.Millisecond)

	// After disconnect, removeSidecar cleans up the service.
	// Re-insert the service entry pointing to the now-absent sidecar
	// to test the "exporter disconnected" path in handleConnect.
	srv.mu.Lock()
	srv.services["disc-svc"] = &exportedService{
		sidecarID: "disc-B",
		protocol:  protocol.ProtoTCP,
		localPort: 6060,
	}
	srv.mu.Unlock()

	// Sidecar A connects and tries to use the service.
	codecA := connectSidecar(t, sockPath, "disc-A")
	defer codecA.Close()

	if err := codecA.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnect,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnect(&protocol.ConnectPayload{
			ServiceName: "disc-svc",
			Protocol:    protocol.ProtoTCP,
		}),
	}); err != nil {
		t.Fatalf("write connect: %v", err)
	}

	frame, err := codecA.ReadFrame()
	if err != nil {
		t.Fatalf("read connect ack: %v", err)
	}
	if frame.Type != protocol.MsgTypeConnectAck {
		t.Fatalf("expected ConnectAck, got 0x%02x", frame.Type)
	}
	ack, err := protocol.DecodeConnectAck(frame.Payload)
	if err != nil {
		t.Fatalf("decode connect ack: %v", err)
	}
	if ack.Status != protocol.StatusNoExporter {
		t.Errorf("status: got %d, want %d (StatusNoExporter)", ack.Status, protocol.StatusNoExporter)
	}
	if ack.ServiceName != "disc-svc" {
		t.Errorf("service name: got %q, want %q", ack.ServiceName, "disc-svc")
	}
}

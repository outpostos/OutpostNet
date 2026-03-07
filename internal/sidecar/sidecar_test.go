package sidecar

import (
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
)

// --- Helpers ---

// newTestSidecar creates a Sidecar pre-wired to a fake server codec via net.Pipe().
// Returns (sidecar, serverCodec, rawServerConn). The rawServerConn can be used
// to set read deadlines to avoid hanging tests.
func newTestSidecar(t *testing.T, config Config) (*Sidecar, *protocol.Codec, net.Conn) {
	t.Helper()
	scConn, srvConn := net.Pipe()
	t.Cleanup(func() {
		scConn.Close()
		srvConn.Close()
	})
	sc := New(config)
	sc.codec = protocol.NewCodec(scConn)
	srvCodec := protocol.NewCodec(srvConn)
	return sc, srvCodec, srvConn
}

// readFrameTimeout reads a frame with a deadline to avoid test hangs.
func readFrameTimeout(t *testing.T, codec *protocol.Codec, conn net.Conn, timeout time.Duration) *protocol.Frame {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	frame, err := codec.ReadFrame()
	conn.SetReadDeadline(time.Time{}) // reset
	if err != nil {
		t.Fatalf("readFrameTimeout: %v", err)
	}
	return frame
}

// --- Group 1: Construction & Config ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ServerSocket != "/var/run/outpostnet/server.sock" {
		t.Errorf("ServerSocket: got %q", cfg.ServerSocket)
	}
	if cfg.HeartbeatInterval != 10*time.Second {
		t.Errorf("HeartbeatInterval: got %v", cfg.HeartbeatInterval)
	}
}

func TestNew(t *testing.T) {
	sc := New(Config{SidecarID: "test-id"})
	if sc.config.SidecarID != "test-id" {
		t.Errorf("SidecarID: got %q", sc.config.SidecarID)
	}
	if sc.streams == nil {
		t.Error("streams map not initialized")
	}
	if sc.udpSessions == nil {
		t.Error("udpSessions map not initialized")
	}
	if sc.connectCh == nil {
		t.Error("connectCh not initialized")
	}
	if sc.done == nil {
		t.Error("done channel not initialized")
	}
}

// --- Group 2: Stop ---

func TestStop_Idempotent(t *testing.T) {
	sc := New(Config{SidecarID: "stop-test"})
	sc.Stop()
	sc.Stop() // must not panic
}

func TestStop_ClosesStreams(t *testing.T) {
	sc, _, _ := newTestSidecar(t, Config{SidecarID: "stop-streams"})

	p1a, p1b := net.Pipe()
	p2a, p2b := net.Pipe()
	defer p1b.Close()
	defer p2b.Close()

	sc.mu.Lock()
	sc.streams[1] = p1a
	sc.streams[2] = p2a
	sc.mu.Unlock()

	sc.Stop()

	sc.mu.RLock()
	count := len(sc.streams)
	sc.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected 0 streams after Stop, got %d", count)
	}

	// Verify the connections were closed.
	if _, err := p1b.Read(make([]byte, 1)); err == nil {
		t.Error("expected read error on closed stream 1")
	}
	if _, err := p2b.Read(make([]byte, 1)); err == nil {
		t.Error("expected read error on closed stream 2")
	}
}

// --- Group 3: Handshake ---

func TestHandshake_Success(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "hs-ok"})

	errCh := make(chan error, 1)
	go func() {
		errCh <- sc.handshake()
	}()

	// Server reads handshake.
	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeHandshake {
		t.Fatalf("expected Handshake, got 0x%02x", frame.Type)
	}
	hs, err := protocol.DecodeHandshake(frame.Payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hs.SidecarID != "hs-ok" {
		t.Errorf("SidecarID: got %q", hs.SidecarID)
	}
	if hs.ProtocolVersion != protocol.ProtocolVersionV1 {
		t.Errorf("version: got %d", hs.ProtocolVersion)
	}

	// Server replies OK.
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshakeAck,
		StreamID: protocol.StreamIDControl,
		Payload:  protocol.EncodeHandshakeAck(&protocol.HandshakeAckPayload{Status: protocol.StatusOK}),
	})

	if err := <-errCh; err != nil {
		t.Fatalf("handshake error: %v", err)
	}
}

func TestHandshake_Rejected(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "hs-reject"})

	errCh := make(chan error, 1)
	go func() {
		errCh <- sc.handshake()
	}()

	readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshakeAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeHandshakeAck(&protocol.HandshakeAckPayload{
			Status:  protocol.StatusInvalid,
			Message: "version mismatch",
		}),
	})

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for rejected handshake")
	}
}

func TestHandshake_WrongResponseType(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "hs-wrong"})

	errCh := make(chan error, 1)
	go func() {
		errCh <- sc.handshake()
	}()

	readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	// Reply with wrong type.
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegisterAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeRegisterAck(&protocol.RegisterAckPayload{
			ServiceName: "x", Status: protocol.StatusOK,
		}),
	})

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for wrong response type")
	}
}

// --- Group 4: Register Export ---

func TestRegisterExport_Success(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "reg-ok"})

	exp := ExportEntry{Name: "web", Protocol: protocol.ProtoTCP, Port: 8080}
	errCh := make(chan error, 1)
	go func() {
		errCh <- sc.registerExport(exp)
	}()

	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeRegister {
		t.Fatalf("expected Register, got 0x%02x", frame.Type)
	}
	reg, _ := protocol.DecodeRegister(frame.Payload)
	if reg.ServiceName != "web" || reg.LocalPort != 8080 || reg.Protocol != protocol.ProtoTCP {
		t.Errorf("register payload: %+v", reg)
	}

	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegisterAck,
		StreamID: protocol.StreamIDControl,
		Payload:  protocol.EncodeRegisterAck(&protocol.RegisterAckPayload{ServiceName: "web", Status: protocol.StatusOK}),
	})

	if err := <-errCh; err != nil {
		t.Fatalf("register error: %v", err)
	}
}

func TestRegisterExport_Rejected(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "reg-rej"})

	errCh := make(chan error, 1)
	go func() {
		errCh <- sc.registerExport(ExportEntry{Name: "dup", Protocol: protocol.ProtoTCP, Port: 80})
	}()

	readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegisterAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeRegisterAck(&protocol.RegisterAckPayload{
			ServiceName: "dup", Status: protocol.StatusDuplicate, Message: "already exists",
		}),
	})

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for rejected register")
	}
}

// --- Group 5: requestConnect ---

func TestRequestConnect_Success(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "rc-ok"})

	type result struct {
		streamID uint32
		err      error
	}
	ch := make(chan result, 1)
	go func() {
		sid, err := sc.requestConnect("db-svc", protocol.ProtoTCP)
		ch <- result{sid, err}
	}()

	// Read the Connect frame.
	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeConnect {
		t.Fatalf("expected Connect, got 0x%02x", frame.Type)
	}
	cp, _ := protocol.DecodeConnect(frame.Payload)
	if cp.ServiceName != "db-svc" {
		t.Errorf("ServiceName: got %q", cp.ServiceName)
	}

	// Deliver ack via connectCh (simulating what messageLoop+handleConnectAck would do).
	sc.connectCh <- &protocol.ConnectAckPayload{
		ServiceName: "db-svc",
		StreamID:    42,
		Status:      protocol.StatusOK,
	}

	r := <-ch
	if r.err != nil {
		t.Fatalf("requestConnect error: %v", r.err)
	}
	if r.streamID != 42 {
		t.Errorf("streamID: got %d, want 42", r.streamID)
	}
}

func TestRequestConnect_Rejected(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "rc-rej"})

	errCh := make(chan error, 1)
	go func() {
		_, err := sc.requestConnect("missing", protocol.ProtoTCP)
		errCh <- err
	}()

	readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	sc.connectCh <- &protocol.ConnectAckPayload{
		ServiceName: "missing",
		Status:      protocol.StatusNotFound,
		Message:     "not found",
	}

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for rejected connect")
	}
}

func TestRequestConnect_Shutdown(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "rc-shut"})

	errCh := make(chan error, 1)
	go func() {
		_, err := sc.requestConnect("svc", protocol.ProtoTCP)
		errCh <- err
	}()

	readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	close(sc.done) // trigger shutdown

	err := <-errCh
	if err == nil {
		t.Fatal("expected error on shutdown")
	}
}

// --- Group 6: handleConnectAck ---

func TestHandleConnectAck_DeliverToWaiter(t *testing.T) {
	sc := New(Config{SidecarID: "ca-ok"})

	frame := &protocol.Frame{
		Type:    protocol.MsgTypeConnectAck,
		Payload: protocol.EncodeConnectAck(&protocol.ConnectAckPayload{ServiceName: "svc", StreamID: 7, Status: protocol.StatusOK}),
	}

	// Start a reader on connectCh.
	done := make(chan *protocol.ConnectAckPayload, 1)
	go func() {
		done <- <-sc.connectCh
	}()

	sc.handleConnectAck(frame)

	select {
	case ack := <-done:
		if ack.StreamID != 7 {
			t.Errorf("StreamID: got %d", ack.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ConnectAck delivery")
	}
}

func TestHandleConnectAck_Dropped(t *testing.T) {
	sc := New(Config{SidecarID: "ca-drop"})

	frame := &protocol.Frame{
		Type:    protocol.MsgTypeConnectAck,
		Payload: protocol.EncodeConnectAck(&protocol.ConnectAckPayload{ServiceName: "svc", StreamID: 1, Status: protocol.StatusOK}),
	}

	// No reader on connectCh — should not block.
	done := make(chan struct{})
	go func() {
		sc.handleConnectAck(frame)
		close(done)
	}()

	select {
	case <-done:
		// OK, did not block.
	case <-time.After(time.Second):
		t.Fatal("handleConnectAck blocked with no waiter")
	}
}

// --- Group 7: handleData ---

func TestHandleData_WritesToLocalConn(t *testing.T) {
	sc := New(Config{SidecarID: "data-ok"})

	localA, localB := net.Pipe()
	defer localA.Close()
	defer localB.Close()

	sc.mu.Lock()
	sc.streams[42] = localA
	sc.mu.Unlock()

	// Run in goroutine because net.Pipe Write blocks until the other end reads.
	go sc.handleData(&protocol.Frame{
		Type:     protocol.MsgTypeData,
		StreamID: 42,
		Payload:  []byte("hello sidecar"),
	})

	buf := make([]byte, 64)
	localB.SetReadDeadline(time.Now().Add(time.Second))
	n, err := localB.Read(buf)
	if err != nil {
		t.Fatalf("read from local: %v", err)
	}
	if string(buf[:n]) != "hello sidecar" {
		t.Errorf("got %q, want %q", string(buf[:n]), "hello sidecar")
	}
}

func TestHandleData_UnknownStream(t *testing.T) {
	sc := New(Config{SidecarID: "data-unknown"})

	// Should not panic with unknown StreamID.
	sc.handleData(&protocol.Frame{
		Type:     protocol.MsgTypeData,
		StreamID: 999,
		Payload:  []byte("orphan"),
	})
}

func TestHandleData_WriteError(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "data-err"})

	localA, localB := net.Pipe()
	localB.Close() // close reading end so Write on localA will fail

	sc.mu.Lock()
	sc.streams[10] = localA
	sc.mu.Unlock()

	// handleData should call closeStream which sends a StreamClose frame.
	go sc.handleData(&protocol.Frame{
		Type:     protocol.MsgTypeData,
		StreamID: 10,
		Payload:  []byte("will fail"),
	})

	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeStreamClose {
		t.Fatalf("expected StreamClose, got 0x%02x", frame.Type)
	}
	if frame.StreamID != 10 {
		t.Errorf("StreamID: got %d, want 10", frame.StreamID)
	}
	sc2, _ := protocol.DecodeStreamClose(frame.Payload)
	if sc2.Reason != protocol.CloseError {
		t.Errorf("reason: got %d, want CloseError(%d)", sc2.Reason, protocol.CloseError)
	}
}

// --- Group 8: handleStreamClose ---

func TestHandleStreamClose_ExistingStream(t *testing.T) {
	sc := New(Config{SidecarID: "sc-close"})

	localA, localB := net.Pipe()
	defer localB.Close()

	sc.mu.Lock()
	sc.streams[10] = localA
	sc.mu.Unlock()

	sc.handleStreamClose(&protocol.Frame{StreamID: 10})

	// Verify stream removed.
	sc.mu.RLock()
	_, ok := sc.streams[10]
	sc.mu.RUnlock()
	if ok {
		t.Error("stream 10 should be removed")
	}

	// Verify connection was closed.
	if _, err := localB.Read(make([]byte, 1)); err == nil {
		t.Error("expected error reading from closed connection")
	}
}

func TestHandleStreamClose_UnknownStream(t *testing.T) {
	sc := New(Config{SidecarID: "sc-close-unknown"})
	// Should not panic.
	sc.handleStreamClose(&protocol.Frame{StreamID: 999})
}

// --- Group 9: closeStream ---

func TestCloseStream_ExistingStream(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "cs-ok"})

	localA, localB := net.Pipe()
	defer localB.Close()

	sc.mu.Lock()
	sc.streams[3] = localA
	sc.mu.Unlock()

	go sc.closeStream(3, protocol.CloseNormal)

	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeStreamClose {
		t.Fatalf("expected StreamClose, got 0x%02x", frame.Type)
	}
	if frame.StreamID != 3 {
		t.Errorf("StreamID: got %d, want 3", frame.StreamID)
	}
	sc2, _ := protocol.DecodeStreamClose(frame.Payload)
	if sc2.Reason != protocol.CloseNormal {
		t.Errorf("reason: got %d, want CloseNormal", sc2.Reason)
	}

	// Verify stream removed.
	sc.mu.RLock()
	_, ok := sc.streams[3]
	sc.mu.RUnlock()
	if ok {
		t.Error("stream 3 should be removed after closeStream")
	}
}

func TestCloseStream_UnknownStream(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "cs-unknown"})

	// closeStream still sends a StreamClose frame even if streamID not in map.
	go sc.closeStream(999, protocol.CloseReset)

	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeStreamClose {
		t.Fatalf("expected StreamClose, got 0x%02x", frame.Type)
	}
	if frame.StreamID != 999 {
		t.Errorf("StreamID: got %d, want 999", frame.StreamID)
	}
}

// --- Group 10: relayLocalToServer ---

func TestRelayLocalToServer(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "relay"})

	localA, localB := net.Pipe()

	sc.mu.Lock()
	sc.streams[5] = localA
	sc.mu.Unlock()

	done := make(chan struct{})
	go func() {
		sc.relayLocalToServer(5, localA)
		close(done)
	}()

	// Write data from "local app" side.
	localB.Write([]byte("chunk1"))
	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeData {
		t.Fatalf("expected Data, got 0x%02x", frame.Type)
	}
	if frame.StreamID != 5 {
		t.Errorf("StreamID: got %d, want 5", frame.StreamID)
	}
	if string(frame.Payload) != "chunk1" {
		t.Errorf("payload: got %q, want %q", string(frame.Payload), "chunk1")
	}

	// Close local app → relay should send StreamClose and exit.
	localB.Close()

	frame = readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeStreamClose {
		t.Fatalf("expected StreamClose after local close, got 0x%02x", frame.Type)
	}
	if frame.StreamID != 5 {
		t.Errorf("close StreamID: got %d, want 5", frame.StreamID)
	}

	// Verify relay goroutine exits.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("relayLocalToServer did not exit after local close")
	}
}

// --- Group 11: heartbeatLoop ---

func TestHeartbeatLoop(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{
		SidecarID:         "hb-loop",
		HeartbeatInterval: 20 * time.Millisecond,
	})

	go sc.heartbeatLoop()

	frame := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if frame.Type != protocol.MsgTypeHeartbeat {
		t.Fatalf("expected Heartbeat, got 0x%02x", frame.Type)
	}
	hb, err := protocol.DecodeHeartbeat(frame.Payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Timestamp should be recent (within last 5 seconds).
	now := uint64(time.Now().UnixMilli())
	if now-hb.Timestamp > 5000 {
		t.Errorf("timestamp too old: %d (now=%d)", hb.Timestamp, now)
	}

	close(sc.done) // stop the loop
}

// --- Group 12: messageLoop ---

func TestMessageLoop_DataDispatch(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "ml-data"})

	localA, localB := net.Pipe()
	defer localA.Close()
	defer localB.Close()

	sc.mu.Lock()
	sc.streams[1] = localA
	sc.mu.Unlock()

	go sc.messageLoop()

	// Server sends Data frame.
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeData,
		StreamID: 1,
		Payload:  []byte("from-server"),
	})

	buf := make([]byte, 64)
	localB.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := localB.Read(buf)
	if err != nil {
		t.Fatalf("read from local: %v", err)
	}
	if string(buf[:n]) != "from-server" {
		t.Errorf("got %q, want %q", string(buf[:n]), "from-server")
	}

	srvConn.Close() // stop messageLoop
}

func TestMessageLoop_StreamCloseDispatch(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "ml-close"})

	localA, localB := net.Pipe()
	defer localB.Close()

	sc.mu.Lock()
	sc.streams[2] = localA
	sc.mu.Unlock()

	go sc.messageLoop()

	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeStreamClose,
		StreamID: 2,
		Payload:  protocol.EncodeStreamClose(&protocol.StreamClosePayload{Reason: protocol.CloseNormal}),
	})

	// Give time for dispatch.
	time.Sleep(50 * time.Millisecond)

	sc.mu.RLock()
	_, ok := sc.streams[2]
	sc.mu.RUnlock()
	if ok {
		t.Error("stream 2 should be removed by messageLoop")
	}

	srvConn.Close()
}

func TestMessageLoop_ConnectAckDispatch(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "ml-cack"})

	go sc.messageLoop()

	// Start a reader before sending the frame.
	ackCh := make(chan *protocol.ConnectAckPayload, 1)
	go func() {
		ackCh <- <-sc.connectCh
	}()

	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnectAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnectAck(&protocol.ConnectAckPayload{
			ServiceName: "svc", StreamID: 77, Status: protocol.StatusOK,
		}),
	})

	select {
	case ack := <-ackCh:
		if ack.StreamID != 77 {
			t.Errorf("StreamID: got %d", ack.StreamID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ConnectAck dispatch")
	}

	srvConn.Close()
}

func TestMessageLoop_HeartbeatAckIgnored(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "ml-hba"})

	go sc.messageLoop()

	// HeartbeatAck should be silently consumed.
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHeartbeatAck,
		StreamID: protocol.StreamIDControl,
		Payload:  protocol.EncodeHeartbeat(&protocol.HeartbeatPayload{Timestamp: 123}),
	})

	// No crash — close to end the loop.
	time.Sleep(50 * time.Millisecond)
	srvConn.Close()
}

func TestMessageLoop_UnknownType(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "ml-unknown"})

	localA, localB := net.Pipe()
	defer localA.Close()
	defer localB.Close()
	sc.mu.Lock()
	sc.streams[1] = localA
	sc.mu.Unlock()

	go sc.messageLoop()

	// Send unknown type — should not crash, loop continues.
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     0xFF,
		StreamID: 0,
		Payload:  []byte("garbage"),
	})

	// Send a valid Data frame to prove the loop is still running.
	srvCodec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeData,
		StreamID: 1,
		Payload:  []byte("still alive"),
	})

	buf := make([]byte, 64)
	localB.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := localB.Read(buf)
	if err != nil {
		t.Fatalf("read after unknown type: %v", err)
	}
	if string(buf[:n]) != "still alive" {
		t.Errorf("got %q", string(buf[:n]))
	}

	srvConn.Close()
}

func TestMessageLoop_ReadError(t *testing.T) {
	sc, _, srvConn := newTestSidecar(t, Config{SidecarID: "ml-err"})

	done := make(chan struct{})
	go func() {
		sc.messageLoop()
		close(done)
	}()

	// Close the server side → ReadFrame returns error → messageLoop exits.
	srvConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("messageLoop did not exit after read error")
	}
}

// --- Group 13: handleOpenStream ---

func TestHandleOpenStream_ServiceNotExported(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{
		SidecarID: "os-notfound",
		Exports:   []ExportEntry{}, // no exports
	})

	frame := &protocol.Frame{
		Type:     protocol.MsgTypeOpenStream,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeOpenStream(&protocol.OpenStreamPayload{
			StreamID:    42,
			ServiceName: "nonexistent",
			Protocol:    protocol.ProtoTCP,
		}),
	}

	go sc.handleOpenStream(frame)

	resp := readFrameTimeout(t, srvCodec, srvConn, 2*time.Second)
	if resp.Type != protocol.MsgTypeOpenStreamAck {
		t.Fatalf("expected OpenStreamAck, got 0x%02x", resp.Type)
	}
	ack, _ := protocol.DecodeOpenStreamAck(resp.Payload)
	if ack.Status != protocol.StatusNotFound {
		t.Errorf("status: got %d, want StatusNotFound(%d)", ack.Status, protocol.StatusNotFound)
	}
	if ack.StreamID != 42 {
		t.Errorf("StreamID: got %d, want 42", ack.StreamID)
	}
}

func TestHandleOpenStream_ConnectFailed(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{
		SidecarID: "os-fail",
		Exports: []ExportEntry{
			{Name: "dead-app", Protocol: protocol.ProtoTCP, Port: 1}, // port 1 unreachable
		},
	})

	frame := &protocol.Frame{
		Type:     protocol.MsgTypeOpenStream,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeOpenStream(&protocol.OpenStreamPayload{
			StreamID:    99,
			ServiceName: "dead-app",
			Protocol:    protocol.ProtoTCP,
		}),
	}

	go sc.handleOpenStream(frame)

	resp := readFrameTimeout(t, srvCodec, srvConn, 10*time.Second) // dial may take a moment
	ack, _ := protocol.DecodeOpenStreamAck(resp.Payload)
	if ack.Status != protocol.StatusConnectionFailed {
		t.Errorf("status: got %d, want StatusConnectionFailed(%d)", ack.Status, protocol.StatusConnectionFailed)
	}
	if ack.StreamID != 99 {
		t.Errorf("StreamID: got %d, want 99", ack.StreamID)
	}
}

func TestHandleOpenStream_Success(t *testing.T) {
	// Start a real TCP listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	// Accept one connection to prevent connection refused.
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Keep connection alive until test ends.
		buf := make([]byte, 1024)
		conn.Read(buf)
	}()

	sc, srvCodec, srvConn := newTestSidecar(t, Config{
		SidecarID: "os-ok",
		Exports: []ExportEntry{
			{Name: "local-app", Protocol: protocol.ProtoTCP, Port: uint16(port)},
		},
	})

	frame := &protocol.Frame{
		Type:     protocol.MsgTypeOpenStream,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeOpenStream(&protocol.OpenStreamPayload{
			StreamID:    7,
			ServiceName: "local-app",
			Protocol:    protocol.ProtoTCP,
		}),
	}

	go sc.handleOpenStream(frame)

	resp := readFrameTimeout(t, srvCodec, srvConn, 5*time.Second)
	if resp.Type != protocol.MsgTypeOpenStreamAck {
		t.Fatalf("expected OpenStreamAck, got 0x%02x", resp.Type)
	}
	ack, _ := protocol.DecodeOpenStreamAck(resp.Payload)
	if ack.Status != protocol.StatusOK {
		t.Fatalf("status: got %d, want StatusOK", ack.Status)
	}
	if ack.StreamID != 7 {
		t.Errorf("StreamID: got %d, want 7", ack.StreamID)
	}

	// Verify stream is registered.
	time.Sleep(50 * time.Millisecond)
	sc.mu.RLock()
	_, ok := sc.streams[7]
	sc.mu.RUnlock()
	if !ok {
		t.Error("stream 7 should be registered after successful OpenStream")
	}
}

// --- Group 14: udpImportConn ---

func TestUdpImportConn_Write(t *testing.T) {
	// Create a UDP socket pair.
	recvConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer recvConn.Close()

	sendConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer sendConn.Close()

	wrapper := &udpImportConn{
		pc:      sendConn,
		srcAddr: recvConn.LocalAddr(),
	}

	n, err := wrapper.Write([]byte("udp-data"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 8 {
		t.Errorf("wrote %d bytes, want 8", n)
	}

	// Read from recvConn.
	buf := make([]byte, 64)
	recvConn.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err = recvConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:n]) != "udp-data" {
		t.Errorf("got %q, want %q", string(buf[:n]), "udp-data")
	}
}

func TestUdpImportConn_ReadReturnsError(t *testing.T) {
	wrapper := &udpImportConn{}
	_, err := wrapper.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected error from Read")
	}
}

func TestUdpImportConn_CloseIsNoop(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	wrapper := &udpImportConn{pc: pc}
	if err := wrapper.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// Underlying PacketConn should still be usable.
	if _, err := pc.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}); err != nil {
		t.Errorf("underlying pc should still work: %v", err)
	}
}

func TestUdpImportConn_AddrMethods(t *testing.T) {
	pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
	defer pc.Close()
	srcAddr := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 5000}

	wrapper := &udpImportConn{pc: pc, srcAddr: srcAddr}

	if wrapper.LocalAddr() == nil {
		t.Error("LocalAddr should not be nil")
	}
	if wrapper.RemoteAddr().String() != "10.0.0.1:5000" {
		t.Errorf("RemoteAddr: got %q", wrapper.RemoteAddr().String())
	}

	// Deadline methods should not error.
	if err := wrapper.SetDeadline(time.Now()); err != nil {
		t.Errorf("SetDeadline: %v", err)
	}
	if err := wrapper.SetReadDeadline(time.Now()); err != nil {
		t.Errorf("SetReadDeadline: %v", err)
	}
	if err := wrapper.SetWriteDeadline(time.Now()); err != nil {
		t.Errorf("SetWriteDeadline: %v", err)
	}
}

// --- Group 15: startImportListener ---

func TestStartImportListener_TCP(t *testing.T) {
	// Use newTestSidecar so codec is not nil (the accept goroutine will use it).
	sc, _, _ := newTestSidecar(t, Config{SidecarID: "imp-tcp"})

	// Find a free port.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	err := sc.startImportListener(ImportEntry{
		Name:       "test-import",
		Protocol:   protocol.ProtoTCP,
		ListenPort: uint16(port),
	})
	if err != nil {
		t.Fatalf("startImportListener: %v", err)
	}

	if len(sc.importListeners) != 1 {
		t.Errorf("expected 1 import listener, got %d", len(sc.importListeners))
	}
}

func TestStartImportListener_UDP(t *testing.T) {
	sc, _, _ := newTestSidecar(t, Config{SidecarID: "imp-udp"})

	tmpPC, _ := net.ListenPacket("udp", "127.0.0.1:0")
	port := tmpPC.LocalAddr().(*net.UDPAddr).Port
	tmpPC.Close()

	err := sc.startImportListener(ImportEntry{
		Name:       "test-udp-import",
		Protocol:   protocol.ProtoUDP,
		ListenPort: uint16(port),
	})
	if err != nil {
		t.Fatalf("startImportListener UDP: %v", err)
	}

	if len(sc.importUDPConns) != 1 {
		t.Errorf("expected 1 UDP conn, got %d", len(sc.importUDPConns))
	}
}

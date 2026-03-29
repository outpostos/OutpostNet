package sidecar

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
)

// fakeUDPServer runs in a goroutine alongside handleUDPImport tests.
// It reads frames from srvCodec: Connect frames are answered via connectCh;
// Data frames are forwarded to dataCh.
func fakeUDPServer(
	t *testing.T,
	sc *Sidecar,
	srvCodec *protocol.Codec,
	srvConn net.Conn,
	nextStreamID *uint32,
	mu *sync.Mutex,
	dataCh chan *protocol.Frame,
	stopCh chan struct{},
) {
	t.Helper()
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			frame, err := srvCodec.ReadFrame()
			if err != nil {
				continue
			}
			switch frame.Type {
			case protocol.MsgTypeConnect:
				mu.Lock()
				sid := *nextStreamID
				*nextStreamID++
				mu.Unlock()
				select {
				case sc.connectCh <- &protocol.ConnectAckPayload{
					StreamID: sid,
					Status:   protocol.StatusOK,
				}:
				case <-stopCh:
					return
				}
			case protocol.MsgTypeData:
				select {
				case dataCh <- frame:
				default:
				}
			}
		}
	}()
}

// --- Group 16: handleUDPImport ---

// TestHandleUDPImport_NewSession verifies that the first packet from a source
// creates a new session, registers the stream, and forwards the data.
func TestHandleUDPImport_NewSession(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "udp-new-sess"})

	var nextSID uint32 = 200
	var sidMu sync.Mutex
	dataCh := make(chan *protocol.Frame, 4)
	stopCh := make(chan struct{})
	defer close(stopCh)

	fakeUDPServer(t, sc, srvCodec, srvConn, &nextSID, &sidMu, dataCh, stopCh)

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	imp := ImportEntry{Name: "udp-svc", Protocol: protocol.ProtoUDP}
	go sc.handleUDPImport(pc, imp)

	sender, err := net.DialUDP("udp", nil, pc.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sender.Close()

	if _, err := sender.Write([]byte("hello-udp")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Expect a Data frame relayed to the server.
	select {
	case f := <-dataCh:
		if string(f.Payload) != "hello-udp" {
			t.Errorf("payload: got %q, want %q", string(f.Payload), "hello-udp")
		}
		if f.StreamID != 200 {
			t.Errorf("StreamID: got %d, want 200", f.StreamID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Data frame")
	}

	// Session must be registered.
	sc.udpMu.RLock()
	sessCount := len(sc.udpSessions)
	sc.udpMu.RUnlock()
	if sessCount != 1 {
		t.Errorf("expected 1 udpSession, got %d", sessCount)
	}

	// Stream must be registered.
	sc.mu.RLock()
	_, streamOK := sc.streams[200]
	sc.mu.RUnlock()
	if !streamOK {
		t.Error("stream 200 should be registered")
	}
}

// TestHandleUDPImport_ExistingSessionReuse verifies that a second packet from
// the same source reuses the existing session (no second Connect).
func TestHandleUDPImport_ExistingSessionReuse(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "udp-reuse"})

	connectCount := 0
	var countMu sync.Mutex
	var nextSID uint32 = 10
	var sidMu sync.Mutex
	dataCh := make(chan *protocol.Frame, 8)
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Custom server goroutine that also counts Connect frames.
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			frame, err := srvCodec.ReadFrame()
			if err != nil {
				continue
			}
			switch frame.Type {
			case protocol.MsgTypeConnect:
				countMu.Lock()
				connectCount++
				countMu.Unlock()

				sidMu.Lock()
				sid := nextSID
				nextSID++
				sidMu.Unlock()

				select {
				case sc.connectCh <- &protocol.ConnectAckPayload{StreamID: sid, Status: protocol.StatusOK}:
				case <-stopCh:
					return
				}
			case protocol.MsgTypeData:
				select {
				case dataCh <- frame:
				default:
				}
			}
		}
	}()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	imp := ImportEntry{Name: "udp-svc", Protocol: protocol.ProtoUDP}
	go sc.handleUDPImport(pc, imp)

	sender, err := net.DialUDP("udp", nil, pc.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sender.Close()

	// First packet.
	sender.Write([]byte("pkt1"))
	select {
	case <-dataCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first Data frame")
	}

	// Second packet from the same sender.
	sender.Write([]byte("pkt2"))
	select {
	case f := <-dataCh:
		if string(f.Payload) != "pkt2" {
			t.Errorf("second payload: got %q", string(f.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second Data frame")
	}

	// Must have issued exactly one Connect.
	countMu.Lock()
	cc := connectCount
	countMu.Unlock()
	if cc != 1 {
		t.Errorf("expected 1 Connect, got %d", cc)
	}

	// Must still have exactly one session.
	sc.udpMu.RLock()
	sessCount := len(sc.udpSessions)
	sc.udpMu.RUnlock()
	if sessCount != 1 {
		t.Errorf("expected 1 udpSession, got %d", sessCount)
	}
}

// TestHandleUDPImport_LastSeenUpdated verifies that lastSeen is updated
// for an existing session on each packet.
func TestHandleUDPImport_LastSeenUpdated(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "udp-lastseen"})

	var nextSID uint32 = 50
	var sidMu sync.Mutex
	dataCh := make(chan *protocol.Frame, 8)
	stopCh := make(chan struct{})
	defer close(stopCh)

	fakeUDPServer(t, sc, srvCodec, srvConn, &nextSID, &sidMu, dataCh, stopCh)

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	imp := ImportEntry{Name: "udp-svc", Protocol: protocol.ProtoUDP}
	go sc.handleUDPImport(pc, imp)

	sender, _ := net.DialUDP("udp", nil, pc.LocalAddr().(*net.UDPAddr))
	defer sender.Close()

	// First packet — creates session.
	sender.Write([]byte("a"))
	select {
	case <-dataCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout on first packet")
	}

	// Record lastSeen after first packet (read under lock).
	readLastSeen := func() time.Time {
		sc.udpMu.RLock()
		defer sc.udpMu.RUnlock()
		for _, s := range sc.udpSessions {
			return s.lastSeen
		}
		return time.Time{}
	}

	firstSeen := readLastSeen()
	if firstSeen.IsZero() {
		t.Fatal("no session found after first packet")
	}

	// Small sleep to ensure time advances.
	time.Sleep(5 * time.Millisecond)

	// Second packet — updates lastSeen.
	sender.Write([]byte("b"))
	select {
	case <-dataCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout on second packet")
	}

	updatedSeen := readLastSeen()
	if !updatedSeen.After(firstSeen) {
		t.Errorf("lastSeen not updated: first=%v updated=%v", firstSeen, updatedSeen)
	}
}

// TestHandleUDPImport_Shutdown verifies that closing the PacketConn causes
// handleUDPImport to exit.
func TestHandleUDPImport_Shutdown(t *testing.T) {
	sc, _, _ := newTestSidecar(t, Config{SidecarID: "udp-shutdown"})

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	imp := ImportEntry{Name: "udp-svc", Protocol: protocol.ProtoUDP}
	done := make(chan struct{})
	go func() {
		sc.handleUDPImport(pc, imp)
		close(done)
	}()

	// Close the connection to trigger exit.
	pc.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleUDPImport did not exit after PacketConn closed")
	}
}

// TestHandleUDPImport_ShutdownViaDone verifies that closing the done channel
// (via Stop) causes handleUDPImport to exit without logging an error.
func TestHandleUDPImport_ShutdownViaDone(t *testing.T) {
	sc, _, _ := newTestSidecar(t, Config{SidecarID: "udp-done"})

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	imp := ImportEntry{Name: "udp-svc", Protocol: protocol.ProtoUDP}
	done := make(chan struct{})
	go func() {
		sc.handleUDPImport(pc, imp)
		close(done)
	}()

	// Signal shutdown, then close the conn to unblock ReadFrom.
	close(sc.done)
	pc.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleUDPImport did not exit after done closed")
	}
}

// TestHandleUDPImport_MultipleDistinctSources verifies that packets from N
// distinct sources each get their own session.
func TestHandleUDPImport_MultipleDistinctSources(t *testing.T) {
	const N = 5
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "udp-multi"})

	var nextSID uint32 = 1
	var sidMu sync.Mutex
	dataCh := make(chan *protocol.Frame, N*2)
	stopCh := make(chan struct{})
	defer close(stopCh)

	fakeUDPServer(t, sc, srvCodec, srvConn, &nextSID, &sidMu, dataCh, stopCh)

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	imp := ImportEntry{Name: "udp-multi-svc", Protocol: protocol.ProtoUDP}
	go sc.handleUDPImport(pc, imp)

	// Open N distinct senders.
	senders := make([]*net.UDPConn, N)
	for i := range senders {
		s, err := net.DialUDP("udp", nil, pc.LocalAddr().(*net.UDPAddr))
		if err != nil {
			t.Fatalf("dial[%d]: %v", i, err)
		}
		defer s.Close()
		senders[i] = s
	}

	// Each sender sends one packet.
	for i, s := range senders {
		msg := []byte{byte('A' + i)}
		s.Write(msg)
	}

	// Wait for N Data frames.
	received := 0
	deadline := time.After(3 * time.Second)
	for received < N {
		select {
		case <-dataCh:
			received++
		case <-deadline:
			t.Fatalf("timeout: received %d/%d Data frames", received, N)
		}
	}

	// Should have N sessions.
	sc.udpMu.RLock()
	sessCount := len(sc.udpSessions)
	sc.udpMu.RUnlock()
	if sessCount != N {
		t.Errorf("expected %d sessions, got %d", N, sessCount)
	}
}

// --- Group 17: cleanUDPSessions ---

// TestCleanUDPSessions_RemovesStaleSession verifies that sessions idle beyond
// the timeout are removed from both udpSessions and streams.
func TestCleanUDPSessions_RemovesStaleSession(t *testing.T) {
	sc := New(Config{SidecarID: "clean-stale"})

	// Inject a stale session.
	sc.udpMu.Lock()
	sc.udpSessions["1.2.3.4:5000"] = &udpSession{
		streamID: 7,
		lastSeen: time.Now().Add(-2 * time.Minute),
	}
	sc.udpMu.Unlock()

	// Inject the corresponding stream.
	sc.mu.Lock()
	p1, p2 := net.Pipe()
	sc.streams[7] = p1
	sc.mu.Unlock()
	defer p2.Close()

	removed := sc.cleanUDPSessions(30 * time.Second)
	if removed != 1 {
		t.Errorf("removed: got %d, want 1", removed)
	}

	sc.udpMu.RLock()
	_, sessOK := sc.udpSessions["1.2.3.4:5000"]
	sc.udpMu.RUnlock()
	if sessOK {
		t.Error("stale session should have been removed from udpSessions")
	}

	sc.mu.RLock()
	_, streamOK := sc.streams[7]
	sc.mu.RUnlock()
	if streamOK {
		t.Error("stale session's stream should have been removed from streams")
	}
}

// TestCleanUDPSessions_KeepsFreshSession verifies that a recent session is
// not removed.
func TestCleanUDPSessions_KeepsFreshSession(t *testing.T) {
	sc := New(Config{SidecarID: "clean-fresh"})

	sc.udpMu.Lock()
	sc.udpSessions["10.0.0.1:1234"] = &udpSession{
		streamID: 3,
		lastSeen: time.Now(),
	}
	sc.udpMu.Unlock()

	removed := sc.cleanUDPSessions(30 * time.Second)
	if removed != 0 {
		t.Errorf("removed: got %d, want 0", removed)
	}

	sc.udpMu.RLock()
	_, ok := sc.udpSessions["10.0.0.1:1234"]
	sc.udpMu.RUnlock()
	if !ok {
		t.Error("fresh session should not have been removed")
	}
}

// TestCleanUDPSessions_MixedSessionAges verifies that only stale sessions are
// removed when the map contains a mix.
func TestCleanUDPSessions_MixedSessionAges(t *testing.T) {
	sc := New(Config{SidecarID: "clean-mixed"})

	sc.udpMu.Lock()
	sc.udpSessions["stale"] = &udpSession{
		streamID: 1,
		lastSeen: time.Now().Add(-2 * time.Minute),
	}
	sc.udpSessions["fresh"] = &udpSession{
		streamID: 2,
		lastSeen: time.Now(),
	}
	sc.udpMu.Unlock()

	removed := sc.cleanUDPSessions(30 * time.Second)
	if removed != 1 {
		t.Errorf("removed: got %d, want 1", removed)
	}

	sc.udpMu.RLock()
	_, staleOK := sc.udpSessions["stale"]
	_, freshOK := sc.udpSessions["fresh"]
	sc.udpMu.RUnlock()

	if staleOK {
		t.Error("stale session should be removed")
	}
	if !freshOK {
		t.Error("fresh session should be kept")
	}
}

// TestCleanUDPSessions_EmptyMap verifies no panic or error on empty map.
func TestCleanUDPSessions_EmptyMap(t *testing.T) {
	sc := New(Config{SidecarID: "clean-empty"})
	removed := sc.cleanUDPSessions(30 * time.Second)
	if removed != 0 {
		t.Errorf("removed: got %d, want 0", removed)
	}
}

// TestCleanUDPSessions_ConcurrentAccess runs cleanUDPSessions concurrently
// with goroutines that read/write the session map. Must pass with -race.
func TestCleanUDPSessions_ConcurrentAccess(t *testing.T) {
	sc := New(Config{SidecarID: "clean-concurrent"})

	// Pre-populate with some stale sessions.
	sc.udpMu.Lock()
	for i := 0; i < 20; i++ {
		key := net.JoinHostPort("10.0.0.1", string(rune('a'+i)))
		sc.udpSessions[key] = &udpSession{
			streamID: uint32(i),
			lastSeen: time.Now().Add(-2 * time.Minute),
		}
	}
	sc.udpMu.Unlock()

	var wg sync.WaitGroup

	// Concurrent cleaners.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				sc.cleanUDPSessions(30 * time.Second)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Concurrent readers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				sc.udpMu.RLock()
				_ = len(sc.udpSessions)
				sc.udpMu.RUnlock()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Concurrent writers (adding new sessions).
	for i := 0; i < 4; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := net.JoinHostPort("192.168.1.1", string(rune('A'+idx)))
			sc.udpMu.Lock()
			sc.udpSessions[key] = &udpSession{
				streamID: uint32(100 + idx),
				lastSeen: time.Now(),
			}
			sc.udpMu.Unlock()
		}()
	}

	wg.Wait()
}

// TestUDPCleanupLoop_StartsAndStops verifies the cleanup loop exits when
// the sidecar is stopped.
func TestUDPCleanupLoop_StartsAndStops(t *testing.T) {
	sc := New(Config{
		SidecarID:         "cleanup-loop",
		UDPSessionTimeout: 20 * time.Millisecond,
	})

	done := make(chan struct{})
	go func() {
		sc.udpCleanupLoop()
		close(done)
	}()

	// Inject a stale session that the loop should remove.
	sc.udpMu.Lock()
	sc.udpSessions["1.2.3.4:9000"] = &udpSession{
		streamID: 1,
		lastSeen: time.Now().Add(-time.Hour),
	}
	sc.udpMu.Unlock()

	// Wait long enough for at least one tick.
	time.Sleep(60 * time.Millisecond)

	sc.udpMu.RLock()
	remaining := len(sc.udpSessions)
	sc.udpMu.RUnlock()
	if remaining != 0 {
		t.Errorf("expected 0 sessions after cleanup loop tick, got %d", remaining)
	}

	close(sc.done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("udpCleanupLoop did not stop after done closed")
	}
}

// TestUDPCleanupLoop_DisabledWhenZeroTimeout verifies the loop exits
// immediately when UDPSessionTimeout is 0.
func TestUDPCleanupLoop_DisabledWhenZeroTimeout(t *testing.T) {
	sc := New(Config{
		SidecarID:         "cleanup-disabled",
		UDPSessionTimeout: 0,
	})
	done := make(chan struct{})
	go func() {
		sc.udpCleanupLoop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("udpCleanupLoop with zero timeout should return immediately")
	}
}

// --- Group 18: handleImportConnection ---

// TestHandleImportConnection_Success tests the full import connection flow:
// requestConnect succeeds, stream is registered, relay runs.
func TestHandleImportConnection_Success(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "imp-conn-ok"})

	// Server goroutine: respond to Connect, then accept Data.
	dataCh := make(chan []byte, 4)
	go func() {
		for {
			srvConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			frame, err := srvCodec.ReadFrame()
			if err != nil {
				return
			}
			switch frame.Type {
			case protocol.MsgTypeConnect:
				sc.connectCh <- &protocol.ConnectAckPayload{
					StreamID: 77,
					Status:   protocol.StatusOK,
				}
			case protocol.MsgTypeData:
				select {
				case dataCh <- append([]byte(nil), frame.Payload...):
				default:
				}
			}
		}
	}()

	appA, appB := net.Pipe()
	defer appB.Close()

	imp := ImportEntry{Name: "back-svc", Protocol: protocol.ProtoTCP}
	go sc.handleImportConnection(appA, imp)

	// Write from app side → should arrive as Data frame.
	appB.Write([]byte("app-data"))

	select {
	case payload := <-dataCh:
		if string(payload) != "app-data" {
			t.Errorf("payload: got %q, want %q", string(payload), "app-data")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for relayed Data frame")
	}

	// Stream 77 should be registered.
	sc.mu.RLock()
	_, ok := sc.streams[77]
	sc.mu.RUnlock()
	if !ok {
		t.Error("stream 77 should be registered")
	}
}

// TestHandleImportConnection_ConnectFail verifies that if requestConnect fails
// the application connection is closed and no stream is registered.
func TestHandleImportConnection_ConnectFail(t *testing.T) {
	sc, srvCodec, srvConn := newTestSidecar(t, Config{SidecarID: "imp-conn-fail"})

	// Server goroutine: reject the Connect.
	go func() {
		srvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		frame, err := srvCodec.ReadFrame()
		if err != nil {
			return
		}
		if frame.Type == protocol.MsgTypeConnect {
			sc.connectCh <- &protocol.ConnectAckPayload{
				Status:  protocol.StatusNotFound,
				Message: "no such service",
			}
		}
	}()

	appA, appB := net.Pipe()
	defer appB.Close()

	imp := ImportEntry{Name: "gone-svc", Protocol: protocol.ProtoTCP}
	done := make(chan struct{})
	go func() {
		sc.handleImportConnection(appA, imp)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleImportConnection did not exit on connect failure")
	}

	// appA should be closed → reading appB should fail.
	appB.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := appB.Read(make([]byte, 1)); err == nil {
		t.Error("expected appB read to fail after appA closed")
	}

	// No stream should be registered.
	sc.mu.RLock()
	count := len(sc.streams)
	sc.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected 0 streams, got %d", count)
	}
}

// --- Group 19: UDP Stress Test ---

// TestUDP_StressSessionCreateDestroy creates 1000 simulated UDP sessions,
// then destroys them via cleanUDPSessions, verifying no leaks.
func TestUDP_StressSessionCreateDestroy(t *testing.T) {
	const N = 1000
	sc := New(Config{SidecarID: "udp-stress"})

	// Directly populate N sessions simulating what handleUDPImport would do.
	sc.udpMu.Lock()
	sc.mu.Lock()
	for i := 0; i < N; i++ {
		key := net.JoinHostPort("10.1.2.3", string(rune(i+1)))
		sid := uint32(i + 1)
		sc.udpSessions[key] = &udpSession{
			streamID: sid,
			lastSeen: time.Now().Add(-2 * time.Minute), // all stale
		}
		// Simulate stream entry.
		sc.streams[sid] = &udpImportConn{}
	}
	sc.mu.Unlock()
	sc.udpMu.Unlock()

	// Verify pre-conditions.
	sc.udpMu.RLock()
	if len(sc.udpSessions) != N {
		t.Fatalf("pre-condition: expected %d sessions, got %d", N, len(sc.udpSessions))
	}
	sc.udpMu.RUnlock()

	// Single cleanup pass should remove all stale sessions.
	removed := sc.cleanUDPSessions(30 * time.Second)
	if removed != N {
		t.Errorf("removed: got %d, want %d", removed, N)
	}

	// Verify no leaks.
	sc.udpMu.RLock()
	udpLeft := len(sc.udpSessions)
	sc.udpMu.RUnlock()
	if udpLeft != 0 {
		t.Errorf("udpSessions leak: %d remaining", udpLeft)
	}

	sc.mu.RLock()
	streamsLeft := len(sc.streams)
	sc.mu.RUnlock()
	if streamsLeft != 0 {
		t.Errorf("streams leak: %d remaining", streamsLeft)
	}
}

// TestUDP_StressSessionConcurrent runs N goroutines each adding a session
// and a cleaner goroutine simultaneously; verifies no race or panic.
func TestUDP_StressSessionConcurrent(t *testing.T) {
	const N = 200
	sc := New(Config{
		SidecarID:         "udp-stress-conc",
		UDPSessionTimeout: 5 * time.Millisecond,
	})

	var wg sync.WaitGroup

	// N goroutines each add one session.
	for i := 0; i < N; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := net.JoinHostPort("10.0.0.1", string(rune(idx+1)))
			sid := uint32(idx + 1)
			sc.udpMu.Lock()
			sc.udpSessions[key] = &udpSession{
				streamID: sid,
				lastSeen: time.Now().Add(-time.Hour),
			}
			sc.udpMu.Unlock()
		}()
	}

	// Cleanup goroutines running concurrently.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				sc.cleanUDPSessions(5 * time.Millisecond)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// After all goroutines finish, a final cleanup should leave no stale sessions.
	sc.cleanUDPSessions(5 * time.Millisecond)

	sc.udpMu.RLock()
	left := len(sc.udpSessions)
	sc.udpMu.RUnlock()

	// Any remaining sessions must be fresh (added after time.Now()-5ms).
	// Since all sessions were added with lastSeen = time.Now()-1hour, all should be gone.
	if left != 0 {
		t.Errorf("expected 0 sessions after final cleanup, got %d", left)
	}
}

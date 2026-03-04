package protocol

import (
	"net"
	"testing"
)

func TestCodecRoundTrip(t *testing.T) {
	// Create a Unix socket pair for testing.
	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()

	writer := NewCodec(s1)
	reader := NewCodec(s2)

	want := &Frame{
		Type:     MsgTypeHeartbeat,
		StreamID: 0,
		Payload:  EncodeHeartbeat(&HeartbeatPayload{Timestamp: 1234567890}),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- writer.WriteFrame(want)
	}()

	got, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if <-errCh != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	if got.Type != want.Type {
		t.Errorf("Type: got %d, want %d", got.Type, want.Type)
	}
	if got.StreamID != want.StreamID {
		t.Errorf("StreamID: got %d, want %d", got.StreamID, want.StreamID)
	}
	if len(got.Payload) != len(want.Payload) {
		t.Errorf("Payload len: got %d, want %d", len(got.Payload), len(want.Payload))
	}
}

func TestCodecEmptyPayload(t *testing.T) {
	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()

	writer := NewCodec(s1)
	reader := NewCodec(s2)

	want := &Frame{
		Type:     MsgTypeHeartbeat,
		StreamID: StreamIDControl,
		Payload:  nil,
	}

	go writer.WriteFrame(want)

	got, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(got.Payload))
	}
}

func TestCodecDataFrame(t *testing.T) {
	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()

	writer := NewCodec(s1)
	reader := NewCodec(s2)

	payload := []byte("SELECT * FROM users WHERE id = 42;")
	want := &Frame{
		Type:     MsgTypeData,
		StreamID: 42,
		Payload:  payload,
	}

	go writer.WriteFrame(want)

	got, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.StreamID != 42 {
		t.Errorf("StreamID: got %d, want 42", got.StreamID)
	}
	if string(got.Payload) != string(payload) {
		t.Errorf("Payload mismatch")
	}
}

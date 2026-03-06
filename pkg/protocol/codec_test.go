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

func TestCodecClose(t *testing.T) {
	s1, s2 := net.Pipe()
	defer s2.Close()

	codec := NewCodec(s1)
	if err := codec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After closing, writing to s1 should fail because the connection is closed.
	_, err := s1.Write([]byte("hello"))
	if err == nil {
		t.Fatal("expected error writing to closed connection")
	}
}

func TestCodecReadFrameTooSmall(t *testing.T) {
	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()

	reader := NewCodec(s2)

	// Write raw bytes: a 4-byte big-endian Length field with value 3 (< FrameHeaderSize=5).
	go func() {
		// Length = 3 which is less than FrameHeaderSize (5).
		raw := []byte{0x00, 0x00, 0x00, 0x03}
		s1.Write(raw)
	}()

	_, err := reader.ReadFrame()
	if err == nil {
		t.Fatal("expected error for frame too small")
	}
	if err.Error() != "frame too small" {
		t.Fatalf("expected 'frame too small' error, got: %v", err)
	}
}

func TestCodecReadFramePayloadTooLarge(t *testing.T) {
	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()

	reader := NewCodec(s2)

	// Write raw bytes: a 4-byte big-endian Length field with value that makes
	// payload exceed MaxFramePayload (1 << 20 = 1048576).
	// payload = length - FrameHeaderSize(5), so length = MaxFramePayload + 5 + 1 = 1048582
	go func() {
		length := uint32(MaxFramePayload + FrameHeaderSize + 1) // 1048582
		raw := []byte{
			byte(length >> 24),
			byte(length >> 16),
			byte(length >> 8),
			byte(length),
		}
		s1.Write(raw)
	}()

	_, err := reader.ReadFrame()
	if err == nil {
		t.Fatal("expected error for payload too large")
	}
}

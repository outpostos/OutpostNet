package protocol

import "testing"

func TestHandshakeRoundTrip(t *testing.T) {
	want := &HandshakePayload{ProtocolVersion: ProtocolVersionV1, SidecarID: "pod-abc-123"}
	data := EncodeHandshake(want)
	got, err := DecodeHandshake(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ProtocolVersion != want.ProtocolVersion || got.SidecarID != want.SidecarID {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestRegisterRoundTrip(t *testing.T) {
	want := &RegisterPayload{ServiceName: "db-service", Protocol: ProtoTCP, LocalPort: 3306}
	data := EncodeRegister(want)
	got, err := DecodeRegister(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ServiceName != want.ServiceName || got.Protocol != want.Protocol || got.LocalPort != want.LocalPort {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestRegisterAckRoundTrip(t *testing.T) {
	want := &RegisterAckPayload{ServiceName: "web", Status: StatusOK, Message: ""}
	data := EncodeRegisterAck(want)
	got, err := DecodeRegisterAck(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ServiceName != want.ServiceName || got.Status != want.Status {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestConnectRoundTrip(t *testing.T) {
	want := &ConnectPayload{ServiceName: "redis", Protocol: ProtoTCP}
	data := EncodeConnect(want)
	got, err := DecodeConnect(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ServiceName != want.ServiceName || got.Protocol != want.Protocol {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestConnectAckRoundTrip(t *testing.T) {
	want := &ConnectAckPayload{ServiceName: "redis", StreamID: 42, Status: StatusOK, Message: ""}
	data := EncodeConnectAck(want)
	got, err := DecodeConnectAck(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ServiceName != want.ServiceName || got.StreamID != want.StreamID || got.Status != want.Status {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestOpenStreamRoundTrip(t *testing.T) {
	want := &OpenStreamPayload{StreamID: 99, ServiceName: "db", Protocol: ProtoTCP}
	data := EncodeOpenStream(want)
	got, err := DecodeOpenStream(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.StreamID != want.StreamID || got.ServiceName != want.ServiceName || got.Protocol != want.Protocol {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestHeartbeatRoundTrip(t *testing.T) {
	want := &HeartbeatPayload{Timestamp: 1709520000000}
	data := EncodeHeartbeat(want)
	got, err := DecodeHeartbeat(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Timestamp != want.Timestamp {
		t.Errorf("got %d, want %d", got.Timestamp, want.Timestamp)
	}
}

func TestStreamCloseRoundTrip(t *testing.T) {
	want := &StreamClosePayload{Reason: CloseNormal}
	data := EncodeStreamClose(want)
	got, err := DecodeStreamClose(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Reason != want.Reason {
		t.Errorf("got %d, want %d", got.Reason, want.Reason)
	}
}

func TestHandshakeAckRoundTrip(t *testing.T) {
	want := &HandshakeAckPayload{Status: StatusOK, Message: "ok"}
	data := EncodeHandshakeAck(want)
	got, err := DecodeHandshakeAck(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %d, want %d", got.Status, want.Status)
	}
	if got.Message != want.Message {
		t.Errorf("Message: got %q, want %q", got.Message, want.Message)
	}
}

func TestOpenStreamAckRoundTrip(t *testing.T) {
	want := &OpenStreamAckPayload{StreamID: 42, Status: StatusOK, Message: ""}
	data := EncodeOpenStreamAck(want)
	got, err := DecodeOpenStreamAck(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.StreamID != want.StreamID {
		t.Errorf("StreamID: got %d, want %d", got.StreamID, want.StreamID)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %d, want %d", got.Status, want.Status)
	}
	if got.Message != want.Message {
		t.Errorf("Message: got %q, want %q", got.Message, want.Message)
	}
}

func TestHandshakeAckWithMessage(t *testing.T) {
	want := &HandshakeAckPayload{Status: StatusInvalid, Message: "version mismatch"}
	data := EncodeHandshakeAck(want)
	got, err := DecodeHandshakeAck(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %d, want %d", got.Status, want.Status)
	}
	if got.Message != want.Message {
		t.Errorf("Message: got %q, want %q", got.Message, want.Message)
	}
}

func TestOpenStreamAckWithMessage(t *testing.T) {
	want := &OpenStreamAckPayload{StreamID: 7, Status: StatusNotFound, Message: "service not found"}
	data := EncodeOpenStreamAck(want)
	got, err := DecodeOpenStreamAck(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.StreamID != want.StreamID {
		t.Errorf("StreamID: got %d, want %d", got.StreamID, want.StreamID)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %d, want %d", got.Status, want.Status)
	}
	if got.Message != want.Message {
		t.Errorf("Message: got %q, want %q", got.Message, want.Message)
	}
}

func TestDecodeHandshakeAckEmpty(t *testing.T) {
	_, err := DecodeHandshakeAck([]byte{})
	if err == nil {
		t.Fatal("expected error when decoding empty data")
	}
}

func TestDecodeOpenStreamAckEmpty(t *testing.T) {
	_, err := DecodeOpenStreamAck([]byte{})
	if err == nil {
		t.Fatal("expected error when decoding empty data")
	}
}

func TestStreamCloseDecodeEmpty(t *testing.T) {
	_, err := DecodeStreamClose([]byte{})
	if err == nil {
		t.Fatal("expected error when decoding empty data")
	}
}

func TestDecodeHandshakeTruncated(t *testing.T) {
	// Only 1 byte — too short for the uint16 version field.
	_, err := DecodeHandshake([]byte{0x00})
	if err == nil {
		t.Fatal("expected error decoding truncated handshake")
	}
}

func TestDecodeRegisterTruncated(t *testing.T) {
	// Only 2 bytes — too short for the string length prefix of ServiceName.
	// decodeString needs at least 2 bytes for length, then more for the string body.
	// With exactly 2 bytes saying length=1 but no body byte, it should fail.
	_, err := DecodeRegister([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error decoding truncated register")
	}
}

func TestDecodeConnectTruncated(t *testing.T) {
	// Only 1 byte — too short for the string length prefix of ServiceName.
	_, err := DecodeConnect([]byte{0x00})
	if err == nil {
		t.Fatal("expected error decoding truncated connect")
	}
}

func TestDecodeConnectAckTruncated(t *testing.T) {
	// Only 2 bytes — too short; needs string length + string body + StreamID + Status + Message.
	_, err := DecodeConnectAck([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error decoding truncated connect ack")
	}
}

func TestDecodeOpenStreamTruncated(t *testing.T) {
	// Only 3 bytes — too short for the uint32 StreamID field (needs 4 bytes).
	_, err := DecodeOpenStream([]byte{0x00, 0x00, 0x01})
	if err == nil {
		t.Fatal("expected error decoding truncated open stream")
	}
}

func TestDecodeRegisterAckTruncated(t *testing.T) {
	// Only 1 byte — too short for the string length prefix of ServiceName.
	_, err := DecodeRegisterAck([]byte{0x00})
	if err == nil {
		t.Fatal("expected error decoding truncated register ack")
	}
}

func TestDecodeHeartbeatTruncated(t *testing.T) {
	// Only 4 bytes — needs 8 bytes for the uint64 Timestamp field.
	_, err := DecodeHeartbeat([]byte{0x00, 0x00, 0x00, 0x01})
	if err == nil {
		t.Fatal("expected error decoding truncated heartbeat")
	}
}

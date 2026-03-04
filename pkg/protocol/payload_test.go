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

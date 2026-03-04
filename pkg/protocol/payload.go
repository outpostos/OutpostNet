package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// --- String encoding helpers ---

func encodeString(buf *bytes.Buffer, s string) {
	_ = binary.Write(buf, binary.BigEndian, uint16(len(s)))
	buf.WriteString(s)
}

func decodeString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// --- Handshake ---

// HandshakePayload is sent by Sidecar -> Server on initial connection.
type HandshakePayload struct {
	ProtocolVersion uint16
	SidecarID       string
}

const ProtocolVersionV1 uint16 = 1

func EncodeHandshake(p *HandshakePayload) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.ProtocolVersion)
	encodeString(&buf, p.SidecarID)
	return buf.Bytes()
}

func DecodeHandshake(data []byte) (*HandshakePayload, error) {
	r := bytes.NewReader(data)
	p := &HandshakePayload{}
	if err := binary.Read(r, binary.BigEndian, &p.ProtocolVersion); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	var err error
	p.SidecarID, err = decodeString(r)
	if err != nil {
		return nil, fmt.Errorf("read sidecar_id: %w", err)
	}
	return p, nil
}

// --- HandshakeAck ---

type HandshakeAckPayload struct {
	Status  uint8
	Message string
}

func EncodeHandshakeAck(p *HandshakeAckPayload) []byte {
	var buf bytes.Buffer
	buf.WriteByte(p.Status)
	encodeString(&buf, p.Message)
	return buf.Bytes()
}

func DecodeHandshakeAck(data []byte) (*HandshakeAckPayload, error) {
	r := bytes.NewReader(data)
	p := &HandshakeAckPayload{}
	if err := binary.Read(r, binary.BigEndian, &p.Status); err != nil {
		return nil, err
	}
	var err error
	p.Message, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// --- Register ---

type RegisterPayload struct {
	ServiceName string
	Protocol    ServiceProtocol
	LocalPort   uint16
}

func EncodeRegister(p *RegisterPayload) []byte {
	var buf bytes.Buffer
	encodeString(&buf, p.ServiceName)
	buf.WriteByte(byte(p.Protocol))
	_ = binary.Write(&buf, binary.BigEndian, p.LocalPort)
	return buf.Bytes()
}

func DecodeRegister(data []byte) (*RegisterPayload, error) {
	r := bytes.NewReader(data)
	p := &RegisterPayload{}
	var err error
	p.ServiceName, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	var proto uint8
	if err := binary.Read(r, binary.BigEndian, &proto); err != nil {
		return nil, err
	}
	p.Protocol = ServiceProtocol(proto)
	if err := binary.Read(r, binary.BigEndian, &p.LocalPort); err != nil {
		return nil, err
	}
	return p, nil
}

// --- RegisterAck ---

type RegisterAckPayload struct {
	ServiceName string
	Status      uint8
	Message     string
}

func EncodeRegisterAck(p *RegisterAckPayload) []byte {
	var buf bytes.Buffer
	encodeString(&buf, p.ServiceName)
	buf.WriteByte(p.Status)
	encodeString(&buf, p.Message)
	return buf.Bytes()
}

func DecodeRegisterAck(data []byte) (*RegisterAckPayload, error) {
	r := bytes.NewReader(data)
	p := &RegisterAckPayload{}
	var err error
	p.ServiceName, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Status); err != nil {
		return nil, err
	}
	p.Message, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// --- Heartbeat ---

type HeartbeatPayload struct {
	Timestamp uint64 // Unix millis
}

func EncodeHeartbeat(p *HeartbeatPayload) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.Timestamp)
	return buf.Bytes()
}

func DecodeHeartbeat(data []byte) (*HeartbeatPayload, error) {
	r := bytes.NewReader(data)
	p := &HeartbeatPayload{}
	if err := binary.Read(r, binary.BigEndian, &p.Timestamp); err != nil {
		return nil, err
	}
	return p, nil
}

// --- Connect ---

type ConnectPayload struct {
	ServiceName string
	Protocol    ServiceProtocol
}

func EncodeConnect(p *ConnectPayload) []byte {
	var buf bytes.Buffer
	encodeString(&buf, p.ServiceName)
	buf.WriteByte(byte(p.Protocol))
	return buf.Bytes()
}

func DecodeConnect(data []byte) (*ConnectPayload, error) {
	r := bytes.NewReader(data)
	p := &ConnectPayload{}
	var err error
	p.ServiceName, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	var proto uint8
	if err := binary.Read(r, binary.BigEndian, &proto); err != nil {
		return nil, err
	}
	p.Protocol = ServiceProtocol(proto)
	return p, nil
}

// --- ConnectAck ---

type ConnectAckPayload struct {
	ServiceName string
	StreamID    uint32
	Status      uint8
	Message     string
}

func EncodeConnectAck(p *ConnectAckPayload) []byte {
	var buf bytes.Buffer
	encodeString(&buf, p.ServiceName)
	_ = binary.Write(&buf, binary.BigEndian, p.StreamID)
	buf.WriteByte(p.Status)
	encodeString(&buf, p.Message)
	return buf.Bytes()
}

func DecodeConnectAck(data []byte) (*ConnectAckPayload, error) {
	r := bytes.NewReader(data)
	p := &ConnectAckPayload{}
	var err error
	p.ServiceName, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.StreamID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Status); err != nil {
		return nil, err
	}
	p.Message, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// --- OpenStream ---

type OpenStreamPayload struct {
	StreamID    uint32
	ServiceName string
	Protocol    ServiceProtocol
}

func EncodeOpenStream(p *OpenStreamPayload) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.StreamID)
	encodeString(&buf, p.ServiceName)
	buf.WriteByte(byte(p.Protocol))
	return buf.Bytes()
}

func DecodeOpenStream(data []byte) (*OpenStreamPayload, error) {
	r := bytes.NewReader(data)
	p := &OpenStreamPayload{}
	if err := binary.Read(r, binary.BigEndian, &p.StreamID); err != nil {
		return nil, err
	}
	var err error
	p.ServiceName, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	var proto uint8
	if err := binary.Read(r, binary.BigEndian, &proto); err != nil {
		return nil, err
	}
	p.Protocol = ServiceProtocol(proto)
	return p, nil
}

// --- OpenStreamAck ---

type OpenStreamAckPayload struct {
	StreamID uint32
	Status   uint8
	Message  string
}

func EncodeOpenStreamAck(p *OpenStreamAckPayload) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.StreamID)
	buf.WriteByte(p.Status)
	encodeString(&buf, p.Message)
	return buf.Bytes()
}

func DecodeOpenStreamAck(data []byte) (*OpenStreamAckPayload, error) {
	r := bytes.NewReader(data)
	p := &OpenStreamAckPayload{}
	if err := binary.Read(r, binary.BigEndian, &p.StreamID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Status); err != nil {
		return nil, err
	}
	var err error
	p.Message, err = decodeString(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// --- StreamClose ---

type StreamClosePayload struct {
	Reason uint8
}

func EncodeStreamClose(p *StreamClosePayload) []byte {
	return []byte{p.Reason}
}

func DecodeStreamClose(data []byte) (*StreamClosePayload, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("stream close payload too short")
	}
	return &StreamClosePayload{Reason: data[0]}, nil
}

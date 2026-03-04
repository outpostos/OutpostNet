// Package protocol defines the Server-Sidecar binary wire protocol for OutpostNet.
//
// Wire format (length-prefixed, big-endian):
//
//	+----------+------+----------+-----------+
//	| Length(4) | Type | StreamID | Payload   |
//	| uint32    | uint8| uint32   | [N bytes] |
//	+----------+------+----------+-----------+
//	           |<----  Length bytes  -------->|
//
// Length = 1 (Type) + 4 (StreamID) + len(Payload)
package protocol

// Frame header sizes.
const (
	LengthFieldSize = 4
	TypeFieldSize   = 1
	StreamIDSize    = 4
	FrameHeaderSize = TypeFieldSize + StreamIDSize // 5 bytes inside length-delimited body
)

// MaxFramePayload is the maximum payload size (1 MB).
const MaxFramePayload = 1 << 20

// StreamIDControl is the reserved stream ID for control messages.
const StreamIDControl uint32 = 0

// MsgType defines the type of message carried in a Frame.
type MsgType uint8

const (
	// Control messages (StreamID must be 0).
	MsgTypeHandshake    MsgType = 0x01
	MsgTypeHandshakeAck MsgType = 0x02
	MsgTypeRegister     MsgType = 0x03
	MsgTypeRegisterAck  MsgType = 0x04
	MsgTypeConnect      MsgType = 0x05
	MsgTypeConnectAck   MsgType = 0x06
	MsgTypeHeartbeat    MsgType = 0x07
	MsgTypeHeartbeatAck MsgType = 0x08

	// Stream lifecycle (Server <-> exporting Sidecar).
	MsgTypeOpenStream    MsgType = 0x10
	MsgTypeOpenStreamAck MsgType = 0x11

	// Data plane (StreamID must be non-zero).
	MsgTypeData        MsgType = 0x20
	MsgTypeStreamClose MsgType = 0x21

	// Error.
	MsgTypeError MsgType = 0x30
)

// Frame is the on-wire unit of communication.
type Frame struct {
	Type     MsgType
	StreamID uint32
	Payload  []byte
}

// ServiceProtocol identifies the transport protocol for a service.
type ServiceProtocol uint8

const (
	ProtoTCP ServiceProtocol = 0
	ProtoUDP ServiceProtocol = 1
)

// Status codes used in Ack messages.
const (
	StatusOK               uint8 = 0
	StatusDuplicate        uint8 = 1
	StatusInvalid          uint8 = 2
	StatusNotFound         uint8 = 3
	StatusNoExporter       uint8 = 4
	StatusTimeout          uint8 = 5
	StatusConnectionFailed uint8 = 6
)

// StreamClose reasons.
const (
	CloseNormal  uint8 = 0
	CloseError   uint8 = 1
	CloseTimeout uint8 = 2
	CloseReset   uint8 = 3
)

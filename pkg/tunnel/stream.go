package tunnel

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ServiceHeader is sent at the start of each QUIC stream to identify the
// target service. The wire format is:
//
//	Standard header (bit 15 of NameLen = 0):
//	+──────────────+──────────────────+
//	│ NameLen (2B) │ ServiceName (var)│
//	+──────────────+──────────────────+
//
//	Relay header (bit 15 of NameLen = 1):
//	+──────────────+──────────────────+──────────────+
//	│ NameLen (2B) │ ServiceName (var)│ HopCount (1B)│
//	+──────────────+──────────────────+──────────────+
//
// After the header, the stream carries raw bidirectional data (L4 forwarding).

const maxServiceNameLen = 255

// relayFlag is set in the high bit of NameLen to indicate a relay header.
const relayFlag = uint16(1 << 15)

// StreamHeader is the parsed result of reading a stream header.
type StreamHeader struct {
	ServiceName string
	HopCount    uint8
	IsRelay     bool
}

// WriteServiceHeader writes a standard service name header to a QUIC stream.
func WriteServiceHeader(w io.Writer, serviceName string) error {
	if len(serviceName) == 0 {
		return fmt.Errorf("empty service name")
	}
	if len(serviceName) > maxServiceNameLen {
		return fmt.Errorf("service name too long: %d > %d", len(serviceName), maxServiceNameLen)
	}
	nameLen := uint16(len(serviceName))
	if err := binary.Write(w, binary.BigEndian, nameLen); err != nil {
		return fmt.Errorf("write name length: %w", err)
	}
	if _, err := io.WriteString(w, serviceName); err != nil {
		return fmt.Errorf("write service name: %w", err)
	}
	return nil
}

// WriteRelayHeader writes a relay-aware service header with hop count.
func WriteRelayHeader(w io.Writer, serviceName string, hopCount uint8) error {
	if len(serviceName) == 0 {
		return fmt.Errorf("empty service name")
	}
	if len(serviceName) > maxServiceNameLen {
		return fmt.Errorf("service name too long: %d > %d", len(serviceName), maxServiceNameLen)
	}
	nameLen := uint16(len(serviceName)) | relayFlag
	if err := binary.Write(w, binary.BigEndian, nameLen); err != nil {
		return fmt.Errorf("write name length: %w", err)
	}
	if _, err := io.WriteString(w, serviceName); err != nil {
		return fmt.Errorf("write service name: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, hopCount); err != nil {
		return fmt.Errorf("write hop count: %w", err)
	}
	return nil
}

// ReadServiceHeader reads the service name header from a QUIC stream.
// It handles both standard and relay headers transparently.
func ReadServiceHeader(r io.Reader) (string, error) {
	hdr, err := ReadStreamHeader(r)
	if err != nil {
		return "", err
	}
	return hdr.ServiceName, nil
}

// ReadStreamHeader reads either a standard or relay header from a QUIC stream,
// returning the full StreamHeader including hop count if present.
func ReadStreamHeader(r io.Reader) (StreamHeader, error) {
	var nameLen uint16
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return StreamHeader{}, fmt.Errorf("read name length: %w", err)
	}

	isRelay := nameLen&relayFlag != 0
	nameLen &= ^relayFlag

	if nameLen == 0 {
		return StreamHeader{}, fmt.Errorf("empty service name")
	}
	if nameLen > maxServiceNameLen {
		return StreamHeader{}, fmt.Errorf("service name too long: %d > %d", nameLen, maxServiceNameLen)
	}

	buf := make([]byte, nameLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return StreamHeader{}, fmt.Errorf("read service name: %w", err)
	}

	hdr := StreamHeader{
		ServiceName: string(buf),
		IsRelay:     isRelay,
	}

	if isRelay {
		if err := binary.Read(r, binary.BigEndian, &hdr.HopCount); err != nil {
			return StreamHeader{}, fmt.Errorf("read hop count: %w", err)
		}
	}

	return hdr, nil
}

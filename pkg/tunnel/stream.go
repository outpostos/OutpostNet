package tunnel

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ServiceHeader is sent at the start of each QUIC stream to identify the
// target service. The wire format is:
//
//	+──────────────+──────────────────+
//	│ NameLen (2B) │ ServiceName (var)│
//	+──────────────+──────────────────+
//
// After the header, the stream carries raw bidirectional data (L4 forwarding).

const maxServiceNameLen = 255

// WriteServiceHeader writes the service name header to a QUIC stream.
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

// ReadServiceHeader reads the service name header from a QUIC stream.
func ReadServiceHeader(r io.Reader) (string, error) {
	var nameLen uint16
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return "", fmt.Errorf("read name length: %w", err)
	}
	if nameLen == 0 {
		return "", fmt.Errorf("empty service name")
	}
	if nameLen > maxServiceNameLen {
		return "", fmt.Errorf("service name too long: %d > %d", nameLen, maxServiceNameLen)
	}
	buf := make([]byte, nameLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("read service name: %w", err)
	}
	return string(buf), nil
}

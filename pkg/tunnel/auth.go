package tunnel

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Auth response status bytes.
const (
	authOK   byte = 0
	authFail byte = 1
)

// maxTokenLen is the maximum allowed token length in bytes.
const maxTokenLen = 1024

// authHandshakeTimeout is the deadline for the auth stream exchange.
const authHandshakeTimeout = 10

// writeAuthToken writes the auth token to w using the wire format:
//
//	[2B token length (big-endian)][token bytes]
func writeAuthToken(w io.Writer, token string) error {
	if len(token) > maxTokenLen {
		return fmt.Errorf("auth token too long: %d > %d", len(token), maxTokenLen)
	}
	tokenLen := uint16(len(token))
	if err := binary.Write(w, binary.BigEndian, tokenLen); err != nil {
		return fmt.Errorf("write token length: %w", err)
	}
	if len(token) > 0 {
		if _, err := io.WriteString(w, token); err != nil {
			return fmt.Errorf("write token: %w", err)
		}
	}
	return nil
}

// readAuthToken reads the auth token from r.
func readAuthToken(r io.Reader) (string, error) {
	var tokenLen uint16
	if err := binary.Read(r, binary.BigEndian, &tokenLen); err != nil {
		return "", fmt.Errorf("read token length: %w", err)
	}
	if tokenLen > maxTokenLen {
		return "", fmt.Errorf("auth token too long: %d > %d", tokenLen, maxTokenLen)
	}
	if tokenLen == 0 {
		return "", nil
	}
	buf := make([]byte, tokenLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("read token: %w", err)
	}
	return string(buf), nil
}

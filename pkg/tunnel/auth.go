package tunnel

import (
	"encoding/binary"
	"errors"
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

// Peer declaration (ADR-0003 D5). After the token, a dialer that knows its
// node_id appends one declaration on the same auth stream:
//
//	[1B version = peerHelloV1][1B node_id length (1-255)][node_id bytes]
//
// The acceptor reads it only after answering authOK, so an old dialer (which
// sends nothing after the token and closes its side once it has the answer)
// is seen as "no declaration", and an old acceptor never reads the extra
// bytes. The node_id is self-reported under a shared token: it labels the
// connection for reverse streams, it does not authenticate the node.
const (
	peerHelloV1      byte = 1
	maxPeerNodeIDLen      = 255
)

// writePeerHello writes the dialer's node_id declaration.
func writePeerHello(w io.Writer, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("empty node_id")
	}
	if len(nodeID) > maxPeerNodeIDLen {
		return fmt.Errorf("node_id too long: %d > %d", len(nodeID), maxPeerNodeIDLen)
	}
	buf := make([]byte, 0, 2+len(nodeID))
	buf = append(buf, peerHelloV1, byte(len(nodeID)))
	buf = append(buf, nodeID...)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write peer declaration: %w", err)
	}
	return nil
}

// readPeerHello reads an optional node_id declaration. A clean end of stream
// before the first byte means the dialer sent none (older version) and
// returns "", nil.
func readPeerHello(r io.Reader) (string, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:1]); err != nil {
		if errors.Is(err, io.EOF) {
			return "", nil
		}
		return "", fmt.Errorf("read peer declaration: %w", err)
	}
	if hdr[0] != peerHelloV1 {
		return "", fmt.Errorf("unsupported peer declaration version %d", hdr[0])
	}
	if _, err := io.ReadFull(r, hdr[1:2]); err != nil {
		return "", fmt.Errorf("read peer declaration length: %w", err)
	}
	if hdr[1] == 0 {
		return "", fmt.Errorf("empty node_id in peer declaration")
	}
	id := make([]byte, hdr[1])
	if _, err := io.ReadFull(r, id); err != nil {
		return "", fmt.Errorf("read peer declaration node_id: %w", err)
	}
	return string(id), nil
}

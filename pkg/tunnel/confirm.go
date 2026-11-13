package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/quic-go/quic-go"
)

// Stream confirmation (OutpostOS ADR-0003 D3). A stream opened with
// WriteConfirmedServiceHeader asks the provider to answer one status byte
// after it has connected the service backend and before it forwards any
// data. An opener that waits for StreamAckOK before forwarding application
// bytes can therefore switch to another provider on any failure without
// ever resending application data.
//
// Wire: bit 14 of the standard header's NameLen. Older providers do not know
// the bit, read an over-long name and close the stream, which the opener
// sees as a failed confirmation.
const confirmFlag = uint16(1 << 14)

// Confirmation status bytes.
const (
	StreamAckOK         byte = 0
	StreamAckNoRoute    byte = 1
	StreamAckDialFailed byte = 2
)

var (
	// ErrStreamNoRoute: the provider exports no such service.
	ErrStreamNoRoute = errors.New("provider has no route for the service")
	// ErrStreamBackendUnavailable: the provider could not connect the
	// service's local backend.
	ErrStreamBackendUnavailable = errors.New("provider could not reach the service backend")
	// ErrStreamNotConfirmed: the stream ended or failed before a status byte
	// arrived (for example an older provider that does not confirm).
	ErrStreamNotConfirmed = errors.New("provider did not confirm the stream")
)

// WriteConfirmedServiceHeader writes a standard header that requests a
// confirmation byte.
func WriteConfirmedServiceHeader(w io.Writer, serviceName string) error {
	if len(serviceName) == 0 {
		return fmt.Errorf("empty service name")
	}
	if len(serviceName) > maxServiceNameLen {
		return fmt.Errorf("service name too long: %d > %d", len(serviceName), maxServiceNameLen)
	}
	buf := make([]byte, 2, 2+len(serviceName))
	binary.BigEndian.PutUint16(buf, uint16(len(serviceName))|confirmFlag)
	buf = append(buf, serviceName...)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write confirmed service header: %w", err)
	}
	return nil
}

// ReadStreamAck reads the provider's status byte.
func ReadStreamAck(r io.Reader) error {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return fmt.Errorf("%w: %v", ErrStreamNotConfirmed, err)
	}
	switch b[0] {
	case StreamAckOK:
		return nil
	case StreamAckNoRoute:
		return ErrStreamNoRoute
	case StreamAckDialFailed:
		return ErrStreamBackendUnavailable
	default:
		return fmt.Errorf("%w: unknown status %d", ErrStreamNotConfirmed, b[0])
	}
}

// OpenConfirmedServiceStream opens a stream with a confirmed header on the
// current connection. The caller reads the status with ReadStreamAck and
// must call StreamDone exactly once after closing the stream, as with
// OpenServiceStream.
func (mc *ManagedClient) OpenConfirmedServiceStream(ctx context.Context, serviceName string) (*quic.Stream, error) {
	mc.mu.RLock()
	conn := mc.conn
	mc.mu.RUnlock()
	if conn == nil {
		return nil, net.ErrClosed
	}
	if !mc.acquireStream() {
		return nil, net.ErrClosed
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		mc.StreamDone()
		return nil, err
	}
	if err := WriteConfirmedServiceHeader(stream, serviceName); err != nil {
		stream.CancelRead(0)
		stream.Close()
		mc.StreamDone()
		return nil, err
	}
	return stream, nil
}

// OpenConfirmedPeerStream is OpenPeerStream with a confirmed header.
func (s *Server) OpenConfirmedPeerStream(ctx context.Context, nodeID, serviceName string) (*quic.Stream, error) {
	s.peersMu.Lock()
	conn := s.peers[nodeID]
	s.peersMu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("peer %q: %w", nodeID, ErrNoPeerConn)
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream to accepted peer %q: %w", nodeID, err)
	}
	if err := WriteConfirmedServiceHeader(stream, serviceName); err != nil {
		stream.CancelRead(0)
		stream.Close()
		return nil, err
	}
	return stream, nil
}

// HasAcceptedPeer reports whether nodeID has a registered accepted
// connection on this server.
func (s *Server) HasAcceptedPeer(nodeID string) bool {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	return s.peers[nodeID] != nil
}

func writeStreamAck(w io.Writer, status byte) error {
	_, err := w.Write([]byte{status})
	return err
}

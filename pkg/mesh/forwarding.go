package mesh

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Forwarding wire protocol between servers within a cluster:
//
// Request:
//
//	+──────────────+──────────────────+
//	│ NameLen (2B) │ ServiceName (var)│  big-endian uint16
//	+──────────────+──────────────────+
//
// Response:
//
//	+──────────+──────────+──────────────────+
//	│ Status(1)│ MsgLen(2)│ Message (var)    │
//	+──────────+──────────+──────────────────+
//
// After Status=0 (OK), bidirectional raw data flows on the connection.

const (
	forwardStatusOK       uint8 = 0
	forwardStatusNotFound uint8 = 1
	forwardStatusError    uint8 = 2
)

var (
	ErrServiceNotFound   = errors.New("service not found in cluster")
	ErrNoHealthyEndpoint = errors.New("no healthy endpoint available")
	ErrForwardingFailed  = errors.New("forwarding to peer failed")
	ErrForwarderClosed   = errors.New("forwarder is closed")
)

// LocalServiceHandler provides access to locally exported services.
// The server implements this interface to allow the forwarder to connect
// to local services when handling incoming forwarded requests.
type LocalServiceHandler interface {
	ConnectLocalService(serviceName string) (io.ReadWriteCloser, error)
}

// LocalServiceHandlerFunc is an adapter to allow the use of ordinary
// functions as LocalServiceHandler.
type LocalServiceHandlerFunc func(serviceName string) (io.ReadWriteCloser, error)

func (f LocalServiceHandlerFunc) ConnectLocalService(serviceName string) (io.ReadWriteCloser, error) {
	return f(serviceName)
}

// ForwarderConfig configures the Forwarder.
type ForwarderConfig struct {
	// ListenAddr is the address the forwarding listener binds to.
	// If empty, the forwarder only initiates outbound forwarding.
	ListenAddr string

	// DialTimeout is the timeout for connecting to peer nodes.
	DialTimeout time.Duration

	// MaxRetries is the number of failover attempts on different nodes.
	MaxRetries int
}

// DefaultForwarderConfig returns a config with sensible defaults.
func DefaultForwarderConfig() ForwarderConfig {
	return ForwarderConfig{
		DialTimeout: 5 * time.Second,
		MaxRetries:  2,
	}
}

// ForwardingMetrics tracks forwarding performance counters.
type ForwardingMetrics struct {
	ForwardTotal     atomic.Int64
	ForwardErrors    atomic.Int64
	ForwardActive    atomic.Int64
	ForwardLatencyNs atomic.Int64 // cumulative latency in nanoseconds
	IncomingTotal    atomic.Int64
	IncomingErrors   atomic.Int64
	IncomingActive   atomic.Int64
}

// ConnHandler is called for connections that are not forwarding requests.
// It is used by the Manager to handle sync connections on the same listener.
type ConnHandler func(conn net.Conn)

// Forwarder handles cross-node service request forwarding within a cluster.
// It maintains a forwarding listener for incoming requests from peers,
// and provides a Forward method for outbound forwarding to peer nodes.
type Forwarder struct {
	config  ForwarderConfig
	logger  *slog.Logger
	cluster *ClusterState
	handler LocalServiceHandler
	Metrics ForwardingMetrics

	// SyncHandler is called when a connection starts with the sync magic byte.
	// If nil, sync connections are dropped.
	SyncHandler ConnHandler

	// FederationHandler is called when a connection starts with the federation
	// magic byte (0xBB). If nil, federation connections are dropped.
	FederationHandler ConnHandler

	// ElectionHandler is called when a connection starts with the election
	// magic byte (0xCC). If nil, election connections are dropped.
	ElectionHandler ConnHandler

	// Round-robin index for load balancing across endpoints.
	rrIndex atomic.Uint64

	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewForwarder creates a new Forwarder.
func NewForwarder(config ForwarderConfig, cluster *ClusterState, handler LocalServiceHandler, logger *slog.Logger) *Forwarder {
	if logger == nil {
		logger = slog.Default()
	}
	return &Forwarder{
		config:  config,
		logger:  logger,
		cluster: cluster,
		handler: handler,
		done:    make(chan struct{}),
	}
}

// ListenAndServe starts the forwarding listener that accepts incoming
// forwarding requests from peer servers. It blocks until the listener
// is closed or an error occurs.
func (f *Forwarder) ListenAndServe() error {
	if f.config.ListenAddr == "" {
		return fmt.Errorf("no listen address configured")
	}

	ln, err := net.Listen("tcp", f.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	f.listener = ln

	f.logger.Info("forwarding listener started", "addr", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-f.done:
				return nil
			default:
				f.logger.Error("accept error", "error", err)
				continue
			}
		}
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			f.handleIncoming(conn)
		}()
	}
}

// Addr returns the listener's address, or nil if not listening.
func (f *Forwarder) Addr() net.Addr {
	if f.listener == nil {
		return nil
	}
	return f.listener.Addr()
}

// handleIncoming processes an incoming connection. It peeks the first
// two bytes to determine if this is a forwarding request or a sync
// connection (distinguished by the sync magic byte 0xAA as the high
// byte of the service name length field, which is invalid for forwarding
// since max name length is 4096).
func (f *Forwarder) handleIncoming(conn net.Conn) {
	defer conn.Close()

	// Peek the first byte to check for sync magic.
	var peek [1]byte
	if _, err := io.ReadFull(conn, peek[:]); err != nil {
		f.logger.Error("peek first byte", "error", err)
		return
	}

	if peek[0] == syncMagic {
		if f.SyncHandler != nil {
			f.SyncHandler(conn)
		}
		return
	}

	if peek[0] == federationMagic {
		if f.FederationHandler != nil {
			f.FederationHandler(conn)
		}
		return
	}

	if peek[0] == electionMagic {
		if f.ElectionHandler != nil {
			f.ElectionHandler(conn)
		}
		return
	}

	// It's a forwarding request. The first byte is the high byte of
	// the name length. Read the second byte and reassemble.
	f.Metrics.IncomingTotal.Add(1)
	f.Metrics.IncomingActive.Add(1)
	defer f.Metrics.IncomingActive.Add(-1)

	var peek2 [1]byte
	if _, err := io.ReadFull(conn, peek2[:]); err != nil {
		f.logger.Error("read name length second byte", "error", err)
		f.Metrics.IncomingErrors.Add(1)
		return
	}

	nameLen := binary.BigEndian.Uint16([]byte{peek[0], peek2[0]})
	if nameLen == 0 || nameLen > 4096 {
		f.logger.Error("invalid service name length", "len", nameLen)
		f.Metrics.IncomingErrors.Add(1)
		return
	}

	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		f.logger.Error("read service name", "error", err)
		f.Metrics.IncomingErrors.Add(1)
		return
	}
	serviceName := string(nameBuf)

	f.logger.Debug("incoming forward request", "service", serviceName, "remote", conn.RemoteAddr())

	if f.handler == nil {
		writeForwardResponse(conn, forwardStatusError, "no local handler")
		f.Metrics.IncomingErrors.Add(1)
		return
	}

	local, err := f.handler.ConnectLocalService(serviceName)
	if err != nil {
		f.logger.Error("connect local service", "service", serviceName, "error", err)
		writeForwardResponse(conn, forwardStatusNotFound, err.Error())
		f.Metrics.IncomingErrors.Add(1)
		return
	}
	defer local.Close()

	if err := writeForwardResponse(conn, forwardStatusOK, ""); err != nil {
		f.logger.Error("write forward response", "error", err)
		f.Metrics.IncomingErrors.Add(1)
		return
	}

	biCopy(conn, local)
}

// Forward connects to a peer node that has the requested service and returns
// a ReadWriteCloser for bidirectional data relay. It implements round-robin
// load balancing with failover: if the selected endpoint is unreachable,
// subsequent endpoints are tried up to MaxRetries times.
func (f *Forwarder) Forward(ctx context.Context, serviceName string) (io.ReadWriteCloser, error) {
	select {
	case <-f.done:
		return nil, ErrForwarderClosed
	default:
	}

	start := time.Now()
	f.Metrics.ForwardTotal.Add(1)

	endpoints := f.cluster.LookupRemote(serviceName)
	if len(endpoints) == 0 {
		f.Metrics.ForwardErrors.Add(1)
		return nil, ErrServiceNotFound
	}

	maxAttempts := min(len(endpoints), f.config.MaxRetries+1)
	startIdx := int(f.rrIndex.Add(1)-1) % len(endpoints)

	var lastErr error
	for i := range maxAttempts {
		idx := (startIdx + i) % len(endpoints)
		ep := endpoints[idx]

		node, ok := f.cluster.GetNode(ep.NodeID)
		if !ok {
			continue
		}

		conn, err := f.dialAndForward(ctx, node.Address, serviceName)
		if err != nil {
			lastErr = err
			f.logger.Warn("forward attempt failed",
				"service", serviceName,
				"node", ep.NodeID,
				"addr", node.Address,
				"attempt", i+1,
				"error", err,
			)
			continue
		}

		latency := time.Since(start)
		f.Metrics.ForwardLatencyNs.Add(latency.Nanoseconds())
		f.Metrics.ForwardActive.Add(1)

		f.logger.Debug("forward established",
			"service", serviceName,
			"node", ep.NodeID,
			"latency", latency,
		)

		return &trackedConn{
			ReadWriteCloser: conn,
			onClose: func() {
				f.Metrics.ForwardActive.Add(-1)
			},
		}, nil
	}

	f.Metrics.ForwardErrors.Add(1)
	return nil, fmt.Errorf("%w: %v", ErrForwardingFailed, lastErr)
}

// dialAndForward connects to a peer and sends the forwarding request.
func (f *Forwarder) dialAndForward(ctx context.Context, addr, serviceName string) (io.ReadWriteCloser, error) {
	dialer := net.Dialer{Timeout: f.config.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	if err := writeForwardHeader(conn, serviceName); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}

	status, msg, err := readForwardResponse(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read response: %w", err)
	}
	if status != forwardStatusOK {
		conn.Close()
		return nil, fmt.Errorf("peer rejected: status=%d msg=%s", status, msg)
	}

	return conn, nil
}

// Close shuts down the forwarder gracefully.
func (f *Forwarder) Close() error {
	close(f.done)
	if f.listener != nil {
		f.listener.Close()
	}
	f.wg.Wait()
	return nil
}

// --- Wire protocol helpers ---

func writeForwardHeader(w io.Writer, serviceName string) error {
	name := []byte(serviceName)
	if len(name) > 0xFFFF {
		return fmt.Errorf("service name too long: %d bytes", len(name))
	}
	buf := make([]byte, 2+len(name))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(name)))
	copy(buf[2:], name)
	_, err := w.Write(buf)
	return err
}

func readForwardHeader(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", fmt.Errorf("read name length: %w", err)
	}
	nameLen := binary.BigEndian.Uint16(lenBuf[:])
	if nameLen == 0 || nameLen > 4096 {
		return "", fmt.Errorf("invalid service name length: %d", nameLen)
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBuf); err != nil {
		return "", fmt.Errorf("read service name: %w", err)
	}
	return string(nameBuf), nil
}

func writeForwardResponse(w io.Writer, status uint8, msg string) error {
	msgBytes := []byte(msg)
	if len(msgBytes) > 0xFFFF {
		msgBytes = msgBytes[:0xFFFF]
	}
	buf := make([]byte, 3+len(msgBytes))
	buf[0] = status
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(msgBytes)))
	copy(buf[3:], msgBytes)
	_, err := w.Write(buf)
	return err
}

func readForwardResponse(r io.Reader) (status uint8, msg string, err error) {
	var hdr [3]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, "", fmt.Errorf("read response header: %w", err)
	}
	status = hdr[0]
	msgLen := binary.BigEndian.Uint16(hdr[1:3])
	if msgLen > 0 {
		msgBuf := make([]byte, msgLen)
		if _, err = io.ReadFull(r, msgBuf); err != nil {
			return 0, "", fmt.Errorf("read response message: %w", err)
		}
		msg = string(msgBuf)
	}
	return status, msg, nil
}

// trackedConn wraps an io.ReadWriteCloser to call onClose exactly once.
type trackedConn struct {
	io.ReadWriteCloser
	once    sync.Once
	onClose func()
}

func (tc *trackedConn) Close() error {
	tc.once.Do(tc.onClose)
	return tc.ReadWriteCloser.Close()
}

// biCopy copies data bidirectionally between a and b until one side
// closes or errors.
func biCopy(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(b, a)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(a, b)
		done <- struct{}{}
	}()
	<-done
}

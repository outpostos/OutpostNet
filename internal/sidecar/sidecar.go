// Package sidecar implements the OutpostNet pod-level sidecar.
// It connects to the node-level Server via Unix domain socket,
// registers exported services, and handles imported service proxying.
package sidecar

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
)

// ExportEntry describes a service this sidecar exports.
type ExportEntry struct {
	Name     string
	Protocol protocol.ServiceProtocol
	Port     uint16
}

// ImportEntry describes a service this sidecar imports.
type ImportEntry struct {
	Name       string
	Protocol   protocol.ServiceProtocol
	ListenPort uint16
}

// Config holds sidecar configuration.
type Config struct {
	ServerSocket      string
	SidecarID         string
	HeartbeatInterval time.Duration
	Exports           []ExportEntry
	Imports           []ImportEntry
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ServerSocket:      "/var/run/outpostnet/server.sock",
		HeartbeatInterval: 10 * time.Second,
	}
}

// Sidecar manages the connection to the Server and proxies services.
type Sidecar struct {
	config Config
	codec  *protocol.Codec

	mu      sync.RWMutex
	streams map[uint32]net.Conn // streamID -> local TCP/UDP connection

	// Connect serialization: only one Connect request in flight at a time.
	connectMu sync.Mutex
	connectCh chan *protocol.ConnectAckPayload

	importListeners []net.Listener

	done chan struct{}
}

// New creates a new Sidecar.
func New(config Config) *Sidecar {
	return &Sidecar{
		config:    config,
		streams:   make(map[uint32]net.Conn),
		connectCh: make(chan *protocol.ConnectAckPayload, 1),
		done:      make(chan struct{}),
	}
}

// Run connects to the Server, registers services, and starts proxying. Blocks until Stop.
func (s *Sidecar) Run() error {
	conn, err := net.Dial("unix", s.config.ServerSocket)
	if err != nil {
		return fmt.Errorf("connect to server: %w", err)
	}
	s.codec = protocol.NewCodec(conn)
	defer s.codec.Close()

	log.Printf("[sidecar] connected to server at %s", s.config.ServerSocket)

	// Step 1: Handshake.
	if err := s.handshake(); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}

	// Step 2: Register exported services.
	for _, exp := range s.config.Exports {
		if err := s.registerExport(exp); err != nil {
			return fmt.Errorf("register export %s: %w", exp.Name, err)
		}
	}

	// Step 3: Start import listeners.
	for _, imp := range s.config.Imports {
		if err := s.startImportListener(imp); err != nil {
			return fmt.Errorf("start import listener %s: %w", imp.Name, err)
		}
	}

	// Step 4: Start heartbeat.
	go s.heartbeatLoop()

	// Step 5: Message loop (blocks).
	s.messageLoop()

	return nil
}

// Stop gracefully shuts down the sidecar.
func (s *Sidecar) Stop() {
	select {
	case <-s.done:
		return // already stopped
	default:
		close(s.done)
	}

	for _, ln := range s.importListeners {
		ln.Close()
	}

	s.mu.Lock()
	for sid, conn := range s.streams {
		conn.Close()
		delete(s.streams, sid)
	}
	s.mu.Unlock()

	if s.codec != nil {
		s.codec.Close()
	}
}

func (s *Sidecar) handshake() error {
	if err := s.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshake,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeHandshake(&protocol.HandshakePayload{
			ProtocolVersion: protocol.ProtocolVersionV1,
			SidecarID:       s.config.SidecarID,
		}),
	}); err != nil {
		return err
	}

	frame, err := s.codec.ReadFrame()
	if err != nil {
		return err
	}
	if frame.Type != protocol.MsgTypeHandshakeAck {
		return fmt.Errorf("expected handshake_ack, got 0x%02x", frame.Type)
	}

	ack, err := protocol.DecodeHandshakeAck(frame.Payload)
	if err != nil {
		return err
	}
	if ack.Status != protocol.StatusOK {
		return fmt.Errorf("handshake rejected: %s", ack.Message)
	}

	log.Printf("[sidecar] handshake OK")
	return nil
}

func (s *Sidecar) registerExport(exp ExportEntry) error {
	if err := s.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegister,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeRegister(&protocol.RegisterPayload{
			ServiceName: exp.Name,
			Protocol:    exp.Protocol,
			LocalPort:   exp.Port,
		}),
	}); err != nil {
		return err
	}

	frame, err := s.codec.ReadFrame()
	if err != nil {
		return err
	}
	if frame.Type != protocol.MsgTypeRegisterAck {
		return fmt.Errorf("expected register_ack, got 0x%02x", frame.Type)
	}

	ack, err := protocol.DecodeRegisterAck(frame.Payload)
	if err != nil {
		return err
	}
	if ack.Status != protocol.StatusOK {
		return fmt.Errorf("register rejected for %s: %s", ack.ServiceName, ack.Message)
	}

	log.Printf("[sidecar] registered export: %s (port=%d, proto=%d)", exp.Name, exp.Port, exp.Protocol)
	return nil
}

// --- Import side (S3) ---

func (s *Sidecar) startImportListener(imp ImportEntry) error {
	if imp.Protocol != protocol.ProtoTCP {
		// UDP import listeners will be added in S4.
		return nil
	}

	addr := fmt.Sprintf("127.0.0.1:%d", imp.ListenPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	s.importListeners = append(s.importListeners, ln)
	log.Printf("[sidecar] import listener: %s -> %s", imp.Name, addr)

	go s.acceptImportConnections(ln, imp)
	return nil
}

func (s *Sidecar) acceptImportConnections(ln net.Listener, imp ImportEntry) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
			default:
				log.Printf("[sidecar] accept import %s: %v", imp.Name, err)
			}
			return
		}
		go s.handleImportConnection(conn, imp)
	}
}

func (s *Sidecar) handleImportConnection(appConn net.Conn, imp ImportEntry) {
	// Send Connect to Server and get a StreamID.
	streamID, err := s.requestConnect(imp.Name, imp.Protocol)
	if err != nil {
		log.Printf("[sidecar] connect request for %s: %v", imp.Name, err)
		appConn.Close()
		return
	}

	// Register stream for data relay.
	s.mu.Lock()
	s.streams[streamID] = appConn
	s.mu.Unlock()

	log.Printf("[sidecar] import stream %d: app -> %s", streamID, imp.Name)

	// Relay local app data to server (blocks until connection closes).
	s.relayLocalToServer(streamID, appConn)
}

// requestConnect sends a Connect message and waits for ConnectAck.
// Uses a mutex to serialize concurrent Connect requests.
func (s *Sidecar) requestConnect(serviceName string, proto protocol.ServiceProtocol) (uint32, error) {
	s.connectMu.Lock()
	defer s.connectMu.Unlock()

	if err := s.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnect,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnect(&protocol.ConnectPayload{
			ServiceName: serviceName,
			Protocol:    proto,
		}),
	}); err != nil {
		return 0, err
	}

	// Wait for ConnectAck from messageLoop.
	select {
	case ack := <-s.connectCh:
		if ack.Status != protocol.StatusOK {
			return 0, fmt.Errorf("connect rejected: %s", ack.Message)
		}
		return ack.StreamID, nil
	case <-s.done:
		return 0, fmt.Errorf("sidecar shutting down")
	case <-time.After(10 * time.Second):
		return 0, fmt.Errorf("connect timeout")
	}
}

// --- Heartbeat ---

func (s *Sidecar) heartbeatLoop() {
	ticker := time.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			ts := uint64(time.Now().UnixMilli())
			err := s.codec.WriteFrame(&protocol.Frame{
				Type:     protocol.MsgTypeHeartbeat,
				StreamID: protocol.StreamIDControl,
				Payload:  protocol.EncodeHeartbeat(&protocol.HeartbeatPayload{Timestamp: ts}),
			})
			if err != nil {
				log.Printf("[sidecar] heartbeat write error: %v", err)
				return
			}
		}
	}
}

// --- Message loop ---

func (s *Sidecar) messageLoop() {
	for {
		frame, err := s.codec.ReadFrame()
		if err != nil {
			select {
			case <-s.done:
			default:
				log.Printf("[sidecar] read error: %v", err)
			}
			return
		}

		switch frame.Type {
		case protocol.MsgTypeHeartbeatAck:
			// Heartbeat response received.
		case protocol.MsgTypeOpenStream:
			s.handleOpenStream(frame)
		case protocol.MsgTypeData:
			s.handleData(frame)
		case protocol.MsgTypeStreamClose:
			s.handleStreamClose(frame)
		case protocol.MsgTypeConnectAck:
			s.handleConnectAck(frame)
		case protocol.MsgTypeRegisterAck:
			// Late ack, already handled during registration.
		default:
			log.Printf("[sidecar] unknown message type 0x%02x", frame.Type)
		}
	}
}

func (s *Sidecar) handleConnectAck(frame *protocol.Frame) {
	ack, err := protocol.DecodeConnectAck(frame.Payload)
	if err != nil {
		log.Printf("[sidecar] decode connect_ack: %v", err)
		return
	}
	// Deliver to the waiting requestConnect goroutine.
	select {
	case s.connectCh <- ack:
	default:
		log.Printf("[sidecar] connect_ack dropped (no waiter): service=%s", ack.ServiceName)
	}
}

// --- Export side (OpenStream handling) ---

func (s *Sidecar) handleOpenStream(frame *protocol.Frame) {
	os, err := protocol.DecodeOpenStream(frame.Payload)
	if err != nil {
		log.Printf("[sidecar] decode open_stream: %v", err)
		return
	}

	// Find the matching export to get the local port.
	var localPort uint16
	for _, exp := range s.config.Exports {
		if exp.Name == os.ServiceName {
			localPort = exp.Port
			break
		}
	}
	if localPort == 0 {
		s.codec.WriteFrame(&protocol.Frame{
			Type:     protocol.MsgTypeOpenStreamAck,
			StreamID: protocol.StreamIDControl,
			Payload: protocol.EncodeOpenStreamAck(&protocol.OpenStreamAckPayload{
				StreamID: os.StreamID,
				Status:   protocol.StatusNotFound,
				Message:  "service not exported locally",
			}),
		})
		return
	}

	// Connect to local app.
	addr := fmt.Sprintf("127.0.0.1:%d", localPort)
	localConn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		log.Printf("[sidecar] connect to local %s: %v", addr, err)
		s.codec.WriteFrame(&protocol.Frame{
			Type:     protocol.MsgTypeOpenStreamAck,
			StreamID: protocol.StreamIDControl,
			Payload: protocol.EncodeOpenStreamAck(&protocol.OpenStreamAckPayload{
				StreamID: os.StreamID,
				Status:   protocol.StatusConnectionFailed,
				Message:  err.Error(),
			}),
		})
		return
	}

	s.mu.Lock()
	s.streams[os.StreamID] = localConn
	s.mu.Unlock()

	log.Printf("[sidecar] opened stream %d -> local %s (service=%s)", os.StreamID, addr, os.ServiceName)

	s.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeOpenStreamAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeOpenStreamAck(&protocol.OpenStreamAckPayload{
			StreamID: os.StreamID,
			Status:   protocol.StatusOK,
		}),
	})

	// Start reading from local conn and sending to server.
	go s.relayLocalToServer(os.StreamID, localConn)
}

// --- Data handling (shared by import and export sides) ---

func (s *Sidecar) handleData(frame *protocol.Frame) {
	s.mu.RLock()
	localConn, ok := s.streams[frame.StreamID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	if _, err := localConn.Write(frame.Payload); err != nil {
		log.Printf("[sidecar] write to local stream %d: %v", frame.StreamID, err)
		s.closeStream(frame.StreamID, protocol.CloseError)
	}
}

func (s *Sidecar) handleStreamClose(frame *protocol.Frame) {
	s.mu.Lock()
	localConn, ok := s.streams[frame.StreamID]
	if ok {
		delete(s.streams, frame.StreamID)
	}
	s.mu.Unlock()

	if ok {
		localConn.Close()
		log.Printf("[sidecar] stream %d closed by remote", frame.StreamID)
	}
}

func (s *Sidecar) relayLocalToServer(streamID uint32, localConn net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := localConn.Read(buf)
		if n > 0 {
			writeErr := s.codec.WriteFrame(&protocol.Frame{
				Type:     protocol.MsgTypeData,
				StreamID: streamID,
				Payload:  buf[:n],
			})
			if writeErr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	s.closeStream(streamID, protocol.CloseNormal)
}

func (s *Sidecar) closeStream(streamID uint32, reason uint8) {
	s.mu.Lock()
	localConn, ok := s.streams[streamID]
	if ok {
		delete(s.streams, streamID)
	}
	s.mu.Unlock()

	if ok {
		localConn.Close()
	}

	s.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeStreamClose,
		StreamID: streamID,
		Payload:  protocol.EncodeStreamClose(&protocol.StreamClosePayload{Reason: reason}),
	})
}

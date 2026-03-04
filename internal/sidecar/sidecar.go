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
	streams map[uint32]net.Conn // streamID -> local TCP connection (for exports)

	done chan struct{}
}

// New creates a new Sidecar.
func New(config Config) *Sidecar {
	return &Sidecar{
		config:  config,
		streams: make(map[uint32]net.Conn),
		done:    make(chan struct{}),
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

	// Step 3: Start heartbeat.
	go s.heartbeatLoop()

	// Step 4: Message loop.
	s.messageLoop()

	return nil
}

// Stop gracefully shuts down the sidecar.
func (s *Sidecar) Stop() {
	close(s.done)
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
			// Heartbeat response received, nothing to do.
		case protocol.MsgTypeOpenStream:
			s.handleOpenStream(frame)
		case protocol.MsgTypeData:
			s.handleData(frame)
		case protocol.MsgTypeStreamClose:
			s.handleStreamClose(frame)
		case protocol.MsgTypeConnectAck:
			// Handled inline by import listeners (S3).
		case protocol.MsgTypeRegisterAck:
			// Late ack, already handled during registration.
		default:
			log.Printf("[sidecar] unknown message type 0x%02x", frame.Type)
		}
	}
}

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

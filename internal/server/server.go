// Package server implements the OutpostNet node-level server.
// It listens on a Unix domain socket, accepts sidecar connections,
// manages service registration, and routes data between sidecars.
package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
)

// Config holds server configuration.
type Config struct {
	SocketPath       string
	HeartbeatTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		SocketPath:       "/var/run/outpostnet/server.sock",
		HeartbeatTimeout: 30 * time.Second,
	}
}

// Server manages sidecar connections and service routing.
type Server struct {
	config   Config
	listener net.Listener
	streams  *protocol.StreamAllocator

	mu       sync.RWMutex
	sidecars map[string]*sidecarConn     // sidecarID -> conn
	services map[string]*exportedService // serviceName -> export info
	routes   map[uint32]*streamRoute     // streamID -> route

	done chan struct{}
}

type sidecarConn struct {
	id       string
	codec    *protocol.Codec
	lastPing time.Time
	mu       sync.Mutex
}

type exportedService struct {
	sidecarID string
	protocol  protocol.ServiceProtocol
	localPort uint16
}

type streamRoute struct {
	importerID string
	exporterID string
	service    string
}

// New creates a new Server.
func New(config Config) *Server {
	return &Server{
		config:   config,
		streams:  protocol.NewStreamAllocator(),
		sidecars: make(map[string]*sidecarConn),
		services: make(map[string]*exportedService),
		routes:   make(map[uint32]*streamRoute),
		done:     make(chan struct{}),
	}
}

// Run starts the server. Blocks until Stop is called or an error occurs.
func (s *Server) Run() error {
	if err := s.setupSocket(); err != nil {
		return fmt.Errorf("setup socket: %w", err)
	}
	defer s.listener.Close()

	log.Printf("[server] listening on %s", s.config.SocketPath)

	go s.heartbeatMonitor()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
				log.Printf("[server] accept error: %v", err)
				continue
			}
		}
		go s.handleConnection(conn)
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	close(s.done)
	if s.listener != nil {
		s.listener.Close()
	}
	os.Remove(s.config.SocketPath)
}

func (s *Server) setupSocket() error {
	dir := filepath.Dir(s.config.SocketPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	// Remove stale socket file.
	os.Remove(s.config.SocketPath)

	ln, err := net.Listen("unix", s.config.SocketPath)
	if err != nil {
		return err
	}
	// Make socket accessible to other containers sharing the volume.
	os.Chmod(s.config.SocketPath, 0666)
	s.listener = ln
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	codec := protocol.NewCodec(conn)
	defer codec.Close()

	// Step 1: Wait for Handshake.
	frame, err := codec.ReadFrame()
	if err != nil {
		log.Printf("[server] handshake read error: %v", err)
		return
	}
	if frame.Type != protocol.MsgTypeHandshake {
		log.Printf("[server] expected handshake, got 0x%02x", frame.Type)
		return
	}
	hs, err := protocol.DecodeHandshake(frame.Payload)
	if err != nil {
		log.Printf("[server] decode handshake: %v", err)
		return
	}
	if hs.ProtocolVersion != protocol.ProtocolVersionV1 {
		codec.WriteFrame(&protocol.Frame{
			Type:     protocol.MsgTypeHandshakeAck,
			StreamID: protocol.StreamIDControl,
			Payload:  protocol.EncodeHandshakeAck(&protocol.HandshakeAckPayload{Status: protocol.StatusInvalid, Message: "unsupported version"}),
		})
		return
	}

	sc := &sidecarConn{
		id:       hs.SidecarID,
		codec:    codec,
		lastPing: time.Now(),
	}

	s.mu.Lock()
	s.sidecars[hs.SidecarID] = sc
	s.mu.Unlock()

	log.Printf("[server] sidecar connected: %s", hs.SidecarID)

	// Send HandshakeAck.
	if err := codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHandshakeAck,
		StreamID: protocol.StreamIDControl,
		Payload:  protocol.EncodeHandshakeAck(&protocol.HandshakeAckPayload{Status: protocol.StatusOK}),
	}); err != nil {
		log.Printf("[server] write handshake ack: %v", err)
		s.removeSidecar(hs.SidecarID)
		return
	}

	// Step 2: Message loop.
	s.messageLoop(sc)

	// Cleanup on disconnect.
	log.Printf("[server] sidecar disconnected: %s", sc.id)
	s.removeSidecar(sc.id)
}

func (s *Server) messageLoop(sc *sidecarConn) {
	for {
		frame, err := sc.codec.ReadFrame()
		if err != nil {
			select {
			case <-s.done:
			default:
				log.Printf("[server] read from %s: %v", sc.id, err)
			}
			return
		}

		switch frame.Type {
		case protocol.MsgTypeRegister:
			s.handleRegister(sc, frame)
		case protocol.MsgTypeHeartbeat:
			s.handleHeartbeat(sc, frame)
		case protocol.MsgTypeConnect:
			s.handleConnect(sc, frame)
		case protocol.MsgTypeOpenStreamAck:
			s.handleOpenStreamAck(sc, frame)
		case protocol.MsgTypeData:
			s.handleData(sc, frame)
		case protocol.MsgTypeStreamClose:
			s.handleStreamClose(sc, frame)
		default:
			log.Printf("[server] unknown message type 0x%02x from %s", frame.Type, sc.id)
		}
	}
}

func (s *Server) handleRegister(sc *sidecarConn, frame *protocol.Frame) {
	reg, err := protocol.DecodeRegister(frame.Payload)
	if err != nil {
		log.Printf("[server] decode register from %s: %v", sc.id, err)
		return
	}

	s.mu.Lock()
	status := protocol.StatusOK
	msg := ""
	if existing, ok := s.services[reg.ServiceName]; ok && existing.sidecarID != sc.id {
		status = protocol.StatusDuplicate
		msg = fmt.Sprintf("already exported by %s", existing.sidecarID)
	} else {
		s.services[reg.ServiceName] = &exportedService{
			sidecarID: sc.id,
			protocol:  reg.Protocol,
			localPort: reg.LocalPort,
		}
	}
	s.mu.Unlock()

	log.Printf("[server] register service=%s sidecar=%s status=%d", reg.ServiceName, sc.id, status)

	sc.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeRegisterAck,
		StreamID: protocol.StreamIDControl,
		Payload:  protocol.EncodeRegisterAck(&protocol.RegisterAckPayload{ServiceName: reg.ServiceName, Status: status, Message: msg}),
	})
}

func (s *Server) handleHeartbeat(sc *sidecarConn, frame *protocol.Frame) {
	sc.mu.Lock()
	sc.lastPing = time.Now()
	sc.mu.Unlock()

	// Echo back as HeartbeatAck with same payload.
	sc.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeHeartbeatAck,
		StreamID: protocol.StreamIDControl,
		Payload:  frame.Payload,
	})
}

func (s *Server) handleConnect(sc *sidecarConn, frame *protocol.Frame) {
	conn, err := protocol.DecodeConnect(frame.Payload)
	if err != nil {
		log.Printf("[server] decode connect from %s: %v", sc.id, err)
		return
	}

	s.mu.RLock()
	svc, ok := s.services[conn.ServiceName]
	s.mu.RUnlock()

	if !ok {
		sc.codec.WriteFrame(&protocol.Frame{
			Type:     protocol.MsgTypeConnectAck,
			StreamID: protocol.StreamIDControl,
			Payload: protocol.EncodeConnectAck(&protocol.ConnectAckPayload{
				ServiceName: conn.ServiceName,
				Status:      protocol.StatusNotFound,
				Message:     "service not registered",
			}),
		})
		return
	}

	streamID := s.streams.Allocate()

	s.mu.Lock()
	s.routes[streamID] = &streamRoute{
		importerID: sc.id,
		exporterID: svc.sidecarID,
		service:    conn.ServiceName,
	}
	s.mu.Unlock()

	// Tell exporter to open a local connection.
	s.mu.RLock()
	exporter, ok := s.sidecars[svc.sidecarID]
	s.mu.RUnlock()

	if !ok {
		s.mu.Lock()
		delete(s.routes, streamID)
		s.mu.Unlock()
		sc.codec.WriteFrame(&protocol.Frame{
			Type:     protocol.MsgTypeConnectAck,
			StreamID: protocol.StreamIDControl,
			Payload: protocol.EncodeConnectAck(&protocol.ConnectAckPayload{
				ServiceName: conn.ServiceName,
				Status:      protocol.StatusNoExporter,
				Message:     "exporter disconnected",
			}),
		})
		return
	}

	exporter.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeOpenStream,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeOpenStream(&protocol.OpenStreamPayload{
			StreamID:    streamID,
			ServiceName: conn.ServiceName,
			Protocol:    svc.protocol,
		}),
	})

	// For now, send ConnectAck immediately. In production we'd wait for OpenStreamAck.
	sc.codec.WriteFrame(&protocol.Frame{
		Type:     protocol.MsgTypeConnectAck,
		StreamID: protocol.StreamIDControl,
		Payload: protocol.EncodeConnectAck(&protocol.ConnectAckPayload{
			ServiceName: conn.ServiceName,
			StreamID:    streamID,
			Status:      protocol.StatusOK,
		}),
	})

	log.Printf("[server] stream %d: %s -> %s (service=%s)", streamID, sc.id, svc.sidecarID, conn.ServiceName)
}

func (s *Server) handleOpenStreamAck(sc *sidecarConn, frame *protocol.Frame) {
	ack, err := protocol.DecodeOpenStreamAck(frame.Payload)
	if err != nil {
		log.Printf("[server] decode open_stream_ack from %s: %v", sc.id, err)
		return
	}
	if ack.Status != protocol.StatusOK {
		log.Printf("[server] open_stream_ack stream=%d status=%d: %s", ack.StreamID, ack.Status, ack.Message)
	}
}

func (s *Server) handleData(sc *sidecarConn, frame *protocol.Frame) {
	s.mu.RLock()
	route, ok := s.routes[frame.StreamID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	// Determine the other side.
	targetID := route.exporterID
	if sc.id == route.exporterID {
		targetID = route.importerID
	}

	s.mu.RLock()
	target, ok := s.sidecars[targetID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	target.codec.WriteFrame(frame)
}

func (s *Server) handleStreamClose(sc *sidecarConn, frame *protocol.Frame) {
	s.mu.RLock()
	route, ok := s.routes[frame.StreamID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	// Forward close to the other side.
	targetID := route.exporterID
	if sc.id == route.exporterID {
		targetID = route.importerID
	}

	s.mu.RLock()
	target, ok := s.sidecars[targetID]
	s.mu.RUnlock()

	if ok {
		target.codec.WriteFrame(frame)
	}

	s.mu.Lock()
	delete(s.routes, frame.StreamID)
	s.mu.Unlock()

	log.Printf("[server] stream %d closed (service=%s)", frame.StreamID, route.service)
}

func (s *Server) removeSidecar(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sidecars, id)

	// Remove exported services from this sidecar.
	for name, svc := range s.services {
		if svc.sidecarID == id {
			delete(s.services, name)
			log.Printf("[server] unregistered service %s (sidecar %s gone)", name, id)
		}
	}

	// Clean up routes involving this sidecar.
	for sid, route := range s.routes {
		if route.importerID == id || route.exporterID == id {
			delete(s.routes, sid)
		}
	}
}

func (s *Server) heartbeatMonitor() {
	ticker := time.NewTicker(s.config.HeartbeatTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.checkHeartbeats()
		}
	}
}

func (s *Server) checkHeartbeats() {
	now := time.Now()
	var stale []string

	s.mu.RLock()
	for id, sc := range s.sidecars {
		sc.mu.Lock()
		if now.Sub(sc.lastPing) > s.config.HeartbeatTimeout {
			stale = append(stale, id)
		}
		sc.mu.Unlock()
	}
	s.mu.RUnlock()

	for _, id := range stale {
		log.Printf("[server] heartbeat timeout: sidecar %s", id)
		s.mu.RLock()
		sc, ok := s.sidecars[id]
		s.mu.RUnlock()
		if ok {
			sc.codec.Close()
			s.removeSidecar(id)
		}
	}
}

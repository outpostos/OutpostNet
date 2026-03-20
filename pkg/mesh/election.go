package mesh

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Election wire protocol: uses the forwarding listener with a dedicated magic byte.
const (
	// electionMagic distinguishes election messages on the shared listener.
	electionMagic byte = 0xCC

	// Election message types.
	elecMsgPropose byte = 0x01 // propose self as leader
	elecMsgAccept  byte = 0x02 // accept proposal
	elecMsgReject  byte = 0x03 // reject proposal (higher-priority leader exists)
	elecMsgQuery   byte = 0x04 // query current leader
	elecMsgLeader  byte = 0x05 // leader announcement response
)

// LeaderState represents this node's role in leader election.
type LeaderState int32

const (
	LeaderStateFollower  LeaderState = 0
	LeaderStateCandidate LeaderState = 1
	LeaderStateLeader    LeaderState = 2
)

func (s LeaderState) String() string {
	switch s {
	case LeaderStateFollower:
		return "follower"
	case LeaderStateCandidate:
		return "candidate"
	case LeaderStateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// LeaderChangeCallback is called when the leader changes.
type LeaderChangeCallback func(leaderID string, isLocal bool)

// ElectionConfig configures the leader election process.
type ElectionConfig struct {
	// NodeID is this node's unique identifier.
	NodeID string

	// NodePriority determines election precedence. Higher priority wins.
	// If equal, lexicographically smaller NodeID wins.
	// Defaults to 0.
	NodePriority int

	// Peers are the other nodes participating in the election.
	Peers []PeerConfig

	// ElectionTimeout is how long to wait for responses during an election.
	// Defaults to 5s.
	ElectionTimeout time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats to followers.
	// Defaults to 10s.
	HeartbeatInterval time.Duration

	// LeaderTimeout is how long a follower waits before triggering re-election
	// if no heartbeat is received. Defaults to 30s.
	LeaderTimeout time.Duration

	// DialTimeout for connecting to peers during election.
	DialTimeout time.Duration
}

// DefaultElectionConfig returns an election config with sensible defaults.
func DefaultElectionConfig(nodeID string) ElectionConfig {
	return ElectionConfig{
		NodeID:            nodeID,
		ElectionTimeout:   5 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		LeaderTimeout:     30 * time.Second,
		DialTimeout:       3 * time.Second,
	}
}

// ElectionMetrics tracks election-related counters.
type ElectionMetrics struct {
	ElectionsStarted atomic.Int64
	ElectionsWon     atomic.Int64
	ElectionsLost    atomic.Int64
	HeartbeatsSent   atomic.Int64
	HeartbeatsRecv   atomic.Int64
}

// LeaderElection implements a simple priority-based leader election protocol.
// The node with the highest priority (or lexicographically smallest ID on tie)
// becomes the leader. Election is triggered on startup and when the current
// leader is unreachable.
type LeaderElection struct {
	config  ElectionConfig
	logger  *slog.Logger
	Metrics ElectionMetrics

	mu              sync.RWMutex
	state           LeaderState
	currentLeaderID string
	lastHeartbeat   time.Time
	callbacks       []LeaderChangeCallback

	done chan struct{}
	wg   sync.WaitGroup
}

// NewLeaderElection creates a new leader election instance.
func NewLeaderElection(config ElectionConfig, logger *slog.Logger) *LeaderElection {
	if logger == nil {
		logger = slog.Default()
	}
	if config.ElectionTimeout == 0 {
		config.ElectionTimeout = 5 * time.Second
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 10 * time.Second
	}
	if config.LeaderTimeout == 0 {
		config.LeaderTimeout = 30 * time.Second
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 3 * time.Second
	}

	return &LeaderElection{
		config: config,
		logger: logger.With("component", "election", "node", config.NodeID),
		done:   make(chan struct{}),
	}
}

// OnLeaderChange registers a callback for leader changes.
func (le *LeaderElection) OnLeaderChange(cb LeaderChangeCallback) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.callbacks = append(le.callbacks, cb)
}

// Start begins the election process and periodic leader monitoring.
func (le *LeaderElection) Start(ctx context.Context) error {
	// If no peers, this node is the leader by default.
	if len(le.config.Peers) == 0 {
		le.becomeLeader()
		return nil
	}

	// Start initial election.
	le.startElection()

	// Start leader monitoring loop.
	le.wg.Add(1)
	go func() {
		defer le.wg.Done()
		le.monitorLoop(ctx)
	}()

	return nil
}

// Stop shuts down the leader election.
func (le *LeaderElection) Stop() {
	close(le.done)
	le.wg.Wait()
}

// IsLeader returns true if this node is the current leader.
func (le *LeaderElection) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.state == LeaderStateLeader
}

// State returns the current leader state.
func (le *LeaderElection) State() LeaderState {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.state
}

// CurrentLeader returns the ID of the current leader.
func (le *LeaderElection) CurrentLeader() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.currentLeaderID
}

// HandleElectionMessage processes an incoming election message.
// This is called by the Forwarder when it receives the election magic byte.
func (le *LeaderElection) HandleElectionMessage(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(le.config.ElectionTimeout))

	var msgType [1]byte
	if _, err := io.ReadFull(conn, msgType[:]); err != nil {
		le.logger.Error("read election msg type", "error", err)
		return
	}

	switch msgType[0] {
	case elecMsgPropose:
		le.handleProposal(conn)
	case elecMsgQuery:
		le.handleQuery(conn)
	case elecMsgLeader:
		le.handleLeaderAnnouncement(conn)
	default:
		le.logger.Warn("unknown election message", "type", msgType[0])
	}
}

// --- Election logic ---

func (le *LeaderElection) startElection() {
	le.mu.Lock()
	le.state = LeaderStateCandidate
	le.mu.Unlock()

	le.Metrics.ElectionsStarted.Add(1)
	le.logger.Info("starting election")

	// Propose self to all peers.
	accepts := 0
	rejects := 0
	var winnerID string

	for _, peer := range le.config.Peers {
		accepted, rejectedBy, err := le.proposeToPeer(peer)
		if err != nil {
			le.logger.Debug("propose failed", "peer", peer.NodeID, "error", err)
			continue
		}
		if accepted {
			accepts++
		} else {
			rejects++
			if rejectedBy != "" {
				winnerID = rejectedBy
			}
		}
	}

	// If no rejections (or all peers unreachable), become leader.
	if rejects == 0 {
		le.becomeLeader()
	} else if winnerID != "" {
		le.setLeader(winnerID)
		le.Metrics.ElectionsLost.Add(1)
	}
}

func (le *LeaderElection) proposeToPeer(peer PeerConfig) (accepted bool, rejectedBy string, err error) {
	dialer := net.Dialer{Timeout: le.config.DialTimeout}
	conn, err := dialer.Dial("tcp", peer.Address)
	if err != nil {
		return false, "", fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(le.config.ElectionTimeout))

	// Send: magic + propose + our nodeID + priority.
	myID := []byte(le.config.NodeID)
	buf := make([]byte, 1+1+2+len(myID)+4) // magic + type + idLen + id + priority
	buf[0] = electionMagic
	buf[1] = elecMsgPropose
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(myID)))
	copy(buf[4:4+len(myID)], myID)
	binary.BigEndian.PutUint32(buf[4+len(myID):], uint32(le.config.NodePriority))

	if _, err := conn.Write(buf); err != nil {
		return false, "", fmt.Errorf("write proposal: %w", err)
	}

	// Read response: type(1) + idLen(2) + id(var)
	var resp [1]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		return false, "", fmt.Errorf("read response: %w", err)
	}

	if resp[0] == elecMsgAccept {
		return true, "", nil
	}

	// Rejected - read the winner's ID.
	var idLen [2]byte
	if _, err := io.ReadFull(conn, idLen[:]); err != nil {
		return false, "", fmt.Errorf("read reject id len: %w", err)
	}
	idBuf := make([]byte, binary.BigEndian.Uint16(idLen[:]))
	if _, err := io.ReadFull(conn, idBuf); err != nil {
		return false, "", fmt.Errorf("read reject id: %w", err)
	}

	return false, string(idBuf), nil
}

func (le *LeaderElection) handleProposal(conn net.Conn) {
	// Read proposer's ID length + ID + priority.
	var idLen [2]byte
	if _, err := io.ReadFull(conn, idLen[:]); err != nil {
		le.logger.Error("read proposal id len", "error", err)
		return
	}
	nameLen := binary.BigEndian.Uint16(idLen[:])
	if nameLen == 0 || nameLen > 256 {
		le.logger.Error("invalid proposal id length", "len", nameLen)
		return
	}

	buf := make([]byte, nameLen+4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		le.logger.Error("read proposal data", "error", err)
		return
	}

	proposerID := string(buf[:nameLen])
	proposerPriority := int(binary.BigEndian.Uint32(buf[nameLen:]))

	le.logger.Debug("received proposal", "from", proposerID, "priority", proposerPriority)

	// Compare: higher priority wins; on tie, smaller ID wins.
	if le.shouldAcceptProposal(proposerID, proposerPriority) {
		// Accept - the proposer has higher priority.
		conn.Write([]byte{elecMsgAccept})
	} else {
		// Reject - we (or our known leader) have higher priority.
		le.mu.RLock()
		winnerID := le.currentLeaderID
		if winnerID == "" {
			winnerID = le.config.NodeID
		}
		le.mu.RUnlock()

		winnerBytes := []byte(winnerID)
		resp := make([]byte, 1+2+len(winnerBytes))
		resp[0] = elecMsgReject
		binary.BigEndian.PutUint16(resp[1:3], uint16(len(winnerBytes)))
		copy(resp[3:], winnerBytes)
		conn.Write(resp)
	}
}

func (le *LeaderElection) shouldAcceptProposal(proposerID string, proposerPriority int) bool {
	myPriority := le.config.NodePriority
	if proposerPriority > myPriority {
		return true
	}
	if proposerPriority < myPriority {
		return false
	}
	// Equal priority: smaller ID wins.
	return proposerID < le.config.NodeID
}

func (le *LeaderElection) handleQuery(conn net.Conn) {
	le.mu.RLock()
	leaderID := le.currentLeaderID
	le.mu.RUnlock()

	leaderBytes := []byte(leaderID)
	resp := make([]byte, 1+2+len(leaderBytes))
	resp[0] = elecMsgLeader
	binary.BigEndian.PutUint16(resp[1:3], uint16(len(leaderBytes)))
	copy(resp[3:], leaderBytes)
	conn.Write(resp)
}

func (le *LeaderElection) handleLeaderAnnouncement(conn net.Conn) {
	// Read leader ID.
	var idLen [2]byte
	if _, err := io.ReadFull(conn, idLen[:]); err != nil {
		return
	}
	nameLen := binary.BigEndian.Uint16(idLen[:])
	if nameLen == 0 || nameLen > 256 {
		return
	}
	idBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, idBuf); err != nil {
		return
	}

	leaderID := string(idBuf)
	le.setLeader(leaderID)
	le.mu.Lock()
	le.lastHeartbeat = time.Now()
	le.mu.Unlock()

	le.Metrics.HeartbeatsRecv.Add(1)
}

// --- State transitions ---

func (le *LeaderElection) becomeLeader() {
	le.mu.Lock()
	oldLeader := le.currentLeaderID
	le.state = LeaderStateLeader
	le.currentLeaderID = le.config.NodeID
	cbs := make([]LeaderChangeCallback, len(le.callbacks))
	copy(cbs, le.callbacks)
	le.mu.Unlock()

	le.Metrics.ElectionsWon.Add(1)
	le.logger.Info("became leader")

	if oldLeader != le.config.NodeID {
		for _, cb := range cbs {
			cb(le.config.NodeID, true)
		}
	}
}

func (le *LeaderElection) setLeader(leaderID string) {
	le.mu.Lock()
	oldLeader := le.currentLeaderID
	le.state = LeaderStateFollower
	le.currentLeaderID = leaderID
	le.lastHeartbeat = time.Now()
	cbs := make([]LeaderChangeCallback, len(le.callbacks))
	copy(cbs, le.callbacks)
	le.mu.Unlock()

	if oldLeader != leaderID {
		le.logger.Info("new leader", "leader", leaderID)
		for _, cb := range cbs {
			cb(leaderID, false)
		}
	}
}

// --- Monitor loop ---

func (le *LeaderElection) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(le.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-le.done:
			return
		case <-ticker.C:
			le.mu.RLock()
			state := le.state
			lastHB := le.lastHeartbeat
			le.mu.RUnlock()

			if state == LeaderStateLeader {
				// Send heartbeats to peers.
				le.sendHeartbeats()
			} else {
				// Check if leader is still alive.
				if !lastHB.IsZero() && time.Since(lastHB) > le.config.LeaderTimeout {
					le.logger.Warn("leader heartbeat timeout, starting re-election")
					le.startElection()
				}
			}
		}
	}
}

func (le *LeaderElection) sendHeartbeats() {
	myID := []byte(le.config.NodeID)

	for _, peer := range le.config.Peers {
		go func(p PeerConfig) {
			dialer := net.Dialer{Timeout: le.config.DialTimeout}
			conn, err := dialer.Dial("tcp", p.Address)
			if err != nil {
				le.logger.Debug("heartbeat dial failed", "peer", p.NodeID, "error", err)
				return
			}
			defer conn.Close()

			conn.SetDeadline(time.Now().Add(le.config.DialTimeout))

			// Send: magic + leader announcement + id.
			buf := make([]byte, 1+1+2+len(myID))
			buf[0] = electionMagic
			buf[1] = elecMsgLeader
			binary.BigEndian.PutUint16(buf[2:4], uint16(len(myID)))
			copy(buf[4:], myID)
			conn.Write(buf)

			le.Metrics.HeartbeatsSent.Add(1)
		}(peer)
	}
}

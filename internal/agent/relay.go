package agent

import (
	"context"
	"log/slog"
	"sync"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
	"github.com/quic-go/quic-go"
)

// relayKey uniquely identifies a relay rule by service + source region.
type relayKey struct {
	service    string
	fromRegion string
}

// relayRule is an active relay rule with a reference to the downstream client.
type relayRule struct {
	toRegion string
	maxHops  int
	client   *tunnel.ManagedClient
}

// RelayManager manages relay forwarding rules. When a tunnel server receives
// a stream for a service with no local route, the RelayManager opens a new
// stream on the downstream tunnel client and relays data bidirectionally.
type RelayManager struct {
	logger *slog.Logger

	mu    sync.RWMutex
	rules map[relayKey]*relayRule
}

// NewRelayManager creates a new RelayManager from agent config.
func NewRelayManager(logger *slog.Logger) *RelayManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &RelayManager{
		logger: logger,
		rules:  make(map[relayKey]*relayRule),
	}
}

// AddRule registers a relay rule. The client must be the ManagedClient
// connected to the toRegion's tunnel server.
func (rm *RelayManager) AddRule(cfg agentconfig.RelayConfig, client *tunnel.ManagedClient) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := relayKey{service: cfg.Service, fromRegion: cfg.FromRegion}
	rm.rules[key] = &relayRule{
		toRegion: cfg.ToRegion,
		maxHops:  cfg.MaxHops,
		client:   client,
	}

	rm.logger.Info("relay rule added",
		"service", cfg.Service,
		"from", cfg.FromRegion,
		"to", cfg.ToRegion,
		"max_hops", cfg.MaxHops,
	)
}

// HasRules returns true if any relay rules are configured.
func (rm *RelayManager) HasRules() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.rules) > 0
}

// HandleStream is the StreamFallback handler for tunnel servers.
// It is called when a stream arrives for a service with no local route.
func (rm *RelayManager) HandleStream(ctx context.Context, stream *quic.Stream, hdr tunnel.StreamHeader) {
	rm.mu.RLock()
	// Try to find a rule matching the service name. The fromRegion is not
	// available from the stream header (QUIC doesn't tell us), so we match
	// on service name only. For multi-source relay, the first matching rule wins.
	var matched *relayRule
	for key, rule := range rm.rules {
		if key.service == hdr.ServiceName {
			matched = rule
			break
		}
	}
	rm.mu.RUnlock()

	if matched == nil {
		rm.logger.Warn("no relay rule for service", "service", hdr.ServiceName)
		return
	}

	// Check hop count.
	hopCount := hdr.HopCount + 1
	if int(hopCount) > matched.maxHops {
		rm.logger.Warn("relay hop limit exceeded",
			"service", hdr.ServiceName,
			"hops", hopCount,
			"max_hops", matched.maxHops,
		)
		return
	}

	rm.logger.Debug("relaying stream",
		"service", hdr.ServiceName,
		"to_region", matched.toRegion,
		"hop", hopCount,
	)

	// Open a stream on the downstream client with relay header.
	downstream, err := matched.client.OpenRelayStream(ctx, hdr.ServiceName, hopCount)
	if err != nil {
		rm.logger.Error("open downstream relay stream",
			"service", hdr.ServiceName,
			"to_region", matched.toRegion,
			"error", err,
		)
		return
	}
	defer downstream.Close()
	defer matched.client.StreamDone()

	// Bidirectional relay between upstream and downstream.
	biRelay(stream, downstream)
}

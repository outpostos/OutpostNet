package agent

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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
	logger  *slog.Logger
	metrics *Metrics

	mu    sync.RWMutex
	rules map[relayKey]*relayRule
}

// NewRelayManager creates a new RelayManager from agent config.
func NewRelayManager(logger *slog.Logger, metrics *Metrics) *RelayManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &RelayManager{
		logger:  logger,
		metrics: metrics,
		rules:   make(map[relayKey]*relayRule),
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
	start := time.Now()

	rm.mu.RLock()
	// Try to find a rule matching the service name. The fromRegion is not
	// available from the stream header (QUIC doesn't tell us), so we match
	// on service name only. For multi-source relay, the first matching rule wins.
	var matched *relayRule
	var matchedFrom string
	for key, rule := range rm.rules {
		if key.service == hdr.ServiceName {
			matched = rule
			matchedFrom = key.fromRegion
			break
		}
	}
	rm.mu.RUnlock()

	if matched == nil {
		rm.logger.Warn("no relay rule for service", "service", hdr.ServiceName)
		if rm.metrics != nil {
			rm.metrics.RelayError("no_rule")
		}
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
		if rm.metrics != nil {
			rm.metrics.RelayError("hop_limit")
		}
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
		if rm.metrics != nil {
			rm.metrics.RelayError("open_stream")
		}
		return
	}
	defer downstream.Close()
	defer matched.client.StreamDone()

	if rm.metrics != nil {
		rm.metrics.ObserveRelayLatency(time.Since(start))
		rm.metrics.RelayStreamStarted(matchedFrom, matched.toRegion, hdr.ServiceName)
		defer rm.metrics.RelayStreamFinished()
	}

	// Bidirectional relay between upstream and downstream with byte counting.
	sent, recv := countingBiRelay(stream, downstream)

	if rm.metrics != nil {
		rm.metrics.AddRelayBytes("sent", matchedFrom, matched.toRegion, sent)
		rm.metrics.AddRelayBytes("recv", matchedFrom, matched.toRegion, recv)
	}
}

// countingBiRelay copies data bidirectionally and returns bytes transferred
// in each direction: sent (a→b) and received (b→a).
func countingBiRelay(a, b io.ReadWriteCloser) (sent, recv int64) {
	var sentCount, recvCount atomic.Int64
	done := make(chan struct{}, 2)
	go func() {
		n, _ := io.Copy(b, a)
		sentCount.Store(n)
		done <- struct{}{}
	}()
	go func() {
		n, _ := io.Copy(a, b)
		recvCount.Store(n)
		done <- struct{}{}
	}()
	<-done
	return sentCount.Load(), recvCount.Load()
}

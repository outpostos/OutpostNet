package agent

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/outpostos/OutpostNet/internal/observation"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
)

// Observation is a running OutpostRange observation producer for one host
// process: a Reporter fed from the TunnelManager's client tunnels and,
// when it could bind, a loopback endpoint. It never stops the host: every
// failure is logged and the product continues without observation.
type Observation struct {
	Reporter *observation.Reporter
	server   *observation.Server
	wg       sync.WaitGroup
}

// ObservationConfig configures StartObservation.
type ObservationConfig struct {
	// Addr is the loopback listen address; empty disables observation.
	Addr string
	// NodeID is the stable node identity; empty disables observation.
	NodeID    string
	Component observation.Component
}

// StartObservation starts reporting tm's client tunnels until ctx is done.
// Call it before tm.Start so the first connection attempts are observed. It
// returns nil when observation is disabled or cannot start; the reason is
// logged.
func StartObservation(ctx context.Context, cfg ObservationConfig, tm *TunnelManager, logger *slog.Logger) *Observation {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Addr == "" {
		return nil
	}
	if cfg.NodeID == "" {
		logger.Error("observation disabled: node_id is required to identify the observation stream", "observation_addr", cfg.Addr)
		return nil
	}
	o := &Observation{Reporter: observation.NewReporter(observation.ReporterConfig{
		NodeID:    cfg.NodeID,
		Component: cfg.Component,
		Logger:    logger,
	})}
	if tm != nil {
		notifier := observation.NewNotifier()
		tm.SetLinkObserver(notifier.Notify)
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			o.Reporter.RunNetSampler(ctx, func() []observation.NetLink { return ObservationLinks(tm) }, notifier.C(), observation.DefaultSampleInterval)
		}()
	}
	server, err := observation.NewServer(cfg.Addr, o.Reporter, logger)
	if err == nil {
		err = server.Start(ctx)
	}
	if err != nil {
		logger.Error("observation endpoint unavailable; continuing without observation API", "error", err)
		return o
	}
	o.server = server
	return o
}

// Addr is the bound observation endpoint, or nil when it is not serving.
func (o *Observation) Addr() net.Addr {
	if o == nil || o.server == nil {
		return nil
	}
	return o.server.Addr()
}

// Wait waits for the sampler to stop after the context passed to
// StartObservation is done.
func (o *Observation) Wait() {
	if o != nil {
		o.wg.Wait()
	}
}

// ObservationLinks maps the client tunnels to Net observation facts.
func ObservationLinks(tm *TunnelManager) []observation.NetLink {
	links := tm.PeerLinks()
	out := make([]observation.NetLink, 0, len(links))
	for _, link := range links {
		out = append(out, observation.NetLink{
			PeerNodeID:    link.NodeID,
			PeerAddress:   link.Status.ServerAddr,
			State:         linkState(link.Status.State),
			LastError:     link.Status.LastError,
			LastErrorTime: link.Status.LastErrorTime,
		})
	}
	return out
}

func linkState(s tunnel.ConnState) observation.LinkState {
	switch s {
	case tunnel.StateConnected:
		return observation.LinkConnected
	case tunnel.StateConnecting, tunnel.StateReconnecting:
		return observation.LinkConnecting
	case tunnel.StateDisconnected:
		return observation.LinkDisconnected
	default:
		return 0
	}
}

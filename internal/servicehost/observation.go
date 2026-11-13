package servicehost

import (
	"errors"
	"net"

	"github.com/outpostos/OutpostNet/internal/agent"
	"github.com/outpostos/OutpostNet/internal/observation"
	"github.com/outpostos/OutpostNet/pkg/service"
)

// EnableObservation makes Run serve the OutpostRange observation API on the
// loopback addr (stream component OUTPOSTSERVICE). Call before Run. Any
// observation failure is logged and never stops the host.
func (h *Host) EnableObservation(addr string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observationAddr = addr
}

// ObservationAddr is the bound observation endpoint, or nil when disabled or
// not serving.
func (h *Host) ObservationAddr() net.Addr {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.observation.Addr()
}

// observedOpener wraps an entry's opener so real connections through it are
// reported as Service access facts. Without observation it returns next.
func observedOpener(o *agent.Observation, tm *agent.TunnelManager, target service.Target, next service.StreamOpener) (service.StreamOpener, *observation.EntryObserver) {
	if o == nil {
		return next, nil
	}
	entry := o.Reporter.ServiceEntry(target.String())
	return entry.Wrap(next, observation.OpenerConfig{
		Resolve: func(t service.Target) (observation.Backend, bool) {
			if t.Logical {
				return observation.Backend{}, false // reported by the opened stream
			}
			if t.Dynamic() {
				mc, ok := tm.DynamicClient(t.NodeID)
				if !ok {
					return observation.Backend{}, false
				}
				return observation.Backend{NodeID: t.NodeID, Address: mc.ServerAddr()}, true
			}
			mc, ok := tm.RegionClient(t.Region)
			if !ok {
				return observation.Backend{}, false
			}
			return observation.Backend{Address: mc.ServerAddr()}, true
		},
		NoBackend: func(err error) bool {
			return errors.Is(err, agent.ErrNoTunnel) || errors.Is(err, service.ErrNoCandidate) || errors.Is(err, service.ErrDirectoryStale)
		},
	}), entry
}

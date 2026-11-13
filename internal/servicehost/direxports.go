package servicehost

import (
	"context"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/outpostos/OutpostNet/internal/meshdirectory"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

// Directory-driven exports (OutpostOS ADR-0003 D4): every usable
// registration in the service directory whose node_id is this node becomes
// the export route <service_name> -> 127.0.0.1:<port>, so remote logical
// entries reach it. A static export of the same name wins (with a
// rate-limited warning). The directory keeps serving a stale answer while
// Mesh is unreachable, and drops instances whose TTL passed, so directory
// exports follow both.

const (
	dirExportsResync       = time.Second
	dirExportConflictEvery = time.Minute
)

// runDirectoryExports reconciles directory exports until ctx ends.
func (h *Host) runDirectoryExports(ctx context.Context, dir *meshdirectory.Directory) {
	kick := make(chan struct{}, 1)
	cancel := dir.Subscribe(func() {
		select {
		case kick <- struct{}{}:
		default:
		}
	})
	defer cancel()
	t := time.NewTicker(dirExportsResync)
	defer t.Stop()
	for {
		h.reconcileDirectoryExports(dir)
		select {
		case <-ctx.Done():
			return
		case <-kick:
		case <-t.C:
		}
	}
}

// desiredDirectoryRoutes computes service name -> local address from the
// directory: per service, the first of this node's usable instances in
// service.Select order.
func desiredDirectoryRoutes(dir *meshdirectory.Directory, self string, now time.Time) map[string]string {
	out := map[string]string{}
	for _, name := range dir.ServiceNames() {
		insts, _ := dir.Instances(name)
		local := insts[:0:0]
		for _, in := range insts {
			if in.NodeID == self {
				local = append(local, in)
			}
		}
		sel := service.Select(local, self, nil, now)
		if len(sel) == 0 {
			continue
		}
		port, _ := sel[0].TCPPort()
		out[name] = net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))
	}
	return out
}

func (h *Host) reconcileDirectoryExports(dir *meshdirectory.Directory) {
	desired := desiredDirectoryRoutes(dir, h.cfg.NodeID, time.Now())
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dirRoutes = desired
	h.syncRoutesLocked()
}

// syncRoutesLocked moves every tunnel server from the applied route table to
// static exports merged with directory routes (static wins) and reports the
// names that changed. h.mu must be held.
func (h *Host) syncRoutesLocked() service.ExportChanges {
	next := make(map[string]string, len(h.exports)+len(h.dirRoutes))
	for name, addr := range h.dirRoutes {
		next[name] = addr
	}
	now := time.Now()
	for _, e := range h.exports {
		if dirAddr, ok := h.dirRoutes[e.Name]; ok {
			if last, warned := h.conflictWarned[e.Name]; !warned || now.Sub(last) >= dirExportConflictEvery {
				h.conflictWarned[e.Name] = now
				h.logger.Warn("static export overrides directory export of the same service", "service", e.Name, "static_addr", e.LocalAddr, "directory_addr", dirAddr)
			}
		} else {
			delete(h.conflictWarned, e.Name)
		}
		next[e.Name] = e.LocalAddr
	}

	var ch service.ExportChanges
	if h.tunnels != nil {
		old, nxt := routeList(h.routes), routeList(next)
		seen := map[string]struct{}{}
		add := func(dst *[]string, names []string) {
			for _, n := range names {
				if _, ok := seen[n]; ok {
					continue
				}
				seen[n] = struct{}{}
				*dst = append(*dst, n)
			}
		}
		for _, srv := range h.tunnels.Servers() {
			part := service.ApplyExports(srv, old, nxt)
			add(&ch.Added, part.Added)
			add(&ch.Updated, part.Updated)
			add(&ch.Removed, part.Removed)
		}
	}
	h.routes = next
	if len(ch.Added)+len(ch.Updated)+len(ch.Removed) > 0 {
		h.logger.Info("exports applied", "added", ch.Added, "updated", ch.Updated, "removed", ch.Removed)
	}
	return ch
}

func routeList(m map[string]string) []agentconfig.ExportConfig {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]agentconfig.ExportConfig, 0, len(names))
	for _, n := range names {
		out = append(out, agentconfig.ExportConfig{Name: n, LocalAddr: m[n], Protocol: "tcp"})
	}
	return out
}

// ExportRoutes returns the routes currently applied to the tunnel servers
// (static exports merged with directory exports).
func (h *Host) ExportRoutes() map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]string, len(h.routes))
	for k, v := range h.routes {
		out[k] = v
	}
	return out
}

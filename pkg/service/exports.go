package service

import "github.com/outpostos/OutpostNet/pkg/agentconfig"

// RouteTable is the Net port used for exports; *tunnel.Server satisfies it.
type RouteTable interface {
	AddRoute(serviceName, localAddr string)
	RemoveRoute(serviceName string)
}

// ExportChanges reports which service names an ApplyExports call touched.
type ExportChanges struct {
	Added   []string
	Updated []string
	Removed []string
}

// RegisterExports installs every export as a route.
func RegisterExports(rt RouteTable, exports []agentconfig.ExportConfig) {
	for _, exp := range exports {
		rt.AddRoute(exp.Name, exp.LocalAddr)
	}
}

// ApplyExports moves rt from the old export set to the new one: removed
// names are dropped, new names added, and names whose local address changed
// are re-pointed. Routes are looked up when a stream opens, so streams that
// are already established keep their original backend.
func ApplyExports(rt RouteTable, old, next []agentconfig.ExportConfig) ExportChanges {
	oldAddr := make(map[string]string, len(old))
	for _, e := range old {
		oldAddr[e.Name] = e.LocalAddr
	}
	nextAddr := make(map[string]string, len(next))
	for _, e := range next {
		nextAddr[e.Name] = e.LocalAddr
	}

	var ch ExportChanges
	for _, e := range old {
		if _, ok := nextAddr[e.Name]; !ok {
			rt.RemoveRoute(e.Name)
			ch.Removed = append(ch.Removed, e.Name)
		}
	}
	for _, e := range next {
		prev, ok := oldAddr[e.Name]
		switch {
		case !ok:
			rt.AddRoute(e.Name, e.LocalAddr)
			ch.Added = append(ch.Added, e.Name)
		case prev != e.LocalAddr:
			rt.AddRoute(e.Name, e.LocalAddr)
			ch.Updated = append(ch.Updated, e.Name)
		}
	}
	return ch
}

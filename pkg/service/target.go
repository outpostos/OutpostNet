package service

import (
	"context"
	"fmt"
	"io"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// maxServiceNameLen mirrors the Net stream-header limit (pkg/tunnel).
const maxServiceNameLen = 255

// Ref is a stable service identity in the form "<service>@<node_id>".
type Ref struct {
	Service string
	NodeID  string
}

// ParseRef parses "<service>@<node_id>" using the shared agentconfig rule.
func ParseRef(s string) (Ref, error) {
	svc, node, err := agentconfig.ParseServiceRef(s)
	if err != nil {
		return Ref{}, err
	}
	return Ref{Service: svc, NodeID: node}, nil
}

// String renders the identity as "<service>@<node_id>".
func (r Ref) String() string { return r.Service + "@" + r.NodeID }

// Target returns the dynamic-path routing target for this identity.
func (r Ref) Target() Target { return Target{Service: r.Service, NodeID: r.NodeID} }

// Target is what the Service layer asks Net to reach.
//
// Exactly one routing path is used:
//   - NodeID set: dynamic path, resolved through the peer directory.
//   - otherwise: static path, the configured client tunnel whose
//     remote_region equals Region (empty Region is the legacy FRP-style
//     unnamed tunnel).
type Target struct {
	// Service is the exported service name carried in the Net stream header.
	Service string
	// NodeID selects the dynamic peer-directory path.
	NodeID string
	// Region selects the static configured-tunnel path.
	Region string
}

// Dynamic reports whether the target uses the peer-directory path.
func (t Target) Dynamic() bool { return t.NodeID != "" }

// String renders the target for logs.
func (t Target) String() string {
	if t.Dynamic() {
		return t.Service + "@" + t.NodeID
	}
	if t.Region != "" {
		return t.Service + " via region " + t.Region
	}
	return t.Service
}

// Validate checks the target against the Net stream-header constraints.
func (t Target) Validate() error {
	if t.Service == "" {
		return fmt.Errorf("service name is required")
	}
	if len(t.Service) > maxServiceNameLen {
		return fmt.Errorf("service name too long: %d > %d", len(t.Service), maxServiceNameLen)
	}
	if t.NodeID != "" && t.Region != "" {
		return fmt.Errorf("target %q: node_id and region are mutually exclusive", t.Service)
	}
	return nil
}

// StreamOpener is the Net port used by entries: open one bidirectional byte
// stream to the target's current endpoint. Closing the stream must release
// any Net-side bookkeeping (e.g. in-flight stream accounting).
type StreamOpener interface {
	OpenStream(ctx context.Context, t Target) (io.ReadWriteCloser, error)
}

// StreamOpenerFunc adapts a function to StreamOpener.
type StreamOpenerFunc func(ctx context.Context, t Target) (io.ReadWriteCloser, error)

// OpenStream implements StreamOpener.
func (f StreamOpenerFunc) OpenStream(ctx context.Context, t Target) (io.ReadWriteCloser, error) {
	return f(ctx, t)
}

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel"
	"github.com/quic-go/quic-go"
)

// DefaultConfirmTimeout bounds the wait for a provider's confirmation when
// the caller's context has no deadline.
const DefaultConfirmTimeout = 5 * time.Second

// OpenConfirmedPeerStream opens a stream named serviceName toward nodeID
// (over the dialed tunnel or, when this node only accepted nodeID, over its
// accepted connection) and waits for the provider to confirm that it has
// connected the service backend (OutpostOS ADR-0003 D3). Nothing has been
// forwarded when it returns an error, so the caller may try another
// provider. Errors wrap tunnel.ErrStreamNoRoute,
// tunnel.ErrStreamBackendUnavailable, tunnel.ErrStreamNotConfirmed or
// ErrNoTunnel.
func (tm *TunnelManager) OpenConfirmedPeerStream(ctx context.Context, nodeID, serviceName string) (io.ReadWriteCloser, error) {
	var (
		stream *quic.Stream
		rw     io.ReadWriteCloser
		err    error
	)
	if mc, ok := tm.DynamicClient(nodeID); ok {
		stream, err = mc.OpenConfirmedServiceStream(ctx, serviceName)
		if err != nil {
			return nil, fmt.Errorf("peer %q: %w", nodeID, err)
		}
		rw = &trackedStream{Stream: stream, mc: mc}
	} else {
		tm.mu.Lock()
		servers := append([]*tunnel.Server(nil), tm.servers...)
		tm.mu.Unlock()
		for _, srv := range servers {
			stream, err = srv.OpenConfirmedPeerStream(ctx, nodeID, serviceName)
			if err == nil {
				break
			}
			if !errors.Is(err, tunnel.ErrNoPeerConn) {
				return nil, err
			}
		}
		if stream == nil {
			return nil, fmt.Errorf("peer %q: no dialed tunnel and no accepted connection: %w", nodeID, ErrNoTunnel)
		}
		rw = stream
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(DefaultConfirmTimeout)
	}
	_ = stream.SetReadDeadline(deadline)
	ackErr := tunnel.ReadStreamAck(stream)
	_ = stream.SetReadDeadline(time.Time{})
	if ackErr != nil {
		stream.CancelRead(0)
		rw.Close()
		return nil, fmt.Errorf("peer %q service %q: %w", nodeID, serviceName, ackErr)
	}
	return rw, nil
}

// PeerReachable reports whether this node currently has a usable tunnel to
// nodeID: a connected dialed client, or an accepted connection on which
// nodeID declared itself.
func (tm *TunnelManager) PeerReachable(nodeID string) bool {
	if mc, ok := tm.DynamicClient(nodeID); ok && mc.State() == tunnel.StateConnected {
		return true
	}
	tm.mu.Lock()
	servers := append([]*tunnel.Server(nil), tm.servers...)
	tm.mu.Unlock()
	for _, srv := range servers {
		if srv.HasAcceptedPeer(nodeID) {
			return true
		}
	}
	return false
}

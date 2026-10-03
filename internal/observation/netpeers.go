package observation

import (
	"context"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// LinkState is the local Net session state toward one peer.
type LinkState int

const (
	// LinkDisconnected means the session failed or is closed.
	LinkDisconnected LinkState = iota + 1
	// LinkConnecting means a dial or redial is in progress.
	LinkConnecting
	// LinkConnected means the session is usable.
	LinkConnected
)

func (s LinkState) proto() (observationv1alpha1.PeerLinkState, bool) {
	switch s {
	case LinkConnected:
		return observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTED, true
	case LinkConnecting:
		return observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTING, true
	case LinkDisconnected:
		return observationv1alpha1.PeerLinkState_PEER_LINK_STATE_DISCONNECTED, true
	default:
		return observationv1alpha1.PeerLinkState_PEER_LINK_STATE_UNSPECIFIED, false
	}
}

// NetLink is one locally dialled Net session as the host sees it now.
type NetLink struct {
	// PeerNodeID is set only when the host knows the peer's node identity.
	PeerNodeID  string
	PeerAddress string
	// State is zero when it cannot be measured; it is then not reported.
	State         LinkState
	LastError     string
	LastErrorTime time.Time
}

func (l NetLink) key() string {
	if l.PeerNodeID != "" {
		return "node:" + l.PeerNodeID
	}
	return "addr:" + l.PeerAddress
}

func (l NetLink) message() *observationv1alpha1.NetPeerChanged {
	msg := &observationv1alpha1.NetPeerChanged{
		PeerNodeId:    l.PeerNodeID,
		PeerAddress:   l.PeerAddress,
		LastError:     l.LastError,
		LastErrorTime: timestampOrNil(l.LastErrorTime),
	}
	if state, ok := l.State.proto(); ok {
		msg.State = &state
	}
	return msg
}

// UpdateNetLinks replaces the current Net peer facts with links. A new or
// changed link appends one net_peer_changed event; a link that disappeared
// (the peer was removed) is reported DISCONNECTED once and then dropped from
// the snapshot.
func (r *Reporter) UpdateNetLinks(links []NetLink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[string]bool, len(links))
	for _, link := range links {
		key := link.key()
		seen[key] = true
		next := link.message()
		if current, ok := r.netPeers[key]; ok && proto.Equal(current, next) {
			continue
		}
		r.netPeers[key] = next
		r.log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_NetPeerChanged{
			NetPeerChanged: proto.Clone(next).(*observationv1alpha1.NetPeerChanged),
		}})
	}
	for _, key := range sortedKeys(r.netPeers) {
		if seen[key] {
			continue
		}
		last := r.netPeers[key]
		delete(r.netPeers, key)
		if last.GetState() == observationv1alpha1.PeerLinkState_PEER_LINK_STATE_DISCONNECTED {
			continue
		}
		gone := proto.Clone(last).(*observationv1alpha1.NetPeerChanged)
		disconnected := observationv1alpha1.PeerLinkState_PEER_LINK_STATE_DISCONNECTED
		gone.State = &disconnected
		r.log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_NetPeerChanged{NetPeerChanged: gone}})
	}
}

// DefaultSampleInterval bounds how stale Net facts can get when a change
// notification was coalesced away.
const DefaultSampleInterval = time.Second

// RunNetSampler reads links on every notification and every interval and
// reports changes, until ctx is done. notify should be fed by a non-blocking
// send (see Notifier) from the Net host's state callbacks.
func (r *Reporter) RunNetSampler(ctx context.Context, links func() []NetLink, notify <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultSampleInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	r.UpdateNetLinks(links())
	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
		case <-ticker.C:
		}
		r.UpdateNetLinks(links())
	}
}

// Notifier is a coalescing, never-blocking change signal.
type Notifier struct{ ch chan struct{} }

// NewNotifier creates a notifier.
func NewNotifier() *Notifier { return &Notifier{ch: make(chan struct{}, 1)} }

// Notify signals a change without blocking; pending signals coalesce.
func (n *Notifier) Notify() {
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

// C is the channel RunNetSampler waits on.
func (n *Notifier) C() <-chan struct{} { return n.ch }

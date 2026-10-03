// Package observationtest reads an observation endpoint the way OutpostRange
// range-agent 563b72a does (internal/rangeagent/observation.go, which Go's
// internal rule keeps out of reach of this module): strict protojson decoding
// of the snapshot and of every SSE event (unknown fields rejected), seq
// acceptance per (node_id, component) stream with the restart-epoch rule, and
// the subscription_snapshot (117) event built from the snapshot when it
// carries four-layer state. Tests use it to prove that real hosts emit events
// the Range contract accepts.
package observationtest

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	snapshotPath = "/outpostrange/observation/v1alpha1/snapshot"
	eventsPath   = "/outpostrange/observation/v1alpha1/events"
)

// Snapshot fetches and strictly decodes the snapshot.
func Snapshot(ctx context.Context, baseURL string) (*observationv1alpha1.Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+snapshotPath, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("snapshot status %d: %s", resp.StatusCode, data)
	}
	var snapshot observationv1alpha1.Snapshot
	if err := protojson.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("strict snapshot decode: %w", err)
	}
	return &snapshot, nil
}

// Subscription collects the events range-agent would forward.
type Subscription struct {
	mu     sync.Mutex
	events []*observationv1alpha1.Event
	err    error
	cancel context.CancelFunc
	done   chan struct{}
}

// Subscribe reads the snapshot, emits the subscription_snapshot event when
// the snapshot carries four-layer state, then streams events after the
// snapshot seq. Any decode or sequence error stops the subscription and is
// returned by Err.
func Subscribe(ctx context.Context, baseURL string, fallback observationv1alpha1.Component) (*Subscription, error) {
	snapshot, err := Snapshot(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Subscription{cancel: cancel, done: make(chan struct{})}
	lastSeq := map[string]uint64{}
	after := snapshot.GetSeq()
	if after > 0 {
		component := snapshot.GetComponent()
		if component == observationv1alpha1.Component_COMPONENT_UNSPECIFIED {
			component = fallback
		}
		lastSeq[snapshot.GetNodeId()+"/"+component.String()] = after
	}
	if event := subscriptionSnapshotEvent(snapshot, fallback); event != nil {
		s.events = append(s.events, event)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s%s?after_seq=%d", strings.TrimRight(baseURL, "/"), eventsPath, after), nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("events status %d", resp.StatusCode)
	}
	go func() {
		defer close(s.done)
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var data strings.Builder
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case line == "" && data.Len() > 0:
				var event observationv1alpha1.Event
				if err := protojson.Unmarshal([]byte(strings.TrimSpace(data.String())), &event); err != nil {
					s.fail(fmt.Errorf("strict event decode: %w", err))
					return
				}
				data.Reset()
				if event.Component == nil {
					component := fallback
					event.Component = &component
				}
				key := event.GetNodeId() + "/" + event.GetComponent().String()
				seq := event.GetSeq()
				last := lastSeq[key]
				switch {
				case seq == 0:
					s.fail(fmt.Errorf("event %s has missing or zero seq", key))
					return
				case seq <= last:
					continue
				case last > 0 && seq != last+1 && seq>>20 == last>>20:
					s.fail(fmt.Errorf("event stream %s has seq gap: last=%d next=%d", key, last, seq))
					return
				}
				lastSeq[key] = seq
				s.mu.Lock()
				s.events = append(s.events, &event)
				s.mu.Unlock()
			case strings.HasPrefix(line, "data:"):
				data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
	}()
	return s, nil
}

func (s *Subscription) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Close stops the subscription.
func (s *Subscription) Close() {
	s.cancel()
	<-s.done
}

// Err is the first contract violation seen, if any.
func (s *Subscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Events returns a copy of the events received so far.
func (s *Subscription) Events() []*observationv1alpha1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*observationv1alpha1.Event(nil), s.events...)
}

// WaitFor waits until some event satisfies match and returns it, or fails
// with the events seen and any contract violation.
func (s *Subscription) WaitFor(timeout time.Duration, what string, match func(*observationv1alpha1.Event) bool) (*observationv1alpha1.Event, error) {
	deadline := time.Now().Add(timeout)
	for {
		if err := s.Err(); err != nil {
			return nil, err
		}
		for _, event := range s.Events() {
			if match(event) {
				return event, nil
			}
		}
		if time.Now().After(deadline) {
			var seen []string
			for _, event := range s.Events() {
				body, _ := protojson.Marshal(event)
				seen = append(seen, string(body))
			}
			return nil, fmt.Errorf("no event for %s within %v; seen:\n%s", what, timeout, strings.Join(seen, "\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func subscriptionSnapshotEvent(snapshot *observationv1alpha1.Snapshot, fallback observationv1alpha1.Component) *observationv1alpha1.Event {
	if snapshot == nil || snapshot.GetNodeId() == "" || !hasFourLayerState(snapshot) {
		return nil
	}
	component := snapshot.GetComponent()
	if component == observationv1alpha1.Component_COMPONENT_UNSPECIFIED {
		component = fallback
	}
	seq := snapshot.GetSeq()
	return &observationv1alpha1.Event{
		NodeId:    snapshot.GetNodeId(),
		Component: &component,
		Seq:       &seq,
		NodeTime:  snapshot.GetNodeTime(),
		Body:      &observationv1alpha1.Event_SubscriptionSnapshot{SubscriptionSnapshot: snapshot},
	}
}

func hasFourLayerState(snapshot *observationv1alpha1.Snapshot) bool {
	return len(snapshot.GetNetPeers()) > 0 || len(snapshot.GetNetReplication()) > 0 ||
		len(snapshot.GetMeshServices()) > 0 || len(snapshot.GetMeshReplication()) > 0 ||
		snapshot.GetLetRuntime() != nil || len(snapshot.GetServiceAccess()) > 0
}

// NetPeers returns the net_peers carried by a subscription_snapshot event
// or the single net_peer_changed body of a regular event.
func NetPeers(event *observationv1alpha1.Event) []*observationv1alpha1.NetPeerChanged {
	if snap := event.GetSubscriptionSnapshot(); snap != nil {
		return snap.GetNetPeers()
	}
	if peer := event.GetNetPeerChanged(); peer != nil {
		return []*observationv1alpha1.NetPeerChanged{peer}
	}
	return nil
}

// ServiceAccess is NetPeers for service_access_changed / service_access.
func ServiceAccess(event *observationv1alpha1.Event) []*observationv1alpha1.ServiceAccessChanged {
	if snap := event.GetSubscriptionSnapshot(); snap != nil {
		return snap.GetServiceAccess()
	}
	if access := event.GetServiceAccessChanged(); access != nil {
		return []*observationv1alpha1.ServiceAccessChanged{access}
	}
	return nil
}

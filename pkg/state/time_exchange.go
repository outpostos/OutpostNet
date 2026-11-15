package state

import (
	"context"
	"errors"
	"time"
)

func (g *GossipEngine) timeReference() *TimeReference {
	if s, ok := g.store.(interface{ TimeReference() *TimeReference }); ok {
		return s.TimeReference()
	}
	return nil
}
func (g *GossipEngine) timeSample() TimeSample {
	if ref := g.timeReference(); ref != nil {
		return ref.Sample()
	}
	// Without a source observer, quality cannot be inferred from a physical clock.
	now := SystemPhysicalClock{}.NowMS()
	if s, ok := g.store.(interface{ TimeReferenceMS() int64 }); ok {
		now = s.TimeReferenceMS()
	}
	return TimeSample{SentPhysicalMS: now, TimeQuality: TimeUnsynced}
}
func (g *GossipEngine) observeExchange(addr, nodeID string, sample TimeSample, started time.Time, err error) {
	ref := g.timeReference()
	if ref == nil {
		return
	}
	if err != nil {
		var resync *ResyncRequiredError
		if errors.As(err, &resync) {
			sample, nodeID = resync.TimeSample, resync.NodeID
		} else {
			nodeID = ""
		}
	}
	g.mu.Lock()
	if g.timePeerIDs == nil {
		g.timePeerIDs = make(map[string]string)
	}
	previous := g.timePeerIDs[addr]
	if nodeID == "" || nodeID == g.nodeID {
		delete(g.timePeerIDs, addr)
	} else {
		g.timePeerIDs[addr] = nodeID
	}
	g.mu.Unlock()
	if previous != "" && previous != nodeID {
		ref.ForgetPeer(previous)
	}
	if nodeID != "" && nodeID != g.nodeID {
		ref.ObservePeer(nodeID, sample, time.Since(started))
	}
}
func (g *GossipEngine) pushExchange(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
	req.TimeSample = g.timeSample()
	started := time.Now()
	resp, err := g.syncer.Push(ctx, addr, req)
	var sample TimeSample
	var nodeID string
	if resp != nil {
		sample, nodeID = resp.TimeSample, resp.NodeID
	}
	g.observeExchange(addr, nodeID, sample, started, err)
	return resp, err
}
func (g *GossipEngine) pullExchange(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	req.TimeSample = g.timeSample()
	started := time.Now()
	resp, err := g.syncer.(SnapshotSyncer).PullSnapshot(ctx, addr, req)
	var sample TimeSample
	var nodeID string
	if resp != nil {
		sample, nodeID = resp.TimeSample, resp.NodeID
	}
	g.observeExchange(addr, nodeID, sample, started, err)
	return resp, err
}

package state

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"
)

// No wire retention: measure the replication paths, not a spy's saved history.
type memorySyncer struct{ target *GossipEngine }

func (s memorySyncer) Push(ctx context.Context, _ string, req PushRequest) (*PushResponse, error) {
	return s.target.HandlePush(ctx, req)
}
func (s memorySyncer) Pull(ctx context.Context, addr string, req PullRequest) ([]Entry, error) {
	r, e := s.PullSnapshot(ctx, addr, req)
	if e != nil {
		return nil, e
	}
	return r.Entries, nil
}
func (s memorySyncer) PullSnapshot(ctx context.Context, _ string, req PullRequest) (*PullResponse, error) {
	return s.target.HandlePullResponse(ctx, req)
}

func memoryFixture(tb testing.TB, valueBytes, smallKeys int) (*GossipEngine, *MemoryStore, uint64) {
	tb.Helper()
	ctx := context.Background()
	a, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local"})
	if err != nil {
		tb.Fatal(err)
	}
	b, err := NewMemoryStore(MemoryStoreConfig{NodeID: "remote"})
	if err != nil {
		tb.Fatal(err)
	}
	payload := make([]byte, valueBytes)
	for i := 0; i < 7500+smallKeys; i++ {
		value := payload
		if i >= 7500 {
			value = []byte("x")
		}
		e := Entry{Key: fmt.Sprintf("key/%06d", i), Value: value, Version: StateVersion{WriterNodeID: "seed", HLC: Timestamp{WallMS: 1000}}, ChangeID: ChangeID{OriginNodeID: "seed", Counter: uint64(i + 1)}}
		if _, err := a.Apply(ctx, e); err != nil {
			tb.Fatal(err)
		}
		if _, err := b.Apply(ctx, e); err != nil {
			tb.Fatal(err)
		}
	}
	target, err := NewGossipEngine(GossipConfig{NodeID: "remote", Store: b, Syncer: noopSyncer{}})
	if err != nil {
		tb.Fatal(err)
	}
	// Two peer exchanges against the same independent replica keep the retained
	// payload at two copies while exercising per-peer digest reconstruction.
	g, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: a, Syncer: memorySyncer{target}, MaxPushPagesPerRound: 2, PeerProvider: NewStaticPeerProvider([]string{"peer-1", "peer-2"})})
	if err != nil {
		tb.Fatal(err)
	}
	return g, a, 2 * (7500*uint64(valueBytes) + uint64(smallKeys))
}

func BenchmarkReplicationRound(b *testing.B) {
	for _, size := range []int{28 << 10, 56 << 10} {
		b.Run(fmt.Sprintf("valueKiB=%d", size>>10), func(b *testing.B) {
			g, _, _ := memoryFixture(b, size, 200000)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := g.AntiEntropyOnce(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPushPageTail(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000, 200000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, err := NewMemoryStore(MemoryStoreConfig{NodeID: "push"})
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < n; i++ {
				if _, err := s.Put(context.Background(), fmt.Sprintf("k/%06d", i), []byte("x")); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.ChangesSinceBytes(context.Background(), 0, 4096); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestReplicationMemoryCeiling(t *testing.T) {
	g, _, stored := memoryFixture(t, 28<<10, 200000)
	// An absolute process-heap ceiling includes both stores and their metadata.
	// GOMEMLIMIT is soft; the sampled high-water mark is the actual assertion.
	ceiling := stored * 5 / 2
	old := debug.SetMemoryLimit(int64(ceiling) * 4 / 5)
	defer debug.SetMemoryLimit(old)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	sample := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		for p := peak.Load(); m.HeapInuse > p; p = peak.Load() {
			if peak.CompareAndSwap(p, m.HeapInuse) {
				break
			}
		}
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				sample()
			}
		}
	}()
	defer func() { close(stop); <-done }()
	for i := 0; i < 2; i++ {
		if err := g.SyncOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := g.AntiEntropyOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	sample()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(g)
	allocated := (after.TotalAlloc - before.TotalAlloc) / 2
	t.Logf("stored_values=%d allocation_per_round=%d peak_HeapInuse=%d ceiling=%d", stored, allocated, peak.Load(), ceiling)
	if allocated > 1<<30 {
		t.Errorf("round allocated %d bytes; want <=1 GiB independent of retained values", allocated)
	}
	if peak.Load() > ceiling {
		t.Errorf("peak HeapInuse %d exceeds 2.5 x stored value bytes (%d)", peak.Load(), ceiling)
	}
}

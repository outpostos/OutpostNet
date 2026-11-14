package state_test

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestTimeReferenceSourceQualityAndPeerSelection(t *testing.T) {
	system := int64(1_000_000)
	mono := time.Unix(0, 0)
	ref, err := state.NewTimeReference(state.TimeReferenceConfig{
		SystemClock:  state.PhysicalClockFunc(func() int64 { return system }),
		MonotonicNow: func() time.Time { return mono },
		DriftPPM:     1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s := ref.TimeStatus(); s.Quality != state.TimeUnsynced || !s.TimeUnknown {
		t.Fatalf("initial: %+v", s)
	}
	if !ref.ObservePeer("healthy", state.TimeSample{SentPhysicalMS: 31_537_000_000, TimeQuality: state.TimeSynced}, 0) {
		t.Fatal("sample refused")
	}
	if ref.NowMS() != 31_537_000_000 || !ref.TimeStatus().ClockSuspect {
		t.Fatalf("reference not selected: %+v", ref.TimeStatus())
	}
	mono = mono.Add(time.Second)
	if ref.NowMS() != 31_537_001_000 {
		t.Fatal("reference did not extrapolate monotonically")
	}
	if err := ref.UpdateSource(state.TimeSourceStatus{Synchronized: true, Uncertainty: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if ref.NowMS() != system || ref.TimeStatus().Quality != state.TimeSynced || ref.TimeStatus().ClockSuspect {
		t.Fatalf("synced node self-suspected: %+v", ref.TimeStatus())
	}
	if err := ref.UpdateSource(state.TimeSourceStatus{}); err != nil {
		t.Fatal(err)
	}
	if ref.TimeStatus().Quality != state.TimeHoldover {
		t.Fatal("missing holdover")
	}
	mono = mono.Add(2000 * time.Second)
	if ref.TimeStatus().Quality != state.TimeUnsynced {
		t.Fatal("holdover exceeded budget")
	}
}

func TestTimeReferenceDrivesHLCAndConservativeRetention(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			system := int64(31_537_000_000)
			mono := time.Unix(0, 0)
			ref, err := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return system }), MonotonicNow: func() time.Time { return mono }})
			if err != nil {
				t.Fatal(err)
			}
			ref.ObservePeer("healthy", state.TimeSample{SentPhysicalMS: 1_000_000, TimeQuality: state.TimeSynced}, 0)
			var store interface {
				state.Store
				CollectTombstones(context.Context) (state.TombstoneGCResult, error)
			}
			if durable {
				dir := t.TempDir()
				s, err := state.NewDurableStore(state.DurableStoreConfig{Dir: dir, NodeID: "ahead", TimeReference: ref, TombstoneRetention: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				store = s
				defer s.Close()
			} else {
				s, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "ahead", TimeReference: ref, TombstoneRetention: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				store = s
			}
			e, err := store.Delete(ctx, "deleted")
			if err != nil {
				t.Fatal(err)
			}
			if e.Version.HLC.WallMS != 1_000_000 {
				t.Fatalf("HLC uses system instead of reference: %+v", e)
			}
			result, err := store.CollectTombstones(ctx)
			if err != nil || result.Collected != 0 {
				t.Fatalf("premature collection: %+v %v", result, err)
			}
			mono = mono.Add(61 * time.Second)
			ref.ObservePeer("healthy", state.TimeSample{SentPhysicalMS: 1_061_000, TimeQuality: state.TimeSynced}, 0)
			result, err = store.CollectTombstones(ctx)
			if err != nil || result.Collected != 1 {
				t.Fatalf("retained after reference retention: %+v %v", result, err)
			}
		})
	}
}

func TestTimeReferenceBehindNodeCompletes410Resync(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			now := int64(31_537_000_000)
			healthyRef, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return now })})
			if err := healthyRef.UpdateSource(state.TimeSourceStatus{Synchronized: true}); err != nil {
				t.Fatal(err)
			}
			behindRef, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 1_000_000 })})
			healthy := referenceStore(t, durable, "healthy", healthyRef)
			behind := referenceStore(t, durable, "behind", behindRef)
			if _, err := healthy.Delete(ctx, "collected"); err != nil {
				t.Fatal(err)
			}
			now += 61_000
			if result, err := healthy.CollectTombstones(ctx); err != nil || result.Collected != 1 {
				t.Fatalf("collect: %+v %v", result, err)
			}
			if _, err := healthy.Put(ctx, "live", []byte("current")); err != nil {
				t.Fatal(err)
			}
			serving := referenceEngine(t, "healthy", healthy, nil)
			handler, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "healthy", Engine: serving, AuthToken: "test-only"})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			pulling := referenceEngine(t, "behind", behind, []string{server.URL})
			if err := pulling.AntiEntropyOnce(ctx); err != nil {
				t.Fatalf("behind node cannot finish 410 resync: %v", err)
			}
			if e, ok, err := behind.Get(ctx, "live"); err != nil || !ok || string(e.Value) != "current" {
				t.Fatalf("not caught up: %+v %v %v", e, ok, err)
			}
			if s := behindRef.TimeStatus(); !s.UsingPeerReference || !s.ClockSuspect {
				t.Fatalf("missing self-suspicion: %+v", s)
			}
			selfObserved := false
			for _, s := range pulling.ClockStatus() {
				if s.NodeID == "behind" && s.ClockSuspect && s.TimeQuality == state.TimeUnsynced {
					selfObserved = true
				}
				if s.NodeID != "behind" && s.ClockSuspect {
					t.Fatalf("healthy peer blamed: %+v", s)
				}
			}
			if !selfObserved {
				t.Fatal("local clock suspicion not exposed by engine")
			}
			e, err := behind.Put(ctx, "new", []byte("valid"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := healthy.Apply(ctx, e); err != nil {
				t.Fatalf("new HLC not reference-based: %v", err)
			}
			if h := behind.GCHorizon(); h.WallMS > behindRef.NowMS()+state.MaxClockOffsetMS || h != healthy.GCHorizon() {
				t.Fatalf("unsafe or missing adopted horizon: %+v", h)
			}
		})
	}
}

type referenceReplica interface {
	state.Store
	CollectTombstones(context.Context) (state.TombstoneGCResult, error)
	GCHorizon() state.Timestamp
}

func referenceStore(t *testing.T, durable bool, id string, ref *state.TimeReference) referenceReplica {
	t.Helper()
	if durable {
		s, err := state.NewDurableStore(state.DurableStoreConfig{Dir: t.TempDir(), NodeID: id, TimeReference: ref, TombstoneRetention: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	s, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: id, TimeReference: ref, TombstoneRetention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func referenceEngine(t *testing.T, id string, store state.Store, peers []string) *state.GossipEngine {
	t.Helper()
	wire, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: id, AuthToken: "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	g, err := state.NewGossipEngine(state.GossipConfig{NodeID: id, Store: store, Syncer: wire, PeerProvider: state.NewStaticPeerProvider(peers)})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestTimeReferenceTwoUnsyncedPeersDoNotBlameEachOther(t *testing.T) {
	ctx := context.Background()
	aheadRef, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 31_537_000_000 })})
	behindRef, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 1_000_000 })})
	ahead := referenceStore(t, false, "ahead", aheadRef)
	behind := referenceStore(t, false, "behind", behindRef)
	if _, err := ahead.Put(ctx, "future", []byte("value")); err != nil {
		t.Fatal(err)
	}
	serving := referenceEngine(t, "ahead", ahead, nil)
	handler, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "ahead", Engine: serving, AuthToken: "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	pulling := referenceEngine(t, "behind", behind, []string{server.URL, server.URL + "/"})
	if err := pulling.AntiEntropyOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !aheadRef.TimeStatus().TimeUnknown || !behindRef.TimeStatus().TimeUnknown {
		t.Fatal("single unsynced peer became a reference")
	}
	for _, s := range pulling.ClockStatus() {
		if s.ClockSuspect {
			t.Fatalf("unknown time blamed a node: %+v", s)
		}
	}
}

func TestTimeReferenceConsensusRTTAndExpiry(t *testing.T) {
	mono := time.Unix(0, 0)
	ref, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 1000 }), MonotonicNow: func() time.Time { return mono }})
	add := func(id string, ms int64, q state.TimeQuality, rtt time.Duration) bool {
		return ref.ObservePeer(id, state.TimeSample{SentPhysicalMS: ms, TimeQuality: q}, rtt)
	}
	add("a", 1_000_000, state.TimeUnsynced, 0)
	if !ref.TimeStatus().TimeUnknown {
		t.Fatal("one unsynced peer was enough")
	}
	add("b", 1_001_000, state.TimeUnsynced, 0)
	if ref.NowMS() != 1_000_500 {
		t.Fatalf("two agreeing peers: %+v", ref.TimeStatus())
	}
	add("c", 2_000_000, state.TimeUnsynced, 0)
	if ref.NowMS() != 1_000_500 {
		t.Fatal("outlier affected majority median")
	}
	add("d", 3_000_000, state.TimeUnsynced, 0)
	if !ref.TimeStatus().TimeUnknown {
		t.Fatal("half the peers is not a majority")
	}
	add("trusted", 4_000_000, state.TimeHoldover, time.Second)
	if ref.NowMS() != 4_000_500 {
		t.Fatalf("trusted peer did not take precedence with RTT midpoint: %+v", ref.TimeStatus())
	}
	if add("trusted", 8_000_000, state.TimeSynced, 2*time.Second+time.Nanosecond) {
		t.Fatal("slow sample accepted")
	}
	if !ref.TimeStatus().TimeUnknown {
		t.Fatal("slow exchange retained previous trusted sample")
	}
	add("trusted", 4_000_000, state.TimeSynced, 0)
	mono = mono.Add(31 * time.Second)
	if !ref.TimeStatus().TimeUnknown {
		t.Fatal("unreachable stale peers retained")
	}
	if s := ref.TimeStatus(); s.AcceptedSamples != 6 || s.DiscardedSamples != 1 {
		t.Fatalf("sample counters: %+v", s)
	}
}

func TestTimeReferenceSyncedReceiverBlamesAheadWriterOnly(t *testing.T) {
	for _, holdover := range []bool{false, true} {
		t.Run(fmt.Sprint(holdover), func(t *testing.T) {
			ctx := context.Background()
			good, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 1_000_000 })})
			if err := good.UpdateSource(state.TimeSourceStatus{Synchronized: true}); err != nil {
				t.Fatal(err)
			}
			if holdover {
				if err := good.UpdateSource(state.TimeSourceStatus{}); err != nil {
					t.Fatal(err)
				}
			}
			bad, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 1_600_000 })})
			a, b := referenceStore(t, false, "a", good), referenceStore(t, false, "b", bad)
			if _, err := b.Put(ctx, "bad", []byte("future")); err != nil {
				t.Fatal(err)
			}
			receiver := referenceEngine(t, "a", a, nil)
			handler, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "a", Engine: receiver, AuthToken: "test-only"})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			sender := referenceEngine(t, "b", b, []string{server.URL})
			if err := sender.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if good.TimeStatus().ClockSuspect || good.NowMS() != 1_000_000 {
				t.Fatalf("trusted node self-suspected: %+v", good.TimeStatus())
			}
			found := false
			for _, s := range receiver.ClockStatus() {
				if s.NodeID == "b" && s.ClockSuspect && s.RefusedEntries == 1 {
					found = true
				}
				if s.NodeID == "a" && s.ClockSuspect {
					t.Fatal("healthy receiver blamed itself")
				}
			}
			if !found {
				t.Fatal("future writer not observed")
			}
			if !bad.TimeStatus().ClockSuspect || bad.NowMS() > 1_000_010 {
				t.Fatalf("unsynced sender did not adopt healthy response: %+v", bad.TimeStatus())
			}
		})
	}
}

func TestTimeReferenceSlowHTTPResponseCannotSupplyReference(t *testing.T) {
	ctx := context.Background()
	healthy, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 31_537_000_000 })})
	if err := healthy.UpdateSource(state.TimeSourceStatus{Synchronized: true}); err != nil {
		t.Fatal(err)
	}
	behind, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 1_000_000 }), MaxSampleRTT: time.Millisecond})
	source, target := referenceStore(t, false, "source", healthy), referenceStore(t, false, "target", behind)
	engine := referenceEngine(t, "source", source, nil)
	handler, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "source", Engine: engine, AuthToken: "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	pulling := referenceEngine(t, "target", target, []string{server.URL})
	if err := pulling.AntiEntropyOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if s := behind.TimeStatus(); !s.TimeUnknown || s.DiscardedSamples != 1 {
		t.Fatalf("slow response used as reference: %+v", s)
	}
}

func TestTimeReferenceRejectsOverflowAndAcceptsEpoch(t *testing.T) {
	ref, _ := state.NewTimeReference(state.TimeReferenceConfig{})
	if ref.ObservePeer("overflow", state.TimeSample{SentPhysicalMS: math.MaxInt64, TimeQuality: state.TimeSynced}, time.Second) {
		t.Fatal("overflowing time sample accepted")
	}
	if !ref.ObservePeer("epoch", state.TimeSample{SentPhysicalMS: 0, TimeQuality: state.TimeSynced}, 0) {
		t.Fatal("1970 epoch rejected")
	}
}

func TestTimeReferenceWireMetadataAndProtocolUpgrade(t *testing.T) {
	ctx := context.Background()
	ref, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 31_537_000_000 })})
	s := referenceStore(t, false, "server", ref)
	g := referenceEngine(t, "server", s, nil)
	handler, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "server", Engine: g, AuthToken: "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, path := range []string{state.PushPath, state.PullPath} {
		body := `{"protocol_version":{"major":2,"minor":0},"policy_id":"` + s.Policy().ID() + `","source":"client","entries":[]}`
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-only")
		req.Header.Set("X-Outpost-Node-ID", "client")
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUpgradeRequired {
			t.Fatalf("old major accepted on %s: %d", path, response.StatusCode)
		}
	}
	wire, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "client", AuthToken: "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	pushed, err := wire.Push(ctx, server.URL, state.PushRequest{TimeSample: state.TimeSample{SentPhysicalMS: 9_000_000_000, TimeQuality: state.TimeSynced}})
	if err != nil {
		t.Fatal(err)
	}
	pulled, err := wire.PullSnapshot(ctx, server.URL, state.PullRequest{TimeSample: state.TimeSample{SentPhysicalMS: 9_000_000_000, TimeQuality: state.TimeSynced}})
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range []state.TimeSample{pushed.TimeSample, pulled.TimeSample} {
		if sample.SentPhysicalMS != 31_537_000_000 || sample.TimeQuality != state.TimeUnsynced {
			t.Fatalf("response lacks physical sample: %+v", sample)
		}
	}
	if !ref.TimeStatus().TimeUnknown {
		t.Fatal("unbounded inbound request became a reference")
	}
	ref.ObservePeer("trusted", state.TimeSample{SentPhysicalMS: 1_000_000, TimeQuality: state.TimeSynced}, 0)
	if sample := ref.Sample(); sample.SentPhysicalMS != 31_537_000_000 || sample.TimeQuality != state.TimeUnsynced {
		t.Fatalf("peer reference was advertised as physical time: %+v", sample)
	}
}

func TestTimeReferenceConcurrentHostUse(t *testing.T) {
	ref, _ := state.NewTimeReference(state.TimeReferenceConfig{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if i%2 == 0 {
					if err := ref.UpdateSource(state.TimeSourceStatus{Synchronized: j%2 == 0}); err != nil {
						t.Error(err)
					}
				} else {
					ref.ObservePeer(fmt.Sprint(i), state.TimeSample{SentPhysicalMS: time.Now().UnixMilli(), TimeQuality: state.TimeSynced}, time.Millisecond)
				}
				ref.Now()
				ref.RetentionMS()
				ref.Sample()
				ref.TimeStatus()
			}
		}(i)
	}
	wg.Wait()
}

func TestTimeReferenceRetentionWaitsForSlowerSystemClock(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			system := int64(1_000_000)
			mono := time.Unix(0, 0)
			ref, _ := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return system }), MonotonicNow: func() time.Time { return mono }})
			ref.ObservePeer("healthy", state.TimeSample{SentPhysicalMS: 31_537_000_000, TimeQuality: state.TimeSynced}, 0)
			s := referenceStore(t, durable, "behind", ref)
			if _, err := s.Delete(ctx, "retained"); err != nil {
				t.Fatal(err)
			}
			mono = mono.Add(61 * time.Second)
			ref.ObservePeer("healthy", state.TimeSample{SentPhysicalMS: 31_537_061_000, TimeQuality: state.TimeSynced}, 0)
			if result, err := s.CollectTombstones(ctx); err != nil || result.Collected != 0 {
				t.Fatalf("ignored slower system retention: %+v %v", result, err)
			}
			system = 31_537_061_000
			if result, err := s.CollectTombstones(ctx); err != nil || result.Collected != 1 {
				t.Fatalf("did not collect after both clocks passed retention: %+v %v", result, err)
			}
		})
	}
}

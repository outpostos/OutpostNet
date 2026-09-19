package state

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDurableStoreRecoversEntriesTombstonesCountersAndConsumerStatus(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &fixedPhysicalClock{now: 5_000}
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: clock,
	})

	created, err := store.Put(ctx, "opaque/entity-1", []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	deleted, err := store.Delete(ctx, "opaque/entity-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.ChangeID.Counter <= created.ChangeID.Counter {
		t.Fatalf("delete counter did not advance: created=%+v deleted=%+v", created.ChangeID, deleted.ChangeID)
	}
	if status := store.LocalStatus(); !status.Ready || status.CurrentPosition != deleted.Position {
		t.Fatalf("unexpected local status: %+v deleted position %d", status, deleted.Position)
	}
	if status := store.CheckConsumerPosition(0); !status.NeedsSnapshot || status.CanResume {
		t.Fatalf("zero consumer position should require snapshot: %+v", status)
	}
	if status := store.CheckConsumerPosition(deleted.Position); !status.CanResume || status.NeedsSnapshot {
		t.Fatalf("current consumer position should resume: %+v", status)
	}
	if status := store.CheckConsumerPosition(deleted.Position + 100); !status.NeedsSnapshot || status.CanResume {
		t.Fatalf("future consumer position should require resync: %+v", status)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clock.now = 1_000
	recovered := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: clock,
	})
	defer recovered.Close()
	got := mustGet(t, recovered, "opaque/entity-1")
	if !got.Deleted || got.ChangeID != deleted.ChangeID || got.Version != deleted.Version {
		t.Fatalf("recovered tombstone mismatch: got %+v want %+v", got, deleted)
	}
	after, err := recovered.Put(ctx, "opaque/entity-2", []byte("after-restart"))
	if err != nil {
		t.Fatalf("Put after restart: %v", err)
	}
	if after.ChangeID.Counter <= deleted.ChangeID.Counter {
		t.Fatalf("restarted store reused or regressed change counter: after=%+v deleted=%+v", after.ChangeID, deleted.ChangeID)
	}
	if after.Version.HLC.Compare(deleted.Version.HLC) <= 0 {
		t.Fatalf("restarted store HLC did not advance with backward physical clock: after=%+v deleted=%+v", after.Version.HLC, deleted.Version.HLC)
	}
}

func TestDurableStoreDoesNotNotifyOrExposeFailedCommits(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fail := atomic.Bool{}
	fail.Store(true)
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: &fixedPhysicalClock{now: 1_000},
		appendHook: func(durableRecord) error {
			if fail.Swap(false) {
				return errors.New("injected append failure")
			}
			return nil
		},
	})
	defer store.Close()

	observed := make(chan Entry, 2)
	stop := store.Observe(func(entry Entry) {
		observed <- entry
	})
	defer stop()

	if _, err := store.Put(ctx, "opaque/local", []byte("lost")); err == nil {
		t.Fatal("expected local append failure")
	}
	if _, ok, err := store.Get(ctx, "opaque/local"); err != nil || ok {
		t.Fatalf("failed local commit became visible: ok=%v err=%v", ok, err)
	}
	assertNoEvent(t, observed)

	committed, err := store.Put(ctx, "opaque/local", []byte("committed"))
	if err != nil {
		t.Fatalf("Put after failure: %v", err)
	}
	if committed.ChangeID.Counter != 2 {
		t.Fatalf("expected explicit counter gap after failed append, got %+v", committed.ChangeID)
	}
	if event := <-observed; string(event.Value) != "committed" {
		t.Fatalf("observer saw wrong committed event: %+v", event)
	}

	fail.Store(true)
	remote := newEntryForTest("opaque/remote", []byte("remote"), "node-b", 1, Timestamp{WallMS: 2_000})
	if applied, err := store.Apply(ctx, remote); err == nil || applied {
		t.Fatalf("expected remote append failure, applied=%v err=%v", applied, err)
	}
	if _, ok, err := store.Get(ctx, "opaque/remote"); err != nil || ok {
		t.Fatalf("failed remote apply became visible: ok=%v err=%v", ok, err)
	}
	assertNoEvent(t, observed)
}

func TestDurableStoreRejectsConcurrentOpenIdentityMismatchAndBadFiles(t *testing.T) {
	dir := t.TempDir()
	store := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a"})
	if _, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "node-a"}); !errors.Is(err, ErrDurableStoreLocked) {
		t.Fatalf("expected concurrent open lock error, got %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "node-b"}); err == nil || !strings.Contains(err.Error(), "node id mismatch") {
		t.Fatalf("expected node identity mismatch, got %v", err)
	}

	t.Run("path is not directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatalf("write file path: %v", err)
		}
		if _, err := NewDurableStore(DurableStoreConfig{Dir: path, NodeID: "node-a"}); err == nil {
			t.Fatal("expected file path open to fail")
		}
	})
	t.Run("corrupt manifest", func(t *testing.T) {
		badDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(badDir, durableManifestFile), []byte("{bad json"), 0o600); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		if _, err := NewDurableStore(DurableStoreConfig{Dir: badDir, NodeID: "node-a"}); err == nil || !strings.Contains(err.Error(), "decode durable state manifest") {
			t.Fatalf("expected manifest decode error, got %v", err)
		}
	})
	t.Run("unsupported format", func(t *testing.T) {
		badDir := t.TempDir()
		data := []byte(`{"format_version":99,"node_id":"node-a","policy_id":"outpostnet.state.hlc-lww/v1"}`)
		if err := os.WriteFile(filepath.Join(badDir, durableManifestFile), data, 0o600); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		if _, err := NewDurableStore(DurableStoreConfig{Dir: badDir, NodeID: "node-a"}); err == nil || !strings.Contains(err.Error(), "unsupported durable state format") {
			t.Fatalf("expected format error, got %v", err)
		}
	})
	t.Run("corrupt log", func(t *testing.T) {
		badDir := makeDurableDir(t, "node-a")
		if err := os.WriteFile(filepath.Join(badDir, durableLogFile), []byte("{bad json}\n"), 0o600); err != nil {
			t.Fatalf("write log: %v", err)
		}
		if _, err := NewDurableStore(DurableStoreConfig{Dir: badDir, NodeID: "node-a"}); err == nil || !strings.Contains(err.Error(), "decode durable state log") {
			t.Fatalf("expected log decode error, got %v", err)
		}
	})
	t.Run("incomplete tail", func(t *testing.T) {
		badDir := makeDurableDir(t, "node-a")
		if err := os.WriteFile(filepath.Join(badDir, durableLogFile), []byte(`{"format_version":1`), 0o600); err != nil {
			t.Fatalf("write log: %v", err)
		}
		if _, err := NewDurableStore(DurableStoreConfig{Dir: badDir, NodeID: "node-a"}); !errors.Is(err, ErrDurableLogTruncated) {
			t.Fatalf("expected incomplete tail error, got %v", err)
		}
	})
}

func TestDurableGossipPersistsPeerProgressOnlyAfterSuccessfulSave(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	failProgress := atomic.Bool{}
	failProgress.Store(true)
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: &fixedPhysicalClock{now: 1_000},
		appendHook: func(record durableRecord) error {
			if record.Type == durableRecordPeerProgress && failProgress.Swap(false) {
				return errors.New("injected progress failure")
			}
			return nil
		},
	})
	defer store.Close()
	entry, err := store.Put(ctx, "opaque/progress", []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	syncer := &recordingSyncer{}
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       syncer,
		PeerProvider: NewStaticPeerProvider([]string{"peer-b"}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}

	err = engine.SyncOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "injected progress failure") {
		t.Fatalf("expected progress save failure, got %v", err)
	}
	if got, err := store.PeerProgress(ctx, "peer-b"); err != nil || got != 0 {
		t.Fatalf("failed progress save advanced cursor: got %d err=%v", got, err)
	}
	if len(syncer.pushes) != 1 || len(syncer.pushes[0]) != 1 {
		t.Fatalf("expected first push to contain entry, got %#v", syncer.pushes)
	}

	clearPeerBackoffForTest(engine, "peer-b")
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatalf("retry SyncOnce: %v", err)
	}
	if len(syncer.pushes) != 2 || syncer.pushes[1][0].ChangeID != entry.ChangeID {
		t.Fatalf("retry did not safely replay unconfirmed entry: %#v", syncer.pushes)
	}
	if got, err := store.PeerProgress(ctx, "peer-b"); err != nil || got != entry.Position {
		t.Fatalf("progress not saved after acknowledged retry: got %d want %d err=%v", got, entry.Position, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	recovered := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a"})
	defer recovered.Close()
	if got, err := recovered.PeerProgress(ctx, "peer-b"); err != nil || got != entry.Position {
		t.Fatalf("recovered peer progress mismatch: got %d want %d err=%v", got, entry.Position, err)
	}
}

func TestDurableHTTPNodesColdRestartPartitionAndTombstoneRecovery(t *testing.T) {
	ctx := context.Background()
	token := "durable-http-token"
	dirA := t.TempDir()
	dirB := t.TempDir()
	clockA := &fixedPhysicalClock{now: 1_000}
	clockB := &fixedPhysicalClock{now: 1_000}
	a := newDurableHTTPTestNode(t, "node-a", token, dirA, clockA, nil)
	b := newDurableHTTPTestNode(t, "node-b", token, dirB, clockB, nil)
	defer closeHTTPTestNode(t, a)
	defer closeHTTPTestNode(t, b)
	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})

	live, err := a.store.Put(ctx, "opaque/target", []byte("live-from-a"))
	if err != nil {
		t.Fatalf("node-a Put: %v", err)
	}
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-a SyncOnce: %v", err)
	}
	if got := mustGet(t, b.store, "opaque/target"); string(got.Value) != "live-from-a" || got.ChangeID != live.ChangeID {
		t.Fatalf("node-b did not receive live value: %+v", got)
	}

	b.server.Close()
	if err := b.store.(*DurableStore).Close(); err != nil {
		t.Fatalf("close node-b store before restart: %v", err)
	}
	b = newDurableHTTPTestNode(t, "node-b", token, dirB, clockB, nil)
	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})
	if got := mustGet(t, b.store, "opaque/target"); string(got.Value) != "live-from-a" || got.ChangeID != live.ChangeID {
		t.Fatalf("node-b cold restart did not recover live value: %+v", got)
	}

	a.peers.Store(nil)
	b.peers.Store(nil)
	clockA.now = 3_000
	deleted, err := a.store.Delete(ctx, "opaque/target")
	if err != nil {
		t.Fatalf("node-a Delete: %v", err)
	}
	clockB.now = 2_000
	if _, err := b.store.Put(ctx, "opaque/target", []byte("partition-live-from-b")); err != nil {
		t.Fatalf("node-b partition Put: %v", err)
	}

	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-a recovery SyncOnce: %v", err)
	}
	if err := b.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-b recovery SyncOnce: %v", err)
	}
	if err := a.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("node-a AntiEntropyOnce: %v", err)
	}
	if err := b.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("node-b AntiEntropyOnce: %v", err)
	}
	for _, node := range []*httpTestNode{a, b} {
		entry := mustGet(t, node.store, "opaque/target")
		if !entry.Deleted || entry.ChangeID != deleted.ChangeID {
			t.Fatalf("%s resurrected deleted state: %+v want delete %+v", node.id, entry, deleted)
		}
	}
}

func TestDurableHTTPTransportFailureRecoveryReplaysUnconfirmedChanges(t *testing.T) {
	ctx := context.Background()
	token := "durable-failure-token"
	dirA := t.TempDir()
	dirB := t.TempDir()
	clockA := &fixedPhysicalClock{now: 1_000}
	clockB := &fixedPhysicalClock{now: 1_000}
	a := newDurableHTTPTestNode(t, "node-a", token, dirA, clockA, nil)
	breaker := &atomic.Bool{}
	b := newDurableHTTPTestNode(t, "node-b", token, dirB, clockB, breaker)
	defer closeHTTPTestNode(t, a)
	defer closeHTTPTestNode(t, b)
	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})

	live, err := a.store.Put(ctx, "opaque/target", []byte("live-from-a"))
	if err != nil {
		t.Fatalf("node-a Put live: %v", err)
	}
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("initial SyncOnce: %v", err)
	}
	assertStoredValue(t, b.store, "opaque/target", "live-from-a")

	breaker.Store(true)
	clockA.now = 2_000
	if _, err := a.store.Put(ctx, "opaque/target", []byte("unconfirmed-update")); err != nil {
		t.Fatalf("node-a Put unconfirmed: %v", err)
	}
	clockA.now = 3_000
	deleted, err := a.store.Delete(ctx, "opaque/target")
	if err != nil {
		t.Fatalf("node-a Delete: %v", err)
	}
	if err := a.engine.SyncOnce(ctx); err == nil {
		t.Fatal("expected transport failure during SyncOnce")
	}
	if got, err := a.store.(*DurableStore).PeerProgress(ctx, b.server.URL); err != nil || got != live.Position {
		t.Fatalf("failed push advanced durable cursor: got %d want %d err=%v", got, live.Position, err)
	}

	a.server.Close()
	if err := a.store.(*DurableStore).Close(); err != nil {
		t.Fatalf("close node-a before recovery: %v", err)
	}
	a = newDurableHTTPTestNode(t, "node-a", token, dirA, clockA, nil)
	a.peers.Store([]string{b.server.URL})
	breaker.Store(false)
	progress, err := a.store.(*DurableStore).PeerProgress(ctx, b.server.URL)
	if err != nil {
		t.Fatalf("recovered peer progress: %v", err)
	}
	if progress != live.Position {
		t.Fatalf("recovered peer progress = %d, want last confirmed live position %d", progress, live.Position)
	}
	changes, err := a.store.ChangesSince(ctx, progress, 0)
	if err != nil {
		t.Fatalf("recovered ChangesSince: %v", err)
	}
	if len(changes) != 1 || changes[0].ChangeID != deleted.ChangeID {
		t.Fatalf("recovered store will not replay unconfirmed delete: changes=%+v want %+v", changes, deleted)
	}
	clearPeerBackoffForTest(a.engine, b.server.URL)
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("recovered SyncOnce: %v", err)
	}
	entry := mustGet(t, b.store, "opaque/target")
	if !entry.Deleted || entry.ChangeID != deleted.ChangeID || entry.Version != deleted.Version {
		t.Fatalf("recovered peer did not receive unconfirmed tombstone: got %+v want %+v", entry, deleted)
	}
}

func TestDurableProgressDoesNotPermanentlySkipPeerRestartAtSameAddress(t *testing.T) {
	ctx := context.Background()
	token := "durable-same-address-token"
	a := newDurableHTTPTestNode(t, "node-a", token, t.TempDir(), &fixedPhysicalClock{now: 1_000}, nil)
	switcher := &switchingHandler{}
	b := newSwitchableHTTPTestNode(t, "node-b", token, &fixedPhysicalClock{now: 1_000}, a.server.URL, switcher)
	defer b.server.Close()
	a.peers.Store([]string{b.server.URL})

	live, err := a.store.Put(ctx, "opaque/restart", []byte("live"))
	if err != nil {
		t.Fatalf("node-a Put: %v", err)
	}
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-a SyncOnce: %v", err)
	}
	assertStoredValue(t, b.store, "opaque/restart", "live")
	if got, err := a.store.(*DurableStore).PeerProgress(ctx, b.server.URL); err != nil || got != live.Position {
		t.Fatalf("node-a durable progress mismatch: got %d want %d err=%v", got, live.Position, err)
	}

	b = replaceSwitchableHTTPTestNode(t, b, token, &fixedPhysicalClock{now: 1_000}, a.server.URL, switcher)
	if _, ok, err := b.store.Get(ctx, "opaque/restart"); err != nil || ok {
		t.Fatalf("replacement peer unexpectedly has state before anti-entropy: ok=%v err=%v", ok, err)
	}
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-a SyncOnce after peer restart: %v", err)
	}
	if _, ok, err := b.store.Get(ctx, "opaque/restart"); err != nil || ok {
		t.Fatalf("incremental push should be skipped by saved address cursor before anti-entropy: ok=%v err=%v", ok, err)
	}
	if err := b.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("replacement peer AntiEntropyOnce: %v", err)
	}
	entry := mustGet(t, b.store, "opaque/restart")
	if string(entry.Value) != "live" || entry.ChangeID != live.ChangeID {
		t.Fatalf("anti-entropy did not refill restarted peer: got %+v want %+v", entry, live)
	}
}

func TestDurableStoreColdStartsInActualSubprocess(t *testing.T) {
	if os.Getenv("OUTPOST_STATE_DURABLE_CHILD") == "1" {
		runDurableStoreColdStartChild(t)
		return
	}
	ctx := context.Background()
	dir := t.TempDir()
	clock := &fixedPhysicalClock{now: 5_000}
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: clock,
	})
	deleted, err := store.Delete(ctx, "opaque/subprocess")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestDurableStoreColdStartsInActualSubprocess")
	cmd.Env = append(os.Environ(),
		"OUTPOST_STATE_DURABLE_CHILD=1",
		"OUTPOST_STATE_DURABLE_DIR="+dir,
		fmt.Sprintf("OUTPOST_STATE_DURABLE_COUNTER=%d", deleted.ChangeID.Counter),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child cold start failed: %v\n%s", err, output)
	}

	recovered := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a"})
	defer recovered.Close()
	entry := mustGet(t, recovered, "opaque/subprocess-child")
	if string(entry.Value) != "child-write" || entry.ChangeID.Counter <= deleted.ChangeID.Counter {
		t.Fatalf("child write not recovered or counter regressed: got %+v deleted %+v", entry, deleted)
	}
}

func runDurableStoreColdStartChild(t *testing.T) {
	t.Helper()
	dir := os.Getenv("OUTPOST_STATE_DURABLE_DIR")
	var previous uint64
	if _, err := fmt.Sscanf(os.Getenv("OUTPOST_STATE_DURABLE_COUNTER"), "%d", &previous); err != nil {
		t.Fatalf("parse previous counter: %v", err)
	}
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: &fixedPhysicalClock{now: 1_000},
	})
	defer store.Close()
	if got := mustGet(t, store, "opaque/subprocess"); !got.Deleted {
		t.Fatalf("child did not recover parent tombstone: %+v", got)
	}
	entry, err := store.Put(context.Background(), "opaque/subprocess-child", []byte("child-write"))
	if err != nil {
		t.Fatalf("child Put: %v", err)
	}
	if entry.ChangeID.Counter <= previous {
		t.Fatalf("child reused previous counter: got %+v previous %d", entry.ChangeID, previous)
	}
}

func newDurableHTTPTestNode(t *testing.T, id, token, dir string, clock *fixedPhysicalClock, breaker *atomic.Bool) *httpTestNode {
	t.Helper()
	node := &httpTestNode{id: id}
	t.Cleanup(func() {
		closeHTTPTestNode(t, node)
	})
	node.store = mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        id,
		PhysicalClock: clock,
	})
	var err error
	node.syncer, err = NewHTTPSyncer(HTTPSyncerConfig{NodeID: id, AuthToken: token, PolicyID: node.store.Policy().ID()})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	node.engine, err = NewGossipEngine(GossipConfig{
		NodeID:       id,
		Store:        node.store,
		Syncer:       node.syncer,
		PeerProvider: PeerProviderFunc(func() []string { return node.peers.Load() }),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	node.server = newHTTPServerForNode(t, node, token, breaker)
	node.syncer.client = node.server.Client()
	return node
}

func newSwitchableHTTPTestNode(t *testing.T, id, token string, clock *fixedPhysicalClock, peer string, switcher *switchingHandler) *httpTestNode {
	t.Helper()
	node := &httpTestNode{id: id}
	node.store = mustStore(t, id, NewHLCLWWPolicy(), clock)
	var err error
	node.syncer, err = NewHTTPSyncer(HTTPSyncerConfig{NodeID: id, AuthToken: token, PolicyID: node.store.Policy().ID()})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	node.engine, err = NewGossipEngine(GossipConfig{
		NodeID:       id,
		Store:        node.store,
		Syncer:       node.syncer,
		PeerProvider: NewStaticPeerProvider([]string{peer}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{
		NodeID:          id,
		Engine:          node.engine,
		AuthToken:       token,
		MaxRequestBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	switcher.Store(handler)
	node.server = httptest.NewServer(switcher)
	node.syncer.client = node.server.Client()
	return node
}

func replaceSwitchableHTTPTestNode(t *testing.T, old *httpTestNode, token string, clock *fixedPhysicalClock, peer string, switcher *switchingHandler) *httpTestNode {
	t.Helper()
	node := &httpTestNode{id: old.id, server: old.server}
	node.store = mustStore(t, old.id, NewHLCLWWPolicy(), clock)
	var err error
	node.syncer, err = NewHTTPSyncer(HTTPSyncerConfig{Client: old.server.Client(), NodeID: old.id, AuthToken: token, PolicyID: node.store.Policy().ID()})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	node.engine, err = NewGossipEngine(GossipConfig{
		NodeID:       old.id,
		Store:        node.store,
		Syncer:       node.syncer,
		PeerProvider: NewStaticPeerProvider([]string{peer}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{
		NodeID:          old.id,
		Engine:          node.engine,
		AuthToken:       token,
		MaxRequestBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	switcher.Store(handler)
	return node
}

type switchingHandler struct {
	handler atomic.Value
}

func (h *switchingHandler) Store(handler http.Handler) {
	h.handler.Store(handler)
}

func (h *switchingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler, _ := h.handler.Load().(http.Handler)
	if handler == nil {
		http.Error(w, "handler not ready", http.StatusServiceUnavailable)
		return
	}
	handler.ServeHTTP(w, r)
}

func closeHTTPTestNode(t *testing.T, node *httpTestNode) {
	t.Helper()
	if node == nil {
		return
	}
	if node.server != nil {
		node.server.Close()
		node.server = nil
	}
	if store, ok := node.store.(*DurableStore); ok {
		if err := store.Close(); err != nil && !strings.Contains(err.Error(), "invalid argument") {
			t.Fatalf("close durable store for %s: %v", node.id, err)
		}
	}
}

func mustDurableStore(t *testing.T, cfg DurableStoreConfig) *DurableStore {
	t.Helper()
	store, err := NewDurableStore(cfg)
	if err != nil {
		t.Fatalf("NewDurableStore: %v", err)
	}
	return store
}

func makeDurableDir(t *testing.T, nodeID string) string {
	t.Helper()
	dir := t.TempDir()
	store := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: nodeID})
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

func assertNoEvent(t *testing.T, observed <-chan Entry) {
	t.Helper()
	select {
	case event := <-observed:
		t.Fatalf("unexpected observer event: %+v", event)
	default:
	}
}

type recordingSyncer struct {
	pushes [][]Entry
}

func (s *recordingSyncer) Push(_ context.Context, _ string, req PushRequest) (*PushResponse, error) {
	copied := make([]Entry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		copied = append(copied, copyEntry(entry))
	}
	s.pushes = append(s.pushes, copied)
	return &PushResponse{ProtocolVersion: StateProtocolVersion, PolicyID: NewHLCLWWPolicy().ID(), Accepted: len(req.Entries)}, nil
}

func (s *recordingSyncer) Pull(context.Context, string, PullRequest) ([]Entry, error) {
	return nil, nil
}

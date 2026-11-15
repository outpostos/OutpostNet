package state

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Two actual processes exchange state over authenticated HTTP. A file-controlled
// partition lasts beyond retention; the warning precedes expiry and reunion
// preserves the exact lost bytes in the archive rather than in live replicas.
func TestResyncLossAcrossProcesses(t *testing.T) {
	if os.Getenv("OUTPOST_RESYNC_CHILD") == "1" {
		runResyncLossPeer(t)
		return
	}
	ctx := context.Background()
	dir := t.TempDir()
	partition := filepath.Join(dir, "partition")
	if err := os.WriteFile(partition, []byte("partition"), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	cmd := exec.Command(os.Args[0], "-test.run=^TestResyncLossAcrossProcesses$")
	cmd.Env = append(os.Environ(), "OUTPOST_RESYNC_CHILD=1", "OUTPOST_RESYNC_DIR="+dir)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	var addr []byte
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		addr, err = os.ReadFile(filepath.Join(dir, "ready"))
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(addr) == 0 {
		t.Fatal("child did not start")
	}
	clock := &fixedPhysicalClock{now: 1_000_000}
	local, err := NewDurableStore(DurableStoreConfig{Dir: filepath.Join(dir, "local"), NodeID: "local", PhysicalClock: clock, TombstoneRetention: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	value := []byte{0, 255, 128, 10, 0}
	lost, err := local.Put(ctx, "local-only", value)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := local.Delete(ctx, "local-delete")
	if err != nil {
		t.Fatal(err)
	}
	forwarded := lost
	forwarded.Key = "forwarded"
	forwarded.Version.WriterNodeID = "third"
	forwarded.ChangeID.OriginNodeID = "third"
	if _, err := local.Apply(ctx, forwarded); err != nil {
		t.Fatal(err)
	}
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "local", AuthToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: local, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{string(addr)}), BackoffBase: time.Nanosecond, BackoffMax: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := engine.SyncOnce(ctx); err == nil {
		t.Fatal("partition did not fail exchange")
	}
	time.Sleep(850 * time.Millisecond)
	_ = engine.SyncOnce(ctx)
	status, err := engine.ReplicationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Peers[0].ResyncLossLimitApproaching || time.Since(started) >= time.Second {
		t.Fatalf("warning missing before retention: %+v", status.Peers[0])
	}
	time.Sleep(200 * time.Millisecond)
	if err := os.Remove(partition); err != nil {
		t.Fatal(err)
	}
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = engine.ReplicationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Peers[0].ResyncLossLimitApproaching || status.Local.Resync.PurgedLocalOrigin != 2 || status.Local.Resync.PurgedForwarded != 1 {
		t.Fatalf("reunion status: %+v", status)
	}
	records := readResyncArchive(t, filepath.Join(dir, "local", status.Local.Resync.LatestArchivePath))
	if len(records) != 3 {
		t.Fatalf("archive count: %d", len(records))
	}
	for _, r := range records {
		if r.Peer != string(addr) {
			t.Fatalf("peer: %q", r.Peer)
		}
		switch r.Key {
		case lost.Key, forwarded.Key:
			if !bytes.Equal(r.Value, value) {
				t.Fatal("value bytes changed")
			}
		case deleted.Key:
			if !r.Deleted || r.ChangeID != deleted.ChangeID {
				t.Fatal("tombstone changed")
			}
		default:
			t.Fatalf("unexpected key %q", r.Key)
		}
		assertAbsent(t, local, r.Key)
	}
	if err := engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if entries, err := local.ListEntries(ctx); err != nil || len(entries) != 0 {
		t.Fatalf("archive replayed/replicated: %+v %v", entries, err)
	}
}

func runResyncLossPeer(t *testing.T) {
	dir := os.Getenv("OUTPOST_RESYNC_DIR")
	clock := &fixedPhysicalClock{now: 1_000_100}
	store, err := NewDurableStore(DurableStoreConfig{Dir: filepath.Join(dir, "peer"), NodeID: "peer", PhysicalClock: clock, TombstoneRetention: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Delete(context.Background(), "gc"); err != nil {
		t.Fatal(err)
	}
	clock.now += 2000
	if _, err := store.CollectTombstones(context.Background()); err != nil {
		t.Fatal(err)
	}
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "peer", AuthToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGossipEngine(GossipConfig{NodeID: "peer", Store: store, Syncer: syncer})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "peer", Engine: engine, AuthToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("http://"+listener.Addr().String()), 0600); err != nil {
		t.Fatal(err)
	}
	err = http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat(filepath.Join(dir, "partition")); err == nil {
			http.Error(w, "partition", http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Fatal(fmt.Errorf("child server stopped: %w", err))
}

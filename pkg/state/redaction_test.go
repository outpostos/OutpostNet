package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"sync"
	"testing"
)

const testURLPassword = "url-password"

// lockedBuffer is a goroutine-safe log sink.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureStateLogs routes the default slog logger, at debug level, into a
// buffer for the rest of the test.
func captureStateLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	sink := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.MultiWriter(sink), &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return sink
}

func credentialedPeer(node string) string {
	return "http://ops:" + testURLPassword + "@" + node
}

// credentialedLoopSyncer reaches loopNet engines through credentialed peer
// URLs. Like a third-party Syncer, its own errors quote the address verbatim.
type credentialedLoopSyncer struct {
	inner        loopSyncer
	failSnapshot bool
}

func (s *credentialedLoopSyncer) target(addr string) (string, error) {
	u, err := neturl.Parse(addr)
	if err != nil {
		return "", err
	}
	return u.Hostname(), nil
}

func (s *credentialedLoopSyncer) Push(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
	node, err := s.target(addr)
	if err != nil {
		return nil, err
	}
	resp, err := s.inner.Push(ctx, node, req)
	if err != nil {
		return nil, fmt.Errorf("push to %s: %w", addr, err)
	}
	return resp, nil
}

func (s *credentialedLoopSyncer) Pull(ctx context.Context, addr string, req PullRequest) ([]Entry, error) {
	node, err := s.target(addr)
	if err != nil {
		return nil, err
	}
	entries, err := s.inner.Pull(ctx, node, req)
	if err != nil {
		return nil, fmt.Errorf("pull from %s: %w", addr, err)
	}
	return entries, nil
}

func (s *credentialedLoopSyncer) PullSnapshot(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	if s.failSnapshot {
		return nil, fmt.Errorf("snapshot from %s: connection reset", addr)
	}
	node, err := s.target(addr)
	if err != nil {
		return nil, err
	}
	return s.inner.PullSnapshot(ctx, node, req)
}

func assertNoPassword(t *testing.T, what, text string) {
	t.Helper()
	if strings.Contains(text, testURLPassword) {
		t.Fatalf("%s leaks the peer URL password:\n%s", what, text)
	}
}

func assertLogged(t *testing.T, logs *lockedBuffer, message string) {
	t.Helper()
	if !strings.Contains(logs.String(), message) {
		t.Fatalf("expected log %q, got:\n%s", message, logs.String())
	}
}

// Review 10-B-Net H1: no pkg/state log line or exported-API error may carry
// the password of a credentialed peer address.
func TestGossipLogsAndErrorsNeverCarryPeerURLPassword(t *testing.T) {
	ctx := context.Background()
	logs := captureStateLogs(t)
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()

	aStore := mustGCMemoryStore(t, "node-a", clock, testRetention)
	lStore := mustGCMemoryStore(t, "node-l", clock, testRetention)
	stale, err := lStore.Put(ctx, "doomed", []byte("stale"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aStore.Apply(ctx, stale); err != nil {
		t.Fatal(err)
	}
	clock.now += 1_000
	if _, err := aStore.Delete(ctx, "doomed"); err != nil {
		t.Fatal(err)
	}
	clock.now += 2 * retentionMS()
	if result, err := aStore.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("GC: %+v err=%v", result, err)
	}

	newEngine := func(id string, store *MemoryStore, syncer Syncer, peer string) *GossipEngine {
		t.Helper()
		engine, err := NewGossipEngine(GossipConfig{
			NodeID:       id,
			Store:        store,
			Syncer:       syncer,
			PeerProvider: NewStaticPeerProvider([]string{peer}),
		})
		if err != nil {
			t.Fatal(err)
		}
		net.mu.Lock()
		net.engines[id] = engine
		net.mu.Unlock()
		return engine
	}
	aSyncer := &credentialedLoopSyncer{inner: loopSyncer{net: net, from: "node-a"}}
	lSyncer := &credentialedLoopSyncer{inner: loopSyncer{net: net, from: "node-l"}}
	a := newEngine("node-a", aStore, aSyncer, credentialedPeer("node-l"))
	l := newEngine("node-l", lStore, lSyncer, credentialedPeer("node-a"))

	// (1) node-a pulls the stale value from the lagging peer and refuses it
	// below its GC horizon.
	if err := a.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("node-a anti-entropy: %v", err)
	}
	assertLogged(t, logs, "refused pulled entries below the local GC horizon")

	// (2) A resync whose snapshot pull fails returns and logs an error naming
	// the peer.
	lSyncer.failSnapshot = true
	err = l.AntiEntropyOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing snapshot resync to be reported")
	}
	assertNoPassword(t, "AntiEntropyOnce error (resync)", err.Error())
	assertLogged(t, logs, "peer sync failed")

	// The next round skips the peer in backoff.
	if err := l.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("round in backoff: %v", err)
	}
	if err := l.SyncOnce(ctx); err != nil {
		t.Fatalf("push round in backoff: %v", err)
	}
	assertLogged(t, logs, "peer in backoff")

	// (3) The resync succeeds and is logged.
	lSyncer.failSnapshot = false
	clearPeerBackoffForTest(l, credentialedPeer("node-a"))
	if err := l.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("node-l resync: %v", err)
	}
	assertLogged(t, logs, "resynced from peer snapshot")
	assertAbsent(t, lStore, "doomed")

	// (4) Plain exchange failures from a Syncer that quotes the address.
	net.setBlocked("node-a", "node-l", true)
	if _, err := lStore.Put(ctx, "fresh", []byte("v")); err != nil {
		t.Fatal(err)
	}
	clearPeerBackoffForTest(l, credentialedPeer("node-a"))
	err = l.SyncOnce(ctx)
	if err == nil {
		t.Fatal("expected a partitioned push to fail")
	}
	assertNoPassword(t, "SyncOnce error", err.Error())
	clearPeerBackoffForTest(l, credentialedPeer("node-a"))
	err = l.AntiEntropyOnce(ctx)
	if err == nil {
		t.Fatal("expected a partitioned pull to fail")
	}
	assertNoPassword(t, "AntiEntropyOnce error", err.Error())

	assertNoPassword(t, "state logs", logs.String())
}

// Review 10-B-Net H1: a peer that echoes the credentialed URL in an HTTP error
// body must not get the password into the returned error or the logs.
func TestHTTPErrorBodyEchoingPeerURLPasswordIsRedacted(t *testing.T) {
	ctx := context.Background()
	logs := captureStateLogs(t)
	var peer string
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream "+peer+r.URL.Path+" refused", http.StatusBadGateway)
	}))
	defer echo.Close()
	peer = strings.Replace(echo.URL, "http://", "http://ops:"+testURLPassword+"@", 1)

	store := mustStore(t, "node-a", NewHLCLWWPolicy(), &fixedPhysicalClock{now: 1_000})
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-a", AuthToken: "token", Client: echo.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = syncer.Pull(ctx, peer, PullRequest{})
	if err == nil {
		t.Fatal("expected the echoing peer to fail the pull")
	}
	assertNoPassword(t, "HTTPSyncer error", err.Error())
	if !strings.Contains(err.Error(), "502") {
		t.Fatalf("redaction must keep the useful part of the error: %q", err)
	}

	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       syncer,
		PeerProvider: NewStaticPeerProvider([]string{peer}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	err = engine.SyncOnce(ctx)
	if err == nil {
		t.Fatal("expected the echoing peer to fail the push")
	}
	assertNoPassword(t, "SyncOnce error", err.Error())
	assertLogged(t, logs, "peer sync failed")
	assertNoPassword(t, "state logs", logs.String())
}

// The redaction helpers read a peer address the way buildPeerURL dials it, so
// a scheme-less credentialed address is covered too, and wrapped errors keep
// their identity.
func TestPeerAddressRedactionHelpers(t *testing.T) {
	for _, addr := range []string{
		"http://ops:" + testURLPassword + "@peer:7946",
		"https://ops:" + testURLPassword + "@peer:7946/base",
		"ops:" + testURLPassword + "@peer:7946",
	} {
		shown := redactedPeerURL(addr)
		assertNoPassword(t, "redactedPeerURL("+addr+")", shown)
		if !strings.Contains(shown, "peer:7946") {
			t.Fatalf("redactedPeerURL(%q) = %q lost the host", addr, shown)
		}
		assertNoPassword(t, "redactPeerError", redactPeerError(addr, "dial "+addr+" failed"))

		cause := &ResyncRequiredError{Peer: "node-b"}
		wrapped := redactPeerErr(addr, fmt.Errorf("pull from %s: %w", addr, cause))
		assertNoPassword(t, "redactPeerErr", wrapped.Error())
		var resync *ResyncRequiredError
		if !errors.As(wrapped, &resync) || resync != cause {
			t.Fatalf("redactPeerErr must keep the wrapped error chain: %v", wrapped)
		}
	}
	plain := errors.New("plain")
	if got := redactPeerErr("http://peer:7946", plain); got != plain {
		t.Fatalf("an address without a password must leave the error as is, got %v", got)
	}
	if redactPeerErr("http://ops:pw@peer", nil) != nil {
		t.Fatal("nil error must stay nil")
	}
}

package meshdirectory

import (
	"fmt"
	"github.com/outpostos/OutpostNet/pkg/service"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func leasedInstance(uid, incarnation string) string {
	s := entityJSON(id(uid), "echo", uid, ready, true, far, 100)
	s = strings.Replace(s, `"expiryTime":`+fmt.Sprintf("%q", far.UTC().Format(time.RFC3339Nano)), `"noExpiry":true`, 1)
	s = strings.Replace(s, `"serviceRegistration":{`, `"serviceRegistration":{"registrarIncarnation":`+fmt.Sprintf("%q", incarnation)+`,`, 1)
	return s
}

func leaseJSON(seq uint64, incarnation string) string {
	return fmt.Sprintf(`{"entityId":"urn:outpostos:node-lease:node-a","isLive":true,"noExpiry":true,"originNodeId":"node-a","nodeLease":{"leaseSeq":%q,"registrarIncarnation":%q,"timeQuality":"TIME_QUALITY_UNSYNCED"}}`, fmt.Sprint(seq), incarnation)
}

// A no-expiry registration must never fall back to the legacy TTL path.
func TestLeaseBackedRegistrationRequiresLease(t *testing.T) {
	s := newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(leasedInstance("a", "boot-a"), "s1"))
		send(complete("s1"))
	})
	d, err := New(Config{GatewayURL: s.srv.URL, Logger: testLogger()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.Instances("echo"); len(got) != 0 {
		t.Fatalf("registration without a node lease resolved: %v", uids(got))
	}
}

func TestLeaseMakesNoExpiryRegistrationResolvable(t *testing.T) {
	s := newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(leasedInstance("a", "boot-a"), "s1"))
		send(snap(leaseJSON(1, "boot-a"), "s1"))
		send(complete("s1"))
	})
	d, err := New(Config{GatewayURL: s.srv.URL, Logger: testLogger()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.Instances("echo"); uids(got) != "a/READY" {
		t.Fatalf("leased registration = %v", uids(got))
	}
}

// Each connection is a transport script; assertions use the Directory read port
// and the public Service selector, never the adapter's internal maps.
func scriptedDirectory(t *testing.T, cfg Config, batches ...[]string) (*Directory, func()) {
	t.Helper()
	var handlers []func(*http.Request, func(string))
	for _, batch := range batches {
		handlers = append(handlers, func(r *http.Request, send func(string)) {
			for _, ev := range batch {
				send(ev)
			}
		})
	}
	s := newSSEServer(t, handlers...)
	cfg.GatewayURL, cfg.Logger = s.srv.URL, testLogger()
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		t.Helper()
		if _, err := d.follow(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func assertInstances(t *testing.T, d *Directory, want string) {
	t.Helper()
	got, _ := d.Instances("echo")
	if uids(got) != want {
		t.Fatalf("instances = %q, want %q", uids(got), want)
	}
	names := strings.Join(d.ServiceNames(), ",")
	if (want == "" && names != "") || (want != "" && names != "echo") {
		t.Fatalf("ServiceNames = %q for %q", names, want)
	}
}

func TestLeaseExpiryUsesOnlySequenceAndMonotonicElapsed(t *testing.T) {
	for _, offset := range []time.Duration{-60 * time.Second, 0, 60 * time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			wall, mono := time.Now(), time.Now()
			// Include a deliberately nonsensical expiry: the lease-backed path ignores it.
			instance := strings.Replace(leasedInstance("a", "boot-a"), `"noExpiry":true`, `"expiryTime":"yesterday"`, 1)
			d, follow := scriptedDirectory(t, Config{Now: func() time.Time { return wall }, MonotonicNow: func() time.Time { return mono }},
				[]string{snap(instance, "s1"), snap(leasedInstance("b", "boot-a"), "s1"), snap(leaseJSON(9007199254740993, "boot-a"), "s1"), complete("s1")},
				[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", leaseJSON(9007199254740993, "boot-a"), "d1")},
				[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", leaseJSON(2, "boot-a"), "d2")},
				[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", leaseJSON(9007199254740994, "boot-a"), "d3")})
			follow()
			assertInstances(t, d, "a/READY,b/READY")
			wall = wall.Add(offset)
			mono = mono.Add(29 * time.Second)
			follow()
			assertInstances(t, d, "a/READY,b/READY")
			// Replays and regressed sequence values do not keep a stopped Let alive.
			mono = mono.Add(2 * time.Second)
			follow()
			assertInstances(t, d, "")
			// Clock correction cannot revive an expired lease.
			wall = wall.Add(-offset)
			assertInstances(t, d, "")
			// Reconnection needs only lease progress; instances were retained.
			follow()
			assertInstances(t, d, "a/READY,b/READY")
			got, _ := d.Instances("echo")
			if len(service.Select(got, "consumer", func(string) bool { return false }, wall)) != 0 {
				t.Fatal("lease bypassed reachability filter")
			}
		})
	}
}

func TestLeaseIncarnationAndLegacyTransition(t *testing.T) {
	mono := time.Now()
	legacy := entityJSON(id("legacy"), "echo", "legacy", ready, true, far, 100)
	d, follow := scriptedDirectory(t, Config{MonotonicNow: func() time.Time { return mono }},
		[]string{snap(leasedInstance("a", "old"), "s"), snap(legacy, "s"), snap(leaseJSON(10, "old"), "s"), complete("s")},
		[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", leaseJSON(1, "new"), "d1")},
		[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", leasedInstance("b", "new"), "d2")},
		[]string{delta("ENTITY_EVENT_TYPE_DELETED", leaseJSON(1, "new"), "d3")})
	follow()
	assertInstances(t, d, "a/READY,legacy/READY")
	follow()
	assertInstances(t, d, "legacy/READY")
	follow()
	assertInstances(t, d, "b/READY,legacy/READY")
	follow()
	assertInstances(t, d, "legacy/READY")
}

func TestLeaseTTLByLinkClassAndSnapshotRefresh(t *testing.T) {
	mono := time.Now()
	d, follow := scriptedDirectory(t, Config{MonotonicNow: func() time.Time { return mono }, LeaseTTLByLinkClass: map[string]time.Duration{"radio": 90 * time.Second}, NodeLinkClass: func(string) string { return "radio" }},
		[]string{snap(leasedInstance("a", "boot"), "s"), snap(leaseJSON(1, "boot"), "s"), complete("s")},
		[]string{snap(leasedInstance("a", "boot"), "s2"), snap(leaseJSON(1, "boot"), "s2"), complete("s2")})
	follow()
	mono = mono.Add(31 * time.Second)
	assertInstances(t, d, "a/READY")
	mono = mono.Add(60 * time.Second)
	assertInstances(t, d, "")
	follow()
	assertInstances(t, d, "a/READY")
	mono = mono.Add(91 * time.Second)
	assertInstances(t, d, "")
}

func TestWriterFilterRejectsLeaseAndInstanceVersions(t *testing.T) {
	forgedLease := strings.Replace(leaseJSON(1, "boot"), `"originNodeId":"node-a"`, `"originNodeId":"node-b"`, 1)
	forgedInstance := strings.Replace(leasedInstance("a", "boot"), `"originNodeId":"node-a"`, `"originNodeId":"node-b"`, 1)
	missingWriter := strings.Replace(leasedInstance("missing", "boot"), `"originNodeId":"node-a",`, "", 1)
	forgedLegacy := strings.Replace(entityJSON(id("legacy"), "echo", "legacy", ready, true, far, 100), `"originNodeId":"node-a"`, `"originNodeId":"node-b"`, 1)
	d, follow := scriptedDirectory(t, Config{},
		[]string{snap(leasedInstance("a", "boot"), "s"), snap(forgedLease, "s"), snap(missingWriter, "s"), snap(forgedLegacy, "s"), complete("s")},
		[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", leaseJSON(1, "boot"), "d1")},
		[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", forgedInstance, "d2")},
		[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", leasedInstance("a", "boot"), "d3"), delta("ENTITY_EVENT_TYPE_PUBLISHED", forgedLease, "d4")})
	follow()
	assertInstances(t, d, "")
	follow()
	assertInstances(t, d, "a/READY")
	follow()
	assertInstances(t, d, "")
	follow()
	assertInstances(t, d, "")
	if st := d.Stats(); st.WriterRejected != 5 {
		t.Fatalf("writer rejects = %d, want 5", st.WriterRejected)
	}
}

func TestLeaseExpiryNotifiesOnceAndRenewalRestores(t *testing.T) {
	events := make(chan string)
	var s *sseServer
	s = newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(leasedInstance("a", "boot"), "s"))
		send(snap(leaseJSON(1, "boot"), "s"))
		send(complete("s"))
		for {
			select {
			case ev := <-events:
				send(ev)
			case <-r.Context().Done():
				return
			}
		}
	})
	h := startDir(t, Config{GatewayURL: s.srv.URL, LeaseTTL: 100 * time.Millisecond})
	eventually(t, "lease snapshot", hasState(h.d, "a/READY"))
	before := h.notices.Load()
	eventually(t, "lease expiry notice", func() bool { return h.d.Stats().LeaseExpired == 1 && h.notices.Load() > before })
	assertInstances(t, h.d, "")
	events <- delta("ENTITY_EVENT_TYPE_PUBLISHED", leaseJSON(2, "boot"), "d1")
	eventually(t, "resolution restored without registration", hasState(h.d, "a/READY"))
}

func TestLeaseCacheRestartIsJustSeenAndStillExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), CacheFileName)
	mono := time.Now()
	cfg := Config{CachePath: path, MonotonicNow: func() time.Time { return mono }}
	d, follow := scriptedDirectory(t, cfg, []string{snap(leasedInstance("a", "boot"), "s"), snap(leaseJSON(42, "boot"), "s"), complete("s")})
	follow()
	assertInstances(t, d, "a/READY")
	// Persisted wall timestamps and downtime cannot decide lease validity.
	mono = mono.Add(24 * time.Hour)
	cached, err := New(Config{GatewayURL: "http://127.0.0.1:1", CachePath: path, MonotonicNow: func() time.Time { return mono }, Now: func() time.Time { return time.Unix(0, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	assertInstances(t, cached, "a/READY")
	if _, fr := cached.Instances("echo"); fr.Synced || !fr.Stale {
		t.Fatalf("cache freshness = %+v", fr)
	}
	mono = mono.Add(31 * time.Second)
	assertInstances(t, cached, "")
	// Cache contents never serialize Seen (a process-local monotonic reading).
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"Seen"`) {
		t.Fatal("monotonic timestamp persisted")
	}
}

func TestLeaseSnapshotReplacesRecordsAtomically(t *testing.T) {
	release := make(chan struct{})
	var s *sseServer
	s = newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(leasedInstance("a", "boot"), "s"))
		send(snap(leaseJSON(1, "boot"), "s"))
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		send(complete("s"))
		s.holdOpen(r)
	})
	h := startDir(t, Config{GatewayURL: s.srv.URL})
	eventually(t, "stream connected", func() bool { return h.d.Stats().Connects == 1 })
	assertInstances(t, h.d, "")
	close(release)
	eventually(t, "atomic registration and lease snapshot", hasState(h.d, "a/READY"))
}

func TestLeaseConfigurationRejectsInvalidTTL(t *testing.T) {
	for _, cfg := range []Config{
		{LeaseTTL: -time.Second},
		{LeaseTTLByLinkClass: map[string]time.Duration{"radio": 0}},
		{LeaseTTLByLinkClass: map[string]time.Duration{"": time.Second}},
	} {
		cfg.GatewayURL = "http://127.0.0.1:1"
		if _, err := New(cfg); err == nil {
			t.Fatalf("invalid config accepted: %+v", cfg)
		}
	}
}

func TestInvalidLeaseReplacementFailsClosed(t *testing.T) {
	for _, bad := range []string{
		strings.Replace(leaseJSON(2, "boot"), `"leaseSeq":"2"`, `"leaseSeq":"0"`, 1),
		strings.Replace(leaseJSON(2, "boot"), `"registrarIncarnation":"boot"`, `"registrarIncarnation":""`, 1),
		strings.Replace(leaseJSON(2, "boot"), `"isLive":true`, `"isLive":false`, 1),
	} {
		t.Run(bad, func(t *testing.T) {
			d, follow := scriptedDirectory(t, Config{},
				[]string{snap(leasedInstance("a", "boot"), "s"), snap(leaseJSON(1, "boot"), "s"), complete("s")},
				[]string{delta("ENTITY_EVENT_TYPE_PUBLISHED", bad, "d1")})
			follow()
			assertInstances(t, d, "a/READY")
			follow()
			assertInstances(t, d, "")
		})
	}
}

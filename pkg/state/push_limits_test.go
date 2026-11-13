package state

import (
	"strings"
	"testing"
)

func TestPushConfigurationLimits(t *testing.T) {
	store := pagingStore(t, false, "source")
	for _, tc := range []struct {
		name    string
		bytes   int64
		pages   int
		request int64
		want    string
	}{
		{"negative bytes", -1, 0, 0, "push"},
		{"negative pages", 0, -1, 0, "push"},
		{"entry and envelope", 512 << 10, 0, 0, "MaxEntryBytes"},
		{"equal peer request", 768 << 10, 0, 768 << 10, "MaxRequestBytes"},
		{"above peer request", 900 << 10, 0, 800 << 10, "MaxRequestBytes"},
		{"defaults", 0, 0, 0, ""},
		{"configured", 700 << 10, 2, 800 << 10, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "source", AuthToken: "limits-test", MaxRequestBytes: tc.request})
			if err != nil {
				t.Fatal(err)
			}
			_, err = NewGossipEngine(GossipConfig{NodeID: "source", Store: store, Syncer: syncer, MaxPushBytes: tc.bytes, MaxPushPagesPerRound: tc.pages})
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error mentioning %s", err, tc.want)
			}
		})
	}
	engine, err := NewGossipEngine(GossipConfig{NodeID: "source", Store: store, Syncer: noopSyncer{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "source", Engine: engine, AuthToken: "limits-test", MaxRequestBytes: 768 << 10}); err == nil || !strings.Contains(err.Error(), "MaxRequestBytes") {
		t.Fatalf("handler accepted incompatible limit: %v", err)
	}
	if _, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "source", AuthToken: "limits-test", MaxRequestBytes: -1}); err == nil {
		t.Fatal("negative request limit accepted")
	}
}

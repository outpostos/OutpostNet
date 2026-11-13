package service

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)

func w(v uint32) *uint32 { return &v }

func inst(id, node string, mut ...func(*Instance)) Instance {
	in := Instance{
		ServiceName: "echo",
		InstanceID:  id,
		NodeID:      node,
		Endpoints:   []Endpoint{{Protocol: "tcp", Port: 7000}},
		Health:      HealthReady,
		Live:        true,
		Expiry:      t0.Add(30 * time.Second),
	}
	for _, m := range mut {
		m(&in)
	}
	return in
}

func ids(insts []Instance) []string {
	out := make([]string, len(insts))
	for i, in := range insts {
		out[i] = in.InstanceID
	}
	return out
}

func onlyReachable(nodes ...string) func(string) bool {
	return func(n string) bool {
		for _, x := range nodes {
			if x == n {
				return true
			}
		}
		return false
	}
}

// Acceptance 1: each filter rule removes the instance on its own.
func TestSelectFiltersEachRuleIndependently(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Instance)
	}{
		{"not live", func(i *Instance) { i.Live = false }},
		{"expired exactly now", func(i *Instance) { i.Expiry = t0 }},
		{"expired in the past", func(i *Instance) { i.Expiry = t0.Add(-time.Second) }},
		{"not ready", func(i *Instance) { i.Health = HealthNotReady }},
		{"draining", func(i *Instance) { i.Health = HealthDraining }},
		{"unknown health", func(i *Instance) { i.Health = HealthUnknown }},
		{"weight zero", func(i *Instance) { i.Weight = w(0) }},
		{"no endpoints", func(i *Instance) { i.Endpoints = nil }},
		{"udp only", func(i *Instance) { i.Endpoints = []Endpoint{{Protocol: "udp", Port: 7000}} }},
		{"tcp port zero", func(i *Instance) { i.Endpoints = []Endpoint{{Protocol: "tcp", Port: 0}} }},
		{"unreachable node", func(i *Instance) { i.NodeID = "node-x" }},
		{"empty node", func(i *Instance) { i.NodeID = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			good := inst("good", "node-b")
			bad := inst("bad", "node-b", tc.mut)
			got := Select([]Instance{bad, good}, "node-a", onlyReachable("node-b"), t0)
			if len(got) != 1 || got[0].InstanceID != "good" {
				t.Fatalf("Select = %v, want only [good]", ids(got))
			}
		})
	}
}

func TestSelectKeepsUsableVariants(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Instance)
	}{
		{"no expiry", func(i *Instance) { i.Expiry = time.Time{} }},
		{"nil weight", func(i *Instance) { i.Weight = nil }},
		{"uppercase TCP", func(i *Instance) { i.Endpoints = []Endpoint{{Protocol: "TCP", Port: 1}} }},
		{"udp then tcp", func(i *Instance) {
			i.Endpoints = []Endpoint{{Protocol: "udp", Port: 1}, {Protocol: "tcp", Port: 2}}
		}},
		{"self is reachable even if reachable says no", func(i *Instance) { i.NodeID = "node-a" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Select([]Instance{inst("x", "node-b", tc.mut)}, "node-a", onlyReachable("node-b"), t0)
			if len(got) != 1 {
				t.Fatalf("Select dropped a usable instance")
			}
		})
	}
	if got := Select([]Instance{inst("x", "node-z")}, "node-a", nil, t0); len(got) != 1 {
		t.Fatalf("nil reachable must treat remote nodes as reachable")
	}
}

func TestTCPPortPicksFirstTCP(t *testing.T) {
	in := inst("x", "n", func(i *Instance) {
		i.Endpoints = []Endpoint{{Protocol: "udp", Port: 1}, {Protocol: "tcp", Port: 2}, {Protocol: "tcp", Port: 3}}
	})
	if p, ok := in.TCPPort(); !ok || p != 2 {
		t.Fatalf("TCPPort = %d,%v want 2,true", p, ok)
	}
}

// Acceptance 2: local first, then weight descending.
func TestSelectOrderLocalFirstThenWeight(t *testing.T) {
	insts := []Instance{
		inst("remote-heavy", "node-b", func(i *Instance) { i.Weight = w(1000) }),
		inst("remote-default", "node-c"),
		inst("remote-light", "node-b", func(i *Instance) { i.Weight = w(5) }),
		inst("local-light", "node-a", func(i *Instance) { i.Weight = w(1) }),
	}
	got := ids(Select(insts, "node-a", nil, t0))
	want := []string{"local-light", "remote-heavy", "remote-default", "remote-light"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// Acceptance 2: equal weight is stable per self and spreads across selves.
func TestSelectSpreadIsStableAndDistributed(t *testing.T) {
	insts := []Instance{inst("i-1", "node-b"), inst("i-2", "node-c"), inst("i-3", "node-d")}
	first := map[string]int{}
	const selves = 1000
	for n := 0; n < selves; n++ {
		self := fmt.Sprintf("consumer-%04d", n)
		a := ids(Select(insts, self, nil, t0))
		// Reversed input must give the same order: stability is a property
		// of (self, InstanceID), not of input order.
		rev := []Instance{insts[2], insts[1], insts[0]}
		b := ids(Select(rev, self, nil, t0))
		if strings.Join(a, ",") != strings.Join(b, ",") {
			t.Fatalf("self %s: order depends on input order: %v vs %v", self, a, b)
		}
		first[a[0]]++
	}
	for _, id := range []string{"i-1", "i-2", "i-3"} {
		if share := float64(first[id]) / selves; share <= 0.20 {
			t.Errorf("instance %s preferred by %.1f%% of consumers, want > 20%% (%v)", id, share*100, first)
		}
	}
}

// Acceptance 3: filtering happens before ordering, so weight cannot rescue a
// failed or unreachable instance, and locality cannot rescue a draining one.
func TestSelectWeightCannotOverrideFilter(t *testing.T) {
	insts := []Instance{
		inst("heavy-draining", "node-b", func(i *Instance) { i.Weight = w(1 << 30); i.Health = HealthDraining }),
		inst("heavy-unreachable", "node-x", func(i *Instance) { i.Weight = w(1 << 30) }),
		inst("heavy-expired", "node-b", func(i *Instance) { i.Weight = w(1 << 30); i.Expiry = t0.Add(-time.Nanosecond) }),
		inst("local-draining", "node-a", func(i *Instance) { i.Health = HealthDraining }),
		inst("light-ok", "node-b", func(i *Instance) { i.Weight = w(1) }),
	}
	got := ids(Select(insts, "node-a", onlyReachable("node-b"), t0))
	if strings.Join(got, ",") != "light-ok" {
		t.Fatalf("Select = %v, want [light-ok]", got)
	}
}

func TestSelectDoesNotModifyInput(t *testing.T) {
	insts := []Instance{inst("b", "node-b"), inst("a", "node-a"), inst("c", "node-c", func(i *Instance) { i.Live = false })}
	before := ids(insts)
	_ = Select(insts, "node-a", nil, t0)
	if strings.Join(ids(insts), ",") != strings.Join(before, ",") {
		t.Fatalf("input reordered: %v", ids(insts))
	}
}

func TestSelectEmpty(t *testing.T) {
	if got := Select(nil, "node-a", nil, t0); len(got) != 0 {
		t.Fatalf("Select(nil) = %v", got)
	}
}

// Acceptance 4.
func TestParseImportService(t *testing.T) {
	cases := []struct {
		in         string
		logical    bool
		name, node string
		wantErr    bool
	}{
		{in: "echo", logical: true, name: "echo"},
		{in: "echo-v2.prod", logical: true, name: "echo-v2.prod"},
		{in: "echo@node-b", name: "echo", node: "node-b"},
		// The node-bound form keeps its existing, permissive parsing.
		{in: "Echo_Svc@Node.B", name: "Echo_Svc", node: "Node.B"},
		{in: "", wantErr: true},
		{in: "Echo", wantErr: true},
		{in: "echo_svc", wantErr: true},
		{in: "-echo", wantErr: true},
		{in: "echo-", wantErr: true},
		{in: "echo..x", wantErr: true},
		{in: ".echo", wantErr: true},
		{in: strings.Repeat("a", 64), wantErr: true},
		{in: strings.Repeat(strings.Repeat("a", 63)+".", 4)[:254], wantErr: true},
		{in: "@node-b", wantErr: true},
		{in: "echo@", wantErr: true},
		{in: "a@b@c", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			logical, name, node, err := ParseImportService(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseImportService(%q) = %v,%q,%q, want error", tc.in, logical, name, node)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseImportService(%q): %v", tc.in, err)
			}
			if logical != tc.logical || name != tc.name || node != tc.node {
				t.Fatalf("ParseImportService(%q) = %v,%q,%q want %v,%q,%q", tc.in, logical, name, node, tc.logical, tc.name, tc.node)
			}
		})
	}
}

// Acceptance 4: the "name@node" path is the existing ParseRef result.
func TestParseImportServiceMatchesParseRef(t *testing.T) {
	for _, s := range []string{"echo@node-b", "svc@n", "Echo_Svc@Node.B"} {
		ref, err := ParseRef(s)
		if err != nil {
			t.Fatal(err)
		}
		_, name, node, err := ParseImportService(s)
		if err != nil || name != ref.Service || node != ref.NodeID {
			t.Fatalf("%q: got %q,%q,%v want %q,%q", s, name, node, err, ref.Service, ref.NodeID)
		}
	}
}

func TestValidateLogicalNameMaxLength(t *testing.T) {
	label := strings.Repeat("a", 63)
	ok := label + "." + label + "." + label + "." + strings.Repeat("b", 61) // 253
	if len(ok) != 253 {
		t.Fatalf("fixture length %d", len(ok))
	}
	if err := ValidateLogicalName(ok); err != nil {
		t.Fatalf("253-char name rejected: %v", err)
	}
	if err := ValidateLogicalName(ok + "b"); err == nil {
		t.Fatal("254-char name accepted")
	}
}

func TestHealthString(t *testing.T) {
	for h, s := range map[Health]string{HealthReady: "READY", HealthNotReady: "NOT_READY", HealthDraining: "DRAINING", HealthUnknown: "UNKNOWN", Health(42): "UNKNOWN"} {
		if h.String() != s {
			t.Errorf("Health(%d).String() = %q want %q", h, h.String(), s)
		}
	}
}

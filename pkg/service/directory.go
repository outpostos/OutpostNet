package service

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"time"
)

// DefaultInstanceWeight is the weight of an instance that registers none.
// It matches the Mesh ServiceRegistration default.
const DefaultInstanceWeight uint32 = 100

// maxLogicalNameLen is the DNS-1123 subdomain limit used by Mesh
// service_registration.service_name.
const maxLogicalNameLen = 253

// Health is the self-reported health of one registered instance.
type Health int

const (
	// HealthUnknown is an unset or unrecognised health state. It is never
	// selected.
	HealthUnknown Health = iota
	// HealthReady accepts new connections.
	HealthReady
	// HealthNotReady is registered but must not receive connections yet.
	HealthNotReady
	// HealthDraining is shutting down and must not receive new connections.
	HealthDraining
)

// String renders the health state for logs.
func (h Health) String() string {
	switch h {
	case HealthReady:
		return "READY"
	case HealthNotReady:
		return "NOT_READY"
	case HealthDraining:
		return "DRAINING"
	default:
		return "UNKNOWN"
	}
}

// Endpoint is one port an instance serves on its registering node.
type Endpoint struct {
	// Protocol is "tcp" or "udp" (case-insensitive).
	Protocol string
	// Port is the port on the registering node (ADR-0003 D1: hostPort).
	Port uint16
}

// Instance is one registered instance of a logical service, as read from the
// service directory (ADR-0003 D1/D2). It is a plain value: the directory
// adapter maps the registry's wire format onto it.
type Instance struct {
	ServiceName string
	InstanceID  string
	// NodeID is the node the instance runs on and registered from.
	NodeID    string
	Endpoints []Endpoint
	// Weight is nil when the registration carries none; see
	// DefaultInstanceWeight. Zero means "never select".
	Weight *uint32
	Health Health
	// Live is false once the registration has been withdrawn.
	Live bool
	// Expiry is a legacy registration's wall-clock deadline. Lease-backed
	// adapters leave it zero and enforce local monotonic liveness before returning
	// the candidate; Select still enforces readiness and reachability.
	Expiry  time.Time
	Version string
}

// EffectiveWeight returns Weight, or DefaultInstanceWeight when unset.
func (i Instance) EffectiveWeight() uint32 {
	if i.Weight == nil {
		return DefaultInstanceWeight
	}
	return *i.Weight
}

// TCPPort returns the first TCP endpoint port, if any.
func (i Instance) TCPPort() (uint16, bool) {
	for _, e := range i.Endpoints {
		if strings.EqualFold(e.Protocol, "tcp") && e.Port != 0 {
			return e.Port, true
		}
	}
	return 0, false
}

// Freshness describes how current a directory answer is.
type Freshness struct {
	// Synced is true once the directory has received at least one complete
	// snapshot from its source since it started.
	Synced bool
	// Stale is true while the answer comes from a cold cache or the source
	// connection is down. Instances are still filtered by the adapter's lease or legacy TTL rules.
	Stale bool
	// AsOf is when the answer was last confirmed by the source.
	AsOf time.Time
}

// Directory is the Service layer's read port onto the logical service
// registry. Hosts provide implementations (for example the Mesh adapter);
// pkg/service never reaches the registry directly.
type Directory interface {
	// Instances returns a snapshot of every known instance of service,
	// potentially including unhealthy or expired ones; callers filter with Select.
	// Adapters must enforce liveness that Select cannot evaluate, such as leases.
	Instances(service string) ([]Instance, Freshness)
	// Subscribe registers fn to be called after any directory change. fn
	// must not block. The returned function cancels the subscription.
	Subscribe(fn func()) (cancel func())
}

// Select applies ADR-0003 D3 to the candidates of one new connection: it
// filters first and only then orders the survivors, so a failing or
// withdrawn instance can never be outranked into use by a high weight.
//
// Filtered out: not Live; Expiry reached; Health other than READY; weight 0;
// no TCP endpoint; a remote node for which reachable returns false (a nil
// reachable treats every node as reachable; self is always reachable).
//
// Ordering: instances on self first; then weight descending; then a hash of
// (self, InstanceID), which is stable for one consumer node and spreads
// different consumer nodes across equal instances. InstanceID breaks the
// (unlikely) hash ties. The input slice is not modified.
func Select(insts []Instance, self string, reachable func(nodeID string) bool, now time.Time) []Instance {
	out := make([]Instance, 0, len(insts))
	for _, in := range insts {
		if Usable(in, self, reachable, now) {
			out = append(out, in)
		}
	}
	keys := make(map[string]uint64, len(out))
	for _, in := range out {
		keys[in.InstanceID] = spreadKey(self, in.InstanceID)
	}
	sort.SliceStable(out, func(a, b int) bool {
		ia, ib := out[a], out[b]
		la, lb := ia.NodeID == self, ib.NodeID == self
		if la != lb {
			return la
		}
		if wa, wb := ia.EffectiveWeight(), ib.EffectiveWeight(); wa != wb {
			return wa > wb
		}
		if ka, kb := keys[ia.InstanceID], keys[ib.InstanceID]; ka != kb {
			return ka < kb
		}
		return ia.InstanceID < ib.InstanceID
	})
	return out
}

// Usable reports whether one instance passes the Select filter.
func Usable(in Instance, self string, reachable func(nodeID string) bool, now time.Time) bool {
	if !in.Live || in.Health != HealthReady || in.EffectiveWeight() == 0 {
		return false
	}
	if !in.Expiry.IsZero() && !now.Before(in.Expiry) {
		return false
	}
	if _, ok := in.TCPPort(); !ok {
		return false
	}
	if in.NodeID == "" {
		return false
	}
	if in.NodeID != self && reachable != nil && !reachable(in.NodeID) {
		return false
	}
	return true
}

// spreadKey is the stable per-consumer ordering key for equal-weight
// instances.
func spreadKey(self, instanceID string) uint64 {
	h := sha256.New()
	h.Write([]byte(self))
	h.Write([]byte{0})
	h.Write([]byte(instanceID))
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

// ParseImportService classifies an import's service field. Without "@" it is
// a logical service name resolved through the Directory (ADR-0003 D3) and
// must be a DNS-1123 subdomain, as Mesh requires for service_name. With "@"
// it is the node-bound "<service>@<node_id>" form, parsed exactly as before.
func ParseImportService(s string) (logical bool, name, node string, err error) {
	if strings.Contains(s, "@") {
		ref, err := ParseRef(s)
		if err != nil {
			return false, "", "", err
		}
		return false, ref.Service, ref.NodeID, nil
	}
	if err := ValidateLogicalName(s); err != nil {
		return false, "", "", err
	}
	return true, s, "", nil
}

// ValidateLogicalName checks a logical service name against the DNS-1123
// subdomain rule: at most 253 characters of dot-separated labels, each 1-63
// characters of [a-z0-9-] starting and ending with [a-z0-9].
func ValidateLogicalName(s string) error {
	if s == "" {
		return fmt.Errorf("logical service name is required")
	}
	if len(s) > maxLogicalNameLen {
		return fmt.Errorf("logical service name %q is longer than %d characters", s, maxLogicalNameLen)
	}
	for _, label := range strings.Split(s, ".") {
		if !validDNSLabel(label) {
			return fmt.Errorf("logical service name %q must be a DNS-1123 subdomain (lowercase letters, digits, '-' and '.', each label 1-63 characters starting and ending with a letter or digit)", s)
		}
	}
	return nil
}

func validDNSLabel(l string) bool {
	if len(l) == 0 || len(l) > 63 {
		return false
	}
	for i := 0; i < len(l); i++ {
		c := l[i]
		alnum := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !alnum && (c != '-' || i == 0 || i == len(l)-1) {
			return false
		}
	}
	return true
}

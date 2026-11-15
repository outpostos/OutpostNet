// Package meshdirectory implements service.Directory over the local
// OutpostMesh gateway (OutpostOS ADR-0003 D2).
//
// It follows the local Mesh SSE stream for registrations and node leases.
// Snapshots replace both atomically; resumed deltas only renew leases when
// their sequence advances. Lease expiry hides instances without deleting them,
// allowing resolution to recover after partitions without re-registration.
// Legacy registrations retain their wall-clock expiry for one transition version.
package meshdirectory

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/outpostos/OutpostNet/pkg/service"
)

const (
	streamPath   = "/v1alpha1/entities:stream"
	streamFilter = "urn:outpostos:"

	defaultInitialBackoff = 100 * time.Millisecond
	defaultMaxBackoff     = 5 * time.Second
	defaultCacheInterval  = 5 * time.Second
	defaultSweepInterval  = 250 * time.Millisecond
	maxEventBytes         = 4 << 20

	// CacheFileName is the cold-start cache inside the host's data dir.
	CacheFileName = "service-directory.json"
	cacheVersion  = 2
)

// Config configures a Directory.
type Config struct {
	// GatewayURL is the local Mesh REST base URL, e.g. http://127.0.0.1:8081.
	GatewayURL string
	// CachePath is the cold-start cache file; empty disables the cache.
	CachePath string
	// HTTPClient is used for the stream; it must not set a total Timeout.
	HTTPClient     *http.Client
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// CacheInterval bounds how often the cache is rewritten after changes.
	CacheInterval time.Duration
	// SweepInterval is how often expired instances are dropped.
	SweepInterval time.Duration
	// Now supplies wall time for legacy expiry and diagnostics only.
	Now func() time.Time
	// MonotonicNow must retain time.Time's monotonic reading (default time.Now).
	// It is independent of Now and any platform wall-time reference.
	MonotonicNow func() time.Time
	// LeaseTTL defaults to 30s. LeaseTTLByLinkClass overrides it for the
	// class returned by NodeLinkClass. Unknown classes use LeaseTTL.
	LeaseTTL            time.Duration
	LeaseTTLByLinkClass map[string]time.Duration
	// NodeLinkClass must be concurrency safe and must not call Directory methods.
	NodeLinkClass func(nodeID string) string
	Logger        *slog.Logger
}

// Stats counts stream activity, for observability and tests.
type Stats struct {
	Connects          uint64
	Snapshots         uint64
	Deltas            uint64
	Malformed         uint64
	Expired           uint64
	LeaseExpired      uint64
	WriterRejected    uint64
	CacheWrites       uint64
	CacheWriteErrors  uint64
	LastResumeCursor  string
	LastConnectError  string
	LastConnectErrorT time.Time
}

// Directory is a service.Directory fed by the Mesh entity stream.
type Directory struct {
	cfg    Config
	logger *slog.Logger
	base   *url.URL

	mu        sync.RWMutex
	entities  map[string]registration // by entity_id
	leases    map[string]nodeLease    // by hosting node_id
	synced    bool
	connected bool
	stale     bool
	asOf      time.Time
	cursor    string
	dirty     bool
	lastSaved time.Time

	cacheMu sync.Mutex // serialize atomic cache replacements
	subMu   sync.Mutex
	subs    map[int]func()
	nextID  int

	statsMu sync.Mutex
	stats   Stats

	running atomic.Bool
}

var _ service.Directory = (*Directory)(nil)

// New creates the directory and loads the cold-start cache, if any. A
// missing cache is normal; a corrupt one is logged and ignored.
func New(cfg Config) (*Directory, error) {
	base, err := url.Parse(strings.TrimRight(cfg.GatewayURL, "/"))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("meshdirectory: gateway URL %q must be http(s)://host[:port]", cfg.GatewayURL)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.CacheInterval <= 0 {
		cfg.CacheInterval = defaultCacheInterval
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = defaultSweepInterval
	}
	if cfg.LeaseTTL < 0 {
		return nil, errors.New("meshdirectory: LeaseTTL must be positive")
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	ttls := make(map[string]time.Duration, len(cfg.LeaseTTLByLinkClass))
	for class, ttl := range cfg.LeaseTTLByLinkClass {
		if class == "" || ttl <= 0 {
			return nil, errors.New("meshdirectory: link classes need a name and positive LeaseTTL")
		}
		ttls[class] = ttl
	}
	cfg.LeaseTTLByLinkClass = ttls
	if cfg.MonotonicNow == nil {
		cfg.MonotonicNow = time.Now
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	d := &Directory{
		cfg:      cfg,
		logger:   logger.With("component", "meshdirectory"),
		base:     base,
		entities: map[string]registration{},
		leases:   map[string]nodeLease{},
		stale:    true,
		subs:     map[int]func(){},
	}
	if cfg.CachePath != "" {
		d.loadCache()
	}
	return d, nil
}

// Instances implements service.Directory. Expired instances are never
// returned, even between sweeps.
func (d *Directory) Instances(name string) ([]service.Instance, service.Freshness) {
	now, mono := d.cfg.Now(), d.cfg.MonotonicNow()
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []service.Instance
	for _, in := range d.entities {
		if in.ServiceName != name {
			continue
		}
		if !d.valid(in, now, mono) {
			continue
		}
		out = append(out, cloneInstance(in.Instance))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, service.Freshness{Synced: d.synced, Stale: d.stale, AsOf: d.asOf}
}

// ServiceNames returns the names of every service with at least one
// unexpired instance, sorted.
func (d *Directory) ServiceNames() []string {
	now, mono := d.cfg.Now(), d.cfg.MonotonicNow()
	d.mu.RLock()
	seen := map[string]struct{}{}
	for _, in := range d.entities {
		if !d.valid(in, now, mono) {
			continue
		}
		seen[in.ServiceName] = struct{}{}
	}
	d.mu.RUnlock()
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Subscribe implements service.Directory.
func (d *Directory) Subscribe(fn func()) (cancel func()) {
	d.subMu.Lock()
	id := d.nextID
	d.nextID++
	d.subs[id] = fn
	d.subMu.Unlock()
	return func() {
		d.subMu.Lock()
		delete(d.subs, id)
		d.subMu.Unlock()
	}
}

func (d *Directory) notify() {
	d.subMu.Lock()
	fns := make([]func(), 0, len(d.subs))
	for _, fn := range d.subs {
		fns = append(fns, fn)
	}
	d.subMu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

// Stats returns a copy of the counters.
func (d *Directory) Stats() Stats {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return d.stats
}

func (d *Directory) count(fn func(*Stats)) {
	d.statsMu.Lock()
	fn(&d.stats)
	d.statsMu.Unlock()
}

// Run follows the stream until ctx ends. It returns nil on cancellation.
func (d *Directory) Run(ctx context.Context) error {
	if !d.running.CompareAndSwap(false, true) {
		return errors.New("meshdirectory: Run called twice")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.maintain(ctx)
	}()
	defer wg.Wait()

	backoff := d.cfg.InitialBackoff
	for ctx.Err() == nil {
		progressed, err := d.follow(ctx)
		d.setDisconnected()
		if ctx.Err() != nil {
			break
		}
		if progressed {
			backoff = d.cfg.InitialBackoff
		}
		d.count(func(s *Stats) {
			s.LastConnectError = errString(err)
			s.LastConnectErrorT = d.cfg.Now()
		})
		d.logger.Warn("mesh entity stream ended; reconnecting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > d.cfg.MaxBackoff {
			backoff = d.cfg.MaxBackoff
		}
	}
	d.saveCacheIfDirty(true)
	return nil
}

func errString(err error) string {
	if err == nil {
		return "stream closed by server"
	}
	return err.Error()
}

// maintain drops expired instances and writes the cache.
func (d *Directory) maintain(ctx context.Context) {
	t := time.NewTicker(d.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := d.sweep(); n > 0 {

				d.notify()
			}
			d.saveCacheIfDirty(false)
		}
	}
}

func (d *Directory) sweep() int {
	now, mono := d.cfg.Now(), d.cfg.MonotonicNow()
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for id, in := range d.entities {
		if in.Incarnation == "" && !in.Expiry.IsZero() && !now.Before(in.Expiry) {
			delete(d.entities, id)
			n++
		}
	}
	if n > 0 {
		d.count(func(s *Stats) { s.Expired += uint64(n) })
	}
	for node, lease := range d.leases {
		if !lease.expired && mono.Sub(lease.Seen) > d.leaseTTL(node) {
			lease.expired = true
			d.leases[node] = lease
			d.count(func(s *Stats) { s.LeaseExpired++ })
			n++
		}
	}
	if n > 0 {
		d.dirty = true
	}
	return n
}

func (d *Directory) setDisconnected() {
	d.mu.Lock()
	changed := !d.stale
	d.connected = false
	d.stale = true
	d.mu.Unlock()
	if changed {
		d.notify()
	}
}

// follow runs one stream connection. progressed reports whether any event
// was received (used to reset the backoff).
func (d *Directory) follow(ctx context.Context) (progressed bool, err error) {
	d.mu.RLock()
	cursor := d.cursor
	d.mu.RUnlock()

	u := *d.base
	u.Path = strings.TrimRight(u.Path, "/") + streamPath
	q := url.Values{}
	q.Set("filter.entityIdPrefix", streamFilter)
	if cursor != "" {
		q.Set("resumeCursor", cursor)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := d.cfg.HTTPClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return false, fmt.Errorf("stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	d.count(func(s *Stats) { s.Connects++; s.LastResumeCursor = cursor })

	// A resumed stream is live as soon as it is accepted; a fresh one only
	// after its snapshot completes.
	d.mu.Lock()
	d.connected = true
	d.asOf = d.cfg.Now()
	becameFresh := false
	if cursor != "" && d.synced && d.stale {
		d.stale = false
		becameFresh = true
	}
	d.mu.Unlock()
	if becameFresh {
		d.notify()
	}

	var pending *snapshot // non-nil while a snapshot is being collected
	r := bufio.NewReaderSize(resp.Body, 64<<10)
	var event, data strings.Builder
	for {
		line, err := readLine(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return progressed, nil
			}
			return progressed, err
		}
		d.mu.Lock()
		d.asOf = d.cfg.Now()
		d.mu.Unlock()
		switch {
		case line == "":
			if data.Len() == 0 && event.Len() == 0 {
				continue
			}
			progressed = true
			pending = d.handle(event.String(), data.String(), pending)
			event.Reset()
			data.Reset()
		case strings.HasPrefix(line, ":"):
			// comment / heartbeat
		case strings.HasPrefix(line, "event:"):
			event.Reset()
			event.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "event:")))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			if data.Len() > maxEventBytes {
				return progressed, fmt.Errorf("event larger than %d bytes", maxEventBytes)
			}
		}
	}
}

func readLine(r *bufio.Reader) (string, error) {
	var b strings.Builder
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return "", err
		}
		b.Write(chunk)
		if b.Len() > maxEventBytes {
			return "", fmt.Errorf("line larger than %d bytes", maxEventBytes)
		}
		if !isPrefix {
			return b.String(), nil
		}
	}
}

// Wire types (protojson of StreamEntitiesResponse / Entity). Unknown fields
// are ignored.
type wireResponse struct {
	Phase     string      `json:"phase"`
	EventType string      `json:"eventType"`
	Entity    *wireEntity `json:"entity"`
	Cursor    string      `json:"cursor"`
}

type wireEntity struct {
	EntityID            string                   `json:"entityId"`
	IsLive              *bool                    `json:"isLive"`
	OriginNodeID        string                   `json:"originNodeId"`
	NodeLease           *wireNodeLease           `json:"nodeLease"`
	ExpiryTime          string                   `json:"expiryTime"`
	ServiceRegistration *wireServiceRegistration `json:"serviceRegistration"`
}

type wireServiceRegistration struct {
	RegistrarIncarnation string         `json:"registrarIncarnation"`
	ServiceName          string         `json:"serviceName"`
	InstanceID           string         `json:"instanceId"`
	NodeID               string         `json:"nodeId"`
	Endpoints            []wireEndpoint `json:"endpoints"`
	Version              string         `json:"version"`
	HealthState          string         `json:"healthState"`
}

type wireEndpoint struct {
	Port *struct {
		Protocol string `json:"protocol"`
		Port     uint32 `json:"port"`
	} `json:"port"`
	Weight *uint32 `json:"weight"`
}

func (d *Directory) malformed(what string, err error, data string) {
	d.count(func(s *Stats) { s.Malformed++ })
	if len(data) > 256 {
		data = data[:256] + "…"
	}
	d.logger.Warn("skipping malformed mesh stream event", "reason", what, "err", err, "data", data)
}

// toInstance maps an entity to an Instance. ok is false for entities that
// carry no ServiceRegistration (they are not part of the directory).
func toInstance(e *wireEntity) (service.Instance, bool, error) {
	if e.EntityID == "" {
		return service.Instance{}, false, errors.New("missing entityId")
	}
	sr := e.ServiceRegistration
	if sr == nil {
		return service.Instance{}, false, nil
	}
	if sr.ServiceName == "" {
		return service.Instance{}, false, errors.New("serviceRegistration without serviceName")
	}
	in := service.Instance{
		ServiceName: sr.ServiceName,
		InstanceID:  sr.InstanceID,
		NodeID:      sr.NodeID,
		Version:     sr.Version,
		Live:        e.IsLive == nil || *e.IsLive,
		Health:      healthOf(sr.HealthState),
	}
	if in.InstanceID == "" {
		in.InstanceID = e.EntityID
	}
	if sr.RegistrarIncarnation == "" && e.ExpiryTime != "" {
		t, err := time.Parse(time.RFC3339Nano, e.ExpiryTime)
		if err != nil {
			return service.Instance{}, false, fmt.Errorf("expiryTime: %w", err)
		}
		in.Expiry = t
	}
	weightSet := false
	for _, ep := range sr.Endpoints {
		if ep.Port == nil || ep.Port.Port == 0 || ep.Port.Port > 65535 {
			continue
		}
		proto := ""
		switch ep.Port.Protocol {
		case "ENDPOINT_PROTOCOL_TCP":
			proto = "tcp"
		case "ENDPOINT_PROTOCOL_UDP":
			proto = "udp"
		default:
			continue
		}
		in.Endpoints = append(in.Endpoints, service.Endpoint{Protocol: proto, Port: uint16(ep.Port.Port)})
		// Mesh weights are per endpoint; the instance takes the weight of
		// its first TCP endpoint, the one Service dials.
		if proto == "tcp" && !weightSet {
			weightSet = true
			if ep.Weight != nil {
				w := *ep.Weight
				in.Weight = &w
			}
		}
	}
	return in, true, nil
}

func healthOf(s string) service.Health {
	switch s {
	case "SERVICE_HEALTH_STATE_READY":
		return service.HealthReady
	case "SERVICE_HEALTH_STATE_NOT_READY":
		return service.HealthNotReady
	case "SERVICE_HEALTH_STATE_DRAINING":
		return service.HealthDraining
	default:
		return service.HealthUnknown
	}
}

func cloneInstance(in service.Instance) service.Instance {
	in.Endpoints = append([]service.Endpoint(nil), in.Endpoints...)
	if in.Weight != nil {
		w := *in.Weight
		in.Weight = &w
	}
	return in
}

// ── cold-start cache ─────────────────────────────────────────────────────────

type cacheFile struct {
	Version   int                  `json:"version"`
	SavedAt   time.Time            `json:"saved_at"`
	Instances []cacheInstance      `json:"instances"`
	Leases    map[string]nodeLease `json:"leases,omitempty"`
}

type cacheInstance struct {
	OriginNodeID string             `json:"origin_node_id"`
	Incarnation  string             `json:"registrar_incarnation,omitempty"`
	EntityID     string             `json:"entity_id"`
	ServiceName  string             `json:"service_name"`
	InstanceID   string             `json:"instance_id"`
	NodeID       string             `json:"node_id"`
	Endpoints    []service.Endpoint `json:"endpoints"`
	Weight       *uint32            `json:"weight,omitempty"`
	Health       service.Health     `json:"health"`
	Live         bool               `json:"live"`
	Expiry       time.Time          `json:"expiry,omitempty"`
	Version      string             `json:"version,omitempty"`
}

func (d *Directory) loadCache() {
	data, err := os.ReadFile(d.cfg.CachePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			d.logger.Warn("ignoring unreadable service directory cache", "path", d.cfg.CachePath, "err", err)
		}
		return
	}
	var f cacheFile
	if err := json.Unmarshal(data, &f); err != nil || f.Version != cacheVersion {
		if err == nil {
			err = fmt.Errorf("unsupported cache version %d", f.Version)
		}
		d.logger.Warn("ignoring corrupt service directory cache", "path", d.cfg.CachePath, "err", err)
		return
	}
	for _, c := range f.Instances {
		if c.EntityID == "" || c.ServiceName == "" || c.NodeID == "" || c.OriginNodeID != c.NodeID || (c.Incarnation == "" && c.Expiry.IsZero()) {
			continue
		}
		d.entities[c.EntityID] = registration{OriginNodeID: c.OriginNodeID, Incarnation: c.Incarnation, Instance: service.Instance{
			ServiceName: c.ServiceName, InstanceID: c.InstanceID, NodeID: c.NodeID,
			Endpoints: c.Endpoints, Weight: c.Weight, Health: c.Health, Live: c.Live,
			Expiry: c.Expiry, Version: c.Version,
		}}
	}
	for node, lease := range f.Leases {
		if node == "" || lease.OriginNodeID != node || lease.Incarnation == "" || lease.Seq == 0 {
			continue
		}
		lease.Seen = d.cfg.MonotonicNow()
		d.leases[node] = lease
	}
	d.asOf = f.SavedAt
	d.logger.Info("loaded stale service directory cache", "path", d.cfg.CachePath, "instances", len(d.entities), "saved_at", f.SavedAt)
}

func (d *Directory) saveCacheIfDirty(force bool) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if d.cfg.CachePath == "" {
		return
	}
	now := d.cfg.Now()
	d.mu.Lock()
	if !d.dirty || !d.synced || (!force && now.Sub(d.lastSaved) < d.cfg.CacheInterval) {
		d.mu.Unlock()
		return
	}
	f := cacheFile{Version: cacheVersion, SavedAt: now, Leases: make(map[string]nodeLease, len(d.leases))}
	for node, lease := range d.leases {
		f.Leases[node] = lease
	}
	for id, in := range d.entities {
		f.Instances = append(f.Instances, cacheInstance{
			OriginNodeID: in.OriginNodeID, Incarnation: in.Incarnation, EntityID: id, ServiceName: in.ServiceName, InstanceID: in.InstanceID, NodeID: in.NodeID,
			Endpoints: in.Endpoints, Weight: in.Weight, Health: in.Health, Live: in.Live,
			Expiry: in.Expiry, Version: in.Version,
		})
	}
	d.dirty = false
	d.lastSaved = now
	d.mu.Unlock()
	sort.Slice(f.Instances, func(i, j int) bool { return f.Instances[i].EntityID < f.Instances[j].EntityID })
	if err := writeFileAtomic(d.cfg.CachePath, f); err != nil {
		d.count(func(s *Stats) { s.CacheWriteErrors++ })
		d.logger.Warn("write service directory cache failed", "path", d.cfg.CachePath, "err", err)
		d.mu.Lock()
		d.dirty = true
		d.mu.Unlock()
		return
	}
	d.count(func(s *Stats) { s.CacheWrites++ })
}

func writeFileAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
}

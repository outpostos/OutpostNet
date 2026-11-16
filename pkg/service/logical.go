package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"
)

// Reasons a logical entry could not give a connection a backend
// (OutpostOS ADR-0003 D3). They are also the keys of LogicalStats.Failures.
const (
	ReasonNoCandidate    = "no_candidate"
	ReasonAllUnreachable = "all_unreachable"
	ReasonDirectoryStale = "directory_stale"
)

var (
	// ErrNoCandidate: the directory holds no usable instance of the service.
	ErrNoCandidate = errors.New("no usable instance of the service")
	// ErrAllUnreachable: every usable instance failed before confirming.
	ErrAllUnreachable = errors.New("every instance of the service was unreachable")
	// ErrDirectoryStale: no usable instance, and the directory is not
	// current (cold cache or Mesh unreachable), so the answer may be wrong.
	ErrDirectoryStale = errors.New("no usable instance in a stale service directory")
)

// DefaultAttemptTimeout bounds one candidate attempt (local dial or remote
// open + confirmation).
const DefaultAttemptTimeout = 3 * time.Second

// ConfirmedPeerStreams is the Net port a logical entry uses for remote
// instances: open a stream named serviceName toward nodeID and return only
// after the provider has confirmed that it connected the backend, so that a
// failure can fall back to another instance without resending any data.
type ConfirmedPeerStreams interface {
	OpenConfirmedPeerStream(ctx context.Context, nodeID, serviceName string) (io.ReadWriteCloser, error)
}

// LogicalConfig configures a LogicalOpener.
type LogicalConfig struct {
	// Self is this node's node_id.
	Self      string
	Directory Directory
	Peers     ConfirmedPeerStreams
	// Reachable reports whether a remote node currently has a tunnel; nil
	// treats every node as reachable.
	Reachable func(nodeID string) bool
	// DialLocal connects an instance on this node; nil dials
	// 127.0.0.1:<port> over TCP.
	DialLocal      func(ctx context.Context, port uint16) (io.ReadWriteCloser, error)
	AttemptTimeout time.Duration
	Now            func() time.Time
	// OnResolve, when set, is called once per connection with the outcome.
	OnResolve func(Resolution)
	Logger    *slog.Logger
}

// Resolution is the outcome of resolving one connection.
type Resolution struct {
	Service string
	// Instance is the instance that confirmed; zero on failure.
	Instance Instance
	Local    bool
	// Attempts is how many candidates were tried.
	Attempts int
	// Candidates is how many usable candidates Select returned.
	Candidates int
	// Stale is true when the directory answer was stale.
	Stale bool
	// Reason is empty on success, otherwise one of the Reason* constants.
	Reason string
	// Errors holds the error of every failed attempt, in order.
	Errors []error
}

// LogicalStats counts resolutions of one logical entry.
type LogicalStats struct {
	Resolved uint64
	// FailedOver counts connections that succeeded on a later candidate.
	FailedOver uint64
	// ResolvedFromStale counts successes on a stale directory answer.
	ResolvedFromStale uint64
	// Failures by reason.
	Failures map[string]uint64
}

// LogicalOpener resolves a logical service per connection. It implements
// StreamOpener for an Entry whose Target is Logical.
//
// Decision point: an attempt counts as successful only once the backend is
// connected (local TCP dial succeeded, or the remote provider confirmed).
// OpenStream returns before the entry reads a single application byte, so
// switching candidates on any failure never resends data; failures after
// OpenStream returned only close the connection (the entry's pipe ends).
type LogicalOpener struct {
	cfg    LogicalConfig
	logger *slog.Logger

	mu    sync.Mutex
	stats LogicalStats
}

// NewLogicalOpener validates cfg and returns the opener.
func NewLogicalOpener(cfg LogicalConfig) (*LogicalOpener, error) {
	if cfg.Self == "" {
		return nil, errors.New("logical opener: self node_id is required")
	}
	if cfg.Directory == nil || cfg.Peers == nil {
		return nil, errors.New("logical opener: directory and peers are required")
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = DefaultAttemptTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.DialLocal == nil {
		cfg.DialLocal = dialLoopback
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &LogicalOpener{cfg: cfg, logger: logger, stats: LogicalStats{Failures: map[string]uint64{}}}, nil
}

func dialLoopback(ctx context.Context, port uint16) (io.ReadWriteCloser, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
}

// Stats returns a copy of the counters.
func (o *LogicalOpener) Stats() LogicalStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	st := o.stats
	st.Failures = make(map[string]uint64, len(o.stats.Failures))
	for k, v := range o.stats.Failures {
		st.Failures[k] = v
	}
	return st
}

// ResolvedStream is returned by OpenStream; it tells observers which
// instance serves the connection.
type ResolvedStream struct {
	io.ReadWriteCloser
	Instance Instance
	Local    bool
	Address  string
}

// ResolvedBackend reports the serving node and address (duck-typed by the
// observation layer, which pkg/service does not import).
func (s *ResolvedStream) ResolvedBackend() (nodeID, address string) {
	return s.Instance.NodeID, s.Address
}

// OpenStream implements StreamOpener. t must be a logical target.
func (o *LogicalOpener) OpenStream(ctx context.Context, t Target) (io.ReadWriteCloser, error) {
	if !t.Logical {
		return nil, fmt.Errorf("logical opener: target %s is not logical", t)
	}
	insts, fresh := o.cfg.Directory.Instances(t.Service)
	stale := fresh.Stale || !fresh.Synced
	cands := Select(insts, o.cfg.Self, o.cfg.Reachable, o.cfg.Now())
	res := Resolution{Service: t.Service, Candidates: len(cands), Stale: stale}

	for _, c := range cands {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		res.Attempts++
		rw, addr, err := o.attempt(ctx, t.Service, c)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("%s on %s: %w", c.InstanceID, c.NodeID, err))
			continue
		}
		res.Instance = c
		res.Local = c.NodeID == o.cfg.Self
		o.finish(res)
		return &ResolvedStream{ReadWriteCloser: rw, Instance: c, Local: res.Local, Address: addr}, nil
	}

	var err error
	switch {
	case len(cands) > 0:
		res.Reason, err = ReasonAllUnreachable, ErrAllUnreachable
	case stale:
		res.Reason, err = ReasonDirectoryStale, ErrDirectoryStale
	default:
		res.Reason, err = ReasonNoCandidate, ErrNoCandidate
	}
	o.finish(res)
	if len(res.Errors) > 0 {
		return nil, fmt.Errorf("%s: %w (%v)", t.Service, err, errors.Join(res.Errors...))
	}
	return nil, fmt.Errorf("%s: %w", t.Service, err)
}

func (o *LogicalOpener) attempt(ctx context.Context, svc string, c Instance) (io.ReadWriteCloser, string, error) {
	port, _ := c.TCPPort() // Select guarantees one
	actx, cancel := context.WithTimeout(ctx, o.cfg.AttemptTimeout)
	defer cancel()
	if c.NodeID == o.cfg.Self {
		rw, err := o.cfg.DialLocal(actx, port)
		return rw, net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))), err
	}
	rw, err := o.cfg.Peers.OpenConfirmedPeerStream(actx, c.NodeID, svc)
	return rw, c.NodeID + "/" + svc, err
}

func (o *LogicalOpener) finish(res Resolution) {
	o.mu.Lock()
	if res.Reason == "" {
		o.stats.Resolved++
		if res.Attempts > 1 {
			o.stats.FailedOver++
		}
		if res.Stale {
			o.stats.ResolvedFromStale++
		}
	} else {
		o.stats.Failures[res.Reason]++
	}
	o.mu.Unlock()

	switch {
	case res.Reason != "":
		o.logger.Warn("logical entry has no backend for connection", "service", res.Service, "reason", res.Reason,
			"candidates", res.Candidates, "attempts", res.Attempts, "stale_directory", res.Stale, "errors", errors.Join(res.Errors...))
	case res.Attempts > 1 || res.Stale:
		o.logger.Info("logical entry resolved", "service", res.Service, "instance_id", res.Instance.InstanceID,
			"node_id", res.Instance.NodeID, "attempts", res.Attempts, "stale_directory", res.Stale, "errors", errors.Join(res.Errors...))
	default:
		o.logger.Debug("logical entry resolved", "service", res.Service, "instance_id", res.Instance.InstanceID, "node_id", res.Instance.NodeID)
	}
	if o.cfg.OnResolve != nil {
		o.cfg.OnResolve(res)
	}
}

package observation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
	"google.golang.org/protobuf/proto"

	"github.com/outpostos/OutpostNet/pkg/service"
)

// Backend is the instance a stable entry's stream goes to: the provider's
// node identity (when known) and its tunnel address.
type Backend struct {
	NodeID  string
	Address string
}

// EntryObserver records access facts for one stable entry from the real
// connections applications make through it. It only reads outcomes; it never
// changes how the entry forwards.
type EntryObserver struct {
	r    *Reporter
	name string
}

// ServiceEntry registers a stable entry by its service identity and returns
// its observer. The entry appears in snapshots once its address is set; its
// state stays unset until a connection goes through it.
func (r *Reporter) ServiceEntry(name string) *EntryObserver {
	return &EntryObserver{r: r, name: name}
}

// SetEntryAddress records the entry's bound address.
func (o *EntryObserver) SetEntryAddress(addr string) {
	o.r.mu.Lock()
	defer o.r.mu.Unlock()
	current := o.r.services[o.name]
	if current == nil {
		current = &observationv1alpha1.ServiceAccessChanged{ServiceName: o.name}
		o.r.services[o.name] = current
	}
	current.EntryAddress = addr
}

// OpenerConfig tells the observer how to read the host's state.
type OpenerConfig struct {
	// Resolve returns the backend the next stream for t uses, if any.
	Resolve func(t service.Target) (Backend, bool)
	// NoBackend reports whether an open error means the entry has no backend
	// to select (for example the target node is not in the peer directory).
	NoBackend func(error) bool
}

// Wrap returns next with access observation. Each OpenStream is one real
// application connection through the entry.
func (o *EntryObserver) Wrap(next service.StreamOpener, cfg OpenerConfig) service.StreamOpener {
	return service.StreamOpenerFunc(func(ctx context.Context, t service.Target) (io.ReadWriteCloser, error) {
		backend, _ := cfg.Resolve(t)
		stream, err := next.OpenStream(ctx, t)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err // the entry is shutting down; nothing was measured
			}
			if cfg.NoBackend != nil && cfg.NoBackend(err) {
				o.record(observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_NO_BACKEND, Backend{}, err)
			} else {
				o.record(observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_UNREACHABLE, backend, err)
			}
			return nil, err
		}
		// Logical entries learn their backend only while opening.
		if rb, ok := stream.(interface{ ResolvedBackend() (string, string) }); ok {
			backend.NodeID, backend.Address = rb.ResolvedBackend()
		}
		return &observedStream{ReadWriteCloser: stream, observer: o, backend: backend}, nil
	})
}

// record applies one measured outcome. An event is appended when the state,
// the backend or the error text changes; a repeated identical error only
// refreshes last_error_time in the snapshot.
func (o *EntryObserver) record(state observationv1alpha1.ServiceAccessState, backend Backend, cause error) {
	r := o.r
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.services[o.name]
	if current == nil {
		current = &observationv1alpha1.ServiceAccessChanged{ServiceName: o.name}
	}
	next := proto.Clone(current).(*observationv1alpha1.ServiceAccessChanged)
	next.State = &state
	next.BackendNodeId = backend.NodeID
	next.BackendAddress = backend.Address
	next.PreviousBackendNodeId = ""
	if cause != nil {
		next.LastError = cause.Error()
		next.LastErrorTime = timestampOrNil(r.now())
	}
	r.services[o.name] = next

	changed := current.State == nil || current.GetState() != state ||
		current.GetBackendNodeId() != next.GetBackendNodeId() ||
		current.GetBackendAddress() != next.GetBackendAddress() ||
		current.GetLastError() != next.GetLastError()
	if !changed {
		return
	}
	event := proto.Clone(next).(*observationv1alpha1.ServiceAccessChanged)
	if prev := current.GetBackendNodeId(); prev != "" && next.GetBackendNodeId() != "" && prev != next.GetBackendNodeId() {
		event.PreviousBackendNodeId = prev
	}
	r.log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_ServiceAccessChanged{ServiceAccessChanged: event}})
}

// errClosedBeforeData explains an UNREACHABLE caused by the provider closing
// the stream without returning any backend byte (tunnel.Server does this when
// it cannot dial the exported local address).
var errClosedBeforeData = errors.New("provider closed the stream before any backend data")

// observedStream reports REACHABLE when the first backend byte arrives, and
// UNREACHABLE when the provider ends the stream before any byte while the
// entry still had it open. A stream the entry closed first proves nothing.
type observedStream struct {
	io.ReadWriteCloser
	observer *EntryObserver
	backend  Backend

	received    atomic.Bool
	localClosed atomic.Bool
	reported    sync.Once
}

func (s *observedStream) Read(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Read(p)
	if n > 0 {
		if s.received.CompareAndSwap(false, true) {
			s.reported.Do(func() {
				s.observer.record(observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_REACHABLE, s.backend, nil)
			})
		}
		return n, err
	}
	if err != nil && !s.received.Load() && !s.localClosed.Load() {
		s.reported.Do(func() {
			cause := errClosedBeforeData
			if !errors.Is(err, io.EOF) {
				cause = fmt.Errorf("%w: %v", errClosedBeforeData, err)
			}
			s.observer.record(observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_UNREACHABLE, s.backend, cause)
		})
	}
	return n, err
}

func (s *observedStream) Close() error {
	s.localClosed.Store(true)
	return s.ReadWriteCloser.Close()
}

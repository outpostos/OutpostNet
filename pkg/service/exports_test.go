package service_test

import (
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

type recordingRoutes struct {
	mu     sync.Mutex
	routes map[string]string
}

func (r *recordingRoutes) AddRoute(name, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.routes == nil {
		r.routes = map[string]string{}
	}
	r.routes[name] = addr
}

func (r *recordingRoutes) RemoveRoute(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.routes, name)
}

func exp(name, addr string) agentconfig.ExportConfig {
	return agentconfig.ExportConfig{Name: name, LocalAddr: addr, Protocol: "tcp"}
}

func TestRegisterExports(t *testing.T) {
	rt := &recordingRoutes{}
	service.RegisterExports(rt, []agentconfig.ExportConfig{exp("a", "127.0.0.1:1"), exp("b", "127.0.0.1:2")})
	want := map[string]string{"a": "127.0.0.1:1", "b": "127.0.0.1:2"}
	if !reflect.DeepEqual(rt.routes, want) {
		t.Fatalf("routes = %v, want %v", rt.routes, want)
	}
}

func TestApplyExportsAddsUpdatesAndRemoves(t *testing.T) {
	rt := &recordingRoutes{}
	old := []agentconfig.ExportConfig{exp("keep", "127.0.0.1:1"), exp("move", "127.0.0.1:2"), exp("gone", "127.0.0.1:3")}
	service.RegisterExports(rt, old)

	next := []agentconfig.ExportConfig{exp("keep", "127.0.0.1:1"), exp("move", "127.0.0.1:20"), exp("new", "127.0.0.1:4")}
	ch := service.ApplyExports(rt, old, next)

	want := map[string]string{"keep": "127.0.0.1:1", "move": "127.0.0.1:20", "new": "127.0.0.1:4"}
	if !reflect.DeepEqual(rt.routes, want) {
		t.Fatalf("routes = %v, want %v", rt.routes, want)
	}
	sort.Strings(ch.Added)
	sort.Strings(ch.Updated)
	sort.Strings(ch.Removed)
	if !reflect.DeepEqual(ch, service.ExportChanges{Added: []string{"new"}, Updated: []string{"move"}, Removed: []string{"gone"}}) {
		t.Fatalf("changes = %+v", ch)
	}
}

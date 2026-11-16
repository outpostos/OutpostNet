package servicehost_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/internal/observation/observationtest"
	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Decode the deployed API independently of its implementation types.
type directoryView struct {
	Configured bool `json:"configured"`
	Synced     bool `json:"synced"`
	Stale      bool `json:"stale"`
	Stats      struct {
		Connects       uint64 `json:"connects"`
		Snapshots      uint64 `json:"snapshots"`
		WriterRejected uint64 `json:"writer_rejected"`
		LeaseExpired   uint64 `json:"lease_expired"`
	} `json:"stats"`
	Leases []struct {
		NodeID string   `json:"node_id"`
		State  string   `json:"state"`
		Seq    uint64   `json:"lease_seq,string"`
		Age    *float64 `json:"last_advance_age_seconds"`
		TTL    float64  `json:"lease_ttl_seconds"`
	} `json:"leases"`
}

func TestDirectoryObservationAndLeaseRecovery(t *testing.T) {
	mesh := newFakeMesh(t)
	app := startCountingApp(t, "lease")
	cfg := logicalNode(t, "node-a", freeAddr(t), mesh.url(), nil)
	host := startObservedHost(t, validated(t, cfg), "127.0.0.1:0", &logBuffer{})
	client := unixHTTP(cfg.Management.SocketPath)
	defer client.CloseIdleConnections()
	sub, err := observationtest.Subscribe(context.Background(), observationURL(t, host), observationv1alpha1.Component_COMPONENT_OUTPOSTSERVICE)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	metric := func(name string, value float64, state string) func(*observationv1alpha1.Event) bool {
		return func(e *observationv1alpha1.Event) bool {
			m := e.GetMetricSample()
			return m != nil && m.Value != nil && m.GetName() == name && m.GetValue() == value &&
				(state == "" || (m.Labels["lease_node_id"] == "node-a" && m.Labels["state"] == state))
		}
	}
	mustEvent(t, sub, "initial directory counters", metric("service_directory_writer_rejected_total", 0, ""))
	forged := reg{service: "forged-echo", uid: "forged-pod", node: "node-c", port: 8080, live: true}.json()
	forged = strings.Replace(forged, `"originNodeId":"node-c"`, `"originNodeId":"node-b"`, 1)
	publishDirectoryRecord(t, mesh, forged)
	waitFor(t, 5*time.Second, "registration rejection endpoint", func() bool { return readDirectory(t, client).Stats.WriterRejected == 1 })
	mustEvent(t, sub, "registration rejection stream", metric("service_directory_writer_rejected_total", 1, ""))
	publishDirectoryRecord(t, mesh, leaseRecord("node-c", "node-b", 1))
	waitFor(t, 5*time.Second, "lease rejection endpoint", func() bool { return readDirectory(t, client).Stats.WriterRejected == 2 })
	mustEvent(t, sub, "lease rejection stream", metric("service_directory_writer_rejected_total", 2, ""))

	const seq = uint64(9007199254740993)
	publishDirectoryRecord(t, mesh, fmt.Sprintf(`{"entityId":"urn:outpostos:service-instance:node-a~echo~pod","isLive":true,"originNodeId":"node-a","serviceRegistration":{"serviceName":"echo","instanceId":"pod","nodeId":"node-a","registrarIncarnation":"registrar-secret-marker","healthState":"SERVICE_HEALTH_STATE_READY","endpoints":[{"port":{"protocol":"ENDPOINT_PROTOCOL_TCP","port":%d}}]}}`, portOf(t, app.addr)))
	waitFor(t, 5*time.Second, "missing lease visible", func() bool {
		v := readDirectory(t, client)
		return len(v.Leases) == 1 && v.Leases[0].State == "missing" && v.Leases[0].Age == nil
	})
	mustEvent(t, sub, "missing lease stream", metric("service_directory_lease_valid", 0, "missing"))
	publishDirectoryRecord(t, mesh, leaseRecord("node-a", "node-a", seq))
	waitFor(t, 5*time.Second, "valid lease visible", func() bool {
		v := readDirectory(t, client)
		return len(v.Leases) == 1 && v.Leases[0].NodeID == "node-a" && v.Leases[0].State == "valid" && v.Leases[0].Seq == seq && v.Leases[0].TTL == 30 && v.Leases[0].Age != nil && *v.Leases[0].Age >= 0
	})
	valid := mustEvent(t, sub, "valid lease stream", metric("service_directory_lease_valid", 1, "valid"))
	askEventually(t, cfg.Imports[0].ListenAddr, "lease:hello")
	waitFor(t, 35*time.Second, "lease expires without deleting registration", func() bool {
		v := readDirectory(t, client)
		return len(v.Leases) == 1 && v.Leases[0].State == "expired" && v.Leases[0].Age != nil && *v.Leases[0].Age > 30 && v.Stats.LeaseExpired == 1
	})
	mustEvent(t, sub, "lease expiry counter", metric("service_directory_lease_expired_total", 1, ""))
	expired := mustEvent(t, sub, "expired lease stream", metric("service_directory_lease_valid", 0, "expired"))
	if expired.GetSeq() <= valid.GetSeq() {
		t.Fatal("lease events out of order")
	}
	probe, err := dialEntry(cfg.Imports[0].ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	_, probeErr := probe.ask("expired")
	probe.c.Close()
	if probeErr == nil {
		t.Fatal("expired lease still routes application connections")
	}
	publishDirectoryRecord(t, mesh, leaseRecord("node-a", "node-a", seq+1))
	waitFor(t, 5*time.Second, "lease recovers", func() bool {
		v := readDirectory(t, client)
		return len(v.Leases) == 1 && v.Leases[0].State == "valid" && v.Leases[0].Seq == seq+1 && v.Leases[0].Age != nil && *v.Leases[0].Age < 5 && v.Stats.LeaseExpired == 1
	})
	mustEvent(t, sub, "recovery stream", func(e *observationv1alpha1.Event) bool {
		return e.GetSeq() > expired.GetSeq() && metric("service_directory_lease_valid", 1, "valid")(e)
	})
	askEventually(t, cfg.Imports[0].ListenAddr, "lease:hello")
	assertDirectoryPrivate(t, client, sub)
	if sub.Err() != nil {
		t.Fatal(sub.Err())
	}
}

func assertDirectoryPrivate(t *testing.T, client *http.Client, sub *observationtest.Subscription) {
	t.Helper()
	resp, err := client.Get("http://unix/v1/directory")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	outputs := []string{string(body)}
	for _, e := range sub.Events() {
		if m := e.GetMetricSample(); m != nil && strings.HasPrefix(m.GetName(), "service_directory_") {
			b, err := protojson.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			outputs = append(outputs, string(b))
		}
	}
	for _, output := range outputs {
		for _, secret := range []string{token, "registrar-secret-marker", "payload-secret-marker", "upstream-secret-marker", "credential-secret-marker", "last_resume_cursor", "last_connect_error", "serviceRegistration", "endpoints"} {
			if strings.Contains(output, secret) {
				t.Fatalf("directory surface leaked forbidden field or marker %q", secret)
			}
		}
	}
	var view struct {
		Stats map[string]uint64 `json:"stats"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"connects", "snapshots", "deltas", "malformed", "expired", "lease_expired", "writer_rejected", "cache_writes", "cache_write_errors"} {
		if _, ok := view.Stats[name]; !ok {
			t.Fatalf("missing counter %q", name)
		}
		mustEvent(t, sub, "counter sample "+name, func(e *observationv1alpha1.Event) bool {
			m := e.GetMetricSample()
			return m != nil && m.Value != nil && m.GetName() == "service_directory_"+name+"_total"
		})
	}
}

func TestDirectoryStatusUnavailableGatewayIsStaleAndPrivate(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream-secret-marker", http.StatusServiceUnavailable)
	}))
	defer gateway.Close()
	url := strings.Replace(gateway.URL, "http://", "http://user:credential-secret-marker@", 1)
	cfg := logicalNode(t, "node-a", freeAddr(t), url, nil)
	logs := &logBuffer{}
	host := startObservedHost(t, validated(t, cfg), "127.0.0.1:0", logs)
	client := unixHTTP(cfg.Management.SocketPath)
	defer client.CloseIdleConnections()
	waitFor(t, 5*time.Second, "upstream failure recorded", func() bool { return strings.Contains(logs.String(), "mesh entity stream ended") })
	v := readDirectory(t, client)
	if !v.Configured || !v.Stale || v.Synced || len(v.Leases) != 0 {
		t.Fatalf("unavailable gateway view: %+v", v)
	}
	sub, err := observationtest.Subscribe(context.Background(), observationURL(t, host), observationv1alpha1.Component_COMPONENT_OUTPOSTSERVICE)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	assertDirectoryPrivate(t, client, sub)
}

func TestDirectoryStatusWithoutMesh(t *testing.T) {
	cfg := exportingNode(t, "node-a", freeAddr(t), token)
	startHost(t, validated(t, cfg))
	client := unixHTTP(cfg.Management.SocketPath)
	defer client.CloseIdleConnections()
	v := readDirectory(t, client)
	if v.Configured || v.Synced || len(v.Leases) != 0 || v.Stats.WriterRejected != 0 {
		t.Fatalf("disabled directory: %+v", v)
	}
}

func readDirectory(t *testing.T, client *http.Client) directoryView {
	t.Helper()
	resp, err := client.Get("http://unix/v1/directory")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/directory: status %d", resp.StatusCode)
	}
	var view directoryView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatal(err)
	}
	return view
}

func publishDirectoryRecord(t *testing.T, mesh *fakeMesh, entity string) {
	t.Helper()
	var id struct {
		EntityID string `json:"entityId"`
	}
	if err := json.Unmarshal([]byte(entity), &id); err != nil {
		t.Fatal(err)
	}
	mesh.mu.Lock()
	defer mesh.mu.Unlock()
	mesh.state[id.EntityID] = entity
	mesh.broadcast("ENTITY_EVENT_TYPE_PUBLISHED", entity)
}

func leaseRecord(node, writer string, seq uint64) string {
	return fmt.Sprintf(`{"entityId":%q,"isLive":true,"originNodeId":%q,"nodeLease":{"leaseSeq":%q,"registrarIncarnation":"registrar-secret-marker"},"unrelatedPayload":"payload-secret-marker"}`, "urn:outpostos:node-lease:"+node, writer, fmt.Sprint(seq))
}

func TestDirectoryManagementRejectsForgedWriters(t *testing.T) {
	mesh := newFakeMesh(t)
	cfg := logicalNode(t, "node-a", freeAddr(t), mesh.url(), nil)
	host := startHost(t, validated(t, cfg))
	client := unixHTTP(cfg.Management.SocketPath)
	defer client.CloseIdleConnections()
	waitFor(t, 5*time.Second, "directory snapshot exposed", func() bool {
		v := readDirectory(t, client)
		return v.Configured && v.Synced && !v.Stale && v.Stats.Connects == 1 && v.Stats.Snapshots == 1
	})

	forged := reg{service: "forged-echo", uid: "forged-pod", node: "node-c", port: 8080, live: true}.json()
	forged = strings.Replace(forged, `"originNodeId":"node-c"`, `"originNodeId":"node-b"`, 1)
	publishDirectoryRecord(t, mesh, forged)
	waitFor(t, 5*time.Second, "forged registration counted", func() bool { return readDirectory(t, client).Stats.WriterRejected == 1 })
	publishDirectoryRecord(t, mesh, leaseRecord("node-c", "node-b", 1))
	waitFor(t, 5*time.Second, "forged lease counted", func() bool { return readDirectory(t, client).Stats.WriterRejected == 2 })
	if v := readDirectory(t, client); len(v.Leases) != 0 {
		t.Fatalf("forged lease trusted: %+v", v)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		req, _ := http.NewRequest(method, "http://unix/v1/directory", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s /v1/directory: %d", method, resp.StatusCode)
		}
	}
	resp, err := http.Get("http://" + host.h.HealthAddr().String() + "/v1/directory")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("directory exposed on health TCP listener: %d", resp.StatusCode)
	}

	// Restart with a clean upstream snapshot: counters are per process, and
	// the accepted lease is treated as just seen by the new directory.
	host.stop(t)
	mesh.mu.Lock()
	mesh.state = map[string]string{}
	mesh.mu.Unlock()
	publishDirectoryRecord(t, mesh, leaseRecord("node-a", "node-a", 7))
	startHost(t, validated(t, cfg))
	waitFor(t, 5*time.Second, "directory status after restart", func() bool {
		v := readDirectory(t, client)
		return v.Synced && v.Stats.WriterRejected == 0 && v.Stats.Snapshots == 1 &&
			len(v.Leases) == 1 && v.Leases[0].State == "valid" && v.Leases[0].Seq == 7 && v.Leases[0].Age != nil && *v.Leases[0].Age < 5
	})
}

// A burst of directory deltas must not emit a full metric set per delta.
func TestDirectoryObservationCoalescesBurst(t *testing.T) {
	mesh := newFakeMesh(t)
	cfg := logicalNode(t, "node-a", freeAddr(t), mesh.url(), nil)
	host := startObservedHost(t, validated(t, cfg), "127.0.0.1:0", &logBuffer{})
	client := unixHTTP(cfg.Management.SocketPath)
	defer client.CloseIdleConnections()
	waitFor(t, 5*time.Second, "directory synced", func() bool { return readDirectory(t, client).Synced })
	sub, err := observationtest.Subscribe(context.Background(), observationURL(t, host), observationv1alpha1.Component_COMPONENT_OUTPOSTSERVICE)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := range 100 {
		publishDirectoryRecord(t, mesh, leaseRecord("node-c", "node-b", uint64(i+1)))
		time.Sleep(time.Millisecond)
	}
	waitFor(t, 5*time.Second, "all rejections counted", func() bool { return readDirectory(t, client).Stats.WriterRejected == 100 })
	mustEvent(t, sub, "coalesced counter", func(e *observationv1alpha1.Event) bool {
		return e.GetMetricSample().GetName() == "service_directory_writer_rejected_total" && e.GetMetricSample().GetValue() == 100
	})
	var samples []*observationv1alpha1.Event
	for _, e := range sub.Events() {
		if e.GetMetricSample().GetName() == "service_directory_writer_rejected_total" {
			samples = append(samples, e)
		}
	}
	for i := 1; i < len(samples); i++ {
		if elapsed := samples[i].GetNodeTime().AsTime().Sub(samples[i-1].GetNodeTime().AsTime()); elapsed < 900*time.Millisecond {
			t.Fatalf("counter samples emitted %s apart during burst, want periodic coalescing", elapsed)
		}
	}
}

//go:build integration

package meshdirectory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestRealMeshGateway is P1-NET-2 acceptance 4. Run with
//
//	OUTPOSTMESH_GATEWAY_BIN=/path/to/outpostmesh-gateway go test -tags integration ./internal/meshdirectory/
func TestRealMeshGateway(t *testing.T) {
	bin := os.Getenv("OUTPOSTMESH_GATEWAY_BIN")
	if bin == "" {
		t.Skip("OUTPOSTMESH_GATEWAY_BIN not set")
	}
	httpAddr, grpcAddr, obsAddr := freePort(t), freePort(t), freePort(t)
	cmd := exec.Command(bin, "-http-listen", httpAddr, "-listen", grpcAddr, "-observation-listen", obsAddr, "-node-id", "node-a")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	base := "http://" + httpAddr
	eventually(t, "gateway up", func() bool {
		resp, err := http.Get(base + "/v1alpha1/entities")
		if err == nil {
			resp.Body.Close()
		}
		return err == nil
	})

	h := startDir(t, Config{GatewayURL: base})
	eventually(t, "initial snapshot", func() bool { _, fr := h.d.Instances("echo"); return fr.Synced })

	put := func(uid string, live bool) time.Time {
		body := entityJSON(id(uid), "echo", uid, ready, live, time.Now().Add(time.Minute), 100)
		body = strings.Replace(body, `"provenance":{"integrationName":"outpostlet"}`,
			fmt.Sprintf(`"provenance":{"integrationName":"outpostlet","sourceUpdateTime":%q}`, time.Now().UTC().Format(time.RFC3339Nano)), 1)
		req, _ := http.NewRequest(http.MethodPut, base+"/v1alpha1/entities/"+id(uid), strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("PUT %s: %d", uid, resp.StatusCode)
		}
		return time.Now()
	}

	start := put("a", true)
	waitWithin(t, 2*time.Second, start, "registration visible", hasState(h.d, "a/READY"))
	start = put("a", false)
	waitWithin(t, 2*time.Second, start, "withdrawal applied", hasState(h.d, ""))
}

func waitWithin(t *testing.T, limit time.Duration, start time.Time, what string, cond func() bool) {
	t.Helper()
	for !cond() {
		if time.Since(start) > limit {
			t.Fatalf("%s not within %s", what, limit)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("%s after %s", what, time.Since(start).Round(time.Millisecond))
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

var _ = context.Background

// TestRealMeshLeaseDirectory validates the actual REST/SSE wire boundary,
// including gateway-derived originNodeId, no-expiry leases, writer rejection
// and expiry while the gateway remains available (registrar stops renewing).
func TestRealMeshLeaseDirectory(t *testing.T) {
	bin := os.Getenv("OUTPOSTMESH_GATEWAY_BIN")
	if bin == "" {
		t.Skip("OUTPOSTMESH_GATEWAY_BIN not set")
	}
	base := "http://" + freePort(t)
	cmd := exec.Command(bin, "-http-listen", strings.TrimPrefix(base, "http://"), "-listen", freePort(t), "-observation-listen", freePort(t), "-node-id", "node-a")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	eventually(t, "gateway up", func() bool {
		resp, err := http.Get(base + "/v1alpha1/entities")
		if err == nil {
			resp.Body.Close()
		}
		return err == nil
	})
	put := func(body string) {
		t.Helper()
		var entity map[string]json.RawMessage
		if err := json.Unmarshal([]byte(body), &entity); err != nil {
			t.Fatal(err)
		}
		stamp := fmt.Sprintf("%q", time.Now().UTC().Format(time.RFC3339Nano))
		entity["createdTime"] = json.RawMessage(stamp)
		entity["provenance"] = json.RawMessage(`{"integrationName":"outpostlet","sourceUpdateTime":` + stamp + `}`)
		encoded, _ := json.Marshal(entity)
		var eid string
		json.Unmarshal(entity["entityId"], &eid)
		req, _ := http.NewRequest(http.MethodPut, base+"/v1alpha1/entities/"+eid, bytes.NewReader(encoded))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("publish: %d %s", resp.StatusCode, body)
		}
	}
	h := startDir(t, Config{GatewayURL: base, LeaseTTL: 500 * time.Millisecond, Now: func() time.Time { return time.Now().Add(60 * time.Second) }})
	eventually(t, "initial snapshot", func() bool { _, fr := h.d.Instances("echo"); return fr.Synced })
	put(leasedInstance("a", "boot"))
	put(leasedInstance("b", "boot"))
	put(leaseJSON(1, "boot"))
	eventually(t, "both leased registrations", hasState(h.d, "a/READY,b/READY"))
	eventually(t, "stopped renewals expire all registrations", hasState(h.d, ""))
	put(leaseJSON(2, "boot"))
	eventually(t, "renewal restores original registrations", hasState(h.d, "a/READY,b/READY"))
	// The caller attempts to claim writer node-b; Mesh overwrites it with node-a.
	forged := strings.Replace(leasedInstance("forged", "boot"), `"nodeId":"node-a"`, `"nodeId":"node-b"`, 1)
	forged = strings.Replace(forged, `"originNodeId":"node-a"`, `"originNodeId":"node-b"`, 1)
	put(forged)
	eventually(t, "writer mismatch counted", func() bool { return h.d.Stats().WriterRejected == 1 })
	got, _ := h.d.Instances("echo")
	for _, in := range got {
		if in.InstanceID == "forged" {
			t.Fatal("forged placement accepted")
		}
	}
}

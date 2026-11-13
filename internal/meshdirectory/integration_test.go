//go:build integration

package meshdirectory

import (
	"context"
	"fmt"
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

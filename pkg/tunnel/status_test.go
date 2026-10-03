package tunnel

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// TestManagedClientStatusReportsLatestFailure checks the read-only status
// observers use: the latest dial/auth failure and its time are kept (without
// the token) after the client later connects.
func TestManagedClientStatusReportsLatestFailure(t *testing.T) {
	const secret = "status-secret-token"
	_, srvAddr := newTestServerWithToken(t, secret)

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr: srvAddr,
			TLS:        tlsutil.ClientTLSConfig(true),
			Token:      "wrong-" + secret,
		},
	}, nil)
	defer mc.Close()

	initial := mc.Status()
	if initial.State != StateDisconnected || initial.ServerAddr != srvAddr || initial.LastError != "" || !initial.LastErrorTime.IsZero() {
		t.Fatalf("initial status = %+v", initial)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before := time.Now()
	if err := mc.Connect(ctx); err == nil {
		t.Fatal("expected authentication failure")
	}
	failed := mc.Status()
	if failed.State != StateDisconnected || failed.LastError == "" || failed.LastErrorTime.Before(before) {
		t.Fatalf("failed status = %+v", failed)
	}
	if strings.Contains(failed.LastError, secret) {
		t.Fatalf("status leaks the token: %q", failed.LastError)
	}

	mc.mu.Lock()
	mc.cfg.Token = secret
	mc.mu.Unlock()
	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("Connect with the right token: %v", err)
	}
	connected := mc.Status()
	if connected.State != StateConnected {
		t.Fatalf("connected status = %+v", connected)
	}
	if connected.LastError != failed.LastError || !connected.LastErrorTime.Equal(failed.LastErrorTime) {
		t.Fatalf("latest failure must stay reported after connecting: %+v", connected)
	}
}

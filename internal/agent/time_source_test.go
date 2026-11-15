package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestAgentOSTimeSourceStatus(t *testing.T) {
	data, err := os.ReadFile("../../pkg/state/testdata/time-source/chrony-node1.csv")
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	cfg := observedAgentConfig(t, "time-node", nil)
	a := New(cfg, slog.New(slog.NewTextHandler(logs, nil)))
	a.timeSourceConfig = state.OSTimeSourceConfig{Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) { return data, nil }}
	a.EnableObservation("127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("agent failed to stop")
		}
	})
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.Management.SocketPath)
	}}}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.Get("http://agent/v1/time")
		if err == nil {
			var status state.TimeStatus
			err = json.NewDecoder(resp.Body).Decode(&status)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 || status.Quality != state.TimeSynced || status.Source.Source != "chrony:202.118.1.130" {
				t.Fatalf("time status: %+v, %v", status, err)
			}
			if a.TimeReference().TimeStatus().Quality != state.TimeSynced {
				t.Fatal("agent reference differs from status")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("time status unavailable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

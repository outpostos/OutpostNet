package app

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func chronyFixture(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		c := NewAgentCommand()
		c.SetArgs(args)
		var out bytes.Buffer
		c.SetOut(&out)
		c.SetErr(&out)
		if err := c.Execute(); err != nil {
			t.Fatal(err)
		}
	}
	ca, caKey := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key")
	cert, key := filepath.Join(dir, "node.pem"), filepath.Join(dir, "node.key")
	run("cert", "ca", "--cert", ca, "--key", caKey)
	run("cert", "issue", "--ca", ca, "--ca-key", caKey, "--node-id", "a", "--cert", cert, "--key", key, "--ip", "192.0.2.1")
	config := filepath.Join(dir, "agent.yaml")
	peers := filepath.Join(dir, "peers.json")
	os.WriteFile(config, []byte("node_id: a\ntls:\n  ca_file: "+ca+"\n  cert_file: "+cert+"\n  key_file: "+key+"\n"), 0600)
	os.WriteFile(peers, []byte(`[{"node_id":"b","addr":"192.0.2.2:4242"}]`), 0600)
	return dir, config, peers
}

func TestChronyCLIRenderAndRejectInvalidCredentials(t *testing.T) {
	dir, config, peers := chronyFixture(t)
	run := func() (string, error) {
		c := NewAgentCommand()
		c.SetArgs([]string{"chrony", "--config", config, "--peers", peers})
		var b bytes.Buffer
		c.SetOut(&b)
		c.SetErr(&b)
		err := c.Execute()
		return b.String(), err
	}
	out, err := run()
	if err != nil || !strings.Contains(out, "server 192.0.2.2 iburst nts certset 1") {
		t.Fatalf("%s %v", out, err)
	}
	if strings.Contains(out, "PRIVATE KEY") {
		t.Fatal("key leaked")
	}
	os.WriteFile(filepath.Join(dir, "node.key"), []byte("corrupt"), 0600)
	if _, err := run(); err == nil {
		t.Fatal("rendered invalid credentials")
	}
}

func TestChronyCLIWatchAppliesPeerAndCertificateChangesAndRetries(t *testing.T) {
	dir, config, peers := chronyFixture(t)
	// External process boundaries; no real host service is touched.
	bin := filepath.Join(dir, "bin")
	os.Mkdir(bin, 0700)
	for name, script := range map[string]string{
		"chronyd":   "#!/bin/sh\n[ ! -e \"$CHRONY_TEST_DIR/reject\" ]\n",
		"systemctl": "#!/bin/sh\n[ \"$1\" = restart ] || exit 2\n[ \"$2\" = chrony.service ] || exit 2\nprintf 'restart\\n' >> \"$CHRONY_TEST_DIR/restarts\"\n[ ! -e \"$CHRONY_TEST_DIR/fail\" ]\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CHRONY_TEST_DIR", dir)
	output := filepath.Join(dir, "group.conf")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := NewAgentCommand()
	cmd.SetArgs([]string{"chrony", "--config", config, "--peers", peers, "--output", output, "--watch", "--interval", "20ms", "--apply"})
	// Avoid reading the command's buffers concurrently with its watch loop.
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()
	wait := func(f func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if f() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("watch did not converge")
	}
	count := func() int {
		b, _ := os.ReadFile(filepath.Join(dir, "restarts"))
		return strings.Count(string(b), "restart")
	}
	wait(func() bool { return count() == 1 })
	// Unchanged inputs cause no repeated restart.
	time.Sleep(80 * time.Millisecond)
	if count() != 1 {
		t.Fatal("restarted unchanged inputs")
	}
	os.WriteFile(filepath.Join(dir, "reject"), nil, 0600)
	os.WriteFile(peers, []byte(`[{"node_id":"c","addr":"192.0.2.3:4242"}]`), 0600)
	time.Sleep(80 * time.Millisecond)
	b, _ := os.ReadFile(output)
	if !strings.Contains(string(b), "192.0.2.2") || count() != 1 {
		t.Fatal("invalid candidate replaced active config")
	}
	os.Remove(filepath.Join(dir, "reject"))
	os.WriteFile(filepath.Join(dir, "fail"), nil, 0600)
	wait(func() bool { return count() >= 2 })
	os.Remove(filepath.Join(dir, "fail"))
	wait(func() bool { return count() >= 3 })
	b, _ = os.ReadFile(output)
	if !strings.Contains(string(b), "192.0.2.3") || strings.Contains(string(b), "192.0.2.2") {
		t.Fatal("stale peer access", string(b))
	}
	// Same paths, changed certificate contents must also trigger reapplication.
	time.Sleep(60 * time.Millisecond)
	before := count()
	cert := filepath.Join(dir, "node.pem")
	b, _ = os.ReadFile(cert)
	os.WriteFile(cert, append(b, '\n'), 0644)
	wait(func() bool { return count() > before })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not stop")
	}
}

func TestChronyCLIRendersLiveNetPeerSet(t *testing.T) {
	dir, config, _ := chronyFixture(t)
	socket := filepath.Join(dir, "net.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/peers" || fail.Load() {
			http.Error(w, "unavailable", 503)
			return
		}
		fmt.Fprint(w, `[{"node_id":"b","addr":"192.0.2.5:4242","status":"pending"}]`)
	})}
	go server.Serve(listener)
	defer server.Close()
	run := func() (string, error) {
		c := NewAgentCommand()
		c.SetArgs([]string{"chrony", "--config", config, "--socket", socket})
		var b bytes.Buffer
		c.SetOut(&b)
		c.SetErr(&b)
		err := c.Execute()
		return b.String(), err
	}
	out, err := run()
	if err != nil || !strings.Contains(out, "server 192.0.2.5 iburst nts certset 1") {
		t.Fatalf("live peer set: %s %v", out, err)
	}
	fail.Store(true)
	if _, err := run(); err == nil {
		t.Fatal("ignored peer directory failure")
	}
}

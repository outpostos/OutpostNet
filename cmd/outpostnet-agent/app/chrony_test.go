package app

import (
	"bytes"
	"crypto/tls"
	"encoding/pem"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func chronyTestCredentials(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()
	caPEM, caKeyPEM, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	caFile = filepath.Join(dir, "group.pem")
	writeTestFile(t, caFile, caPEM)
	caKeyPath := filepath.Join(dir, "group.key")
	writeTestFile(t, caKeyPath, caKeyPEM)
	ca, err := tls.LoadX509KeyPair(caFile, caKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := tlsutil.IssueNodeWithSANs(ca, "node-a", time.Now(),
		[]string{"node-a.edge.local"}, []net.IP{net.ParseIP("10.0.0.5")})
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, "node-a.pem")
	keyFile = filepath.Join(dir, "node-a.key")
	writeTestFile(t, certFile, certPEM)
	writeTestFile(t, keyFile, keyPEM)
	return certFile, keyFile, caFile
}

func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func runChrony(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewAgentCommand()
	var b bytes.Buffer
	cmd.SetOut(&b)
	cmd.SetErr(&b)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return b.String(), err
}

func TestChronyRenderCLI(t *testing.T) {
	certFile, keyFile, caFile := chronyTestCredentials(t)
	base := []string{
		"chrony", "render",
		"--node-id", "node-a", "--cert", certFile, "--key", keyFile, "--ca", caFile,
	}
	out, err := runChrony(t, append(base,
		"--peer", "node-b=10.0.0.6:4433",
		"--peer", "node-c=node-c.edge.local",
		"--allow", "10.0.0.0/24")...)
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	for _, want := range []string{
		"ntsservercert " + certFile,
		"nosystemcert",
		"ntstrustedcerts 1 " + caFile,
		"local stratum 10 orphan",
		"makestep 1.0 3",
		"nocerttimecheck 1",
		"allow 10.0.0.0/24",
		"allow 10.0.0.6",
		"server 10.0.0.6 nts iburst certset 1",
		"server node-c.edge.local nts iburst certset 1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered fragment missing %q:\n%s", want, out)
		}
	}
}

func TestChronyRenderCLISplitSourcesAndGNSS(t *testing.T) {
	certFile, keyFile, caFile := chronyTestCredentials(t)
	dir := t.TempDir()
	main := filepath.Join(dir, "outpostnet.conf")
	sources := filepath.Join(dir, "outpostnet.sources")
	out, err := runChrony(t, "chrony", "render",
		"--node-id", "node-a", "--cert", certFile, "--key", keyFile, "--ca", caFile,
		"--peer", "node-b=10.0.0.6", "--allow", "10.0.0.0/24",
		"--output", main, "--sources-output", sources,
		"--gnss-shm", "0", "--gnss-pps", "/dev/pps0")
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	mainData, err := os.ReadFile(main)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainData), "sourcedir "+dir) {
		t.Fatalf("main fragment missing sourcedir:\n%s", mainData)
	}
	if strings.Contains(string(mainData), "server 10.0.0.6") {
		t.Fatalf("split main fragment contains sources:\n%s", mainData)
	}
	if !strings.Contains(string(mainData), "refclock PPS /dev/pps0 lock GPS refid PPS prefer") {
		t.Fatalf("main fragment missing GNSS template:\n%s", mainData)
	}
	sourceData, err := os.ReadFile(sources)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sourceData), "server 10.0.0.6 nts iburst certset 1") {
		t.Fatalf("sources file missing peer:\n%s", sourceData)
	}
}

func TestChronyRenderCLIPeersFromManagementSocket(t *testing.T) {
	certFile, keyFile, caFile := chronyTestCredentials(t)
	socket := filepath.Join(t.TempDir(), "agent.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/peers" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"node_id":"node-a","addr":"10.0.0.5:4433"},{"node_id":"node-b","addr":"10.0.0.6:4433"}]`))
	})}
	go srv.Serve(ln)
	defer srv.Close()

	out, err := runChrony(t, "chrony", "render",
		"--node-id", "node-a", "--cert", certFile, "--key", keyFile, "--ca", caFile,
		"--peers-socket", socket, "--peer", "node-c=10.0.0.7", "--allow", "10.0.0.0/24")
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	if !strings.Contains(out, "server 10.0.0.6 nts") || !strings.Contains(out, "server 10.0.0.7 nts") {
		t.Fatalf("peer directory peers missing:\n%s", out)
	}
	if strings.Contains(out, "server 10.0.0.5 nts") {
		t.Fatalf("local peer rendered as a source:\n%s", out)
	}
}

func TestChronyRenderCLIRejectsBadInput(t *testing.T) {
	certFile, keyFile, caFile := chronyTestCredentials(t)
	base := []string{"chrony", "render", "--node-id", "node-a", "--cert", certFile, "--key", keyFile, "--ca", caFile}
	if _, err := runChrony(t, append(base, "--sources-output", filepath.Join(t.TempDir(), "peers.conf"))...); err == nil {
		t.Fatal("accepted a sources file without .sources suffix")
	}
	if _, err := runChrony(t, append(base, "--peer", "node-b")...); err == nil {
		t.Fatal("accepted --peer without NODE_ID=HOST")
	}
	if _, err := runChrony(t, append(base, "--gnss-pps", "/dev/pps0")...); err == nil {
		t.Fatal("accepted --gnss-pps without a GNSS source")
	}
	if _, err := runChrony(t, append(base, "--nocerttimecheck", "0")...); err == nil {
		t.Fatal("accepted --nocerttimecheck 0")
	}
}

func TestPeerHost(t *testing.T) {
	tests := []struct {
		in, want string
		wantErr  bool
	}{
		{"10.0.0.6", "10.0.0.6", false},
		{"10.0.0.6:4433", "10.0.0.6", false},
		{"node-b.edge.local:4433", "node-b.edge.local", false},
		{"node-b.edge.local", "node-b.edge.local", false},
		{"[fd00::6]:4433", "fd00::6", false},
		{"fd00::6", "fd00::6", false},
		{"", "", true},
		{"10.0.0.6:notaport", "", true},
	}
	for _, tc := range tests {
		got, err := peerHost(tc.in)
		if (err != nil) != tc.wantErr {
			t.Fatalf("peerHost(%q) err=%v wantErr=%v", tc.in, err, tc.wantErr)
		}
		if !tc.wantErr && got != tc.want {
			t.Fatalf("peerHost(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Ensure the certificate issued by the round-trip helper parses (guards the
// test helper against silent fixture corruption).
func TestChronyTestCredentialsParse(t *testing.T) {
	certFile, _, _ := chronyTestCredentials(t)
	data, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatal("fixture is not a certificate")
	}
}

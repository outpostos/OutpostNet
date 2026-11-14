package servicehost_test

import (
	"crypto/tls"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func TestServiceHostsUseGroupCertificatesAndExposeSecurityCounters(t *testing.T) {
	dir := t.TempDir()
	caPEM, caKey, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := tls.X509KeyPair(caPEM, caKey)
	if err != nil {
		t.Fatal(err)
	}
	configure := func(node string) agentconfig.TLSConfig {
		cert, key, err := tlsutil.IssueNode(ca, node, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		c := filepath.Join(dir, node+".pem")
		k := filepath.Join(dir, node+".key")
		root := filepath.Join(dir, "group.pem")
		for p, data := range map[string][]byte{c: cert, k: key, root: caPEM} {
			if err := os.WriteFile(p, data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		return agentconfig.TLSConfig{CertFile: c, KeyFile: k, CAFile: root}
	}
	bCfg := exportingNode(t, "node-b", startApp(t, "cert-b"), "")
	bCfg.TLS = configure("node-b")
	b := startHost(t, bCfg)
	aCfg := consumingNode(t, "node-a", "127.0.0.1:0", tunnelAddr(t, b), "")
	aCfg.TLS = configure("node-a")
	a := startHost(t, aCfg)
	askEventually(t, a.h.EntryAddrs()[0].String(), "cert-b:hello")
	client := unixHTTP(bCfg.Management.SocketPath)
	resp, err := client.Get("http://unix/v1/security")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("security status=%d", resp.StatusCode)
	}
	var stats map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats["certificate_auth"] != true || stats["claim_rejected"] != float64(0) {
		t.Fatalf("stats=%v", stats)
	}
}

package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestIssueNodeWithSANs(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	caPEM, caKeyPEM, err := GenerateCA(now)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := tls.X509KeyPair(caPEM, caKeyPEM)
	if err != nil {
		t.Fatalf("load CA: %v", err)
	}

	certPEM, keyPEM, err := IssueNodeWithSANs(ca, "node-a", now,
		[]string{"node-a.edge.local", "Node-A.Alt."},
		[]net.IP{net.ParseIP("10.0.0.5"), net.ParseIP("fd00::5")})
	if err != nil {
		t.Fatalf("IssueNodeWithSANs: %v", err)
	}

	leaf := parseLeaf(t, certPEM)
	if want := []string{"node-a.edge.local", "Node-A.Alt"}; !reflect.DeepEqual(leaf.DNSNames, want) {
		t.Fatalf("DNS SANs = %v, want %v", leaf.DNSNames, want)
	}
	if want := []string{"10.0.0.5", "fd00::5"}; !reflect.DeepEqual(ipStrings(leaf.IPAddresses), want) {
		t.Fatalf("IP SANs = %v, want %v", ipStrings(leaf.IPAddresses), want)
	}
	if got, err := NodeID(leaf); err != nil || got != "node-a" {
		t.Fatalf("NodeID = %q, %v", got, err)
	}

	// The SANs must not disturb chain/usage verification, and a node with extra
	// SANs still behaves as an ordinary group member.
	roots := x509.NewCertPool()
	roots.AddCert(parseLeaf(t, caPEM))
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: leaf.NotBefore, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatalf("verify leaf with SANs: %v", err)
	}
	if err := leaf.VerifyHostname("10.0.0.5"); err != nil {
		t.Fatalf("verify hostname by IP: %v", err)
	}
	if err := leaf.VerifyHostname("node-a.edge.local"); err != nil {
		t.Fatalf("verify hostname by DNS: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "node.pem")
	keyFile := filepath.Join(dir, "node.key")
	caFile := filepath.Join(dir, "ca.pem")
	for path, data := range map[string][]byte{certFile: certPEM, keyFile: keyPEM, caFile: caPEM} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	if _, err := LoadIdentity(certFile, keyFile, caFile, "node-a", nil); err != nil {
		t.Fatalf("LoadIdentity with SANs: %v", err)
	}
}

func TestIssueNodeWithoutSANs(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	caPEM, caKeyPEM, err := GenerateCA(now)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := tls.X509KeyPair(caPEM, caKeyPEM)
	if err != nil {
		t.Fatalf("load CA: %v", err)
	}
	certPEM, _, err := IssueNode(ca, "node-a", now)
	if err != nil {
		t.Fatalf("IssueNode: %v", err)
	}
	leaf := parseLeaf(t, certPEM)
	if len(leaf.DNSNames) != 0 || len(leaf.IPAddresses) != 0 {
		t.Fatalf("unexpected SANs: dns=%v ip=%v", leaf.DNSNames, leaf.IPAddresses)
	}
}

func TestIssueNodeRejectsInvalidSANs(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	caPEM, caKeyPEM, err := GenerateCA(now)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := tls.X509KeyPair(caPEM, caKeyPEM)
	if err != nil {
		t.Fatalf("load CA: %v", err)
	}
	cases := []struct {
		name string
		dns  []string
		ips  []net.IP
	}{
		{"empty dns", []string{""}, nil},
		{"blank dns", []string{"   "}, nil},
		{"space in dns", []string{"bad name"}, nil},
		{"ip literal as dns", []string{"10.0.0.5"}, nil},
		{"leading hyphen", []string{"-bad.local"}, nil},
		{"duplicate dns case-insensitive", []string{"a.local", "A.LOCAL"}, nil},
		{"nil ip", nil, []net.IP{nil}},
		{"duplicate mapped ip", nil, []net.IP{net.ParseIP("10.0.0.5"), net.ParseIP("::ffff:10.0.0.5")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := IssueNodeWithSANs(ca, "node-a", now, tc.dns, tc.ips); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func parseLeaf(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("no certificate PEM")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return leaf
}

func ipStrings(ips []net.IP) []string {
	out := make([]string, len(ips))
	for i, ip := range ips {
		out[i] = ip.String()
	}
	return out
}

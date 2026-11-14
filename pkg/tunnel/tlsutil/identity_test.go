package tlsutil_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func credentials(t *testing.T, names []string, usage x509.ExtKeyUsage) (tls.Certificate, []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-group"}, NotBefore: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2045, 1, 1, 0, 0, 0, 0, time.UTC), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := x509.ParseCertificate(der)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	for _, name := range names {
		u, _ := url.Parse(name)
		leaf.URIs = append(leaf.URIs, u)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, root, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestNodeVerificationIgnoresWallClockAndWarns(t *testing.T) {
	cert, ca := credentials(t, []string{"urn:outpostos:node:node-a"}, x509.ExtKeyUsageAny)
	var logs bytes.Buffer
	id, err := tlsutil.NewIdentity(cert, ca, "node-a", slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, now := range []time.Time{time.Unix(0, 0), time.Date(2036, 1, 1, 0, 0, 0, 0, time.UTC)} {
		cfg := id.TLSConfig(false)
		cfg.Time = func() time.Time { return now }
		if err := cfg.VerifyPeerCertificate(cert.Certificate, nil); err != nil {
			t.Fatalf("%v: %v", now, err)
		}
	}
	if got := id.Stats().ValidityWarnings; got != 2 {
		t.Fatalf("warnings=%d, want 2", got)
	}
	if !bytes.Contains(logs.Bytes(), []byte("outside validity window")) {
		t.Fatal("missing validity warning")
	}
}

func TestIdentityRejectsUntrustedMalformedAndWrongUsage(t *testing.T) {
	cert, ca := credentials(t, []string{"urn:outpostos:node:node-a"}, x509.ExtKeyUsageAny)
	id, err := tlsutil.NewIdentity(cert, ca, "node-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	foreign, _ := credentials(t, []string{"urn:outpostos:node:node-b"}, x509.ExtKeyUsageAny)
	for _, raw := range [][][]byte{nil, foreign.Certificate, {{1, 2, 3}}} {
		if err := id.TLSConfig(true).VerifyPeerCertificate(raw, nil); err == nil {
			t.Fatal("invalid certificate accepted")
		}
	}
	if id.Stats().CertificateRejected != 3 {
		t.Fatalf("stats=%+v", id.Stats())
	}
	for _, uris := range [][]string{{}, {"urn:outpostos:node:"}, {"urn:outpostos:node:a", "urn:outpostos:node:b"}, {"urn:outpostos:node:a", "urn:outpostos:node:a"}} {
		c, roots := credentials(t, uris, x509.ExtKeyUsageAny)
		if _, err := tlsutil.NewIdentity(c, roots, "a", nil); err == nil {
			t.Fatalf("accepted URIs %v", uris)
		}
	}
	if _, err := tlsutil.NewIdentity(cert, ca, "forged", nil); err == nil {
		t.Fatal("accepted local identity mismatch")
	}
	clientOnly, roots := credentials(t, []string{"urn:outpostos:node:a"}, x509.ExtKeyUsageClientAuth)
	c, err := tlsutil.NewIdentity(clientOnly, roots, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.TLSConfig(false).VerifyPeerCertificate(clientOnly.Certificate, nil); err == nil {
		t.Fatal("accepted client-only certificate as server")
	}
}

func TestIdentityLoadsPEMWithTrailingWhitespace(t *testing.T) {
	cert, ca := credentials(t, []string{"urn:outpostos:node:node-a"}, x509.ExtKeyUsageAny)
	ca = append(ca, []byte("\n \t\r\n")...)
	if _, err := tlsutil.NewIdentity(cert, ca, "node-a", nil); err != nil {
		t.Fatal(err)
	}
}

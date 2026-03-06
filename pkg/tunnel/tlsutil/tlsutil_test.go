package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	cert, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert() error: %v", err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("expected at least one certificate")
	}
	if cert.PrivateKey == nil {
		t.Fatal("expected non-nil private key")
	}
}

func TestServerTLSConfig(t *testing.T) {
	cert, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert: %v", err)
	}
	cfg := ServerTLSConfig(cert)
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}
	if len(cfg.NextProtos) != 1 || cfg.NextProtos[0] != "outpostnet" {
		t.Fatalf("expected ALPN [outpostnet], got %v", cfg.NextProtos)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("expected MinVersion TLS 1.3, got %d", cfg.MinVersion)
	}
}

func TestClientTLSConfig(t *testing.T) {
	cfg := ClientTLSConfig(false)
	if cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=false")
	}

	cfg = ClientTLSConfig(true)
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=true")
	}
	if len(cfg.NextProtos) != 1 || cfg.NextProtos[0] != "outpostnet" {
		t.Fatalf("expected ALPN [outpostnet], got %v", cfg.NextProtos)
	}
}

func TestLoadTLSConfig(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	certPEM, keyPEM := generateTestPEM(t)
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	loaded, err := LoadTLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}
	if len(loaded.Certificate) == 0 {
		t.Fatal("expected loaded certificate")
	}
}

func TestLoadTLSConfigNotFound(t *testing.T) {
	_, err := LoadTLSConfig("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Fatal("expected error for nonexistent files")
	}
}

func generateTestPEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

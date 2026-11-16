package tlsutil

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

const nodeURIPrefix = "urn:outpostos:node:"

// NodeID extracts exactly one nonempty node identity URI from a leaf SAN.
func NodeID(cert *x509.Certificate) (string, error) {
	var id string
	count := 0
	for _, u := range cert.URIs {
		if strings.HasPrefix(u.String(), nodeURIPrefix) {
			count++
			id = strings.TrimPrefix(u.String(), nodeURIPrefix)
		}
	}
	if count != 1 || id == "" || len(id) > 255 || strings.ContainsAny(id, " \t\r\n/?#%") {
		return "", fmt.Errorf("certificate must contain exactly one valid node identity URI")
	}
	return id, nil
}

// SecurityStats is a process-lifetime snapshot. Counters never contain credentials.
type SecurityStats struct {
	CertificateRejected uint64 `json:"certificate_rejected"`
	ClaimRejected       uint64 `json:"claim_rejected"`
	ValidityWarnings    uint64 `json:"validity_warnings"`
}

// Identity holds a node's local group credentials and concurrent security counters.
// Verification is offline, with certificate validity independent of wall time (ADR-0008).
type Identity struct {
	nodeID   string
	cert     tls.Certificate
	roots    *x509.CertPool
	logger   *slog.Logger
	rejected atomic.Uint64
	claims   atomic.Uint64
	warnings atomic.Uint64
}

// LoadIdentity loads a PEM node key pair and local group CA bundle. No fallback is used.
func LoadIdentity(certFile, keyFile, caFile, nodeID string, logger *slog.Logger) (*Identity, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load node key pair: %w", err)
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("load group CA: %w", err)
	}
	return NewIdentity(cert, ca, nodeID, logger)
}

// NewIdentity validates local credentials, including their claimed node_id.
func NewIdentity(cert tls.Certificate, caPEM []byte, nodeID string, logger *slog.Logger) (*Identity, error) {
	if logger == nil {
		logger = slog.Default()
	}
	roots := x509.NewCertPool()
	for len(bytes.TrimSpace(caPEM)) > 0 {
		block, rest := pem.Decode(caPEM)
		if block == nil {
			return nil, fmt.Errorf("invalid group CA PEM")
		}
		caPEM = rest
		ca, err := x509.ParseCertificate(block.Bytes)
		if err != nil || block.Type != "CERTIFICATE" || !ca.IsCA {
			return nil, fmt.Errorf("group CA bundle must contain CA certificates")
		}
		roots.AddCert(ca)
	}
	if len(roots.Subjects()) == 0 {
		return nil, fmt.Errorf("empty group CA bundle")
	}
	if len(cert.Certificate) == 0 || cert.PrivateKey == nil {
		return nil, fmt.Errorf("missing node credentials")
	}
	id := &Identity{nodeID: nodeID, cert: cert, roots: roots, logger: logger}
	// Local validation is not a peer attempt and does not increment peer counters.
	leaf, err := id.verify(cert.Certificate, x509.ExtKeyUsageAny)
	if err != nil {
		return nil, fmt.Errorf("local node certificate: %w", err)
	}
	got, err := NodeID(leaf)
	if err != nil || got != nodeID {
		return nil, fmt.Errorf("local certificate node_id does not match configured node_id")
	}
	return id, nil
}

// NodeID returns the authenticated local identity.
func (i *Identity) NodeID() string { return i.nodeID }

// RejectClaim counts an identity claim or expected-peer mismatch without logging its input.
func (i *Identity) RejectClaim() { i.claims.Add(1); i.logger.Warn("tunnel identity claim rejected") }

// Stats returns counters safe to expose through a local read-only endpoint.
func (i *Identity) Stats() SecurityStats {
	return SecurityStats{i.rejected.Load(), i.claims.Load(), i.warnings.Load()}
}

func (i *Identity) verify(raw [][]byte, usage x509.ExtKeyUsage) (*x509.Certificate, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("peer certificate required")
	}
	leaf, err := x509.ParseCertificate(raw[0])
	if err != nil {
		return nil, err
	}
	intermediates := x509.NewCertPool()
	for _, der := range raw[1:] {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		intermediates.AddCert(c)
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: i.roots, Intermediates: intermediates, CurrentTime: leaf.NotBefore, KeyUsages: []x509.ExtKeyUsage{usage}})
	if err != nil {
		return nil, err
	}
	if leaf.IsCA {
		return nil, fmt.Errorf("peer must be a node leaf certificate")
	}
	if _, err := NodeID(leaf); err != nil {
		return nil, err
	}
	return leaf, nil
}

// TLSConfig returns a fresh mutual TLS configuration. RequestClientCert lets the
// custom verifier count and reject missing client certificates as well as invalid
// ones; acceptance always requires a valid group chain. Session tickets are disabled
// so every connection executes the verifier. DNS names do not define node identity.
func (i *Identity) TLSConfig(server bool) *tls.Config {
	cfg := &tls.Config{Certificates: []tls.Certificate{i.cert}, NextProtos: []string{"outpostnet"}, MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, SessionTicketsDisabled: true}
	usage := x509.ExtKeyUsageServerAuth
	if server {
		cfg.ClientAuth = tls.RequestClientCert
		usage = x509.ExtKeyUsageClientAuth
	}
	cfg.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
		leaf, err := i.verify(raw, usage)
		if err != nil {
			i.rejected.Add(1)
			i.logger.Warn("node certificate rejected")
			return err
		}
		now := time.Now()
		if cfg.Time != nil {
			now = cfg.Time()
		}
		if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
			i.warnings.Add(1)
			i.logger.Warn("node certificate outside validity window")
		}
		return nil
	}
	return cfg
}

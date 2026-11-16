package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"
)

// GenerateCA generates an offline group CA, valid for twenty years.
func GenerateCA(now time.Time) (certPEM, keyPEM []byte, err error) {
	return issue(nil, nil, "", now, nil, nil)
}

// IssueNode issues a ten-year node certificate with both TLS usages. The CA
// key is needed only by the offline issuing tool, never on running nodes.
func IssueNode(ca tls.Certificate, nodeID string, now time.Time) (certPEM, keyPEM []byte, err error) {
	return IssueNodeWithSANs(ca, nodeID, now, nil, nil)
}

// IssueNodeWithSANs adds NTS server names while preserving the node URI identity.
func IssueNodeWithSANs(ca tls.Certificate, nodeID string, now time.Time, dns []string, ips []net.IP) (certPEM, keyPEM []byte, err error) {
	for _, name := range dns {
		if !ValidDNSName(name) {
			return nil, nil, fmt.Errorf("invalid DNS SAN")
		}
	}
	for _, ip := range ips {
		if ip.To16() == nil {
			return nil, nil, fmt.Errorf("invalid IP SAN")
		}
	}
	if len(ca.Certificate) == 0 {
		return nil, nil, fmt.Errorf("missing CA")
	}
	parent, err := x509.ParseCertificate(ca.Certificate[0])
	if err != nil {
		return nil, nil, err
	}
	signer, ok := ca.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("CA must use an ECDSA key")
	}
	if !parent.IsCA || parent.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, nil, fmt.Errorf("issuer is not a signing CA")
	}
	return issue(parent, signer, nodeID, now, dns, ips)
}

func issue(parent *x509.Certificate, signer *ecdsa.PrivateKey, nodeID string, now time.Time, dns []string, ips []net.IP) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	cert := &x509.Certificate{SerialNumber: serial, NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(20, 0, 0), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, IsCA: true, Subject: pkix.Name{CommonName: "OutpostOS group CA"}}
	if parent == nil {
		parent = cert
		signer = key
	} else {
		uri, err := url.Parse(nodeURIPrefix + nodeID)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid node_id")
		}
		cert.DNSNames = dns
		cert.IPAddresses = ips
		cert.IsCA = false
		cert.KeyUsage = x509.KeyUsageDigitalSignature
		cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}
		cert.URIs = []*url.URL{uri}
		cert.Subject = pkix.Name{}
		cert.NotAfter = now.AddDate(10, 0, 0)
		if got, err := NodeID(cert); err != nil || got != nodeID {
			return nil, nil, fmt.Errorf("invalid node_id")
		}
		if cert.NotBefore.Before(parent.NotBefore) {
			cert.NotBefore = parent.NotBefore
		}
		if cert.NotAfter.After(parent.NotAfter) {
			cert.NotAfter = parent.NotAfter
		}
		if !cert.NotAfter.After(cert.NotBefore) {
			return nil, nil, fmt.Errorf("issuer has no overlapping validity window")
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, parent, &key.PublicKey, signer)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

// ValidDNSName accepts concrete ASCII host names, excluding wildcards and IPs.
func ValidDNSName(name string) bool {
	if name == "" || len(name) > 253 || net.ParseIP(name) != nil {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

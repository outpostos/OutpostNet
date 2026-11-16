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

// IssueNodeWithSANs issues a node certificate that additionally carries the
// given DNS and IP subject alternative names. Node identity still comes only
// from the urn:outpostos:node: URI, so existing certificates remain valid;
// these names exist so NTS servers can be verified by IP or DNS (ADR-0006 D7).
func IssueNodeWithSANs(ca tls.Certificate, nodeID string, now time.Time, dnsNames []string, ips []net.IP) (certPEM, keyPEM []byte, err error) {
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
	dnsNames, ips, err = normalizeSANs(dnsNames, ips)
	if err != nil {
		return nil, nil, err
	}
	return issue(parent, signer, nodeID, now, dnsNames, ips)
}

// normalizeSANs validates and canonicalizes extra subject alternative names.
func normalizeSANs(dnsNames []string, ips []net.IP) ([]string, []net.IP, error) {
	names := make([]string, 0, len(dnsNames))
	seenNames := make(map[string]struct{}, len(dnsNames))
	for _, raw := range dnsNames {
		name := strings.TrimSuffix(strings.TrimSpace(raw), ".")
		if !ValidDNSName(name) {
			return nil, nil, fmt.Errorf("invalid DNS SAN %q", raw)
		}
		key := strings.ToLower(name)
		if _, dup := seenNames[key]; dup {
			return nil, nil, fmt.Errorf("duplicate DNS SAN %q", raw)
		}
		seenNames[key] = struct{}{}
		names = append(names, name)
	}
	addrs := make([]net.IP, 0, len(ips))
	seenAddrs := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		if ip == nil {
			return nil, nil, fmt.Errorf("invalid IP SAN")
		}
		if v4 := ip.To4(); v4 != nil {
			ip = v4
		}
		key := ip.String()
		if _, dup := seenAddrs[key]; dup {
			return nil, nil, fmt.Errorf("duplicate IP SAN %q", key)
		}
		seenAddrs[key] = struct{}{}
		addrs = append(addrs, ip)
	}
	return names, addrs, nil
}

// ValidDNSName accepts RFC 1123 hostname labels, but not IP literals.
func ValidDNSName(name string) bool {
	if name == "" || len(name) > 253 || net.ParseIP(name) != nil {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return true
}

func issue(parent *x509.Certificate, signer *ecdsa.PrivateKey, nodeID string, now time.Time, dnsNames []string, ips []net.IP) ([]byte, []byte, error) {
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
		cert.IsCA = false
		cert.KeyUsage = x509.KeyUsageDigitalSignature
		cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}
		cert.URIs = []*url.URL{uri}
		cert.DNSNames = dnsNames
		cert.IPAddresses = ips
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

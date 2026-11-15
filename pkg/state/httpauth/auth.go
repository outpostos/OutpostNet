// Package httpauth shares the replication and delivery HTTP authentication policy.
package httpauth

import (
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// Mode selects token membership, both token and node certificate, or certificate only.
type Mode string

const (
	Token     Mode = "token"
	TokenCert Mode = "token+cert"
	Cert      Mode = "cert"
)

// Auth is immutable and may be shared by concurrent HTTP requests.
type Auth struct {
	mode     Mode
	token    string
	identity *tlsutil.Identity
}

// New validates an HTTP authentication policy against the local identity.
func New(mode Mode, token, node string, identity *tlsutil.Identity) (*Auth, error) {
	if mode == "" {
		mode = Token
	}
	if mode != Token && mode != TokenCert && mode != Cert {
		return nil, errors.New("invalid HTTP auth_mode")
	}
	if mode != Cert && token == "" {
		return nil, errors.New("HTTP auth token is required")
	}
	if mode != Token && (identity == nil || identity.NodeID() != node) {
		return nil, errors.New("HTTP certificate identity must match local node")
	}
	if mode == Token && identity != nil {
		return nil, errors.New("HTTP identity requires certificate auth_mode")
	}
	return &Auth{mode: mode, token: token, identity: identity}, nil
}

// CertificateMode reports whether HTTPS and a node certificate are required.
func (a *Auth) CertificateMode() bool { return a.mode != Token }

// TLSConfig adapts the offline node verifier to HTTP. Install this on the host's
// shared replication/delivery listener; hosts own serving and timeouts.
func TLSConfig(identity *tlsutil.Identity, server bool) *tls.Config {
	cfg := identity.TLSConfig(server)
	cfg.NextProtos = []string{"http/1.1"}
	return cfg
}

// Client copies configuration without mutating a caller's client or transport.
// Certificate modes own TLS verification; custom TLS dialers cannot bypass it.
func (a *Auth) Client(base *http.Client) (*http.Client, error) {
	client := http.Client{Timeout: 10 * time.Second}
	if base != nil {
		client = *base
		if client.Timeout == 0 {
			client.Timeout = 10 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if a.CertificateMode() {
		baseTransport := client.Transport
		if baseTransport == nil {
			baseTransport = http.DefaultTransport
		}
		transport, ok := baseTransport.(*http.Transport)
		if !ok {
			return nil, errors.New("certificate HTTP requires a standard HTTP transport")
		}

		if transport.DialTLS != nil || transport.DialTLSContext != nil {
			return nil, errors.New("certificate HTTP forbids custom TLS dialers")
		}
		cloned := transport.Clone()
		cloned.TLSClientConfig = TLSConfig(a.identity, false)
		client.Transport = cloned
	}
	return &client, nil
}

// Prepare checks the destination transport and sets only the required credentials.
func (a *Auth) Prepare(r *http.Request) error {
	if a.CertificateMode() && (r.URL.Scheme != "https" || r.URL.User != nil) {
		return errors.New("certificate HTTP requires HTTPS without URL credentials")
	}
	if a.mode != Cert {
		r.Header.Set("Authorization", "Bearer "+a.token)
	}
	return nil
}

// PeerURL accepts bare host:port addresses, choosing HTTPS in certificate modes.
func (a *Auth) PeerURL(addr, path string) (string, error) {
	if !strings.Contains(addr, "://") {
		if a.CertificateMode() {
			addr = "https://" + addr
		} else {
			addr = "http://" + addr
		}
	}
	raw := strings.TrimRight(addr, "/") + path
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("invalid HTTP peer address")
	}
	if a.CertificateMode() && (u.Scheme != "https" || u.User != nil) {
		return "", errors.New("certificate HTTP requires HTTPS without URL credentials")
	}
	return raw, nil
}

// Authorize returns an HTTP status (zero on success). Certificate verification
// is repeated at the handler boundary so mounting on a weaker listener cannot
// turn an unverified TLS certificate or forwarded header into node identity.
func (a *Auth) Authorize(r *http.Request) int {
	if a.CertificateMode() {
		var raw [][]byte
		if r.TLS != nil {
			for _, c := range r.TLS.PeerCertificates {
				raw = append(raw, c.Raw)
			}
		}
		if err := TLSConfig(a.identity, true).VerifyPeerCertificate(raw, nil); err != nil {
			return http.StatusForbidden
		}
	}
	if a.mode != Cert {
		value := r.Header.Get("Authorization")
		if !strings.HasPrefix(value, "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(value, "Bearer ")), []byte(a.token)) != 1 {
			return http.StatusUnauthorized
		}
	}
	return 0
}

// RejectClaim records a refused body, header or directory identity claim.
func (a *Auth) RejectClaim() {
	if a.identity != nil {
		a.identity.RejectClaim()
	}
}

// MatchSource binds a decoded request source to the verified node certificate.
func (a *Auth) MatchSource(r *http.Request, source string) bool {
	if !a.CertificateMode() {
		return true
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		a.RejectClaim()
		return false
	}
	node, err := tlsutil.NodeID(r.TLS.PeerCertificates[0])
	if err != nil || source != node {
		a.RejectClaim()
		return false
	}
	return true
}

// MatchResponse binds responder metadata (including 410 horizons) and optional
// directory expectations to the peer actually authenticated by the TLS client.
func (a *Auth) MatchResponse(r *http.Response, node, expected string) error {
	if !a.CertificateMode() {
		return nil
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		a.RejectClaim()
		return errors.New("HTTP response missing authenticated peer")
	}
	got, err := tlsutil.NodeID(r.TLS.PeerCertificates[0])
	if err != nil || node != got || (expected != "" && got != expected) {
		a.RejectClaim()
		return errors.New("HTTP response certificate identity mismatch")
	}
	return nil
}

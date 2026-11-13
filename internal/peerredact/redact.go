package peerredact

import (
	neturl "net/url"
	"strings"
)

// Peer addresses may embed a password (scheme://user:password@host, or
// user:password@host, which buildPeerURL dials as http). Every log line and
// every error that pkg/state produces names a peer through URL,
// and every per-peer error that leaves GossipEngine (returned, logged or
// reported in ReplicationStatus) passes through Error, so that a
// Syncer quoting the raw address, or a peer echoing it, cannot leak it.

// Secret replaces credentials in errors and status reports.
const Secret = "[REDACTED]"

// peerPassword returns the password embedded in a peer address, read the way
// buildPeerURL dials it.
func peerPassword(addr string) (string, bool) {
	u, ok := parsePeerURL(addr)
	if !ok || u.User == nil {
		return "", false
	}
	password, ok := u.User.Password()
	if !ok || password == "" {
		return "", false
	}
	return password, true
}

func parsePeerURL(addr string) (*neturl.URL, bool) {
	raw := addr
	if !hasHTTPScheme(addr) {
		raw = "http://" + addr
	}
	u, err := neturl.Parse(raw)
	if err != nil {
		return nil, false
	}
	return u, true
}

func hasHTTPScheme(addr string) bool {
	return strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://")
}

// URL names a peer in logs and errors: a password embedded in the
// address is masked. Addresses without one are returned as is.
func URL(addr string) string {
	if _, ok := peerPassword(addr); !ok {
		return addr
	}
	u, _ := parsePeerURL(addr)
	shown := u.Redacted()
	if !hasHTTPScheme(addr) {
		shown = strings.TrimPrefix(shown, "http://")
	}
	return shown
}

// Text removes the password embedded in addr from a text produced
// by any Syncer or echoed by a peer.
func Text(addr, message string) string {
	password, ok := peerPassword(addr)
	if !ok {
		return message
	}
	message = strings.ReplaceAll(message, password, Secret)
	if escaped := neturl.QueryEscape(password); escaped != password {
		message = strings.ReplaceAll(message, escaped, Secret)
	}
	if escaped := neturl.PathEscape(password); escaped != password {
		message = strings.ReplaceAll(message, escaped, Secret)
	}
	return message
}

// Error returns err with the password embedded in addr removed from
// its text. The original error stays reachable through errors.Is/As.
func Error(addr string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := peerPassword(addr); !ok {
		return err
	}
	return &redactedPeerError{text: Text(addr, err.Error()), err: err}
}

type redactedPeerError struct {
	text string
	err  error
}

func (e *redactedPeerError) Error() string { return e.text }

func (e *redactedPeerError) Unwrap() error { return e.err }

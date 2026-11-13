package state

import (
	"strings"

	"github.com/outpostos/OutpostNet/internal/peerredact"
)

const redactedSecret = peerredact.Secret

func redactedPeerURL(addr string) string          { return peerredact.URL(addr) }
func redactPeerError(addr, message string) string { return peerredact.Text(addr, message) }
func redactPeerErr(addr string, err error) error  { return peerredact.Error(addr, err) }
func hasHTTPScheme(addr string) bool {
	return strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://")
}

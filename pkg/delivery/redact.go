package delivery

import (
	"strings"

	"github.com/outpostos/OutpostNet/internal/peerredact"
)

type httpError struct {
	message string
	cause   error
}

func (e *httpError) Error() string { return e.message }
func (e *httpError) Unwrap() error { return e.cause }
func safeHTTPError(addr, token string, err error) error {
	message := peerredact.Text(addr, err.Error())
	// Drop the complete address as well as credentials. No endpoint appears in
	// delivery errors or local observation, even if an HTTP client echoes it.
	for _, secret := range []string{addr, peerredact.URL(addr), token} {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, peerredact.Secret)
		}
	}
	return &httpError{message: "delivery HTTP: " + message, cause: err}
}

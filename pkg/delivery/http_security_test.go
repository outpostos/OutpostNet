package delivery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/delivery"
)

func TestHTTPIdentityProtocolAndResponseValidation(t *testing.T) {
	peers := map[string]string{}
	b := newHTTPNode(t, "b", peers, nil)
	a := newHTTPNode(t, "a", peers, nil)
	id := sendHTTP(t, a, "b", []byte("hello"))
	e := delivery.Envelope{MessageID: id, DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: []byte("hello"), Checksum: delivery.Checksum([]byte("hello")), ExpiryTime: time.Now().Add(time.Hour)}
	for _, tc := range []struct {
		name string
		body string
		code int
	}{
		{"version", `{"protocol_version":{"major":99},"source":"a"}`, 426},
		{"missing_source", `{"protocol_version":{"major":1}}`, 403},
		{"envelope_source", `{"protocol_version":{"major":1},"source":"a","envelopes":[{"message_id":{"source_node_id":"c"}}]}`, 403},
		{"receipt_source", `{"protocol_version":{"major":1},"source":"a","receipts":[{"receiver_node_id":"c"}]}`, 403},
		{"trailing_json", `{} {}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", delivery.BatchPath, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer test-token")
			req.Header.Set("X-Outpost-Node-ID", "a")
			w := httptest.NewRecorder()
			b.handler.ServeHTTP(w, req)
			if w.Code != tc.code {
				t.Fatalf("code %d: %s", w.Code, w.Body.String())
			}
		})
	}
	// A forged envelope or receipt is refused as an entire batch.
	e.MessageID.SourceNodeID = "forged"
	if _, err := a.wire.Send(context.Background(), "b", delivery.Batch{Envelopes: []delivery.Envelope{e}}); err == nil {
		t.Fatal("forged source accepted")
	}
	if _, err := a.wire.Send(context.Background(), "b", delivery.Batch{Receipts: []delivery.Receipt{{MessageID: id, ReceiverNodeID: "forged"}}}); err == nil {
		t.Fatal("forged receipt accepted")
	}
	for _, tc := range []struct {
		name string
		body func() string
	}{
		{"wrong_node", func() string { return `{"protocol_version":{"major":1},"node_id":"c"}` }},
		{"old_protocol", func() string { return `{"protocol_version":{"major":0},"node_id":"b"}` }},
		{"missing_results", func() string { return `{"protocol_version":{"major":1},"node_id":"b"}` }},
		{"wrong_id", func() string {
			return `{"protocol_version":{"major":1},"node_id":"b","envelopes":[{"message_id":{"source_node_id":"wrong"},"code":"accepted"}]}`
		}},
		{"unknown_code", func() string {
			body, _ := json.Marshal(map[string]any{"protocol_version": delivery.DeliveryProtocolVersion, "node_id": "b", "envelopes": []delivery.Result{{MessageID: id, Code: "maybe"}}})
			return string(body)
		}},
		{"too_big", func() string { return strings.Repeat("x", (1<<20)+1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body()) }))
			defer s.Close()
			peers["b"] = s.URL
			// Use the Transport boundary directly to avoid backoff across these failures.
			e.MessageID = id
			if _, err := a.wire.Send(context.Background(), "b", delivery.Batch{Envelopes: []delivery.Envelope{e}}); err == nil {
				t.Fatal("invalid response accepted")
			}
			statusHTTP(t, a, id, delivery.Pending)
		})
	}
}

func TestHTTPNoCredentialOrPeerURLInErrorsOrLogs(t *testing.T) {
	const token = "unique-test-bearer"
	const password = "unique-peer-password"
	for _, mode := range []string{"remote_body", "client_error", "decode", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			peers := map[string]string{}
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			targetCalls := 0
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls++ }))
			defer target.Close()
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "redirect":
					w.Header().Set("Location", target.URL)
					w.WriteHeader(307)
				case "decode":
					io.WriteString(w, token)
				default:
					http.Error(w, token+" "+password+" "+peers["b"], 500)
				}
			}))
			defer endpoint.Close()
			peers["b"] = strings.Replace(endpoint.URL, "http://", "http://user:"+password+"@", 1)
			a := newHTTPNode(t, "a", peers, func(_ *delivery.Config, w *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
				w.AuthToken = token
				if mode == "client_error" {
					w.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, fmt.Errorf("echo %s %s", token, peers["b"]) })}
				}
			})
			id := sendHTTP(t, a, "b", []byte(token))
			err := a.out.Flush(context.Background())
			if err == nil {
				t.Fatal("expected failure")
			}
			slog.Error("flush failed", "error", err)
			raw, _ := json.Marshal(a.out.DeliveryStatus())
			text := logs.String() + err.Error() + string(raw)
			for _, secret := range []string{token, password, endpoint.URL, peers["b"]} {
				if strings.Contains(text, secret) {
					t.Fatalf("credential or address leaked in %s", mode)
				}
			}
			if targetCalls != 0 {
				t.Fatal("redirect followed")
			}
			statusHTTP(t, a, id, delivery.Pending)
		})
	}
}

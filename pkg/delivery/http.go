package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/outpostos/OutpostNet/pkg/state"
	"github.com/outpostos/OutpostNet/pkg/state/httpauth"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

const BatchPath = "/delivery/v1/batch"

// DeliveryProtocolVersion evolves independently of the state protocol.
var DeliveryProtocolVersion = state.ProtocolVersion{Major: 1, Minor: 0}

var ErrUnknownPeer = errors.New("delivery destination unknown")

type httpBatchRequest struct {
	ProtocolVersion state.ProtocolVersion `json:"protocol_version"`
	Source          string                `json:"source"`
	Batch
}
type httpBatchResponse struct {
	ProtocolVersion state.ProtocolVersion `json:"protocol_version"`
	NodeID          string                `json:"node_id"`
	BatchResult
}

// HTTPTransportConfig uses the same membership and address-to-node mapping as
// state replication. Callbacks must be concurrency safe. No discovery is added.
type HTTPTransportConfig struct {
	AuthMode     httpauth.Mode
	Identity     *tlsutil.Identity
	NodeID       string
	AuthToken    string
	PeerProvider state.PeerProvider
	PeerIdentity func(address string) string
	Client       *http.Client
	// Limits include the complete JSON wire envelope, not just Batch.
	MaxRequestBytes  int64
	MaxResponseBytes int64
}
type HTTPTransport struct {
	auth   *httpauth.Auth
	cfg    HTTPTransportConfig
	client *http.Client
}

func httpLimits(request, response int64) (int64, int64, error) {
	if request == 0 {
		request = 1 << 20
	}
	if response == 0 {
		response = 1 << 20
	}
	if request < 1 || response < 1 {
		return 0, 0, errors.New("delivery HTTP byte limits must be positive")
	}
	return request, response, nil
}
func NewHTTPTransport(cfg HTTPTransportConfig) (*HTTPTransport, error) {
	if cfg.NodeID == "" || cfg.PeerProvider == nil || cfg.PeerIdentity == nil {
		return nil, errors.New("delivery HTTP requires node, peer provider and identity mapping")
	}
	var err error
	cfg.MaxRequestBytes, cfg.MaxResponseBytes, err = httpLimits(cfg.MaxRequestBytes, cfg.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	auth, err := httpauth.New(cfg.AuthMode, cfg.AuthToken, cfg.NodeID, cfg.Identity)
	if err != nil {
		return nil, err
	}
	client, err := auth.Client(cfg.Client)
	if err != nil {
		return nil, err
	}
	return &HTTPTransport{cfg: cfg, client: client, auth: auth}, nil
}
func (t *HTTPTransport) Send(ctx context.Context, dest string, batch Batch) (BatchResult, error) {
	addr := ""
	for _, candidate := range t.cfg.PeerProvider.Peers() {
		if candidate != "" && t.cfg.PeerIdentity(candidate) == dest {
			if addr != "" && addr != candidate {
				return BatchResult{}, errors.New("delivery destination mapping ambiguous")
			}
			addr = candidate
		}
	}
	if addr == "" {
		return BatchResult{}, ErrUnknownPeer
	}
	return t.sendPages(ctx, dest, addr, batch)
}

// A failed later page invalidates the whole call. The next Flush safely
// resends earlier committed pages and receives duplicate acknowledgements.
func (t *HTTPTransport) sendPages(ctx context.Context, dest, addr string, batch Batch) (BatchResult, error) {
	var result BatchResult
	page := Batch{}
	fits := func(b Batch) bool {
		return int64(encodedSize(httpBatchRequest{DeliveryProtocolVersion, t.cfg.NodeID, b})) <= t.cfg.MaxRequestBytes && int64(encodedSize(worstHTTPResponse(dest, b))) <= t.cfg.MaxResponseBytes
	}
	flush := func() error {
		if len(page.Envelopes)+len(page.Receipts) == 0 {
			return nil
		}
		r, err := t.sendPage(ctx, dest, addr, page)
		if err != nil {
			return err
		}
		if !validBatchResult(page, r) {
			return errors.New("invalid delivery transport response")
		}
		result.Envelopes = append(result.Envelopes, r.Envelopes...)
		result.Receipts = append(result.Receipts, r.Receipts...)
		page = Batch{}
		return nil
	}
	for _, e := range batch.Envelopes {
		candidate := Batch{Envelopes: append(append([]Envelope(nil), page.Envelopes...), e), Receipts: page.Receipts}
		if !fits(candidate) {
			if err := flush(); err != nil {
				return BatchResult{}, err
			}
			candidate = Batch{Envelopes: []Envelope{e}}
		}
		if !fits(candidate) {
			return BatchResult{}, errors.New("delivery envelope cannot fit HTTP byte limits")
		}
		page = candidate
	}
	for _, r := range batch.Receipts {
		candidate := Batch{Envelopes: page.Envelopes, Receipts: append(append([]Receipt(nil), page.Receipts...), r)}
		if !fits(candidate) {
			if err := flush(); err != nil {
				return BatchResult{}, err
			}
			candidate = Batch{Receipts: []Receipt{r}}
		}
		if !fits(candidate) {
			return BatchResult{}, errors.New("delivery receipt cannot fit HTTP byte limits")
		}
		page = candidate
	}
	if err := flush(); err != nil {
		return BatchResult{}, err
	}
	return result, nil
}
func worstHTTPResponse(node string, b Batch) httpBatchResponse {
	r := httpBatchResponse{ProtocolVersion: DeliveryProtocolVersion, NodeID: node}
	for _, e := range b.Envelopes {
		r.Envelopes = append(r.Envelopes, Result{e.MessageID, RejectedExpired})
	}
	for _, e := range b.Receipts {
		r.Receipts = append(r.Receipts, Result{e.MessageID, RejectedExpired})
	}
	return r
}
func (t *HTTPTransport) sendPage(ctx context.Context, dest, addr string, batch Batch) (BatchResult, error) {
	request := httpBatchRequest{DeliveryProtocolVersion, t.cfg.NodeID, batch}
	data, err := json.Marshal(request)
	if err != nil {
		return BatchResult{}, errors.New("delivery request encoding failed")
	}
	if int64(len(data)) > t.cfg.MaxRequestBytes {
		return BatchResult{}, errors.New("delivery HTTP request exceeds byte limit")
	}
	url, err := t.auth.PeerURL(addr, BatchPath)
	if err != nil {
		return BatchResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return BatchResult{}, errors.New("delivery invalid peer address")
	}
	req.Header.Set("Content-Type", "application/json")
	if err := t.auth.Prepare(req); err != nil {
		return BatchResult{}, err
	}
	req.Header.Set("X-Outpost-Node-ID", t.cfg.NodeID)
	resp, err := t.client.Do(req)
	if err != nil {
		return BatchResult{}, safeHTTPError(addr, t.cfg.AuthToken, err)
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(io.LimitReader(resp.Body, t.cfg.MaxResponseBytes+1))
	if err != nil {
		return BatchResult{}, safeHTTPError(addr, t.cfg.AuthToken, err)
	}
	if int64(len(data)) > t.cfg.MaxResponseBytes {
		return BatchResult{}, errors.New("delivery HTTP response exceeds byte limit")
	}
	// Remote bodies can echo credentials or addresses; never include them in errors.
	if resp.StatusCode != http.StatusOK {
		return BatchResult{}, fmt.Errorf("delivery HTTP status %d", resp.StatusCode)
	}
	var result httpBatchResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return BatchResult{}, errors.New("delivery invalid HTTP response JSON")
	}
	if !result.ProtocolVersion.CompatibleWith(DeliveryProtocolVersion) {
		return BatchResult{}, errors.New("delivery HTTP response protocol mismatch")
	}
	if err := t.auth.MatchResponse(resp, result.NodeID, dest); err != nil {
		return BatchResult{}, err
	}
	if result.NodeID != dest {
		return BatchResult{}, errors.New("delivery HTTP response node mismatch")
	}
	return result.BatchResult, nil
}

// HTTPHandlerConfig mounts on the host's authenticated replication listener.
// At least one box is required; both boxes must have NodeID as their identity.
type HTTPHandlerConfig struct {
	AuthMode         httpauth.Mode
	Identity         *tlsutil.Identity
	NodeID           string
	AuthToken        string
	Inbox            *Inbox
	Outbox           *Outbox
	MaxRequestBytes  int64
	MaxResponseBytes int64
}

func NewHTTPHandler(cfg HTTPHandlerConfig) (http.Handler, error) {
	if cfg.NodeID == "" || (cfg.Inbox == nil && cfg.Outbox == nil) {
		return nil, errors.New("delivery HTTP handler requires node and a box")
	}
	if (cfg.Inbox != nil && cfg.Inbox.cfg.NodeID != cfg.NodeID) || (cfg.Outbox != nil && cfg.Outbox.cfg.NodeID != cfg.NodeID) {
		return nil, errors.New("delivery HTTP box node mismatch")
	}
	var err error
	cfg.MaxRequestBytes, cfg.MaxResponseBytes, err = httpLimits(cfg.MaxRequestBytes, cfg.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	auth, err := httpauth.New(cfg.AuthMode, cfg.AuthToken, cfg.NodeID, cfg.Identity)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc(BatchPath, func(w http.ResponseWriter, r *http.Request) {
		fail := func(code int, message string) {
			if int64(len(message)+1) > cfg.MaxResponseBytes {
				w.WriteHeader(code)
				return
			}
			http.Error(w, message, code)
		}
		if r.Method != http.MethodPost {
			fail(http.StatusMethodNotAllowed, "delivery requires POST")
			return
		}
		if status := auth.Authorize(r); status != 0 {
			fail(status, "delivery authentication rejected")
			return
		}
		defer r.Body.Close()
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, cfg.MaxRequestBytes))
		if err != nil {
			var size *http.MaxBytesError
			if errors.As(err, &size) {
				fail(http.StatusRequestEntityTooLarge, "delivery request exceeds byte limit")
			} else {
				fail(http.StatusBadRequest, "delivery request read failed")
			}
			return
		}
		var request httpBatchRequest
		if json.Unmarshal(data, &request) != nil {
			fail(http.StatusBadRequest, "delivery invalid request JSON")
			return
		}
		if !request.ProtocolVersion.CompatibleWith(DeliveryProtocolVersion) {
			fail(http.StatusUpgradeRequired, "delivery protocol mismatch")
			return
		}
		if !auth.MatchSource(r, request.Source) {
			fail(http.StatusForbidden, "delivery certificate source mismatch")
			return
		}
		if request.Source == "" || request.Source != r.Header.Get("X-Outpost-Node-ID") || request.Source == cfg.NodeID {
			auth.RejectClaim()
			fail(http.StatusForbidden, "delivery source identity mismatch")
			return
		}
		for _, e := range request.Envelopes {
			if e.DestinationNodeID != cfg.NodeID {
				auth.RejectClaim()
				fail(http.StatusForbidden, "delivery destination identity mismatch")
				return
			}
			if e.MessageID.SourceNodeID != request.Source {
				auth.RejectClaim()
				fail(http.StatusForbidden, "delivery envelope source mismatch")
				return
			}
		}
		for _, receipt := range request.Receipts {
			if receipt.MessageID.SourceNodeID != cfg.NodeID {
				auth.RejectClaim()
				fail(http.StatusForbidden, "delivery receipt destination mismatch")
				return
			}
			if receipt.ReceiverNodeID != request.Source {
				auth.RejectClaim()
				fail(http.StatusForbidden, "delivery receipt source mismatch")
				return
			}
		}
		if (len(request.Envelopes) > 0 && cfg.Inbox == nil) || (len(request.Receipts) > 0 && cfg.Outbox == nil) {
			fail(http.StatusServiceUnavailable, "delivery box unavailable")
			return
		}
		// Refuse before persistence if any legal result could exceed the cap.
		if int64(encodedSize(worstHTTPResponse(cfg.NodeID, request.Batch))) > cfg.MaxResponseBytes {
			fail(http.StatusRequestEntityTooLarge, "delivery result exceeds byte limit")
			return
		}
		result := httpBatchResponse{ProtocolVersion: DeliveryProtocolVersion, NodeID: cfg.NodeID}
		if len(request.Envelopes) > 0 {
			result.Envelopes, err = cfg.Inbox.Accept(r.Context(), request.Envelopes)
			if err != nil {
				fail(http.StatusInternalServerError, "delivery inbox persistence failed")
				return
			}
		}
		if len(request.Receipts) > 0 {
			result.Receipts, err = cfg.Outbox.AcceptReceipts(r.Context(), request.Receipts)
			if err != nil {
				fail(http.StatusInternalServerError, "delivery receipt persistence failed")
				return
			}
		}
		data, _ = json.Marshal(result)
		if int64(len(data)) > cfg.MaxResponseBytes {
			fail(http.StatusInternalServerError, "delivery response exceeds byte limit")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})
	return mux, nil
}

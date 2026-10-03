package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"time"
)

const (
	// PushPath is the HTTP path for push replication.
	PushPath = "/state/v1/push"
	// PullPath is the HTTP path for pull replication.
	PullPath = "/state/v1/pull"
)

const (
	defaultMaxRequestBytes  int64 = 1 << 20
	defaultMaxResponseBytes int64 = 1 << 20
)

// HTTPSyncerConfig configures HTTP peer calls.
type HTTPSyncerConfig struct {
	Client           *http.Client
	NodeID           string
	AuthToken        string
	PolicyID         string
	MaxResponseBytes int64
}

// HTTPSyncer implements Syncer over JSON/HTTP.
type HTTPSyncer struct {
	client           *http.Client
	nodeID           string
	authToken        string
	policyID         string
	maxResponseBytes int64
}

// NewHTTPSyncer creates an authenticated HTTP syncer.
func NewHTTPSyncer(cfg HTTPSyncerConfig) (*HTTPSyncer, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("http syncer node id is required")
	}
	if cfg.AuthToken == "" {
		return nil, fmt.Errorf("http syncer auth token is required")
	}
	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultMaxResponseBytes
	}
	if maxResponseBytes < 0 {
		return nil, fmt.Errorf("http syncer max response bytes must be >= 0")
	}
	policyID := cfg.PolicyID
	if policyID == "" {
		policyID = NewHLCLWWPolicy().ID()
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPSyncer{
		client:           client,
		nodeID:           cfg.NodeID,
		authToken:        cfg.AuthToken,
		policyID:         policyID,
		maxResponseBytes: maxResponseBytes,
	}, nil
}

// Push sends entries to a remote peer.
func (s *HTTPSyncer) Push(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
	req.ProtocolVersion = StateProtocolVersion
	if req.PolicyID == "" {
		req.PolicyID = s.policyID
	}
	if req.SourceNodeID == "" {
		req.SourceNodeID = s.nodeID
	}
	var resp PushResponse
	if err := s.doJSON(ctx, http.MethodPost, addr, PushPath, req, &resp); err != nil {
		return nil, err
	}
	if err := validateProtocolResponse(addr, "push", resp.ProtocolVersion); err != nil {
		return nil, err
	}
	if err := validatePolicyResponse(addr, "push", req.PolicyID, resp.PolicyID); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Pull retrieves entries from a remote peer.
func (s *HTTPSyncer) Pull(ctx context.Context, addr string, req PullRequest) ([]Entry, error) {
	resp, err := s.pull(ctx, addr, req)
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// PullSnapshot performs a pull and returns the full response, including the
// peer's GC horizon and paging cursor. A paged position pull (Limit > 0, no
// digest) asks the peer to keep each page within this syncer's response limit.
func (s *HTTPSyncer) PullSnapshot(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	if req.MaxBytes == 0 && req.Limit > 0 && len(req.Digest) == 0 {
		req.MaxBytes = s.maxResponseBytes
	}
	return s.pull(ctx, addr, req)
}

func (s *HTTPSyncer) pull(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	req.ProtocolVersion = StateProtocolVersion
	if req.PolicyID == "" {
		req.PolicyID = s.policyID
	}
	if req.SourceNodeID == "" {
		req.SourceNodeID = s.nodeID
	}
	var resp PullResponse
	if err := s.doJSON(ctx, http.MethodPost, addr, PullPath, req, &resp); err != nil {
		return nil, err
	}
	if err := validateProtocolResponse(addr, "pull", resp.ProtocolVersion); err != nil {
		return nil, err
	}
	if err := validatePolicyResponse(addr, "pull", req.PolicyID, resp.PolicyID); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (s *HTTPSyncer) doJSON(ctx context.Context, method, addr, path string, reqBody, respBody any) error {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal %s %s request: %w", method, path, err)
	}
	url := buildPeerURL(addr, path)
	// Errors name the peer by its redacted URL and never repeat the bearer
	// token, even if a peer echoes it back in an error body: callers log these
	// errors and GossipEngine.ReplicationStatus reports them.
	shown := redactedPeerURL(url)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build %s %s request: %w", method, shown, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	req.Header.Set("X-Outpost-Node-ID", s.nodeID)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, shown, err)
	}
	defer resp.Body.Close()
	body, err := readLimitedResponseBody(resp, s.maxResponseBytes)
	if err != nil {
		return fmt.Errorf("read %s %s response: %w", method, shown, err)
	}
	if resp.StatusCode == http.StatusGone {
		var gone resyncRequiredBody
		if err := json.Unmarshal(body, &gone); err == nil && !gone.GCHorizon.isZero() {
			return &ResyncRequiredError{Peer: redactedPeerURL(addr), Horizon: gone.GCHorizon}
		}
	}
	if resp.StatusCode >= 300 {
		text := strings.ReplaceAll(strings.TrimSpace(string(body)), s.authToken, redactedSecret)
		return fmt.Errorf("%s %s: status %d: %s", method, shown, resp.StatusCode, text)
	}
	if respBody == nil {
		return nil
	}
	if err := json.Unmarshal(body, respBody); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, shown, err)
	}
	return nil
}

// redactedSecret replaces credentials in errors and status reports.
const redactedSecret = "[REDACTED]"

// redactedPeerURL hides a password embedded in a peer address
// (scheme://user:password@host). Addresses without one are returned as is.
func redactedPeerURL(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	if _, ok := u.User.Password(); !ok {
		return raw
	}
	return u.Redacted()
}

func buildPeerURL(addr, path string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/") + path
	}
	return "http://" + strings.TrimRight(addr, "/") + path
}

// HTTPHandlerConfig configures HTTP replication endpoints.
type HTTPHandlerConfig struct {
	NodeID          string
	Engine          *GossipEngine
	AuthToken       string
	MaxRequestBytes int64
}

// NewHTTPHandler exposes authenticated state replication endpoints.
func NewHTTPHandler(cfg HTTPHandlerConfig) (http.Handler, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("http state handler node id is required")
	}
	if cfg.Engine == nil {
		return nil, fmt.Errorf("http state handler engine is required")
	}
	if cfg.AuthToken == "" {
		return nil, fmt.Errorf("http state handler auth token is required")
	}
	limit := cfg.MaxRequestBytes
	if limit == 0 {
		limit = defaultMaxRequestBytes
	}
	if limit < 0 {
		return nil, fmt.Errorf("http state handler max request bytes must be >= 0")
	}
	h := &httpHandler{
		nodeID:          cfg.NodeID,
		engine:          cfg.Engine,
		authToken:       cfg.AuthToken,
		maxRequestBytes: limit,
	}
	mux := http.NewServeMux()
	mux.HandleFunc(PushPath, h.handlePush)
	mux.HandleFunc(PullPath, h.handlePull)
	return mux, nil
}

type httpHandler struct {
	nodeID          string
	engine          *GossipEngine
	authToken       string
	maxRequestBytes int64
}

func (h *httpHandler) handlePush(w http.ResponseWriter, r *http.Request) {
	if !h.preflight(w, r, http.MethodPost) {
		return
	}
	var req PushRequest
	if err := decodeJSONBody(w, r, h.maxRequestBytes, &req); err != nil {
		writeDecodeError(w, err)
		return
	}
	if !protocolMatches(req.ProtocolVersion) {
		writeJSONError(w, http.StatusUpgradeRequired, fmt.Sprintf("state protocol version mismatch: got %s want major %d", req.ProtocolVersion, StateProtocolVersion.Major))
		return
	}
	if !h.identityMatches(w, r, req.SourceNodeID) {
		return
	}
	if !h.policyMatches(w, req.PolicyID) {
		return
	}
	for _, entry := range req.Entries {
		if err := h.engine.policy.ValidateApply(entry); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	resp, err := h.engine.HandlePush(r.Context(), req)
	if err != nil {
		if writeResyncRequired(w, err) {
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp.ProtocolVersion = StateProtocolVersion
	resp.PolicyID = h.engine.policy.ID()
	writeJSON(w, http.StatusOK, resp)
}

func (h *httpHandler) handlePull(w http.ResponseWriter, r *http.Request) {
	if !h.preflight(w, r, http.MethodPost) {
		return
	}
	var req PullRequest
	if err := decodeJSONBody(w, r, h.maxRequestBytes, &req); err != nil {
		writeDecodeError(w, err)
		return
	}
	if !protocolMatches(req.ProtocolVersion) {
		writeJSONError(w, http.StatusUpgradeRequired, fmt.Sprintf("state protocol version mismatch: got %s want major %d", req.ProtocolVersion, StateProtocolVersion.Major))
		return
	}
	if !h.identityMatches(w, r, req.SourceNodeID) {
		return
	}
	if !h.policyMatches(w, req.PolicyID) {
		return
	}
	resp, err := h.engine.HandlePullResponse(r.Context(), req)
	if err != nil {
		if writeResyncRequired(w, err) {
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// resyncRequiredBody is the 410 Gone body for ResyncRequiredError.
type resyncRequiredBody struct {
	Error     string    `json:"error"`
	GCHorizon Timestamp `json:"gc_horizon"`
}

// writeResyncRequired maps ResyncRequiredError to 410 Gone: the history needed
// for an incremental exchange has been collected.
func writeResyncRequired(w http.ResponseWriter, err error) bool {
	var resync *ResyncRequiredError
	if !errors.As(err, &resync) {
		return false
	}
	writeJSON(w, http.StatusGone, resyncRequiredBody{Error: resync.Error(), GCHorizon: resync.Horizon})
	return true
}

func (h *httpHandler) preflight(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		writeJSONError(w, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not allowed", r.Method))
		return false
	}
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") ||
		strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != h.authToken {
		writeJSONError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return false
	}
	return true
}

func (h *httpHandler) identityMatches(w http.ResponseWriter, r *http.Request, source string) bool {
	header := r.Header.Get("X-Outpost-Node-ID")
	if source == "" || header == "" {
		writeJSONError(w, http.StatusBadRequest, "source node identity is required")
		return false
	}
	if source != header {
		writeJSONError(w, http.StatusForbidden, "source node identity does not match request header")
		return false
	}
	if source == h.nodeID {
		writeJSONError(w, http.StatusForbidden, "self-replication request rejected")
		return false
	}
	return true
}

func (h *httpHandler) policyMatches(w http.ResponseWriter, policyID string) bool {
	if err := validatePolicyID(h.engine.policy, policyID); err != nil {
		if errors.Is(err, errPolicyIDRequired) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return false
		}
		writeJSONError(w, http.StatusConflict, err.Error())
		return false
	}
	return true
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, limit int64, out any) error {
	defer r.Body.Close()
	reader := http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(reader)
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("decode request: multiple JSON values")
	}
	return nil
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		writeJSONError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	writeJSONError(w, http.StatusBadRequest, err.Error())
}

func protocolMatches(version ProtocolVersion) bool {
	return version.CompatibleWith(StateProtocolVersion)
}

func validateProtocolResponse(peer, operation string, version ProtocolVersion) error {
	if protocolMatches(version) {
		return nil
	}
	return fmt.Errorf("state protocol version mismatch in %s response from %s: got %s want major %d",
		operation, peer, version, StateProtocolVersion.Major)
}

func validatePolicyResponse(peer, operation, wantPolicyID, gotPolicyID string) error {
	if strings.TrimSpace(gotPolicyID) == "" {
		return fmt.Errorf("state policy id missing in %s response from %s", operation, peer)
	}
	if gotPolicyID != wantPolicyID {
		return fmt.Errorf("state policy id mismatch in %s response from %s: got %q want %q",
			operation, peer, gotPolicyID, wantPolicyID)
	}
	return nil
}

func readLimitedResponseBody(resp *http.Response, limit int64) ([]byte, error) {
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return body, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// Package wecom implements the text-only HTTPS callback for WeCom self-built
// application Bindings.
package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- WeCom requires SHA-1 callback signatures.
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

var (
	// ErrInvalid reports malformed WeCom callback configuration or payload.
	ErrInvalid = errors.New("invalid wecom callback")
	// ErrVerification reports a failed WeCom callback signature or decryption check.
	ErrVerification = errors.New("wecom callback verification failed")
)

const wecomBlockSize = 32

// Credentials is the private credential bundle for one Binding SecretRef.
type Credentials struct {
	CallbackToken  string
	EncodingAESKey string
	AppSecret      string
}

// CredentialResolver resolves the one secret bundle that a verified Binding
// is permitted to use.
type CredentialResolver interface {
	Resolve(context.Context, channels.SecretScope) (Credentials, error)
}

// Config contains either a static callback target or the dependencies required
// to resolve a current trusted Binding for each callback.
type Config struct {
	Token            string
	EncodingAESKey   string
	ReceiveID        string
	AgentID          string
	RouteKey         string
	Target           channels.RoutingTarget
	Dispatcher       gateway.DispatchService
	MaxBodyBytes     int64
	ExecutionTimeout time.Duration

	Candidates  channels.CandidateConsumer
	Tenants     tenant.Repository
	Apps        agent.Repository
	Credentials CredentialResolver
	// AuditWriter receives mandatory accepted and duplicate ingress facts.
	AuditWriter audit.Writer
	// Observability supplies provider-neutral trace and metric hooks.
	Observability observability.Provider
}

type callbackState struct {
	token     string
	receiveID string
	agentID   string
	key       []byte
	principal gateway.Principal
}

// Handler owns accepted execution drains. BeginShutdown prevents new drains
// and Close joins every drain before the owning Runtime releases dependencies.
type Handler struct {
	static *callbackState
	// These retained fields preserve the package's focused cryptographic tests
	// and are populated together with static. Dynamic callbacks use a local
	// verified state instead.
	token, receiveID string
	key              []byte
	routeKey         string
	dynamic          bool
	candidates       channels.CandidateConsumer
	tenants          tenant.Repository
	apps             agent.Repository
	credentials      CredentialResolver
	dispatcher       gateway.DispatchService
	maxBodyBytes     int64
	executionTimeout time.Duration
	auditWriter      audit.Writer
	telemetry        observability.Provider
	metrics          metrics.Catalog

	mu      sync.Mutex
	closing bool
	baseCtx context.Context
	cancel  context.CancelFunc
	drains  sync.WaitGroup
}

var _ channels.WebhookAdapter = (*Handler)(nil)

// New validates a text callback Handler. Dynamic mode receives the complete
// trusted target only after protocol verification.
//
//nolint:gocyclo
func New(config Config) (*Handler, error) {
	if config.Dispatcher == nil {
		return nil, ErrInvalid
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = 1 << 20
	}
	if config.MaxBodyBytes < 1 {
		return nil, ErrInvalid
	}
	if config.ExecutionTimeout == 0 {
		config.ExecutionTimeout = 4 * time.Minute
	}
	if config.ExecutionTimeout < 1 {
		return nil, ErrInvalid
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	handler := &Handler{routeKey: strings.Trim(config.RouteKey, "/"), dispatcher: config.Dispatcher, maxBodyBytes: config.MaxBodyBytes, executionTimeout: config.ExecutionTimeout, auditWriter: config.AuditWriter, baseCtx: baseCtx, cancel: cancel}
	handler.telemetry, handler.metrics = config.Observability, metrics.New(config.Observability)
	if config.Candidates != nil || config.Tenants != nil || config.Apps != nil || config.Credentials != nil {
		if config.Candidates == nil || config.Tenants == nil || config.Apps == nil || config.Credentials == nil || handler.routeKey != "" {
			cancel()
			return nil, ErrInvalid
		}
		handler.dynamic = true
		handler.candidates, handler.tenants, handler.apps, handler.credentials = config.Candidates, config.Tenants, config.Apps, config.Credentials
		return handler, nil
	}
	if strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.ReceiveID) == "" || strings.TrimSpace(config.AgentID) == "" {
		cancel()
		return nil, ErrInvalid
	}
	if err := config.Target.Validate(); err != nil || config.Target.Channel != channels.ChannelWeCom {
		cancel()
		return nil, ErrInvalid
	}
	principal, err := gateway.NewChannelPrincipal(config.Target)
	if err != nil {
		cancel()
		return nil, ErrInvalid
	}
	key, err := decodeAESKey(config.EncodingAESKey)
	if err != nil {
		cancel()
		return nil, ErrInvalid
	}
	handler.static = &callbackState{token: config.Token, receiveID: config.ReceiveID, agentID: strings.TrimSpace(config.AgentID), key: key, principal: principal}
	handler.token, handler.receiveID, handler.key = config.Token, config.ReceiveID, key
	return handler, nil
}

// ServeHTTP verifies the URL challenge or accepts one encrypted text message.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || r == nil || !h.matchesRoute(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.handleChallenge(w, r)
	case http.MethodPost:
		h.handleMessage(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) matchesRoute(path string) bool {
	if h.dynamic {
		_, ok := callbackRouteKey(path)
		return ok
	}
	if h.routeKey == "" {
		return true
	}
	routeKey, ok := callbackRouteKey(path)
	return ok && routeKey == h.routeKey
}

func callbackRouteKey(path string) (string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "wecom" || parts[1] != "callback" || strings.TrimSpace(parts[2]) == "" {
		return "", false
	}
	return parts[2], true
}

func (h *Handler) handleChallenge(w http.ResponseWriter, r *http.Request) {
	plain, _, err := h.verify(r, r.URL.Query().Get("echostr"))
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, string(plain))
}

//nolint:gocyclo // Callback handling intentionally keeps protocol validation and admission in one ordered boundary.
func (h *Handler) handleMessage(w http.ResponseWriter, r *http.Request) {
	capture := &statusCaptureWriter{ResponseWriter: w}
	w = capture
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(r.Context(), h.telemetry, observability.OperationChannelReceive, "channel")
	_ = h.metrics.Request(operationCtx, map[string]string{"component": "channel", "operation": observability.OperationChannelReceive, "channel": "wecom", "status": "started"})
	defer func() {
		var outcome error
		if ctxErr := r.Context().Err(); ctxErr != nil {
			outcome = ctxErr
		} else if capture.status >= http.StatusBadRequest {
			outcome = errors.New("wecom callback failed")
		}
		finish(outcome)
		_ = h.metrics.Operation(operationCtx, started, map[string]string{"component": "channel", "operation": observability.OperationChannelReceive, "channel": "wecom"}, outcome)
	}()
	r = r.WithContext(operationCtx)
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBodyBytes+1))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > h.maxBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	decoder := xml.NewDecoder(bytes.NewReader(body))
	var envelope callbackEnvelope
	if err := decoder.Decode(&envelope); err != nil || envelope.Encrypt == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var trailing callbackEnvelope
	if err := decoder.Decode(&trailing); err != io.EOF {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	plain, state, err := h.verify(r, envelope.Encrypt)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var message inboundXML
	if err := xml.Unmarshal(plain, &message); err != nil || message.MsgType != "text" || strings.TrimSpace(message.Content) == "" || strings.TrimSpace(message.MsgID) == "" || strings.TrimSpace(message.FromUserName) == "" || strings.TrimSpace(message.AgentID) != state.agentID {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	executionCtx, cancel, ok := h.beginDrain(operationCtx)
	if !ok {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	accepted := make(chan struct{}, 1)
	result := make(chan error, 1)
	requestID, traceID := uuid.NewString(), uuid.NewString()
	go func() {
		defer h.drains.Done()
		defer cancel()
		stream, dispatchErr := h.dispatcher.Dispatch(executionCtx, gateway.DispatchRequest{Accepted: accepted, Principal: state.principal, RequestID: requestID, TraceID: traceID, Message: gateway.InboundMessage{Content: message.Content, ContentType: gateway.ContentTypeText, ExternalMessageID: message.MsgID, ExternalUserID: message.FromUserName, ConversationKind: channels.ConversationDirect, ExternalPeerID: message.FromUserName}})
		if dispatchErr == nil && stream != nil {
			for range stream {
			}
		}
		result <- dispatchErr
	}()
	select {
	case <-accepted:
		h.writeIngressSuccess(w, r.Context(), state.principal, message, requestID, traceID, audit.EventIMIngressAccepted, audit.DecisionAccepted, "")
	case dispatchErr := <-result:
		// Dispatch implementations may notify acceptance immediately before
		// returning a completed result. Since both channels can then be ready,
		// make acceptance take precedence so the mandatory ingress audit is not
		// skipped by select's random ready-case choice.
		if h.tryAcceptedIngress(accepted, w, r.Context(), state.principal, message, requestID, traceID) {
			return
		}
		if dispatchErr == nil {
			h.writeSuccess(w)
			return
		}
		if errors.Is(dispatchErr, gateway.ErrDuplicateMessage) {
			h.writeIngressSuccess(w, r.Context(), state.principal, message, requestID, traceID, audit.EventIMIngressDuplicate, audit.DecisionDuplicate, string(audit.ErrorDuplicate))
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	case <-r.Context().Done():
	}
}

func (h *Handler) tryAcceptedIngress(accepted <-chan struct{}, w http.ResponseWriter, ctx context.Context, principal gateway.Principal, message inboundXML, requestID, traceID string) bool {
	select {
	case <-accepted:
		h.writeIngressSuccess(w, ctx, principal, message, requestID, traceID, audit.EventIMIngressAccepted, audit.DecisionAccepted, "")
		return true
	default:
		return false
	}
}

type statusCaptureWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCaptureWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusCaptureWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (h *Handler) writeIngressSuccess(w http.ResponseWriter, ctx context.Context, principal gateway.Principal, message inboundXML, requestID, traceID string, eventType audit.EventType, decision audit.Decision, errorType string) {
	if h.recordIngress(ctx, principal, message, requestID, traceID, eventType, decision, errorType) != nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	h.writeSuccess(w)
}

func (h *Handler) recordIngress(ctx context.Context, principal gateway.Principal, message inboundXML, requestID, traceID string, eventType audit.EventType, decision audit.Decision, errorType string) error {
	if h == nil || h.auditWriter == nil {
		return nil
	}
	event := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: audit.NewEventID(requestID, string(eventType)), EventType: eventType, TenantID: principal.TenantID(), Channel: string(channels.ChannelWeCom), UserID: message.FromUserName, AgentAppID: principal.AppID(), Decision: decision, ErrorType: errorType, RequestID: requestID, TraceID: traceID, ActorType: string(principal.Kind()), ActorID: principal.SubjectID(), OccurredAt: time.Now().UTC()}
	_, err := h.auditWriter.Append(ctx, event)
	return err
}

func (h *Handler) beginDrain(parent context.Context) (context.Context, context.CancelFunc, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closing {
		return nil, nil, false
	}
	if parent == nil {
		parent = h.baseCtx
		if parent == nil {
			parent = context.Background()
		}
	}
	// Keep the verified receive context as the trace parent while also making
	// handler shutdown cancel every accepted drain. A request context alone is
	// insufficient because Close must join in-flight dispatches immediately.
	merged, mergeCancel := context.WithCancel(context.WithoutCancel(parent))
	base := h.baseCtx
	if base == nil {
		base = context.Background()
	}
	stopBase := context.AfterFunc(base, mergeCancel)
	withTimeout, timeoutCancel := context.WithTimeout(merged, h.executionTimeout)
	cancel := func() {
		timeoutCancel()
		mergeCancel()
		stopBase()
	}
	h.drains.Add(1)
	return withTimeout, cancel, true
}

// BeginShutdown prevents accepting new execution drains and cancels drains already owned by this Handler.
func (h *Handler) BeginShutdown() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if !h.closing {
		h.closing = true
		h.cancel()
	}
	h.mu.Unlock()
}

// Channel identifies the protocol owned by this Handler.
func (*Handler) Channel() channels.Channel { return channels.ChannelWeCom }

// Close joins accepted execution drains after canceling their process context.
func (h *Handler) Close() error {
	if h == nil {
		return nil
	}
	h.BeginShutdown()
	h.drains.Wait()
	return nil
}

func (h *Handler) verify(r *http.Request, ciphertext string) ([]byte, callbackState, error) {
	if h.static != nil {
		if !validSignature(h.static.token, r.URL.Query().Get("msg_signature"), r.URL.Query().Get("timestamp"), r.URL.Query().Get("nonce"), ciphertext) {
			return nil, callbackState{}, ErrVerification
		}
		plain, err := decrypt(h.static.key, h.static.receiveID, ciphertext)
		return plain, *h.static, err
	}
	routeKey, ok := callbackRouteKey(r.URL.Path)
	if !ok {
		return nil, callbackState{}, ErrVerification
	}
	digest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, routeKey)
	if err != nil {
		return nil, callbackState{}, ErrVerification
	}
	candidates, err := h.candidates.LookupCandidates(r.Context(), channels.ChannelWeCom, digest)
	if err != nil {
		return nil, callbackState{}, ErrVerification
	}
	for _, candidate := range candidates {
		var verifiedState callbackState
		var verifiedPlain []byte
		target, resolveErr := channels.ResolveCandidateRoutingTarget(r.Context(), h.candidates, h.tenants, h.apps, candidate, func(ctx context.Context, binding channels.Binding) error {
			if binding.Channel != channels.ChannelWeCom || binding.Protocol.WeCom == nil {
				return ErrVerification
			}
			credentials, credentialErr := h.credentials.Resolve(ctx, channels.SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef})
			if credentialErr != nil {
				return credentialErr
			}
			key, keyErr := decodeAESKey(credentials.EncodingAESKey)
			if keyErr != nil || !validSignature(credentials.CallbackToken, r.URL.Query().Get("msg_signature"), r.URL.Query().Get("timestamp"), r.URL.Query().Get("nonce"), ciphertext) {
				return ErrVerification
			}
			plain, decryptErr := decrypt(key, binding.Protocol.WeCom.ReceiveID, ciphertext)
			if decryptErr != nil {
				return decryptErr
			}
			verifiedState = callbackState{token: credentials.CallbackToken, receiveID: binding.Protocol.WeCom.ReceiveID, agentID: binding.Protocol.WeCom.AgentID, key: key}
			verifiedPlain = plain
			return nil
		})
		if resolveErr != nil {
			if channels.IsContextCancellation(resolveErr) {
				return nil, callbackState{}, resolveErr
			}
			continue
		}
		principal, principalErr := gateway.NewChannelPrincipal(target)
		if principalErr != nil {
			continue
		}
		verifiedState.principal = principal
		return verifiedPlain, verifiedState, nil
	}
	return nil, callbackState{}, ErrVerification
}

func (h *Handler) writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "success")
}
func (h *Handler) validSignature(signature, timestamp, nonce, ciphertext string) bool {
	if h == nil {
		return false
	}
	if h.static != nil {
		return validSignature(h.static.token, signature, timestamp, nonce, ciphertext)
	}
	return validSignature(h.token, signature, timestamp, nonce, ciphertext)
}
func validSignature(token, signature, timestamp, nonce, ciphertext string) bool {
	if token == "" || signature == "" || timestamp == "" || nonce == "" || ciphertext == "" {
		return false
	}
	parts := []string{token, timestamp, nonce, ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- required by the WeCom protocol.
	want := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(signature), []byte(want)) == 1
}
func decodeAESKey(value string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(value + "=")
	if err != nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	return key, nil
}
func (h *Handler) decrypt(value string) ([]byte, error) {
	if h == nil {
		return nil, ErrVerification
	}
	if h.static != nil {
		return decrypt(h.static.key, h.static.receiveID, value)
	}
	return decrypt(h.key, h.receiveID, value)
}
func decrypt(key []byte, receiveID, value string) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, ErrVerification
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrVerification
	}
	iv := make([]byte, aes.BlockSize)
	copy(iv, key)
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plain, ciphertext)
	plain, err = unpad(plain)
	if err != nil || len(plain) < 20 {
		return nil, ErrVerification
	}
	size := int(binary.BigEndian.Uint32(plain[16:20]))
	if size < 0 || size > len(plain)-20 || subtle.ConstantTimeCompare(plain[20+size:], []byte(receiveID)) != 1 {
		return nil, ErrVerification
	}
	return append([]byte(nil), plain[20:20+size]...), nil
}
func unpad(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, ErrVerification
	}
	count := int(value[len(value)-1])
	if count < 1 || count > wecomBlockSize || count > len(value) || !bytes.Equal(value[len(value)-count:], bytes.Repeat([]byte{byte(count)}, count)) {
		return nil, ErrVerification
	}
	return value[:len(value)-count], nil
}

type callbackEnvelope struct {
	Encrypt string `xml:"Encrypt"`
}
type inboundXML struct {
	MsgID        string `xml:"MsgId"`
	FromUserName string `xml:"FromUserName"`
	MsgType      string `xml:"MsgType"`
	AgentID      string `xml:"AgentID"`
	Content      string `xml:"Content"`
}

var _ http.Handler = (*Handler)(nil)

package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- WeCom requires SHA-1 callback signatures.
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestDecodeAESKeyAndDecryptRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	plain := append([]byte(strings.Repeat("x", 16)), []byte{0, 0, 0, 3}...)
	plain = append(plain, []byte("abcRID")...)
	block, _ := aes.NewCipher(key)
	padded := append([]byte(nil), plain...)
	n := aes.BlockSize - len(padded)%aes.BlockSize
	padded = append(padded, bytes.Repeat([]byte{byte(n)}, n)...)
	encrypted := make([]byte, len(padded))
	iv := key[:aes.BlockSize]
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	h := &Handler{key: key, receiveID: "RID"}
	got, err := h.decrypt(base64.StdEncoding.EncodeToString(encrypted))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(plain[20:23]) {
		t.Fatalf("got %q", got)
	}
}

func TestSignatureAndInvalidPadding(t *testing.T) {
	h := &Handler{token: "token"}
	if h.validSignature("", "1", "2", "3") {
		t.Fatal("empty signature accepted")
	}
	if h.validSignature("bad", "1", "2", "3") {
		t.Fatal("bad signature accepted")
	}
	if _, err := unpad([]byte{1, 2}); err == nil {
		t.Fatal("invalid padding accepted")
	}
}

func TestProviderCachesTokenAndDeliversText(t *testing.T) {
	var tokenCalls, sendCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/cgi-bin/gettoken" {
			tokenCalls++
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"secret-token","expires_in":3600}`)
			return
		}
		if r.URL.Path == "/cgi-bin/message/send" {
			sendCalls++
			if r.URL.Query().Get("access_token") != "secret-token" {
				t.Errorf("token missing")
			}
			_, _ = io.WriteString(w, `{"errcode":0,"msgid":"m-1"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "app-secret", BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return time.Unix(100, 0).UTC() }}
	value := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"}}
	if id, err := p.Deliver(context.Background(), value); err != nil || id != "m-1" {
		t.Fatalf("deliver = %q, %v", id, err)
	}
	if _, err := p.Deliver(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if tokenCalls != 1 || sendCalls != 2 {
		t.Fatalf("calls token=%d send=%d", tokenCalls, sendCalls)
	}
}

func TestProviderRejectsOversizedText(t *testing.T) {
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "app-secret"}
	_, err := p.Deliver(context.Background(), storage.ReplyOutbox{Payload: strings.Repeat("界", 683), ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"}})
	if err == nil {
		t.Fatal("oversized text was accepted")
	}
}

func TestProviderClassifiesDeliveryOutcomes(t *testing.T) {
	for _, test := range []struct {
		name       string
		code, http int
		class      string
		retryable  bool
	}{
		{name: "expired token", code: 42001, class: "unauthenticated", retryable: true},
		{name: "rate limited", code: 45009, class: "rate_limited", retryable: true},
		{name: "server error", http: http.StatusBadGateway, class: "unavailable", retryable: true},
		{name: "provider rejection", code: 40003, http: http.StatusOK, class: "provider_error", retryable: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			class, retryable := classifyWeCom(test.code, test.http)
			if class != test.class || retryable != test.retryable {
				t.Fatalf("classifyWeCom(%d, %d) = %q, %t", test.code, test.http, class, retryable)
			}
		})
	}
	provider := &Provider{}
	if status, _, err := provider.Reconcile(context.Background(), storage.ReplyOutbox{}); status != outbox.DeliveryUnknown || err != nil {
		t.Fatalf("reconcile = %q, %v", status, err)
	}
	bindingProvider := &BindingProvider{}
	if _, err := bindingProvider.Deliver(context.Background(), storage.ReplyOutbox{}); err == nil {
		t.Fatal("unconfigured binding provider delivered a reply")
	}
	if status, _, err := bindingProvider.Reconcile(context.Background(), storage.ReplyOutbox{}); status != outbox.DeliveryUnknown || err == nil {
		t.Fatalf("unconfigured binding provider reconcile = %q, %v", status, err)
	}
}

func TestHandlerAcceptsEncryptedTextWithRequestAndTraceIDs(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	handler := newCallbackTestHandler(t, dispatcher)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, callbackTestRequest(t, "message-1", "user-1", "hello"))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "success" {
		t.Fatalf("callback response = %d %q", recorder.Code, recorder.Body.String())
	}
	select {
	case request := <-dispatcher.requests:
		if request.RequestID == "" || request.TraceID == "" {
			t.Fatalf("request trace fields = request_id %q trace_id %q", request.RequestID, request.TraceID)
		}
		if request.Message.Content != "hello" || request.Message.ExternalMessageID != "message-1" || request.Message.ExternalUserID != "user-1" {
			t.Fatalf("dispatch message = %+v", request.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("callback did not dispatch")
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerCloseCancelsAndJoinsAcceptedDrain(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1), canceled: make(chan struct{})}
	handler := newCallbackTestHandler(t, dispatcher)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, callbackTestRequest(t, "message-2", "user-2", "wait"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("callback response = %d", recorder.Code)
	}
	select {
	case <-dispatcher.requests:
	case <-time.After(time.Second):
		t.Fatal("callback did not reach dispatcher")
	}
	closed := make(chan error, 1)
	go func() { closed <- handler.Close() }()
	select {
	case <-dispatcher.canceled:
	case <-time.After(time.Second):
		t.Fatal("handler close did not cancel dispatch")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler close did not join dispatch")
	}
	shutdownResponse := httptest.NewRecorder()
	handler.ServeHTTP(shutdownResponse, callbackTestRequest(t, "message-3", "user-2", "after shutdown"))
	if shutdownResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown response = %d", shutdownResponse.Code)
	}
}

func TestDynamicHandlerRoutesOnlyVerifiedBinding(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "route-key", "env/wecom", app.AppID)
	handler, err := New(Config{
		Candidates:  &dynamicCandidateConsumer{binding: binding},
		Tenants:     dynamicTenantRepository{value: dynamicTestTenant(t)},
		Apps:        dynamicAppRepository{value: app},
		Credentials: dynamicCredentials{values: map[string]Credentials{binding.SecretRef: {CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), AppSecret: "app-secret"}}},
		Dispatcher:  dispatcher,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, callbackTestRequestAtPath(t, "/wecom/callback/route-key", "message-dynamic", "user-1", "hello"))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "success" {
		t.Fatalf("dynamic callback response = %d %q", recorder.Code, recorder.Body.String())
	}
	select {
	case request := <-dispatcher.requests:
		target, ok := request.Principal.RoutingTarget()
		if !ok || request.Principal.TenantID() != binding.TenantID || target.BindingID != binding.BindingID || request.RequestID == "" || request.TraceID == "" {
			t.Fatalf("dynamic dispatch request = %+v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("verified dynamic callback did not dispatch")
	}

	badSignature := callbackTestRequestAtPath(t, "/wecom/callback/route-key", "message-bad", "user-1", "hello")
	badQuery := badSignature.URL.Query()
	badQuery.Set("msg_signature", "bad")
	badSignature.URL.RawQuery = badQuery.Encode()
	badResponse := httptest.NewRecorder()
	handler.ServeHTTP(badResponse, badSignature)
	if badResponse.Code != http.StatusForbidden {
		t.Fatalf("bad dynamic signature response = %d", badResponse.Code)
	}

	unknown := httptest.NewRecorder()
	handler.ServeHTTP(unknown, callbackTestRequestAtPath(t, "/wecom/callback", "message-unknown", "user-1", "hello"))
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown route response = %d", unknown.Code)
	}
}

func TestDynamicHandlerAnswersVerifiedChallenge(t *testing.T) {
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "challenge-key", "env/wecom", app.AppID)
	handler, err := New(Config{
		Candidates:  &dynamicCandidateConsumer{binding: binding},
		Tenants:     dynamicTenantRepository{value: dynamicTestTenant(t)},
		Apps:        dynamicAppRepository{value: app},
		Credentials: dynamicCredentials{values: map[string]Credentials{binding.SecretRef: {CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), AppSecret: "app-secret"}}},
		Dispatcher:  &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	ciphertext := encryptCallbackTestPayload(t, bytes.Repeat([]byte{1}, 32), "receive", []byte("challenge"))
	parts := []string{"token", "123", "456", ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- WeCom requires SHA-1 callback signatures.
	request := httptest.NewRequest(http.MethodGet, "/wecom/callback/challenge-key?msg_signature="+hex.EncodeToString(sum[:])+"&timestamp=123&nonce=456&echostr="+url.QueryEscape(ciphertext), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "challenge" {
		t.Fatalf("challenge response = %d %q", response.Code, response.Body.String())
	}
}

func TestBindingProviderUsesActiveWeComBindingAndCachesProvider(t *testing.T) {
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "binding-provider-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", BindingKey: "wecom", Channel: channels.ChannelWeCom,
		ProviderAccountID: "corp", PublicRouteKeyDigest: routeDigest, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SecretRef: "env/wecom", Protocol: channels.ProtocolConfiguration{WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp", AgentID: "1", ReceiveID: "receive"}}, Status: channels.StatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup := &bindingLookupStub{binding: binding}
	credentials := &credentialResolverStub{credentials: Credentials{AppSecret: "app-secret"}}
	provider := &BindingProvider{Bindings: lookup, Credentials: credentials}
	value := storage.ReplyOutbox{TenantID: binding.TenantID, ReplyTarget: storage.ReplyTarget{BindingID: binding.BindingID, ConversationKind: "direct", ReceiverID: "user-1"}}
	first, err := provider.provider(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.provider(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.CorpID != "corp" || first.AgentID != "1" {
		t.Fatalf("binding provider = %+v, cached=%t", first, first == second)
	}
	if lookup.calls != 2 || credentials.calls != 2 {
		t.Fatalf("lookup=%d credentials=%d", lookup.calls, credentials.calls)
	}

	inactive := binding.Clone()
	inactive.Status = channels.StatusSuspended
	lookup.binding = &inactive
	provider.providers = nil
	_, err = provider.provider(context.Background(), value)
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Retryable || deliveryErr.Class != "invalid" {
		t.Fatalf("inactive binding error = %v", err)
	}
}

type callbackDispatchStub struct {
	requests chan gateway.DispatchRequest
	canceled chan struct{}
}

func (stub *callbackDispatchStub) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	stub.requests <- request
	if request.Accepted != nil {
		request.Accepted <- struct{}{}
	}
	output := make(chan gateway.DispatchEvent)
	if stub.canceled == nil {
		close(output)
		return output, nil
	}
	go func() {
		<-ctx.Done()
		close(stub.canceled)
		close(output)
	}()
	return output, nil
}

func newCallbackTestHandler(t *testing.T, dispatcher gateway.DispatchService) *Handler {
	t.Helper()
	key := bytes.Repeat([]byte{1}, 32)
	baseCtx, cancel := context.WithCancel(context.Background())
	return &Handler{
		static:           &callbackState{token: "token", receiveID: "receive", agentID: "1", key: key},
		dispatcher:       dispatcher,
		maxBodyBytes:     1 << 20,
		executionTimeout: time.Minute,
		baseCtx:          baseCtx,
		cancel:           cancel,
	}
}

func callbackTestRequest(t *testing.T, messageID, userID, content string) *http.Request {
	return callbackTestRequestAtPath(t, "/", messageID, userID, content)
}

func callbackTestRequestAtPath(t *testing.T, path, messageID, userID, content string) *http.Request {
	t.Helper()
	plain := []byte("<xml><MsgId>" + messageID + "</MsgId><FromUserName>" + userID + "</FromUserName><MsgType>text</MsgType><AgentID>1</AgentID><Content>" + content + "</Content></xml>")
	ciphertext := encryptCallbackTestPayload(t, bytes.Repeat([]byte{1}, 32), "receive", plain)
	timestamp, nonce := "123", "456"
	parts := []string{"token", timestamp, nonce, ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- required by the WeCom protocol.
	query := url.Values{"msg_signature": {hex.EncodeToString(sum[:])}, "timestamp": {timestamp}, "nonce": {nonce}}
	return httptest.NewRequest(http.MethodPost, path+"?"+query.Encode(), strings.NewReader("<xml><Encrypt>"+ciphertext+"</Encrypt></xml>"))
}

func encryptCallbackTestPayload(t *testing.T, key []byte, receiveID string, message []byte) string {
	t.Helper()
	plain := append(bytes.Repeat([]byte{2}, 16), make([]byte, 4)...)
	binary.BigEndian.PutUint32(plain[16:20], uint32(len(message))) // #nosec G115 -- test payloads are bounded by the callback fixture.
	plain = append(plain, message...)
	plain = append(plain, receiveID...)
	padding := wecomBlockSize - len(plain)%wecomBlockSize
	plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, plain)
	return base64.StdEncoding.EncodeToString(encrypted)
}

type bindingLookupStub struct {
	binding *channels.Binding
	calls   int
}

func (stub *bindingLookupStub) Get(_ context.Context, _, _ string) (*channels.Binding, error) {
	stub.calls++
	value := stub.binding.Clone()
	return &value, nil
}

type credentialResolverStub struct {
	credentials Credentials
	calls       int
}

func (stub *credentialResolverStub) Resolve(_ context.Context, _ channels.SecretScope) (Credentials, error) {
	stub.calls++
	return stub.credentials, nil
}

type dynamicCandidateConsumer struct{ binding *channels.Binding }

func (stub *dynamicCandidateConsumer) LookupCandidates(_ context.Context, channel channels.Channel, _ string) ([]channels.CandidateBindingContext, error) {
	if channel != channels.ChannelWeCom {
		return nil, errors.New("unexpected channel")
	}
	return []channels.CandidateBindingContext{{Channel: channel}}, nil
}
func (stub *dynamicCandidateConsumer) Get(_ context.Context, tenantID, bindingID string) (*channels.Binding, error) {
	if stub.binding == nil || stub.binding.TenantID != tenantID || stub.binding.BindingID != bindingID {
		return nil, channels.ErrNotFound
	}
	value := stub.binding.Clone()
	return &value, nil
}
func (stub *dynamicCandidateConsumer) ConsumeCandidate(context.Context, channels.CandidateBindingContext) (*channels.Binding, error) {
	value := stub.binding.Clone()
	return &value, nil
}

type dynamicCredentials struct{ values map[string]Credentials }

func (resolver dynamicCredentials) Resolve(_ context.Context, scope channels.SecretScope) (Credentials, error) {
	credentials, ok := resolver.values[scope.SecretRef]
	if !ok {
		return Credentials{}, errors.New("credential not found")
	}
	return credentials, nil
}

type dynamicTenantRepository struct {
	tenant.Repository
	value *tenant.Tenant
}

func (repository dynamicTenantRepository) Get(_ context.Context, tenantID string) (*tenant.Tenant, error) {
	if repository.value == nil || repository.value.TenantID != tenantID {
		return nil, channels.ErrNotFound
	}
	value := repository.value.Clone()
	return &value, nil
}

type dynamicAppRepository struct {
	agent.Repository
	value *agent.App
}

func (repository dynamicAppRepository) Get(_ context.Context, tenantID, appID string) (*agent.App, error) {
	if repository.value == nil || repository.value.TenantID != tenantID || repository.value.AppID != appID {
		return nil, channels.ErrNotFound
	}
	value := repository.value.Clone()
	return &value, nil
}

func dynamicTestBinding(t *testing.T, routeKey, secretRef, appID string) *channels.Binding {
	t.Helper()
	digest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, routeKey)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", BindingKey: "wecom", Channel: channels.ChannelWeCom,
		ProviderAccountID: "corp", PublicRouteKeyDigest: digest, AppID: appID, SecretRef: secretRef,
		Protocol: channels.ProtocolConfiguration{WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp", AgentID: "1", ReceiveID: "receive"}}, Status: channels.StatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func dynamicTestTenant(t *testing.T) *tenant.Tenant {
	t.Helper()
	value, err := tenant.NewTenant(tenant.CreateInput{TenantKey: "dynamic", DisplayName: "Dynamic Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	value.TenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	return value
}

func dynamicTestApp(t *testing.T, tenantID string) *agent.App {
	t.Helper()
	value, err := agent.NewApp(agent.CreateInput{TenantID: tenantID, AppKey: "dynamic", DisplayName: "Dynamic App", Description: "callback test"})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(1)
	value.Status = agent.StatusActive
	value.CurrentRevision = &revision
	value.Version = 2
	value.UpdatedAt = value.CreatedAt.Add(time.Second)
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	return value
}

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
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
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

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	stub := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	for _, config := range []Config{
		{},
		{Dispatcher: stub, MaxBodyBytes: -1},
		{Dispatcher: stub, ExecutionTimeout: -1},
		{Dispatcher: stub, Token: "token", ReceiveID: "receive", AgentID: "1"},
		{Dispatcher: stub, Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: "bad", Target: channels.RoutingTarget{}},
		{Dispatcher: stub, Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Target: channels.RoutingTarget{}, Candidates: &dynamicCandidateConsumer{}},
	} {
		if handler, err := New(config); handler != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid config = handler %v, err %v", handler, err)
		}
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
	if _, err := decodeAESKey("bad"); err == nil {
		t.Fatal("invalid AES key accepted")
	}
	if (&Handler{}).validSignature("a", "b", "c", "d") {
		t.Fatal("unconfigured handler accepted signature")
	}
}

func TestHandlerVerifyStaticCallbackState(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	t.Cleanup(func() { _ = handler.Close() })
	ciphertext := encryptCallbackTestPayload(t, handler.static.key, handler.static.receiveID, []byte("static callback"))
	request := callbackVerificationRequest("token", "/", ciphertext)
	plain, state, err := handler.verify(request, ciphertext)
	if err != nil || string(plain) != "static callback" || state.token != handler.static.token || state.receiveID != handler.static.receiveID || state.agentID != handler.static.agentID || !bytes.Equal(state.key, handler.static.key) {
		t.Fatalf("verify = plain %q state %+v err %v", plain, state, err)
	}
	query := request.URL.Query()
	query.Set("msg_signature", "bad")
	request.URL.RawQuery = query.Encode()
	if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) {
		t.Fatalf("bad static signature error = %v", err)
	}
}

func TestHandlerVerifyDynamicCandidateBoundaries(t *testing.T) {
	t.Run("rejects malformed route and lookup failure", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		request.URL.Path = "/wecom/callback"
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) {
			t.Fatalf("malformed route error = %v", err)
		}
		request.URL.Path = "/wecom/callback/verify-route"
		consumer.lookupErr = errors.New("candidate lookup failed")
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) {
			t.Fatalf("lookup error = %v", err)
		}
	})

	t.Run("skips failed candidate and returns verified target", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		consumer.candidates = []channels.CandidateBindingContext{{Channel: channels.ChannelWeCom}, {Channel: channels.ChannelWeCom}}
		handler.credentials = &sequenceCredentialResolver{values: []Credentials{
			{CallbackToken: "wrong-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
			{CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
		}}
		plain, state, err := handler.verify(request, ciphertext)
		if err != nil || string(plain) != "dynamic callback" || state.principal.TenantID() == "" || consumer.consumeCalls != 2 {
			t.Fatalf("verify = plain %q state %+v consumes %d err %v", plain, state, consumer.consumeCalls, err)
		}
		target, ok := state.principal.RoutingTarget()
		if !ok || target.BindingID != consumer.binding.BindingID {
			t.Fatalf("principal target = %+v, ok=%t", target, ok)
		}
	})

	t.Run("rejects when every candidate fails verification", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		consumer.candidates = []channels.CandidateBindingContext{{Channel: channels.ChannelWeCom}, {Channel: channels.ChannelWeCom}}
		handler.credentials = &sequenceCredentialResolver{values: []Credentials{
			{CallbackToken: "wrong-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
			{CallbackToken: "wrong-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
		}}
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) || consumer.consumeCalls != 2 {
			t.Fatalf("exhausted candidates error = %v consumes %d", err, consumer.consumeCalls)
		}
	})

	t.Run("propagates cancellation", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		consumer.consumeErrs = []error{context.Canceled}
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	})
}

func TestHandlerRejectsUnsupportedMethodsAndRoutes(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	t.Cleanup(func() { _ = handler.Close() })
	methodResponse := httptest.NewRecorder()
	handler.ServeHTTP(methodResponse, httptest.NewRequest(http.MethodPut, "/", nil))
	if methodResponse.Code != http.StatusMethodNotAllowed || methodResponse.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("method response = %d allow %q", methodResponse.Code, methodResponse.Header().Get("Allow"))
	}
	routeResponse := httptest.NewRecorder()
	handler.routeKey = "expected"
	handler.ServeHTTP(routeResponse, httptest.NewRequest(http.MethodGet, "/wecom/callback/other", nil))
	if routeResponse.Code != http.StatusNotFound {
		t.Fatalf("route response = %d", routeResponse.Code)
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

func TestProviderMapsTransportAndPayloadErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		response string
		status   int
		want     string
	}{
		{name: "malformed token", response: "not-json", status: http.StatusOK, want: "provider_error"},
		{name: "token rejected", response: `{"errcode":40014}`, status: http.StatusOK, want: "unauthenticated"},
		{name: "send malformed", response: `{"errcode":0,"access_token":"token","expires_in":3600}`, status: http.StatusOK, want: "provider_error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(test.status)
				if request.URL.Path == "/cgi-bin/gettoken" && test.name == "send malformed" {
					_, _ = io.WriteString(writer, `{"errcode":0,"access_token":"token","expires_in":3600}`)
					return
				}
				_, _ = io.WriteString(writer, test.response)
			}))
			defer server.Close()
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client()}
			_, err := provider.Deliver(context.Background(), storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
			var deliveryErr *outbox.DeliveryError
			if !errors.As(err, &deliveryErr) || deliveryErr.Class != test.want {
				t.Fatalf("delivery error = %v", err)
			}
		})
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

func TestProviderDefaultsAndContextCancellation(t *testing.T) {
	provider := &Provider{}
	if provider.baseURL() != "https://qyapi.weixin.qq.com" || provider.client() == nil {
		t.Fatalf("provider defaults are invalid")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret"}).Deliver(canceled, storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "canceled" && deliveryErr.Class != "unavailable" {
		t.Fatalf("canceled delivery = %v", err)
	}
}

func TestProviderRejectsInvalidDeliveryInputs(t *testing.T) {
	valid := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}}
	provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", token: "token", tokenExpiry: time.Now().Add(time.Hour)}
	for _, test := range []struct {
		name     string
		provider *Provider
		context  context.Context
		value    storage.ReplyOutbox
	}{
		{name: "nil provider", context: context.Background(), value: valid},
		{name: "missing credentials", provider: &Provider{}, context: context.Background(), value: valid},
		{name: "nil context", provider: provider, value: valid},
		{name: "group target", provider: provider, context: context.Background(), value: storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "group", ReceiverID: "group"}}},
		{name: "missing recipient", provider: provider, context: context.Background(), value: storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct"}}},
		{name: "empty payload", provider: provider, context: context.Background(), value: storage.ReplyOutbox{ReplyTarget: valid.ReplyTarget}},
		{name: "oversized payload", provider: provider, context: context.Background(), value: storage.ReplyOutbox{Payload: strings.Repeat("x", maximumTextBytes+1), ReplyTarget: valid.ReplyTarget}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.provider.Deliver(test.context, test.value)
			assertDeliveryErrorClass(t, err, "invalid", false)
		})
	}
}

func TestProviderHandlesSendFailuresAndInvalidatesRejectedToken(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		response  string
		class     string
		retryable bool
		clears    bool
	}{
		{name: "non successful status", status: http.StatusBadGateway, class: "unavailable", retryable: true},
		{name: "malformed response", status: http.StatusOK, response: "not-json", class: "provider_error", retryable: true},
		{name: "missing message id", status: http.StatusOK, response: `{"errcode":0}`, class: "provider_error", retryable: true},
		{name: "expired token", status: http.StatusOK, response: `{"errcode":42001}`, class: "unauthenticated", retryable: true, clears: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/cgi-bin/message/send" {
					t.Fatalf("unexpected endpoint %s", request.URL.Path)
				}
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.response)
			}))
			defer server.Close()
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client(), token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
			_, err := provider.Deliver(context.Background(), storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
			assertDeliveryErrorClass(t, err, test.class, test.retryable)
			if test.clears && (!provider.tokenExpiry.IsZero() || provider.token != "") {
				t.Fatal("rejected access token remained cached")
			}
		})
	}
}

func TestProviderMapsCanceledAndTimedOutTransport(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		class string
	}{
		{name: "canceled", err: context.Canceled, class: "canceled"},
		{name: "timeout", err: context.DeadlineExceeded, class: "timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, test.err })}, token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
			_, err := provider.Deliver(context.Background(), storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
			assertDeliveryErrorClass(t, err, test.class, true)
		})
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

func TestHandlerAcceptedDrainOutlivesRequestCancellation(t *testing.T) {
	dispatched := make(chan context.Context, 1)
	canceled := make(chan struct{})
	dispatcher := dispatchFunc(func(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		dispatched <- ctx
		if request.Accepted != nil {
			request.Accepted <- struct{}{}
		}
		stream := make(chan gateway.DispatchEvent)
		go func() {
			<-ctx.Done()
			close(canceled)
			close(stream)
		}()
		return stream, nil
	})
	handler := newCallbackTestHandler(t, dispatcher)
	defer func() { _ = handler.Close() }()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request := callbackTestRequest(t, "message-request-cancel", "user", "hello").WithContext(requestCtx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("callback response = %d", response.Code)
	}
	var dispatchContext context.Context
	select {
	case dispatchContext = <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("callback did not reach dispatcher")
	}
	cancelRequest()
	if err := dispatchContext.Err(); err != nil {
		t.Fatalf("request cancellation canceled accepted drain: %v", err)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler close did not cancel accepted drain")
	}
}

func TestHandlerAcknowledgesCompletedAndDuplicateDispatch(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code int
	}{
		{name: "completed synchronously", code: http.StatusOK},
		{name: "duplicate", err: gateway.ErrDuplicateMessage, code: http.StatusOK},
		{name: "unavailable", err: errors.New("dispatcher unavailable"), code: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newCallbackTestHandler(t, dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
				return nil, test.err
			}))
			defer func() { _ = handler.Close() }()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, callbackTestRequest(t, "message-"+test.name, "user", "hello"))
			if response.Code != test.code {
				t.Fatalf("callback response = %d", response.Code)
			}
		})
	}
}

func TestHandlerRejectsInvalidChallengeAndMessageShape(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	defer func() { _ = handler.Close() }()
	challenge := httptest.NewRequest(http.MethodGet, "/?msg_signature=bad&timestamp=1&nonce=2&echostr=bad", nil)
	challengeResponse := httptest.NewRecorder()
	handler.ServeHTTP(challengeResponse, challenge)
	if challengeResponse.Code != http.StatusForbidden {
		t.Fatalf("invalid challenge response = %d", challengeResponse.Code)
	}
	request := callbackTestRequest(t, "message", "user", "hello")
	request.Body = io.NopCloser(strings.NewReader(requestBodyWithTrailingXML(t, request)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("trailing XML response = %d", response.Code)
	}
}

func TestDynamicHandlerRoutesOnlyVerifiedBinding(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "route-key", "env/wecom", app.AppID)
	writer, err := audit.NewInMemory(binding.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	writeSignal := make(chan struct{}, 2)
	handler, err := New(Config{
		Candidates:  &dynamicCandidateConsumer{binding: binding},
		Tenants:     dynamicTenantRepository{value: dynamicTestTenant(t)},
		Apps:        dynamicAppRepository{value: app},
		Credentials: dynamicCredentials{values: map[string]Credentials{binding.SecretRef: {CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), AppSecret: "app-secret"}}},
		Dispatcher:  dispatcher,
		AuditWriter: signalingAuditWriter{Writer: writer, Signal: writeSignal},
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
	case <-writeSignal:
	case <-time.After(time.Second):
		t.Fatal("accepted ingress audit was not appended")
	}
	var target channels.RoutingTarget
	var requestID, traceID string
	select {
	case request := <-dispatcher.requests:
		var ok bool
		target, ok = request.Principal.RoutingTarget()
		if !ok || request.Principal.TenantID() != binding.TenantID || target.BindingID != binding.BindingID || request.RequestID == "" || request.TraceID == "" {
			t.Fatalf("dynamic dispatch request = %+v", request)
		}
		requestID, traceID = request.RequestID, request.TraceID
	case <-time.After(time.Second):
		t.Fatal("verified dynamic callback did not dispatch")
	}
	staticHandler, err := New(Config{Dispatcher: dispatcher, Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Target: target})
	if err != nil {
		t.Fatalf("static handler = %v", err)
	}
	if staticHandler == nil {
		t.Fatal("static handler is nil")
	}
	_ = staticHandler.Close()
	assertIngressAudit(t, writer, 1, audit.EventIMIngressAccepted, audit.DecisionAccepted, "", requestID, traceID)
	assertDuplicateIngressAudit(t, target, writer)

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

func assertIngressAudit(t *testing.T, writer audit.Reader, count int, eventType audit.EventType, decision audit.Decision, errorType, requestID, traceID string) {
	t.Helper()
	var events []audit.Event
	var err error
	deadline := time.Now().Add(time.Second)
	for {
		events, err = writer.List(context.Background(), audit.Query{})
		if err != nil {
			t.Fatalf("ingress audit = %+v, err=%v", events, err)
		}
		if len(events) == count || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(events) != count {
		t.Fatalf("ingress audit = %+v, err=%v", events, err)
	}
	var event audit.Event
	found := 0
	for _, candidate := range events {
		if candidate.EventType == eventType {
			event = candidate
			found++
		}
	}
	if found != 1 {
		t.Fatalf("ingress event type %q count = %d, events = %+v", eventType, found, events)
	}
	if event.EventType != eventType {
		t.Fatalf("ingress event type = %q, want %q", event.EventType, eventType)
	}
	if event.Decision != decision {
		t.Fatalf("ingress decision = %q, want %q", event.Decision, decision)
	}
	if event.ErrorType != errorType {
		t.Fatalf("ingress error type = %q, want %q", event.ErrorType, errorType)
	}
	if requestID != "" && event.RequestID != requestID {
		t.Fatalf("ingress request ID = %q, want %q", event.RequestID, requestID)
	}
	if traceID != "" && event.TraceID != traceID {
		t.Fatalf("ingress trace ID = %q, want %q", event.TraceID, traceID)
	}
}

func assertDuplicateIngressAudit(t *testing.T, target channels.RoutingTarget, writer audit.Writer) {
	t.Helper()
	handler, err := New(Config{Dispatcher: dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		return nil, gateway.ErrDuplicateMessage
	}), Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Target: target, AuditWriter: writer})
	if err != nil {
		t.Fatalf("duplicate handler = %v", err)
	}
	defer func() { _ = handler.Close() }()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callbackTestRequest(t, "message-duplicate", "user-1", "hello"))
	if response.Code != http.StatusOK || response.Body.String() != "success" {
		t.Fatalf("duplicate callback response = %d %q", response.Code, response.Body.String())
	}
	assertIngressAudit(t, writer.(audit.Reader), 2, audit.EventIMIngressDuplicate, audit.DecisionDuplicate, string(audit.ErrorDuplicate), "", "")
}

func TestHandlerRejectsMalformedMessages(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	t.Cleanup(func() { _ = handler.Close() })
	for _, body := range []string{"", "<xml></xml>", "<xml><Encrypt>bad</Encrypt></xml>"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		if response.Code != http.StatusForbidden && response.Code != http.StatusBadRequest {
			t.Fatalf("malformed body %q status = %d", body, response.Code)
		}
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

func TestBindingProviderPreservesRetryableResolutionErrorsAndRotatesSecrets(t *testing.T) {
	binding := dynamicTestBinding(t, "binding-provider-resolution", "env/wecom", "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	value := storage.ReplyOutbox{TenantID: binding.TenantID, ReplyTarget: storage.ReplyTarget{BindingID: binding.BindingID, ConversationKind: "direct", ReceiverID: "user-1"}}

	t.Run("binding cancellation is preserved", func(t *testing.T) {
		provider := &BindingProvider{Bindings: &bindingLookupStub{err: context.Canceled}, Credentials: &credentialResolverStub{}}
		if _, err := provider.provider(context.Background(), value); !errors.Is(err, context.Canceled) {
			t.Fatalf("provider error = %v", err)
		}
	})

	t.Run("credential failures are retryable unless invalid", func(t *testing.T) {
		for _, want := range []struct {
			name  string
			err   error
			class string
			retry bool
		}{
			{name: "canceled", err: context.Canceled},
			{name: "unavailable", err: errors.New("resolver unavailable"), class: "unavailable", retry: true},
			{name: "invalid secret ref", err: channels.ErrNotFound, class: "invalid", retry: false},
		} {
			t.Run(want.name, func(t *testing.T) {
				provider := &BindingProvider{Bindings: &bindingLookupStub{binding: binding}, Credentials: &credentialResolverStub{err: want.err}}
				_, err := provider.provider(context.Background(), value)
				if errors.Is(want.err, context.Canceled) {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("provider error = %v", err)
					}
					return
				}
				var deliveryErr *outbox.DeliveryError
				if !errors.As(err, &deliveryErr) || deliveryErr.Class != want.class || deliveryErr.Retryable != want.retry {
					t.Fatalf("provider error = %v", err)
				}
			})
		}
	})

	t.Run("secret rotation replaces cached provider", func(t *testing.T) {
		provider := &BindingProvider{Bindings: &bindingLookupStub{binding: binding}, Credentials: &sequenceCredentialResolver{values: []Credentials{{AppSecret: "old"}, {AppSecret: "new"}}}}
		first, err := provider.provider(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		second, err := provider.provider(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		if first == second || first.AppSecret != "old" || second.AppSecret != "new" {
			t.Fatalf("provider rotation = first %+v second %+v", first, second)
		}
	})
}

func TestHandlerRejectsBodyLargerThanConfiguredLimit(t *testing.T) {
	handler := &Handler{maxBodyBytes: 3}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("1234"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("response status = %d", response.Code)
	}
}

func TestProviderRejectsInvalidAgentAndMapsTokenFailures(t *testing.T) {
	value := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}}
	invalidAgent := &Provider{CorpID: "corp", AgentID: "01", AppSecret: "secret", token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
	_, err := invalidAgent.Deliver(context.Background(), value)
	assertDeliveryErrorClass(t, err, "invalid", false)

	for _, test := range []struct {
		name      string
		status    int
		body      string
		class     string
		retryable bool
	}{
		{name: "unavailable", status: http.StatusBadGateway, class: "unavailable", retryable: true},
		{name: "malformed", status: http.StatusOK, body: "bad-json", class: "provider_error", retryable: true},
		{name: "missing token", status: http.StatusOK, body: `{"errcode":0}`, class: "provider_error", retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/cgi-bin/gettoken" {
					t.Fatalf("path = %s", request.URL.Path)
				}
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.body)
			}))
			defer server.Close()
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client()}
			_, err := provider.accessToken(context.Background())
			assertDeliveryErrorClass(t, err, test.class, test.retryable)
		})
	}

	for _, test := range []struct {
		name  string
		err   error
		class string
	}{
		{name: "deadline", err: context.DeadlineExceeded, class: "timeout"},
		{name: "unavailable", err: errors.New("network unavailable"), class: "unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, test.err })}}
			_, err := provider.accessToken(context.Background())
			assertDeliveryErrorClass(t, err, test.class, true)
		})
	}

	invalidURL := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: "http://[::1", token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
	_, err = invalidURL.Deliver(context.Background(), value)
	assertDeliveryErrorClass(t, err, "invalid", false)
}

func TestHandlerDrainsStreamAndRejectsCryptographicBoundaryFailures(t *testing.T) {
	stream := make(chan gateway.DispatchEvent, 1)
	stream <- gateway.DispatchEvent{}
	close(stream)
	handler := newCallbackTestHandler(t, dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		return stream, nil
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callbackTestRequest(t, "stream", "user", "hello"))
	if response.Code != http.StatusOK {
		t.Fatalf("stream response = %d", response.Code)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	if err := (*Handler)(nil).Close(); err != nil {
		t.Fatalf("nil close = %v", err)
	}
	if (*Handler)(nil).validSignature("signature", "timestamp", "nonce", "ciphertext") {
		t.Fatal("nil handler accepted a signature")
	}
	if _, err := (*Handler)(nil).decrypt("ciphertext"); !errors.Is(err, ErrVerification) {
		t.Fatalf("nil decrypt error = %v", err)
	}
	if _, err := decrypt(nil, "receive", "not-base64"); !errors.Is(err, ErrVerification) {
		t.Fatalf("invalid ciphertext error = %v", err)
	}
	if _, err := decrypt([]byte{1}, "receive", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, aes.BlockSize))); !errors.Is(err, ErrVerification) {
		t.Fatalf("invalid AES key error = %v", err)
	}
	key := bytes.Repeat([]byte{1}, 32)
	if _, err := decrypt(key, "receive", encryptCallbackTestPayload(t, key, "other", []byte("message"))); !errors.Is(err, ErrVerification) {
		t.Fatalf("receive ID mismatch error = %v", err)
	}
	if _, err := unpad(nil); !errors.Is(err, ErrVerification) {
		t.Fatalf("empty padding error = %v", err)
	}
}

type callbackDispatchStub struct {
	requests chan gateway.DispatchRequest
	canceled chan struct{}
}

type signalingAuditWriter struct {
	audit.Writer
	Signal chan<- struct{}
}

func (writer signalingAuditWriter) Append(ctx context.Context, event audit.Event) (audit.AppendResult, error) {
	result, err := writer.Writer.Append(ctx, event)
	if err == nil {
		writer.Signal <- struct{}{}
	}
	return result, err
}

type dispatchFunc func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error)

func (dispatch dispatchFunc) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	return dispatch(ctx, request)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func assertDeliveryErrorClass(t *testing.T, err error, class string, retryable bool) {
	t.Helper()
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != class || deliveryErr.Retryable != retryable {
		t.Fatalf("delivery error = %v, want class %q retryable %t", err, class, retryable)
	}
}

func requestBodyWithTrailingXML(t *testing.T, request *http.Request) string {
	t.Helper()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body) + "<trailing/>"
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

func callbackVerificationRequest(token, path, ciphertext string) *http.Request {
	timestamp, nonce := "123", "456"
	parts := []string{token, timestamp, nonce, ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- required by the WeCom protocol.
	query := url.Values{"msg_signature": {hex.EncodeToString(sum[:])}, "timestamp": {timestamp}, "nonce": {nonce}}
	return httptest.NewRequest(http.MethodGet, path+"?"+query.Encode(), nil)
}

func callbackTestRequestAtPath(t *testing.T, path, messageID, userID, content string) *http.Request {
	t.Helper()
	plain := []byte("<xml><MsgId>" + messageID + "</MsgId><FromUserName>" + userID + "</FromUserName><MsgType>text</MsgType><AgentID>1</AgentID><Content>" + content + "</Content></xml>")
	ciphertext := encryptCallbackTestPayload(t, bytes.Repeat([]byte{1}, 32), "receive", plain)
	request := callbackVerificationRequest("token", path, ciphertext)
	request.Method = http.MethodPost
	request.Body = io.NopCloser(strings.NewReader("<xml><Encrypt>" + ciphertext + "</Encrypt></xml>"))
	return request
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
	err     error
}

func (stub *bindingLookupStub) Get(_ context.Context, _, _ string) (*channels.Binding, error) {
	stub.calls++
	if stub.err != nil {
		return nil, stub.err
	}
	value := stub.binding.Clone()
	return &value, nil
}

type credentialResolverStub struct {
	credentials Credentials
	calls       int
	err         error
}

func (stub *credentialResolverStub) Resolve(_ context.Context, _ channels.SecretScope) (Credentials, error) {
	stub.calls++
	if stub.err != nil {
		return Credentials{}, stub.err
	}
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

type verifyCandidateConsumer struct {
	binding      *channels.Binding
	candidates   []channels.CandidateBindingContext
	lookupErr    error
	consumeErrs  []error
	consumeCalls int
}

func (stub *verifyCandidateConsumer) LookupCandidates(_ context.Context, channel channels.Channel, _ string) ([]channels.CandidateBindingContext, error) {
	if stub.lookupErr != nil {
		return nil, stub.lookupErr
	}
	if channel != channels.ChannelWeCom {
		return nil, errors.New("unexpected channel")
	}
	return append([]channels.CandidateBindingContext(nil), stub.candidates...), nil
}
func (stub *verifyCandidateConsumer) Get(context.Context, string, string) (*channels.Binding, error) {
	return nil, errors.New("unsupported")
}
func (stub *verifyCandidateConsumer) ConsumeCandidate(context.Context, channels.CandidateBindingContext) (*channels.Binding, error) {
	index := stub.consumeCalls
	stub.consumeCalls++
	if index < len(stub.consumeErrs) && stub.consumeErrs[index] != nil {
		return nil, stub.consumeErrs[index]
	}
	value := stub.binding.Clone()
	return &value, nil
}

type sequenceCredentialResolver struct {
	values []Credentials
	calls  int
}

func (resolver *sequenceCredentialResolver) Resolve(context.Context, channels.SecretScope) (Credentials, error) {
	if resolver.calls >= len(resolver.values) {
		return Credentials{}, errors.New("unexpected credential resolution")
	}
	value := resolver.values[resolver.calls]
	resolver.calls++
	return value, nil
}

func newDynamicVerifyFixture(t *testing.T) (*Handler, *verifyCandidateConsumer, string, *http.Request) {
	t.Helper()
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "verify-route", "env/wecom", app.AppID)
	consumer := &verifyCandidateConsumer{binding: binding, candidates: []channels.CandidateBindingContext{{Channel: channels.ChannelWeCom}}}
	handler := &Handler{
		candidates:  consumer,
		tenants:     dynamicTenantRepository{value: dynamicTestTenant(t)},
		apps:        dynamicAppRepository{value: app},
		credentials: &sequenceCredentialResolver{values: []Credentials{{CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))}}},
	}
	ciphertext := encryptCallbackTestPayload(t, bytes.Repeat([]byte{1}, 32), "receive", []byte("dynamic callback"))
	return handler, consumer, ciphertext, callbackVerificationRequest("token", "/wecom/callback/verify-route", ciphertext)
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

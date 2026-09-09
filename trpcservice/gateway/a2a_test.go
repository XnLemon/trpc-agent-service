package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestA2ACompatibilityUsesPlatformAuthenticationAndDispatch(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{
		{Type: DispatchEventMessage, Text: "a2a reply"},
		{Type: DispatchEventDone, Done: true},
	}}
	identity := APIIdentity{TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "api-subject"}
	authenticator, err := NewStaticAPIAuthenticator(map[string]APIIdentity{"credential": identity})
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := NewTenantLimiter(TenantLimiterConfig{MaxConcurrent: 8, MaxRequests: 100})
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyStore(IdempotencyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPConfig{
		Dispatcher: stub, Authenticator: authenticator, Ready: func() bool { return true },
		Limiter: limiter, Idempotency: idempotency,
		A2A: A2AConfig{Enabled: true, Host: "http://example.invalid", Path: "/a2a", AgentName: "tenant-agent"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	card := httptest.NewRecorder()
	cardRequest := httptest.NewRequest(http.MethodGet, "/a2a/.well-known/agent-card.json", nil)
	handler.ServeHTTP(card, cardRequest)
	if card.Code != http.StatusOK || !strings.Contains(card.Body.String(), "tenant-agent") {
		t.Fatalf("agent card status=%d body=%s", card.Code, card.Body.String())
	}

	requestBody := `{"jsonrpc":"2.0","id":"rpc-1","method":"message/send","params":{"message":{"kind":"message","messageId":"message-1","role":"user","contextId":"session-1","parts":[{"kind":"text","text":"hello"}]}}}`
	request := httptest.NewRequest(http.MethodPost, "/a2a/", strings.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer credential")
	request.Header.Set("X-Request-ID", "a2a-request")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("A2A status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("A2A response=%q: %v", recorder.Body.String(), err)
	}
	if response["error"] != nil {
		t.Fatalf("A2A error response=%#v", response)
	}
	calls, dispatch := stub.snapshot()
	if calls != 1 || dispatch.Principal.TenantID() != identity.TenantID || dispatch.Message.Content != "hello" {
		t.Fatalf("dispatch calls=%d request=%#v", calls, dispatch)
	}
}

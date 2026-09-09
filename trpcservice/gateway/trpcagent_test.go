package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTRPCAgentCompatibilityAuthenticatesBeforeUpstreamHandler(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{
		{Type: DispatchEventMessage, Text: "native reply"},
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
		TRPCAgent: TRPCAgentConfig{Enabled: true, AppName: "gateway-agent"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	requestBody := `{"session":{"userId":"forged-user","sessionId":"session-1"},"input":{"role":"user","content":"hello"},"runOptions":{"requestId":"native-request"}}`
	request := httptest.NewRequest(http.MethodPost, "/trpc-agent/v1/apps/gateway-agent/runs", strings.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer credential")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("native status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["status"] != "completed" {
		t.Fatalf("native response=%#v", response)
	}
	calls, dispatch := stub.snapshot()
	if calls != 1 || dispatch.Principal.AppID() != identity.AppID || dispatch.Message.ExternalUserID != identity.SubjectID {
		t.Fatalf("dispatch calls=%d request=%#v", calls, dispatch)
	}

	unauthorized := httptest.NewRecorder()
	unauthorizedRequest := httptest.NewRequest(http.MethodPost, "/trpc-agent/v1/apps/gateway-agent/runs", strings.NewReader(requestBody))
	unauthorizedRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(unauthorized, unauthorizedRequest)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
}

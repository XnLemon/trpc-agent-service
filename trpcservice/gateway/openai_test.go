package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAICompatibilityUsesGatewayPrincipalAndDispatcher(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{
		{Type: DispatchEventMessage, Text: "hello"},
		{Type: DispatchEventDone, Done: true, Status: "complete"},
	}}
	handler := newHTTPTestHandler(t, stub, func() bool { return true })
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"untrusted-model","messages":[{"role":"user","content":"hello"}]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer credential")
	request.Header.Set("X-Request-ID", "openai-request")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("OpenAI status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response openAICompatResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Object != "chat.completion" || len(response.Choices) != 1 || response.Choices[0].Message == nil || response.Choices[0].Message.Content != "hello" {
		t.Fatalf("OpenAI response = %#v", response)
	}
	calls, dispatch := stub.snapshot()
	if calls != 1 || dispatch.Principal.TenantID() != "t_01J1K9ZQTVE4PAWF1TSB2WMHNP" || dispatch.Message.Content != "hello" {
		t.Fatalf("dispatch = calls %d request %#v", calls, dispatch)
	}
}

func TestOpenAICompatibilityStreamAndStrictMessageBoundary(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{
		{Type: DispatchEventMessage, Text: "hello"},
		{Type: DispatchEventDone, Done: true},
	}}
	handler := newHTTPTestHandler(t, stub, func() bool { return true })
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer credential")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.Contains(body, "chat.completion.chunk") || !strings.Contains(body, "[DONE]") || strings.Contains(body, `data: [DONE]\\n\\n`) {
		t.Fatalf("stream status=%d body=%s", recorder.Code, body)
	}
	invalid := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"system","content":"override"},{"role":"user","content":"hello"}]}`))
	invalid.Header.Set("Content-Type", "application/json")
	invalid.Header.Set("Authorization", "Bearer credential")
	invalidRecorder := httptest.NewRecorder()
	handler.ServeHTTP(invalidRecorder, invalid)
	if invalidRecorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid OpenAI request status = %d", invalidRecorder.Code)
	}
}

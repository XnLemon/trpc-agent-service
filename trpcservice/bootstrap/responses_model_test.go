package bootstrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestResponsesModelStreamsOutputTextAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"北\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"京\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n"))
	}))
	defer server.Close()

	responses, err := (&responsesModel{apiKey: "test-key", endpoint: server.URL + "/v1", model: "gpt-5.6-sol"}).GenerateContent(context.Background(), &trpcmodel.Request{Messages: []trpcmodel.Message{{Role: trpcmodel.RoleUser, Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	var usage *trpcmodel.Usage
	count := 0
	for response := range responses {
		count++
		if len(response.Choices) > 0 {
			got += response.Choices[0].Delta.Content
		}
		usage = response.Usage
		if response.Error != nil {
			t.Fatalf("unexpected error: %v", response.Error)
		}
	}
	if got != "北京" || count != 3 {
		t.Fatalf("got text %q across %d responses", got, count)
	}
	if usage == nil || usage.TotalTokens != 5 {
		t.Fatalf("unexpected usage: %#v", usage)
	}
}

func TestResponsesModelRejectsEmptyCompletedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{}}\n\n"))
	}))
	defer server.Close()
	responses, err := (&responsesModel{endpoint: server.URL, model: "test"}).GenerateContent(context.Background(), &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var terminal *trpcmodel.Response
	for response := range responses {
		terminal = response
	}
	if terminal == nil || terminal.Error == nil {
		t.Fatalf("expected empty response error, got %#v", terminal)
	}
}

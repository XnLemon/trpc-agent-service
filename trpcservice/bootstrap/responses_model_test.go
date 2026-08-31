package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestResponsesModelInfoAndInputDefaults(t *testing.T) {
	model := &responsesModel{model: "gpt-5.6-sol"}
	if got := model.Info(); got.Name != "gpt-5.6-sol" {
		t.Fatalf("Info name = %q", got.Name)
	}
	input := responsesInput(&trpcmodel.Request{Messages: []trpcmodel.Message{
		{ContentParts: []trpcmodel.ContentPart{{Text: stringPtr("part-a")}, {Text: nil}, {Text: stringPtr("part-b")}}},
	}})
	if len(input) != 1 || input[0].Role != string(trpcmodel.RoleUser) || input[0].Content[0].Text != "part-apart-b" {
		t.Fatalf("input defaults = %#v", input)
	}
}

func TestResponsesModelMapsContentParts(t *testing.T) {
	input := responsesInput(&trpcmodel.Request{Messages: []trpcmodel.Message{{
		Role:    trpcmodel.RoleUser,
		Content: "describe",
		ContentParts: []trpcmodel.ContentPart{
			{Type: trpcmodel.ContentTypeImage, Image: &trpcmodel.Image{Data: []byte{1, 2}, Detail: "auto", Format: "png"}},
			{Type: trpcmodel.ContentTypeFile, File: &trpcmodel.File{Name: "brief.pdf", Data: []byte("pdf"), MimeType: "application/pdf"}},
			{Type: trpcmodel.ContentTypeAudio, Audio: &trpcmodel.Audio{Data: []byte("mp3"), Format: "audio/mpeg"}},
			{Type: trpcmodel.ContentTypeVideo, Video: &trpcmodel.Video{Data: []byte("mp4"), Format: "mp4"}},
		},
	}}})
	if len(input) != 1 || len(input[0].Content) != 5 {
		t.Fatalf("input = %#v", input)
	}
	parts := input[0].Content
	if parts[0].Type != "input_text" || parts[0].Text != "describe" {
		t.Fatalf("text part = %#v", parts[0])
	}
	if parts[1].Type != "input_image" || parts[1].ImageURL != "data:image/png;base64,AQI=" || parts[1].Detail != "auto" {
		t.Fatalf("image part = %#v", parts[1])
	}
	if parts[2].Type != "input_file" || parts[2].Filename != "brief.pdf" || parts[2].FileData != "data:application/pdf;base64,cGRm" {
		t.Fatalf("file part = %#v", parts[2])
	}
	if parts[3].Type != "input_audio" || parts[3].InputAudio == nil || parts[3].InputAudio.Data != "bXAz" || parts[3].InputAudio.Format != "mp3" {
		t.Fatalf("audio part = %#v", parts[3])
	}
	if parts[4].Type != "input_text" || parts[4].Text != "[video attachment omitted: model video input is not enabled]" {
		t.Fatalf("video fallback part = %#v", parts[4])
	}
}

func TestResponsesModelGenerateContentRejectsInvalidArguments(t *testing.T) {
	model := &responsesModel{}
	if responses, err := model.GenerateContent(nil, &trpcmodel.Request{}); responses != nil || err == nil {
		t.Fatalf("nil context = responses %#v err %v", responses, err)
	}
	if responses, err := model.GenerateContent(context.Background(), nil); responses != nil || err == nil {
		t.Fatalf("nil request = responses %#v err %v", responses, err)
	}
}

func TestResponsesModelStreamsOutputTextAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer test-key" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var body struct {
			Model   string               `json:"model"`
			Input   []responsesInputItem `json:"input"`
			Store   bool                 `json:"store"`
			Stream  bool                 `json:"stream"`
			Include []string             `json:"include"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Model != "gpt-5.6-sol" || len(body.Input) != 1 || body.Input[0].Content[0].Text != "hello" || body.Store || !body.Stream || len(body.Include) != 1 {
			t.Fatalf("request body = %#v err=%v", body, err)
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

func TestResponsesModelReturnsHTTPAndTransportErrors(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		status   int
		wantText string
	}{
		{name: "http status", status: http.StatusBadGateway, wantText: "responses API returned status 502"},
		{name: "transport", endpoint: "://bad endpoint", wantText: "missing protocol scheme"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := tt.endpoint
			if endpoint == "" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "upstream", tt.status) }))
				defer server.Close()
				endpoint = server.URL
			}
			responses, err := (&responsesModel{endpoint: endpoint}).GenerateContent(context.Background(), &trpcmodel.Request{})
			if err != nil {
				t.Fatal(err)
			}
			got := <-responses
			message := ""
			if got != nil && got.Error != nil {
				message = got.Error.Message
			}
			if got == nil || got.Error == nil || !strings.Contains(message, tt.wantText) || !got.Done {
				t.Fatalf("error response = %#v message=%q", got, message)
			}
			if _, ok := <-responses; ok {
				t.Fatal("response channel did not close")
			}
		})
	}
}

func TestResponsesModelSkipsMalformedEventsAndReportsScannerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "comment\n")
		_, _ = io.WriteString(w, "data: not-json\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")
	}))
	defer server.Close()
	responses, err := (&responsesModel{endpoint: server.URL}).GenerateContent(context.Background(), &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for response := range responses {
		if len(response.Choices) > 0 {
			got = append(got, response.Choices[0].Delta.Content)
		}
	}
	if !bytes.Equal([]byte(strings.Join(got, "")), []byte("ok")) {
		t.Fatalf("text = %q", got)
	}

	reader := &errorReader{}
	_, _, err = consumeResponsesStream(context.Background(), bufio.NewScanner(reader), make(chan *trpcmodel.Response, 1))
	if !errors.Is(err, errReaderFailure) {
		t.Fatalf("scanner error = %v", err)
	}
}

func TestSendResponseHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sendResponse(ctx, make(chan *trpcmodel.Response), &trpcmodel.Response{}) {
		t.Fatal("sendResponse succeeded with canceled context")
	}
}

var errReaderFailure = errors.New("reader failure")

type errorReader struct{}

func (*errorReader) Read([]byte) (int, error) { return 0, errReaderFailure }

func stringPtr(value string) *string { return &value }

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

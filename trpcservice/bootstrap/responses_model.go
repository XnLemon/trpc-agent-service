package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type responsesModel struct {
	apiKey   string
	endpoint string
	model    string
	client   *http.Client
}

func (m *responsesModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: m.model} }
func (m *responsesModel) GenerateContent(ctx context.Context, request *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	if ctx == nil {
		return nil, fmt.Errorf("responses model context is required")
	}
	if request == nil {
		return nil, fmt.Errorf("responses model request is required")
	}
	out := make(chan *trpcmodel.Response, 8)
	go m.stream(ctx, request, out)
	return out, nil
}

type responsesInputItem struct {
	Role    string                 `json:"role"`
	Content []responsesContentPart `json:"content"`
}
type responsesContentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (m *responsesModel) stream(ctx context.Context, request *trpcmodel.Request, out chan<- *trpcmodel.Response) {
	defer close(out)
	body, err := m.requestBody(request)
	if err != nil {
		m.emitError(out, err)
		return
	}
	resp, err := m.doRequest(ctx, body)
	if err != nil {
		m.emitError(out, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		m.emitError(out, fmt.Errorf("responses API returned status %d", resp.StatusCode))
		return
	}
	usage, emittedText, err := consumeResponsesStream(ctx, bufio.NewScanner(resp.Body), out)
	if err != nil {
		m.emitError(out, err)
		return
	}
	terminal := &trpcmodel.Response{Object: "response", Done: true, Usage: usage}
	if !emittedText {
		terminal.Error = &trpcmodel.ResponseError{Message: "responses API completed without output text", Type: trpcmodel.ErrorTypeAPIError}
	}
	sendResponse(ctx, out, terminal)
}

func (m *responsesModel) requestBody(request *trpcmodel.Request) ([]byte, error) {
	return json.Marshal(map[string]any{
		"model":   m.model,
		"input":   responsesInput(request),
		"store":   false,
		"stream":  true,
		"include": []string{"reasoning.encrypted_content"},
	})
}

func responsesInput(request *trpcmodel.Request) []responsesInputItem {
	input := make([]responsesInputItem, 0, len(request.Messages))
	for _, message := range request.Messages {
		role := string(message.Role)
		if role == "" {
			role = string(trpcmodel.RoleUser)
		}
		text := message.Content
		if text == "" {
			for _, part := range message.ContentParts {
				if part.Text != nil {
					text += *part.Text
				}
			}
		}
		input = append(input, responsesInputItem{Role: role, Content: []responsesContentPart{{Type: "input_text", Text: text}}})
	}
	return input
}

func (m *responsesModel) doRequest(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(m.endpoint, "/")+"/responses", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+m.apiKey)
	req.Header.Set("Content-Type", "application/json")
	client := m.client
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

type responsesEvent struct {
	Type     string `json:"type"`
	Delta    string `json:"delta"`
	Response struct {
		Usage *struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
			TotalTokens  int64 `json:"total_tokens"`
		} `json:"usage"`
	} `json:"response"`
}

func consumeResponsesStream(ctx context.Context, scanner *bufio.Scanner, out chan<- *trpcmodel.Response) (*trpcmodel.Usage, bool, error) {
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var usage *trpcmodel.Usage
	emittedText := false
	for scanner.Scan() {
		event, ok := parseResponsesEvent(scanner.Text())
		if !ok {
			continue
		}
		if event.Type == "response.output_text.delta" && event.Delta != "" {
			emittedText = true
			if !sendResponse(ctx, out, &trpcmodel.Response{Object: "response.output_text.delta", IsPartial: true, Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Role: trpcmodel.RoleAssistant, Content: event.Delta}}}}) {
				return usage, emittedText, nil
			}
		}
		if event.Type == "response.completed" && event.Response.Usage != nil {
			u := event.Response.Usage
			usage = &trpcmodel.Usage{PromptTokens: int(u.InputTokens), CompletionTokens: int(u.OutputTokens), TotalTokens: int(u.TotalTokens)}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, emittedText, err
	}
	return usage, emittedText, nil
}

func parseResponsesEvent(line string) (responsesEvent, bool) {
	if !strings.HasPrefix(line, "data: ") {
		return responsesEvent{}, false
	}
	data := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
	if data == "" || data == "[DONE]" {
		return responsesEvent{}, false
	}
	var event responsesEvent
	if json.Unmarshal([]byte(data), &event) != nil {
		return responsesEvent{}, false
	}
	return event, true
}
func (m *responsesModel) emitError(out chan<- *trpcmodel.Response, err error) {
	out <- &trpcmodel.Response{Done: true, Error: &trpcmodel.ResponseError{Message: err.Error(), Type: trpcmodel.ErrorTypeAPIError}}
}
func sendResponse(ctx context.Context, out chan<- *trpcmodel.Response, response *trpcmodel.Response) bool {
	select {
	case out <- response:
		return true
	case <-ctx.Done():
		return false
	}
}

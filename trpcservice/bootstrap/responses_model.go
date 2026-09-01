package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
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
	Type       string               `json:"type"`
	Text       string               `json:"text,omitempty"`
	ImageURL   string               `json:"image_url,omitempty"`
	Detail     string               `json:"detail,omitempty"`
	FileID     string               `json:"file_id,omitempty"`
	FileURL    string               `json:"file_url,omitempty"`
	FileData   string               `json:"file_data,omitempty"`
	Filename   string               `json:"filename,omitempty"`
	InputAudio *responsesInputAudio `json:"input_audio,omitempty"`
}
type responsesInputAudio struct {
	Data   string `json:"data"`
	Format string `json:"format"`
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
		input = append(input, responsesInputItem{Role: role, Content: responsesContent(message)})
	}
	return input
}

func responsesContent(message trpcmodel.Message) []responsesContentPart {
	parts := make([]responsesContentPart, 0, 1+len(message.ContentParts))
	text := message.Content
	if text == "" {
		for _, part := range message.ContentParts {
			if part.Text != nil {
				text += *part.Text
			}
		}
	}
	if text != "" || len(message.ContentParts) == 0 {
		parts = append(parts, responsesTextPart(text))
	}
	for _, part := range message.ContentParts {
		if part.Type == trpcmodel.ContentTypeText || part.Text != nil {
			continue
		}
		if converted, ok := responsesContentFromPart(part); ok {
			parts = append(parts, converted)
		}
	}
	if len(parts) == 0 {
		parts = append(parts, responsesTextPart(""))
	}
	return parts
}

func responsesContentFromPart(part trpcmodel.ContentPart) (responsesContentPart, bool) {
	switch part.Type {
	case trpcmodel.ContentTypeImage:
		if part.Image == nil {
			return responsesTextPart("[image attachment omitted: image data is missing]"), true
		}
		if strings.TrimSpace(part.Image.URL) != "" {
			return responsesContentPart{Type: "input_image", ImageURL: strings.TrimSpace(part.Image.URL), Detail: part.Image.Detail}, true
		}
		if len(part.Image.Data) == 0 {
			return responsesTextPart("[image attachment omitted: image data is missing]"), true
		}
		return responsesContentPart{Type: "input_image", ImageURL: dataURL("image", part.Image.Format, part.Image.Data), Detail: part.Image.Detail}, true
	case trpcmodel.ContentTypeFile:
		return responsesFilePart(part.File)
	case trpcmodel.ContentTypeAudio:
		return responsesAudioPart(part.Audio)
	case trpcmodel.ContentTypeVideo:
		return responsesTextPart("[video attachment omitted: model video input is not enabled]"), true
	default:
		return responsesContentPart{}, false
	}
}

func responsesFilePart(file *trpcmodel.File) (responsesContentPart, bool) {
	if file == nil {
		return responsesTextPart("[file attachment omitted: file data is missing]"), true
	}
	if id := strings.TrimSpace(file.FileID); id != "" {
		return responsesContentPart{Type: "input_file", FileID: id}, true
	}
	if fileURL := strings.TrimSpace(file.URL); fileURL != "" {
		return responsesContentPart{Type: "input_file", FileURL: fileURL}, true
	}
	if len(file.Data) == 0 {
		return responsesTextPart("[file attachment omitted: file data is missing]"), true
	}
	return responsesContentPart{Type: "input_file", FileData: dataURL("application", file.MimeType, file.Data), Filename: responsesFilename(file.Name)}, true
}

func responsesAudioPart(audio *trpcmodel.Audio) (responsesContentPart, bool) {
	if audio == nil || len(audio.Data) == 0 {
		return responsesTextPart("[audio attachment omitted: audio data is missing]"), true
	}
	format := responsesAudioFormat(audio.Format)
	if format == "" {
		return responsesTextPart("[audio attachment omitted: unsupported audio format]"), true
	}
	return responsesContentPart{Type: "input_audio", InputAudio: &responsesInputAudio{Data: base64.StdEncoding.EncodeToString(audio.Data), Format: format}}, true
}

func responsesTextPart(text string) responsesContentPart {
	return responsesContentPart{Type: "input_text", Text: text}
}

func dataURL(defaultFamily, format string, data []byte) string {
	mediaType := strings.ToLower(strings.TrimSpace(format))
	if mediaType == "" {
		mediaType = defaultFamily + "/octet-stream"
	} else if !strings.Contains(mediaType, "/") {
		mediaType = defaultFamily + "/" + strings.TrimPrefix(mediaType, ".")
	}
	return "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(data)
}

func responsesFilename(name string) string {
	if trimmed := strings.TrimSpace(name); trimmed != "" {
		return trimmed
	}
	return "attachment"
}

func responsesAudioFormat(format string) string {
	value := strings.ToLower(strings.TrimSpace(format))
	if _, subtype, ok := strings.Cut(value, "/"); ok {
		value = subtype
	}
	switch value {
	case "mpeg", "mpga":
		return "mp3"
	case "x-wav", "wave":
		return "wav"
	case "mp3", "wav":
		return value
	default:
		return ""
	}
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
	Type     string                     `json:"type"`
	Delta    string                     `json:"delta"`
	Text     string                     `json:"text"`
	Response responsesCompletedResponse `json:"response"`
	Item     responsesOutputItem        `json:"item"`
	Part     responsesOutputContent     `json:"part"`
}
type responsesCompletedResponse struct {
	Status string          `json:"status"`
	Error  *responsesError `json:"error"`
	Usage  *struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
		TotalTokens  int64 `json:"total_tokens"`
	} `json:"usage"`
	IncompleteDetails *responsesIncompleteDetails `json:"incomplete_details"`
	Output            []responsesOutputItem       `json:"output"`
	OutputText        string                      `json:"output_text"`
}
type responsesOutputItem struct {
	Type    string                   `json:"type"`
	Status  string                   `json:"status"`
	Content []responsesOutputContent `json:"content"`
}
type responsesOutputContent struct {
	Text    string `json:"text"`
	Refusal string `json:"refusal"`
}
type responsesError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
type responsesIncompleteDetails struct {
	Reason string `json:"reason"`
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
		if !emittedText && event.Type == "response.output_text.done" && event.Text != "" {
			emittedText = true
			if !sendResponse(ctx, out, responsesTextDelta("response.output_text.done", event.Text)) {
				return usage, emittedText, nil
			}
		}
		if !emittedText && event.Type == "response.content_part.done" {
			if text := event.Part.outputText(); text != "" {
				emittedText = true
				if !sendResponse(ctx, out, responsesTextDelta("response.content_part.done", text)) {
					return usage, emittedText, nil
				}
			}
		}
		if !emittedText && event.Type == "response.output_item.done" {
			if text := event.Item.outputText(); text != "" {
				emittedText = true
				if !sendResponse(ctx, out, responsesTextDelta("response.output_item.done", text)) {
					return usage, emittedText, nil
				}
			}
		}
		if event.Type == "response.failed" {
			return usage, emittedText, fmt.Errorf("responses API failed: %s", event.Response.errorText())
		}
		if event.Type == "response.incomplete" {
			return usage, emittedText, fmt.Errorf("responses API incomplete: %s", event.Response.incompleteReason())
		}
		if event.Type == "response.completed" && event.Response.Usage != nil {
			u := event.Response.Usage
			usage = &trpcmodel.Usage{PromptTokens: int(u.InputTokens), CompletionTokens: int(u.OutputTokens), TotalTokens: int(u.TotalTokens)}
		}
		if !emittedText && event.Type == "response.completed" {
			if text := event.Response.outputText(); text != "" {
				emittedText = true
				if !sendResponse(ctx, out, responsesTextDelta("response.completed", text)) {
					return usage, emittedText, nil
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, emittedText, err
	}
	return usage, emittedText, nil
}

func (response responsesCompletedResponse) outputText() string {
	if response.OutputText != "" {
		return response.OutputText
	}
	var builder strings.Builder
	for _, item := range response.Output {
		builder.WriteString(item.outputText())
	}
	return builder.String()
}
func (item responsesOutputItem) outputText() string {
	var builder strings.Builder
	for _, content := range item.Content {
		builder.WriteString(content.outputText())
	}
	return builder.String()
}
func (content responsesOutputContent) outputText() string {
	if content.Text != "" {
		return content.Text
	}
	return content.Refusal
}
func (response responsesCompletedResponse) errorText() string {
	if response.Error == nil {
		return "unknown error"
	}
	if response.Error.Message != "" {
		return response.Error.Message
	}
	if response.Error.Code != "" {
		return response.Error.Code
	}
	if response.Error.Type != "" {
		return response.Error.Type
	}
	return "unknown error"
}
func (response responsesCompletedResponse) incompleteReason() string {
	if response.IncompleteDetails != nil && response.IncompleteDetails.Reason != "" {
		return response.IncompleteDetails.Reason
	}
	if response.Status != "" {
		return response.Status
	}
	return "unknown reason"
}

func responsesTextDelta(object, text string) *trpcmodel.Response {
	return &trpcmodel.Response{Object: object, IsPartial: true, Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Role: trpcmodel.RoleAssistant, Content: text}}}}
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

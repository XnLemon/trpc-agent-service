package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
)

type openAICompatRequest struct {
	Messages []openAICompatMessage `json:"messages"`
	Model    string                `json:"model,omitempty"`
	Stream   bool                  `json:"stream,omitempty"`
	User     string                `json:"user,omitempty"`
}

type openAICompatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type openAICompatResponse struct {
	ID      string               `json:"id"`
	Object  string               `json:"object"`
	Created int64                `json:"created"`
	Model   string               `json:"model"`
	Choices []openAICompatChoice `json:"choices"`
}

type openAICompatChoice struct {
	Index        int                     `json:"index"`
	Message      *openAICompatMessageOut `json:"message,omitempty"`
	Delta        *openAICompatDelta      `json:"delta,omitempty"`
	FinishReason *string                 `json:"finish_reason"`
}

type openAICompatMessageOut struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAICompatDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type openAICompatError struct {
	Error openAICompatErrorBody `json:"error"`
}

type openAICompatErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}

func (handler *HTTPHandler) openAICompletions(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		handler.writeOpenAIError(writer, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "method_not_allowed")
		return
	}
	requestID, traceID, err := requestCorrelation(request)
	if err != nil {
		handler.writeOpenAIError(writer, http.StatusBadRequest, "invalid request correlation", "invalid_request_error", "invalid_request")
		return
	}
	if !handler.Ready() {
		handler.writeOpenAIError(writer, http.StatusServiceUnavailable, "not ready", "server_error", "not_ready")
		return
	}
	ctx := request.Context()
	if handler.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, handler.requestTimeout)
		defer cancel()
	}
	authenticated, err := authenticateAPI(ctx, handler.authenticator, request)
	if err != nil {
		handler.writeOpenAIError(writer, http.StatusUnauthorized, "authentication failed", "authentication_error", "unauthorized")
		return
	}
	principal, err := newAPIPrincipal(authenticated)
	if err != nil {
		handler.writeOpenAIError(writer, http.StatusUnauthorized, "authentication failed", "authentication_error", "unauthorized")
		return
	}
	input, stream, err := handler.decodeOpenAIMessage(writer, request, principal)
	if err != nil {
		handler.writeOpenAIError(writer, http.StatusBadRequest, "invalid request", "invalid_request_error", "invalid_request")
		return
	}
	if idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key")); idempotencyKey != "" {
		input.ExternalMessageID = idempotencyKey
	} else if input.ExternalMessageID == "" {
		input.ExternalMessageID = requestID
	}
	limitLease, err := handler.limiter.Acquire(ctx, principal.TenantID())
	if err != nil {
		handler.writeOpenAIError(writer, http.StatusTooManyRequests, "rate limit exceeded", "rate_limit_error", "rate_limited")
		return
	}
	defer func() { _ = limitLease.Release() }()
	claim, replay, err := handler.idempotency.Begin(ctx, principal, input)
	if err != nil {
		status, message := mapHTTPError(err)
		handler.writeOpenAIError(writer, status, message, "invalid_request_error", "idempotency_error")
		return
	}
	if claim == nil {
		if stream {
			if !supportsFlush(writer) {
				handler.writeOpenAIError(writer, http.StatusInternalServerError, "streaming unavailable", "server_error", "streaming_unavailable")
				return
			}
			handler.writeOpenAIReplayStream(writer, replay, requestID, traceID)
			return
		}
		handler.writeOpenAICompletion(writer, requestID, traceID, replay)
		return
	}
	completed := false
	defer func() {
		if !completed {
			_ = claim.Fail()
		}
	}()
	if stream && !supportsFlush(writer) {
		handler.writeOpenAIError(writer, http.StatusInternalServerError, "streaming unavailable", "server_error", "streaming_unavailable")
		return
	}
	events, err := callDispatch(ctx, handler.dispatcher, DispatchRequest{Principal: principal, Message: input, RequestID: requestID, TraceID: traceID})
	if err != nil {
		status, message := mapHTTPError(err)
		handler.writeOpenAIError(writer, status, message, "server_error", "execution_failed")
		return
	}
	if events == nil {
		handler.writeOpenAIError(writer, http.StatusBadGateway, "execution failed", "server_error", "execution_failed")
		return
	}
	if stream {
		if !supportsFlush(writer) {
			handler.writeOpenAIError(writer, http.StatusInternalServerError, "streaming unavailable", "server_error", "streaming_unavailable")
			return
		}
		completed = handler.writeOpenAIStream(writer, ctx, claim, requestID, traceID, events)
		return
	}
	collected, err := collectHTTPEvents(ctx, events)
	if err != nil {
		status, message := mapHTTPError(err)
		handler.writeOpenAIError(writer, status, message, "server_error", "execution_failed")
		return
	}
	if err := claim.Complete(collected); err != nil {
		handler.writeOpenAIError(writer, http.StatusInternalServerError, "gateway error", "server_error", "gateway_error")
		return
	}
	completed = true
	handler.writeOpenAICompletion(writer, requestID, traceID, collected)
}

// decodeOpenAIMessage deliberately accepts only a single user text payload.
// The authenticated Agent App still owns system instructions, model choice,
// tools, and the session boundary; OpenAI request fields cannot override them.
func (handler *HTTPHandler) decodeOpenAIMessage(writer http.ResponseWriter, request *http.Request, principal Principal) (InboundMessage, bool, error) {
	if request == nil || request.Body == nil || !isJSONContentType(request.Header.Get("Content-Type")) {
		return InboundMessage{}, false, ErrInvalid
	}
	body := http.MaxBytesReader(writer, request.Body, handler.maxBodyBytes)
	payload, err := io.ReadAll(body)
	closeErr := safeCloseProtocolBody(body)
	if err != nil || closeErr != nil || jsonstrict.Validate(payload, true) != nil {
		return InboundMessage{}, false, ErrInvalid
	}
	request.Body = io.NopCloser(bytes.NewReader(payload))
	request.ContentLength = int64(len(payload))
	var input openAICompatRequest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return InboundMessage{}, false, ErrInvalid
	}
	if len(input.Messages) != 1 || strings.TrimSpace(input.Messages[0].Role) != "user" {
		return InboundMessage{}, false, fmt.Errorf("%w: exactly one user message is required", ErrInvalid)
	}
	var content string
	if err := json.Unmarshal(input.Messages[0].Content, &content); err != nil || strings.TrimSpace(content) == "" {
		return InboundMessage{}, false, ErrInvalid
	}
	userID := strings.TrimSpace(input.User)
	if userID == "" {
		userID = principal.SubjectID()
	}
	if userID == "" {
		userID = "openai-user"
	}
	conversationID := strings.TrimSpace(request.Header.Get("X-Session-ID"))
	if conversationID == "" {
		conversationID = userID
	}
	message := InboundMessage{
		Content: content, ContentType: ContentTypeText,
		ExternalUserID: userID, ConversationKind: channels.ConversationDirect,
		ExternalPeerID: conversationID, ExternalMessageID: strings.TrimSpace(request.Header.Get("Idempotency-Key")),
	}
	normalized, err := message.Normalize()
	return normalized, input.Stream, err
}

func (handler *HTTPHandler) writeOpenAICompletion(writer http.ResponseWriter, requestID, traceID string, events []DispatchEvent) {
	response := finalChatResponse(requestID, traceID, events)
	content := response.Text
	finishReason := "stop"
	if response.Error != "" {
		finishReason = "error"
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(openAICompatResponse{
		ID: "chatcmpl-" + requestID, Object: "chat.completion", Created: time.Now().Unix(),
		Model: "trpc-agent", Choices: []openAICompatChoice{{Index: 0, Message: &openAICompatMessageOut{Role: "assistant", Content: content}, FinishReason: &finishReason}},
	})
}

func (handler *HTTPHandler) writeOpenAIReplayStream(writer http.ResponseWriter, events []DispatchEvent, requestID, traceID string) {
	if !supportsFlush(writer) {
		handler.writeOpenAIError(writer, http.StatusInternalServerError, "streaming unavailable", "server_error", "streaming_unavailable")
		return
	}
	ctx := context.Background()
	handler.writeOpenAIStream(writer, ctx, nil, requestID, traceID, sliceDispatchEvents(events))
}

func sliceDispatchEvents(events []DispatchEvent) <-chan DispatchEvent {
	channel := make(chan DispatchEvent, len(events))
	for _, event := range events {
		channel <- event
	}
	close(channel)
	return channel
}

func (handler *HTTPHandler) writeOpenAIStream(writer http.ResponseWriter, ctx context.Context, claim *IdempotencyClaim, requestID, traceID string, events <-chan DispatchEvent) bool {
	flusher, ok := unwrapFlusher(writer)
	if !ok {
		return false
	}
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()
	collected := make([]DispatchEvent, 0, 4)
	first := true
	finishReason := "stop"
	for {
		select {
		case event, open := <-events:
			if !open {
				if claim != nil {
					if len(collected) == 0 || claim.Complete(collected) != nil {
						return false
					}
				}
				_ = writeOpenAIChunk(writer, requestID, traceID, "", finishReason, first)
				_, _ = io.WriteString(writer, "data: [DONE]\n\n")
				flusher.Flush()
				return true
			}
			collected = append(collected, event)
			if event.Type == DispatchEventMessage && event.Text != "" {
				if err := writeOpenAIChunk(writer, requestID, traceID, event.Text, "", first); err != nil {
					return false
				}
				first = false
			}
			if event.Type == DispatchEventError {
				finishReason = "error"
				if err := writeOpenAIChunk(writer, requestID, traceID, event.Error, "error", first); err != nil {
					return false
				}
				first = false
			}
			if event.Done {
				if claim != nil {
					if len(collected) == 0 || claim.Complete(collected) != nil {
						return false
					}
				}
				if err := writeOpenAIChunk(writer, requestID, traceID, "", finishReason, first); err != nil {
					return false
				}
				_, _ = io.WriteString(writer, "data: [DONE]\n\n")
				flusher.Flush()
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}

func unwrapFlusher(writer http.ResponseWriter) (http.Flusher, bool) {
	if wrapped, ok := writer.(*httpStatusWriter); ok {
		flusher, ok := wrapped.ResponseWriter.(http.Flusher)
		return flusher, ok
	}
	flusher, ok := writer.(http.Flusher)
	return flusher, ok
}

func writeOpenAIChunk(writer io.Writer, requestID, _ string, content, finishReason string, first bool) error {
	delta := &openAICompatDelta{Content: content}
	if first {
		delta.Role = "assistant"
	}
	var finish *string
	if finishReason != "" {
		finish = &finishReason
	}
	payload, err := json.Marshal(openAICompatResponse{
		ID: "chatcmpl-" + requestID, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: "trpc-agent",
		Choices: []openAICompatChoice{{Index: 0, Delta: delta, FinishReason: finish}},
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "data: %s\n\n", payload)
	return err
}

func (handler *HTTPHandler) writeOpenAIError(writer http.ResponseWriter, status int, message, errorType, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(openAICompatError{Error: openAICompatErrorBody{Message: message, Type: errorType, Code: code}})
}

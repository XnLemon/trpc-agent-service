package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	trpcagentserver "trpc.group/trpc-go/trpc-agent-go/server/trpcagent"
)

// TRPCAgentConfig enables the upstream native tRPC-Agent HTTP adapter. The
// request is authenticated before the upstream handler runs, and the runner
// below ignores caller-supplied app/profile/runtime fields in favor of the
// platform Principal and Dispatcher.
type TRPCAgentConfig struct {
	Enabled        bool
	BasePath       string
	AppName        string
	RequestTimeout time.Duration
}

type trpcAgentPrincipalContextKey struct{}
type trpcAgentCorrelationContextKey struct{}

type trpcAgentCorrelation struct {
	requestID string
	traceID   string
}

func platformTrpcAgentMiddleware(authenticator APIAuthenticator, ready func() bool, maxBodyBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if isNilGatewayValue(authenticator) || ready == nil || maxBodyBytes < 1 || isNilGatewayValue(next) || request == nil || isNilGatewayValue(writer) || !callReady(ready) {
			writeProtocolJSONError(writer, http.StatusServiceUnavailable, "not ready")
			return
		}
		requestID, traceID, err := requestCorrelation(request)
		if err != nil {
			writeProtocolJSONError(writer, http.StatusBadRequest, "invalid request correlation")
			return
		}
		authenticated, err := authenticateAPI(request.Context(), authenticator, request)
		if err != nil {
			writeProtocolJSONError(writer, http.StatusUnauthorized, "authentication failed")
			return
		}
		principal, err := newAPIPrincipal(authenticated)
		if err != nil {
			writeProtocolJSONError(writer, http.StatusUnauthorized, "authentication failed")
			return
		}
		if request.Method == http.MethodPost {
			if err := validateAndRestoreProtocolBody(writer, request, maxBodyBytes); err != nil {
				writeProtocolJSONError(writer, http.StatusBadRequest, "invalid request")
				return
			}
		}
		ctx := context.WithValue(request.Context(), trpcAgentPrincipalContextKey{}, principal)
		ctx = context.WithValue(ctx, trpcAgentCorrelationContextKey{}, trpcAgentCorrelation{requestID: requestID, traceID: traceID})
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

type trpcAgentPlatformRunner struct {
	dispatcher     DispatchService
	ready          func() bool
	requestTimeout time.Duration
}

func (runnerAdapter *trpcAgentPlatformRunner) Run(ctx context.Context, _ string, sessionID string, message model.Message, runOpts ...agent.RunOption) (<-chan *event.Event, error) {
	if runnerAdapter == nil || isNilGatewayValue(runnerAdapter.dispatcher) || runnerAdapter.ready == nil || nilvalue.Is(ctx) || !callReady(runnerAdapter.ready) {
		return nil, ErrNotReady
	}
	if message.Role != model.RoleUser || strings.TrimSpace(message.Content) == "" || len(message.ContentParts) != 0 || len(message.ToolCalls) != 0 {
		return nil, ErrInvalid
	}
	principal, ok := ctx.Value(trpcAgentPrincipalContextKey{}).(Principal)
	if !ok || principal.Validate() != nil {
		return nil, ErrUnauthenticated
	}
	correlation, _ := ctx.Value(trpcAgentCorrelationContextKey{}).(trpcAgentCorrelation)
	// The upstream server validates returned event IDs against its wire-level
	// RunOptions.RequestID. Keep that protocol correlation separate from the
	// platform request ID used by Dispatcher, audit, idempotency, and budgets;
	// caller-supplied runtime options still cannot alter the platform scope.
	protocolRequestID := correlation.requestID
	options := agent.RunOptions{}
	for _, option := range runOpts {
		if option != nil {
			option(&options)
		}
	}
	if strings.TrimSpace(options.RequestID) != "" {
		protocolRequestID = strings.TrimSpace(options.RequestID)
	}
	if !validRunnerCorrelationID(protocolRequestID) || !validRunnerCorrelationID(correlation.requestID) {
		return nil, ErrInvalid
	}
	platformRequestID := correlation.requestID
	traceID := correlation.traceID
	input := InboundMessage{
		Content: message.Content, ContentType: ContentTypeText,
		ExternalMessageID: platformRequestID, ExternalUserID: principal.SubjectID(),
		ConversationKind: ConversationDirect, ExternalPeerID: sessionID,
	}
	input, err := input.Normalize()
	if err != nil {
		return nil, ErrInvalid
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if runnerAdapter.requestTimeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, runnerAdapter.requestTimeout)
	}
	dispatchEvents, err := callDispatch(runCtx, runnerAdapter.dispatcher, DispatchRequest{
		Principal: principal, Message: input, RequestID: platformRequestID, TraceID: traceID,
	})
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, redactProtocolError(err)
	}
	if dispatchEvents == nil {
		if cancel != nil {
			cancel()
		}
		return nil, ErrNotReady
	}
	output := make(chan *event.Event)
	go func() {
		defer close(output)
		if cancel != nil {
			defer cancel()
		}
		completed := false
		for {
			select {
			case dispatchEvent, open := <-dispatchEvents:
				if !open {
					if !completed {
						select {
						case output <- trpcAgentCompletionEvent(protocolRequestID):
						case <-runCtx.Done():
						}
					}
					return
				}
				// Preserve the platform event contents but adapt only the
				// protocol correlation expected by the upstream server.
				dispatchEvent.RequestID = protocolRequestID
				converted, terminal := convertTrpcAgentDispatchEvent(dispatchEvent, protocolRequestID)
				if converted == nil {
					if terminal {
						completed = true
					}
					continue
				}
				select {
				case output <- converted:
				case <-runCtx.Done():
					return
				}
				if terminal {
					completed = true
				}
			case <-runCtx.Done():
				return
			}
		}
	}()
	return output, nil
}

func (runnerAdapter *trpcAgentPlatformRunner) Close() error { return nil }

func convertTrpcAgentDispatchEvent(dispatchEvent DispatchEvent, requestID string) (*event.Event, bool) {
	requestID = firstNonEmpty(dispatchEvent.RequestID, requestID)
	switch dispatchEvent.Type {
	case DispatchEventMessage:
		message := model.NewAssistantMessage(dispatchEvent.Text)
		return &event.Event{
			Response:  &model.Response{ID: uuid.NewString(), Object: model.ObjectTypeChatCompletionChunk, Created: time.Now().Unix(), Model: "trpc-agent", Choices: []model.Choice{{Index: 0, Message: message, Delta: message}}},
			RequestID: requestID, InvocationID: uuid.NewString(), Author: "assistant", ID: uuid.NewString(), Timestamp: time.Now().UTC(), Version: event.CurrentVersion,
		}, false
	case DispatchEventError:
		message := dispatchEvent.Error
		if message == "" {
			message = "execution failed"
		}
		return &event.Event{
			Response:  &model.Response{ID: uuid.NewString(), Object: model.ObjectTypeError, Created: time.Now().Unix(), Model: "trpc-agent", Error: &model.ResponseError{Message: message, Type: model.ErrorTypeRunError}},
			RequestID: requestID, InvocationID: uuid.NewString(), Author: "assistant", ID: uuid.NewString(), Timestamp: time.Now().UTC(), Version: event.CurrentVersion,
		}, false
	case DispatchEventDone:
		return trpcAgentCompletionEvent(requestID), true
	default:
		return nil, false
	}
}

func trpcAgentCompletionEvent(requestID string) *event.Event {
	return &event.Event{
		Response:  &model.Response{ID: "trpc-agent-runner-completion-" + uuid.NewString(), Object: model.ObjectTypeRunnerCompletion, Created: time.Now().Unix(), Model: "trpc-agent", Done: true},
		RequestID: requestID, InvocationID: uuid.NewString(), Author: "assistant", ID: uuid.NewString(), Timestamp: time.Now().UTC(), Version: event.CurrentVersion,
	}
}

func newTRPCAgentHandler(config TRPCAgentConfig, dispatcher DispatchService, authenticator APIAuthenticator, ready func() bool, maxBodyBytes int64) (http.Handler, io.Closer, string, error) {
	if !config.Enabled {
		return nil, nil, "", nil
	}
	if isNilGatewayValue(dispatcher) || isNilGatewayValue(authenticator) || ready == nil || maxBodyBytes < 1 {
		return nil, nil, "", ErrInvalid
	}
	if !utf8.ValidString(config.BasePath) || !utf8.ValidString(config.AppName) {
		return nil, nil, "", ErrInvalid
	}
	basePath := strings.TrimSpace(config.BasePath)
	if basePath == "" {
		basePath = "/trpc-agent/v1/apps"
	}
	if !strings.HasPrefix(basePath, "/") || strings.HasSuffix(basePath, "/") || strings.Contains(basePath, "//") || strings.Contains(basePath, "..") || strings.IndexFunc(basePath, unicode.IsControl) >= 0 {
		return nil, nil, "", ErrInvalid
	}
	appName := strings.TrimSpace(config.AppName)
	if appName == "" || strings.ContainsAny(appName, "/\\") || strings.Contains(appName, "..") || !validRunnerCorrelationID(appName) {
		return nil, nil, "", ErrInvalid
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultHTTPRequestTimeout
	}
	if requestTimeout < 0 {
		return nil, nil, "", ErrInvalid
	}
	adapter := &trpcAgentPlatformRunner{dispatcher: dispatcher, ready: ready, requestTimeout: requestTimeout}
	server, err := trpcagentserver.New(
		trpcagentserver.WithBasePath(basePath), trpcagentserver.WithAppName(appName),
		trpcagentserver.WithTimeout(requestTimeout), trpcagentserver.WithRunner(adapter),
	)
	if err != nil {
		return nil, nil, "", errors.New("failed to initialize tRPC-Agent adapter")
	}
	return platformTrpcAgentMiddleware(authenticator, ready, maxBodyBytes, server.Handler()), adapter, basePath + "/" + appName + "/", nil
}

func validRunnerCorrelationID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || !utf8.ValidString(value) || len([]rune(value)) > maxPrincipalIDRunes {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func writeProtocolJSONError(writer http.ResponseWriter, status int, message string) {
	if isNilGatewayValue(writer) {
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": message})
}

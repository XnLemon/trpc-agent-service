package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	"github.com/google/uuid"
	auth "trpc.group/trpc-go/trpc-a2a-go/auth"
	a2aserver "trpc.group/trpc-go/trpc-a2a-go/server"
	"trpc.group/trpc-go/trpc-a2a-go/taskmanager"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	trpcagenta2a "trpc.group/trpc-go/trpc-agent-go/server/a2a"
)

// A2AConfig enables the upstream A2A protocol adapter. The platform keeps
// authentication, tenant/app selection, budgets, and execution in Dispatcher;
// the upstream server is used only for A2A JSON-RPC/task translation.
type A2AConfig struct {
	Enabled        bool
	Host           string
	Path           string
	AgentName      string
	Description    string
	AgentCard      *a2aserver.AgentCard
	RequestTimeout time.Duration
}

type a2aPlatformAuth struct {
	authenticator APIAuthenticator
}

func (provider a2aPlatformAuth) Authenticate(request *http.Request) (*auth.User, error) {
	if request == nil || isNilGatewayValue(provider.authenticator) {
		return nil, ErrUnauthenticated
	}
	requestID, traceID, err := requestCorrelation(request)
	if err != nil {
		return nil, ErrInvalid
	}
	identity, err := authenticateAPI(request.Context(), provider.authenticator, request)
	if err != nil {
		return nil, ErrUnauthenticated
	}
	principal, err := newAPIPrincipal(identity)
	if err != nil {
		return nil, ErrUnauthenticated
	}
	return &auth.User{
		ID: principal.SubjectID(),
		Claims: map[string]any{
			"trpc_tenant_id":  principal.TenantID(),
			"trpc_app_id":     principal.AppID(),
			"trpc_subject_id": principal.SubjectID(),
			"trpc_request_id": requestID,
			"trpc_trace_id":   traceID,
		},
	}, nil
}

type a2aPlatformRunner struct {
	dispatcher     DispatchService
	ready          func() bool
	requestTimeout time.Duration
}

func (runnerAdapter *a2aPlatformRunner) Run(ctx context.Context, userID, sessionID string, message model.Message, _ ...agent.RunOption) (<-chan *event.Event, error) {
	if runnerAdapter == nil || isNilGatewayValue(runnerAdapter.dispatcher) || runnerAdapter.ready == nil || nilvalue.Is(ctx) || !callReady(runnerAdapter.ready) {
		return nil, ErrNotReady
	}
	if strings.TrimSpace(message.Content) == "" || len(message.ContentParts) != 0 || len(message.ToolCalls) != 0 {
		return nil, ErrInvalid
	}
	principal, err := a2aPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	requestID, _ := a2aClaimString(ctx, "trpc_request_id")
	if requestID == "" {
		requestID = uuid.NewString()
	}
	traceID, _ := a2aClaimString(ctx, "trpc_trace_id")
	input := InboundMessage{
		Content: message.Content, ContentType: ContentTypeText,
		ExternalMessageID: requestID, ExternalUserID: userID,
		ConversationKind: "direct", ExternalPeerID: sessionID,
	}
	input, err = input.Normalize()
	if err != nil {
		return nil, ErrInvalid
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if runnerAdapter.requestTimeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, runnerAdapter.requestTimeout)
	}
	dispatchEvents, err := callDispatch(runCtx, runnerAdapter.dispatcher, DispatchRequest{
		Principal: principal, Message: input, RequestID: requestID, TraceID: traceID,
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
		for {
			select {
			case dispatchEvent, open := <-dispatchEvents:
				if !open {
					return
				}
				converted := convertA2ADispatchEvent(dispatchEvent, requestID, traceID)
				if converted == nil {
					continue
				}
				select {
				case output <- converted:
				case <-runCtx.Done():
					return
				}
			case <-runCtx.Done():
				return
			}
		}
	}()
	return output, nil
}

func (runnerAdapter *a2aPlatformRunner) Close() error { return nil }

func convertA2ADispatchEvent(dispatchEvent DispatchEvent, requestID, traceID string) *event.Event {
	requestID = firstNonEmpty(dispatchEvent.RequestID, requestID)
	traceID = firstNonEmpty(dispatchEvent.TraceID, traceID)
	response := &model.Response{ID: uuid.NewString(), Object: model.ObjectTypeChatCompletionChunk, Created: time.Now().Unix(), Model: "trpc-agent"}
	switch dispatchEvent.Type {
	case DispatchEventMessage:
		message := model.NewAssistantMessage(dispatchEvent.Text)
		response.Choices = []model.Choice{{Index: 0, Message: message, Delta: message}}
	case DispatchEventError:
		errorMessage := dispatchEvent.Error
		if errorMessage == "" {
			errorMessage = "execution failed"
		}
		response.Object = model.ObjectTypeError
		response.Error = &model.ResponseError{Message: errorMessage, Type: model.ErrorTypeRunError}
		response.Done = true
	default:
		return nil
	}
	return &event.Event{
		Response: response, RequestID: requestID, InvocationID: uuid.NewString(),
		Author: "assistant", ID: uuid.NewString(), Timestamp: time.Now().UTC(),
		Version: event.CurrentVersion,
	}
}

func a2aPrincipalFromContext(ctx context.Context) (Principal, error) {
	if nilvalue.Is(ctx) {
		return Principal{}, ErrUnauthenticated
	}
	user, ok := ctx.Value(auth.AuthUserKey).(*auth.User)
	if !ok || user == nil {
		return Principal{}, ErrUnauthenticated
	}
	tenantID, tenantOK := a2aClaimString(ctx, "trpc_tenant_id")
	appID, appOK := a2aClaimString(ctx, "trpc_app_id")
	subjectID, subjectOK := a2aClaimString(ctx, "trpc_subject_id")
	if !subjectOK || subjectID == "" {
		subjectID = strings.TrimSpace(user.ID)
	}
	if !tenantOK || !appOK || tenantID == "" || appID == "" || subjectID == "" {
		return Principal{}, ErrUnauthenticated
	}
	authenticated, err := newAuthenticatedAPI(APIIdentity{TenantID: tenantID, AppID: appID, SubjectID: subjectID})
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	return newAPIPrincipal(authenticated)
}

func a2aClaimString(ctx context.Context, key string) (string, bool) {
	if nilvalue.Is(ctx) {
		return "", false
	}
	user, ok := ctx.Value(auth.AuthUserKey).(*auth.User)
	if !ok || user == nil || user.Claims == nil {
		return "", false
	}
	value, ok := user.Claims[key]
	text, textOK := value.(string)
	return strings.TrimSpace(text), ok && textOK
}

func newA2AHandler(config A2AConfig, dispatcher DispatchService, authenticator APIAuthenticator, ready func() bool, maxBodyBytes int64) (http.Handler, io.Closer, error) {
	if !config.Enabled {
		return nil, nil, nil
	}
	if isNilGatewayValue(dispatcher) || isNilGatewayValue(authenticator) || ready == nil || maxBodyBytes < 1 {
		return nil, nil, ErrInvalid
	}
	if !utf8.ValidString(config.Path) || !utf8.ValidString(config.Host) || !utf8.ValidString(config.AgentName) || !utf8.ValidString(config.Description) {
		return nil, nil, ErrInvalid
	}
	path := strings.TrimSpace(config.Path)
	if path == "" {
		path = "/a2a"
	}
	if !strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") || strings.Contains(path, "//") || strings.Contains(path, "..") || strings.IndexFunc(path, unicode.IsControl) >= 0 {
		return nil, nil, ErrInvalid
	}
	host := strings.TrimSpace(config.Host)
	if strings.IndexFunc(host, unicode.IsControl) >= 0 || strings.IndexFunc(config.Description, unicode.IsControl) >= 0 {
		return nil, nil, ErrInvalid
	}
	if host == "" && config.AgentCard == nil {
		return nil, nil, ErrInvalid
	}
	name := strings.TrimSpace(config.AgentName)
	if config.AgentCard == nil && (name == "" || strings.IndexFunc(name, unicode.IsControl) >= 0) {
		return nil, nil, ErrInvalid
	}
	var card a2aserver.AgentCard
	var err error
	if config.AgentCard != nil {
		card = *config.AgentCard
		if !utf8.ValidString(card.Name) || !utf8.ValidString(card.URL) || card.Name == "" || card.URL == "" || hasControl(card.Name) || hasControl(card.URL) {
			return nil, nil, ErrInvalid
		}
	} else {
		card, err = trpcagenta2a.NewAgentCard(name, config.Description, strings.TrimRight(host, "/")+path, true)
		if err != nil {
			return nil, nil, ErrInvalid
		}
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultHTTPRequestTimeout
	}
	if requestTimeout < 0 {
		return nil, nil, ErrInvalid
	}
	adapter := &a2aPlatformRunner{dispatcher: dispatcher, ready: ready, requestTimeout: requestTimeout}
	var taskManagerCloser io.Closer
	taskManagerBuilder := func(processor taskmanager.MessageProcessor) taskmanager.TaskManager {
		manager, err := taskmanager.NewMemoryTaskManager(processor)
		if err != nil {
			return nil
		}
		taskManagerCloser = manager
		return manager
	}
	server, err := trpcagenta2a.New(
		trpcagenta2a.WithRunner(adapter), trpcagenta2a.WithAgentCard(card),
		trpcagenta2a.WithTaskManagerBuilder(taskManagerBuilder),
		trpcagenta2a.WithExtraA2AOptions(
			a2aserver.WithBasePath(path),
			a2aserver.WithCORSEnabled(false),
			a2aserver.WithAuthProvider(a2aPlatformAuth{authenticator: authenticator}),
		),
	)
	if err != nil {
		if !isNilGatewayValue(taskManagerCloser) {
			_ = safeCloseHTTPComponent(taskManagerCloser)
		}
		return nil, nil, errors.New("failed to initialize A2A adapter")
	}
	return limitProtocolBody(server.Handler(), maxBodyBytes), taskManagerCloser, nil
}

func validateAndRestoreProtocolBody(writer http.ResponseWriter, request *http.Request, maxBodyBytes int64) error {
	if request == nil || request.Body == nil || maxBodyBytes < 1 {
		return ErrInvalid
	}
	body := http.MaxBytesReader(writer, request.Body, maxBodyBytes)
	payload, err := io.ReadAll(body)
	closeErr := safeCloseProtocolBody(body)
	if err != nil || closeErr != nil || jsonstrict.Validate(payload, true) != nil {
		return ErrInvalid
	}
	request.Body = io.NopCloser(bytes.NewReader(payload))
	request.ContentLength = int64(len(payload))
	return nil
}

func limitProtocolBody(next http.Handler, maxBodyBytes int64) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if isNilGatewayValue(next) || request == nil || isNilGatewayValue(writer) {
			return
		}
		if request.Method == http.MethodPost {
			if err := validateAndRestoreProtocolBody(writer, request, maxBodyBytes); err != nil {
				writeProtocolJSONError(writer, http.StatusBadRequest, "invalid request")
				return
			}
		}
		next.ServeHTTP(writer, request)
	})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

var _ runner.Runner = (*a2aPlatformRunner)(nil)

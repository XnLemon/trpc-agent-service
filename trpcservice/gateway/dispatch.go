package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

var (
	// ErrExecution is the stable, redacted execution failure exposed by
	// Dispatch. Provider messages and stack traces never cross this boundary.
	ErrExecution = errors.New("execution failed")
	// ErrExecutionCanceled is the stable cancellation result for a Dispatch
	// stream after its Runner events have been drained.
	ErrExecutionCanceled = errors.New("execution canceled")
)

const defaultDispatchDrainTimeout = 250 * time.Millisecond

// DispatchEventType identifies the protocol-neutral event surface consumed by
// JSON and SSE adapters.
type DispatchEventType string

const (
	DispatchEventMessage DispatchEventType = "message"
	DispatchEventStatus  DispatchEventType = "status"
	DispatchEventError   DispatchEventType = "error"
	DispatchEventDone    DispatchEventType = "done"
)

// DispatchRequest is the trusted input to the protocol-neutral execution
// boundary. Principal fields are never reconstructed from Message.
type DispatchRequest struct {
	Principal Principal
	Message   InboundMessage
	RequestID string
	TraceID   string
}

// DispatchEvent is a redacted event safe for a protocol adapter. It contains
// no Plan, repository object, Secret, provider response, or raw error.
type DispatchEvent struct {
	Type      DispatchEventType `json:"type"`
	RequestID string            `json:"request_id"`
	TraceID   string            `json:"trace_id,omitempty"`
	Text      string            `json:"text,omitempty"`
	Status    string            `json:"status,omitempty"`
	Error     string            `json:"error,omitempty"`
	Done      bool              `json:"done"`
}

// DispatchService is the protocol-neutral contract implemented by Dispatcher.
type DispatchService interface {
	Dispatch(context.Context, DispatchRequest) (<-chan DispatchEvent, error)
}

// DispatchConfig configures the Resolver/Registry execution boundary.
type DispatchConfig struct {
	Resolver      *PlanResolver
	Registry      *RunnerRegistry
	DrainTimeout  time.Duration
	Observability observability.Provider
}

// Dispatcher resolves a fixed plan, acquires a Runner lease, and translates
// Runner events into a bounded, redacted event stream.
type Dispatcher struct {
	resolver     *PlanResolver
	registry     *RunnerRegistry
	drainTimeout time.Duration
	telemetry    observability.Provider
	metrics      metrics.Catalog
}

// NewDispatcher validates the protocol-neutral execution dependencies.
func NewDispatcher(config DispatchConfig) (*Dispatcher, error) {
	if config.Resolver == nil || config.Registry == nil {
		return nil, fmt.Errorf("%w: dispatcher dependencies are required", ErrInvalid)
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = defaultDispatchDrainTimeout
	}
	if config.DrainTimeout < 0 {
		return nil, fmt.Errorf("%w: dispatch drain timeout cannot be negative", ErrInvalid)
	}
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	return &Dispatcher{resolver: config.Resolver, registry: config.Registry, drainTimeout: config.DrainTimeout, telemetry: config.Observability, metrics: metrics.New(config.Observability)}, nil
}

// Ready reports whether both plan resolution and Runner acquisition are ready.
func (dispatcher *Dispatcher) Ready() bool {
	return dispatcher != nil && dispatcher.resolver != nil && dispatcher.resolver.Ready() && dispatcher.registry != nil && dispatcher.registry.Ready()
}

// Dispatch starts one execution and returns a redacted event stream. The
// returned stream owns the Runner lease until it reaches terminal state or the
// caller Context is canceled.
func (dispatcher *Dispatcher) Dispatch(ctx context.Context, request DispatchRequest) (<-chan DispatchEvent, error) {
	if dispatcher == nil || dispatcher.resolver == nil || dispatcher.registry == nil {
		return nil, ErrNotReady
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := request.Principal.Validate(); err != nil {
		return nil, ErrUnauthenticated
	}
	message, err := request.Message.Normalize()
	if err != nil {
		return nil, err
	}
	requestID, err := normalizeCorrelationID(request.RequestID, true)
	if err != nil {
		return nil, err
	}
	traceID, err := normalizeCorrelationID(request.TraceID, false)
	if err != nil {
		return nil, err
	}
	ctx, span := dispatcher.telemetry.Tracer("trpcservice.gateway").Start(observability.WithCorrelation(ctx, requestID, traceID), observability.OperationGatewayDispatch,
		observability.Attribute{Key: "component", Value: "gateway"}, observability.Attribute{Key: "operation", Value: observability.OperationGatewayDispatch})
	started := time.Now()
	_ = dispatcher.metrics.Request(ctx, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch, "status": "started"})
	finishWithError := func(cause error) {
		span.SetAttributes(observability.Attribute{Key: "error_class", Value: observability.ErrorClass(cause)})
		span.SetStatus(observability.StatusError, observability.ErrorClass(cause))
		span.RecordError(cause)
		span.End()
		_ = dispatcher.metrics.Duration(ctx, observability.DurationMilliseconds(started), map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch, "status": "error", "error_class": observability.ErrorClass(cause)})
	}

	plan, err := dispatcher.resolver.Resolve(ctx, request.Principal)
	if err != nil {
		finishWithError(err)
		return nil, err
	}
	identity, err := dispatchRunnerIdentity(request.Principal, message)
	if err != nil {
		finishWithError(err)
		return nil, err
	}
	lease, err := dispatcher.registry.Acquire(ctx, plan)
	if err != nil {
		finishWithError(err)
		return nil, err
	}
	runnerValue := lease.Runner()
	if runnerValue == nil {
		_ = lease.Release()
		finishWithError(ErrRunnerUnavailable)
		return nil, ErrRunnerUnavailable
	}
	runnerEvents, err := runnerValue.Run(ctx, identity.UserID, identity.SessionID, trpcmodel.NewUserMessage(message.Content), trpcagent.WithRequestID(requestID))
	if err != nil {
		_ = lease.Release()
		if IsContextCancellation(err) {
			finishWithError(err)
			return nil, err
		}
		finishWithError(err)
		return nil, ErrExecution
	}
	if runnerEvents == nil {
		_ = lease.Release()
		finishWithError(ErrExecution)
		return nil, ErrExecution
	}

	output := make(chan DispatchEvent, 32)
	_ = dispatcher.metrics.Active(ctx, 1, map[string]string{"component": "runner"})
	go dispatcher.forward(ctx, requestID, traceID, runnerEvents, lease, output, span, started)
	return output, nil
}

func (dispatcher *Dispatcher) forward(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, lease *RunnerLease, output chan<- DispatchEvent, span observability.Span, started time.Time) {
	defer close(output)
	defer func() { _ = lease.Release() }()
	defer func() {
		_ = dispatcher.metrics.Active(ctx, -1, map[string]string{"component": "runner"})
		_ = dispatcher.metrics.Duration(ctx, observability.DurationMilliseconds(started), map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch, "status": "complete"})
		span.SetStatus(observability.StatusOK, "")
		span.End()
	}()
	for {
		if ctx.Err() != nil {
			dispatcher.finishCanceled(ctx, requestID, traceID, runnerEvents, output)
			return
		}
		select {
		case event, ok := <-runnerEvents:
			if !ok {
				trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: "complete", Done: true})
				return
			}
			mapped, done := mapRunnerEvent(event, requestID, traceID)
			for _, item := range mapped {
				if !sendDispatchEvent(ctx, output, item) {
					drainRunnerEvents(runnerEvents, dispatcher.drainTimeout)
					return
				}
			}
			if done {
				drainRunnerEvents(runnerEvents, dispatcher.drainTimeout)
				return
			}
		case <-ctx.Done():
			dispatcher.finishCanceled(ctx, requestID, traceID, runnerEvents, output)
			return
		}
	}
}

func (dispatcher *Dispatcher) finishCanceled(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, output chan<- DispatchEvent) {
	drainRunnerEvents(runnerEvents, dispatcher.drainTimeout)
	trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventError, RequestID: requestID, TraceID: traceID, Error: ErrExecutionCanceled.Error()})
	trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: cancellationStatus(ctx), Done: true})
}

func cancellationStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "canceled"
}

func mapRunnerEvent(event *trpcevent.Event, requestID, traceID string) ([]DispatchEvent, bool) {
	if event == nil || event.Response == nil {
		return []DispatchEvent{{Type: DispatchEventStatus, RequestID: requestID, TraceID: traceID, Status: "progress"}}, false
	}
	response := event.Response
	if response.Error != nil {
		return []DispatchEvent{
			{Type: DispatchEventError, RequestID: requestID, TraceID: traceID, Error: ErrExecution.Error()},
			{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: "error", Done: true},
		}, true
	}
	text := responseText(response)
	result := make([]DispatchEvent, 0, 2)
	if text != "" {
		result = append(result, DispatchEvent{Type: DispatchEventMessage, RequestID: requestID, TraceID: traceID, Text: text})
	}
	if response.Done {
		result = append(result, DispatchEvent{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: "complete", Done: true})
		return result, true
	}
	if len(result) == 0 {
		status := "progress"
		if response.IsPartial {
			status = "partial"
		}
		result = append(result, DispatchEvent{Type: DispatchEventStatus, RequestID: requestID, TraceID: traceID, Status: status})
	}
	return result, false
}

func responseText(response *trpcmodel.Response) string {
	if response == nil {
		return ""
	}
	var builder strings.Builder
	for _, choice := range response.Choices {
		text := choice.Delta.Content
		if text == "" {
			text = choice.Message.Content
		}
		if text != "" {
			builder.WriteString(text)
		}
	}
	return builder.String()
}

func sendDispatchEvent(ctx context.Context, output chan<- DispatchEvent, event DispatchEvent) bool {
	select {
	case output <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func trySendDispatchEvent(output chan<- DispatchEvent, event DispatchEvent) {
	select {
	case output <- event:
	default:
	}
}

func drainRunnerEvents(events <-chan *trpcevent.Event, timeout time.Duration) {
	if events == nil {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-timer.C:
			return
		}
	}
}

func normalizeCorrelationID(value string, generate bool) (string, error) {
	if value == "" && generate {
		return uuid.NewString(), nil
	}
	if value == "" {
		return "", nil
	}
	if strings.TrimSpace(value) == "" || hasControl(value) || len([]rune(value)) > maxPrincipalIDRunes {
		return "", fmt.Errorf("%w: correlation ID is invalid", ErrInvalid)
	}
	return value, nil
}

func dispatchRunnerIdentity(principal Principal, message InboundMessage) (tenant.RunnerIdentity, error) {
	switch principal.Kind() {
	case PrincipalChannel:
		target, ok := principal.RoutingTarget()
		if !ok {
			return tenant.RunnerIdentity{}, ErrUnauthenticated
		}
		return target.RunnerIdentity(channels.IdentityInput{
			ExternalUserID: message.ExternalUserID, Kind: message.ConversationKind,
			ExternalPeerID: message.ExternalPeerID, ExternalChatID: message.ExternalChatID,
			ExternalThreadID: message.ExternalThreadID,
		})
	case PrincipalAPI:
		conversation := message.ExternalPeerID
		if message.ConversationKind == channels.ConversationGroup {
			conversation = message.ExternalChatID
		}
		sessionID := encodeDispatchIdentity("api", principal.AppID(), string(message.ConversationKind), conversation, message.ExternalThreadID)
		userID := encodeDispatchIdentity("api", principal.AppID(), principal.SubjectID())
		return tenant.NewRunnerIdentity(principal.TenantID(), userID, sessionID)
	default:
		return tenant.RunnerIdentity{}, ErrUnauthenticated
	}
}

func encodeDispatchIdentity(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

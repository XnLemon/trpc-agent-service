package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
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
	// ErrAuditWriteFailed is the stable redacted failure when a mandatory audit
	// lifecycle fact cannot be durably written.
	ErrAuditWriteFailed = errors.New("audit_write_failed")
)

const (
	forwardTerminalPending uint32 = iota
	forwardTerminalCanceled
	forwardTerminalCommitted

	defaultDispatchDrainTimeout      = 250 * time.Millisecond
	durableInboundLeaseGrace         = 30 * time.Second
	durableFailureFallbackReply      = "An error occurred during execution. Please contact the service provider."
	maxDurableExternalMessageIDRunes = 512
)

// DispatchEventType identifies the protocol-neutral event surface consumed by
// JSON and SSE adapters.
type DispatchEventType string

const (
	// DispatchEventMessage identifies an inbound message dispatch event.
	DispatchEventMessage DispatchEventType = "message"
	// DispatchEventStatus identifies a dispatch status event.
	DispatchEventStatus DispatchEventType = "status"
	// DispatchEventError identifies a dispatch failure event.
	DispatchEventError DispatchEventType = "error"
	// DispatchEventDone identifies a completed dispatch event.
	DispatchEventDone DispatchEventType = "done"
)

// DispatchRequest is the trusted input to the protocol-neutral execution
// boundary. Principal fields are never reconstructed from Message.
type DispatchRequest struct {
	Principal Principal
	Message   InboundMessage
	RequestID string
	TraceID   string
	// Accepted is notified after the durable channel claim and handoff reserve
	// succeed, before runner execution begins. It is optional for API callers.
	Accepted chan<- struct{}
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
	// RuntimeStore enables durable inbound claims for verified Channel principals.
	// API principals remain protected by the HTTP IdempotencyStore.
	RuntimeStore runtimestorage.RuntimeStore
	Materializer *outbox.Materializer
	// AuditWriter receives mandatory execution lifecycle facts. It is optional
	// for compatibility with deployments that have not enabled audit storage.
	AuditWriter audit.Writer
	// HandoffStore durably reserves and finalizes execution audit facts.
	HandoffStore audit.HandoffStore
	// Attachments loads verified tenant-owned media only when an inbound message
	// contains attachment references. Text-only dispatches remain independent of it.
	Attachments     attachment.Reader
	AttachmentStore runtimestorage.AttachmentStore
}

// Dispatcher resolves a fixed plan, acquires a Runner lease, and translates
// Runner events into a bounded, redacted event stream.
type Dispatcher struct {
	resolver        *PlanResolver
	registry        *RunnerRegistry
	drainTimeout    time.Duration
	telemetry       observability.Provider
	metrics         metrics.Catalog
	runtimeStore    runtimestorage.RuntimeStore
	materializer    *outbox.Materializer
	auditWriter     audit.Writer
	handoffStore    audit.HandoffStore
	attachments     attachment.Reader
	attachmentStore runtimestorage.AttachmentStore
}

type durableExecution struct {
	store        runtimestorage.RuntimeStore
	tenantID     string
	eventID      string
	owner        string
	fencingToken int64
	replyTarget  runtimestorage.ReplyTarget
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
	if config.Attachments == nil {
		if reader, ok := config.RuntimeStore.(attachment.Reader); ok {
			config.Attachments = reader
		}
	}
	if config.AttachmentStore == nil {
		if store, ok := config.RuntimeStore.(runtimestorage.AttachmentStore); ok {
			config.AttachmentStore = store
		} else if store, ok := config.Attachments.(runtimestorage.AttachmentStore); ok {
			config.AttachmentStore = store
		}
	}
	config.AuditWriter = metrics.WrapAuditWriter(config.AuditWriter, config.Observability)
	if config.Materializer == nil && config.RuntimeStore != nil {
		materializer, err := outbox.NewMaterializer(outbox.MaterializerConfig{Store: config.RuntimeStore, Observability: config.Observability})
		if err != nil {
			return nil, err
		}
		config.Materializer = materializer
	}
	return &Dispatcher{resolver: config.Resolver, registry: config.Registry, drainTimeout: config.DrainTimeout, telemetry: config.Observability, metrics: metrics.New(config.Observability), runtimeStore: config.RuntimeStore, materializer: config.Materializer, auditWriter: config.AuditWriter, handoffStore: config.HandoffStore, attachments: config.Attachments, attachmentStore: config.AttachmentStore}, nil
}

// Ready reports whether both plan resolution and Runner acquisition are ready.
func (dispatcher *Dispatcher) Ready() bool {
	return dispatcher != nil && dispatcher.resolver != nil && dispatcher.resolver.Ready() && dispatcher.registry != nil && dispatcher.registry.Ready()
}

// Dispatch starts one execution and returns a redacted event stream. The
// returned stream owns the Runner lease until it reaches terminal state or the
// caller Context is canceled.
//
//nolint:gocyclo // Dispatch coordinates validation, durable state, audit, lease, and stream lifecycle.
func (dispatcher *Dispatcher) Dispatch(ctx context.Context, request DispatchRequest) (<-chan DispatchEvent, error) {
	if dispatcher == nil || dispatcher.resolver == nil || dispatcher.registry == nil {
		return nil, ErrNotReady
	}
	message, requestID, traceID, err := normalizeDispatchRequest(ctx, request)
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
		_ = dispatcher.metrics.Operation(ctx, started, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch}, cause)
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
	planSnapshot := plan.AgentSnapshot()
	planApp := planSnapshot.App()
	durable, err := dispatcher.claimInboundWithLease(ctx, request.Principal, message, identity, durableInboundLeaseForRuntime(planSnapshot.Revision().Runtime))
	if err != nil {
		finishWithError(err)
		return nil, err
	}
	attachmentEventID := ""
	if durable != nil {
		attachmentEventID = durable.eventID
	}
	if len(message.Attachments) > 0 {
		binder, ok := dispatcher.attachments.(attachment.Binder)
		if !ok || attachmentEventID == "" {
			dispatcher.failDurable(durable, ErrExecution)
			finishWithError(ErrExecution)
			return nil, ErrExecution
		}
		if err := binder.BindAttachments(ctx, request.Principal.TenantID(), attachmentEventID, message.Attachments); err != nil {
			dispatcher.failDurable(durable, err)
			finishWithError(err)
			return nil, ErrExecution
		}
	}
	userMessage, err := buildUserMessage(ctx, dispatcher.attachments, request.Principal.TenantID(), attachmentEventID, message)
	if err != nil {
		dispatcher.failDurable(durable, err)
		finishWithError(err)
		if IsContextCancellation(err) {
			return nil, err
		}
		return nil, ErrExecution
	}
	if planApp.CanaryRevision != nil && planSnapshot.Revision().Revision == *planApp.CanaryRevision {
		selectedRevision := planSnapshot.Revision().Revision
		if err := dispatcher.writeExecutionAuditRevision(ctx, request.Principal, message, identity, requestID, traceID, audit.EventCanarySelected, "", &selectedRevision); err != nil {
			dispatcher.failDurable(durable, err)
			finishWithError(err)
			return nil, auditWriteFailure()
		}
	}
	if err := dispatcher.writeExecutionAudit(ctx, request.Principal, message, identity, requestID, traceID, audit.EventExecutionStarted, ""); err != nil {
		dispatcher.failDurable(durable, err)
		finishWithError(err)
		return nil, auditWriteFailure()
	}
	if err := dispatcher.reserveHandoff(ctx, request.Principal, requestID, traceID); err != nil {
		finishWithError(err)
		return nil, auditWriteFailure()
	}
	if request.Accepted != nil {
		select {
		case request.Accepted <- struct{}{}:
		default:
		}
	}
	lease, err := dispatcher.registry.Acquire(ctx, plan)
	if err != nil {
		dispatcher.failDurable(durable, err)
		if auditErr := dispatcher.writeExecutionAudit(context.Background(), request.Principal, message, identity, requestID, traceID, audit.EventExecutionFailed, string(audit.ErrorUnavailable)); auditErr != nil {
			dispatcher.failDurable(durable, auditErr)
			finishWithError(auditErr)
			return nil, auditWriteFailure()
		}
		finishWithError(err)
		return nil, err
	}
	runnerValue := lease.Runner()
	if runnerValue == nil {
		_ = lease.Release()
		_ = dispatcher.metrics.Lease(ctx, -1, map[string]string{"component": "runner", "status": "active"})
		dispatcher.failDurable(durable, ErrRunnerUnavailable)
		if auditErr := dispatcher.writeExecutionAudit(context.Background(), request.Principal, message, identity, requestID, traceID, audit.EventExecutionFailed, string(audit.ErrorUnavailable)); auditErr != nil {
			dispatcher.failDurable(durable, auditErr)
			finishWithError(auditErr)
			return nil, auditWriteFailure()
		}
		finishWithError(ErrRunnerUnavailable)
		return nil, ErrRunnerUnavailable
	}
	_ = dispatcher.metrics.Lease(ctx, 1, map[string]string{"component": "runner", "status": "active"})
	runnerStarted := time.Now()
	runnerCtx, _, finishRunner := observability.StartOperation(ctx, dispatcher.telemetry, observability.OperationRunnerExecution, "runner")
	_ = dispatcher.metrics.Request(runnerCtx, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution, "status": "started"})
	mediaReplies := servicetool.NewReplyCollector()
	if durable != nil {
		runnerCtx = servicetool.WithExecutionContext(runnerCtx, servicetool.ExecutionContext{
			TenantID: request.Principal.TenantID(), EventID: durable.eventID, RequestID: requestID, TraceID: traceID,
			Attachments: dispatcher.attachmentStore, Replies: mediaReplies,
			Audit: audit.Recorder{Writer: dispatcher.auditWriter, TenantID: request.Principal.TenantID()},
		})
	}
	runnerEvents, err := runnerValue.Run(runnerCtx, identity.UserID, identity.SessionID, userMessage, trpcagent.WithRequestID(requestID))
	if err != nil {
		finishRunner(err)
		_ = dispatcher.metrics.Operation(runnerCtx, runnerStarted, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution}, err)
		_ = lease.Release()
		_ = dispatcher.metrics.Lease(ctx, -1, map[string]string{"component": "runner", "status": "active"})
		dispatcher.failDurable(durable, err)
		eventType, errorType := audit.EventExecutionFailed, string(audit.ErrorUnavailable)
		if IsContextCancellation(err) {
			eventType, errorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
		}
		if auditErr := dispatcher.writeExecutionAudit(context.Background(), request.Principal, message, identity, requestID, traceID, eventType, errorType); auditErr != nil {
			dispatcher.failDurable(durable, auditErr)
			finishWithError(auditErr)
			return nil, auditWriteFailure()
		}
		if IsContextCancellation(err) {
			finishWithError(err)
			return nil, err
		}
		finishWithError(err)
		return nil, ErrExecution
	}
	if runnerEvents == nil {
		finishRunner(ErrExecution)
		_ = dispatcher.metrics.Operation(runnerCtx, runnerStarted, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution}, ErrExecution)
		_ = lease.Release()
		_ = dispatcher.metrics.Lease(ctx, -1, map[string]string{"component": "runner", "status": "active"})
		dispatcher.failDurable(durable, ErrExecution)
		if auditErr := dispatcher.writeExecutionAudit(context.Background(), request.Principal, message, identity, requestID, traceID, audit.EventExecutionFailed, string(audit.ErrorUnavailable)); auditErr != nil {
			dispatcher.failDurable(durable, auditErr)
			finishWithError(auditErr)
			return nil, auditWriteFailure()
		}
		finishWithError(ErrExecution)
		return nil, ErrExecution
	}

	output := make(chan DispatchEvent, 32)
	_ = dispatcher.metrics.Active(ctx, 1, map[string]string{"component": "runner"})
	go dispatcher.forward(runnerCtx, requestID, traceID, runnerEvents, lease, durable, output, span, started, request.Principal, message, identity, finishRunner, runnerStarted, mediaReplies)
	return output, nil
}

func (dispatcher *Dispatcher) reserveHandoff(ctx context.Context, principal Principal, requestID, traceID string) error {
	if dispatcher.handoffStore == nil {
		return nil
	}
	_, err := dispatcher.handoffStore.Reserve(ctx, audit.ExecutionHandoff{
		TenantID: principal.TenantID(), HandoffID: audit.NewEventID(requestID, "handoff"),
		RequestID: requestID, TraceID: traceID, EventID: audit.NewEventID(requestID, string(audit.EventExecutionStarted)), State: audit.HandoffPending,
	})
	return err
}

func normalizeDispatchRequest(ctx context.Context, request DispatchRequest) (InboundMessage, string, string, error) {
	if ctx == nil {
		return InboundMessage{}, "", "", fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return InboundMessage{}, "", "", err
	}
	if err := request.Principal.Validate(); err != nil {
		return InboundMessage{}, "", "", ErrUnauthenticated
	}
	message, err := request.Message.Normalize()
	if err != nil {
		return InboundMessage{}, "", "", err
	}
	requestID, err := normalizeCorrelationID(request.RequestID, true)
	if err != nil {
		return InboundMessage{}, "", "", err
	}
	traceID, err := normalizeCorrelationID(request.TraceID, false)
	if err != nil {
		return InboundMessage{}, "", "", err
	}
	return message, requestID, traceID, nil
}

func detachedCorrelationContext(parent context.Context, requestID, traceID string) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return observability.WithCorrelation(context.WithoutCancel(parent), requestID, traceID)
}

func (dispatcher *Dispatcher) claimInbound(ctx context.Context, principal Principal, message InboundMessage, identity tenant.RunnerIdentity) (result *durableExecution, err error) {
	return dispatcher.claimInboundWithLease(ctx, principal, message, identity, durableInboundLeaseForRuntime(appmodel.DefaultRuntimePolicy()))
}

func (dispatcher *Dispatcher) claimInboundWithLease(ctx context.Context, principal Principal, message InboundMessage, identity tenant.RunnerIdentity, leaseDuration time.Duration) (result *durableExecution, err error) {
	if dispatcher.runtimeStore == nil || principal.Kind() != PrincipalChannel {
		return nil, nil
	}
	if leaseDuration <= 0 {
		leaseDuration = durableInboundLeaseForRuntime(appmodel.DefaultRuntimePolicy())
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, dispatcher.telemetry, observability.OperationStorageOperation, "storage")
	_ = dispatcher.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "status": "started"})
	defer func() {
		finish(err)
		_ = dispatcher.metrics.Operation(operationCtx, started, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation}, err)
		status := "success"
		if err != nil {
			status = observability.ErrorClass(err)
			if status == "" {
				status = "error"
			}
		}
		_ = dispatcher.metrics.BackendDuration(operationCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "status": status, "error_class": observability.ErrorClass(err)})
	}()
	ctx = operationCtx
	target, ok := principal.RoutingTarget()
	if !ok || message.ExternalMessageID == "" || len([]rune(message.ExternalMessageID)) > maxDurableExternalMessageIDRunes {
		return nil, fmt.Errorf("%w: durable Channel messages require an external message ID", ErrInvalid)
	}
	store := dispatcher.runtimeStore
	replyTarget, err := replyTarget(target, message)
	if err != nil {
		return nil, err
	}
	if err := ensureInboundSession(ctx, store, principal.TenantID(), identity.SessionID); err != nil {
		return nil, err
	}
	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{
		TenantID: principal.TenantID(), EventID: uuid.NewString(), SessionID: identity.SessionID,
		BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID,
		IdempotencyKey: message.ExternalMessageID,
		ReplyTarget:    replyTarget,
	})
	if err != nil {
		return nil, err
	}
	owner := "gateway-" + uuid.NewString()
	event, err = prepareInboundEvent(ctx, store, principal.TenantID(), event, duplicate, owner)
	if err != nil {
		return nil, err
	}
	from := event.Status
	running, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{
		TenantID: principal.TenantID(), EventID: event.EventID, From: from,
		To: runtimestorage.EventRunning, Owner: owner, LeaseDuration: leaseDuration,
	})
	if err != nil {
		if duplicate && errors.Is(err, runtimestorage.ErrConflict) {
			return nil, ErrDuplicateMessage
		}
		return nil, err
	}
	return &durableExecution{store: store, tenantID: principal.TenantID(), eventID: event.EventID, owner: owner, fencingToken: running.FencingToken, replyTarget: event.ReplyTarget}, nil
}

func durableInboundLeaseForRuntime(policy appmodel.RuntimePolicy) time.Duration {
	seconds := policy.ExecutionTimeoutSeconds
	if seconds <= 0 {
		seconds = appmodel.DefaultRuntimePolicy().ExecutionTimeoutSeconds
	}
	return time.Duration(seconds)*time.Second + durableInboundLeaseGrace
}

func replyTarget(target channels.RoutingTarget, message InboundMessage) (runtimestorage.ReplyTarget, error) {
	reply := runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: string(message.ConversationKind), ThreadID: message.ExternalThreadID}
	switch message.ConversationKind {
	case channels.ConversationDirect:
		reply.ReceiverID = message.ExternalPeerID
	case channels.ConversationGroup:
		reply.ReceiverID = message.ExternalChatID
	default:
		return runtimestorage.ReplyTarget{}, fmt.Errorf("%w: reply conversation kind is invalid", ErrInvalid)
	}
	if err := runtimestorage.ValidateReplyTarget(reply); err != nil {
		return runtimestorage.ReplyTarget{}, fmt.Errorf("%w: reply target is invalid", ErrInvalid)
	}
	return reply, nil
}

func ensureInboundSession(ctx context.Context, store runtimestorage.RuntimeStore, tenantID, sessionID string) error {
	if _, err := store.GetSession(ctx, tenantID, sessionID); err == nil {
		return nil
	} else if !errors.Is(err, runtimestorage.ErrNotFound) {
		return err
	}
	_, err := store.CreateSession(ctx, tenantID, sessionID, nil)
	if errors.Is(err, runtimestorage.ErrDuplicate) {
		return nil
	}
	return err
}

func prepareInboundEvent(ctx context.Context, store runtimestorage.RuntimeStore, tenantID string, event runtimestorage.MessageEvent, duplicate bool, owner string) (runtimestorage.MessageEvent, error) {
	if !duplicate {
		return event, nil
	}
	if event.Status == runtimestorage.EventRunning && (event.LeaseExpiresAt == nil || event.LeaseExpiresAt.After(time.Now().UTC())) {
		return runtimestorage.MessageEvent{}, ErrDuplicateMessage
	}
	if event.Status == runtimestorage.EventRunning {
		if _, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: tenantID, EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: owner}); err != nil {
			return runtimestorage.MessageEvent{}, ErrDuplicateMessage
		}
		event.Status = runtimestorage.EventExecutionReconciling
	}
	if event.Status != runtimestorage.EventReceived && event.Status != runtimestorage.EventExecutionReconciling {
		return runtimestorage.MessageEvent{}, ErrDuplicateMessage
	}
	return event, nil
}

func (dispatcher *Dispatcher) failDurable(durable *durableExecution, cause error) {
	if durable == nil {
		return
	}
	to := runtimestorage.EventFailed
	_ = dispatcher.observeStorage(context.Background(), func(operationCtx context.Context) error {
		_, err := durable.store.TransitionMessage(operationCtx, runtimestorage.MessageTransition{
			TenantID: durable.tenantID, EventID: durable.eventID, From: runtimestorage.EventRunning,
			To: to, Owner: durable.owner, FencingToken: durable.fencingToken,
		})
		return err
	})
}

func (dispatcher *Dispatcher) finishDurable(ctx context.Context, requestID, traceID string, durable *durableExecution, terminalErr error, reply string, mediaReplies []servicetool.ReplyIntent) {
	if durable == nil {
		return
	}
	durableCtx := detachedCorrelationContext(ctx, requestID, traceID)
	if terminalErr != nil && !IsContextCancellation(terminalErr) {
		reply = durableFailureFallbackReply
		mediaReplies = nil
	}
	segments := 0
	replyID := ""
	if dispatcher.materializer != nil {
		input := outbox.MaterializeInput{TenantID: durable.tenantID, EventID: durable.eventID, ReplyID: durable.eventID, RequestID: requestID, TraceID: traceID, TraceParent: observability.TraceParentFromContext(durableCtx), ReplyTarget: durable.replyTarget}
		if terminalErr == nil && len(mediaReplies) > 0 {
			input.Segments = mediaReplySegments(mediaReplies)
		} else {
			input.Payload = reply
		}
		if strings.TrimSpace(input.Payload) != "" || len(input.Segments) > 0 {
			var err error
			segments, err = dispatcher.materializer.Materialize(durableCtx, input)
			if err != nil {
				terminalErr = err
				if len(mediaReplies) > 0 {
					fallback := outbox.MaterializeInput{TenantID: durable.tenantID, EventID: durable.eventID, ReplyID: durable.eventID, RequestID: requestID, TraceID: traceID, TraceParent: observability.TraceParentFromContext(durableCtx), Payload: durableFailureFallbackReply, ReplyTarget: durable.replyTarget}
					segments, err = dispatcher.materializer.Materialize(durableCtx, fallback)
					if err == nil {
						replyID = durable.eventID
					}
				}
			} else {
				replyID = durable.eventID
			}
		}
	}
	to := runtimestorage.EventCompleted
	if terminalErr != nil && replyID == "" {
		to = runtimestorage.EventFailed
	}
	_ = dispatcher.observeStorage(durableCtx, func(operationCtx context.Context) error {
		_, err := durable.store.TransitionMessage(operationCtx, runtimestorage.MessageTransition{
			TenantID: durable.tenantID, EventID: durable.eventID, From: runtimestorage.EventRunning,
			To: to, Owner: durable.owner, FencingToken: durable.fencingToken, ReplyID: replyID, SegmentCount: segments,
		})
		return err
	})
}

func mediaReplySegments(intents []servicetool.ReplyIntent) []outbox.ReplySegment {
	segments := make([]outbox.ReplySegment, 0, len(intents))
	for _, intent := range intents {
		segments = append(segments, outbox.ReplySegment{Kind: intent.Kind, Payload: intent.Payload, Attachment: intent.Attachment, Fallback: intent.Fallback})
	}
	return segments
}

func (dispatcher *Dispatcher) observeStorage(ctx context.Context, operation func(context.Context) error) error {
	if dispatcher == nil || operation == nil {
		return ErrInvalid
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, dispatcher.telemetry, observability.OperationStorageOperation, "storage")
	_ = dispatcher.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other", "status": "started"})
	err := operation(operationCtx)
	finish(err)
	labels := map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other"}
	_ = dispatcher.metrics.Operation(operationCtx, started, labels, err)
	status := "success"
	if err != nil {
		status = observability.ErrorClass(err)
		if status == "" {
			status = "error"
		}
	}
	_ = dispatcher.metrics.BackendDuration(operationCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "provider": "other", "status": status, "error_class": observability.ErrorClass(err)})
	return err
}

func (dispatcher *Dispatcher) forward(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, lease *RunnerLease, durable *durableExecution, output chan<- DispatchEvent, span observability.Span, started time.Time, principal Principal, message InboundMessage, identity tenant.RunnerIdentity, finishRunner func(error), runnerStarted time.Time, mediaReplies *servicetool.ReplyCollector) {
	defer close(output)
	var terminalState atomic.Uint32
	terminalState.Store(forwardTerminalPending)
	cancelWatchDone := make(chan struct{})
	defer close(cancelWatchDone)
	go func() {
		select {
		case <-ctx.Done():
			terminalState.CompareAndSwap(forwardTerminalPending, forwardTerminalCanceled)
		case <-cancelWatchDone:
		}
	}()
	defer func() {
		_ = lease.Release()
		_ = dispatcher.metrics.Lease(ctx, -1, map[string]string{"component": "runner", "status": "active"})
	}()
	var terminalErr error
	var reply strings.Builder
	var terminalEventType audit.EventType
	var terminalErrorType string
	terminalCommitted := false
	terminalErrorEmitted := false
	auditFinalized := false
	finalizeAudit := func(eventType audit.EventType, errorType string) error {
		if auditFinalized {
			return nil
		}
		err := dispatcher.writeExecutionAudit(detachedCorrelationContext(ctx, requestID, traceID), principal, message, identity, requestID, traceID, eventType, errorType)
		if err == nil {
			auditFinalized = true
		}
		return err
	}
	finalizeHandoff := func(result audit.ExecutionResult, errorType string) error {
		if dispatcher.handoffStore == nil {
			return nil
		}
		_, err := dispatcher.handoffStore.Finalize(detachedCorrelationContext(ctx, requestID, traceID), audit.ExecutionHandoff{TenantID: principal.TenantID(), HandoffID: audit.NewEventID(requestID, "handoff"), State: audit.HandoffFinalized, Result: result, ErrorType: errorType})
		return err
	}
	defer func() {
		dispatcher.prepareForwardTerminal(ctx, requestID, traceID, runnerEvents, output, &terminalErr, &terminalEventType, &terminalErrorType, &terminalCommitted, &terminalState, finalizeAudit, finalizeHandoff)
		if finishRunner != nil {
			finishRunner(terminalErr)
			_ = dispatcher.metrics.Operation(ctx, runnerStarted, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution}, terminalErr)
		}
		terminalErr = dispatcher.finalizeForward(ctx, requestID, traceID, durable, principal, terminalErr, reply.String(), mediaReplies.Intents(), terminalEventType, terminalErrorType, finalizeAudit)
		finishForwardOutput(ctx, output, requestID, traceID, terminalErr, terminalCommitted, terminalErrorEmitted)
		_ = dispatcher.metrics.Active(ctx, -1, map[string]string{"component": "runner"})
		if terminalErr != nil {
			class := observability.ErrorClass(terminalErr)
			span.SetAttributes(observability.Attribute{Key: "error_class", Value: class})
			span.SetStatus(observability.StatusError, class)
			span.RecordError(terminalErr)
		} else {
			span.SetStatus(observability.StatusOK, "")
		}
		_ = dispatcher.metrics.Operation(ctx, started, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch}, terminalErr)
		span.End()
	}()
	for {
		if ctx.Err() != nil {
			terminalErr = ctx.Err()
			terminalEventType, terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
			terminalErr = dispatcher.handleForwardCancellation(ctx, requestID, traceID, runnerEvents, output, finalizeAudit, finalizeHandoff)
			terminalCommitted, terminalErrorEmitted = true, true
			return
		}
		select {
		case event, ok := <-runnerEvents:
			// A canceled context and a ready runner event can both win the
			// select. Re-check cancellation after receiving so a late event
			// cannot turn a canceled execution into a successful completion.
			if ctx.Err() != nil {
				terminalErr = ctx.Err()
				terminalEventType, terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
				terminalErr = dispatcher.handleForwardCancellation(ctx, requestID, traceID, runnerEvents, output, finalizeAudit, finalizeHandoff)
				terminalCommitted, terminalErrorEmitted = true, true
				return
			}
			if !ok {
				if dispatcher.cancelForwardIfNeeded(ctx, requestID, traceID, runnerEvents, output, &terminalErr, &terminalEventType, &terminalErrorType, &terminalCommitted, finalizeAudit, finalizeHandoff) {
					terminalErrorEmitted = true
					return
				}
				terminalEventType, terminalErrorType = audit.EventExecutionCompleted, ""
				terminalErr = dispatcher.handleForwardStreamClosed(ctx, requestID, traceID, runnerEvents, output, &terminalCommitted, finalizeAudit, finalizeHandoff)
				return
			}
			if dispatcher.handleForwardRunnerEvent(ctx, requestID, traceID, runnerEvents, output, event, &reply, &terminalErr, &terminalEventType, &terminalErrorType, &terminalCommitted, &terminalErrorEmitted, finalizeAudit, finalizeHandoff) {
				return
			}
		case <-ctx.Done():
			terminalErr = ctx.Err()
			terminalEventType, terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
			terminalErr = dispatcher.handleForwardCancellation(ctx, requestID, traceID, runnerEvents, output, finalizeAudit, finalizeHandoff)
			terminalCommitted, terminalErrorEmitted = true, true
			return
		}
	}
}

func (dispatcher *Dispatcher) prepareForwardTerminal(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, output chan<- DispatchEvent, terminalErr *error, terminalEventType *audit.EventType, terminalErrorType *string, terminalCommitted *bool, terminalState *atomic.Uint32, finalizeAudit func(audit.EventType, string) error, finalizeHandoff func(audit.ExecutionResult, string) error) {
	if *terminalErr != nil || *terminalCommitted {
		return
	}
	if ctx.Err() == nil && terminalState.CompareAndSwap(forwardTerminalPending, forwardTerminalCommitted) {
		return
	}
	cancelErr := ctx.Err()
	if cancelErr == nil {
		cancelErr = context.Canceled
	}
	*terminalErr = cancelErr
	*terminalEventType, *terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	if err := dispatcher.handleForwardCancellation(ctx, requestID, traceID, runnerEvents, output, finalizeAudit, finalizeHandoff); err != nil {
		*terminalErr = err
	}
	*terminalCommitted = true
}

func finishForwardOutput(ctx context.Context, output chan<- DispatchEvent, requestID, traceID string, terminalErr error, terminalCommitted, terminalErrorEmitted bool) {
	if terminalCommitted {
		return
	}
	if IsContextCancellation(terminalErr) {
		if !terminalErrorEmitted {
			trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventError, RequestID: requestID, TraceID: traceID, Error: ErrExecutionCanceled.Error()})
		}
		trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: cancellationStatus(ctx), Done: true})
		return
	}
	if terminalErr != nil {
		if !terminalErrorEmitted {
			errorText := ErrExecution.Error()
			if errors.Is(terminalErr, ErrAuditWriteFailed) {
				errorText = ErrAuditWriteFailed.Error()
			}
			trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventError, RequestID: requestID, TraceID: traceID, Error: errorText})
		}
		trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: "error", Done: true})
		return
	}
	trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: "complete", Done: true})
}

func (dispatcher *Dispatcher) handleForwardStreamClosed(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, output chan<- DispatchEvent, terminalCommitted *bool, finalizeAudit func(audit.EventType, string) error, finalizeHandoff func(audit.ExecutionResult, string) error) error {
	if ctx.Err() != nil {
		err := dispatcher.handleForwardCancellation(ctx, requestID, traceID, runnerEvents, output, finalizeAudit, finalizeHandoff)
		*terminalCommitted = true
		return err
	}
	return nil
}

func (dispatcher *Dispatcher) cancelForwardIfNeeded(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, output chan<- DispatchEvent, terminalErr *error, terminalEventType *audit.EventType, terminalErrorType *string, terminalCommitted *bool, finalizeAudit func(audit.EventType, string) error, finalizeHandoff func(audit.ExecutionResult, string) error) bool {
	if ctx.Err() == nil {
		return false
	}
	*terminalErr = ctx.Err()
	*terminalEventType, *terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	*terminalErr = dispatcher.handleForwardCancellation(ctx, requestID, traceID, runnerEvents, output, finalizeAudit, finalizeHandoff)
	*terminalCommitted = true
	return true
}

//nolint:gocyclo // terminal event mapping, cancellation, audit, handoff, and stream ownership are one state transition.
func (dispatcher *Dispatcher) handleForwardRunnerEvent(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, output chan<- DispatchEvent, event *trpcevent.Event, reply *strings.Builder, terminalErr *error, terminalEventType *audit.EventType, terminalErrorType *string, terminalCommitted, terminalErrorEmitted *bool, finalizeAudit func(audit.EventType, string) error, finalizeHandoff func(audit.ExecutionResult, string) error) bool {
	if dispatcher.cancelForwardIfNeeded(ctx, requestID, traceID, runnerEvents, output, terminalErr, terminalEventType, terminalErrorType, terminalCommitted, finalizeAudit, finalizeHandoff) {
		return true
	}
	mapped, done := mapRunnerEvent(event, requestID, traceID)
	if done {
		*terminalErr = nil
		for _, item := range mapped {
			if item.Type == DispatchEventError {
				*terminalErr = ErrExecution
			}
		}
		eventType, errorType := audit.EventExecutionCompleted, ""
		if *terminalErr != nil {
			eventType, errorType = audit.EventExecutionFailed, string(audit.ErrorUnavailable)
		}
		*terminalEventType, *terminalErrorType = eventType, errorType
	}
	for _, item := range mapped {
		if dispatcher.cancelForwardIfNeeded(ctx, requestID, traceID, runnerEvents, output, terminalErr, terminalEventType, terminalErrorType, terminalCommitted, finalizeAudit, finalizeHandoff) {
			return true
		}
		if done && item.Type == DispatchEventDone {
			continue
		}
		if item.Type == DispatchEventMessage {
			reply.WriteString(item.Text)
		}
		if item.Type == DispatchEventError {
			*terminalErr = ErrExecution
		}
		if !sendDispatchEvent(ctx, output, item) {
			*terminalErr = ctx.Err()
			if IsContextCancellation(*terminalErr) {
				*terminalEventType, *terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
			}
			drainRunnerEvents(runnerEvents, dispatcher.drainTimeout)
			return true
		}
		if item.Type == DispatchEventError {
			*terminalErrorEmitted = true
		}
	}
	if !done {
		return false
	}
	drainRunnerEvents(runnerEvents, dispatcher.drainTimeout)
	return true
}

func (dispatcher *Dispatcher) finalizeForward(ctx context.Context, requestID, traceID string, durable *durableExecution, principal Principal, terminalErr error, reply string, mediaReplies []servicetool.ReplyIntent, terminalEventType audit.EventType, terminalErrorType string, finalizeAudit func(audit.EventType, string) error) error {
	eventType := terminalEventType
	errorType := terminalErrorType
	if eventType == "" {
		eventType = audit.EventExecutionCompleted
		if terminalErr != nil {
			eventType = audit.EventExecutionFailed
			if IsContextCancellation(terminalErr) {
				eventType = audit.EventExecutionCanceled
			}
		}
	}
	if errorType == "" {
		errorType = terminalAuditError(terminalErr)
	}
	if dispatcher.handoffStore != nil {
		result := audit.ResultSuccess
		if terminalErr != nil {
			result = audit.ResultFailure
			if IsContextCancellation(terminalErr) {
				result = audit.ResultCanceled
			}
		}
		if _, err := dispatcher.handoffStore.Finalize(detachedCorrelationContext(ctx, requestID, traceID), audit.ExecutionHandoff{TenantID: principal.TenantID(), HandoffID: audit.NewEventID(requestID, "handoff"), State: audit.HandoffFinalized, Result: result, ErrorType: errorType}); err != nil && terminalErr == nil {
			terminalErr = auditWriteFailure()
		}
	}
	if err := finalizeAudit(eventType, errorType); err != nil && terminalErr == nil {
		terminalErr = auditWriteFailure()
	}
	dispatcher.finishDurable(ctx, requestID, traceID, durable, terminalErr, reply, mediaReplies)
	return terminalErr
}

func (dispatcher *Dispatcher) handleForwardCancellation(ctx context.Context, requestID, traceID string, runnerEvents <-chan *trpcevent.Event, output chan<- DispatchEvent, finalizeAudit func(audit.EventType, string) error, finalizeHandoff func(audit.ExecutionResult, string) error) error {
	if err := finalizeAudit(audit.EventExecutionCanceled, string(audit.ErrorCanceled)); err != nil {
		dispatcher.finishAuditFailure(requestID, traceID, runnerEvents, output)
		return auditWriteFailure()
	}
	if err := finalizeHandoff(audit.ResultCanceled, string(audit.ErrorCanceled)); err != nil {
		dispatcher.finishAuditFailure(requestID, traceID, runnerEvents, output)
		return auditWriteFailure()
	}
	dispatcher.finishCanceled(ctx, requestID, traceID, runnerEvents, output)
	return ctx.Err()
}

func (dispatcher *Dispatcher) finishAuditFailure(requestID, traceID string, runnerEvents <-chan *trpcevent.Event, output chan<- DispatchEvent) {
	drainRunnerEvents(runnerEvents, dispatcher.drainTimeout)
	trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventError, RequestID: requestID, TraceID: traceID, Error: ErrAuditWriteFailed.Error()})
	trySendDispatchEvent(output, DispatchEvent{Type: DispatchEventDone, RequestID: requestID, TraceID: traceID, Status: "error", Done: true})
}

func auditWriteFailure() error {
	return errors.Join(ErrExecution, ErrAuditWriteFailed)
}

func terminalAuditError(err error) string {
	if err == nil {
		return ""
	}
	if IsContextCancellation(err) {
		return string(audit.ErrorCanceled)
	}
	return string(audit.ErrorUnavailable)
}

func (dispatcher *Dispatcher) writeExecutionAudit(ctx context.Context, principal Principal, message InboundMessage, identity tenant.RunnerIdentity, requestID, traceID string, eventType audit.EventType, errorType string) error {
	return dispatcher.writeExecutionAuditRevision(ctx, principal, message, identity, requestID, traceID, eventType, errorType, nil)
}

func (dispatcher *Dispatcher) writeExecutionAuditRevision(ctx context.Context, principal Principal, message InboundMessage, identity tenant.RunnerIdentity, requestID, traceID string, eventType audit.EventType, errorType string, revision *int64) error {
	if dispatcher.auditWriter == nil {
		return nil
	}
	channel := string(principal.Kind())
	if target, ok := principal.RoutingTarget(); ok {
		channel = string(target.Channel)
	}
	event := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: audit.NewEventID(requestID, string(eventType)), EventType: eventType, TenantID: principal.TenantID(), Channel: channel, UserID: message.ExternalUserID, SessionID: identity.SessionID, AgentAppID: principal.AppID(), Revision: revision, ErrorType: errorType, RequestID: requestID, TraceID: traceID, ActorType: string(principal.Kind()), ActorID: principal.SubjectID(), OccurredAt: time.Now().UTC()}
	if _, err := dispatcher.auditWriter.Append(ctx, event); err != nil {
		return err
	}
	return nil
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
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	default:
	}
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

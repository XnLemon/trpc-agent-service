package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/execution"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"github.com/google/uuid"
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

// DispatchConfig configures the Resolver and Runtime Execution boundary.
type DispatchConfig struct {
	Resolver      *PlanResolver
	Registry      *runtimerunner.RunnerRegistry
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

// Dispatcher resolves a fixed plan, prepares the Gateway execution context,
// and adapts runtime events into a bounded, redacted protocol stream.
type Dispatcher struct {
	resolver        *PlanResolver
	executor        *execution.Coordinator
	telemetry       observability.Provider
	metrics         metrics.Catalog
	runtimeStore    dispatchStore
	materializer    *outbox.Materializer
	auditWriter     audit.Writer
	handoffStore    audit.HandoffStore
	attachments     attachment.Reader
	attachmentStore runtimestorage.AttachmentStore
}

type dispatchStore interface {
	runtimestorage.SessionStateStore
	runtimestorage.MessageStore
}

type durableExecution struct {
	store        runtimestorage.MessageStore
	tenantID     string
	eventID      string
	owner        string
	fencingToken int64
	replyTarget  runtimestorage.ReplyTarget
}

// dispatchMetadata contains the trusted identity and correlation data shared
// by Gateway execution phases.
type dispatchMetadata struct {
	principal Principal
	message   InboundMessage
	identity  tenant.RunnerIdentity
	requestID string
	traceID   string
}

// dispatchExecution owns Gateway-side state for one asynchronous execution.
// Gateway consumes the runtime event stream; the runtime Coordinator owns its
// closure.
type dispatchExecution struct {
	metadata        dispatchMetadata
	durable         *durableExecution
	mediaReplies    *servicetool.ReplyCollector
	span            observability.Span
	started         time.Time
	executionEvents <-chan execution.Event
	output          chan<- DispatchEvent
	auditFinalized  bool
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
	executor, err := execution.NewCoordinator(execution.Config{
		Registry: config.Registry, DrainTimeout: config.DrainTimeout, Observability: config.Observability,
	})
	if err != nil {
		return nil, err
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
	return &Dispatcher{resolver: config.Resolver, executor: executor, telemetry: config.Observability, metrics: metrics.New(config.Observability), runtimeStore: config.RuntimeStore, materializer: config.Materializer, auditWriter: config.AuditWriter, handoffStore: config.HandoffStore, attachments: config.Attachments, attachmentStore: config.AttachmentStore}, nil
}

// Ready reports whether both plan resolution and Runner acquisition are ready.
func (dispatcher *Dispatcher) Ready() bool {
	return dispatcher != nil && dispatcher.resolver != nil && dispatcher.resolver.Ready() && dispatcher.executor != nil && dispatcher.executor.Ready()
}

// Dispatch starts one execution and returns a redacted event stream. The
// returned stream owns the Runner lease until it reaches terminal state or the
// caller Context is canceled.
//
//nolint:gocyclo // Dispatch coordinates validation, durable state, audit, lease, and stream lifecycle.
func (dispatcher *Dispatcher) Dispatch(ctx context.Context, request DispatchRequest) (<-chan DispatchEvent, error) {
	if dispatcher == nil || dispatcher.resolver == nil || dispatcher.executor == nil {
		return nil, ErrNotReady
	}
	message, requestID, traceID, err := normalizeDispatchRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	metadata := dispatchMetadata{principal: request.Principal, message: message, requestID: requestID, traceID: traceID}
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
		logDispatchFailure(metadata.principal, metadata.requestID, metadata.traceID, cause)
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
	metadata.identity = identity
	planSnapshot := plan.AgentSnapshot()
	planApp := planSnapshot.App()
	durable, err := dispatcher.claimInboundWithLease(ctx, metadata, durableInboundLeaseForRuntime(planSnapshot.Revision().Runtime))
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
		if err := dispatcher.writeExecutionAuditRevision(ctx, metadata, audit.EventCanarySelected, "", &selectedRevision); err != nil {
			dispatcher.failDurable(durable, err)
			finishWithError(err)
			return nil, auditWriteFailure()
		}
	}
	if err := dispatcher.writeExecutionAudit(ctx, metadata, audit.EventExecutionStarted, ""); err != nil {
		dispatcher.failDurable(durable, err)
		finishWithError(err)
		return nil, auditWriteFailure()
	}
	if err := dispatcher.reserveHandoff(ctx, metadata); err != nil {
		finishWithError(err)
		return nil, auditWriteFailure()
	}
	if request.Accepted != nil {
		select {
		case request.Accepted <- struct{}{}:
		default:
		}
	}
	runnerCtx := ctx
	mediaReplies := servicetool.NewReplyCollector()
	if durable != nil {
		runnerCtx = servicetool.WithExecutionContext(runnerCtx, servicetool.ExecutionContext{
			TenantID: request.Principal.TenantID(), EventID: durable.eventID, RequestID: requestID, TraceID: traceID,
			Attachments: dispatcher.attachmentStore, Replies: mediaReplies,
			Audit: audit.Recorder{Writer: dispatcher.auditWriter, TenantID: request.Principal.TenantID()},
		})
	}
	runnerEvents, err := dispatcher.executor.Execute(runnerCtx, execution.Request{
		Plan: plan, Identity: identity, Message: userMessage, RequestID: requestID, TraceID: traceID,
	})
	if err != nil {
		executionErr := err
		if errors.Is(executionErr, execution.ErrExecution) {
			executionErr = ErrExecution
		}
		dispatcher.failDurable(durable, executionErr)
		eventType, errorType := audit.EventExecutionFailed, string(audit.ErrorUnavailable)
		if IsContextCancellation(err) {
			eventType, errorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
		}
		if auditErr := dispatcher.writeExecutionAudit(context.Background(), metadata, eventType, errorType); auditErr != nil {
			dispatcher.failDurable(durable, auditErr)
			finishWithError(auditErr)
			return nil, auditWriteFailure()
		}
		if IsContextCancellation(err) {
			finishWithError(err)
			return nil, err
		}
		finishWithError(executionErr)
		return nil, executionErr
	}

	output := make(chan DispatchEvent, 32)
	run := &dispatchExecution{
		metadata: metadata, durable: durable, mediaReplies: mediaReplies, span: span, started: started,
		executionEvents: runnerEvents, output: output,
	}
	go dispatcher.forwardExecution(runnerCtx, run)
	return output, nil
}

func (dispatcher *Dispatcher) reserveHandoff(ctx context.Context, metadata dispatchMetadata) error {
	if dispatcher.handoffStore == nil {
		return nil
	}
	_, err := dispatcher.handoffStore.Reserve(ctx, audit.ExecutionHandoff{
		TenantID: metadata.principal.TenantID(), HandoffID: audit.NewEventID(metadata.requestID, "handoff"),
		RequestID: metadata.requestID, TraceID: metadata.traceID, EventID: audit.NewEventID(metadata.requestID, string(audit.EventExecutionStarted)), State: audit.HandoffPending,
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

func (dispatcher *Dispatcher) claimInbound(ctx context.Context, metadata dispatchMetadata) (result *durableExecution, err error) {
	return dispatcher.claimInboundWithLease(ctx, metadata, durableInboundLeaseForRuntime(appmodel.DefaultRuntimePolicy()))
}

func (dispatcher *Dispatcher) claimInboundWithLease(ctx context.Context, metadata dispatchMetadata, leaseDuration time.Duration) (result *durableExecution, err error) {
	if dispatcher.runtimeStore == nil || metadata.principal.Kind() != PrincipalChannel {
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
	target, ok := metadata.principal.RoutingTarget()
	if !ok || metadata.message.ExternalMessageID == "" || len([]rune(metadata.message.ExternalMessageID)) > maxDurableExternalMessageIDRunes {
		return nil, fmt.Errorf("%w: durable Channel messages require an external message ID", ErrInvalid)
	}
	store := dispatcher.runtimeStore
	replyTarget, err := replyTarget(target, metadata.message)
	if err != nil {
		return nil, err
	}
	if err := ensureInboundSession(ctx, store, metadata.principal.TenantID(), metadata.identity.SessionID); err != nil {
		return nil, err
	}
	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{
		TenantID: metadata.principal.TenantID(), EventID: uuid.NewString(), SessionID: metadata.identity.SessionID,
		BindingID: target.BindingID, ExternalMessageID: metadata.message.ExternalMessageID,
		IdempotencyKey: metadata.message.ExternalMessageID,
		ReplyTarget:    replyTarget,
	})
	if err != nil {
		return nil, err
	}
	owner := "gateway-" + uuid.NewString()
	event, err = prepareInboundEvent(ctx, store, inboundEventPreparation{tenantID: metadata.principal.TenantID(), event: event, duplicate: duplicate, owner: owner})
	if err != nil {
		return nil, err
	}
	from := event.Status
	running, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{
		TenantID: metadata.principal.TenantID(), EventID: event.EventID, From: from,
		To: runtimestorage.EventRunning, Owner: owner, LeaseDuration: leaseDuration,
	})
	if err != nil {
		if duplicate && errors.Is(err, runtimestorage.ErrConflict) {
			return nil, ErrDuplicateMessage
		}
		return nil, err
	}
	return &durableExecution{store: store, tenantID: metadata.principal.TenantID(), eventID: event.EventID, owner: owner, fencingToken: running.FencingToken, replyTarget: event.ReplyTarget}, nil
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

func ensureInboundSession(ctx context.Context, store runtimestorage.SessionStateStore, tenantID, sessionID string) error {
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

type inboundEventPreparation struct {
	tenantID  string
	event     runtimestorage.MessageEvent
	duplicate bool
	owner     string
}

func prepareInboundEvent(ctx context.Context, store runtimestorage.MessageStore, preparation inboundEventPreparation) (runtimestorage.MessageEvent, error) {
	if !preparation.duplicate {
		return preparation.event, nil
	}
	event := preparation.event
	if event.Status == runtimestorage.EventRunning && (event.LeaseExpiresAt == nil || event.LeaseExpiresAt.After(time.Now().UTC())) {
		return runtimestorage.MessageEvent{}, ErrDuplicateMessage
	}
	if event.Status == runtimestorage.EventRunning {
		if _, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: preparation.tenantID, EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: preparation.owner}); err != nil {
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

func (dispatcher *Dispatcher) finishDurable(ctx context.Context, metadata dispatchMetadata, durable *durableExecution, terminalErr error, reply string, mediaReplies []servicetool.ReplyIntent) {
	if durable == nil {
		return
	}
	durableCtx := detachedCorrelationContext(ctx, metadata.requestID, metadata.traceID)
	if terminalErr != nil && !IsContextCancellation(terminalErr) {
		reply = durableFailureFallbackReply
		mediaReplies = nil
	}
	segments := 0
	replyID := ""
	if dispatcher.materializer != nil {
		input := outbox.MaterializeInput{TenantID: durable.tenantID, EventID: durable.eventID, ReplyID: durable.eventID, RequestID: metadata.requestID, TraceID: metadata.traceID, TraceParent: observability.TraceParentFromContext(durableCtx), ReplyTarget: durable.replyTarget}
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
					fallback := outbox.MaterializeInput{TenantID: durable.tenantID, EventID: durable.eventID, ReplyID: durable.eventID, RequestID: metadata.requestID, TraceID: metadata.traceID, TraceParent: observability.TraceParentFromContext(durableCtx), Payload: durableFailureFallbackReply, ReplyTarget: durable.replyTarget}
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

type executionForwardState struct {
	terminalErr          error
	terminalEventType    audit.EventType
	terminalErrorType    string
	terminalErrorEmitted bool
	terminalSeen         bool
	reply                strings.Builder
}

func (state *executionForwardState) observe(event execution.Event) {
	switch event.Type {
	case execution.EventMessage:
		state.reply.WriteString(event.Text)
	case execution.EventError:
		state.terminalErr = event.Err
		if IsContextCancellation(state.terminalErr) {
			state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
			return
		}
		if state.terminalErr == nil {
			state.terminalErr = ErrExecution
		}
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionFailed, string(audit.ErrorUnavailable)
	}
	if event.Done {
		state.terminalSeen = true
	}
	if !event.Done || state.terminalErr != nil {
		return
	}
	switch event.Status {
	case "error":
		state.terminalErr = ErrExecution
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionFailed, string(audit.ErrorUnavailable)
	case "canceled":
		state.terminalErr = context.Canceled
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	case "deadline_exceeded":
		state.terminalErr = context.DeadlineExceeded
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	default:
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCompleted, ""
	}
}

func (state *executionForwardState) skip(event execution.Event) bool {
	return event.Type == execution.EventDone || (event.Type == execution.EventError && IsContextCancellation(event.Err))
}

func (state *executionForwardState) markSendFailure(ctx context.Context) {
	state.terminalErr = ctx.Err()
	if state.terminalErr == nil {
		state.terminalErr = context.Canceled
	}
	state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	state.terminalErrorEmitted = false
}

func (state *executionForwardState) ensureTerminal(ctx context.Context) {
	if state.terminalSeen || state.terminalErr != nil {
		return
	}
	if ctx.Err() != nil {
		state.terminalErr = ctx.Err()
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
		return
	}
	state.terminalEventType, state.terminalErrorType = audit.EventExecutionCompleted, ""
}

func (dispatcher *Dispatcher) forwardExecution(ctx context.Context, run *dispatchExecution) {
	defer close(run.output)

	state := executionForwardState{}
	for event := range run.executionEvents {
		state.observe(event)
		if state.skip(event) {
			continue
		}
		mapped := mapExecutionEvent(event)
		if mapped.Type != DispatchEventMessage && mapped.Type != DispatchEventStatus && mapped.Type != DispatchEventError {
			continue
		}
		if mapped.Type == DispatchEventError {
			state.terminalErrorEmitted = true
		}
		if !sendDispatchEvent(ctx, run.output, mapped) {
			state.markSendFailure(ctx)
			break
		}
	}
	state.ensureTerminal(ctx)
	mediaIntents := []servicetool.ReplyIntent(nil)
	if run.mediaReplies != nil {
		mediaIntents = run.mediaReplies.Intents()
	}
	terminalErr := dispatcher.finalizeForward(ctx, run, &state, mediaIntents)
	run.finishForwardOutput(ctx, terminalErr, state.terminalErrorEmitted)
	if terminalErr != nil {
		class := observability.ErrorClass(terminalErr)
		run.span.SetAttributes(observability.Attribute{Key: "error_class", Value: class})
		run.span.SetStatus(observability.StatusError, class)
		run.span.RecordError(terminalErr)
	} else {
		run.span.SetStatus(observability.StatusOK, "")
	}
	_ = dispatcher.metrics.Operation(ctx, run.started, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch}, terminalErr)
	logDispatchFailure(run.metadata.principal, run.metadata.requestID, run.metadata.traceID, terminalErr)
	run.span.End()
}

func mapExecutionEvent(event execution.Event) DispatchEvent {
	result := DispatchEvent{Type: DispatchEventType(event.Type), RequestID: event.RequestID, TraceID: event.TraceID, Text: event.Text, Status: event.Status, Done: event.Done}
	if event.Type == execution.EventError {
		if IsContextCancellation(event.Err) {
			result.Error = ErrExecutionCanceled.Error()
		} else {
			result.Error = ErrExecution.Error()
		}
	}
	return result
}

func (run *dispatchExecution) finishForwardOutput(ctx context.Context, terminalErr error, terminalErrorEmitted bool) {
	if IsContextCancellation(terminalErr) {
		if !terminalErrorEmitted {
			trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventError, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Error: ErrExecutionCanceled.Error()})
		}
		trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventDone, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Status: cancellationStatus(ctx), Done: true})
		return
	}
	if terminalErr != nil {
		if !terminalErrorEmitted {
			errorText := ErrExecution.Error()
			if errors.Is(terminalErr, ErrAuditWriteFailed) {
				errorText = ErrAuditWriteFailed.Error()
			}
			trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventError, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Error: errorText})
		}
		trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventDone, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Status: "error", Done: true})
		return
	}
	trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventDone, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Status: "complete", Done: true})
}

func (dispatcher *Dispatcher) finalizeForward(ctx context.Context, run *dispatchExecution, state *executionForwardState, mediaReplies []servicetool.ReplyIntent) error {
	terminalErr := state.terminalErr
	eventType := state.terminalEventType
	errorType := state.terminalErrorType
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
		if _, err := dispatcher.handoffStore.Finalize(detachedCorrelationContext(ctx, run.metadata.requestID, run.metadata.traceID), audit.ExecutionHandoff{TenantID: run.metadata.principal.TenantID(), HandoffID: audit.NewEventID(run.metadata.requestID, "handoff"), State: audit.HandoffFinalized, Result: result, ErrorType: errorType}); err != nil && (terminalErr == nil || IsContextCancellation(terminalErr)) {
			terminalErr = auditWriteFailure()
		}
	}
	if err := dispatcher.finalizeExecutionAudit(ctx, run, eventType, errorType); err != nil && (terminalErr == nil || IsContextCancellation(terminalErr)) {
		terminalErr = auditWriteFailure()
	}
	dispatcher.finishDurable(ctx, run.metadata, run.durable, terminalErr, state.reply.String(), mediaReplies)
	return terminalErr
}

func (dispatcher *Dispatcher) finalizeExecutionAudit(ctx context.Context, run *dispatchExecution, eventType audit.EventType, errorType string) error {
	if run.auditFinalized {
		return nil
	}
	err := dispatcher.writeExecutionAudit(detachedCorrelationContext(ctx, run.metadata.requestID, run.metadata.traceID), run.metadata, eventType, errorType)
	if err == nil {
		run.auditFinalized = true
	}
	return err
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

func (dispatcher *Dispatcher) writeExecutionAudit(ctx context.Context, metadata dispatchMetadata, eventType audit.EventType, errorType string) error {
	return dispatcher.writeExecutionAuditRevision(ctx, metadata, eventType, errorType, nil)
}

func (dispatcher *Dispatcher) writeExecutionAuditRevision(ctx context.Context, metadata dispatchMetadata, eventType audit.EventType, errorType string, revision *int64) error {
	if dispatcher.auditWriter == nil {
		return nil
	}
	channel := string(metadata.principal.Kind())
	if target, ok := metadata.principal.RoutingTarget(); ok {
		channel = string(target.Channel)
	}
	event := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: audit.NewEventID(metadata.requestID, string(eventType)), EventType: eventType, TenantID: metadata.principal.TenantID(), Channel: channel, UserID: metadata.message.ExternalUserID, SessionID: metadata.identity.SessionID, AgentAppID: metadata.principal.AppID(), Revision: revision, ErrorType: errorType, RequestID: metadata.requestID, TraceID: metadata.traceID, ActorType: string(metadata.principal.Kind()), ActorID: metadata.principal.SubjectID(), OccurredAt: time.Now().UTC()}
	if _, err := dispatcher.auditWriter.Append(ctx, event); err != nil {
		return err
	}
	return nil
}

func cancellationStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "canceled"
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

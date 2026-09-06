package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimequeue "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

const (
	// ExecutionTaskKind identifies the versioned Gateway-to-Worker payload
	// contract persisted in the durable queue.
	ExecutionTaskKind = "agent.execution.v1"

	executionTaskPayloadVersion = 1
	maxExecutionTaskPayloadSize = 1 << 20
)

var (
	// ErrInvalidExecutionTask reports a malformed or stale durable task.
	ErrInvalidExecutionTask = errors.New("invalid execution task")
)

type executionTaskPayload struct {
	Version        int            `json:"version"`
	TenantID       string         `json:"tenant_id"`
	AppID          string         `json:"app_id"`
	BindingID      string         `json:"binding_id"`
	BindingVersion int64          `json:"binding_version"`
	ConfigDigest   string         `json:"config_digest"`
	RequestID      string         `json:"request_id"`
	TraceID        string         `json:"trace_id,omitempty"`
	PrincipalKind  PrincipalKind  `json:"principal_kind"`
	EventID        string         `json:"event_id"`
	Message        InboundMessage `json:"message"`
}

// Enqueue persists one verified Channel request for an independent Agent
// Worker. It resolves the current plan before admission, records the inbound
// lifecycle in Received, and signals Accepted only after the queue write.
func (dispatcher *Dispatcher) Enqueue(ctx context.Context, request DispatchRequest) (EnqueueResult, error) {
	if dispatcher == nil || dispatcher.resolver == nil || dispatcher.executionQueue == nil || dispatcher.runtimeStore == nil {
		return EnqueueResult{}, ErrNotReady
	}
	message, requestID, traceID, err := normalizeDispatchRequest(ctx, request)
	if err != nil {
		return EnqueueResult{}, err
	}
	if request.Principal.Kind() != PrincipalChannel {
		return EnqueueResult{}, fmt.Errorf("%w: only Channel requests can be enqueued", ErrInvalid)
	}
	target, ok := request.Principal.RoutingTarget()
	if !ok || message.ExternalMessageID == "" {
		return EnqueueResult{}, fmt.Errorf("%w: durable Channel messages require a verified route and external message ID", ErrInvalid)
	}
	if _, err := dispatcher.resolver.Resolve(ctx, request.Principal); err != nil {
		return EnqueueResult{}, err
	}
	event, err := dispatcher.prepareQueuedEvent(ctx, request.Principal, target, message)
	if err != nil {
		return EnqueueResult{}, err
	}
	payload, err := marshalExecutionTask(executionTaskPayload{
		Version: executionTaskPayloadVersion, TenantID: request.Principal.TenantID(), AppID: request.Principal.AppID(),
		BindingID: target.BindingID, BindingVersion: target.BindingVersion, ConfigDigest: target.ConfigDigest,
		RequestID: requestID, TraceID: traceID, PrincipalKind: PrincipalChannel, EventID: event.EventID, Message: message,
	})
	if err != nil {
		return EnqueueResult{}, ErrInvalidExecutionTask
	}
	task, err := dispatcher.enqueueExecutionTask(ctx, event, payload)
	if err != nil {
		return EnqueueResult{}, err
	}
	notifyAccepted(request.Accepted)
	return EnqueueResult{TaskID: task.TaskID}, nil
}

func (dispatcher *Dispatcher) prepareQueuedEvent(ctx context.Context, principal Principal, target channels.RoutingTarget, message InboundMessage) (runtimestorage.MessageEvent, error) {
	identity, err := dispatchRunnerIdentity(principal, message)
	if err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	reply, err := replyTarget(target, message)
	if err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if err := ensureInboundSession(ctx, dispatcher.runtimeStore, principal.TenantID(), identity.SessionID); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	event, duplicate, err := dispatcher.runtimeStore.RecordMessage(ctx, runtimestorage.MessageEventInput{
		TenantID: principal.TenantID(), EventID: uuid.NewString(), SessionID: identity.SessionID,
		BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID,
		IdempotencyKey: message.ExternalMessageID, ReplyTarget: reply,
	})
	if err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	event, err = prepareInboundEvent(ctx, dispatcher.runtimeStore, inboundEventPreparation{
		tenantID: principal.TenantID(), event: event, duplicate: duplicate,
		owner: "enqueue-" + uuid.NewString(),
	})
	if err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if len(message.Attachments) == 0 {
		return event, nil
	}
	binder, ok := dispatcher.attachments.(attachment.Binder)
	if !ok {
		return runtimestorage.MessageEvent{}, ErrExecution
	}
	if err := binder.BindAttachments(ctx, principal.TenantID(), event.EventID, message.Attachments); err != nil {
		return runtimestorage.MessageEvent{}, ErrExecution
	}
	return event, nil
}

func marshalExecutionTask(payload executionTaskPayload) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil || len(encoded) > maxExecutionTaskPayloadSize {
		return nil, ErrInvalidExecutionTask
	}
	return encoded, nil
}

func (dispatcher *Dispatcher) enqueueExecutionTask(ctx context.Context, event runtimestorage.MessageEvent, payload []byte) (runtimequeue.Task, error) {
	task, _, err := dispatcher.executionQueue.Enqueue(ctx, runtimequeue.TaskInput{
		TenantID: event.TenantID, TaskID: event.EventID, Kind: ExecutionTaskKind, Payload: payload,
	})
	if err == nil {
		return task, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return runtimequeue.Task{}, err
	}
	// A prior accepted queue row is authoritative for this durable event.
	// This handles a client retry whose HTTP correlation ID differs from
	// the first enqueue without replacing the original payload.
	existing, lookupErr := dispatcher.executionQueue.Get(context.Background(), event.TenantID, event.EventID)
	if lookupErr != nil || existing.Kind != ExecutionTaskKind || string(existing.Payload) != string(payload) {
		return runtimequeue.Task{}, ErrExecution
	}
	return existing, nil
}

func notifyAccepted(accepted chan<- struct{}) {
	if accepted == nil {
		return
	}
	select {
	case accepted <- struct{}{}:
	default:
	}
}

func decodeExecutionTask(task runtimequeue.Task) (executionTaskPayload, error) {
	if task.Kind != ExecutionTaskKind || len(task.Payload) == 0 || len(task.Payload) > maxExecutionTaskPayloadSize {
		return executionTaskPayload{}, ErrInvalidExecutionTask
	}
	decoder := json.NewDecoder(bytes.NewReader(task.Payload))
	decoder.DisallowUnknownFields()
	var payload executionTaskPayload
	if err := decoder.Decode(&payload); err != nil {
		return executionTaskPayload{}, ErrInvalidExecutionTask
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return executionTaskPayload{}, ErrInvalidExecutionTask
	}
	if !validExecutionTaskIdentity(task, payload) {
		return executionTaskPayload{}, ErrInvalidExecutionTask
	}
	message, err := payload.Message.Normalize()
	if err != nil || message.ExternalMessageID == "" {
		return executionTaskPayload{}, ErrInvalidExecutionTask
	}
	payload.Message = message
	if _, err := normalizeCorrelationID(payload.RequestID, false); err != nil {
		return executionTaskPayload{}, ErrInvalidExecutionTask
	}
	if _, err := normalizeCorrelationID(payload.TraceID, false); err != nil {
		return executionTaskPayload{}, ErrInvalidExecutionTask
	}
	return payload, nil
}

func validExecutionTaskIdentity(task runtimequeue.Task, payload executionTaskPayload) bool {
	return payload.Version == executionTaskPayloadVersion && payload.PrincipalKind == PrincipalChannel &&
		strings.TrimSpace(payload.TenantID) != "" && strings.TrimSpace(payload.EventID) != "" &&
		payload.TenantID == task.TenantID && payload.EventID == task.TaskID &&
		strings.TrimSpace(payload.AppID) != "" && strings.TrimSpace(payload.BindingID) != "" &&
		payload.BindingVersion >= 1 && strings.TrimSpace(payload.ConfigDigest) != "" && strings.TrimSpace(payload.RequestID) != ""
}

// HandleExecutionTask is the Bootstrap-owned queue Handler. It rehydrates a
// current trusted Channel target, claims the message lease, and runs the same
// audit/outbox finalization path as synchronous Dispatch.
func (dispatcher *Dispatcher) HandleExecutionTask(ctx context.Context, task runtimequeue.Task) error {
	if dispatcher == nil || dispatcher.resolver == nil || dispatcher.runtimeStore == nil || dispatcher.channels == nil || dispatcher.tenants == nil || dispatcher.apps == nil {
		return ErrNotReady
	}
	if ctx == nil {
		return ErrInvalidExecutionTask
	}
	payload, err := decodeExecutionTask(task)
	if err != nil {
		return err
	}
	event, err := dispatcher.runtimeStore.GetMessage(ctx, payload.TenantID, payload.EventID)
	if err != nil {
		if errors.Is(err, runtimestorage.ErrNotFound) {
			return err
		}
		return runtimequeue.Retry(err)
	}
	if event.EventID != payload.EventID || event.BindingID != payload.BindingID || event.ExternalMessageID != payload.Message.ExternalMessageID {
		return ErrInvalidExecutionTask
	}
	if terminalMessageStatus(event.Status) {
		return nil
	}
	principal, err := dispatcher.rehydrateTaskPrincipal(ctx, payload)
	if err != nil {
		return dispatcher.rejectUnclaimedTask(ctx, event, err)
	}
	plan, err := dispatcher.resolver.Resolve(ctx, principal)
	if err != nil {
		return runtimequeue.Retry(err)
	}
	identity, err := dispatchRunnerIdentity(principal, payload.Message)
	if err != nil {
		return dispatcher.rejectUnclaimedTask(ctx, event, err)
	}
	durable, err := dispatcher.claimQueuedInbound(ctx, event, payload, durableInboundLeaseForRuntime(plan.AgentSnapshot().Revision().Runtime))
	if err != nil {
		return err
	}
	return dispatcher.executeQueuedTask(ctx, payload, principal, plan, identity, durable)
}

func (dispatcher *Dispatcher) executeQueuedTask(ctx context.Context, payload executionTaskPayload, principal Principal, plan runtime.ExecutionPlan, identity tenant.RunnerIdentity, durable *durableExecution) error {
	metadata := dispatchMetadata{principal: principal, message: payload.Message, identity: identity, requestID: payload.RequestID, traceID: payload.TraceID}
	base := observability.WithCorrelation(ctx, payload.RequestID, payload.TraceID)
	executionCtx, span := dispatcher.telemetry.Tracer("trpcservice.gateway").Start(base, observability.OperationGatewayDispatch,
		observability.Attribute{Key: "component", Value: "gateway"}, observability.Attribute{Key: "operation", Value: observability.OperationGatewayDispatch},
		observability.Attribute{Key: "execution_mode", Value: "worker"})
	started := time.Now()
	_ = dispatcher.metrics.Request(executionCtx, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch, "status": "started"})
	timeout := time.Duration(plan.AgentSnapshot().Revision().Runtime.ExecutionTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(300) * time.Second
	}
	executionCtx, cancel := context.WithTimeout(executionCtx, timeout)
	defer cancel()
	userMessage, err := buildUserMessage(executionCtx, dispatcher.attachments, principal.TenantID(), durable.eventID, payload.Message)
	if err != nil {
		dispatcher.failDurable(durable, err)
		finishWorkerExecution(span, dispatcher, metadata, started, err)
		return dispatcher.finishQueuedTask(payload.TenantID, durable.eventID)
	}
	output, err := dispatcher.startExecution(executionCtx, metadata, plan, identity, userMessage, durable, span, started, nil)
	if err != nil {
		finishWorkerExecution(span, dispatcher, metadata, started, err)
		return dispatcher.finishQueuedTask(payload.TenantID, durable.eventID)
	}
	for range output {
	}
	return dispatcher.finishQueuedTask(payload.TenantID, durable.eventID)
}

func (dispatcher *Dispatcher) rejectUnclaimedTask(ctx context.Context, event runtimestorage.MessageEvent, cause error) error {
	if IsContextCancellation(cause) {
		return cause
	}
	dispatcher.failUnclaimedEvent(event)
	return cause
}

func (dispatcher *Dispatcher) failUnclaimedEvent(event runtimestorage.MessageEvent) {
	if dispatcher == nil || dispatcher.runtimeStore == nil || terminalMessageStatus(event.Status) {
		return
	}
	owner := "worker-reject-" + uuid.NewString()
	_ = dispatcher.observeStorage(context.Background(), func(operationCtx context.Context) error {
		if event.Status == runtimestorage.EventRunning {
			if event.LeaseExpiresAt == nil || event.LeaseExpiresAt.After(time.Now().UTC()) {
				return nil
			}
			if _, err := dispatcher.runtimeStore.TransitionMessage(operationCtx, runtimestorage.MessageTransition{
				TenantID: event.TenantID, EventID: event.EventID, From: runtimestorage.EventRunning,
				To: runtimestorage.EventExecutionReconciling, Owner: owner,
			}); err != nil {
				return err
			}
			event.Status = runtimestorage.EventExecutionReconciling
		}
		if event.Status != runtimestorage.EventReceived && event.Status != runtimestorage.EventExecutionReconciling {
			return nil
		}
		_, err := dispatcher.runtimeStore.TransitionMessage(operationCtx, runtimestorage.MessageTransition{
			TenantID: event.TenantID, EventID: event.EventID, From: event.Status,
			To: runtimestorage.EventFailed, Owner: owner,
		})
		return err
	})
}

func (dispatcher *Dispatcher) rehydrateTaskPrincipal(ctx context.Context, payload executionTaskPayload) (Principal, error) {
	target, err := channels.ResolveConfiguredRoutingTarget(ctx, dispatcher.channels, dispatcher.tenants, dispatcher.apps, payload.TenantID, payload.BindingID)
	if err != nil {
		return Principal{}, err
	}
	if target.TenantID != payload.TenantID || target.AppID != payload.AppID || target.BindingVersion != payload.BindingVersion || target.ConfigDigest != payload.ConfigDigest {
		return Principal{}, ErrInvalidExecutionTask
	}
	return NewChannelPrincipal(target)
}

func (dispatcher *Dispatcher) claimQueuedInbound(ctx context.Context, event runtimestorage.MessageEvent, payload executionTaskPayload, leaseDuration time.Duration) (*durableExecution, error) {
	owner := "worker-" + uuid.NewString()
	if event.Status == runtimestorage.EventRunning {
		if event.LeaseExpiresAt == nil || event.LeaseExpiresAt.After(time.Now().UTC()) {
			return nil, runtimequeue.Retry(ErrDuplicateMessage)
		}
		var err error
		event, err = dispatcher.runtimeStore.TransitionMessage(ctx, runtimestorage.MessageTransition{
			TenantID: payload.TenantID, EventID: payload.EventID, From: runtimestorage.EventRunning,
			To: runtimestorage.EventExecutionReconciling, Owner: owner,
		})
		if err != nil {
			if errors.Is(err, runtimestorage.ErrConflict) {
				return nil, runtimequeue.Retry(err)
			}
			return nil, err
		}
	}
	if event.Status != runtimestorage.EventReceived && event.Status != runtimestorage.EventExecutionReconciling {
		return nil, ErrInvalidExecutionTask
	}
	running, err := dispatcher.runtimeStore.TransitionMessage(ctx, runtimestorage.MessageTransition{
		TenantID: payload.TenantID, EventID: payload.EventID, From: event.Status,
		To: runtimestorage.EventRunning, Owner: owner, LeaseDuration: leaseDuration,
	})
	if err != nil {
		if errors.Is(err, runtimestorage.ErrConflict) {
			return nil, runtimequeue.Retry(err)
		}
		return nil, err
	}
	return &durableExecution{store: dispatcher.runtimeStore, tenantID: payload.TenantID, eventID: payload.EventID, owner: owner, fencingToken: running.FencingToken, replyTarget: running.ReplyTarget}, nil
}

func (dispatcher *Dispatcher) finishQueuedTask(tenantID, eventID string) error {
	event, err := dispatcher.runtimeStore.GetMessage(context.Background(), tenantID, eventID)
	if err != nil {
		if errors.Is(err, runtimestorage.ErrNotFound) {
			return err
		}
		return runtimequeue.Retry(err)
	}
	if terminalMessageStatus(event.Status) {
		return nil
	}
	return runtimequeue.Retry(ErrExecution)
}

func terminalMessageStatus(status string) bool {
	switch status {
	case runtimestorage.EventCompleted, runtimestorage.EventFailed, runtimestorage.EventReplyPending, runtimestorage.EventReplied:
		return true
	default:
		return false
	}
}

func finishWorkerExecution(span observability.Span, dispatcher *Dispatcher, metadata dispatchMetadata, started time.Time, cause error) {
	if span == nil {
		return
	}
	class := observability.ErrorClass(cause)
	span.SetAttributes(observability.Attribute{Key: "error_class", Value: class})
	span.SetStatus(observability.StatusError, class)
	span.RecordError(cause)
	span.End()
	_ = dispatcher.metrics.Operation(context.Background(), started, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch}, cause)
	logDispatchFailure(metadata.principal, metadata.requestID, metadata.traceID, cause)
}

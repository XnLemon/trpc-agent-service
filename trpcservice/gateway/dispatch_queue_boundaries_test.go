package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/execution"
	runtimequeue "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestExecutionTaskCodecRejectsMalformedPayloads(t *testing.T) {
	base := executionTaskPayload{
		Version: executionTaskPayloadVersion, TenantID: "tenant", AppID: "app", BindingID: "binding",
		BindingVersion: 1, ConfigDigest: "digest", RequestID: "request", TraceID: "trace", PrincipalKind: PrincipalChannel,
		EventID: "event", Message: InboundMessage{ExternalMessageID: "external", ExternalUserID: "user", Content: "hello", ContentType: ContentTypeText, ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	validTask := runtimequeue.Task{TenantID: "tenant", TaskID: "event", Kind: ExecutionTaskKind, Payload: encoded}
	if decoded, err := decodeExecutionTask(validTask); err != nil || decoded.EventID != "event" || decoded.Message.Content != "hello" {
		t.Fatalf("valid execution task = %+v, err=%v", decoded, err)
	}
	unknownField := []byte(strings.TrimSuffix(string(encoded), "}") + ",\"unknown\":true}")
	trailingJSON := append(append([]byte(nil), encoded...), []byte("{}")...)
	tooLarge := runtimequeue.Task{TenantID: "tenant", TaskID: "event", Kind: ExecutionTaskKind, Payload: make([]byte, maxExecutionTaskPayloadSize+1)}
	cases := map[string]runtimequeue.Task{
		"wrong kind":        {TenantID: "tenant", TaskID: "event", Kind: "other", Payload: encoded},
		"empty payload":     {TenantID: "tenant", TaskID: "event", Kind: ExecutionTaskKind},
		"too large":         tooLarge,
		"malformed json":    {TenantID: "tenant", TaskID: "event", Kind: ExecutionTaskKind, Payload: []byte("{")},
		"unknown field":     {TenantID: "tenant", TaskID: "event", Kind: ExecutionTaskKind, Payload: unknownField},
		"trailing json":     {TenantID: "tenant", TaskID: "event", Kind: ExecutionTaskKind, Payload: trailingJSON},
		"invalid message":   taskWithPayload(base, func(value *executionTaskPayload) { value.Message.ExternalMessageID = "" }),
		"invalid request":   taskWithPayload(base, func(value *executionTaskPayload) { value.RequestID = "request\nvalue" }),
		"invalid trace":     taskWithPayload(base, func(value *executionTaskPayload) { value.TraceID = "trace\nvalue" }),
		"wrong version":     taskWithPayload(base, func(value *executionTaskPayload) { value.Version = 2 }),
		"wrong principal":   taskWithPayload(base, func(value *executionTaskPayload) { value.PrincipalKind = PrincipalAPI }),
		"wrong task tenant": {TenantID: "other", TaskID: "event", Kind: ExecutionTaskKind, Payload: encoded},
	}
	for name, task := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeExecutionTask(task); !errors.Is(err, ErrInvalidExecutionTask) {
				t.Fatalf("decode error = %v", err)
			}
		})
	}
	if _, err := marshalExecutionTask(executionTaskPayload{RequestID: strings.Repeat("x", maxExecutionTaskPayloadSize)}); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("oversized marshal error = %v", err)
	}
	if sameExecutionTaskIdentity(validTask, []byte("{")) {
		t.Fatal("malformed candidate payload matched an existing task identity")
	}
	accepted := make(chan struct{}, 1)
	accepted <- struct{}{}
	notifyAccepted(accepted)
	if len(accepted) != 1 {
		t.Fatalf("notifyAccepted replaced a pending signal: len=%d", len(accepted))
	}
}

func TestEnqueueExecutionTaskPreservesAcceptedPayloadAndCancellation(t *testing.T) {
	event := runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event"}
	payload := []byte(`{"version":1}`)
	canceled := &executionQueueStoreStub{enqueueErr: context.Canceled}
	if _, err := (&Dispatcher{executionQueue: canceled}).enqueueExecutionTask(context.Background(), event, payload); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled queue write = %v", err)
	}
	accepted := runtimequeue.Task{TenantID: event.TenantID, TaskID: event.EventID, Kind: ExecutionTaskKind, Payload: payload}
	fallback := &executionQueueStoreStub{enqueueErr: errors.New("already accepted"), task: accepted}
	if got, err := (&Dispatcher{executionQueue: fallback}).enqueueExecutionTask(context.Background(), event, payload); err != nil || got.TaskID != event.EventID {
		t.Fatalf("matching queue replay = %+v, err=%v", got, err)
	}
	missing := &executionQueueStoreStub{enqueueErr: errors.New("queue unavailable"), getErr: runtimequeue.ErrNotFound}
	if _, err := (&Dispatcher{executionQueue: missing}).enqueueExecutionTask(context.Background(), event, payload); !errors.Is(err, ErrExecution) {
		t.Fatalf("missing queue replay = %v", err)
	}
}

func TestDispatcherEnqueueRejectsAdmissionBoundaries(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, _ := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	store := &queueMessageStoreStub{}
	queue := runtimequeue.NewMemory()
	defer queue.Close()
	dispatcher := &Dispatcher{resolver: resolver, runtimeStore: store, executionQueue: queue}
	message := InboundMessage{Content: "hello", ContentType: ContentTypeText, ExternalMessageID: "external", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}
	request := DispatchRequest{Principal: principal, Message: message}
	var nilDispatcher *Dispatcher
	if _, err := nilDispatcher.Enqueue(context.Background(), request); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil dispatcher enqueue = %v", err)
	}
	if _, err := dispatcher.Enqueue(nil, request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil enqueue context = %v", err)
	}
	apiPrincipal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	if _, err := dispatcher.Enqueue(context.Background(), DispatchRequest{Principal: apiPrincipal, Message: message}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("API enqueue = %v", err)
	}
	missingExternalID := request
	missingExternalID.Message.ExternalMessageID = ""
	if _, err := dispatcher.Enqueue(context.Background(), missingExternalID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing external ID enqueue = %v", err)
	}
	store.getSessionErr = errors.New("session unavailable")
	if _, err := dispatcher.Enqueue(context.Background(), request); err == nil || !strings.Contains(err.Error(), "session unavailable") {
		t.Fatalf("session admission error = %v", err)
	}
	store.getSessionErr = nil
	badConfig := resolverTestConfig(fixture)
	badConfig.Tenants = failingTenantRepository{err: errors.New("tenant unavailable")}
	badResolver, err := NewPlanResolver(badConfig)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.resolver = badResolver
	if _, err := dispatcher.Enqueue(context.Background(), request); !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("plan admission error = %v", err)
	}

	dispatcher.resolver = resolver
	dispatcher.executionQueue = &executionQueueStoreStub{enqueueErr: errors.New("queue unavailable"), getErr: runtimequeue.ErrNotFound}
	if _, err := dispatcher.Enqueue(context.Background(), request); !errors.Is(err, ErrExecution) {
		t.Fatalf("queue admission error = %v", err)
	}
	if store.recordCalls != 0 {
		t.Fatalf("event was recorded after queue admission failure: %d calls", store.recordCalls)
	}
}

func TestDispatcherEnqueueLeavesDurableTaskWhenEventPersistenceFails(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, _ := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	store := &queueMessageStoreStub{recordErr: errors.New("event store unavailable")}
	queue := runtimequeue.NewMemory()
	defer queue.Close()
	dispatcher := &Dispatcher{resolver: resolver, runtimeStore: store, executionQueue: queue}
	request := DispatchRequest{Principal: principal, RequestID: "recovery-request", Message: InboundMessage{
		Content: "hello", ContentType: ContentTypeText, ExternalMessageID: "recovery-external", ExternalUserID: "user",
		ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer",
	}}
	if _, err := dispatcher.Enqueue(context.Background(), request); err == nil || !strings.Contains(err.Error(), "event store unavailable") {
		t.Fatalf("event persistence error = %v", err)
	}
	if store.lastRecordInput.EventID == "" {
		t.Fatal("event persistence did not receive an event identity")
	}
	task, err := queue.Get(context.Background(), fixture.tenant.TenantID, store.lastRecordInput.EventID)
	if err != nil || task.Status != runtimequeue.StatusQueued {
		t.Fatalf("recoverable task = %+v, err=%v", task, err)
	}
}

func TestDispatcherEnqueueAttachmentFailureLeavesDurableEventRecoverable(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, _ := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	store := &queueMessageStoreStub{}
	queue := runtimequeue.NewMemory()
	t.Cleanup(func() { _ = queue.Close() })
	dispatcher := &Dispatcher{resolver: resolver, runtimeStore: store, executionQueue: queue}
	request := DispatchRequest{Principal: principal, Message: InboundMessage{
		Content: "caption", ContentType: ContentTypeMedia, ExternalMessageID: "attachment-external", ExternalUserID: "user",
		ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer",
		Attachments: []attachment.Reference{testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))},
	}}
	if _, err := dispatcher.Enqueue(context.Background(), request); !errors.Is(err, ErrExecution) {
		t.Fatalf("attachment admission error = %v", err)
	}
	if len(store.transitions) != 1 || store.transitions[0].To != runtimestorage.EventFailed {
		t.Fatalf("attachment admission transitions = %+v", store.transitions)
	}
	task, err := queue.Get(context.Background(), principal.TenantID(), store.lastRecordInput.EventID)
	if err != nil || task.Status != runtimequeue.StatusQueued {
		t.Fatalf("recoverable attachment task = %+v, err=%v", task, err)
	}
}

func TestDispatcherQueueAdmissionDuplicateAndRecoveryBoundaries(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, _ := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	message := InboundMessage{Content: "hello", ContentType: ContentTypeText, ExternalMessageID: "duplicate-external", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}
	store := &queueMessageStoreStub{recordDuplicate: true}
	queue := runtimequeue.NewMemory()
	t.Cleanup(func() { _ = queue.Close() })
	dispatcher := &Dispatcher{resolver: resolver, runtimeStore: store, executionQueue: queue}
	if _, err := dispatcher.Enqueue(context.Background(), DispatchRequest{Principal: principal, Message: message}); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate queue admission error = %v", err)
	}
	if store.recordCalls != 1 {
		t.Fatalf("duplicate queue admission record calls = %d, want 1", store.recordCalls)
	}

	target := principalMustTarget(t, principal)
	store = &queueMessageStoreStub{
		event:           runtimestorage.MessageEvent{TenantID: fixture.tenant.TenantID, EventID: "existing-event", SessionID: "session", BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID, Status: runtimestorage.EventReceived},
		recordDuplicate: true,
	}
	dispatcher.runtimeStore = store
	if _, err := dispatcher.prepareQueuedEvent(context.Background(), principal, target, message); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate event preparation error = %v", err)
	}

	dispatcher, fixture, principal, store = newTaskHandlerBoundary(t)
	task, payload := taskForPrincipal(t, principal, "recovery-duplicate")
	identity, err := dispatchRunnerIdentity(principal, payload.Message)
	if err != nil {
		t.Fatal(err)
	}
	store.event = runtimestorage.MessageEvent{TenantID: payload.TenantID, EventID: payload.EventID, BindingID: payload.BindingID, ExternalMessageID: payload.Message.ExternalMessageID, Status: runtimestorage.EventReceived}
	store.forceMissingEvent = true
	store.recordDuplicate = true
	if err := dispatcher.HandleExecutionTask(context.Background(), task); err != nil {
		t.Fatalf("duplicate event recovery = %v", err)
	}

	store.forceMissingEvent = false
	store.recordDuplicate = false
	store.event.EventID = "different-event"
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("recovered event identity mismatch = %v", err)
	}

	store.event = runtimestorage.MessageEvent{}
	badPayload := payload
	badPayload.Message.ExternalUserID = ""
	badTask := taskForPayload(t, task, badPayload)
	if err := dispatcher.HandleExecutionTask(context.Background(), badTask); err == nil {
		t.Fatal("invalid runner identity unexpectedly succeeded")
	}

	missingRoute := &queueMessageStoreStub{forceMissingEvent: true}
	dispatcher.runtimeStore = missingRoute
	if _, _, err := dispatcher.ensureQueuedEvent(context.Background(), payload, Principal{kind: PrincipalAPI}, identity); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("missing routing target error = %v", err)
	}
	badReply := payload
	badReply.Message.ConversationKind = "unsupported"
	if _, _, err := dispatcher.ensureQueuedEvent(context.Background(), badReply, principal, identity); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid reply target error = %v", err)
	}
	store = missingRoute
	dispatcher.runtimeStore = store
	store.getSessionErr = errors.New("session unavailable")
	if _, _, err := dispatcher.ensureQueuedEvent(context.Background(), payload, principal, identity); !strings.Contains(err.Error(), "session unavailable") {
		t.Fatalf("missing event session error = %v", err)
	}

	store = &queueMessageStoreStub{event: runtimestorage.MessageEvent{TenantID: payload.TenantID, EventID: payload.EventID, BindingID: payload.BindingID, ExternalMessageID: payload.Message.ExternalMessageID, Status: runtimestorage.EventReceived}}
	dispatcher.runtimeStore = store
	attachments := testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))
	payload.Message.Attachments = []attachment.Reference{attachments}
	task = taskForPayload(t, task, payload)
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, ErrExecution) {
		t.Fatalf("missing attachment binder error = %v", err)
	}
}

func TestEnsureQueuedEventRebuildsMissingEvent(t *testing.T) {
	dispatcher, _, principal, _ := newTaskHandlerBoundary(t)
	task, payload := taskForPrincipal(t, principal, "rebuild-event")
	identity, err := dispatchRunnerIdentity(principal, payload.Message)
	if err != nil {
		t.Fatal(err)
	}
	event, duplicate, err := dispatcher.ensureQueuedEvent(context.Background(), payload, principal, identity)
	if err != nil || duplicate {
		t.Fatalf("rebuilt event = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	if event.TenantID != payload.TenantID || event.EventID != payload.EventID || event.Status != runtimestorage.EventReceived {
		t.Fatalf("rebuilt event identity = %+v", event)
	}
	if task.TaskID != event.EventID {
		t.Fatalf("task/event identity mismatch: task=%s event=%s", task.TaskID, event.EventID)
	}
}

func TestPrepareQueuedEventDefensiveBranches(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, _ := newQueueChannelPrincipal(t, fixture)
	target := principalMustTarget(t, principal)
	reference := testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))
	message := InboundMessage{Content: "hello", ContentType: ContentTypeText, ExternalMessageID: "external", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}
	newStore := func() *queueMessageStoreStub {
		return &queueMessageStoreStub{event: runtimestorage.MessageEvent{TenantID: fixture.tenant.TenantID, EventID: "event", SessionID: "session", BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID, Status: runtimestorage.EventReceived}}
	}
	cases := map[string]struct {
		message     InboundMessage
		store       *queueMessageStoreStub
		attachments attachment.Reader
		wantErr     error
		wantText    string
	}{
		"runner identity": {message: func() InboundMessage { value := message; value.ExternalUserID = ""; return value }(), store: newStore(), wantText: "invalid channel binding"},
		"reply target":    {message: func() InboundMessage { value := message; value.ExternalPeerID = " "; return value }(), store: newStore(), wantErr: ErrInvalid},
		"session lookup": {message: message, store: func() *queueMessageStoreStub {
			value := newStore()
			value.getSessionErr = errors.New("session unavailable")
			return value
		}(), wantText: "session unavailable"},
		"message record": {message: message, store: func() *queueMessageStoreStub {
			value := newStore()
			value.recordErr = errors.New("record unavailable")
			return value
		}(), wantText: "record unavailable"},
		"missing binder": {message: func() InboundMessage {
			value := message
			value.Attachments = []attachment.Reference{reference}
			return value
		}(), store: newStore(), wantErr: ErrExecution},
		"binder failure": {message: func() InboundMessage {
			value := message
			value.Attachments = []attachment.Reference{reference}
			return value
		}(), store: newStore(), attachments: dispatchAttachmentStore{bindFn: func(context.Context, string, string, []attachment.Reference) error { return errors.New("bind failed") }}, wantErr: ErrExecution},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			dispatcher := &Dispatcher{runtimeStore: test.store, attachments: test.attachments}
			_, err := dispatcher.prepareQueuedEvent(context.Background(), principal, target, test.message)
			if test.wantErr != nil && (err == nil || !errors.Is(err, test.wantErr)) {
				t.Fatalf("prepare error = %v, want %v", err, test.wantErr)
			}
			if test.wantText != "" && (err == nil || !strings.Contains(err.Error(), test.wantText)) {
				t.Fatalf("prepare error = %v, want text %q", err, test.wantText)
			}
		})
	}
	store := newStore()
	dispatcher := &Dispatcher{runtimeStore: store, attachments: dispatchAttachmentStore{bindFn: func(context.Context, string, string, []attachment.Reference) error { return nil }}}
	value, err := dispatcher.prepareQueuedEvent(context.Background(), principal, target, func() InboundMessage {
		value := message
		value.Attachments = []attachment.Reference{reference}
		return value
	}())
	if err != nil || value.EventID == "" {
		t.Fatalf("successful attachment preparation = %+v, err=%v", value, err)
	}
}

func TestHandleExecutionTaskRejectsStaleAndUnavailableState(t *testing.T) {
	dispatcher, fixture, principal, store := newTaskHandlerBoundary(t)
	const eventID = "event"
	task, payload := taskForPrincipal(t, principal, eventID)
	store.event = runtimestorage.MessageEvent{TenantID: fixture.tenant.TenantID, EventID: eventID, BindingID: payload.BindingID, ExternalMessageID: payload.Message.ExternalMessageID, Status: runtimestorage.EventReceived}
	var nilDispatcher *Dispatcher
	if err := nilDispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil dispatcher = %v", err)
	}
	if err := dispatcher.HandleExecutionTask(nil, task); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("nil handler context = %v", err)
	}
	if err := dispatcher.HandleExecutionTask(context.Background(), runtimequeue.Task{TenantID: task.TenantID, TaskID: task.TaskID, Kind: ExecutionTaskKind, Payload: []byte("{")}); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("malformed task = %v", err)
	}
	store.getMessageErr = runtimestorage.ErrNotFound
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing message event = %v", err)
	}
	storageErr := errors.New("storage unavailable")
	store.getMessageErr = storageErr
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, storageErr) || !isRetryableQueueError(err) {
		t.Fatalf("storage error = %v", err)
	}
	store.getMessageErr = context.Canceled
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation from event store = %v", err)
	}
	store.getMessageErr = nil
	store.event.BindingID = "other-binding"
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("event identity mismatch = %v", err)
	}
	store.event.BindingID = payload.BindingID
	store.event.Status = runtimestorage.EventCompleted
	if err := dispatcher.HandleExecutionTask(context.Background(), task); err != nil {
		t.Fatalf("terminal message replay = %v", err)
	}

	store.event.Status = runtimestorage.EventReceived
	badResolver := resolverForTaskError(t, fixture)
	dispatcher.resolver = badResolver
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, ErrPlanUnavailable) || !isRetryableQueueError(err) {
		t.Fatalf("plan resolution failure = %v", err)
	}

	dispatcher.resolver = resolverForTaskBoundary(t, fixture)
	payload.AppID = "other-app"
	task = taskForPayload(t, task, payload)
	store.event.Status = runtimestorage.EventReceived
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("stale app task = %v", err)
	}
	payload.AppID = principal.AppID()
	task = taskForPayload(t, task, payload)
	store.event.Status = runtimestorage.EventReceived
	store.transitionErr = runtimestorage.ErrConflict
	if err := dispatcher.HandleExecutionTask(context.Background(), task); !isRetryableQueueError(err) {
		t.Fatalf("claim conflict = %v", err)
	}
	store.transitionErr = errors.New("claim unavailable")
	if err := dispatcher.HandleExecutionTask(context.Background(), task); err == nil || !strings.Contains(err.Error(), "claim unavailable") {
		t.Fatalf("claim failure = %v", err)
	}

	if err := (&Dispatcher{runtimeStore: store}).rejectExecutionTask(context.Background(), payload, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled task rejection = %v", err)
	}
	store.getMessageErr = runtimestorage.ErrNotFound
	if err := (&Dispatcher{runtimeStore: store}).rejectExecutionTask(context.Background(), payload, ErrInvalidExecutionTask); !errors.Is(err, ErrInvalidExecutionTask) {
		t.Fatalf("missing task rejection = %v", err)
	}
	store.getMessageErr = errors.New("rejection lookup failed")
	if err := (&Dispatcher{runtimeStore: store}).rejectExecutionTask(context.Background(), payload, ErrInvalidExecutionTask); !isRetryableQueueError(err) {
		t.Fatalf("lookup task rejection = %v", err)
	}
}

func TestClaimQueuedInboundHandlesLeaseRecoveryAndConflicts(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, _ := newQueueChannelPrincipal(t, fixture)
	task, payload := taskForPrincipal(t, principal, "event")
	_ = task
	expired := time.Now().UTC().Add(-time.Minute)
	future := time.Now().UTC().Add(time.Minute)
	cases := map[string]struct {
		event         runtimestorage.MessageEvent
		transitionErr error
		transitionFn  func(*queueMessageStoreStub, runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error)
		wantRetry     bool
		wantErr       error
		wantText      string
	}{
		"active running":            {event: runtimestorage.MessageEvent{Status: runtimestorage.EventRunning, LeaseExpiresAt: &future}, wantRetry: true, wantErr: ErrDuplicateMessage},
		"expired recovery conflict": {event: runtimestorage.MessageEvent{Status: runtimestorage.EventRunning, LeaseExpiresAt: &expired}, transitionErr: runtimestorage.ErrConflict, wantRetry: true, wantErr: runtimestorage.ErrConflict},
		"expired recovery failure":  {event: runtimestorage.MessageEvent{Status: runtimestorage.EventRunning, LeaseExpiresAt: &expired}, transitionErr: errors.New("recovery failed"), wantText: "recovery failed"},
		"invalid state":             {event: runtimestorage.MessageEvent{Status: runtimestorage.EventCompleted}, wantErr: ErrInvalidExecutionTask},
		"running conflict":          {event: runtimestorage.MessageEvent{Status: runtimestorage.EventReceived}, transitionErr: runtimestorage.ErrConflict, wantRetry: true, wantErr: runtimestorage.ErrConflict},
		"running failure":           {event: runtimestorage.MessageEvent{Status: runtimestorage.EventReceived}, transitionErr: errors.New("claim failed"), wantText: "claim failed"},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			store := &queueMessageStoreStub{event: test.event, transitionErr: test.transitionErr}
			_, err := (&Dispatcher{runtimeStore: store}).claimQueuedInbound(context.Background(), test.event, payload, time.Minute)
			if test.wantRetry && !isRetryableQueueError(err) {
				t.Fatalf("error = %v, want retryable", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if test.wantText != "" && (err == nil || !strings.Contains(err.Error(), test.wantText)) {
				t.Fatalf("error = %v, want text %q", err, test.wantText)
			}
		})
	}
	store := &queueMessageStoreStub{event: runtimestorage.MessageEvent{TenantID: fixture.tenant.TenantID, EventID: payload.EventID, Status: runtimestorage.EventRunning, LeaseExpiresAt: &expired, ReplyTarget: runtimestorage.ReplyTarget{BindingID: payload.BindingID}}}
	store.transitionFn = func(_ context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
		value := store.event
		value.Status = transition.To
		value.FencingToken++
		return value, nil
	}
	durable, err := (&Dispatcher{runtimeStore: store}).claimQueuedInbound(context.Background(), store.event, payload, time.Minute)
	if err != nil || durable == nil || durable.fencingToken == 0 {
		t.Fatalf("recovered durable execution = %+v, err=%v", durable, err)
	}
}

func TestFailUnclaimedAndFinishQueuedTaskBoundaries(t *testing.T) {
	provider := observability.NewNoopProvider()
	store := &queueMessageStoreStub{}
	dispatcher := &Dispatcher{runtimeStore: store, telemetry: provider, metrics: metrics.New(provider)}
	var nilDispatcher *Dispatcher
	nilDispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{})
	dispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{Status: runtimestorage.EventCompleted})
	future := time.Now().UTC().Add(time.Minute)
	dispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{Status: runtimestorage.EventRunning, LeaseExpiresAt: &future})
	if len(store.transitions) != 0 {
		t.Fatalf("active running event was transitioned = %+v", store.transitions)
	}
	dispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event", Status: runtimestorage.EventReceived})
	if len(store.transitions) != 1 || store.transitions[0].To != runtimestorage.EventFailed {
		t.Fatalf("received event transitions = %+v", store.transitions)
	}
	store.transitionErr = errors.New("reject transition failed")
	dispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event", Status: runtimestorage.EventReceived})
	store.transitionErr = nil
	dispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event", Status: "unknown"})
	store.transitions = nil
	expired := time.Now().UTC().Add(-time.Minute)
	store.transitionErr = errors.New("reconcile transition failed")
	dispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event", Status: runtimestorage.EventRunning, LeaseExpiresAt: &expired})
	store.transitionErr = nil
	store.transitions = nil
	dispatcher.failUnclaimedEvent(runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event", Status: runtimestorage.EventRunning, LeaseExpiresAt: &expired})
	if len(store.transitions) != 2 || store.transitions[0].To != runtimestorage.EventExecutionReconciling || store.transitions[1].To != runtimestorage.EventFailed {
		t.Fatalf("expired event transitions = %+v", store.transitions)
	}
	if err := dispatcher.rejectUnclaimedTask(context.Background(), runtimestorage.MessageEvent{}, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rejection = %v", err)
	}

	for name, getErr := range map[string]error{"not found": runtimestorage.ErrNotFound, "storage": errors.New("read failed")} {
		store.getMessageErr = getErr
		err := dispatcher.finishQueuedTask("tenant", "event")
		if name == "not found" && !errors.Is(err, runtimestorage.ErrNotFound) {
			t.Fatalf("finish %s = %v", name, err)
		}
		if name == "storage" && !isRetryableQueueError(err) {
			t.Fatalf("finish %s = %v", name, err)
		}
	}
	store.getMessageErr = nil
	store.event = runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event", Status: runtimestorage.EventCompleted}
	if err := dispatcher.finishQueuedTask("tenant", "event"); err != nil {
		t.Fatalf("finished queue task = %v", err)
	}
	store.event.Status = runtimestorage.EventRunning
	if err := dispatcher.finishQueuedTask("tenant", "event"); !isRetryableQueueError(err) {
		t.Fatalf("unfinished queue task = %v", err)
	}
	_, span := provider.Tracer("test").Start(context.Background(), "worker")
	finishWorkerExecution(span, dispatcher, dispatchMetadata{requestID: "request", traceID: "trace"}, time.Now(), errors.New("worker failed"))
	finishWorkerExecution(nil, dispatcher, dispatchMetadata{}, time.Now(), errors.New("ignored"))
}

func TestPreserveQueuedTaskKeepsWorkerOwnedMessageRecoverable(t *testing.T) {
	cause := runtimequeue.ErrWorkerShutdown
	future := time.Now().UTC().Add(time.Minute)
	store := &queueMessageStoreStub{event: runtimestorage.MessageEvent{
		TenantID: "tenant", EventID: "event", Status: runtimestorage.EventRunning, LeaseExpiresAt: &future,
	}}
	dispatcher := &Dispatcher{runtimeStore: store}
	durable := &durableExecution{tenantID: "tenant", eventID: "event"}
	err := dispatcher.preserveQueuedTask(durable, cause)
	if !isRetryableQueueError(err) || !errors.Is(err, cause) {
		t.Fatalf("active worker task preservation = %v", err)
	}
	if len(store.transitions) != 0 {
		t.Fatalf("active lease was reconciled early = %+v", store.transitions)
	}

	expired := time.Now().UTC().Add(-time.Minute)
	store = &queueMessageStoreStub{event: runtimestorage.MessageEvent{
		TenantID: "tenant", EventID: "event", Status: runtimestorage.EventRunning, LeaseExpiresAt: &expired,
	}}
	store.transitionFn = func(_ context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
		store.event.Status = transition.To
		return store.event, nil
	}
	dispatcher.runtimeStore = store
	err = dispatcher.preserveQueuedTask(durable, runtimequeue.ErrLeaseLost)
	if !isRetryableQueueError(err) || !errors.Is(err, runtimequeue.ErrLeaseLost) {
		t.Fatalf("expired worker task preservation = %v", err)
	}
	if len(store.transitions) != 1 || store.transitions[0].To != runtimestorage.EventExecutionReconciling {
		t.Fatalf("expired lease recovery = %+v", store.transitions)
	}

	store.event.Status = runtimestorage.EventCompleted
	if err := dispatcher.preserveQueuedTask(durable, cause); err != nil {
		t.Fatalf("terminal worker task preservation = %v", err)
	}
	if err := dispatcher.preserveQueuedTask(nil, cause); !isRetryableQueueError(err) || !errors.Is(err, cause) {
		t.Fatalf("nil durable preservation = %v", err)
	}
	store.getMessageErr = errors.New("message store unavailable")
	if err := dispatcher.preserveQueuedTask(durable, cause); !isRetryableQueueError(err) {
		t.Fatalf("message lookup preservation = %v", err)
	}
}

func TestExecuteQueuedTaskHandlesAttachmentAndRunnerFailures(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, _ := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	target := principalMustTarget(t, principal)
	basePayload := executionTaskPayload{Version: executionTaskPayloadVersion, TenantID: principal.TenantID(), AppID: principal.AppID(), BindingID: target.BindingID, BindingVersion: target.BindingVersion, ConfigDigest: target.ConfigDigest, RequestID: "worker-request", PrincipalKind: PrincipalChannel, EventID: "event", Message: InboundMessage{Content: "hello", ContentType: ContentTypeText, ExternalMessageID: "external", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}}
	provider := observability.NewNoopProvider()
	newDispatcherStore := func() (*Dispatcher, *queueMessageStoreStub, *durableExecution) {
		store := &queueMessageStoreStub{event: runtimestorage.MessageEvent{TenantID: principal.TenantID(), EventID: "event", BindingID: target.BindingID, ExternalMessageID: "external", Status: runtimestorage.EventRunning, ReplyTarget: runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: string(channels.ConversationDirect), ReceiverID: "peer"}}}
		store.transitionFn = func(_ context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
			store.event.Status = transition.To
			return store.event, nil
		}
		dispatcher := &Dispatcher{runtimeStore: store, telemetry: provider, metrics: metrics.New(provider)}
		durable := &durableExecution{store: store, tenantID: principal.TenantID(), eventID: "event", owner: "worker", fencingToken: 1, replyTarget: store.event.ReplyTarget}
		return dispatcher, store, durable
	}

	dispatcher, store, durable := newDispatcherStore()
	attachmentReference := testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))
	attachmentPayload := basePayload
	attachmentPayload.Message.Attachments = []attachment.Reference{attachmentReference}
	if err := dispatcher.executeQueuedTask(context.Background(), attachmentPayload, principal, runtime.ExecutionPlan{}, tenant.RunnerIdentity{}, durable); err != nil {
		t.Fatalf("attachment failure execution = %v", err)
	}
	if store.event.Status != runtimestorage.EventFailed {
		t.Fatalf("attachment failure event = %+v", store.event)
	}

	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return nil, errors.New("runner unavailable")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	executor, err := execution.NewCoordinator(execution.Config{Registry: registry, Observability: provider})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, store, durable = newDispatcherStore()
	dispatcher.executor = executor
	identity, err := dispatchRunnerIdentity(principal, basePayload.Message)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.executeQueuedTask(context.Background(), basePayload, principal, plan, identity, durable); err != nil {
		t.Fatalf("runner failure execution = %v", err)
	}
	if store.event.Status != runtimestorage.EventFailed {
		t.Fatalf("runner failure event = %+v", store.event)
	}
}

func taskWithPayload(value executionTaskPayload, mutate func(*executionTaskPayload)) runtimequeue.Task {
	mutated := value
	mutate(&mutated)
	encoded, _ := json.Marshal(mutated)
	return runtimequeue.Task{TenantID: mutated.TenantID, TaskID: mutated.EventID, Kind: ExecutionTaskKind, Payload: encoded}
}

func taskForPrincipal(t *testing.T, principal Principal, eventID string) (runtimequeue.Task, executionTaskPayload) {
	t.Helper()
	target := principalMustTarget(t, principal)
	payload := executionTaskPayload{
		Version: executionTaskPayloadVersion, TenantID: principal.TenantID(), AppID: principal.AppID(), BindingID: target.BindingID,
		BindingVersion: target.BindingVersion, ConfigDigest: target.ConfigDigest, RequestID: "request-" + eventID, PrincipalKind: PrincipalChannel,
		EventID: eventID, Message: InboundMessage{Content: "hello", ContentType: ContentTypeText, ExternalMessageID: "external-" + eventID, ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
	}
	return taskForPayload(t, runtimequeue.Task{TenantID: payload.TenantID, TaskID: eventID, Kind: ExecutionTaskKind}, payload), payload
}

func taskForPayload(t *testing.T, task runtimequeue.Task, payload executionTaskPayload) runtimequeue.Task {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	task.TenantID, task.TaskID, task.Kind, task.Payload = payload.TenantID, payload.EventID, ExecutionTaskKind, encoded
	return task
}

func newTaskHandlerBoundary(t *testing.T) (*Dispatcher, gatewayFixture, Principal, *queueMessageStoreStub) {
	t.Helper()
	fixture := newGatewayFixture(t)
	principal, channelRepo := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	store := &queueMessageStoreStub{}
	provider := observability.NewNoopProvider()
	return &Dispatcher{resolver: resolver, runtimeStore: store, channels: channelRepo, tenants: fixture.tenants, apps: fixture.apps, telemetry: provider, metrics: metrics.New(provider)}, fixture, principal, store
}

func resolverForTaskError(t *testing.T, fixture gatewayFixture) *PlanResolver {
	t.Helper()
	config := resolverTestConfig(fixture)
	config.Tenants = failingTenantRepository{err: errors.New("tenant unavailable")}
	resolver, err := NewPlanResolver(config)
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func resolverForTaskBoundary(t *testing.T, fixture gatewayFixture) *PlanResolver {
	t.Helper()
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func isRetryableQueueError(err error) bool {
	var retryable *runtimequeue.RetryableError
	return errors.As(err, &retryable)
}

type executionQueueStoreStub struct {
	runtimequeue.Store
	enqueueErr error
	task       runtimequeue.Task
	getErr     error
}

func (store *executionQueueStoreStub) Enqueue(context.Context, runtimequeue.TaskInput) (runtimequeue.Task, bool, error) {
	return runtimequeue.Task{}, false, store.enqueueErr
}

func (store *executionQueueStoreStub) Get(context.Context, string, string) (runtimequeue.Task, error) {
	if store.getErr != nil {
		return runtimequeue.Task{}, store.getErr
	}
	return store.task, nil
}

type queueMessageStoreStub struct {
	event             runtimestorage.MessageEvent
	getSessionErr     error
	createErr         error
	recordErr         error
	getMessageErr     error
	transitionErr     error
	transitions       []runtimestorage.MessageTransition
	transitionFn      func(context.Context, runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error)
	recordCalls       int
	lastRecordInput   runtimestorage.MessageEventInput
	recordDuplicate   bool
	forceMissingEvent bool
}

func (store *queueMessageStoreStub) GetSession(context.Context, string, string) (sessionstorage.Session, error) {
	if store.getSessionErr != nil {
		return sessionstorage.Session{}, store.getSessionErr
	}
	return sessionstorage.Session{Status: runtimestorage.SessionActive}, nil
}

func (store *queueMessageStoreStub) CreateSession(context.Context, string, string, map[string]any) (sessionstorage.Session, error) {
	if store.createErr != nil {
		return sessionstorage.Session{}, store.createErr
	}
	return sessionstorage.Session{Status: runtimestorage.SessionActive}, nil
}

func (*queueMessageStoreStub) UpdateSessionState(context.Context, string, string, int64, map[string]any) (sessionstorage.Session, error) {
	return sessionstorage.Session{}, nil
}

func (*queueMessageStoreStub) DeleteSession(context.Context, string, string) error { return nil }

func (store *queueMessageStoreStub) RecordMessage(_ context.Context, input runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	store.recordCalls++
	store.lastRecordInput = input
	if store.recordErr != nil {
		return runtimestorage.MessageEvent{}, false, store.recordErr
	}
	if store.event.EventID == "" {
		store.event = runtimestorage.MessageEvent{TenantID: input.TenantID, EventID: input.EventID, SessionID: input.SessionID, BindingID: input.BindingID, ExternalMessageID: input.ExternalMessageID, Status: runtimestorage.EventReceived, ReplyTarget: input.ReplyTarget}
	}
	return store.event, store.recordDuplicate, nil
}

func (store *queueMessageStoreStub) GetMessage(context.Context, string, string) (runtimestorage.MessageEvent, error) {
	if store.getMessageErr != nil {
		return runtimestorage.MessageEvent{}, store.getMessageErr
	}
	if store.forceMissingEvent {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrNotFound
	}
	if store.event.EventID == "" {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrNotFound
	}
	return store.event, nil
}

func (store *queueMessageStoreStub) TransitionMessage(ctx context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	store.transitions = append(store.transitions, transition)
	if store.transitionFn != nil {
		return store.transitionFn(ctx, transition)
	}
	if store.transitionErr != nil {
		return runtimestorage.MessageEvent{}, store.transitionErr
	}
	value := store.event
	value.Status = transition.To
	value.FencingToken++
	return value, nil
}

var _ dispatchStore = (*queueMessageStoreStub)(nil)
var _ tenant.Repository = failingTenantRepository{}

type failingTenantRepository struct {
	tenant.Repository
	err error
}

func (repository failingTenantRepository) Get(context.Context, string) (*tenant.Tenant, error) {
	return nil, repository.err
}

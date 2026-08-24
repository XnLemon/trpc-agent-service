package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type capturedRun struct {
	mu        sync.Mutex
	userID    string
	sessionID string
	message   trpcmodel.Message
	requestID string
}

type claimStoreStub struct {
	runtimestorage.RuntimeStore
	getErr, createErr, recordErr, transitionErr error
}

func (s *claimStoreStub) GetSession(context.Context, string, string) (runtimestorage.Session, error) {
	if s.getErr != nil {
		return runtimestorage.Session{}, s.getErr
	}
	return s.RuntimeStore.GetSession(context.Background(), "unused", "unused")
}
func (s *claimStoreStub) CreateSession(ctx context.Context, tenantID, sessionID string, state map[string]any) (runtimestorage.Session, error) {
	if s.createErr != nil {
		return runtimestorage.Session{}, s.createErr
	}
	return s.RuntimeStore.CreateSession(ctx, tenantID, sessionID, state)
}
func (s *claimStoreStub) RecordMessage(context.Context, runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if s.recordErr != nil {
		return runtimestorage.MessageEvent{}, false, s.recordErr
	}
	return runtimestorage.MessageEvent{}, false, nil
}
func (s *claimStoreStub) TransitionMessage(context.Context, runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	if s.transitionErr != nil {
		return runtimestorage.MessageEvent{}, s.transitionErr
	}
	return runtimestorage.MessageEvent{}, nil
}

func newTestDispatcher(t *testing.T, runnerValue *testRunner) (*Dispatcher, Principal) {
	t.Helper()
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) { return runnerValue, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return dispatcher, mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
}

func TestDispatcherMapsEventsAndPropagatesIdentityAndRequestID(t *testing.T) {
	runnerValue := &testRunner{}
	var captured capturedRun
	runnerValue.runFn = func(_ context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		settings := trpcagent.RunOptions{}
		for _, option := range options {
			option(&settings)
		}
		captured.mu.Lock()
		captured.userID, captured.sessionID, captured.message, captured.requestID = userID, sessionID, message, settings.RequestID
		captured.mu.Unlock()
		events := make(chan *trpcevent.Event, 2)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "hello"}}}}}
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}
	dispatcher, principal := newTestDispatcher(t, runnerValue)
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal,
		Message: InboundMessage{
			Content: "  hello  ", ExternalUserID: "external-user", ConversationKind: channels.ConversationDirect,
			ExternalPeerID: "external-peer",
		},
		TraceID: "trace-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Type != DispatchEventMessage || events[0].Text != "hello" || !events[1].Done {
		t.Fatalf("dispatch events = %+v", events)
	}
	requestID := events[0].RequestID
	if requestID == "" || requestID != events[1].RequestID || events[0].TraceID != "trace-1" {
		t.Fatalf("event correlation = %+v", events)
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if captured.userID == "" || captured.sessionID == "" || captured.message.Content != "hello" || captured.requestID != requestID {
		t.Fatalf("captured Runner call user=%q session=%q content=%q request=%q", captured.userID, captured.sessionID, captured.message.Content, captured.requestID)
	}
}

func TestDispatcherUsesVerifiedChannelIdentity(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	channelPrincipal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	var captured capturedRun
	runnerValue := &testRunner{}
	runnerValue.runFn = func(_ context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		captured.mu.Lock()
		captured.userID, captured.sessionID, captured.message = userID, sessionID, message
		captured.mu.Unlock()
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}
	resolver, err := NewPlanResolver(PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: channelPrincipal,
		Message: InboundMessage{
			Content: "group message", ExternalUserID: "user-1", ConversationKind: channels.ConversationGroup,
			ExternalChatID: "chat-1", ExternalThreadID: "thread-1",
		},
		RequestID: "request-channel",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 1 || !events[0].Done {
		t.Fatalf("channel dispatch events = %+v", events)
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if !strings.Contains(captured.userID, target.BindingID) || !strings.Contains(captured.sessionID, target.BindingID) {
		t.Fatalf("channel identity omitted Binding scope: user=%q session=%q", captured.userID, captured.sessionID)
	}
}

func TestDispatcherDurableChannelClaimSuppressesDuplicateRunner(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	runnerValue := &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	store := inmemory.New()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, RuntimeStore: store, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	request := DispatchRequest{Principal: principal, Message: InboundMessage{Content: "duplicate", ExternalMessageID: "channel-message-1", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}, RequestID: "channel-message-1"}
	firstDone := make(chan error, 1)
	go func() {
		stream, dispatchErr := dispatcher.Dispatch(context.Background(), request)
		if dispatchErr != nil {
			firstDone <- dispatchErr
			return
		}
		collectDispatchEvents(stream)
		firstDone <- nil
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first Runner did not start")
	}
	if _, err := dispatcher.Dispatch(context.Background(), request); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate dispatch error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("duplicate started %d Runner calls", got)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherDurableClaimReclaimsReceivedAndExpiredRunning(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	store := inmemory.New()
	dispatcher := &Dispatcher{runtimeStore: store}
	message := InboundMessage{Content: "claim", ExternalMessageID: "claim-received", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}
	identity, err := dispatchRunnerIdentity(principal, message)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(context.Background(), principal.TenantID(), identity.SessionID, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: "received-event", SessionID: identity.SessionID, BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID}); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := dispatcher.claimInbound(context.Background(), principal, message, identity)
	if err != nil || reclaimed == nil {
		t.Fatalf("received reclaim = %+v err=%v", reclaimed, err)
	}
	dispatcher.failDurable(reclaimed, errors.New("runner unavailable"))
	message.ExternalMessageID = "claim-expired"
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: "expired-event", SessionID: identity.SessionID, BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: "expired-event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "old", LeaseDuration: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	recovered, err := dispatcher.claimInbound(context.Background(), principal, message, identity)
	if err != nil || recovered == nil {
		t.Fatalf("expired reclaim = %+v err=%v", recovered, err)
	}
	if _, err := dispatcher.claimInbound(context.Background(), principal, message, identity); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("active duplicate error = %v", err)
	}
	seedClaimEvent := func(eventID, externalID string, status string) {
		t.Helper()
		if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: eventID, SessionID: identity.SessionID, BindingID: target.BindingID, ExternalMessageID: externalID}); err != nil {
			t.Fatal(err)
		}
		if status == runtimestorage.EventFailed {
			if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventReceived, To: runtimestorage.EventFailed, Owner: "seed"}); err != nil {
				t.Fatal(err)
			}
			return
		}
		running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "seed", LeaseDuration: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if status == runtimestorage.EventCompleted {
			if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "seed", FencingToken: running.FencingToken}); err != nil {
				t.Fatal(err)
			}
			return
		}
		if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "seed", FencingToken: running.FencingToken}); err == nil {
			t.Fatal("expected live lease reconciliation to fail")
		}
	}
	seedClaimEvent("completed-event", "claim-completed", runtimestorage.EventCompleted)
	message.ExternalMessageID = "claim-completed"
	if _, err := dispatcher.claimInbound(context.Background(), principal, message, identity); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("completed duplicate error = %v", err)
	}
	seedClaimEvent("failed-event", "claim-failed", runtimestorage.EventFailed)
	message.ExternalMessageID = "claim-failed"
	if _, err := dispatcher.claimInbound(context.Background(), principal, message, identity); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("failed duplicate error = %v", err)
	}
	message.ExternalMessageID = "claim-reconciling"
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: "reconciling-event", SessionID: identity.SessionID, BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID}); err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: "reconciling-event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "seed", LeaseDuration: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: "reconciling-event", From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "recovery", FencingToken: running.FencingToken}); err != nil {
		t.Fatal(err)
	}
	recoveredReconciling, err := dispatcher.claimInbound(context.Background(), principal, message, identity)
	if err != nil || recoveredReconciling == nil {
		t.Fatalf("reconciling reclaim = %+v err=%v", recoveredReconciling, err)
	}
	dispatcher.finishDurable(recoveredReconciling, errors.New("execution failed"))
	message.ExternalMessageID = ""
	if _, err := dispatcher.claimInbound(context.Background(), principal, message, identity); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing external ID error = %v", err)
	}
	message.ExternalMessageID = strings.Repeat("x", maxDurableExternalMessageIDRunes+1)
	if _, err := dispatcher.claimInbound(context.Background(), principal, message, identity); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized durable external ID error = %v", err)
	}
}

func TestDispatcherDurableClaimMapsStorageErrors(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	message := InboundMessage{Content: "claim-errors", ExternalMessageID: "claim-errors", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}
	identity, err := dispatchRunnerIdentity(principal, message)
	if err != nil {
		t.Fatal(err)
	}
	base := inmemory.New()
	claim := func(store runtimestorage.RuntimeStore) error {
		_, err := (&Dispatcher{runtimeStore: store}).claimInbound(context.Background(), principal, message, identity)
		return err
	}
	if err := claim(&claimStoreStub{RuntimeStore: base, getErr: errors.New("storage")}); err == nil {
		t.Fatal("expected GetSession storage error")
	}
	if err := claim(&claimStoreStub{RuntimeStore: base, getErr: runtimestorage.ErrNotFound, createErr: errors.New("create")}); err == nil {
		t.Fatal("expected CreateSession storage error")
	}
	if err := claim(&claimStoreStub{RuntimeStore: base, recordErr: errors.New("record")}); err == nil {
		t.Fatal("expected RecordMessage storage error")
	}
	if err := claim(&claimStoreStub{RuntimeStore: base, transitionErr: errors.New("transition")}); err == nil {
		t.Fatal("expected TransitionMessage storage error")
	}
}

func TestDispatcherRedactsRunnerErrors(t *testing.T) {
	runnerValue := &testRunner{}
	runnerValue.runFn = func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "provider secret endpoint"}}}
		close(events)
		return events, nil
	}
	dispatcher, principal := newTestDispatcher(t, runnerValue)
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal,
		Message:   InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		RequestID: "request-error",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Type != DispatchEventError || events[0].Error != ErrExecution.Error() || !events[1].Done {
		t.Fatalf("error dispatch events = %+v", events)
	}
	if strings.Contains(events[0].Error, "secret") {
		t.Fatal("Runner provider detail escaped into Dispatch event")
	}
}

func TestDispatcherCancellationDrainsRunnerEventsAndReleasesLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	senderFinished := make(chan struct{})
	runnerValue := &testRunner{}
	runnerValue.runFn = func(ctx context.Context, _ string, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event)
		go func() {
			defer close(senderFinished)
			select {
			case events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "late"}}}}}:
			case <-ctx.Done():
			}
			close(events)
		}()
		return events, nil
	}
	dispatcher, principal := newTestDispatcher(t, runnerValue)
	stream, err := dispatcher.Dispatch(ctx, DispatchRequest{
		Principal: principal,
		Message:   InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		RequestID: "request-cancel",
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Error != ErrExecutionCanceled.Error() || !events[1].Done {
		t.Fatalf("cancellation events = %+v", events)
	}
	select {
	case <-senderFinished:
	case <-time.After(time.Second):
		t.Fatal("Runner event sender was not drained")
	}
}

func TestDispatcherRejectsInvalidBoundaryInputs(t *testing.T) {
	dispatcher, principal := newTestDispatcher(t, &testRunner{})
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Message: InboundMessage{Content: "hello"}}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("invalid principal error = %v", err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal,
		Message:   InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		TraceID:   "bad\ntrace",
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid trace ID error = %v", err)
	}
}

func collectDispatchEvents(stream <-chan DispatchEvent) []DispatchEvent {
	events := make([]DispatchEvent, 0, 4)
	for event := range stream {
		events = append(events, event)
	}
	return events
}

func TestDispatcherConfigurationAndEventMappingEdges(t *testing.T) {
	if _, err := NewDispatcher(DispatchConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing dispatcher dependency error = %v", err)
	}
	dispatcher, principal := newTestDispatcher(t, &testRunner{})
	if _, err := NewDispatcher(DispatchConfig{Resolver: dispatcher.resolver, Registry: dispatcher.registry, DrainTimeout: -time.Second}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative drain timeout error = %v", err)
	}
	readyDispatcher, err := NewDispatcher(DispatchConfig{Resolver: dispatcher.resolver, Registry: dispatcher.registry})
	if err != nil {
		t.Fatal(err)
	}
	if !readyDispatcher.Ready() {
		t.Fatal("configured dispatcher is not ready")
	}
	var nilDispatcher *Dispatcher
	if nilDispatcher.Ready() {
		t.Fatal("nil dispatcher is ready")
	}
	if _, err := nilDispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil dispatcher error = %v", err)
	}
	var nilContext context.Context
	if _, err := dispatcher.Dispatch(nilContext, DispatchRequest{Principal: principal}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil dispatch context error = %v", err)
	}

	for name, event := range map[string]*trpcevent.Event{
		"nil event":        nil,
		"partial status":   {Response: &trpcmodel.Response{IsPartial: true}},
		"message fallback": {Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.Message{Content: "fallback"}}}}},
		"done with text":   {Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "final"}}}, Done: true}},
	} {
		t.Run(name, func(t *testing.T) {
			mapped, done := mapRunnerEvent(event, "request", "trace")
			if name == "done with text" && !done {
				t.Fatal("done event was not terminal")
			}
			if len(mapped) == 0 || mapped[0].RequestID != "request" {
				t.Fatalf("mapped event = %+v", mapped)
			}
		})
	}
	if got := cancellationStatus(contextWithDeadline(t)); got != "deadline_exceeded" {
		t.Fatalf("deadline cancellation status = %q", got)
	}
	if got := cancellationStatus(canceledContext()); got != "canceled" {
		t.Fatalf("canceled status = %q", got)
	}
	if responseText(nil) != "" || responseText(&trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.Message{Content: "message"}}, {Delta: trpcmodel.Message{Content: "delta"}}}}) != "messagedelta" {
		t.Fatal("response text mapping was incorrect")
	}
	if _, done := mapRunnerEvent(&trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "secret"}}}, "request", "trace"); !done {
		t.Fatal("error event was not terminal")
	}
	if _, err := dispatchRunnerIdentity(Principal{}, InboundMessage{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unknown principal identity error = %v", err)
	}
	if got := encodeDispatchIdentity("a", "bc"); got != "1:a2:bc" {
		t.Fatalf("encoded identity = %q", got)
	}
	groupMessage := InboundMessage{
		Content: "group", ExternalUserID: "user", ConversationKind: channels.ConversationGroup, ExternalChatID: "chat",
	}
	if _, err := dispatchRunnerIdentity(principal, groupMessage); err != nil {
		t.Fatalf("API group identity error = %v", err)
	}
}

func TestDispatcherRunAndChannelTerminalEdges(t *testing.T) {
	request := func(principal Principal) DispatchRequest {
		return DispatchRequest{Principal: principal, Message: InboundMessage{
			Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer",
		}}
	}
	for name, runFn := range map[string]func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error){
		"runner error": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, errors.New("provider detail")
		},
		"runner canceled": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, context.Canceled
		},
		"nil event stream": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, nil
		},
		"closed event stream": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			events := make(chan *trpcevent.Event)
			close(events)
			return events, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &testRunner{runFn: runFn}
			dispatcher, principal := newTestDispatcher(t, runner)
			stream, err := dispatcher.Dispatch(context.Background(), request(principal))
			switch name {
			case "runner error":
				if !errors.Is(err, ErrExecution) {
					t.Fatalf("runner error = %v", err)
				}
			case "runner canceled":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("runner cancellation = %v", err)
				}
			case "nil event stream":
				if !errors.Is(err, ErrExecution) {
					t.Fatalf("nil event stream error = %v", err)
				}
			case "closed event stream":
				if err != nil {
					t.Fatal(err)
				}
				events := collectDispatchEvents(stream)
				if len(events) != 1 || !events[0].Done {
					t.Fatalf("closed stream events = %+v", events)
				}
			}
		})
	}

	dispatcher, principal := newTestDispatcher(t, &testRunner{})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := dispatcher.Dispatch(canceled, request(principal)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dispatch error = %v", err)
	}

	ctx, stop := context.WithCancel(context.Background())
	stop()
	if sendDispatchEvent(ctx, make(chan DispatchEvent), DispatchEvent{}) {
		t.Fatal("sendDispatchEvent succeeded after cancellation")
	}
	drainRunnerEvents(nil, time.Millisecond)
	drainRunnerEvents(make(chan *trpcevent.Event), time.Millisecond)

	var nilLease *RunnerLease
	if nilLease.Runner() != nil || nilLease.Release() != nil {
		t.Fatal("nil lease was not safe")
	}
	if (&RunnerLease{}).Runner() != nil || (&RunnerLease{}).Release() != nil {
		t.Fatal("empty lease was not safe")
	}
	if (&runnerEntry{}).close() != nil {
		t.Fatal("empty Runner entry close failed")
	}
}

func contextWithDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

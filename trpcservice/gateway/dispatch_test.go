package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
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
	defer registry.Close()
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

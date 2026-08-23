package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRunnerRegistryConfigurationAndBoundaryErrors(t *testing.T) {
	factory := func(context.Context, runtime.ExecutionPlan) (Runner, error) { return &testRunner{}, nil }
	for name, config := range map[string]RunnerRegistryConfig{
		"missing factory":        {},
		"negative entries":       {Factory: factory, MaxEntries: -1},
		"negative idle ttl":      {Factory: factory, IdleTTL: -time.Second},
		"negative close timeout": {Factory: factory, CloseTimeout: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewRunnerRegistry(config); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewRunnerRegistry() error = %v", err)
			}
		})
	}

	plan := testExecutionPlan(t)
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{Factory: factory})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.Ready() {
		t.Fatal("new registry is not ready")
	}
	if _, err := registry.Acquire(nil, plan); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Acquire(canceled, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	if _, err := registry.Acquire(context.Background(), runtime.ExecutionPlan{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid plan error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if registry.Ready() {
		t.Fatal("closed registry is ready")
	}
	if _, err := registry.Acquire(context.Background(), plan); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed registry acquire error = %v", err)
	}
	if err := registry.Invalidate(runtime.CacheKey{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed registry invalidation error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	var nilRegistry *RunnerRegistry
	if nilRegistry.Ready() {
		t.Fatal("nil registry is ready")
	}
	if _, err := nilRegistry.Acquire(context.Background(), plan); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil registry acquire error = %v", err)
	}
	if err := nilRegistry.Invalidate(runtime.CacheKey{}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil registry invalidation error = %v", err)
	}
}

type stage2ModelFactory struct{}

func (stage2ModelFactory) New(context.Context, model.ModelFactoryInput, model.SecretValue) (trpcmodel.Model, error) {
	return nil, nil
}

func TestRuntimeRunnerRegistryWiresBorrowedDependencies(t *testing.T) {
	if _, err := NewRuntimeRunnerRegistry(RuntimeRunnerRegistryConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing runtime dependency error = %v", err)
	}
	registry, err := NewRuntimeRunnerRegistry(RuntimeRunnerRegistryConfig{
		ModelFactory: stage2ModelFactory{},
		Sessions:     sessioninmemory.NewSessionService(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.Ready() {
		t.Fatal("runtime registry is not ready")
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryFactoryAndPendingCancellationEdges(t *testing.T) {
	plan := testExecutionPlan(t)
	for name, factory := range map[string]RunnerFactory{
		"nil runner": func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			return nil, nil
		},
		"context failure": func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			return nil, context.DeadlineExceeded
		},
		"runner with failure": func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			return &testRunner{}, errors.New("provider detail")
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry, err := NewRunnerRegistry(RunnerRegistryConfig{Factory: factory})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = registry.Close() }()
			_, err = registry.Acquire(context.Background(), plan)
			switch name {
			case "context failure":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("factory context error = %v", err)
				}
			default:
				if !errors.Is(err, ErrRunnerUnavailable) {
					t.Fatalf("factory error = %v", err)
				}
			}
		})
	}

	started := make(chan struct{})
	release := make(chan struct{})
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(ctx context.Context, _ runtime.ExecutionPlan) (Runner, error) {
			close(started)
			select {
			case <-release:
				return &testRunner{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	first := make(chan *RunnerLease, 1)
	go func() {
		lease, acquireErr := registry.Acquire(context.Background(), plan)
		if acquireErr != nil {
			t.Errorf("first acquire error = %v", acquireErr)
			return
		}
		first <- lease
	}()
	<-started
	canceled, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() {
		_, acquireErr := registry.Acquire(canceled, plan)
		second <- acquireErr
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatalf("pending canceled acquire error = %v", err)
	}
	close(release)
	lease := <-first
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryIdleCloseErrorAndTimeoutEdges(t *testing.T) {
	planOne := testExecutionPlan(t)
	planTwo := testExecutionPlan(t)
	now := time.Unix(100, 0)
	var runners []*testRunner
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			runner := &testRunner{}
			runners = append(runners, runner)
			return runner, nil
		},
		MaxEntries: 2, IdleTTL: time.Minute, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	otherLease, err := registry.Acquire(context.Background(), planTwo)
	if err != nil {
		t.Fatal(err)
	}
	if got := runners[0].closeCount.Load(); got != 1 {
		t.Fatalf("idle Runner close count = %d, want 1", got)
	}
	if err := otherLease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	closeFail := &testRunner{closeErr: errors.New("provider close detail")}
	failing, err := NewRunnerRegistry(RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) { return closeFail, nil }})
	if err != nil {
		t.Fatal(err)
	}
	failingLease, err := failing.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if err := failingLease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := failing.Close(); !errors.Is(err, ErrRunnerClose) {
		t.Fatalf("close failure = %v", err)
	}
	if err := failing.Close(); !errors.Is(err, ErrRunnerClose) {
		t.Fatalf("repeated close failure = %v", err)
	}

	borrowed := &testRunner{}
	timeoutRegistry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory:      func(context.Context, runtime.ExecutionPlan) (Runner, error) { return borrowed, nil },
		CloseTimeout: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	borrowedLease, err := timeoutRegistry.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if err := timeoutRegistry.Close(); !errors.Is(err, ErrRegistryCloseTimeout) {
		t.Fatalf("close timeout = %v", err)
	}
	if err := borrowedLease.Release(); err != nil {
		t.Fatal(err)
	}
	if got := borrowed.closeCount.Load(); got != 1 {
		t.Fatalf("eventual close count = %d, want 1", got)
	}
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
	if _, err := dispatcher.Dispatch(nil, DispatchRequest{Principal: principal}); !errors.Is(err, ErrInvalid) {
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
		Content: "group", ExternalUserID: "user", ConversationKind: "group", ExternalChatID: "chat",
	}
	if _, err := dispatchRunnerIdentity(principal, groupMessage); err != nil {
		t.Fatalf("API group identity error = %v", err)
	}
}

func TestDispatcherRunAndChannelTerminalEdges(t *testing.T) {
	request := func(principal Principal) DispatchRequest {
		return DispatchRequest{Principal: principal, Message: InboundMessage{
			Content: "hello", ExternalUserID: "user", ConversationKind: "direct", ExternalPeerID: "peer",
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

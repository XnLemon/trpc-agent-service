package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type runnerStreamTestRunner struct {
	events    <-chan *trpcevent.Event
	err       error
	requestID string
}

func (runner *runnerStreamTestRunner) Run(_ context.Context, _ string, _ string, _ trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	settings := trpcagent.RunOptions{}
	for _, option := range options {
		option(&settings)
	}
	runner.requestID = settings.RequestID
	return runner.events, runner.err
}

func (*runnerStreamTestRunner) Close() error { return nil }

func TestInvokeMapsAndStopsAtTerminalExternalEvent(t *testing.T) {
	source := make(chan *trpcevent.Event, 3)
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "hello"}}}}}
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "ignored"}}}}}
	runner := &runnerStreamTestRunner{events: source}
	stream, err := Invoke(context.Background(), runner, Invocation{UserID: "user", SessionID: "session", RequestID: "request"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	events := collectRunnerStreamEvents(stream)
	if len(events) != 2 || events[0].Type != RunnerEventMessage || events[0].Text != "hello" || events[1].Type != RunnerEventDone || !events[1].Done {
		t.Fatalf("events=%+v", events)
	}
	if runner.requestID != "request" {
		t.Fatalf("upstream request ID=%q", runner.requestID)
	}
}

func TestInvokeRedactsExternalRunnerErrors(t *testing.T) {
	source := make(chan *trpcevent.Event, 1)
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "provider secret"}}}
	runner := &runnerStreamTestRunner{events: source}
	stream, err := Invoke(context.Background(), runner, Invocation{UserID: "user", SessionID: "session", RequestID: "request"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	events := collectRunnerStreamEvents(stream)
	if len(events) != 2 || events[0].Type != RunnerEventError || !errors.Is(events[0].Err, ErrRunnerExecution) || events[1].Type != RunnerEventDone {
		t.Fatalf("events=%+v", events)
	}
	if strings.Contains(events[0].Err.Error(), "provider secret") {
		t.Fatal("provider error escaped Agent boundary")
	}
}

func TestInvokeCancellationDrainsExternalStream(t *testing.T) {
	source := make(chan *trpcevent.Event)
	runner := &runnerStreamTestRunner{events: source}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := Invoke(ctx, runner, Invocation{UserID: "user", SessionID: "session", RequestID: "request"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("canceled invocation emitted an event")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled invocation did not close its stream")
	}
}

func TestInvokeRejectsInvalidInput(t *testing.T) {
	runner := &runnerStreamTestRunner{events: make(chan *trpcevent.Event)}
	tests := []struct {
		name   string
		ctx    context.Context
		runner Runner
		input  Invocation
	}{
		{name: "nil context", runner: runner, input: Invocation{UserID: "user", SessionID: "session", RequestID: "request"}},
		{name: "nil runner", ctx: context.Background(), input: Invocation{UserID: "user", SessionID: "session", RequestID: "request"}},
		{name: "missing identity", ctx: context.Background(), runner: runner, input: Invocation{RequestID: "request"}},
		{name: "missing request ID", ctx: context.Background(), runner: runner, input: Invocation{UserID: "user", SessionID: "session"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Invoke(test.ctx, test.runner, test.input, time.Millisecond); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Invoke() error=%v", err)
			}
		})
	}
}

func collectRunnerStreamEvents(stream <-chan RunnerEvent) []RunnerEvent {
	events := make([]RunnerEvent, 0, 4)
	for event := range stream {
		events = append(events, event)
	}
	return events
}

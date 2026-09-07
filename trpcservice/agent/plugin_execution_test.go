package agent

import (
	"context"
	"sync/atomic"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRunnerExecutesAndClosesUpstreamPlugin(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = nil
	probe := &lifecycleProbePlugin{}
	counter := &countingCompositeModel{}
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input,
		ModelFactory: knowledgeModelFactory(func(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error) {
			return counter, nil
		}),
		Sessions: sessioninmemory.NewSessionService(), Plugins: []plugin.Plugin{probe},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := runner.Run(context.Background(), "user", "session", model.NewUserMessage("run"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	if probe.events.Load() == 0 || probe.afterRuns.Load() != 1 || counter.calls.Load() != 1 {
		t.Fatalf("plugin hooks: events=%d after_runs=%d model_calls=%d", probe.events.Load(), probe.afterRuns.Load(), counter.calls.Load())
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if probe.closes.Load() != 1 {
		t.Fatalf("plugin closes = %d", probe.closes.Load())
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if probe.closes.Load() != 1 {
		t.Fatalf("plugin closed more than once: %d", probe.closes.Load())
	}
}

func TestRunnerClosesUpstreamPluginWhenAssemblyFails(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = nil
	probe := &lifecycleProbePlugin{}
	_, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, Sessions: sessioninmemory.NewSessionService(),
		ModelFactory: runnerBuilderModelFactory{err: context.DeadlineExceeded}, Plugins: []plugin.Plugin{probe},
	})
	if err == nil {
		t.Fatal("assembly failure unexpectedly succeeded")
	}
	if probe.closes.Load() != 1 {
		t.Fatalf("plugin close calls after assembly failure = %d", probe.closes.Load())
	}
}

type lifecycleProbePlugin struct {
	events    atomic.Int64
	afterRuns atomic.Int64
	closes    atomic.Int64
}

func (*lifecycleProbePlugin) Name() string { return "platform-lifecycle-probe" }

func (probe *lifecycleProbePlugin) Register(registry *plugin.Registry) {
	registry.OnEvent(func(_ context.Context, _ *agent.Invocation, value *event.Event) (*event.Event, error) {
		probe.events.Add(1)
		return value, nil
	})
	registry.AfterRun(func(context.Context, *plugin.AfterRunArgs) error {
		probe.afterRuns.Add(1)
		return nil
	})
}

func (probe *lifecycleProbePlugin) Close(context.Context) error {
	probe.closes.Add(1)
	return nil
}

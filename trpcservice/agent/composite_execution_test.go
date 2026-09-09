package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestNativeCompositeAgentsExecuteThroughPlatformRunner(t *testing.T) {
	for _, kind := range []appmodel.Kind{appmodel.KindChain, appmodel.KindParallel, appmodel.KindCycle, appmodel.KindGraph} {
		t.Run(string(kind), func(t *testing.T) {
			input := runnerBuilderInputForTest(t)
			input.Agent.Tools = nil
			input.Agent.Kind = kind
			input.Agent.Runtime.MaxLLMCalls = 4
			input.Agent.Chain = &appmodel.ChainConfiguration{Steps: []appmodel.ChainStep{
				{Name: "first", Instruction: "Produce the first result."},
				{Name: "second", Instruction: "Produce the second result."},
			}}
			counter := &countingCompositeModel{}
			runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
				Input: input,
				ModelFactory: knowledgeModelFactory(func(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error) {
					return counter, nil
				}),
				StorageFactory: compositeStorageFactory(),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close()
			events, err := runner.Run(context.Background(), "user", "session", model.NewUserMessage("run"))
			if err != nil {
				t.Fatal(err)
			}
			for range events {
			}
			calls := counter.calls.Load()
			if calls < 2 {
				t.Fatalf("native %s executed %d model calls, want at least both nodes", kind, calls)
			}
			if calls > int64(input.Agent.Runtime.MaxLLMCalls) {
				t.Fatalf("native %s exceeded model-call policy: %d > %d", kind, calls, input.Agent.Runtime.MaxLLMCalls)
			}
		})
	}
}

func TestNativeGraphExecutionHonorsCancellation(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = nil
	input.Agent.Kind = appmodel.KindGraph
	input.Agent.Chain = &appmodel.ChainConfiguration{Steps: []appmodel.ChainStep{
		{Name: "first", Instruction: "Wait."},
		{Name: "second", Instruction: "Never run after cancellation."},
	}}
	blocking := &blockingCompositeModel{started: make(chan struct{})}
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input,
		ModelFactory: knowledgeModelFactory(func(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error) {
			return blocking, nil
		}),
		StorageFactory: compositeStorageFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	ctx, cancel := context.WithCancel(context.Background())
	events, err := runner.Run(ctx, "user", "session", model.NewUserMessage("run"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("graph model did not start")
	}
	cancel()
	done := make(chan struct{})
	go func() {
		for range events {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("graph event stream did not close after cancellation")
	}
	if blocking.calls.Load() != 1 {
		t.Fatalf("graph cancellation allowed %d model calls", blocking.calls.Load())
	}
}

func compositeStorageFactory() storagefactory.StorageFactory {
	sessions := sessioninmemory.NewSessionService()
	return storagefactory.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: sessions})
	})
}

type countingCompositeModel struct{ calls atomic.Int64 }

func (*countingCompositeModel) Info() model.Info { return model.Info{Name: "composite-counter"} }

func (counter *countingCompositeModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	counter.calls.Add(1)
	responses := make(chan *model.Response, 1)
	select {
	case responses <- &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage("done")}}}:
	case <-ctx.Done():
	}
	close(responses)
	return responses, nil
}

type blockingCompositeModel struct {
	calls   atomic.Int64
	started chan struct{}
}

func (*blockingCompositeModel) Info() model.Info { return model.Info{Name: "composite-blocker"} }

func (blocking *blockingCompositeModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	if blocking.calls.Add(1) == 1 {
		close(blocking.started)
	}
	responses := make(chan *model.Response)
	go func() {
		defer close(responses)
		<-ctx.Done()
	}()
	return responses, nil
}

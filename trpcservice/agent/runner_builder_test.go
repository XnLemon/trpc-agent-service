package agent

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestNewRunnerWithConfigRejectsInvalidDependencies(t *testing.T) {
	var nilContext context.Context
	for _, test := range []struct {
		name   string
		ctx    context.Context
		config RunnerConfig
	}{
		{name: "nil context", ctx: nilContext, config: RunnerConfig{Sessions: sessioninmemory.NewSessionService()}},
		{name: "missing session capability", ctx: context.Background()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.config.Sessions != nil {
				t.Cleanup(func() { _ = test.config.Sessions.Close() })
			}
			if _, err := NewRunnerWithConfig(test.ctx, test.config); err == nil {
				t.Fatal("invalid runner configuration unexpectedly succeeded")
			}
		})
	}
}

func TestNewRunnerWithConfigRejectsStorageMaterializationFailures(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	factoryErr := errors.New("storage provider failed")
	tests := []struct {
		name    string
		factory storagefactory.StorageFactory
		wantErr error
	}{
		{
			name: "factory error",
			factory: storagefactory.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
				return nil, factoryErr
			}),
			wantErr: factoryErr,
		},
		{
			name: "nil capability set",
			factory: storagefactory.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
				return nil, nil
			}),
			wantErr: storagefactory.ErrStorageFactory,
		},
		{
			name: "missing session capability",
			factory: storagefactory.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
				return storagefactory.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilityMemory: struct{}{}})
			}),
			wantErr: storagefactory.ErrCapabilityUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewRunnerWithConfig(context.Background(), RunnerConfig{Input: input, ModelFactory: runnerBuilderModelFactory{}, StorageFactory: test.factory})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("NewRunnerWithConfig() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestNewRunnerWithConfigCleansUpAfterAssemblyFailure(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	base := sessioninmemory.NewSessionService()
	tracked := &agentCloseTrackingSession{Service: base}
	factory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{backend.CapabilitySession: tracked})
	})
	_, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, ModelFactory: runnerBuilderModelFactory{err: errors.New("model unavailable")}, StorageFactory: factory,
	})
	if err == nil {
		// ResolveAndBuild deliberately redacts provider failures to its stable
		// model-factory category; the important contract here is cleanup.
		t.Fatal("assembly failure unexpectedly succeeded")
	}
	if tracked.calls != 1 {
		t.Fatalf("storage capability close calls = %d, want 1", tracked.calls)
	}
}

func TestNewRunnerWithConfigCoversModelToolTelemetryAndStorageBoundaries(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = nil

	modelFailure := errors.New("model provider detail")
	modelSessions := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = modelSessions.Close() })
	if _, err := NewRunnerWithConfig(context.Background(), RunnerConfig{Input: input, Sessions: modelSessions, ModelFactory: runnerBuilderModelFactory{err: modelFailure}}); err == nil {
		// The adapter preserves the model runtime category rather than provider
		// details; assert only that the build failed and did not return a Runner.
		t.Fatal("model assembly unexpectedly succeeded")
	}

	toolInput := input
	toolInput.Agent.Tools = []appmodel.ToolAuthorization{{ToolID: "required-tool-not-installed", Required: true}}
	toolSessions := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = toolSessions.Close() })
	_, err := NewRunnerWithConfig(context.Background(), RunnerConfig{Input: toolInput, Sessions: toolSessions, ModelFactory: runnerBuilderModelFactory{}})
	if err == nil {
		t.Fatal("required unavailable tool unexpectedly succeeded")
	}

	telemetrySessions := sessioninmemory.NewSessionService()
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, Sessions: telemetrySessions, ModelFactory: runnerBuilderModelFactory{}, Observability: observability.NewNoopProvider(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	storageSessions := sessioninmemory.NewSessionService()
	storageFactory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{backend.CapabilitySession: storageSessions})
	})
	runner, err = NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, ModelFactory: runnerBuilderModelFactory{}, Observability: observability.NewNoopProvider(),
		StorageFactory: storageFactory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewRunnerWithConfigInjectsNativeServices(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = []appmodel.ToolAuthorization{{ToolID: "knowledge_search", Required: true}}
	sessions := sessioninmemory.NewSessionService()
	memories := memoryinmemory.NewMemoryService()
	artifacts := artifactinmemory.NewService()
	kb := knowledge.New()
	factory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{
			backend.CapabilitySession:   sessions,
			backend.CapabilityMemory:    memories,
			backend.CapabilityArtifact:  artifacts,
			backend.CapabilityKnowledge: kb,
		})
	})
	captured := make(chan *trpcagent.Invocation, 1)
	registries, err := NewAgentFactoryRegistry(AgentFactoryRegistration{
		Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Factory: func(_ context.Context, build AgentBuildInput) (trpcagent.Agent, error) {
			if build.Knowledge != kb {
				t.Fatal("native Knowledge was not passed to the Agent factory")
			}
			return invocationCaptureAgent{captured: captured}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, ModelFactory: runnerBuilderModelFactory{}, StorageFactory: factory, AgentFactories: registries,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	events, err := runner.Run(context.Background(), "user", "session", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	invocation := <-captured
	if invocation.MemoryService == nil || invocation.ArtifactService == nil {
		t.Fatalf("native services missing from Invocation: memory=%T artifact=%T", invocation.MemoryService, invocation.ArtifactService)
	}
}

type invocationCaptureAgent struct {
	captured chan<- *trpcagent.Invocation
}

func (a invocationCaptureAgent) Run(_ context.Context, invocation *trpcagent.Invocation) (<-chan *trpcevent.Event, error) {
	a.captured <- invocation
	events := make(chan *trpcevent.Event)
	close(events)
	return events, nil
}
func (invocationCaptureAgent) Tools() []tool.Tool                  { return nil }
func (invocationCaptureAgent) Info() trpcagent.Info                { return trpcagent.Info{Name: "capture"} }
func (invocationCaptureAgent) SubAgents() []trpcagent.Agent        { return nil }
func (invocationCaptureAgent) FindSubAgent(string) trpcagent.Agent { return nil }

func runnerBuilderInputForTest(t *testing.T) RunnerInput {
	t.Helper()
	tenantRoot, tenantSnapshot, appRoot, revision := executionFixture(t)
	snapshot, err := NewAgentExecutionSnapshot(tenantSnapshot, appRoot, revision)
	if err != nil {
		t.Fatal(err)
	}
	agentInput, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	return RunnerInput{
		Tenant: *tenantRoot,
		Agent:  agentInput,
		Model: modelprofile.ModelFactoryInput{
			TenantID: tenantRoot.TenantID, TenantVersion: tenantRoot.Version,
			ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileVersion: 1,
			ContentDigest: "model-digest", SchemaVersion: modelprofile.SchemaVersionV1,
			Provider: "fake", Model: "deterministic",
		},
		Storage: backend.StorageFactoryInput{
			TenantID: tenantRoot.TenantID, TenantVersion: tenantRoot.Version,
			ProfileID: "bp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileVersion: 1,
			ContentDigest: "backend-digest", SchemaVersion: 1,
		},
	}
}

type runnerBuilderModelFactory struct {
	err       error
	returnNil bool
}

func (factory runnerBuilderModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error) {
	if factory.err != nil {
		return nil, factory.err
	}
	if factory.returnNil {
		return nil, nil
	}
	return agentTestModel{}, nil
}

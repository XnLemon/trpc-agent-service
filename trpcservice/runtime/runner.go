package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// NewRunner resolves a model from a fixed ExecutionPlan and assembles the
// minimum tRPC-Agent-Go LLMAgent/Runner spine. The supplied Session service is
// borrowed by the returned Runner and remains owned by the caller.
func NewRunner(
	ctx context.Context,
	plan ExecutionPlan,
	resolver modelprofile.SecretResolver,
	factory modelprofile.ModelFactory,
	sessions session.Service,
	storageFactories ...backend.StorageFactory,
) (trpcrunner.Runner, error) {
	return NewRunnerWithObservability(ctx, plan, resolver, factory, sessions, nil, storageFactories...)
}

// NewRunnerWithObservability is NewRunner with provider-neutral model and tool telemetry.
func NewRunnerWithObservability(
	ctx context.Context,
	plan ExecutionPlan,
	resolver modelprofile.SecretResolver,
	factory modelprofile.ModelFactory,
	sessions session.Service,
	telemetry observability.Provider,
	storageFactories ...backend.StorageFactory,
) (trpcrunner.Runner, error) {
	if ctx == nil {
		return nil, errors.New("invalid runner: context is required")
	}
	if sessions == nil && len(storageFactories) == 0 {
		return nil, errors.New("invalid runner: session service is required")
	}
	agentInput, err := plan.AgentFactoryInput()
	if err != nil {
		return nil, fmt.Errorf("build runner: agent input: %w", err)
	}
	modelInput, err := plan.ModelFactoryInput()
	if err != nil {
		return nil, fmt.Errorf("build runner: model input: %w", err)
	}
	if _, err := plan.StorageFactoryInput(); err != nil {
		return nil, fmt.Errorf("build runner: storage input: %w", err)
	}
	var capabilities *backend.CapabilitySet
	if len(storageFactories) > 1 {
		return nil, errors.New("invalid runner: multiple storage factories")
	}
	if len(storageFactories) == 1 && storageFactories[0] == nil {
		return nil, errors.New("invalid runner: storage factory is required")
	}
	if len(storageFactories) == 1 {
		storageCtx := ctx
		started := time.Now()
		var finishStorage func(error)
		if telemetry != nil {
			storageCtx, _, finishStorage = observability.StartOperation(ctx, telemetry, observability.OperationStorageOperation, "storage")
			_ = metrics.New(telemetry).Request(storageCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "status": "started"})
		}
		capabilities, err = storageFactories[0].New(storageCtx, mustStorageInput(plan))
		if finishStorage != nil {
			finishStorage(err)
			status := "success"
			if err != nil {
				status = "error"
			}
			_ = metrics.New(telemetry).BackendDuration(storageCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "status": status, "error_class": observability.ErrorClass(err)})
		}
		if err != nil {
			return nil, fmt.Errorf("build runner: storage capability: %w", err)
		}
		sessions, err = capabilities.Session()
		if err != nil {
			_ = capabilities.Close()
			return nil, fmt.Errorf("build runner: session capability: %w", err)
		}
	}
	ownedCapabilities := capabilities != nil
	defer func() {
		if ownedCapabilities {
			_ = capabilities.Close()
		}
	}()
	scopedSessions, err := NewTenantSessionService(plan.Tenant(), sessions)
	if err != nil {
		return nil, fmt.Errorf("build runner: session scope: %w", err)
	}
	model, err := modelprofile.ResolveAndBuild(ctx, modelInput, resolver, factory)
	if err != nil {
		return nil, fmt.Errorf("build runner: model: %w", err)
	}
	llmOptions := llmAgentOptions(agentInput, model)
	if telemetry != nil {
		llmOptions = append(llmOptions, telemetryOptions(telemetry, modelInput.Provider, modelInput.Model)...)
	}
	llmAgent := llmagent.New(agentInput.Name, llmOptions...)
	delegate := trpcrunner.NewRunner(
		agentInput.AppID,
		llmAgent,
		trpcrunner.WithSessionService(scopedSessions),
	)
	runner := &policyRunner{
		delegate:     delegate,
		capabilities: capabilities,
		runOptions: []trpcagent.RunOption{
			trpcagent.WithMaxRunDuration(time.Duration(agentInput.Runtime.ExecutionTimeoutSeconds) * time.Second),
		},
	}
	ownedCapabilities = false
	return runner, nil
}

func llmAgentOptions(input agent.LLMAgentFactoryInput, model trpcmodel.Model) []llmagent.Option {
	return []llmagent.Option{
		llmagent.WithDescription(input.Description),
		llmagent.WithInstruction(input.Instruction),
		llmagent.WithGlobalInstruction(input.GlobalInstruction),
		llmagent.WithModel(model),
		llmagent.WithGenerationConfig(toTRPCGenerationConfig(input.Generation)),
		llmagent.WithMaxLLMCalls(input.Runtime.MaxLLMCalls),
		llmagent.WithMaxToolIterations(input.Runtime.MaxToolCalls),
		llmagent.WithEnableParallelTools(input.Runtime.EnableParallelTools),
		llmagent.WithToolConcurrencyConfig(trpctool.ConcurrencyConfig{MaxConcurrency: input.Runtime.MaxParallelTools}),
	}
}

type callbackStateKey struct{}
type callbackState struct {
	finish  func(error)
	started time.Time
}

func telemetryOptions(provider observability.Provider, providerName, modelFamily string) []llmagent.Option {
	catalog := metrics.New(provider)
	modelCallbacks := trpcmodel.NewCallbacks().RegisterBeforeModel(func(ctx context.Context, _ *trpcmodel.BeforeModelArgs) (*trpcmodel.BeforeModelResult, error) {
		started := time.Now()
		next, _, finish := observability.StartOperation(ctx, provider, observability.OperationModelCall, "model")
		_ = catalog.Request(next, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": providerName, "model_family": modelFamily, "status": "started"})
		return &trpcmodel.BeforeModelResult{Context: context.WithValue(next, callbackStateKey{}, callbackState{finish: finish, started: started})}, nil
	}).RegisterAfterModel(func(ctx context.Context, args *trpcmodel.AfterModelArgs) (*trpcmodel.AfterModelResult, error) {
		state, _ := ctx.Value(callbackStateKey{}).(callbackState)
		if state.finish != nil {
			var err error
			if args != nil {
				err = args.Error
				if args.Response != nil && args.Response.Usage != nil {
					usage := args.Response.Usage
					_ = catalog.Tokens(ctx, int64(usage.PromptTokens), map[string]string{"component": "model", "provider": providerName, "model_family": modelFamily})
					_ = catalog.Tokens(ctx, int64(usage.CompletionTokens), map[string]string{"component": "model", "provider": providerName, "model_family": modelFamily})
				}
			}
			state.finish(err)
			status := "success"
			if err != nil {
				status = "error"
			}
			_ = catalog.Duration(ctx, observability.DurationMilliseconds(state.started), map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": providerName, "model_family": modelFamily, "status": status, "error_class": observability.ErrorClass(err)})
		}
		return nil, nil
	})
	toolCallbacks := trpctool.NewCallbacks().RegisterBeforeTool(func(ctx context.Context, _ *trpctool.BeforeToolArgs) (*trpctool.BeforeToolResult, error) {
		started := time.Now()
		next, _, finish := observability.StartOperation(ctx, provider, observability.OperationToolCall, "tool")
		_ = catalog.Request(next, map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "started"})
		return &trpctool.BeforeToolResult{Context: context.WithValue(next, callbackStateKey{}, callbackState{finish: finish, started: started})}, nil
	}).RegisterAfterTool(func(ctx context.Context, args *trpctool.AfterToolArgs) (*trpctool.AfterToolResult, error) {
		state, _ := ctx.Value(callbackStateKey{}).(callbackState)
		if state.finish != nil {
			var err error
			if args != nil {
				err = args.Error
			}
			state.finish(err)
			status := "success"
			if err != nil {
				status = "error"
			}
			_ = catalog.Duration(ctx, observability.DurationMilliseconds(state.started), map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": status, "error_class": observability.ErrorClass(err)})
		}
		return nil, nil
	})
	return []llmagent.Option{llmagent.WithModelCallbacks(modelCallbacks), llmagent.WithToolCallbacks(toolCallbacks)}
}

// policyRunner preserves the published Agent runtime policy at the Runner
// boundary. Caller-provided options are applied first; the fixed policy is
// appended last so an execution cannot silently extend its control-plane limits.
type policyRunner struct {
	delegate     trpcrunner.Runner
	capabilities *backend.CapabilitySet
	runOptions   []trpcagent.RunOption
}

func (runner *policyRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	allOptions := make([]trpcagent.RunOption, 0, len(options)+len(runner.runOptions))
	allOptions = append(allOptions, options...)
	allOptions = append(allOptions, runner.runOptions...)
	return runner.delegate.Run(ctx, userID, sessionID, message, allOptions...)
}

func (runner *policyRunner) Close() error {
	if runner == nil {
		return nil
	}
	return errors.Join(runner.delegate.Close(), runner.capabilities.Close())
}

func mustStorageInput(plan ExecutionPlan) backend.StorageFactoryInput {
	input, _ := plan.StorageFactoryInput()
	return input
}

func toTRPCGenerationConfig(configuration agent.GenerationConfig) trpcmodel.GenerationConfig {
	return trpcmodel.GenerationConfig{
		Temperature: configuration.Temperature,
		TopP:        configuration.TopP,
		MaxTokens:   configuration.MaxOutputTokens,
	}
}

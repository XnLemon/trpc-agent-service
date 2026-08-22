package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
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
) (trpcrunner.Runner, error) {
	if ctx == nil {
		return nil, errors.New("invalid runner: context is required")
	}
	if sessions == nil {
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
	scopedSessions, err := NewTenantSessionService(plan.Tenant(), sessions)
	if err != nil {
		return nil, fmt.Errorf("build runner: session scope: %w", err)
	}
	model, err := modelprofile.ResolveAndBuild(ctx, modelInput, resolver, factory)
	if err != nil {
		return nil, fmt.Errorf("build runner: model: %w", err)
	}
	llmAgent := llmagent.New(agentInput.Name, llmAgentOptions(agentInput, model)...)
	delegate := trpcrunner.NewRunner(
		agentInput.AppID,
		llmAgent,
		trpcrunner.WithSessionService(scopedSessions),
	)
	return &policyRunner{
		delegate: delegate,
		runOptions: []trpcagent.RunOption{
			trpcagent.WithMaxRunDuration(time.Duration(agentInput.Runtime.ExecutionTimeoutSeconds) * time.Second),
		},
	}, nil
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

// policyRunner preserves the published Agent runtime policy at the Runner
// boundary. Caller-provided options are applied first; the fixed policy is
// appended last so an execution cannot silently extend its control-plane limits.
type policyRunner struct {
	delegate   trpcrunner.Runner
	runOptions []trpcagent.RunOption
}

func (runner *policyRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	allOptions := make([]trpcagent.RunOption, 0, len(options)+len(runner.runOptions))
	allOptions = append(allOptions, options...)
	allOptions = append(allOptions, runner.runOptions...)
	return runner.delegate.Run(ctx, userID, sessionID, message, allOptions...)
}

func (runner *policyRunner) Close() error { return runner.delegate.Close() }

func toTRPCGenerationConfig(configuration agent.GenerationConfig) trpcmodel.GenerationConfig {
	return trpcmodel.GenerationConfig{
		Temperature: configuration.Temperature,
		TopP:        configuration.TopP,
		MaxTokens:   configuration.MaxOutputTokens,
	}
}

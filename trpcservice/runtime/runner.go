package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
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
	llmAgent := llmagent.New(
		agentInput.Name,
		llmagent.WithDescription(agentInput.Description),
		llmagent.WithInstruction(agentInput.Instruction),
		llmagent.WithGlobalInstruction(agentInput.GlobalInstruction),
		llmagent.WithModel(model),
		llmagent.WithGenerationConfig(toTRPCGenerationConfig(agentInput.Generation)),
		llmagent.WithMaxLLMCalls(agentInput.Runtime.MaxLLMCalls),
		llmagent.WithMaxToolIterations(agentInput.Runtime.MaxToolCalls),
	)
	return trpcrunner.NewRunner(
		agentInput.AppID,
		llmAgent,
		trpcrunner.WithSessionService(scopedSessions),
	), nil
}

func toTRPCGenerationConfig(configuration agent.GenerationConfig) trpcmodel.GenerationConfig {
	return trpcmodel.GenerationConfig{
		Temperature: configuration.Temperature,
		TopP:        configuration.TopP,
		MaxTokens:   configuration.MaxOutputTokens,
	}
}

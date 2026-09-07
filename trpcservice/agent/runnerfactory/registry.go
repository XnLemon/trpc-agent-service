// Package runnerfactory assembles the external Agent runtime for the generic
// runtime Runner registry. It is the composition boundary between execution
// plans and tRPC-Agent-Go; runtime/runner itself only manages leases and
// lifecycle.
package runnerfactory

import (
	"context"
	"errors"
	"fmt"

	serviceagent "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// PluginFactory materializes runner-owned upstream plugins from a sealed plan.
// A factory must return fresh instances because the upstream Runner closes its
// plugins when the runner lease ends.
type PluginFactory func(context.Context, runtime.ExecutionPlan) ([]plugin.Plugin, error)

// ToolSetFactory materializes runner-owned upstream tool sets from a sealed
// plan. MCP implementations must fail closed before entering the Runner cache.
type ToolSetFactory func(context.Context, runtime.ExecutionPlan) ([]tool.ToolSet, error)

// Config wires the concrete external-agent assembly into a generic Runner
// registry. Session, Secret Resolver, Model Factory, and Storage Factory are
// borrowed by the resulting Runners and remain owned by the caller.
type Config struct {
	Registry             runtimerunner.RunnerRegistryConfig
	SecretResolver       modelprofile.SecretResolver
	ModelFactory         modelprofile.ModelFactory
	Sessions             session.Service
	StorageFactory       storagefactory.StorageFactory
	Observability        observability.Provider
	ToolRegistry         *servicetool.Registry
	AgentFactories       *serviceagent.AgentFactoryRegistry
	PluginFactory        PluginFactory
	ToolSetFactory       ToolSetFactory
	EnableUsageCallbacks bool
}

// NewRuntimeRunnerRegistry creates a generic Runner registry backed by the
// concrete Agent adapter. The concrete assembly deliberately lives in this
// package so runtime/runner stays independent of Agent implementations.
func NewRuntimeRunnerRegistry(config Config) (*runtimerunner.RunnerRegistry, error) {
	if config.ModelFactory == nil || (config.Sessions == nil && config.StorageFactory == nil) {
		return nil, fmt.Errorf("%w: runtime Runner dependencies are required", runtimerunner.ErrInvalid)
	}
	config.Registry.Factory = func(ctx context.Context, plan runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		input, err := runnerInputFromPlan(plan)
		if err != nil {
			return nil, err
		}
		toolSets, err := materializeToolSets(ctx, config.ToolSetFactory, plan)
		if err != nil {
			return nil, err
		}
		plugins, err := materializePlugins(ctx, config.PluginFactory, plan)
		if err != nil {
			_ = closeToolSets(toolSets)
			return nil, err
		}
		if config.StorageFactory != nil {
			return serviceagent.NewRunnerWithConfig(ctx, serviceagent.RunnerConfig{
				Input: input, SecretResolver: config.SecretResolver, ModelFactory: config.ModelFactory,
				Sessions: config.Sessions, StorageFactory: config.StorageFactory,
				Observability: config.Observability, ToolRegistry: config.ToolRegistry, AgentFactories: config.AgentFactories,
				Plugins: plugins, ToolSets: toolSets, EnableUsageCallbacks: config.EnableUsageCallbacks,
			})
		}
		return serviceagent.NewRunnerWithConfig(ctx, serviceagent.RunnerConfig{
			Input: input, SecretResolver: config.SecretResolver, ModelFactory: config.ModelFactory,
			Sessions: config.Sessions, Observability: config.Observability, ToolRegistry: config.ToolRegistry, AgentFactories: config.AgentFactories,
			Plugins: plugins, ToolSets: toolSets, EnableUsageCallbacks: config.EnableUsageCallbacks,
		})
	}
	return runtimerunner.NewRunnerRegistry(config.Registry)
}

func materializeToolSets(ctx context.Context, factory ToolSetFactory, plan runtime.ExecutionPlan) ([]tool.ToolSet, error) {
	if factory == nil {
		return nil, nil
	}
	toolSets, err := factory(ctx, plan)
	if err != nil {
		_ = closeToolSets(toolSets)
		return nil, fmt.Errorf("%w: materialize tool sets", runtimerunner.ErrInvalid)
	}
	seen := make(map[string]struct{}, len(toolSets))
	for _, candidate := range toolSets {
		if candidate == nil || candidate.Name() == "" {
			_ = closeToolSets(toolSets)
			return nil, fmt.Errorf("%w: invalid tool set", runtimerunner.ErrInvalid)
		}
		if _, duplicate := seen[candidate.Name()]; duplicate {
			_ = closeToolSets(toolSets)
			return nil, fmt.Errorf("%w: duplicate tool set", runtimerunner.ErrInvalid)
		}
		seen[candidate.Name()] = struct{}{}
	}
	return toolSets, nil
}

func closeToolSets(toolSets []tool.ToolSet) error {
	var errs []error
	for index := len(toolSets) - 1; index >= 0; index-- {
		if toolSets[index] != nil {
			errs = append(errs, toolSets[index].Close())
		}
	}
	return errors.Join(errs...)
}

func materializePlugins(ctx context.Context, factory PluginFactory, plan runtime.ExecutionPlan) ([]plugin.Plugin, error) {
	if factory == nil {
		return nil, nil
	}
	plugins, err := factory(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("%w: materialize plugins", runtimerunner.ErrInvalid)
	}
	for _, candidate := range plugins {
		if candidate == nil || candidate.Name() == "" {
			return nil, fmt.Errorf("%w: invalid plugin", runtimerunner.ErrInvalid)
		}
	}
	return plugins, nil
}

func runnerInputFromPlan(plan runtime.ExecutionPlan) (serviceagent.RunnerInput, error) {
	agentInput, err := plan.AgentFactoryInput()
	if err != nil {
		return serviceagent.RunnerInput{}, err
	}
	modelInput, err := plan.ModelFactoryInput()
	if err != nil {
		return serviceagent.RunnerInput{}, err
	}
	storageInput, err := plan.StorageFactoryInput()
	if err != nil {
		return serviceagent.RunnerInput{}, err
	}
	return serviceagent.RunnerInput{
		Tenant: plan.Tenant(), Agent: agentInput, Model: modelInput, Storage: storageInput,
	}, nil
}

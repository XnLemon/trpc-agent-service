// Package runnerfactory assembles the external Agent runtime for the generic
// runtime Runner registry. It is the composition boundary between execution
// plans and tRPC-Agent-Go; runtime/runner itself only manages leases and
// lifecycle.
package runnerfactory

import (
	"context"
	"fmt"

	serviceagent "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Config wires the concrete external-agent assembly into a generic Runner
// registry. Session, Secret Resolver, Model Factory, and Storage Factory are
// borrowed by the resulting Runners and remain owned by the caller.
type Config struct {
	Registry       runtimerunner.RunnerRegistryConfig
	SecretResolver modelprofile.SecretResolver
	ModelFactory   modelprofile.ModelFactory
	Sessions       session.Service
	StorageFactory storagefactory.StorageFactory
	Observability  observability.Provider
	ToolRegistry   *servicetool.Registry
}

// NewRuntimeRunnerRegistry creates a generic Runner registry backed by the
// concrete Agent adapter. The concrete assembly deliberately lives in this
// package so runtime/runner stays independent of Agent implementations.
func NewRuntimeRunnerRegistry(config Config) (*runtimerunner.RunnerRegistry, error) {
	if config.ModelFactory == nil || (config.Sessions == nil && config.StorageFactory == nil) {
		return nil, fmt.Errorf("%w: runtime Runner dependencies are required", runtimerunner.ErrInvalid)
	}
	config.Registry.Factory = func(ctx context.Context, plan runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		input, err := plan.AgentRunnerInput()
		if err != nil {
			return nil, err
		}
		if config.StorageFactory != nil {
			return serviceagent.NewRunnerWithToolRegistry(ctx, input, config.SecretResolver, config.ModelFactory, config.Sessions, config.Observability, config.ToolRegistry, config.StorageFactory)
		}
		return serviceagent.NewRunnerWithToolRegistry(ctx, input, config.SecretResolver, config.ModelFactory, config.Sessions, config.Observability, config.ToolRegistry)
	}
	return runtimerunner.NewRunnerRegistry(config.Registry)
}

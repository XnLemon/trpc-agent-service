package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// RunnerConfig groups the dependencies used to materialize one external-agent
// Runner. Session, registries, factories, and Observability are borrowed by
// the returned Runner; the optional StorageFactory produces capabilities owned
// by that Runner. Plugin and ToolSet ownership transfers to the returned Runner;
// both are closed with it.
type RunnerConfig struct {
	Input                RunnerInput
	SecretResolver       modelprofile.SecretResolver
	ModelFactory         modelprofile.ModelFactory
	Sessions             session.Service
	StorageFactory       storagefactory.StorageFactory
	Observability        observability.Provider
	ToolRegistry         *servicetool.Registry
	AgentFactories       *AgentFactoryRegistry
	Plugins              []plugin.Plugin
	ToolSets             []trpctool.ToolSet
	EnableUsageCallbacks bool
}

// NewRunnerWithConfig materializes one Runner from an explicit dependency
// group.
func NewRunnerWithConfig(ctx context.Context, config RunnerConfig) (trpcrunner.Runner, error) {
	if err := validateRunnerConfig(ctx, config); err != nil {
		return nil, err
	}
	pluginsOwned := true
	defer func() {
		if pluginsOwned {
			_ = closePlugins(config.Plugins)
		}
	}()
	toolSetsOwned := true
	defer func() {
		if toolSetsOwned {
			_ = closeToolSets(config.ToolSets)
		}
	}()
	if err := initializeToolSets(ctx, config.ToolSets); err != nil {
		return nil, err
	}
	resources, err := materializeRunnerResources(ctx, config)
	if err != nil {
		return nil, err
	}
	owned := resources.capabilities != nil
	defer func() {
		if owned {
			_ = resources.capabilities.Close()
		}
	}()
	runner, err := assembleRunner(ctx, config, resources)
	if err != nil {
		return nil, err
	}
	owned = false
	pluginsOwned = false
	toolSetsOwned = false
	return runner, nil
}

type initializableToolSet interface {
	Init(context.Context) error
}

func initializeToolSets(ctx context.Context, toolSets []trpctool.ToolSet) error {
	for _, toolSet := range toolSets {
		if toolSet == nil || toolSet.Name() == "" {
			return errors.New("build runner: invalid tool set")
		}
		if initializable, ok := toolSet.(initializableToolSet); ok {
			if err := initializable.Init(ctx); err != nil {
				return fmt.Errorf("build runner: initialize tool set: %w", err)
			}
		}
	}
	return nil
}

func closePlugins(plugins []plugin.Plugin) error {
	errs := make([]error, 0, len(plugins))
	for index := len(plugins) - 1; index >= 0; index-- {
		if closer, ok := plugins[index].(plugin.Closer); ok {
			errs = append(errs, closer.Close(context.Background()))
		}
	}
	return errors.Join(errs...)
}

func closeToolSets(toolSets []trpctool.ToolSet) error {
	errs := make([]error, 0, len(toolSets))
	for index := len(toolSets) - 1; index >= 0; index-- {
		if toolSets[index] != nil {
			errs = append(errs, toolSets[index].Close())
		}
	}
	return errors.Join(errs...)
}

type runnerResources struct {
	sessions     session.Service
	memory       memory.Service
	artifact     artifact.Service
	knowledge    knowledge.Knowledge
	capabilities *storagefactory.CapabilitySet
}

func validateRunnerConfig(ctx context.Context, config RunnerConfig) error {
	if ctx == nil {
		return errors.New("invalid runner: context is required")
	}
	if config.Sessions == nil && config.StorageFactory == nil {
		return errors.New("invalid runner: session service is required")
	}
	return nil
}

func materializeRunnerResources(ctx context.Context, config RunnerConfig) (runnerResources, error) {
	resources := runnerResources{sessions: config.Sessions}
	if config.StorageFactory == nil {
		return resources, nil
	}
	capabilities, err := materializeStorageCapabilities(ctx, config)
	if err != nil {
		return runnerResources{}, err
	}
	if capabilities == nil {
		return runnerResources{}, fmt.Errorf("build runner: storage capability: %w", storagefactory.ErrStorageFactory)
	}
	sessions, err := capabilities.Session()
	if err != nil {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: session capability: %w", err)
	}
	resources.sessions = sessions
	if service, serviceErr := capabilities.Memory(); serviceErr == nil {
		resources.memory = service
	} else if !errors.Is(serviceErr, storagefactory.ErrCapabilityUnavailable) {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: memory capability: %w", serviceErr)
	}
	if service, serviceErr := capabilities.Artifact(); serviceErr == nil {
		resources.artifact = service
	} else if !errors.Is(serviceErr, storagefactory.ErrCapabilityUnavailable) {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: artifact capability: %w", serviceErr)
	}
	if service, serviceErr := capabilities.Knowledge(); serviceErr == nil {
		resources.knowledge = service
	} else if !errors.Is(serviceErr, storagefactory.ErrCapabilityUnavailable) {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: knowledge capability: %w", serviceErr)
	}
	resources.capabilities = capabilities
	return resources, nil
}

func materializeStorageCapabilities(ctx context.Context, config RunnerConfig) (*storagefactory.CapabilitySet, error) {
	storageCtx := ctx
	started := time.Now()
	var finishStorage func(error)
	storageMetrics := metrics.New(config.Observability)
	if config.Observability != nil {
		storageCtx, _, finishStorage = observability.StartOperation(ctx, config.Observability, observability.OperationStorageOperation, "storage")
		_ = storageMetrics.Request(storageCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other", "status": "started"})
	}
	capabilities, err := config.StorageFactory.New(storageCtx, config.Input.Storage)
	if finishStorage != nil {
		finishStorage(err)
		_ = storageMetrics.Operation(storageCtx, started, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other"}, err)
		status := "success"
		if err != nil {
			status = observability.ErrorClass(err)
			if status == "" {
				status = "error"
			}
		}
		_ = storageMetrics.BackendDuration(storageCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "provider": "other", "status": status, "error_class": observability.ErrorClass(err)})
	}
	if err != nil {
		return nil, fmt.Errorf("build runner: storage capability: %w", err)
	}
	return capabilities, nil
}

func withoutKnowledgeAuthorization(authorizations []appmodel.ToolAuthorization) []appmodel.ToolAuthorization {
	filtered := make([]appmodel.ToolAuthorization, 0, len(authorizations))
	for _, authorization := range authorizations {
		if authorization.ToolID != "knowledge_search" {
			filtered = append(filtered, authorization)
		}
	}
	return filtered
}

func validateToolSetDeclarations(ctx context.Context, tools []trpctool.Tool, toolSets []trpctool.ToolSet) error {
	seen := make(map[string]struct{}, len(tools))
	for _, candidate := range tools {
		if candidate == nil || candidate.Declaration() == nil || candidate.Declaration().Name == "" {
			return errors.New("build runner: invalid tool declaration")
		}
		name := candidate.Declaration().Name
		if _, exists := seen[name]; exists {
			return fmt.Errorf("build runner: duplicate tool declaration %q", name)
		}
		seen[name] = struct{}{}
	}
	for _, set := range toolSets {
		if set == nil || set.Name() == "" {
			return errors.New("build runner: invalid tool set")
		}
		for _, candidate := range set.Tools(ctx) {
			if candidate == nil || candidate.Declaration() == nil || candidate.Declaration().Name == "" {
				return fmt.Errorf("build runner: invalid tool declaration in %q", set.Name())
			}
			name := candidate.Declaration().Name
			if _, exists := seen[name]; exists {
				return fmt.Errorf("build runner: duplicate tool declaration %q", name)
			}
			seen[name] = struct{}{}
		}
	}
	return nil
}

func authorizedKnowledge(authorizations []appmodel.ToolAuthorization, service knowledge.Knowledge) knowledge.Knowledge {
	if service == nil {
		return nil
	}
	for _, authorization := range authorizations {
		if authorization.ToolID == "knowledge_search" {
			return service
		}
	}
	return nil
}

type authorizedMemoryService struct {
	base        memory.Service
	permissions map[string]bool
	autoExtract bool
}

func newAuthorizedMemoryService(base memory.Service, authorizations []appmodel.ToolAuthorization) memory.Service {
	permissions := make(map[string]bool, len(authorizations))
	for _, authorization := range authorizations {
		permissions[authorization.ToolID] = true
	}
	return authorizedMemoryService{base: base, permissions: permissions, autoExtract: permissions["memory_auto_extract"]}
}

func (service authorizedMemoryService) allows(name string) bool {
	return service.permissions[name]
}

func (service authorizedMemoryService) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, opts ...memory.AddOption) error {
	if !service.allows(memory.AddToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	return service.base.AddMemory(ctx, key, value, topics, opts...)
}

func (service authorizedMemoryService) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) error {
	if !service.allows(memory.UpdateToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	return service.base.UpdateMemory(ctx, key, value, topics, opts...)
}

func (service authorizedMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	if !service.allows(memory.DeleteToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	return service.base.DeleteMemory(ctx, key)
}

func (service authorizedMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	if !service.allows(memory.ClearToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	return service.base.ClearMemories(ctx, key)
}

func (service authorizedMemoryService) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	if !service.allows(memory.LoadToolName) && !service.allows(memory.SearchToolName) {
		return nil, storagefactory.ErrCapabilityUnavailable
	}
	return service.base.ReadMemories(ctx, key, limit)
}

func (service authorizedMemoryService) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	if !service.allows(memory.SearchToolName) {
		return nil, storagefactory.ErrCapabilityUnavailable
	}
	return service.base.SearchMemories(ctx, key, query, opts...)
}

func (service authorizedMemoryService) Tools() []trpctool.Tool {
	tools := service.base.Tools()
	filtered := make([]trpctool.Tool, 0, len(tools))
	for _, candidate := range tools {
		if candidate == nil || candidate.Declaration() == nil || service.allows(candidate.Declaration().Name) {
			if candidate != nil && candidate.Declaration() != nil {
				filtered = append(filtered, candidate)
			}
		}
	}
	return filtered
}

func (service authorizedMemoryService) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	if !service.autoExtract {
		return nil
	}
	return service.base.EnqueueAutoMemoryJob(ctx, sess)
}

func (service authorizedMemoryService) Close() error {
	if service.base == nil {
		return nil
	}
	return service.base.Close()
}

func assembleRunner(ctx context.Context, config RunnerConfig, resources runnerResources) (trpcrunner.Runner, error) {
	agentInput := config.Input.Agent.Clone()
	scopedSessions, err := NewTenantSessionService(config.Input.Tenant, resources.sessions)
	if err != nil {
		return nil, fmt.Errorf("build runner: session scope: %w", err)
	}
	model, err := modelruntime.ResolveAndBuild(ctx, config.Input.Model, config.SecretResolver, config.ModelFactory)
	if err != nil {
		return nil, fmt.Errorf("build runner: model: %w", err)
	}
	if resources.memory != nil {
		resources.memory = newAuthorizedMemoryService(resources.memory, agentInput.Tools)
	}
	telemetryProvider := config.Observability
	if telemetryProvider == nil && config.EnableUsageCallbacks {
		telemetryProvider = observability.NewNoopProvider()
	}
	if telemetryProvider != nil {
		model = wrapTelemetryModel(model)
	}
	toolRegistry := config.ToolRegistry
	if toolRegistry == nil {
		toolRegistry = servicetool.DefaultRegistry()
	}
	var nativeMemoryTools []trpctool.Tool
	if resources.memory != nil {
		nativeMemoryTools = resources.memory.Tools()
	}
	tools, err := toolRegistry.ResolveWith(withoutKnowledgeAuthorization(agentInput.Tools), nativeMemoryTools...)
	if err != nil {
		return nil, fmt.Errorf("build runner: tools: %w", err)
	}
	if err := validateToolSetDeclarations(ctx, tools, config.ToolSets); err != nil {
		return nil, err
	}
	knowledgeService := authorizedKnowledge(agentInput.Tools, resources.knowledge)
	modelOptions := []llmagent.Option(nil)
	if telemetryProvider != nil {
		modelOptions = append(modelOptions, telemetryOptions(telemetryProvider, config.Input.Model.Provider, config.Input.Model.Model)...)
	}
	factories := config.AgentFactories
	if factories == nil {
		factories = DefaultAgentFactoryRegistry()
	}
	builtAgent, err := factories.Build(ctx, AgentBuildInput{
		Definition: agentInput, Model: model, Tools: tools, ToolSets: config.ToolSets, Knowledge: knowledgeService, ModelOptions: modelOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("build runner: Agent Factory: %w", err)
	}
	runnerOptions := []trpcrunner.Option{trpcrunner.WithSessionService(scopedSessions)}
	if len(config.Plugins) > 0 {
		runnerOptions = append(runnerOptions, trpcrunner.WithPlugins(config.Plugins...))
	}
	if resources.memory != nil {
		runnerOptions = append(runnerOptions, trpcrunner.WithMemoryService(resources.memory))
	}
	if resources.artifact != nil {
		runnerOptions = append(runnerOptions, trpcrunner.WithArtifactService(resources.artifact))
	}
	delegate := trpcrunner.NewRunner(agentInput.AppID, builtAgent, runnerOptions...)
	return &policyRunner{
		delegate:     delegate,
		capabilities: resources.capabilities,
		toolSets:     config.ToolSets,
		runOptions: []trpcagent.RunOption{
			trpcagent.WithMaxRunDuration(time.Duration(agentInput.Runtime.ExecutionTimeoutSeconds) * time.Second),
		},
	}, nil
}

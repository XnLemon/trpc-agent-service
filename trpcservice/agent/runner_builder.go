package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	skillsecurity "github.com/XnLemon/trpc-agent-service/trpcservice/skill"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/skill"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// RunnerConfig groups the dependencies used to materialize one external-agent
// Runner. Session, registries, factories, and Observability are borrowed by
// the returned Runner; the optional StorageFactory produces capabilities owned
// by that Runner. Plugin and ToolSet ownership transfers to the returned Runner;
// both are closed with it.
type RunnerConfig struct {
	Input                   RunnerInput
	SecretResolver          modelprofile.SecretResolver
	ModelFactory            modelprofile.ModelFactory
	Sessions                session.Service
	StorageFactory          storagefactory.StorageFactory
	Observability           observability.Provider
	ToolRegistry            *servicetool.Registry
	AgentFactories          *AgentFactoryRegistry
	SkillRepositoryProvider skill.RepositoryProvider
	// SkillTrustPolicy is process-owned trust configuration for revision-pinned
	// skill manifests. A zero policy intentionally rejects declared Skills.
	SkillTrustPolicy skillsecurity.TrustPolicy
	// ToolInvocationStore is fixed into the policy Runner and injected into
	// direct Runner calls as well as Gateway dispatches. A caller cannot
	// replace the app-scoped side-effect ledger through a context value.
	ToolInvocationStore  runtimestorage.ToolInvocationStore
	Plugins              []plugin.Plugin
	ToolSets             []trpctool.ToolSet
	EnableUsageCallbacks bool
}

// NewRunnerWithConfig materializes one Runner from an explicit dependency
// group.
func NewRunnerWithConfig(ctx context.Context, config RunnerConfig) (runner trpcrunner.Runner, err error) {
	// Plugin and ToolSet ownership starts at this boundary. Even a malformed
	// context or dependency group must not strand caller-provided resources
	// that were assembled immediately before this call.
	pluginsOwned := true
	defer func() {
		if err != nil && pluginsOwned {
			_ = closePlugins(config.Plugins)
		}
	}()
	toolSetsOwned := true
	defer func() {
		if err != nil && toolSetsOwned {
			_ = closeToolSets(config.ToolSets)
		}
	}()
	if err = validateRunnerConfig(ctx, config); err != nil {
		return nil, err
	}
	if isNilAgentValue(config.Sessions) {
		config.Sessions = nil
	}
	if isNilAgentValue(config.StorageFactory) {
		config.StorageFactory = nil
	}
	if isNilAgentValue(config.SecretResolver) {
		config.SecretResolver = nil
	}
	if isNilAgentValue(config.ToolInvocationStore) {
		config.ToolInvocationStore = nil
	}
	// Backend profiles are tenant-owned, so their sealed input does not carry
	// an App. Every direct Runner construction still receives the immutable App
	// selected by the Agent snapshot before a provider can materialize a
	// Memory/Knowledge/Artifact client.
	if config.Input.Storage.AppID != "" && config.Input.Storage.AppID != config.Input.Agent.AppID {
		return nil, fmt.Errorf("%w: storage App scope does not match Agent App", ErrInvalid)
	}
	config.Input.Storage.AppID = config.Input.Agent.AppID
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
	runner, err = callAssembleRunner(ctx, config, resources)
	if err != nil {
		return nil, err
	}
	if isNilAgentValue(runner) {
		return nil, errors.New("build runner: assembled runner is nil")
	}
	if contextErr := agentContextErr(ctx); contextErr != nil {
		_ = closeAgentRunner(runner)
		owned = false
		pluginsOwned = false
		toolSetsOwned = false
		return nil, contextErr
	}
	owned = false
	pluginsOwned = false
	toolSetsOwned = false
	return runner, nil
}

func callAssembleRunner(ctx context.Context, config RunnerConfig, resources runnerResources) (runner trpcrunner.Runner, err error) {
	defer func() {
		if recover() != nil {
			runner = nil
			err = errors.New("build runner: assembly failed")
		}
	}()
	return assembleRunner(ctx, config, resources)
}

type initializableToolSet interface {
	Init(context.Context) error
}

func initializeToolSets(ctx context.Context, toolSets []trpctool.ToolSet) error {
	if isNilAgentValue(ctx) {
		return errors.New("build runner: context is required")
	}
	if err := agentContextErr(ctx); err != nil {
		return err
	}
	for _, toolSet := range toolSets {
		if isNilAgentValue(toolSet) || !validToolSetName(toolSet) {
			return errors.New("build runner: invalid tool set")
		}
		if initializable, ok := toolSet.(initializableToolSet); ok && !isNilAgentValue(initializable) {
			if err := callToolSetInit(ctx, initializable); err != nil {
				return fmt.Errorf("build runner: initialize tool set: %w", err)
			}
		}
	}
	return nil
}

func closePlugins(plugins []plugin.Plugin) error {
	errs := make([]error, 0, len(plugins))
	for index := len(plugins) - 1; index >= 0; index-- {
		if closer, ok := plugins[index].(plugin.Closer); ok && !isNilAgentValue(closer) {
			if err := closePlugin(closer); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func closeToolSets(toolSets []trpctool.ToolSet) error {
	errs := make([]error, 0, len(toolSets))
	for index := len(toolSets) - 1; index >= 0; index-- {
		if !isNilAgentValue(toolSets[index]) {
			if err := closeToolSet(toolSets[index]); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func closePlugin(closer plugin.Closer) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("build runner: plugin close failed")
		}
	}()
	return closer.Close(context.Background())
}

func closeToolSet(set trpctool.ToolSet) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("build runner: tool set close failed")
		}
	}()
	return set.Close()
}

func callToolSetInit(ctx context.Context, initializable initializableToolSet) (err error) {
	if isNilAgentValue(ctx) || isNilAgentValue(initializable) {
		return errors.New("build runner: tool set is unavailable")
	}
	defer func() {
		if recover() != nil {
			err = errors.New("build runner: tool set initialization failed")
		}
	}()
	return initializable.Init(ctx)
}

func validToolSetName(set trpctool.ToolSet) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return strings.TrimSpace(set.Name()) != ""
}

type runnerResources struct {
	sessions     session.Service
	memory       memory.Service
	artifact     artifact.Service
	knowledge    knowledge.Knowledge
	capabilities *storagefactory.CapabilitySet
}

func validateRunnerConfig(ctx context.Context, config RunnerConfig) error {
	if isNilAgentValue(ctx) {
		return errors.New("invalid runner: context is required")
	}
	if err := agentContextErr(ctx); err != nil {
		return err
	}
	if isNilAgentValue(config.Sessions) && isNilAgentValue(config.StorageFactory) {
		return errors.New("invalid runner: session service is required")
	}
	if isNilAgentValue(config.ModelFactory) {
		return errors.New("invalid runner: model factory is required")
	}
	return nil
}

func materializeRunnerResources(ctx context.Context, config RunnerConfig) (runnerResources, error) {
	resources := runnerResources{sessions: config.Sessions}
	if isNilAgentValue(config.StorageFactory) {
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
	if service, serviceErr := capabilities.Memory(); serviceErr == nil && !isNilAgentValue(service) {
		resources.memory = service
	} else if !errors.Is(serviceErr, storagefactory.ErrCapabilityUnavailable) {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: memory capability: %w", serviceErr)
	}
	if service, serviceErr := capabilities.Artifact(); serviceErr == nil && !isNilAgentValue(service) {
		resources.artifact = service
	} else if !errors.Is(serviceErr, storagefactory.ErrCapabilityUnavailable) {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: artifact capability: %w", serviceErr)
	}
	if service, serviceErr := capabilities.Knowledge(); serviceErr == nil && !isNilAgentValue(service) {
		resources.knowledge = service
	} else if !errors.Is(serviceErr, storagefactory.ErrCapabilityUnavailable) {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: knowledge capability: %w", serviceErr)
	}
	resources.capabilities = capabilities
	return resources, nil
}

func callStorageFactory(ctx context.Context, factory storagefactory.StorageFactory, input backend.StorageFactoryInput) (capabilities *storagefactory.CapabilitySet, err error) {
	if isNilAgentValue(ctx) || isNilAgentValue(factory) {
		return nil, errors.New("build runner: storage factory is unavailable")
	}
	defer func() {
		if recover() != nil {
			capabilities = nil
			err = errors.New("build runner: storage factory failed")
		}
	}()
	return factory.New(ctx, input)
}

func materializeStorageCapabilities(ctx context.Context, config RunnerConfig) (*storagefactory.CapabilitySet, error) {
	storageCtx := ctx
	started := time.Now()
	var finishStorage func(error)
	storageMetrics := metrics.New(config.Observability)
	if !isNilAgentValue(config.Observability) {
		storageCtx, _, finishStorage = observability.StartOperation(ctx, config.Observability, observability.OperationStorageOperation, "storage")
		_ = storageMetrics.Request(storageCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other", "status": "started"})
	}
	capabilities, err := callStorageFactory(storageCtx, config.StorageFactory, config.Input.Storage)
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
	if err := agentContextErr(ctx); err != nil {
		return errors.New("build runner: context is required")
	}
	seen := make(map[string]struct{}, len(tools))
	for _, candidate := range tools {
		declaration, ok := safeAgentToolDeclaration(candidate)
		if !ok || strings.TrimSpace(declaration.Name) == "" {
			return errors.New("build runner: invalid tool declaration")
		}
		name := declaration.Name
		if _, exists := seen[name]; exists {
			return fmt.Errorf("build runner: duplicate tool declaration %q", name)
		}
		seen[name] = struct{}{}
	}
	for _, set := range toolSets {
		name, nameOK := safeAgentToolSetName(set)
		if !nameOK || strings.TrimSpace(name) == "" {
			return errors.New("build runner: invalid tool set")
		}
		candidates, toolsOK := safeAgentToolSetTools(ctx, set)
		if !toolsOK {
			return fmt.Errorf("build runner: tool set %q is unavailable", name)
		}
		for _, candidate := range candidates {
			declaration, declarationOK := safeAgentToolDeclaration(candidate)
			if !declarationOK || strings.TrimSpace(declaration.Name) == "" {
				return fmt.Errorf("build runner: invalid tool declaration in %q", name)
			}
			toolName := declaration.Name
			if _, exists := seen[toolName]; exists {
				return fmt.Errorf("build runner: duplicate tool declaration %q", toolName)
			}
			seen[toolName] = struct{}{}
		}
	}
	return nil
}

func safeAgentToolSetName(set trpctool.ToolSet) (name string, ok bool) {
	if isNilAgentValue(set) {
		return "", false
	}
	defer func() {
		if recover() != nil {
			name, ok = "", false
		}
	}()
	name = set.Name()
	return name, name != ""
}

func safeAgentToolSetTools(ctx context.Context, set trpctool.ToolSet) (tools []trpctool.Tool, ok bool) {
	if agentContextErr(ctx) != nil || isNilAgentValue(set) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			tools, ok = nil, false
		}
	}()
	return set.Tools(ctx), true
}

func safeAgentToolDeclaration(candidate trpctool.Tool) (declaration *trpctool.Declaration, ok bool) {
	if isNilAgentValue(candidate) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			declaration, ok = nil, false
		}
	}()
	declaration = candidate.Declaration()
	return declaration, declaration != nil
}

func safeMemoryTools(service memory.Service) (tools []trpctool.Tool, ok bool) {
	if isNilAgentValue(service) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			tools, ok = nil, false
		}
	}()
	return service.Tools(), true
}

func callToolRegistryResolve(registry *servicetool.Registry, authorizations []appmodel.ToolAuthorization, candidates ...trpctool.Tool) (tools []trpctool.Tool, err error) {
	if registry == nil {
		return nil, errors.New("build runner: tool registry is unavailable")
	}
	defer func() {
		if recover() != nil {
			tools, err = nil, errors.New("build runner: tool registry failed")
		}
	}()
	return registry.ResolveWith(authorizations, candidates...)
}

func authorizedKnowledge(authorizations []appmodel.ToolAuthorization, service knowledge.Knowledge, tenantID, appID string) knowledge.Knowledge {
	if isNilAgentValue(service) {
		return nil
	}
	for _, authorization := range authorizations {
		if authorization.ToolID == "knowledge_search" {
			return newScopedKnowledge(service, tenantID, appID)
		}
	}
	return nil
}

type authorizedMemoryService struct {
	base        memory.Service
	permissions map[string]bool
	autoExtract bool
	tenantID    string
	appID       string
}

func newAuthorizedMemoryService(base memory.Service, authorizations []appmodel.ToolAuthorization) memory.Service {
	return newAuthorizedMemoryServiceScoped(base, authorizations, "", "")
}

func newAuthorizedMemoryServiceScoped(base memory.Service, authorizations []appmodel.ToolAuthorization, tenantID, appID string) memory.Service {
	permissions := make(map[string]bool, len(authorizations))
	for _, authorization := range authorizations {
		permissions[authorization.ToolID] = true
	}
	return authorizedMemoryService{base: base, permissions: permissions, autoExtract: permissions["memory_auto_extract"], tenantID: tenantID, appID: appID}
}

func (service authorizedMemoryService) allows(name string) bool {
	return service.permissions[name]
}

func (service authorizedMemoryService) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, opts ...memory.AddOption) error {
	if !service.allows(memory.AddToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	if err := service.checkUserKey(ctx, key.AppName, key.UserID); err != nil {
		return err
	}
	return service.base.AddMemory(ctx, key, value, topics, opts...)
}

func (service authorizedMemoryService) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) error {
	if !service.allows(memory.UpdateToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	if err := service.checkUserKey(ctx, key.AppName, key.UserID); err != nil {
		return err
	}
	return service.base.UpdateMemory(ctx, key, value, topics, opts...)
}

func (service authorizedMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	if !service.allows(memory.DeleteToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	if err := service.checkUserKey(ctx, key.AppName, key.UserID); err != nil {
		return err
	}
	return service.base.DeleteMemory(ctx, key)
}

func (service authorizedMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	if !service.allows(memory.ClearToolName) {
		return storagefactory.ErrCapabilityUnavailable
	}
	if err := service.checkUserKey(ctx, key.AppName, key.UserID); err != nil {
		return err
	}
	return service.base.ClearMemories(ctx, key)
}

func (service authorizedMemoryService) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	if !service.allows(memory.LoadToolName) && !service.allows(memory.SearchToolName) {
		return nil, storagefactory.ErrCapabilityUnavailable
	}
	if err := service.checkUserKey(ctx, key.AppName, key.UserID); err != nil {
		return nil, err
	}
	return service.base.ReadMemories(ctx, key, limit)
}

func (service authorizedMemoryService) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	if !service.allows(memory.SearchToolName) {
		return nil, storagefactory.ErrCapabilityUnavailable
	}
	if err := service.checkUserKey(ctx, key.AppName, key.UserID); err != nil {
		return nil, err
	}
	return service.base.SearchMemories(ctx, key, query, opts...)
}

func (service authorizedMemoryService) checkUserKey(ctx context.Context, appName, userID string) error {
	if service.appID == "" && service.tenantID == "" {
		return nil
	}
	metadata, ok := ExecutionMetadataFromContext(ctx)
	expectedApp := tenantScopedIdentifier(service.tenantID, service.appID)
	expectedUser := ""
	if ok {
		expectedUser = tenantScopedIdentifier(metadata.TenantID, metadata.UserID)
	}
	if !ok || metadata.TenantID != service.tenantID || metadata.AppID != service.appID || appName != expectedApp || userID != expectedUser {
		return storagefactory.ErrCapabilityUnavailable
	}
	return nil
}

func (service authorizedMemoryService) Tools() []trpctool.Tool {
	tools := service.base.Tools()
	filtered := make([]trpctool.Tool, 0, len(tools))
	for _, candidate := range tools {
		if isNilAgentValue(candidate) || candidate.Declaration() == nil || service.allows(candidate.Declaration().Name) {
			if !isNilAgentValue(candidate) && candidate.Declaration() != nil {
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
	if sess != nil && service.checkUserKey(ctx, sess.AppName, sess.UserID) != nil {
		return storagefactory.ErrCapabilityUnavailable
	}
	if sess == nil && (service.appID != "" || service.tenantID != "") {
		return storagefactory.ErrCapabilityUnavailable
	}
	return service.base.EnqueueAutoMemoryJob(ctx, sess)
}

func (service authorizedMemoryService) Close() error {
	if isNilAgentValue(service.base) {
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
	modelOwned := true
	defer func() {
		if modelOwned {
			_ = closeAgentModel(model)
		}
	}()
	if !isNilAgentValue(resources.memory) {
		resources.memory = newAuthorizedMemoryServiceScoped(resources.memory, agentInput.Tools, agentInput.TenantID, agentInput.AppID)
	}
	if !isNilAgentValue(resources.artifact) {
		resources.artifact = newScopedArtifactService(resources.artifact, agentInput.TenantID, agentInput.AppID)
	}
	telemetryProvider := config.Observability
	if isNilAgentValue(telemetryProvider) && config.EnableUsageCallbacks {
		telemetryProvider = observability.NewNoopProvider()
	}
	if !isNilAgentValue(telemetryProvider) {
		model = wrapTelemetryModel(model)
	}
	toolRegistry := config.ToolRegistry
	if toolRegistry == nil {
		toolRegistry = servicetool.DefaultRegistry()
	}
	var nativeMemoryTools []trpctool.Tool
	if !isNilAgentValue(resources.memory) {
		var toolsOK bool
		nativeMemoryTools, toolsOK = safeMemoryTools(resources.memory)
		if !toolsOK {
			return nil, errors.New("build runner: memory tools are unavailable")
		}
	}
	tools, err := callToolRegistryResolve(toolRegistry, withoutKnowledgeAuthorization(agentInput.Tools), nativeMemoryTools...)
	if err != nil {
		return nil, fmt.Errorf("build runner: tools: %w", err)
	}
	if err := validateToolSetDeclarations(ctx, tools, config.ToolSets); err != nil {
		return nil, err
	}
	knowledgeService := authorizedKnowledge(agentInput.Tools, resources.knowledge, agentInput.TenantID, agentInput.AppID)
	var skillProvider skill.RepositoryProvider
	if len(agentInput.Skills) > 0 {
		if isNilAgentValue(config.SkillRepositoryProvider) {
			return nil, fmt.Errorf("build runner: skill repository provider is required")
		}
		if len(agentInput.SkillAuthorizations) != len(agentInput.Skills) {
			return nil, fmt.Errorf("build runner: every Skill requires a pinned authorization")
		}
		if _, trustErr := config.SkillTrustPolicy.Normalize(); trustErr != nil {
			return nil, fmt.Errorf("build runner: skill trust policy: %w", trustErr)
		}
		skillProvider = newSecuredTenantSkillRepositoryProvider(config.SkillRepositoryProvider, agentInput.TenantID, agentInput.AppID, agentInput.Revision, agentInput.SkillAuthorizations, config.SkillTrustPolicy)
	}
	modelOptions := []llmagent.Option(nil)
	if !isNilAgentValue(telemetryProvider) {
		modelOptions = append(modelOptions, telemetryOptions(telemetryProvider, config.Input.Model.Provider, config.Input.Model.Model)...)
	}
	factories := config.AgentFactories
	if factories == nil {
		factories = DefaultAgentFactoryRegistry()
	}
	builtAgent, err := factories.Build(ctx, AgentBuildInput{
		Definition: agentInput, Model: model, Tools: tools, ToolSets: config.ToolSets, Knowledge: knowledgeService, SkillRepositoryProvider: skillProvider, ModelOptions: modelOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("build runner: Agent Factory: %w", err)
	}
	runnerOptions := []trpcrunner.Option{trpcrunner.WithSessionService(scopedSessions)}
	if len(config.Plugins) > 0 {
		runnerOptions = append(runnerOptions, trpcrunner.WithPlugins(config.Plugins...))
	}
	if !isNilAgentValue(resources.memory) {
		runnerOptions = append(runnerOptions, trpcrunner.WithMemoryService(resources.memory))
	}
	if !isNilAgentValue(resources.artifact) {
		runnerOptions = append(runnerOptions, trpcrunner.WithArtifactService(resources.artifact))
	}
	delegate := trpcrunner.NewRunner(agentInput.AppID, builtAgent, runnerOptions...)
	result := &policyRunner{
		delegate: delegate, capabilities: resources.capabilities, toolSets: config.ToolSets,
		tenantID: agentInput.TenantID, appID: agentInput.AppID, revision: agentInput.Revision,
		toolInvocations: config.ToolInvocationStore,
		runOptions: []trpcagent.RunOption{
			trpcagent.WithMaxRunDuration(time.Duration(agentInput.Runtime.ExecutionTimeoutSeconds) * time.Second),
		},
	}
	modelOwned = false
	return result, nil
}

func closeAgentModel(model trpcmodel.Model) (err error) {
	if isNilAgentValue(model) {
		return nil
	}
	closer, ok := model.(interface{ Close() error })
	if !ok || isNilAgentValue(closer) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errors.New("build runner: model close failed")
		}
	}()
	return closer.Close()
}

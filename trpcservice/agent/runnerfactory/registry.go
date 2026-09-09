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
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval"
	approvalreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/promptinjection"
	promptreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/promptinjection/review"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/unsafeintent"
	unsafereview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/unsafeintent/review"
	"trpc.group/trpc-go/trpc-agent-go/plugin/identity"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/skill"
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
	Registry                runtimerunner.RunnerRegistryConfig
	SecretResolver          modelprofile.SecretResolver
	ModelFactory            modelprofile.ModelFactory
	Sessions                session.Service
	StorageFactory          storagefactory.StorageFactory
	Observability           observability.Provider
	ToolRegistry            *servicetool.Registry
	AgentFactories          *serviceagent.AgentFactoryRegistry
	SkillRepositoryProvider skill.RepositoryProvider
	ToolInvocationStore     runtimestorage.ToolInvocationStore
	PluginFactory           PluginFactory
	ToolSetFactory          ToolSetFactory
	EnableUsageCallbacks    bool
	// Guardrail reviewers are injected by the process owner. A published
	// revision can enable a guardrail only when its matching reviewer is
	// configured; otherwise materialization fails closed.
	ApprovalReviewer        approvalreview.Reviewer
	PromptInjectionReviewer promptreview.Reviewer
	UnsafeIntentReviewer    unsafereview.Reviewer
}

// NewRuntimeRunnerRegistry creates a generic Runner registry backed by the
// concrete Agent adapter. The concrete assembly deliberately lives in this
// package so runtime/runner stays independent of Agent implementations.
// NewDefaultPluginFactory returns a fresh identity plugin and the enabled
// upstream guardrails for each sealed execution plan. Every returned plugin is
// runner-owned and must not be shared between cached Runners.
func NewDefaultPluginFactory(config Config) PluginFactory {
	return func(ctx context.Context, plan runtime.ExecutionPlan) (plugins []plugin.Plugin, err error) {
		defer func() {
			if recover() != nil {
				_ = closePlugins(plugins)
				plugins = nil
				err = fmt.Errorf("%w: plugin factory panicked", runtimerunner.ErrInvalid)
			}
		}()
		if isNilFactoryValue(ctx) {
			return nil, fmt.Errorf("%w: context is required", runtimerunner.ErrInvalid)
		}
		if err := factoryContextErr(ctx); err != nil {
			return nil, err
		}
		plugins = []plugin.Plugin{identity.NewPlugin(identity.ProviderFunc(func(_ context.Context, userID, _ string) (*identity.Identity, error) {
			return &identity.Identity{UserID: userID}, nil
		}))}
		if !isNilFactoryValue(config.ToolInvocationStore) {
			plugins = append(plugins, servicetool.NewToolInvocationPreparePlugin())
		}
		guardrailPlugins, guardrailErr := NewGuardrailPlugins(ctx, plan, config)
		if guardrailErr != nil {
			_ = closePlugins(plugins)
			plugins = nil
			return nil, guardrailErr
		}
		plugins = append(plugins, guardrailPlugins...)
		if !isNilFactoryValue(config.ToolInvocationStore) {
			plugins = append(plugins, servicetool.NewToolInvocationDispatchPlugin())
		}
		return plugins, nil
	}
}

// NewGuardrailPlugins materializes the upstream guardrail facade from the
// immutable RuntimePolicy in the sealed ExecutionPlan. It deliberately does
// not infer policy from tool metadata or untrusted request values.
func NewGuardrailPlugins(ctx context.Context, plan runtime.ExecutionPlan, config Config) (plugins []plugin.Plugin, err error) {
	defer func() {
		if recover() != nil {
			_ = closePlugins(plugins)
			plugins = nil
			err = fmt.Errorf("%w: guardrail construction failed", runtimerunner.ErrInvalid)
		}
	}()
	if isNilFactoryValue(ctx) {
		return nil, fmt.Errorf("%w: context is required", runtimerunner.ErrInvalid)
	}
	if err := factoryContextErr(ctx); err != nil {
		return nil, err
	}
	input, err := plan.AgentFactoryInput()
	if err != nil {
		return nil, err
	}
	policy := input.Runtime.Guardrail
	if policy == nil || (!policy.PromptInjection && !policy.UnsafeIntent && len(policy.ApprovalTools) == 0) {
		return nil, nil
	}
	options := make([]guardrail.Option, 0, 3)
	if len(policy.ApprovalTools) > 0 {
		if isNilFactoryValue(config.ApprovalReviewer) {
			return nil, fmt.Errorf("%w: approval reviewer is required", runtimerunner.ErrInvalid)
		}
		approvalOptions := []approval.Option{
			approval.WithReviewer(safeApprovalReviewer{delegate: config.ApprovalReviewer}),
			approval.WithDefaultToolPolicy(approval.ToolPolicySkipApproval),
		}
		for _, toolID := range policy.ApprovalTools {
			approvalOptions = append(approvalOptions, approval.WithToolPolicy(toolID, approval.ToolPolicyRequireApproval))
		}
		approvalPlugin, approvalErr := approval.New(approvalOptions...)
		if approvalErr != nil {
			return nil, fmt.Errorf("%w: construct approval guardrail", runtimerunner.ErrInvalid)
		}
		options = append(options, guardrail.WithApproval(approvalPlugin))
	}
	if policy.PromptInjection {
		if isNilFactoryValue(config.PromptInjectionReviewer) {
			return nil, fmt.Errorf("%w: prompt injection reviewer is required", runtimerunner.ErrInvalid)
		}
		promptPlugin, promptErr := promptinjection.New(promptinjection.WithReviewer(safePromptInjectionReviewer{delegate: config.PromptInjectionReviewer}))
		if promptErr != nil {
			return nil, fmt.Errorf("%w: construct prompt injection guardrail", runtimerunner.ErrInvalid)
		}
		options = append(options, guardrail.WithPromptInjection(promptPlugin))
	}
	if policy.UnsafeIntent {
		if isNilFactoryValue(config.UnsafeIntentReviewer) {
			return nil, fmt.Errorf("%w: unsafe intent reviewer is required", runtimerunner.ErrInvalid)
		}
		unsafePlugin, unsafeErr := unsafeintent.New(unsafeintent.WithReviewer(safeUnsafeIntentReviewer{delegate: config.UnsafeIntentReviewer}))
		if unsafeErr != nil {
			return nil, fmt.Errorf("%w: construct unsafe intent guardrail", runtimerunner.ErrInvalid)
		}
		options = append(options, guardrail.WithUnsafeIntent(unsafePlugin))
	}
	pluginValue, pluginErr := guardrail.New(options...)
	if pluginErr != nil || isNilFactoryValue(pluginValue) {
		return nil, fmt.Errorf("%w: construct guardrail facade", runtimerunner.ErrInvalid)
	}
	return []plugin.Plugin{pluginValue}, nil
}

type safeApprovalReviewer struct{ delegate approvalreview.Reviewer }

func (reviewer safeApprovalReviewer) Review(ctx context.Context, request *approvalreview.Request) (decision *approvalreview.Decision, err error) {
	if isNilFactoryValue(ctx) || isNilFactoryValue(reviewer.delegate) {
		return nil, runtimerunner.ErrInvalid
	}
	defer func() {
		if recover() != nil {
			decision, err = nil, runtimerunner.ErrInvalid
		}
	}()
	decision, err = reviewer.delegate.Review(ctx, request)
	if err != nil {
		if contextErr := factoryContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, runtimerunner.ErrInvalid
	}
	if decision == nil {
		return nil, runtimerunner.ErrInvalid
	}
	copy := *decision
	return &copy, nil
}

type safePromptInjectionReviewer struct{ delegate promptreview.Reviewer }

func (reviewer safePromptInjectionReviewer) Review(ctx context.Context, request *promptreview.Request) (decision *promptreview.Decision, err error) {
	if isNilFactoryValue(ctx) || isNilFactoryValue(reviewer.delegate) {
		return nil, runtimerunner.ErrInvalid
	}
	defer func() {
		if recover() != nil {
			decision, err = nil, runtimerunner.ErrInvalid
		}
	}()
	decision, err = reviewer.delegate.Review(ctx, request)
	if err != nil {
		if contextErr := factoryContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, runtimerunner.ErrInvalid
	}
	if decision == nil {
		return nil, runtimerunner.ErrInvalid
	}
	copy := *decision
	return &copy, nil
}

type safeUnsafeIntentReviewer struct{ delegate unsafereview.Reviewer }

func (reviewer safeUnsafeIntentReviewer) Review(ctx context.Context, request *unsafereview.Request) (decision *unsafereview.Decision, err error) {
	if isNilFactoryValue(ctx) || isNilFactoryValue(reviewer.delegate) {
		return nil, runtimerunner.ErrInvalid
	}
	defer func() {
		if recover() != nil {
			decision, err = nil, runtimerunner.ErrInvalid
		}
	}()
	decision, err = reviewer.delegate.Review(ctx, request)
	if err != nil {
		if contextErr := factoryContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, runtimerunner.ErrInvalid
	}
	if decision == nil {
		return nil, runtimerunner.ErrInvalid
	}
	copy := *decision
	return &copy, nil
}

func NewRuntimeRunnerRegistry(config Config) (*runtimerunner.RunnerRegistry, error) {
	if isNilFactoryValue(config.ModelFactory) || (isNilFactoryValue(config.Sessions) && isNilFactoryValue(config.StorageFactory)) {
		return nil, fmt.Errorf("%w: runtime Runner dependencies are required", runtimerunner.ErrInvalid)
	}
	if isNilFactoryValue(config.Sessions) {
		config.Sessions = nil
	}
	if isNilFactoryValue(config.StorageFactory) {
		config.StorageFactory = nil
	}
	if isNilFactoryValue(config.SecretResolver) {
		config.SecretResolver = nil
	}
	if isNilFactoryValue(config.ToolInvocationStore) {
		config.ToolInvocationStore = nil
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
		if !isNilFactoryValue(config.StorageFactory) {
			return serviceagent.NewRunnerWithConfig(ctx, serviceagent.RunnerConfig{
				Input: input, SecretResolver: config.SecretResolver, ModelFactory: config.ModelFactory,
				Sessions: config.Sessions, StorageFactory: config.StorageFactory,
				Observability: config.Observability, ToolRegistry: config.ToolRegistry, AgentFactories: config.AgentFactories,
				SkillRepositoryProvider: config.SkillRepositoryProvider, ToolInvocationStore: config.ToolInvocationStore,
				Plugins: plugins, ToolSets: toolSets, EnableUsageCallbacks: config.EnableUsageCallbacks,
			})
		}
		return serviceagent.NewRunnerWithConfig(ctx, serviceagent.RunnerConfig{
			Input: input, SecretResolver: config.SecretResolver, ModelFactory: config.ModelFactory,
			Sessions: config.Sessions, Observability: config.Observability, ToolRegistry: config.ToolRegistry, AgentFactories: config.AgentFactories,
			SkillRepositoryProvider: config.SkillRepositoryProvider, ToolInvocationStore: config.ToolInvocationStore,
			Plugins: plugins, ToolSets: toolSets, EnableUsageCallbacks: config.EnableUsageCallbacks,
		})
	}
	return runtimerunner.NewRunnerRegistry(config.Registry)
}

func materializeToolSets(ctx context.Context, factory ToolSetFactory, plan runtime.ExecutionPlan) ([]tool.ToolSet, error) {
	if factory == nil {
		return nil, nil
	}
	if isNilFactoryValue(ctx) {
		return nil, fmt.Errorf("%w: context is required", runtimerunner.ErrInvalid)
	}
	if err := factoryContextErr(ctx); err != nil {
		return nil, err
	}
	toolSets, err := callToolSetFactory(ctx, factory, plan)
	if err != nil {
		_ = closeToolSets(toolSets)
		return nil, fmt.Errorf("%w: materialize tool sets", runtimerunner.ErrInvalid)
	}
	seen := make(map[string]struct{}, len(toolSets))
	for _, candidate := range toolSets {
		name, ok := safeToolSetName(candidate)
		if !ok || name == "" {
			_ = closeToolSets(toolSets)
			return nil, fmt.Errorf("%w: invalid tool set", runtimerunner.ErrInvalid)
		}
		if _, duplicate := seen[name]; duplicate {
			_ = closeToolSets(toolSets)
			return nil, fmt.Errorf("%w: duplicate tool set", runtimerunner.ErrInvalid)
		}
		seen[name] = struct{}{}
	}
	return toolSets, nil
}

func closeToolSets(toolSets []tool.ToolSet) error {
	var errs []error
	for index := len(toolSets) - 1; index >= 0; index-- {
		if isNilFactoryValue(toolSets[index]) {
			continue
		}
		if err := closeToolSet(toolSets[index]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func safeToolSetName(set tool.ToolSet) (name string, ok bool) {
	if isNilFactoryValue(set) {
		return "", false
	}
	defer func() {
		if recover() != nil {
			name, ok = "", false
		}
	}()
	return set.Name(), true
}

func safeToolDeclaration(candidate tool.Tool) (declaration *tool.Declaration, ok bool) {
	if isNilFactoryValue(candidate) {
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

func safePluginName(value plugin.Plugin) (name string, ok bool) {
	if isNilFactoryValue(value) {
		return "", false
	}
	defer func() {
		if recover() != nil {
			name, ok = "", false
		}
	}()
	return value.Name(), true
}

func closeToolSet(set tool.ToolSet) (err error) {
	if isNilFactoryValue(set) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: close tool set panicked", runtimerunner.ErrInvalid)
		}
	}()
	return set.Close()
}

func callToolSetFactory(ctx context.Context, factory ToolSetFactory, plan runtime.ExecutionPlan) (sets []tool.ToolSet, err error) {
	defer func() {
		if recover() != nil {
			// Keep any partially materialized values visible to the caller so
			// the ownership boundary can close them after a panic.
			err = fmt.Errorf("%w: tool set factory panicked", runtimerunner.ErrInvalid)
		}
	}()
	return factory(ctx, plan)
}

func materializePlugins(ctx context.Context, factory PluginFactory, plan runtime.ExecutionPlan) ([]plugin.Plugin, error) {
	if factory == nil {
		return nil, nil
	}
	if isNilFactoryValue(ctx) {
		return nil, fmt.Errorf("%w: context is required", runtimerunner.ErrInvalid)
	}
	if err := factoryContextErr(ctx); err != nil {
		return nil, err
	}
	plugins, err := callPluginFactory(ctx, factory, plan)
	if err != nil {
		_ = closePlugins(plugins)
		return nil, fmt.Errorf("%w: materialize plugins", runtimerunner.ErrInvalid)
	}
	for _, candidate := range plugins {
		name, ok := safePluginName(candidate)
		if !ok || name == "" {
			_ = closePlugins(plugins)
			return nil, fmt.Errorf("%w: invalid plugin", runtimerunner.ErrInvalid)
		}
	}
	return plugins, nil
}

func closePlugins(plugins []plugin.Plugin) error {
	var errs []error
	for index := len(plugins) - 1; index >= 0; index-- {
		candidate := plugins[index]
		if isNilFactoryValue(candidate) {
			continue
		}
		if closer, ok := candidate.(plugin.Closer); ok && !isNilFactoryValue(closer) {
			if err := closePlugin(closer); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func closePlugin(closer plugin.Closer) (err error) {
	if isNilFactoryValue(closer) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: close plugin panicked", runtimerunner.ErrInvalid)
		}
	}()
	return closer.Close(context.Background())
}

func callPluginFactory(ctx context.Context, factory PluginFactory, plan runtime.ExecutionPlan) (plugins []plugin.Plugin, err error) {
	defer func() {
		if recover() != nil {
			// Preserve the partial slice for materializePlugins to close in
			// reverse order.
			err = fmt.Errorf("%w: plugin factory panicked", runtimerunner.ErrInvalid)
		}
	}()
	return factory(ctx, plan)
}

func isNilFactoryValue(value any) bool { return nilvalue.Is(value) }

func factoryContextErr(ctx context.Context) error {
	if isNilFactoryValue(ctx) {
		return runtimerunner.ErrInvalid
	}
	return ctx.Err()
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
	// Backend profiles are tenant-owned, while app-aware capabilities need the
	// immutable Agent App partition selected by this execution plan.
	storageInput.AppID = agentInput.AppID
	return serviceagent.RunnerInput{
		Tenant: plan.Tenant(), Agent: agentInput, Model: modelInput, Storage: storageInput,
	}, nil
}

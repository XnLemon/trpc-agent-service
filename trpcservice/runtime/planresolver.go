package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrInvalidPlanResolverConfig reports missing plan resolver dependencies.
	ErrInvalidPlanResolverConfig = errors.New("invalid execution plan resolver configuration")
	// ErrInvalidPlanRequest reports a malformed runtime plan request.
	ErrInvalidPlanRequest = errors.New("invalid execution plan request")
	// ErrPlanResolverNotReady reports a resolver that cannot accept requests.
	ErrPlanResolverNotReady = errors.New("execution plan resolver is not ready")
	// ErrPlanUnavailable is the stable, redacted plan-resolution failure.
	ErrPlanUnavailable = errors.New("execution plan unavailable")
)

// PlanResolverConfig contains the repository and provider catalog dependencies
// needed to construct an immutable ExecutionPlan.
type PlanResolverConfig struct {
	Tenants        tenant.Repository
	Apps           appmodel.Repository
	Models         modelprofile.Repository
	Backends       backend.Repository
	ModelCatalog   *modelprofile.ProviderCatalog
	BackendCatalog *backend.ProviderCatalog
}

// PlanRequest identifies the tenant and App whose published configuration
// should be frozen into one execution plan. Authentication and route proof
// validation belong to the caller-facing Gateway boundary.
type PlanRequest struct {
	TenantID string
	AppID    string
}

// Validate checks the minimum identity required for plan resolution. Runtime
// deliberately does not recreate the Gateway's proof validation rules.
func (request PlanRequest) Validate() error {
	if request.TenantID != strings.TrimSpace(request.TenantID) || request.AppID != strings.TrimSpace(request.AppID) ||
		modelprofile.ValidateTenantID(request.TenantID) != nil || appmodel.ValidateAppID(request.AppID) != nil {
		return fmt.Errorf("%w: canonical tenant and app IDs are required", ErrInvalidPlanRequest)
	}
	return nil
}

// PlanResolver resolves one fixed ExecutionPlan from a neutral PlanRequest.
// It owns configuration lookup and snapshot assembly, but not authentication.
type PlanResolver struct {
	tenants        tenant.Repository
	apps           appmodel.Repository
	models         modelprofile.Repository
	backends       backend.Repository
	modelCatalog   *modelprofile.ProviderCatalog
	backendCatalog *backend.ProviderCatalog
}

type resolvedPlanInputs struct {
	tenantSnapshot tenant.ConfigurationSnapshot
	app            *appmodel.App
	revision       *appmodel.Revision
	model          *modelprofile.Profile
	backend        *backend.Profile
}

// NewPlanResolver validates the dependencies required to resolve execution
// plans from tenant-scoped configuration.
func NewPlanResolver(config PlanResolverConfig) (*PlanResolver, error) {
	if nilvalue.Is(config.Tenants) || nilvalue.Is(config.Apps) || nilvalue.Is(config.Models) || nilvalue.Is(config.Backends) || nilvalue.Is(config.ModelCatalog) || nilvalue.Is(config.BackendCatalog) {
		return nil, fmt.Errorf("%w: all repositories and provider catalogs are required", ErrInvalidPlanResolverConfig)
	}
	return &PlanResolver{
		tenants: config.Tenants, apps: config.Apps, models: config.Models, backends: config.Backends,
		modelCatalog: config.ModelCatalog, backendCatalog: config.BackendCatalog,
	}, nil
}

// Ready reports whether all required resolver dependencies are present.
func (resolver *PlanResolver) Ready() bool {
	return resolver != nil && !nilvalue.Is(resolver.tenants) && !nilvalue.Is(resolver.apps) && !nilvalue.Is(resolver.models) && !nilvalue.Is(resolver.backends) && !nilvalue.Is(resolver.modelCatalog) && !nilvalue.Is(resolver.backendCatalog)
}

// Resolve constructs one immutable plan. All non-cancellation failures are
// reduced to ErrPlanUnavailable so repository existence and provider details do
// not escape this internal scheduling boundary.
func (resolver *PlanResolver) Resolve(ctx context.Context, request PlanRequest) (plan ExecutionPlan, err error) {
	defer func() {
		if recover() != nil {
			plan = ExecutionPlan{}
			if contextErr := planContextErr(ctx); contextErr != nil {
				err = contextErr
			} else {
				err = ErrPlanUnavailable
			}
		}
	}()
	if nilvalue.Is(ctx) {
		return ExecutionPlan{}, fmt.Errorf("%w: context is required", ErrInvalidPlanRequest)
	}
	if err := planContextErr(ctx); err != nil {
		return ExecutionPlan{}, err
	}
	if !resolver.Ready() {
		return ExecutionPlan{}, ErrPlanResolverNotReady
	}
	if err := request.Validate(); err != nil {
		return ExecutionPlan{}, err
	}
	inputs, err := resolver.resolveInputs(ctx, request)
	if err != nil {
		return ExecutionPlan{}, err
	}
	plan, err = NewExecutionPlanFromInput(ExecutionPlanInput{
		TenantSnapshot: inputs.tenantSnapshot,
		AppRoot:        inputs.app,
		Revision:       inputs.revision,
		ModelProfile:   inputs.model,
		ModelCatalog:   resolver.modelCatalog,
		BackendProfile: inputs.backend,
		BackendCatalog: resolver.backendCatalog,
	})
	if err != nil {
		return ExecutionPlan{}, ErrPlanUnavailable
	}
	if _, err := plan.CacheKey(); err != nil {
		return ExecutionPlan{}, ErrPlanUnavailable
	}
	if err := planContextErr(ctx); err != nil {
		return ExecutionPlan{}, err
	}
	return plan, nil
}

func (resolver *PlanResolver) resolveInputs(ctx context.Context, request PlanRequest) (resolvedPlanInputs, error) {
	tenantValue, err := resolver.tenants.Get(ctx, request.TenantID)
	if err != nil || nilvalue.Is(tenantValue) {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := planContextErr(ctx); err != nil {
		return resolvedPlanInputs{}, err
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(tenantValue)
	if err != nil {
		return resolvedPlanInputs{}, ErrPlanUnavailable
	}
	appValue, err := resolver.apps.Get(ctx, request.TenantID, request.AppID)
	if err != nil || nilvalue.Is(appValue) || appValue.CurrentRevision == nil || appValue.TenantID != request.TenantID || appValue.AppID != request.AppID || !appValue.CanAcceptExecution() {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if tenantValue.DefaultAgentAppID != nil && *tenantValue.DefaultAgentAppID != request.AppID {
		defaultApp, defaultErr := resolver.apps.Get(ctx, request.TenantID, *tenantValue.DefaultAgentAppID)
		if defaultErr != nil || nilvalue.Is(defaultApp) || defaultApp.TenantID != request.TenantID || defaultApp.AppID != *tenantValue.DefaultAgentAppID || !defaultApp.CanAcceptExecution() {
			return resolvedPlanInputs{}, resolver.planError(ctx)
		}
		if err := defaultApp.Validate(); err != nil {
			return resolvedPlanInputs{}, ErrPlanUnavailable
		}
	}
	if err := planContextErr(ctx); err != nil {
		return resolvedPlanInputs{}, err
	}
	selectedRevision := appValue.CurrentRevision
	if appValue.CanaryRevision != nil {
		selectedRevision = appValue.CanaryRevision
	}
	revisionValue, err := resolver.apps.GetRevision(ctx, request.TenantID, request.AppID, *selectedRevision)
	if err != nil || nilvalue.Is(revisionValue) || revisionValue.TenantID != request.TenantID || revisionValue.AppID != request.AppID || revisionValue.Revision != *selectedRevision || revisionValue.State != appmodel.RevisionStatePublished {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	modelValue, err := resolver.models.Get(ctx, request.TenantID, revisionValue.ModelProfileID)
	if err != nil || nilvalue.Is(modelValue) || modelValue.TenantID != request.TenantID || modelValue.ProfileID != revisionValue.ModelProfileID {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := planContextErr(ctx); err != nil {
		return resolvedPlanInputs{}, err
	}
	if tenantValue.DefaultBackendProfileID == nil {
		return resolvedPlanInputs{}, ErrPlanUnavailable
	}
	backendValue, err := resolver.backends.Get(ctx, request.TenantID, *tenantValue.DefaultBackendProfileID)
	if err != nil || nilvalue.Is(backendValue) || backendValue.TenantID != request.TenantID || backendValue.ProfileID != *tenantValue.DefaultBackendProfileID || !backendValue.CanAcceptExecution() {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := planContextErr(ctx); err != nil {
		return resolvedPlanInputs{}, err
	}
	return resolvedPlanInputs{tenantSnapshot: tenantSnapshot, app: appValue, revision: revisionValue, model: modelValue, backend: backendValue}, nil
}

func (resolver *PlanResolver) planError(ctx context.Context) error {
	if err := planContextErr(ctx); err != nil {
		return err
	}
	return ErrPlanUnavailable
}

func planContextErr(ctx context.Context) (err error) {
	if nilvalue.Is(ctx) {
		return ErrInvalidPlanRequest
	}
	defer func() {
		if recover() != nil {
			err = ErrInvalidPlanRequest
		}
	}()
	return nilvalue.ContextErr(ctx)
}

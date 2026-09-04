package gateway

import (
	"context"
	"errors"
	"fmt"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// PlanResolverConfig contains only repository and catalog dependencies. The
// interfaces deliberately match the domain contracts so an HTTP Gateway does
// not depend on an InMemory implementation.
type PlanResolverConfig struct {
	Tenants        tenant.Repository
	Apps           appmodel.Repository
	Models         model.Repository
	Backends       backend.Repository
	ModelCatalog   *model.ProviderCatalog
	BackendCatalog *backend.ProviderCatalog
}

// PlanResolver resolves one fixed runtime.ExecutionPlan from a trusted
// Principal. It never accepts tenant/app/profile IDs from a request body.
type PlanResolver struct {
	tenants        tenant.Repository
	apps           appmodel.Repository
	models         model.Repository
	backends       backend.Repository
	modelCatalog   *model.ProviderCatalog
	backendCatalog *backend.ProviderCatalog
}

type resolvedPlanInputs struct {
	tenantSnapshot tenant.ConfigurationSnapshot
	app            *appmodel.App
	revision       *appmodel.Revision
	model          *model.Profile
	backend        *backend.Profile
}

// NewPlanResolver validates the control-plane dependencies.
func NewPlanResolver(config PlanResolverConfig) (*PlanResolver, error) {
	if config.Tenants == nil || config.Apps == nil || config.Models == nil || config.Backends == nil || config.ModelCatalog == nil || config.BackendCatalog == nil {
		return nil, fmt.Errorf("%w: plan resolver dependencies are required", ErrInvalid)
	}
	return &PlanResolver{
		tenants: config.Tenants, apps: config.Apps, models: config.Models, backends: config.Backends,
		modelCatalog: config.ModelCatalog, backendCatalog: config.BackendCatalog,
	}, nil
}

// Ready reports whether all required resolver dependencies are present.
func (resolver *PlanResolver) Ready() bool {
	return resolver != nil && resolver.tenants != nil && resolver.apps != nil && resolver.models != nil && resolver.backends != nil && resolver.modelCatalog != nil && resolver.backendCatalog != nil
}

// ResolveAuthenticatedAPI converts an authenticator-issued proof into the
// trusted API principal before resolving a plan. Callers cannot manufacture
// the proof-bearing AuthenticatedAPI value from request-shaped IDs.
func (resolver *PlanResolver) ResolveAuthenticatedAPI(ctx context.Context, authenticated AuthenticatedAPI) (runtime.ExecutionPlan, error) {
	principal, err := newAPIPrincipal(authenticated)
	if err != nil {
		return runtime.ExecutionPlan{}, err
	}
	return resolver.Resolve(ctx, principal)
}

// Resolve constructs one immutable plan. All non-cancellation failures are
// reduced to ErrPlanUnavailable so repository existence and provider details do
// not escape to a caller or reveal another tenant's configuration.
func (resolver *PlanResolver) Resolve(ctx context.Context, principal Principal) (runtime.ExecutionPlan, error) {
	if ctx == nil {
		return runtime.ExecutionPlan{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return runtime.ExecutionPlan{}, err
	}
	if !resolver.Ready() {
		return runtime.ExecutionPlan{}, ErrNotReady
	}
	if err := principal.Validate(); err != nil {
		return runtime.ExecutionPlan{}, ErrUnauthenticated
	}
	inputs, err := resolver.resolveInputs(ctx, principal)
	if err != nil {
		return runtime.ExecutionPlan{}, err
	}
	plan, err := runtime.NewExecutionPlan(inputs.tenantSnapshot, inputs.app, inputs.revision, inputs.model, resolver.modelCatalog, inputs.backend, resolver.backendCatalog)
	if err != nil {
		return runtime.ExecutionPlan{}, ErrPlanUnavailable
	}
	if _, err := plan.CacheKey(); err != nil {
		return runtime.ExecutionPlan{}, ErrPlanUnavailable
	}
	if err := ctx.Err(); err != nil {
		return runtime.ExecutionPlan{}, err
	}
	return plan, nil
}

func (resolver *PlanResolver) resolveInputs(ctx context.Context, principal Principal) (resolvedPlanInputs, error) {
	tenantValue, err := resolver.tenants.Get(ctx, principal.TenantID())
	if err != nil || tenantValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(tenantValue)
	if err != nil {
		return resolvedPlanInputs{}, ErrPlanUnavailable
	}
	appValue, err := resolver.apps.Get(ctx, principal.TenantID(), principal.AppID())
	if err != nil || appValue == nil || appValue.CurrentRevision == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	selectedRevision := appValue.CurrentRevision
	if appValue.CanaryRevision != nil {
		selectedRevision = appValue.CanaryRevision
	}
	revisionValue, err := resolver.apps.GetRevision(ctx, principal.TenantID(), principal.AppID(), *selectedRevision)
	if err != nil || revisionValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	modelValue, err := resolver.models.Get(ctx, principal.TenantID(), revisionValue.ModelProfileID)
	if err != nil || modelValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	if tenantValue.DefaultBackendProfileID == nil {
		return resolvedPlanInputs{}, ErrPlanUnavailable
	}
	backendValue, err := resolver.backends.Get(ctx, principal.TenantID(), *tenantValue.DefaultBackendProfileID)
	if err != nil || backendValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	return resolvedPlanInputs{tenantSnapshot: tenantSnapshot, app: appValue, revision: revisionValue, model: modelValue, backend: backendValue}, nil
}

func (resolver *PlanResolver) planError(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrPlanUnavailable
}

// IsContextCancellation reports whether an error is a caller cancellation.
func IsContextCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

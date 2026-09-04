// Package runtime composes immutable control-plane snapshots into one
// tenant-scoped execution plan and exposes internal scheduling boundaries.
package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

type executionPlanContextKey struct{}

// CacheKey is the comparable identity of one fixed execution plan. It contains
// all control-plane versions and content digests but no secrets or clients.
type CacheKey struct {
	TenantID              string
	TenantVersion         int64
	AppID                 string
	AppVersion            int64
	Revision              int64
	AgentContentDigest    string
	ModelProfileID        string
	ModelProfileVersion   int64
	ModelContentDigest    string
	BackendProfileID      string
	BackendProfileVersion int64
	BackendContentDigest  string
}

// ExecutionPlan is the sealed input for one execution. Every embedded
// snapshot is immutable and returns defensive copies at its public boundary.
type ExecutionPlan struct {
	tenant  *tenant.Tenant
	agent   agent.AgentExecutionSnapshot
	model   modelprofile.ModelExecutionSnapshot
	backend backend.BackendExecutionSnapshot
}

// NewExecutionPlan validates and freezes one Tenant, current Agent Revision,
// active Model Profile, and active Backend Profile. All objects must belong to
// the same tenant and the Model Profile must satisfy the Revision reference.
func NewExecutionPlan(
	tenantSnapshot tenant.ConfigurationSnapshot,
	appRoot *appmodel.App,
	revision *appmodel.Revision,
	modelProfile *modelprofile.Profile,
	modelCatalog *modelprofile.ProviderCatalog,
	backendProfile *backend.Profile,
	backendCatalog *backend.ProviderCatalog,
) (ExecutionPlan, error) {
	tenantValue := tenantSnapshot.Tenant()
	if err := tenantValue.Validate(); err != nil {
		return ExecutionPlan{}, errors.New("invalid execution plan: tenant snapshot is invalid")
	}
	if !tenantValue.CanAcceptExecution() {
		return ExecutionPlan{}, errors.New("invalid execution plan: tenant cannot accept execution")
	}
	if appRoot != nil && revision != nil && appRoot.AppID != revision.AppID {
		return ExecutionPlan{}, errors.New("invalid execution plan: revision does not belong to App")
	}
	agentSnapshot, err := agent.NewAgentExecutionSnapshot(tenantSnapshot, appRoot, revision)
	if err != nil {
		return ExecutionPlan{}, fmt.Errorf("invalid execution plan: agent snapshot: %w", err)
	}
	modelSnapshot, err := modelprofile.NewModelExecutionSnapshot(tenantSnapshot, modelProfile, modelCatalog)
	if err != nil {
		return ExecutionPlan{}, fmt.Errorf("invalid execution plan: model snapshot: %w", err)
	}
	backendSnapshot, err := backend.NewBackendExecutionSnapshot(tenantSnapshot, backendProfile, backendCatalog)
	if err != nil {
		return ExecutionPlan{}, fmt.Errorf("invalid execution plan: backend snapshot: %w", err)
	}
	if revision == nil || modelProfile == nil || revision.ModelProfileID != modelProfile.ProfileID {
		return ExecutionPlan{}, errors.New("invalid execution plan: revision model reference does not match profile")
	}
	tenantCopy := tenantValue.Clone()
	return ExecutionPlan{tenant: &tenantCopy, agent: agentSnapshot, model: modelSnapshot, backend: backendSnapshot}, nil
}

// Tenant returns a defensive copy of the Tenant version fixed in the plan.
func (plan ExecutionPlan) Tenant() tenant.Tenant {
	if plan.tenant == nil {
		return tenant.Tenant{}
	}
	return plan.tenant.Clone()
}

// AgentSnapshot returns the fixed Agent execution snapshot.
func (plan ExecutionPlan) AgentSnapshot() agent.AgentExecutionSnapshot { return plan.agent }

// ModelSnapshot returns the fixed Model execution snapshot.
func (plan ExecutionPlan) ModelSnapshot() modelprofile.ModelExecutionSnapshot { return plan.model }

// BackendSnapshot returns the fixed Backend execution snapshot.
func (plan ExecutionPlan) BackendSnapshot() backend.BackendExecutionSnapshot { return plan.backend }

// CacheKey returns the complete comparable identity for the plan.
func (plan ExecutionPlan) CacheKey() (CacheKey, error) {
	if err := plan.validate(); err != nil {
		return CacheKey{}, err
	}
	agentKey, err := plan.agent.CacheKey()
	if err != nil {
		return CacheKey{}, err
	}
	modelKey, err := plan.model.CacheKey()
	if err != nil {
		return CacheKey{}, err
	}
	backendKey, err := plan.backend.CacheKey()
	if err != nil {
		return CacheKey{}, err
	}
	return CacheKey{
		TenantID: plan.tenant.TenantID, TenantVersion: plan.tenant.Version,
		AppID: agentKey.AppID, AppVersion: agentKey.AppVersion,
		Revision: agentKey.Revision, AgentContentDigest: agentKey.ContentDigest,
		ModelProfileID: modelKey.ProfileID, ModelProfileVersion: modelKey.ProfileVersion,
		ModelContentDigest: modelKey.ContentDigest, BackendProfileID: backendKey.ProfileID,
		BackendProfileVersion: backendKey.ProfileVersion, BackendContentDigest: backendKey.ContentDigest,
	}, nil
}

// AgentFactoryInput returns the secret-free Agent Factory boundary.
func (plan ExecutionPlan) AgentFactoryInput() (agent.LLMAgentFactoryInput, error) {
	if err := plan.validate(); err != nil {
		return agent.LLMAgentFactoryInput{}, err
	}
	return plan.agent.FactoryInput()
}

// ModelFactoryInput returns the secret-free Model Factory boundary.
func (plan ExecutionPlan) ModelFactoryInput() (modelprofile.ModelFactoryInput, error) {
	if err := plan.validate(); err != nil {
		return modelprofile.ModelFactoryInput{}, err
	}
	return plan.model.FactoryInput()
}

// StorageFactoryInput returns the secret-free Storage Factory boundary.
func (plan ExecutionPlan) StorageFactoryInput() (backend.StorageFactoryInput, error) {
	if err := plan.validate(); err != nil {
		return backend.StorageFactoryInput{}, err
	}
	return plan.backend.FactoryInput()
}

// AgentRunnerInput projects a validated plan into the external-agent adapter
// boundary. Runtime owns the complete plan; agent owns tRPC-Agent-Go wiring.
func (plan ExecutionPlan) AgentRunnerInput() (agent.RunnerInput, error) {
	if err := plan.validate(); err != nil {
		return agent.RunnerInput{}, err
	}
	agentInput, err := plan.agent.FactoryInput()
	if err != nil {
		return agent.RunnerInput{}, err
	}
	modelInput, err := plan.model.FactoryInput()
	if err != nil {
		return agent.RunnerInput{}, err
	}
	storageInput, err := plan.backend.FactoryInput()
	if err != nil {
		return agent.RunnerInput{}, err
	}
	return agent.RunnerInput{
		Tenant:  plan.Tenant(),
		Agent:   agentInput,
		Model:   modelInput,
		Storage: storageInput,
	}, nil
}

// WithExecutionPlan carries a validated defensive plan in a Context.
func WithExecutionPlan(ctx context.Context, plan ExecutionPlan) context.Context {
	if plan.validate() != nil {
		return context.WithValue(ctx, executionPlanContextKey{}, ExecutionPlan{})
	}
	return context.WithValue(ctx, executionPlanContextKey{}, plan.clone())
}

// ExecutionPlanFromContext returns a validated defensive plan copy.
func ExecutionPlanFromContext(ctx context.Context) (ExecutionPlan, bool) {
	plan, ok := ctx.Value(executionPlanContextKey{}).(ExecutionPlan)
	if !ok || plan.validate() != nil {
		return ExecutionPlan{}, false
	}
	return plan.clone(), true
}

func (plan ExecutionPlan) validate() error {
	if plan.tenant == nil {
		return errors.New("invalid execution plan: plan is not initialized")
	}
	if err := plan.tenant.Validate(); err != nil || !plan.tenant.CanAcceptExecution() {
		return errors.New("invalid execution plan: tenant is not active")
	}
	if _, err := plan.agent.FactoryInput(); err != nil {
		return err
	}
	if _, err := plan.model.FactoryInput(); err != nil {
		return err
	}
	if _, err := plan.backend.FactoryInput(); err != nil {
		return err
	}
	return nil
}

func (plan ExecutionPlan) clone() ExecutionPlan {
	tenantCopy := plan.tenant.Clone()
	return ExecutionPlan{tenant: &tenantCopy, agent: plan.agent, model: plan.model, backend: plan.backend}
}

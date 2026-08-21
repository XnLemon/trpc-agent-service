package agent

import (
	"context"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

type executionSnapshotContextKey struct{}

// AgentExecutionSnapshot is the sealed, immutable control-plane input for one
// Worker execution. It contains no credentials or live runtime clients.
type AgentExecutionSnapshot struct {
	tenant   *tenant.Tenant
	app      *App
	revision *Revision
}

// FactoryCacheKey is the comparable identity for one materialized Agent.
// Versions prevent mutable tenant or App metadata from reusing stale entries.
type FactoryCacheKey struct {
	TenantID      string
	TenantVersion int64
	AppID         string
	AppVersion    int64
	Revision      int64
	ContentDigest string
}

// LLMAgentFactoryInput is the provider-neutral subset mapped into
// tRPC-Agent-Go's LLMAgent and runtime options by a later dependency resolver.
// References remain IDs; secrets and live clients are intentionally absent.
type LLMAgentFactoryInput struct {
	TenantID          string
	TenantVersion     int64
	AppID             string
	AppKey            string
	AppVersion        int64
	DisplayName       string
	Name              string
	Description       string
	Revision          int64
	ContentDigest     string
	Kind              Kind
	SchemaVersion     int
	Instruction       string
	GlobalInstruction string
	ModelProfileID    string
	Generation        GenerationConfig
	Runtime           RuntimePolicy
	Tools             []ToolAuthorization
}

// Clone returns a defensive copy of Factory input pointer and slice fields.
func (input LLMAgentFactoryInput) Clone() LLMAgentFactoryInput {
	clone := input
	clone.Generation = cloneGenerationConfig(input.Generation)
	clone.Tools = cloneTools(input.Tools)
	return clone
}

// NewAgentExecutionSnapshot validates and freezes an active Tenant snapshot,
// active App root, and the App's selected immutable published Revision.
func NewAgentExecutionSnapshot(tenantSnapshot tenant.ConfigurationSnapshot, app *App, revision *Revision) (AgentExecutionSnapshot, error) {
	tenantValue := tenantSnapshot.Tenant()
	if err := validateExecutionState(tenantValue, app, revision); err != nil {
		return AgentExecutionSnapshot{}, err
	}
	tenantCopy := tenantValue.Clone()
	appCopy := app.Clone()
	revisionCopy := revision.Clone()
	return AgentExecutionSnapshot{tenant: &tenantCopy, app: &appCopy, revision: &revisionCopy}, nil
}

func validateExecutionState(tenantValue tenant.Tenant, app *App, revision *Revision) error {
	if err := tenantValue.Validate(); err != nil {
		return fmt.Errorf("%w: invalid tenant snapshot: %v", ErrInvalid, err)
	}
	if !tenantValue.CanAcceptExecution() {
		return fmt.Errorf("%w: tenant status %q cannot accept execution", ErrInvalid, tenantValue.Status)
	}
	if app == nil {
		return fmt.Errorf("%w: App snapshot is required", ErrInvalid)
	}
	if err := app.Validate(); err != nil {
		return fmt.Errorf("%w: invalid App snapshot: %v", ErrInvalid, err)
	}
	if !app.CanAcceptExecution() {
		return fmt.Errorf("%w: App status %q cannot accept execution", ErrInvalid, app.Status)
	}
	if revision == nil {
		return fmt.Errorf("%w: Revision snapshot is required", ErrInvalid)
	}
	if err := revision.Validate(); err != nil {
		return fmt.Errorf("%w: invalid Revision snapshot: %v", ErrInvalid, err)
	}
	if revision.State != RevisionStatePublished {
		return fmt.Errorf("%w: execution requires a published Revision", ErrInvalid)
	}
	if tenantValue.TenantID != app.TenantID || tenantValue.TenantID != revision.TenantID || app.AppID != revision.AppID {
		return fmt.Errorf("%w: Tenant, App, and Revision scopes must match", ErrInvalid)
	}
	if app.CurrentRevision == nil || *app.CurrentRevision != revision.Revision {
		return fmt.Errorf("%w: Revision is not the App current revision", ErrInvalid)
	}
	return nil
}

// Tenant returns the fixed tenant version captured for this execution.
func (snapshot AgentExecutionSnapshot) Tenant() tenant.Tenant {
	if snapshot.tenant == nil {
		return tenant.Tenant{}
	}
	return snapshot.tenant.Clone()
}

// App returns a defensive copy of the fixed App root.
func (snapshot AgentExecutionSnapshot) App() App {
	if snapshot.app == nil {
		return App{}
	}
	return snapshot.app.Clone()
}

// Revision returns a defensive copy of immutable executable content.
func (snapshot AgentExecutionSnapshot) Revision() Revision {
	if snapshot.revision == nil {
		return Revision{}
	}
	return snapshot.revision.Clone()
}

// CacheKey returns the stable Factory cache identity for the snapshot.
func (snapshot AgentExecutionSnapshot) CacheKey() (FactoryCacheKey, error) {
	if err := snapshot.validate(); err != nil {
		return FactoryCacheKey{}, err
	}
	return FactoryCacheKey{
		TenantID: snapshot.tenant.TenantID, TenantVersion: snapshot.tenant.Version,
		AppID: snapshot.app.AppID, AppVersion: snapshot.app.Version,
		Revision: snapshot.revision.Revision, ContentDigest: snapshot.revision.ContentDigest,
	}, nil
}

// FactoryInput maps the sealed domain state into a secret-free LLMAgent
// construction contract. Name uses the stable App key; executable description
// and behavior come from the immutable Revision.
func (snapshot AgentExecutionSnapshot) FactoryInput() (LLMAgentFactoryInput, error) {
	if err := snapshot.validate(); err != nil {
		return LLMAgentFactoryInput{}, err
	}
	return LLMAgentFactoryInput{
		TenantID: snapshot.tenant.TenantID, TenantVersion: snapshot.tenant.Version,
		AppID: snapshot.app.AppID, AppKey: snapshot.app.AppKey, AppVersion: snapshot.app.Version,
		DisplayName: snapshot.app.DisplayName, Name: snapshot.app.AppKey,
		Description: snapshot.revision.Description, Revision: snapshot.revision.Revision,
		ContentDigest: snapshot.revision.ContentDigest, Kind: snapshot.revision.Kind,
		SchemaVersion: snapshot.revision.SchemaVersion, Instruction: snapshot.revision.Instruction,
		GlobalInstruction: snapshot.revision.GlobalInstruction, ModelProfileID: snapshot.revision.ModelProfileID,
		Generation: cloneGenerationConfig(snapshot.revision.Generation), Runtime: snapshot.revision.Runtime,
		Tools: cloneTools(snapshot.revision.Tools),
	}, nil
}

// WithAgentExecutionSnapshot carries a defensive snapshot copy for one
// execution. Invalid or zero snapshots overwrite the key with an empty value.
func WithAgentExecutionSnapshot(ctx context.Context, snapshot AgentExecutionSnapshot) context.Context {
	if err := snapshot.validate(); err != nil {
		return context.WithValue(ctx, executionSnapshotContextKey{}, AgentExecutionSnapshot{})
	}
	return context.WithValue(ctx, executionSnapshotContextKey{}, snapshot.clone())
}

// AgentExecutionSnapshotFromContext returns a validated defensive copy.
func AgentExecutionSnapshotFromContext(ctx context.Context) (AgentExecutionSnapshot, bool) {
	snapshot, ok := ctx.Value(executionSnapshotContextKey{}).(AgentExecutionSnapshot)
	if !ok || snapshot.validate() != nil {
		return AgentExecutionSnapshot{}, false
	}
	return snapshot.clone(), true
}

func (snapshot AgentExecutionSnapshot) validate() error {
	if snapshot.tenant == nil || snapshot.app == nil || snapshot.revision == nil {
		return fmt.Errorf("%w: execution snapshot is not initialized", ErrInvalid)
	}
	return validateExecutionState(*snapshot.tenant, snapshot.app, snapshot.revision)
}

func (snapshot AgentExecutionSnapshot) clone() AgentExecutionSnapshot {
	tenantCopy := snapshot.tenant.Clone()
	appCopy := snapshot.app.Clone()
	revisionCopy := snapshot.revision.Clone()
	return AgentExecutionSnapshot{tenant: &tenantCopy, app: &appCopy, revision: &revisionCopy}
}

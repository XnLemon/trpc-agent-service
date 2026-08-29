package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/agent/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/backend/postgres"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

var (
	// ErrDemoInitialization reports a failed local demo bootstrap operation.
	ErrDemoInitialization = errors.New("demo initialization failed")
	// ErrDemoState reports a partial or incompatible state that the demo flow
	// cannot safely repair without guessing the operator's intent.
	ErrDemoState = errors.New("demo initialization state is incompatible")
)

const (
	demoTenantKey         = "demo"
	demoTenantName        = "Local Demo Tenant"
	demoAppKey            = "assistant"
	demoAppName           = "Local Demo Assistant"
	demoAppDescription    = "Offline deterministic demo agent"
	demoModelProfileKey   = "deterministic"
	demoModelDisplayName  = "Deterministic Demo Model"
	demoBackendProfileKey = "local"
	demoBackendName       = "Local Demo Backend"
	demoInstruction       = "Answer as a helpful local demo assistant."
	demoActorType         = "bootstrap"
	demoActorID           = "trpc-service-demo"
	demoReason            = "provision local demo graph"
	demoCorrelationID     = "demo-bootstrap"
)

// DemoConfig contains the stable metadata for the local, offline demo graph.
// Empty fields use the documented defaults so `trpc-service demo --confirm`
// remains a one-command path while callers can choose non-conflicting keys.
type DemoConfig struct {
	TenantKey         string
	TenantDisplayName string
	AppKey            string
	AppDisplayName    string
	AppDescription    string
	ModelProfileKey   string
	BackendProfileKey string
}

// DefaultDemoConfig returns the metadata used by the local golden path.
func DefaultDemoConfig() DemoConfig {
	return DemoConfig{
		TenantKey: demoTenantKey, TenantDisplayName: demoTenantName,
		AppKey: demoAppKey, AppDisplayName: demoAppName,
		AppDescription: demoAppDescription, ModelProfileKey: demoModelProfileKey,
		BackendProfileKey: demoBackendProfileKey,
	}
}

// DemoResult identifies the complete graph provisioned by InitializeDemo.
type DemoResult struct {
	TenantID         string
	AppID            string
	ModelProfileID   string
	BackendProfileID string
	Revision         int64
	Created          bool
}

// InitializeDemo provisions the minimum executable graph for an offline
// local demo. It uses the existing domain repositories for every write and
// fails closed when existing state is partial or incompatible.
func InitializeDemo(ctx context.Context, db *sql.DB, input DemoConfig) (DemoResult, error) {
	if ctx == nil {
		return DemoResult{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return DemoResult{}, err
	}
	if db == nil {
		return DemoResult{}, ErrDemoInitialization
	}
	config := normalizeDemoConfig(input)
	if err := config.validate(); err != nil {
		return DemoResult{}, err
	}
	initial, err := Initialize(ctx, db, InitConfig{
		TenantKey: config.TenantKey, TenantDisplayName: config.TenantDisplayName,
		AppKey: config.AppKey, AppDisplayName: config.AppDisplayName,
		AppDescription: config.AppDescription,
	})
	if err != nil {
		return DemoResult{}, err
	}
	modelCatalog, backendCatalog, err := environmentCatalogs(environmentConfig{
		demoMode: true, modelProvider: demoModelProvider, modelNames: []string{demoModelName},
	})
	if err != nil {
		return DemoResult{}, fmt.Errorf("%w: demo catalogs", ErrDemoInitialization)
	}
	tenantRepo := tenantpostgres.NewRepository(db)
	appRepo := agentpostgres.NewRepository(db)
	modelRepo := modelpostgres.NewRepository(db, modelCatalog)
	backendRepo := backendpostgres.NewRepository(db, backendCatalog)
	tenantRoot, err := tenantRepo.Get(ctx, initial.TenantID)
	if err != nil {
		return DemoResult{}, demoDependencyError(err)
	}
	app, err := appRepo.Get(ctx, initial.TenantID, initial.AppID)
	if err != nil {
		return DemoResult{}, demoDependencyError(err)
	}
	if tenantRoot.TenantKey != config.TenantKey || app.AppKey != config.AppKey {
		return DemoResult{}, fmt.Errorf("%w: initial tenant or app key does not match demo configuration", ErrDemoState)
	}
	created := initial.Created
	modelID, modelCreated, err := ensureDemoModel(ctx, db, modelRepo, initial.TenantID, config.ModelProfileKey)
	if err != nil {
		return DemoResult{}, err
	}
	created = created || modelCreated
	backendID, backendCreated, err := ensureDemoBackend(ctx, db, backendRepo, initial.TenantID, config.BackendProfileKey)
	if err != nil {
		return DemoResult{}, err
	}
	created = created || backendCreated
	tenantRoot, tenantChanged, err := ensureDemoDefaults(ctx, tenantRepo, tenantRoot, app.AppID, backendID)
	if err != nil {
		return DemoResult{}, err
	}
	created = created || tenantChanged
	app, revision, revisionCreated, err := ensureDemoRevision(ctx, db, appRepo, tenantRoot, app, modelID)
	if err != nil {
		return DemoResult{}, err
	}
	created = created || revisionCreated
	return DemoResult{TenantID: tenantRoot.TenantID, AppID: app.AppID, ModelProfileID: modelID, BackendProfileID: backendID, Revision: revision.Revision, Created: created}, nil
}

func normalizeDemoConfig(config DemoConfig) DemoConfig {
	defaults := DefaultDemoConfig()
	if strings.TrimSpace(config.TenantKey) == "" {
		config.TenantKey = defaults.TenantKey
	}
	if strings.TrimSpace(config.TenantDisplayName) == "" {
		config.TenantDisplayName = defaults.TenantDisplayName
	}
	if strings.TrimSpace(config.AppKey) == "" {
		config.AppKey = defaults.AppKey
	}
	if strings.TrimSpace(config.AppDisplayName) == "" {
		config.AppDisplayName = defaults.AppDisplayName
	}
	if strings.TrimSpace(config.AppDescription) == "" {
		config.AppDescription = defaults.AppDescription
	}
	if strings.TrimSpace(config.ModelProfileKey) == "" {
		config.ModelProfileKey = defaults.ModelProfileKey
	}
	if strings.TrimSpace(config.BackendProfileKey) == "" {
		config.BackendProfileKey = defaults.BackendProfileKey
	}
	config.TenantKey = strings.TrimSpace(config.TenantKey)
	config.TenantDisplayName = strings.TrimSpace(config.TenantDisplayName)
	config.AppKey = strings.TrimSpace(config.AppKey)
	config.AppDisplayName = strings.TrimSpace(config.AppDisplayName)
	config.AppDescription = strings.TrimSpace(config.AppDescription)
	config.ModelProfileKey = strings.TrimSpace(config.ModelProfileKey)
	config.BackendProfileKey = strings.TrimSpace(config.BackendProfileKey)
	return config
}

func (config DemoConfig) validate() error {
	if err := (InitConfig{TenantKey: config.TenantKey, TenantDisplayName: config.TenantDisplayName, AppKey: config.AppKey, AppDisplayName: config.AppDisplayName, AppDescription: config.AppDescription}).Validate(); err != nil {
		return err
	}
	modelCatalog, backendCatalog, err := environmentCatalogs(environmentConfig{demoMode: true, modelProvider: demoModelProvider, modelNames: []string{demoModelName}})
	if err != nil {
		return fmt.Errorf("%w: demo catalogs", ErrDemoInitialization)
	}
	if _, err := modelprofile.NewProfile(modelprofile.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileKey: config.ModelProfileKey, DisplayName: demoModelDisplayName, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}}, modelCatalog); err != nil {
		return fmt.Errorf("%w: invalid demo model configuration", ErrDemoState)
	}
	if _, err := backend.NewProfile(backend.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileKey: config.BackendProfileKey, DisplayName: demoBackendName, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}}, backendCatalog); err != nil {
		return fmt.Errorf("%w: invalid demo backend configuration", ErrDemoState)
	}
	return nil
}

func ensureDemoModel(ctx context.Context, db *sql.DB, repo modelprofile.Repository, tenantID, profileKey string) (string, bool, error) {
	profileID, found, err := findProfileID(ctx, db, "model_profile", tenantID, profileKey)
	if err != nil {
		return "", false, err
	}
	metadata := demoModelMetadata()
	if !found {
		value, _, createErr := repo.Create(ctx, modelprofile.CreateInput{TenantID: tenantID, ProfileKey: profileKey, DisplayName: demoModelDisplayName, Status: modelprofile.StatusActive, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Metadata: metadata})
		if createErr != nil {
			return "", false, demoDependencyError(createErr)
		}
		return value.ProfileID, true, nil
	}
	profile, err := repo.Get(ctx, tenantID, profileID)
	if err != nil {
		return "", false, demoDependencyError(err)
	}
	if profile.ProfileKey != profileKey || profile.Configuration.Provider != demoModelProvider || profile.Configuration.Model != demoModelName || profile.Configuration.SecretRef != "" || profile.Configuration.Endpoint != "" || len(profile.Configuration.Options) != 0 || !emptyModelGeneration(profile.Configuration.Generation) {
		return "", false, fmt.Errorf("%w: model profile does not match offline demo", ErrDemoState)
	}
	if profile.Status == modelprofile.StatusDisabled {
		return "", false, fmt.Errorf("%w: model profile is disabled", ErrDemoState)
	}
	if profile.Status == modelprofile.StatusSuspended {
		if _, _, err := repo.TransitionStatus(ctx, modelprofile.TransitionStatusInput{TenantID: tenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, NextStatus: modelprofile.StatusActive, Metadata: metadata}); err != nil {
			return "", false, demoDependencyError(err)
		}
	}
	return profile.ProfileID, false, nil
}

func ensureDemoBackend(ctx context.Context, db *sql.DB, repo backend.Repository, tenantID, profileKey string) (string, bool, error) {
	profileID, found, err := findProfileID(ctx, db, "backend_profile", tenantID, profileKey)
	if err != nil {
		return "", false, err
	}
	metadata := demoBackendMetadata()
	if !found {
		value, _, createErr := repo.Create(ctx, backend.CreateInput{TenantID: tenantID, ProfileKey: profileKey, DisplayName: demoBackendName, Status: backend.StatusActive, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: metadata})
		if createErr != nil {
			return "", false, demoDependencyError(createErr)
		}
		return value.ProfileID, true, nil
	}
	profile, err := repo.Get(ctx, tenantID, profileID)
	if err != nil {
		return "", false, demoDependencyError(err)
	}
	if profile.ProfileKey != profileKey || len(profile.Bindings) != 1 || profile.Bindings[0].Capability != backend.CapabilitySession || profile.Bindings[0].Provider != "inmemory" || profile.Bindings[0].Endpoint != "" || profile.Bindings[0].SecretRef != "" || len(profile.Bindings[0].Options) != 0 {
		return "", false, fmt.Errorf("%w: backend profile does not match offline demo", ErrDemoState)
	}
	if profile.Status == backend.StatusDisabled {
		return "", false, fmt.Errorf("%w: backend profile is disabled", ErrDemoState)
	}
	if profile.Status == backend.StatusSuspended {
		if _, _, err := repo.TransitionStatus(ctx, backend.TransitionStatusInput{TenantID: tenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, NextStatus: backend.StatusActive, Metadata: metadata}); err != nil {
			return "", false, demoDependencyError(err)
		}
	}
	return profile.ProfileID, false, nil
}

func ensureDemoDefaults(ctx context.Context, repo tenant.Repository, root *tenant.Tenant, appID, backendID string) (*tenant.Tenant, bool, error) {
	if root.DefaultAgentAppID != nil && *root.DefaultAgentAppID == appID && root.DefaultBackendProfileID != nil && *root.DefaultBackendProfileID == backendID {
		return root, false, nil
	}
	updated, err := repo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName,
		RateLimitRPM: cloneInt64Pointer(root.RateLimitRPM), MaxConcurrentExecutions: cloneInt64Pointer(root.MaxConcurrentExecutions),
		MonthlyTokenBudget: cloneInt64Pointer(root.MonthlyTokenBudget), MonthlySpendLimitMinor: cloneInt64Pointer(root.MonthlySpendLimitMinor),
		BillingCurrency: root.BillingCurrency, AuditRetentionDays: root.AuditRetentionDays, LogMaskingLevel: root.LogMaskingLevel,
		TraceSamplingRate: root.TraceSamplingRate, DefaultAgentAppID: stringPointer(appID), DefaultBackendProfileID: stringPointer(backendID),
	})
	if err != nil {
		return nil, false, demoDependencyError(err)
	}
	return updated, true, nil
}

func ensureDemoRevision(ctx context.Context, db *sql.DB, apps agent.Repository, root *tenant.Tenant, app *agent.App, modelID string) (*agent.App, *agent.Revision, bool, error) {
	metadata := agent.ChangeMetadata{ActorType: demoActorType, ActorID: demoActorID, Reason: demoReason, CorrelationID: demoCorrelationID}
	if app.CurrentRevision != nil {
		revision, err := apps.GetRevision(ctx, root.TenantID, app.AppID, *app.CurrentRevision)
		if err != nil {
			return nil, nil, false, demoDependencyError(err)
		}
		if revision.State != agent.RevisionStatePublished || !demoRevisionMatches(revision, modelID) || app.Status == agent.StatusDisabled {
			return nil, nil, false, fmt.Errorf("%w: published app graph does not match offline demo", ErrDemoState)
		}
		if app.Status == agent.StatusSuspended {
			active, _, err := apps.TransitionStatus(ctx, agent.TransitionStatusInput{TenantID: root.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: agent.StatusActive, Metadata: metadata})
			if err != nil {
				return nil, nil, false, demoDependencyError(err)
			}
			app = active
		}
		return app, revision, false, nil
	}
	if app.Status != agent.StatusDraft {
		return nil, nil, false, fmt.Errorf("%w: app has no published revision", ErrDemoState)
	}
	revisions, err := findRevisionNumbers(ctx, db, root.TenantID, app.AppID)
	if err != nil {
		return nil, nil, false, err
	}
	var draft *agent.Revision
	created := false
	switch len(revisions) {
	case 0:
		createdDraft, createErr := apps.CreateDraft(ctx, agent.CreateDraftInput{TenantID: root.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Kind: agent.KindLLM, SchemaVersion: agent.SchemaVersionV1, Configuration: agent.DraftConfiguration{Instruction: demoInstruction, ModelProfileID: modelID, Runtime: agent.DefaultRuntimePolicy()}})
		if createErr != nil {
			return nil, nil, false, demoDependencyError(createErr)
		}
		draft, created = createdDraft, true
	default:
		if len(revisions) != 1 {
			return nil, nil, false, fmt.Errorf("%w: app has multiple unpublished revisions", ErrDemoState)
		}
		draft, err = apps.GetRevision(ctx, root.TenantID, app.AppID, revisions[0])
		if err != nil {
			return nil, nil, false, demoDependencyError(err)
		}
		if draft.State != agent.RevisionStateDraft || !demoRevisionMatches(draft, modelID) {
			return nil, nil, false, fmt.Errorf("%w: draft revision does not match offline demo", ErrDemoState)
		}
	}
	app, err = apps.Get(ctx, root.TenantID, app.AppID)
	if err != nil {
		return nil, nil, false, demoDependencyError(err)
	}
	activeApp, published, _, err := apps.Publish(ctx, agent.PublishInput{TenantID: root.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: root.Status == tenant.StatusActive, Metadata: metadata})
	if err != nil {
		return nil, nil, false, demoDependencyError(err)
	}
	return activeApp, published, created, nil
}

func findProfileID(ctx context.Context, db *sql.DB, kind, tenantID, profileKey string) (string, bool, error) {
	query := ""
	switch kind {
	case "model_profile":
		query = "SELECT profile_id FROM public.model_profile WHERE tenant_id = $1 AND profile_key = $2 LIMIT 2"
	case "backend_profile":
		query = "SELECT profile_id FROM public.backend_profile WHERE tenant_id = $1 AND profile_key = $2 LIMIT 2"
	default:
		return "", false, ErrDemoInitialization
	}
	rows, err := db.QueryContext(ctx, query, tenantID, profileKey)
	if err != nil {
		return "", false, demoDependencyError(err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", false, demoDependencyError(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return "", false, demoDependencyError(err)
	}
	if len(ids) > 1 {
		return "", false, fmt.Errorf("%w: duplicate %s profile key", ErrDemoState, kind)
	}
	if len(ids) == 0 {
		return "", false, nil
	}
	return ids[0], true, nil
}

func findRevisionNumbers(ctx context.Context, db *sql.DB, tenantID, appID string) ([]int64, error) {
	rows, err := db.QueryContext(ctx, "SELECT revision FROM public.agent_app_revision WHERE tenant_id = $1 AND app_id = $2 ORDER BY revision LIMIT 2", tenantID, appID)
	if err != nil {
		return nil, demoDependencyError(err)
	}
	defer func() { _ = rows.Close() }()
	values := make([]int64, 0, 2)
	for rows.Next() {
		var revision int64
		if err := rows.Scan(&revision); err != nil {
			return nil, demoDependencyError(err)
		}
		values = append(values, revision)
	}
	if err := rows.Err(); err != nil {
		return nil, demoDependencyError(err)
	}
	return values, nil
}

func demoDependencyError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: control-plane operation failed", ErrDemoInitialization)
}

func demoModelMetadata() modelprofile.ChangeMetadata {
	return modelprofile.ChangeMetadata{ActorType: demoActorType, ActorID: demoActorID, Reason: demoReason, CorrelationID: demoCorrelationID}
}

func demoBackendMetadata() backend.ChangeMetadata {
	return backend.ChangeMetadata{ActorType: demoActorType, ActorID: demoActorID, Reason: demoReason, CorrelationID: demoCorrelationID}
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func stringPointer(value string) *string { return &value }

func emptyModelGeneration(generation modelprofile.GenerationConfig) bool {
	return generation.Temperature == nil && generation.TopP == nil && generation.MaxOutputTokens == nil
}

func demoRevisionMatches(revision *agent.Revision, modelID string) bool {
	return revision != nil && revision.Kind == agent.KindLLM && revision.SchemaVersion == agent.SchemaVersionV1 &&
		revision.Description == "" && revision.Instruction == demoInstruction && revision.GlobalInstruction == "" &&
		revision.ModelProfileID == modelID && emptyAgentGeneration(revision.Generation) &&
		revision.Runtime == agent.DefaultRuntimePolicy() && len(revision.Tools) == 0
}

func emptyAgentGeneration(generation agent.GenerationConfig) bool {
	return generation.Temperature == nil && generation.TopP == nil && generation.MaxOutputTokens == nil
}

// WriteDemoResult emits only non-secret values needed to run the local demo.
func WriteDemoResult(w io.Writer, result DemoResult) error {
	if w == nil || !validInitOutputID(result.TenantID, "t_") || !validInitOutputID(result.AppID, "app_") || !validInitOutputID(result.ModelProfileID, "mp_") || !validInitOutputID(result.BackendProfileID, "bp_") || result.Revision < 1 {
		return ErrDemoInitialization
	}
	if result.Created {
		if _, err := fmt.Fprintln(w, "# trpc-service demo created the offline deterministic graph"); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintln(w, "# trpc-service demo found the existing offline deterministic graph"); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "export TRPC_DEMO_MODE='true'\nexport TRPC_MODEL_PROVIDER='fake'\nexport TRPC_MODEL_NAMES='deterministic'\nexport TRPC_SESSION_BACKEND='inmemory'\nexport TRPC_TENANT_ID='%s'\nexport TRPC_APP_ID='%s'\n# TRPC_DEMO_MODEL_PROFILE_ID='%s'\n# TRPC_DEMO_BACKEND_PROFILE_ID='%s'\n# TRPC_DEMO_REVISION='%d'\n", result.TenantID, result.AppID, result.ModelProfileID, result.BackendProfileID, result.Revision)
	return err
}

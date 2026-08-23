package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLRepositories(t *testing.T) {
	dsn := os.Getenv("POSTGRES_REPOSITORY_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_REPOSITORY_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := runRepositoryMigrations(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("close repository database: %v", err)
		}
	}()

	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"},
		SecretRefPolicy: model.FieldOptional,
		Options:         map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}

	metadata := tenant.TransitionMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "repo-test"}
	tenants := postgres.NewTenantRepository(db)
	root, err := tenants.Create(ctx, tenant.CreateInput{
		TenantKey: "postgres-repo", DisplayName: "Postgres Repository", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := tenants.Get(ctx, root.TenantID); err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	updatedTenant, err := tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: "Postgres Repository Updated",
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatalf("update tenant: %v", err)
	}
	if updatedTenant.Version != root.Version+1 {
		t.Fatalf("tenant version = %d, want %d", updatedTenant.Version, root.Version+1)
	}
	suspendedTenant, _, err := tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{
		TenantID: root.TenantID, ExpectedVersion: updatedTenant.Version,
		NextStatus: tenant.StatusSuspended, Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("suspend tenant: %v", err)
	}
	if _, _, err := tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{
		TenantID: root.TenantID, ExpectedVersion: suspendedTenant.Version,
		NextStatus: tenant.StatusActive, Metadata: metadata,
	}); err != nil {
		t.Fatalf("resume tenant: %v", err)
	}

	models := postgres.NewModelRepository(db, modelCatalog)
	profile, _, err := models.Create(ctx, model.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary", DisplayName: "Primary Model", Status: model.StatusActive,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
		Metadata:      model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "model-test"},
	})
	if err != nil {
		t.Fatalf("create model profile: %v", err)
	}
	loadedProfile, err := models.Get(ctx, root.TenantID, profile.ProfileID)
	if err != nil {
		t.Fatalf("get model profile: %v", err)
	}
	if loadedProfile.TenantID != root.TenantID || loadedProfile.ProfileID != profile.ProfileID || loadedProfile.Configuration.Options["mode"] != "safe" {
		t.Fatalf("loaded model scope = %s/%s", loadedProfile.TenantID, loadedProfile.ProfileID)
	}
	loadedProfile.Configuration.Options["mode"] = "mutated"
	if again, err := models.Get(ctx, root.TenantID, profile.ProfileID); err != nil || again.Configuration.Options["mode"] != "safe" {
		t.Fatalf("model defensive copy = %+v, err=%v", again, err)
	}
	updatedProfile, _, err := models.UpdateConfiguration(ctx, model.UpdateConfigurationInput{
		TenantID: root.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		DisplayName: "Primary Model Updated", SchemaVersion: profile.SchemaVersion,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
		Metadata:      model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "model-update"},
	})
	if err != nil {
		t.Fatalf("update model profile: %v", err)
	}
	profile = updatedProfile
	if _, _, err := models.TransitionStatus(ctx, model.TransitionStatusInput{
		TenantID: root.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		NextStatus: model.StatusSuspended,
		Metadata:   model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "model-status"},
	}); err != nil {
		t.Fatalf("suspend model profile: %v", err)
	}

	apps := postgres.NewAgentRepository(db)
	app, err := apps.Create(ctx, agent.CreateInput{TenantID: root.TenantID, AppKey: "primary-app", DisplayName: "Primary App"})
	if err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	draft, err := apps.CreateDraft(ctx, agent.CreateDraftInput{
		TenantID: root.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Kind: agent.KindLLM, SchemaVersion: agent.SchemaVersionV1,
		Configuration: agent.DraftConfiguration{
			Instruction: "Answer briefly", ModelProfileID: profile.ProfileID,
			Runtime: agent.DefaultRuntimePolicy(),
		},
	})
	if err != nil {
		t.Fatalf("create agent draft: %v", err)
	}
	publishedApp, publishedRevision, event, err := apps.Publish(ctx, agent.PublishInput{
		TenantID: root.TenantID, AppID: app.AppID, Revision: draft.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion,
		TenantActive: true,
		Metadata:     agent.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-publish"},
	})
	if err != nil {
		t.Fatalf("publish agent: %v", err)
	}
	if publishedApp.CurrentRevision == nil || *publishedApp.CurrentRevision != publishedRevision.Revision || event.EventType != agent.ChangePublished {
		t.Fatalf("publication result = app=%+v revision=%+v event=%+v", publishedApp, publishedRevision, event)
	}
	app, err = apps.UpdateMetadata(ctx, agent.UpdateMetadataInput{
		TenantID: root.TenantID, AppID: app.AppID, ExpectedVersion: publishedApp.Version,
		DisplayName: "Primary App Updated", Description: "integration app",
	})
	if err != nil {
		t.Fatalf("update agent metadata: %v", err)
	}
	secondDraft, err := apps.CreateDraft(ctx, agent.CreateDraftInput{
		TenantID: root.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Kind: agent.KindLLM, SchemaVersion: agent.SchemaVersionV1,
		Configuration: agent.DraftConfiguration{
			Instruction: "Answer with more detail", ModelProfileID: profile.ProfileID,
			Runtime: agent.DefaultRuntimePolicy(),
		},
	})
	if err != nil {
		t.Fatalf("create second agent draft: %v", err)
	}
	updatedDraft, err := apps.UpdateDraft(ctx, agent.UpdateDraftInput{
		TenantID: root.TenantID, AppID: app.AppID, Revision: secondDraft.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: secondDraft.DraftVersion,
		Configuration: agent.DraftConfiguration{
			Instruction: "Answer with detail", ModelProfileID: profile.ProfileID,
			Runtime: agent.DefaultRuntimePolicy(),
		},
	})
	if err != nil {
		t.Fatalf("update second agent draft: %v", err)
	}
	publishedApp, _, _, err = apps.Publish(ctx, agent.PublishInput{
		TenantID: root.TenantID, AppID: app.AppID, Revision: updatedDraft.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: updatedDraft.DraftVersion,
		TenantActive: true,
		Metadata:     agent.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-publish-two"},
	})
	if err != nil {
		t.Fatalf("publish second agent revision: %v", err)
	}
	app = publishedApp
	rolledBackApp, rollbackEvent, err := apps.Rollback(ctx, agent.RollbackInput{
		TenantID: root.TenantID, AppID: app.AppID, TargetRevision: draft.Revision,
		ExpectedAppVersion: app.Version,
		Metadata:           agent.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-rollback"},
	})
	if err != nil {
		t.Fatalf("rollback agent revision: %v", err)
	}
	if rolledBackApp.CurrentRevision == nil || *rolledBackApp.CurrentRevision != draft.Revision || rollbackEvent.EventType != agent.ChangeRolledBack {
		t.Fatalf("rollback result = app=%+v event=%+v", rolledBackApp, rollbackEvent)
	}
	app = rolledBackApp

	backends := postgres.NewBackendRepository(db, backendCatalog)
	backendProfile, _, err := backends.Create(ctx, backend.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary-backend", DisplayName: "Primary Backend", Status: backend.StatusActive,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "primary"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-test"},
	})
	if err != nil {
		t.Fatalf("create backend profile: %v", err)
	}
	if loaded, err := backends.Get(ctx, root.TenantID, backendProfile.ProfileID); err != nil || len(loaded.Bindings) != 1 || loaded.Bindings[0].Options["namespace"] != "primary" {
		t.Fatalf("get backend profile = %+v, err=%v", loaded, err)
	}
	updatedBackend, _, err := backends.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{
		TenantID: root.TenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version,
		DisplayName: "Primary Backend Updated", SchemaVersion: backendProfile.SchemaVersion,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "updated"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-update"},
	})
	if err != nil {
		t.Fatalf("update backend profile: %v", err)
	}
	backendProfile = updatedBackend
	if _, _, err := backends.TransitionStatus(ctx, backend.TransitionStatusInput{
		TenantID: root.TenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version,
		NextStatus: backend.StatusSuspended,
		Metadata:   backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-suspend"},
	}); err != nil {
		t.Fatalf("suspend backend profile: %v", err)
	}
	_, _, err = backends.TransitionStatus(ctx, backend.TransitionStatusInput{
		TenantID: root.TenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version + 1,
		NextStatus: backend.StatusActive,
		Metadata:   backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-resume"},
	})
	if err != nil {
		t.Fatalf("resume backend profile: %v", err)
	}

	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "repo-test-route")
	if err != nil {
		t.Fatal(err)
	}
	channelRepo := postgres.NewChannelRepository(db)
	binding, _, err := channelRepo.Create(ctx, channels.CreateInput{
		TenantID: root.TenantID, BindingKey: "primary-channel", Channel: channels.ChannelTelegram,
		ProviderAccountID: "repo-account", PublicRouteKeyDigest: routeDigest, AppID: app.AppID,
		SecretRef: "secret://repo-test", Protocol: channels.ProtocolConfiguration{
			Telegram: &channels.TelegramProtocolConfiguration{},
		}, Status: channels.StatusDraft,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-test"},
	})
	if err != nil {
		t.Fatalf("create channel binding: %v", err)
	}
	activeBinding, _, err := channelRepo.Activate(ctx, channels.TransitionStatusInput{
		TenantID: root.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-activate"},
	})
	if err != nil {
		t.Fatalf("activate channel binding: %v", err)
	}
	updatedBinding, _, err := channelRepo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{
		TenantID: root.TenantID, BindingID: binding.BindingID, ExpectedVersion: activeBinding.Version,
		ProviderAccountID: "repo-account", PublicRouteKeyDigest: routeDigest, AppID: app.AppID,
		SecretRef: "secret://repo-test", Protocol: channels.ProtocolConfiguration{
			Telegram: &channels.TelegramProtocolConfiguration{},
		}, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-update"},
	})
	if err != nil {
		t.Fatalf("update channel binding: %v", err)
	}
	activeBinding = updatedBinding
	if _, _, err := channelRepo.Suspend(ctx, channels.TransitionStatusInput{
		TenantID: root.TenantID, BindingID: binding.BindingID, ExpectedVersion: activeBinding.Version,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-suspend"},
	}); err != nil {
		t.Fatalf("suspend channel binding: %v", err)
	}
	activeBinding, _, err = channelRepo.Resume(ctx, channels.TransitionStatusInput{
		TenantID: root.TenantID, BindingID: binding.BindingID, ExpectedVersion: activeBinding.Version + 1,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-resume"},
	})
	if err != nil {
		t.Fatalf("resume channel binding: %v", err)
	}
	candidates, err := channelRepo.LookupCandidates(ctx, channels.ChannelTelegram, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("lookup candidates = %d, err=%v", len(candidates), err)
	}
	consumed, err := channelRepo.ConsumeCandidate(ctx, candidates[0])
	if err != nil {
		t.Fatalf("consume candidate: %v", err)
	}
	if consumed.BindingID != activeBinding.BindingID || consumed.Version != activeBinding.Version {
		t.Fatalf("consumed binding = %+v, active = %+v", consumed, activeBinding)
	}
	if _, err := channelRepo.ConsumeCandidate(ctx, candidates[0]); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate replay error = %v", err)
	}

	canceled, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	if _, err := models.Get(canceled, root.TenantID, profile.ProfileID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled model get error = %v", err)
	}
}

func runRepositoryMigrations(ctx context.Context, dsn string) error {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("locate repository migration test source")
	}
	migrationDir := filepath.Join(filepath.Dir(sourceFile), "..", "..", "..", "migrations")
	for _, name := range []string{"0001_control_plane.up.sql", "0002_control_plane_repository_functions.up.sql"} {
		contents, err := os.ReadFile(filepath.Join(migrationDir, name))
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(contents)); err != nil {
			return err
		}
	}
	return nil
}

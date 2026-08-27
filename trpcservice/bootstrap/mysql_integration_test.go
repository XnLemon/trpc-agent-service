package bootstrap_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentmysql "github.com/XnLemon/trpc-agent-service/trpcservice/agent/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmysql "github.com/XnLemon/trpc-agent-service/trpcservice/backend/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/model/mysql"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
)

func TestMySQLControlPlaneRepositoriesLive(t *testing.T) {
	t.Helper()
	dsn := os.Getenv("MYSQL_CONTROL_PLANE_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_CONTROL_PLANE_TEST_DSN is not configured")
	}
	ctx := context.Background()
	db := openMySQLControlPlaneTestDB(t, ctx, dsn)
	defer func() { _ = db.Close() }()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	first, second := createMySQLTestTenants(t, ctx, db, suffix)
	profile := createMySQLTestModel(t, ctx, db, first.TenantID, suffix)
	modelRepo := modelmysql.NewRepository(db, testMySQLModelCatalog(t))
	if _, err := modelRepo.Get(ctx, second.TenantID, profile.ProfileID); !errors.Is(err, modelprofile.ErrNotFound) {
		t.Fatalf("cross-tenant model read = %v", err)
	}

	createMySQLTestBackend(t, ctx, db, first.TenantID, suffix)

	app, draft := createMySQLTestDraft(t, ctx, db, first.TenantID, profile.ProfileID, suffix)
	publishedApp, publishedRevision := publishMySQLTestDraft(t, ctx, db, first.TenantID, app, draft, suffix)

	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "route-"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	assertMySQLTestChannel(t, ctx, db, first.TenantID, app.AppID, routeDigest, suffix)
	if publishedApp.CurrentRevision == nil || publishedRevision.State != agent.RevisionStatePublished {
		t.Fatalf("publish = app=%+v revision=%+v", publishedApp, publishedRevision)
	}
}

func openMySQLControlPlaneTestDB(t *testing.T, ctx context.Context, dsn string) *sql.DB {
	t.Helper()
	db, err := storage.Open(ctx, dsn, storage.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyMySQL(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

func createMySQLTestTenants(t *testing.T, ctx context.Context, db *sql.DB, suffix string) (*tenant.Tenant, *tenant.Tenant) {
	t.Helper()
	tenants := tenantmysql.NewRepository(db)
	first, err := tenants.Create(ctx, tenant.CreateInput{TenantKey: "mysql-" + suffix, DisplayName: "Primary"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := tenants.Create(ctx, tenant.CreateInput{TenantKey: "mysql-other-" + suffix, DisplayName: "Other"})
	if err != nil {
		t.Fatal(err)
	}
	return first, second
}

func testMySQLModelCatalog(t *testing.T) *modelprofile.ProviderCatalog {
	t.Helper()
	catalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: modelprofile.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: modelprofile.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func createMySQLTestModel(t *testing.T, ctx context.Context, db *sql.DB, tenantID, suffix string) *modelprofile.Profile {
	t.Helper()
	repo := modelmysql.NewRepository(db, testMySQLModelCatalog(t))
	profile, _, err := repo.Create(ctx, modelprofile.CreateInput{
		TenantID: tenantID, ProfileKey: "primary-" + suffix, DisplayName: "Primary",
		Configuration: modelprofile.Configuration{Provider: "public", Model: "chat"},
		Metadata:      modelprofile.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "create", CorrelationID: suffix},
	})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func createMySQLTestBackend(t *testing.T, ctx context.Context, db *sql.DB, tenantID, suffix string) {
	t.Helper()
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = backendmysql.NewRepository(db, catalog).Create(ctx, backend.CreateInput{
		TenantID: tenantID, ProfileKey: "default-" + suffix, DisplayName: "Default",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": suffix}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "create", CorrelationID: suffix},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func createMySQLTestDraft(t *testing.T, ctx context.Context, db *sql.DB, tenantID, profileID, suffix string) (*agent.App, *agent.Revision) {
	t.Helper()
	repo := agentmysql.NewRepository(db)
	app, err := repo.Create(ctx, agent.CreateInput{TenantID: tenantID, AppKey: "assistant-" + suffix, DisplayName: "Assistant"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := repo.CreateDraft(ctx, agent.CreateDraftInput{
		TenantID: tenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Configuration: agent.DraftConfiguration{Description: "draft", Instruction: "Answer clearly.", ModelProfileID: profileID},
	})
	if err != nil {
		t.Fatal(err)
	}
	return app, draft
}

func publishMySQLTestDraft(t *testing.T, ctx context.Context, db *sql.DB, tenantID string, app *agent.App, draft *agent.Revision, suffix string) (*agent.App, *agent.Revision) {
	t.Helper()
	repo := agentmysql.NewRepository(db)
	publishedApp, publishedRevision, _, err := repo.Publish(ctx, agent.PublishInput{
		TenantID: tenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version,
		ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "publish", CorrelationID: suffix},
	})
	if err != nil {
		t.Fatal(err)
	}
	return publishedApp, publishedRevision
}

func assertMySQLTestChannel(t *testing.T, ctx context.Context, db *sql.DB, tenantID, appID string, routeDigest, suffix string) {
	t.Helper()
	repo := channelmysql.NewRepository(db)
	binding, _, err := repo.Create(ctx, channels.CreateInput{
		TenantID: tenantID, BindingKey: "telegram-" + suffix, Channel: channels.ChannelTelegram,
		ProviderAccountID: "account-" + suffix, PublicRouteKeyDigest: routeDigest, AppID: appID,
		SecretRef: "env/telegram-" + suffix, Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{WebhookPath: "/inbound"}},
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "create", CorrelationID: suffix},
	})
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := repo.Activate(ctx, channels.TransitionStatusInput{TenantID: tenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "activate", CorrelationID: suffix}})
	if err != nil || active.Status != channels.StatusActive {
		t.Fatalf("activate = %+v, err=%v", active, err)
	}
	candidates, err := repo.LookupCandidates(ctx, channels.ChannelTelegram, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidate lookup = %+v, err=%v", candidates, err)
	}
	consumed, err := repo.ConsumeCandidate(ctx, candidates[0])
	if err != nil || consumed.BindingID != binding.BindingID || consumed.TenantID != tenantID {
		t.Fatalf("candidate consume = %+v, err=%v", consumed, err)
	}
	if _, err := repo.ConsumeCandidate(ctx, candidates[0]); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate reuse = %v", err)
	}
}

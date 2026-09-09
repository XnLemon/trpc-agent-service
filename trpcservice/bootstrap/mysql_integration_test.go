package bootstrap_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmysql "github.com/XnLemon/trpc-agent-service/trpcservice/app/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	auditmysql "github.com/XnLemon/trpc-agent-service/trpcservice/audit/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmysql "github.com/XnLemon/trpc-agent-service/trpcservice/backend/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/model/mysql"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestoragemysql "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/mysql"
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
	migrationDSN := os.Getenv("MYSQL_CONTROL_PLANE_REPOSITORY_MIGRATION_DSN")
	if migrationDSN == "" {
		t.Skip("MYSQL_CONTROL_PLANE_REPOSITORY_MIGRATION_DSN is not configured")
	}
	ctx := context.Background()
	db := openMySQLControlPlaneTestDB(t, ctx, dsn, migrationDSN)
	defer func() { _ = db.Close() }()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	first, second := createMySQLTestTenants(t, ctx, db, suffix)
	profile := createMySQLTestModel(t, ctx, db, first.TenantID, suffix)
	modelRepo := modelmysql.NewRepository(db, testMySQLModelCatalog(t))
	if _, err := modelRepo.Get(ctx, second.TenantID, profile.ProfileID); !errors.Is(err, modelprofile.ErrNotFound) {
		t.Fatalf("cross-tenant model read = %v", err)
	}

	backendProfile := createMySQLTestBackend(t, ctx, db, first.TenantID, suffix)
	backendRepo := backendmysql.NewRepository(db, testMySQLBackendCatalog(t))
	updatedBackend, _, err := backendRepo.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{
		TenantID: first.TenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version,
		DisplayName: "Updated", Description: backendProfile.Description, SchemaVersion: backendProfile.SchemaVersion,
		Bindings: backendProfile.Bindings,
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "update", CorrelationID: suffix + "-update"},
	})
	if err != nil {
		t.Fatal(err)
	}
	suspendedBackend, _, err := backendRepo.TransitionStatus(ctx, backend.TransitionStatusInput{
		TenantID: first.TenantID, ProfileID: updatedBackend.ProfileID, ExpectedVersion: updatedBackend.Version,
		NextStatus: backend.StatusSuspended,
		Metadata:   backend.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "suspend", CorrelationID: suffix + "-suspend"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if suspendedBackend.Status != backend.StatusSuspended {
		t.Fatalf("backend suspend = %+v", suspendedBackend)
	}
	resumedBackend, _, err := backendRepo.TransitionStatus(ctx, backend.TransitionStatusInput{
		TenantID: first.TenantID, ProfileID: suspendedBackend.ProfileID, ExpectedVersion: suspendedBackend.Version,
		NextStatus: backend.StatusActive,
		Metadata:   backend.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "resume", CorrelationID: suffix + "-resume"},
	})
	if err != nil || resumedBackend.Status != backend.StatusActive {
		t.Fatalf("backend resume = %+v, err=%v", resumedBackend, err)
	}

	appRoot, draft := createMySQLTestDraft(t, ctx, db, first.TenantID, profile.ProfileID, suffix)
	publishedApp, publishedRevision := publishMySQLTestDraft(t, ctx, db, first.TenantID, appRoot, draft, suffix)

	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "route-"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	assertMySQLTestChannel(t, ctx, db, first.TenantID, appRoot.AppID, routeDigest, suffix)
	if publishedApp.CurrentRevision == nil || publishedRevision.State != appmodel.RevisionStatePublished {
		t.Fatalf("publish = app=%+v revision=%+v", publishedApp, publishedRevision)
	}

	invocations := runtimestoragemysql.New(db)
	invocationInput := runtimestorage.ToolInvocationInput{
		TenantID: first.TenantID, AppID: appRoot.AppID, EventID: "tool-event-" + suffix, RequestID: "tool-request-" + suffix,
		TraceID: "tool-trace-" + suffix, ToolCallID: "tool-call-" + suffix,
		ToolName: "mcp_demo__write", ArgsSHA256: strings.Repeat("a", 64), Owner: "tool-request-" + suffix,
	}
	invocationInput.InvocationID = runtimestorage.DeriveToolInvocationIDForApp(invocationInput.TenantID, invocationInput.AppID, invocationInput.EventID, invocationInput.ToolCallID, invocationInput.ToolName, invocationInput.ArgsSHA256)
	prepared, err := invocations.PrepareToolInvocation(ctx, invocationInput)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := invocations.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: invocationInput.TenantID, AppID: invocationInput.AppID, InvocationID: invocationInput.InvocationID,
		From: prepared.Status, To: runtimestorage.ToolInvocationDispatching,
		Owner: invocationInput.Owner, FencingToken: prepared.FencingToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err = invocations.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: invocationInput.TenantID, AppID: invocationInput.AppID, InvocationID: invocationInput.InvocationID,
		From: accepted.Status, To: runtimestorage.ToolInvocationAccepted,
		Owner: invocationInput.Owner, FencingToken: accepted.FencingToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := invocations.RecoverStaleToolInvocations(ctx, time.Now().UTC().Add(time.Second))
	if err != nil || len(recovered) == 0 || recovered[0].Status != runtimestorage.ToolInvocationUnknown {
		t.Fatalf("tool invocation recovery = %+v, err=%v", recovered, err)
	}
	if _, err := invocations.GetToolInvocation(ctx, second.TenantID, invocationInput.AppID, invocationInput.InvocationID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant tool invocation read = %v", err)
	}
	if _, err := invocations.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: invocationInput.TenantID, AppID: invocationInput.AppID, InvocationID: invocationInput.InvocationID,
		From: runtimestorage.ToolInvocationAccepted, To: runtimestorage.ToolInvocationSucceeded,
		Owner: invocationInput.Owner, FencingToken: accepted.FencingToken,
	}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale tool invocation completion = %v", err)
	}
	auditWriter, err := auditmysql.New(db, first.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	auditEvent := audit.Event{
		SchemaVersion: audit.SchemaVersion, EventID: "audit-event-" + suffix, EventType: audit.EventToolExecuted,
		TenantID: first.TenantID, ToolName: invocationInput.ToolName, RequestID: invocationInput.RequestID,
		TraceID: invocationInput.TraceID, Decision: audit.DecisionAccepted, OccurredAt: time.Now().UTC(),
	}
	if _, err := auditWriter.Append(ctx, auditEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := auditWriter.Append(ctx, auditEvent); err != nil {
		t.Fatalf("idempotent audit append = %v", err)
	}
	auditEvent.Reason = "conflicting digest"
	if _, err := auditWriter.Append(ctx, auditEvent); !errors.Is(err, audit.ErrConflict) {
		t.Fatalf("conflicting audit append = %v", err)
	}
	otherAudit, err := auditmysql.New(db, second.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherAudit.Append(ctx, audit.Event{
		SchemaVersion: audit.SchemaVersion, EventID: "audit-other-" + suffix, EventType: audit.EventToolExecuted,
		TenantID: first.TenantID, ToolName: invocationInput.ToolName, RequestID: invocationInput.RequestID,
		TraceID: invocationInput.TraceID, Decision: audit.DecisionAccepted, OccurredAt: time.Now().UTC(),
	}); !errors.Is(err, audit.ErrConflict) {
		t.Fatalf("cross-tenant audit append = %v", err)
	}
}

func openMySQLControlPlaneTestDB(t *testing.T, ctx context.Context, dsn, migrationDSN string) *sql.DB {
	t.Helper()
	migrationDB, err := storage.Open(ctx, migrationDSN, storage.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyMySQL(ctx, migrationDB); err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	if err := migrations.VerifyMySQL(ctx, migrationDB); err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	migrationUser, err := storage.CurrentUser(ctx, migrationDB)
	if err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	migrationDatabase, err := storage.CurrentDatabase(ctx, migrationDB)
	if err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	if err := migrationDB.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(ctx, dsn, storage.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	appUser, err := storage.CurrentUser(ctx, db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	appDatabase, err := storage.CurrentDatabase(ctx, db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if appUser == migrationUser || appDatabase != migrationDatabase {
		_ = db.Close()
		t.Fatalf("migration and application identities are invalid: users %q/%q databases %q/%q", migrationUser, appUser, migrationDatabase, appDatabase)
	}
	if err := storage.VerifyApplicationPrivileges(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var sessionTimeZone string
	if err := db.QueryRowContext(ctx, "SELECT @@session.time_zone").Scan(&sessionTimeZone); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if sessionTimeZone != "+00:00" {
		_ = db.Close()
		t.Fatalf("session time_zone = %q, want +00:00", sessionTimeZone)
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

func testMySQLBackendCatalog(t *testing.T) *backend.ProviderCatalog {
	t.Helper()
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func createMySQLTestBackend(t *testing.T, ctx context.Context, db *sql.DB, tenantID, suffix string) *backend.Profile {
	t.Helper()
	catalog := testMySQLBackendCatalog(t)
	profile, _, err := backendmysql.NewRepository(db, catalog).Create(ctx, backend.CreateInput{
		TenantID: tenantID, ProfileKey: "default-" + suffix, DisplayName: "Default",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": suffix}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "create", CorrelationID: suffix},
	})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func createMySQLTestDraft(t *testing.T, ctx context.Context, db *sql.DB, tenantID, profileID, suffix string) (*appmodel.App, *appmodel.Revision) {
	t.Helper()
	repo := appmysql.NewAppRepository(db)
	appRoot, err := repo.Create(ctx, appmodel.CreateInput{TenantID: tenantID, AppKey: "assistant-" + suffix, DisplayName: "Assistant"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := repo.CreateDraft(ctx, appmodel.CreateDraftInput{
		TenantID: tenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version,
		Configuration: appmodel.DraftConfiguration{Description: "draft", Instruction: "Answer clearly.", ModelProfileID: profileID},
	})
	if err != nil {
		t.Fatal(err)
	}
	return appRoot, draft
}

func publishMySQLTestDraft(t *testing.T, ctx context.Context, db *sql.DB, tenantID string, appRoot *appmodel.App, draft *appmodel.Revision, suffix string) (*appmodel.App, *appmodel.Revision) {
	t.Helper()
	repo := appmysql.NewAppRepository(db)
	publishedApp, publishedRevision, _, err := repo.Publish(ctx, appmodel.PublishInput{
		TenantID: tenantID, AppID: appRoot.AppID, Revision: draft.Revision, ExpectedAppVersion: appRoot.Version,
		ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "publish", CorrelationID: suffix},
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

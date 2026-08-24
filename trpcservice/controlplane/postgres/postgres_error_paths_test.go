package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

var registerPostgresErrorDriver sync.Once

type postgresErrorDriver struct{}

func (postgresErrorDriver) Open(string) (driver.Conn, error) { return postgresErrorConn{}, nil }

type postgresErrorConn struct{}

func (postgresErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare failure")
}
func (postgresErrorConn) Close() error              { return nil }
func (postgresErrorConn) Begin() (driver.Tx, error) { return postgresErrorTx{}, nil }
func (postgresErrorConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return postgresErrorTx{}, nil
}
func (postgresErrorConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("exec failure")
}
func (postgresErrorConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("query failure")
}
func (postgresErrorConn) Ping(context.Context) error { return nil }

type postgresErrorTx struct{}

func (postgresErrorTx) Commit() error   { return errors.New("commit failure") }
func (postgresErrorTx) Rollback() error { return nil }

func openPostgresErrorDB(t *testing.T) *sql.DB {
	t.Helper()
	registerPostgresErrorDriver.Do(func() {
		sql.Register("trpc-postgres-error", postgresErrorDriver{})
	})
	db, err := sql.Open("trpc-postgres-error", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPostgreSQLRepositoriesMapDriverFailures(t *testing.T) {
	db := openPostgresErrorDB(t)
	ctx := context.Background()
	metadata := tenant.TransitionMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	changeMetadata := agent.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	modelMetadata := model.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	backendMetadata := backend.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	channelMetadata := channels.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}

	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}

	tenantRepo := NewTenantRepository(db)
	if _, err := tenantRepo.Create(ctx, tenant.CreateInput{
		TenantKey: "error-path", DisplayName: "Error Path", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Create error = %v", err)
	}
	if _, err := tenantRepo.Get(ctx, "tenant"); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Get error = %v", err)
	}
	if _, err := tenantRepo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
		TenantID: "tenant", ExpectedVersion: 1, DisplayName: "Tenant", AuditRetentionDays: 90,
		LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Update error = %v", err)
	}
	if _, _, err := tenantRepo.TransitionStatus(ctx, tenant.TransitionStatusInput{
		TenantID: "tenant", ExpectedVersion: 1, NextStatus: tenant.StatusSuspended, Metadata: metadata,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Transition error = %v", err)
	}

	modelRepo := NewModelRepository(db, modelCatalog)
	if _, _, err := modelRepo.Create(ctx, model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Model", Status: model.StatusActive,
		Configuration: model.Configuration{Provider: "public", Model: "chat"}, Metadata: modelMetadata,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Create error = %v", err)
	}
	if _, err := modelRepo.Get(ctx, "tenant", "profile"); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Get error = %v", err)
	}
	if _, _, err := modelRepo.UpdateConfiguration(ctx, model.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Update error = %v", err)
	}
	if _, _, err := modelRepo.TransitionStatus(ctx, model.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Transition error = %v", err)
	}

	backendRepo := NewBackendRepository(db, backendCatalog)
	if _, _, err := backendRepo.Create(ctx, backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Backend", Status: backend.StatusActive,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: backendMetadata,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Create error = %v", err)
	}
	if _, err := backendRepo.Get(ctx, "tenant", "profile"); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Get error = %v", err)
	}
	if _, _, err := backendRepo.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Update error = %v", err)
	}
	if _, _, err := backendRepo.TransitionStatus(ctx, backend.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Transition error = %v", err)
	}

	agentRepo := NewAgentRepository(db)
	if _, err := agentRepo.Create(ctx, agent.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", AppKey: "primary", DisplayName: "Agent",
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent Create error = %v", err)
	}
	if _, err := agentRepo.Get(ctx, "tenant", "app"); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent Get error = %v", err)
	}
	if _, err := agentRepo.UpdateMetadata(ctx, agent.UpdateMetadataInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent metadata error = %v", err)
	}
	if _, err := agentRepo.CreateDraft(ctx, agent.CreateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent draft error = %v", err)
	}
	if _, err := agentRepo.UpdateDraft(ctx, agent.UpdateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent draft update error = %v", err)
	}
	if _, err := agentRepo.GetRevision(ctx, "tenant", "app", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent revision error = %v", err)
	}
	if _, _, _, err := agentRepo.Publish(ctx, agent.PublishInput{
		TenantID: "tenant", AppID: "app", Revision: 1, ExpectedAppVersion: 1,
		ExpectedDraftVersion: 1, TenantActive: true, Metadata: changeMetadata,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent publish error = %v", err)
	}
	if _, _, err := agentRepo.Rollback(ctx, agent.RollbackInput{
		TenantID: "tenant", AppID: "app", TargetRevision: 1, ExpectedAppVersion: 1, Metadata: changeMetadata,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent rollback error = %v", err)
	}
	if _, _, err := agentRepo.TransitionStatus(ctx, agent.TransitionStatusInput{
		TenantID: "tenant", AppID: "app", ExpectedVersion: 1, NextStatus: agent.StatusSuspended, Metadata: changeMetadata,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent transition error = %v", err)
	}

	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "error-path")
	if err != nil {
		t.Fatal(err)
	}
	channelRepo := NewChannelRepository(db)
	if _, _, err := channelRepo.Create(ctx, channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", BindingKey: "primary", Channel: channels.ChannelTelegram,
		ProviderAccountID: "account", PublicRouteKeyDigest: routeDigest,
		AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAW", SecretRef: "secret://binding",
		Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Status: channels.StatusDraft, Metadata: channelMetadata,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Create error = %v", err)
	}
	if _, err := channelRepo.Get(ctx, "tenant", "binding"); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Get error = %v", err)
	}
	if _, _, err := channelRepo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Update error = %v", err)
	}
	if _, _, err := channelRepo.TransitionStatus(ctx, channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Transition error = %v", err)
	}
	for name, operation := range map[string]func(context.Context, channels.TransitionStatusInput) error{
		"activate": func(ctx context.Context, input channels.TransitionStatusInput) error {
			_, _, err := channelRepo.Activate(ctx, input)
			return err
		},
		"suspend": func(ctx context.Context, input channels.TransitionStatusInput) error {
			_, _, err := channelRepo.Suspend(ctx, input)
			return err
		},
		"resume": func(ctx context.Context, input channels.TransitionStatusInput) error {
			_, _, err := channelRepo.Resume(ctx, input)
			return err
		},
		"disable": func(ctx context.Context, input channels.TransitionStatusInput) error {
			_, _, err := channelRepo.Disable(ctx, input)
			return err
		},
	} {
		if err := operation(ctx, channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
			t.Fatalf("channel %s error = %v", name, err)
		}
	}
	if _, err := channelRepo.LookupCandidates(ctx, channels.ChannelTelegram, routeDigest); !errors.Is(err, channels.ErrCandidateUnavailable) && !errors.Is(err, ErrStorage) {
		t.Fatalf("channel lookup error = %v", err)
	}
	if _, err := channelRepo.ConsumeCandidate(ctx, validCandidate(routeDigest)); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("channel consume error = %v", err)
	}
}

func validCandidate(routeDigest string) channels.CandidateBindingContext {
	now := time.Now().UTC()
	return channels.CandidateBindingContext{
		Channel: channels.ChannelTelegram, PublicRouteKeyDigest: routeDigest, BindingVersion: 1,
		ConfigDigest: strings.Repeat("a", 64), Purpose: channels.PurposeWebhookVerification,
		CandidateToken: "candidate-token", IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
}

package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

func TestPrincipalKindsCannotBeForgedAcrossAuthenticationPaths(t *testing.T) {
	tenantID := "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	appID := "app_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	apiPrincipal, err := NewAPIPrincipal(tenantID, appID, "api-subject")
	if err != nil {
		t.Fatal(err)
	}
	if err := apiPrincipal.Validate(); err != nil || apiPrincipal.Kind() != PrincipalAPI || apiPrincipal.TenantID() != tenantID || apiPrincipal.AppID() != appID || apiPrincipal.SubjectID() != "api-subject" {
		t.Fatalf("API principal = %+v, err=%v", apiPrincipal, err)
	}
	if _, ok := apiPrincipal.RoutingTarget(); ok {
		t.Fatal("API principal exposed a Channel routing target")
	}

	target := channels.RoutingTarget{
		TenantID: tenantID, BindingID: "cb_01J1K9ZQTVE4PAWF1TSB2WMHNP", BindingVersion: 1,
		AppID: appID, Channel: channels.ChannelWeCom, ProviderAccountID: "corp-a", ConfigDigest: strings.Repeat("a", 64),
	}
	channelPrincipal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := channelPrincipal.Validate(); err != nil || channelPrincipal.Kind() != PrincipalChannel || channelPrincipal.SubjectID() != "" {
		t.Fatalf("Channel principal = %+v, err=%v", channelPrincipal, err)
	}
	if got, ok := channelPrincipal.RoutingTarget(); !ok || got != target {
		t.Fatalf("Channel target = %+v, ok=%v", got, ok)
	}

	bad := target
	bad.TenantID = "not-a-tenant"
	if _, err := NewChannelPrincipal(bad); err == nil {
		t.Fatal("invalid channel route unexpectedly succeeded")
	}
	if _, err := NewAPIPrincipal(tenantID, "tenant-not-app", "subject"); err == nil {
		t.Fatal("invalid API app ID unexpectedly succeeded")
	}
}

func TestInboundMessageNormalizesTextAndRejectsUnsupportedInput(t *testing.T) {
	message, err := (InboundMessage{
		Content: "  hello  ", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer",
	}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != "hello" || message.ContentType != ContentTypeText {
		t.Fatalf("normalized message = %+v", message)
	}
	for name, input := range map[string]InboundMessage{
		"unsupported content":  {Content: "hello", ContentType: "image", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		"missing user":         {Content: "hello", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		"missing conversation": {Content: "hello", ExternalUserID: "user"},
		"control identity":     {Content: "hello", ExternalUserID: "user\n", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := input.Normalize(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Normalize error = %v", err)
			}
		})
	}
}

func TestPlanResolverBuildsFixedPlanFromRepositoryInterfaces(t *testing.T) {
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resolver.Ready() {
		t.Fatal("resolver is not ready with complete dependencies")
	}
	principal, err := NewAPIPrincipal(fixture.tenant.TenantID, fixture.app.AppID, "api-subject")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != fixture.tenant.TenantID || key.AppID != fixture.app.AppID || key.Revision != fixture.revision.Revision || key.ModelProfileID != fixture.model.ProfileID || key.BackendProfileID != fixture.backend.ProfileID {
		t.Fatalf("unexpected plan key: %+v", key)
	}

	routingTarget := channels.RoutingTarget{
		TenantID: fixture.tenant.TenantID, BindingID: "cb_01J1K9ZQTVE4PAWF1TSB2WMHNP", BindingVersion: 1,
		AppID: fixture.app.AppID, Channel: channels.ChannelTelegram, ProviderAccountID: "bot-a", ConfigDigest: strings.Repeat("b", 64),
	}
	channelPrincipal, err := NewChannelPrincipal(routingTarget)
	if err != nil {
		t.Fatal(err)
	}
	channelPlan, err := resolver.Resolve(context.Background(), channelPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	channelKey, err := channelPlan.CacheKey()
	if err != nil || channelKey != key {
		t.Fatalf("channel plan key = %+v, err=%v, API key=%+v", channelKey, err, key)
	}

	otherTenant := principal
	otherTenant, err = NewAPIPrincipal("t_01J1K9ZQTVE4PAWF1TSB2WMHNQ", fixture.app.AppID, "api-subject")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), otherTenant); !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("cross-tenant plan error = %v", err)
	}
}

func TestPlanResolverPreservesCancellationAndRedactsDependencyFailures(t *testing.T) {
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := NewAPIPrincipal(fixture.tenant.TenantID, fixture.app.AppID, "api-subject")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, principal); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resolver error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), Principal{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("invalid principal error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), mustAPIPrincipal(t, fixture.tenant.TenantID, "app_01J1K9ZQTVE4PAWF1TSB2WMHNQ")); !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("missing app error = %v", err)
	}
	if strings.Contains(errString(ErrPlanUnavailable), "secret") || strings.Contains(errString(ErrPlanUnavailable), "provider") {
		t.Fatal("stable plan error contains sensitive detail")
	}
}

func mustAPIPrincipal(t *testing.T, tenantID, appID string) Principal {
	t.Helper()
	principal, err := NewAPIPrincipal(tenantID, appID, "api-subject")
	if err != nil {
		t.Fatal(err)
	}
	return principal
}

func errString(err error) string { return err.Error() }

type gatewayFixture struct {
	tenant         *tenant.Tenant
	app            *agent.App
	revision       *agent.Revision
	model          *model.Profile
	backend        *backend.Profile
	modelCatalog   *model.ProviderCatalog
	backendCatalog *backend.ProviderCatalog
	tenants        *tenantinmemory.InMemoryRepository
	apps           *agentinmemory.InMemoryRepository
	models         *modelinmemory.InMemoryRepository
	backends       *backendinmemory.InMemoryRepository
}

func newGatewayFixture(t *testing.T) gatewayFixture {
	t.Helper()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: model.FieldForbidden, SecretRefPolicy: model.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantinmemory.NewRepository()
	root, err := tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "gateway-tenant", DisplayName: "Gateway Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	models := modelinmemory.NewRepository(modelCatalog)
	modelProfile, _, err := models.Create(context.Background(), model.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary-model", DisplayName: "Primary Model",
		Configuration: model.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "fixture", Reason: "fixture", CorrelationID: "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	backends := backendinmemory.NewRepository(backendCatalog)
	backendProfile, _, err := backends.Create(context.Background(), backend.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary-backend", DisplayName: "Primary Backend",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "gateway"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "fixture", Reason: "fixture", CorrelationID: "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	apps := agentinmemory.NewRepository()
	app, err := apps.Create(context.Background(), agent.CreateInput{TenantID: root.TenantID, AppKey: "gateway-app", DisplayName: "Gateway App", Description: "Fixture"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(context.Background(), agent.CreateDraftInput{
		TenantID: root.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Configuration: agent.DraftConfiguration{Description: "Gateway revision", Instruction: "Answer clearly.", ModelProfileID: modelProfile.ProfileID, Runtime: agent.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	publishedApp, published, _, err := apps.Publish(context.Background(), agent.PublishInput{
		TenantID: root.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version,
		ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "fixture", Reason: "fixture", CorrelationID: "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	appID, backendID := publishedApp.AppID, backendProfile.ProfileID
	updatedRoot, err := tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName,
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
		DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return gatewayFixture{
		tenant: updatedRoot, app: publishedApp, revision: published, model: modelProfile, backend: backendProfile,
		modelCatalog: modelCatalog, backendCatalog: backendCatalog, tenants: tenants, apps: apps, models: models, backends: backends,
	}
}

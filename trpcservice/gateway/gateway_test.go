package gateway

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelsinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

func TestPrincipalKindsCannotBeForgedAcrossAuthenticationPaths(t *testing.T) {
	tenantID := "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	appID := "app_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	authenticated, err := newAuthenticatedAPI(APIIdentity{TenantID: tenantID, AppID: appID, SubjectID: "api-subject"})
	if err != nil {
		t.Fatal(err)
	}
	apiPrincipal, err := newAPIPrincipal(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	if err := apiPrincipal.Validate(); err != nil || apiPrincipal.Kind() != PrincipalAPI || apiPrincipal.TenantID() != tenantID || apiPrincipal.AppID() != appID || apiPrincipal.SubjectID() != "api-subject" {
		t.Fatalf("API principal = %+v, err=%v", apiPrincipal, err)
	}
	if _, ok := apiPrincipal.RoutingTarget(); ok {
		t.Fatal("API principal exposed a Channel routing target")
	}

	fabricated := channels.RoutingTarget{
		TenantID: tenantID, BindingID: "cb_01J1K9ZQTVE4PAWF1TSB2WMHNP", BindingVersion: 1,
		AppID: appID, Channel: channels.ChannelWeCom, ProviderAccountID: "corp-a", ConfigDigest: strings.Repeat("a", 64),
	}
	if _, err := NewChannelPrincipal(fabricated); err == nil {
		t.Fatal("shape-valid fabricated channel route unexpectedly succeeded")
	}

	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
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
	if _, err := newAuthenticatedAPI(APIIdentity{TenantID: tenantID, AppID: "tenant-not-app", SubjectID: "subject"}); err == nil {
		t.Fatal("invalid API app ID unexpectedly succeeded")
	}
}

func TestAPIAuthenticatorIsTheOnlyPublicPrincipalIssuer(t *testing.T) {
	identity := APIIdentity{TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "subject"}
	authenticator, err := NewStaticAPIAuthenticator(map[string]APIIdentity{"credential": identity})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/chat", nil)
	request.Header.Set("Authorization", "Bearer credential")
	result, err := authenticator.Authenticate(request.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := newAPIPrincipal(result)
	if err != nil || principal.Kind() != PrincipalAPI || principal.TenantID() != identity.TenantID || principal.AppID() != identity.AppID {
		t.Fatalf("authenticated principal = %+v, err=%v", principal, err)
	}
	if _, err := newAPIPrincipal(AuthenticatedAPI{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("zero authenticator result was accepted: %v", err)
	}
	mutated := result
	mutated.identity.TenantID = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
	if _, err := newAPIPrincipal(mutated); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("mutated authenticator result was accepted: %v", err)
	}
	request.Header.Set("Authorization", "Bearer unknown")
	if _, err := authenticator.Authenticate(request.Context(), request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unknown credential was accepted: %v", err)
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
		"blank identity":       {Content: "hello", ExternalUserID: " ", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
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
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
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

	routingTarget := newTrustedRoutingTarget(t, fixture)
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

	otherTenant := mustAPIPrincipal(t, "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ", fixture.app.AppID)
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
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
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
	authenticated, err := newAuthenticatedAPI(APIIdentity{TenantID: tenantID, AppID: appID, SubjectID: "api-subject"})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := newAPIPrincipal(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	return principal
}

func errString(err error) string { return err.Error() }

func newTrustedRoutingTarget(t *testing.T, fixture gatewayFixture) channels.RoutingTarget {
	t.Helper()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "gateway-test-route")
	if err != nil {
		t.Fatal(err)
	}
	channelRepo := channelsinmemory.NewInMemoryRepository()
	binding, _, err := channelRepo.Create(context.Background(), channels.CreateInput{
		TenantID: fixture.tenant.TenantID, BindingKey: "gateway-test-binding", Channel: channels.ChannelWeCom,
		ProviderAccountID: "corp-gateway", PublicRouteKeyDigest: routeDigest, AppID: fixture.app.AppID,
		SecretRef: "secret/gateway-test", Protocol: channels.ProtocolConfiguration{
			WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp-gateway", ReceiveID: "receive"},
		}, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "gateway", Reason: "fixture", CorrelationID: "gateway"},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err = channelRepo.Activate(context.Background(), channels.TransitionStatusInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "gateway", Reason: "fixture", CorrelationID: "gateway"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secret := "offline-gateway-secret"
	resolver := channelsinmemory.NewFakeCandidateResolver(channelRepo, map[channels.SecretScope]string{{TenantID: binding.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := channelRepo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("gateway route discovery failed: candidates=%d err=%v", len(candidates), err)
	}
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verification := channels.VerificationRequest{
		Purpose: channels.PurposeWebhookVerification, Timestamp: time.Now().UTC(), Nonce: "gateway-nonce",
		MessageDigest: strings.Repeat("a", 64), ReceiveID: "receive",
	}
	verification.Signature = channelsinmemory.SignFakeRequest(secret, verification)
	verified, err := resolver.Verify(context.Background(), handle, verification)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(fixture.tenant)
	if err != nil {
		t.Fatal(err)
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, fixture.app, verified)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

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

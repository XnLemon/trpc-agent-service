package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestModelExecutionSnapshotFreezesInputAndKeepsSecretOutOfState(t *testing.T) {
	root, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "secret://tenant/model")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	key, err := snapshot.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != root.TenantID || key.TenantVersion != root.Version || key.ProfileID != profile.ProfileID || key.ProfileVersion != profile.Version || key.ContentDigest != profile.ContentDigest {
		t.Fatalf("unexpected model cache key: %+v", key)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if input.TenantID != root.TenantID || input.ProfileID != profile.ProfileID || input.Provider != "public" || input.Model != "chat" || input.SecretRef != profile.Configuration.SecretRef {
		t.Fatalf("unexpected model factory input: %+v", input)
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "super-secret") {
		t.Fatal("secret value entered serialized factory input")
	}

	profile.Configuration.SecretRef = "secret://other/value"
	if snapshot.Profile().Configuration.SecretRef != "secret://tenant/model" {
		t.Fatal("snapshot retained mutable source Profile")
	}
	input.SecretRef = "caller-mutation"
	again, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if again.SecretRef != "secret://tenant/model" {
		t.Fatal("Factory input mutation changed snapshot state")
	}

	ctx := WithModelExecutionSnapshot(context.Background(), snapshot)
	fromContext, ok := ModelExecutionSnapshotFromContext(ctx)
	if !ok || fromContext.Profile().ProfileID != profile.ProfileID {
		t.Fatal("valid model snapshot was not carried by context")
	}
	ctx = WithModelExecutionSnapshot(ctx, ModelExecutionSnapshot{})
	if _, ok := ModelExecutionSnapshotFromContext(ctx); ok {
		t.Fatal("zero model snapshot entered context")
	}
}

func TestResolveAndBuildUsesExplicitConditionalTenantSecretScope(t *testing.T) {
	root, tenantSnapshot, optionalProfile, catalog := modelExecutionFixture(t, "")
	optionalSnapshot, err := NewModelExecutionSnapshot(tenantSnapshot, optionalProfile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	optionalInput, err := optionalSnapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingResolver{}
	factory := &recordingFactory{}
	if _, err := ResolveAndBuild(context.Background(), optionalInput, nil, factory); err != nil {
		t.Fatalf("optional no-secret build error = %v", err)
	}
	if factory.calls != 1 || factory.secret.Value() != "" {
		t.Fatalf("optional no-secret factory calls=%d secret=%q", factory.calls, factory.secret.Value())
	}
	if resolver.calls != 0 {
		t.Fatalf("optional no-secret resolver calls = %d, want zero", resolver.calls)
	}

	secretProfile, err := NewProfile(CreateInput{
		TenantID: root.TenantID, ProfileKey: "optional-secret", DisplayName: "Optional Secret",
		Configuration: Configuration{Provider: "public", Model: "chat", SecretRef: "secret://tenant/model"},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	secretSnapshot, err := NewModelExecutionSnapshot(tenantSnapshot, secretProfile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	secretInput, err := secretSnapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	secret, err := NewSecretValue("super-secret")
	if err != nil {
		t.Fatal(err)
	}
	resolver.value = secret
	factory = &recordingFactory{}
	if _, err := ResolveAndBuild(context.Background(), secretInput, resolver, factory); err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 || resolver.scope != (SecretScope{TenantID: root.TenantID, SecretRef: "secret://tenant/model"}) {
		t.Fatalf("unexpected resolver scope/calls: %+v calls=%d", resolver.scope, resolver.calls)
	}
	if factory.secret.Value() != "super-secret" || factory.input.SecretRef != "secret://tenant/model" {
		t.Fatalf("secret was not passed only to factory: value=%q input=%+v", factory.secret.Value(), factory.input)
	}
	if fmt.Sprint(factory.secret) != "<redacted-secret>" {
		t.Fatalf("secret String() leaked value: %q", fmt.Sprint(factory.secret))
	}
}

func TestResolveAndBuildRedactsResolverAndFactoryErrors(t *testing.T) {
	_, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "secret://tenant/model")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingResolver{err: errors.New("KMS returned super-secret")}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, &recordingFactory{}); !errors.Is(err, ErrSecretResolution) || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("resolver error was not redacted/classified: %v", err)
	}
	resolver.err = nil
	factory := &recordingFactory{err: errors.New("provider rejected super-secret")}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, factory); !errors.Is(err, ErrModelFactory) || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("factory error was not redacted/classified: %v", err)
	}
}

func TestSecretScopeRejectsMissingOrInvalidTenant(t *testing.T) {
	if err := (SecretScope{SecretRef: "secret://model"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing tenant error = %v", err)
	}
	if err := (SecretScope{TenantID: "other-tenant", SecretRef: "secret://model"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant error = %v", err)
	}
}

type recordingResolver struct {
	calls int
	scope SecretScope
	value SecretValue
	err   error
}

func (resolver *recordingResolver) Resolve(_ context.Context, scope SecretScope) (SecretValue, error) {
	resolver.calls++
	resolver.scope = scope
	if resolver.err != nil {
		return SecretValue{}, resolver.err
	}
	return resolver.value, nil
}

type recordingFactory struct {
	calls  int
	input  ModelFactoryInput
	secret SecretValue
	err    error
}

func (factory *recordingFactory) New(_ context.Context, input ModelFactoryInput, secret SecretValue) (trpcmodel.Model, error) {
	factory.calls++
	factory.input = input
	factory.secret = secret
	if factory.err != nil {
		return nil, factory.err
	}
	return fakeModel{}, nil
}

type fakeModel struct{}

func (fakeModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "chat"} }

func (fakeModel) GenerateContent(ctx context.Context, _ *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	responses := make(chan *trpcmodel.Response, 1)
	select {
	case responses <- &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.NewAssistantMessage("ok")}}, Done: true}:
	case <-ctx.Done():
	}
	close(responses)
	return responses, nil
}

func modelExecutionFixture(t *testing.T, secretRef string) (*tenant.Tenant, tenant.ConfigurationSnapshot, *Profile, *ProviderCatalog) {
	t.Helper()
	catalog := modelTestCatalog(t)
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "model-runtime", DisplayName: "Model Runtime",
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewProfile(CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary", DisplayName: "Primary",
		Configuration: Configuration{Provider: "public", Model: "chat", SecretRef: secretRef},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	return root, tenantSnapshot, profile, catalog
}

package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/resilience"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestModelExecutionSnapshotFreezesInputAndKeepsSecretOutOfState(t *testing.T) {
	root, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "secret://tenant/model")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := assertModelSnapshotCacheAndInput(t, snapshot, root, profile)
	assertModelSnapshotDefensiveCopies(t, snapshot, profile, input)
	assertModelSnapshotContextBoundary(t, snapshot, profile)
}

func assertModelSnapshotCacheAndInput(t *testing.T, snapshot ModelExecutionSnapshot, root *tenant.Tenant, profile *Profile) ModelFactoryInput {
	t.Helper()
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
	return input
}

func assertModelSnapshotDefensiveCopies(t *testing.T, snapshot ModelExecutionSnapshot, profile *Profile, input ModelFactoryInput) {
	t.Helper()
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
}

func assertModelSnapshotContextBoundary(t *testing.T, snapshot ModelExecutionSnapshot, profile *Profile) {
	t.Helper()
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
	resolver.err = fmt.Errorf("provider rejected super-secret: %w", ErrInvalid)
	if _, err := ResolveAndBuild(context.Background(), input, resolver, &recordingFactory{}); !errors.Is(err, ErrSecretResolution) || strings.Contains(err.Error(), "super-secret") || errors.Is(err, ErrInvalid) {
		t.Fatalf("wrapped resolver error was not redacted/classified: %v", err)
	}
	resolver.err = nil
	resolver.value, err = NewSecretValue("super-secret")
	if err != nil {
		t.Fatal(err)
	}
	factory := &recordingFactory{err: errors.New("provider rejected super-secret")}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, factory); !errors.Is(err, ErrModelFactory) || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("factory error was not redacted/classified: %v", err)
	}
}

func TestResolveAndBuildWithPolicyRetriesModelConstruction(t *testing.T) {
	_, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := resilience.New(resilience.Config{
		Timeout: time.Second, MaxAttempts: 3, FailureThreshold: 3, OpenTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	factory := &retryingFactory{}
	if _, err := ResolveAndBuildWithPolicy(context.Background(), input, nil, factory, policy); err != nil {
		t.Fatalf("ResolveAndBuildWithPolicy() = %v", err)
	}
	if factory.calls != 3 {
		t.Fatalf("factory calls = %d, want three bounded attempts", factory.calls)
	}
	if policy.State() != resilience.StateClosed {
		t.Fatalf("policy state = %q, want closed", policy.State())
	}
}

func TestResolveAndBuildWithPolicyRejectsSuccessfulSecretFallback(t *testing.T) {
	_, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "secret://tenant/model")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name          string
		openCircuit   bool
		resolverCalls int
	}{
		{name: "exhausted attempts", resolverCalls: 1},
		{name: "open circuit", openCircuit: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy, err := resilience.New(resilience.Config{
				Timeout: time.Second, MaxAttempts: 1, FailureThreshold: 1, OpenTimeout: time.Minute,
				Fallback: func(context.Context, error) error { return nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.openCircuit {
				if err := policy.Execute(context.Background(), func(context.Context) error { return errors.New("dependency unavailable") }); err != nil {
					t.Fatalf("open circuit setup = %v", err)
				}
			}
			resolver := &recordingResolver{err: errors.New("secret unavailable")}
			factory := &recordingFactory{}
			model, err := ResolveAndBuildWithPolicy(context.Background(), input, resolver, factory, policy)
			if model != nil || !errors.Is(err, ErrSecretResolution) {
				t.Fatalf("ResolveAndBuildWithPolicy() model=%v err=%v, want secret resolution error", model, err)
			}
			if resolver.calls != test.resolverCalls || factory.calls != 0 {
				t.Fatalf("resolver calls=%d factory calls=%d, want %d and 0", resolver.calls, factory.calls, test.resolverCalls)
			}
		})
	}
}

func TestResolveAndBuildWithPolicyRejectsZeroPolicy(t *testing.T) {
	_, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	var policy resilience.Policy
	if _, err := ResolveAndBuildWithPolicy(context.Background(), input, nil, &recordingFactory{}, &policy); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ResolveAndBuildWithPolicy() = %v, want ErrInvalid", err)
	}
}

func TestSecretScopeRejectsMissingOrInvalidTenant(t *testing.T) {
	if err := (SecretScope{SecretRef: "secret://model"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing tenant error = %v", err)
	}
	if err := (SecretScope{TenantID: "other-tenant", SecretRef: "secret://model"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant error = %v", err)
	}
	if err := (SecretScope{TenantID: modelTestTenantOne}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing secret reference error = %v", err)
	}
}

func TestModelExecutionSnapshotRejectsInvalidStatesAndInputs(t *testing.T) {
	root, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	assertInvalidModelSnapshotAccessors(t)
	assertInvalidModelExecutionStates(t, root, profile, catalog)
	assertResolveAndBuildBoundaries(t, snapshot, root, tenantSnapshot, catalog)
	assertInvalidModelFactoryInputs(t, root)
}

func assertInvalidModelSnapshotAccessors(t *testing.T) {
	t.Helper()
	if zero := (ModelExecutionSnapshot{}).Tenant(); zero.TenantID != "" {
		t.Fatalf("zero snapshot tenant = %+v", zero)
	}
	if zero := (ModelExecutionSnapshot{}).Profile(); zero.ProfileID != "" {
		t.Fatalf("zero snapshot profile = %+v", zero)
	}
	if _, err := (ModelExecutionSnapshot{}).CacheKey(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot cache key error = %v", err)
	}
	if _, err := (ModelExecutionSnapshot{}).FactoryInput(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot factory input error = %v", err)
	}
	if _, ok := ModelExecutionSnapshotFromContext(context.Background()); ok {
		t.Fatal("empty context returned a model snapshot")
	}
	if contextWithZero := WithModelExecutionSnapshot(context.Background(), ModelExecutionSnapshot{}); func() bool {
		_, ok := ModelExecutionSnapshotFromContext(contextWithZero)
		return ok
	}() {
		t.Fatal("zero model snapshot entered context")
	}
}

func assertInvalidModelExecutionStates(t *testing.T, root *tenant.Tenant, profile *Profile, catalog *ProviderCatalog) {
	t.Helper()
	if _, err := NewModelExecutionSnapshot(tenant.ConfigurationSnapshot{}, profile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant snapshot error = %v", err)
	}

	invalidTenant := root.Clone()
	invalidTenant.TenantID = "tenant"
	if err := validateExecutionState(invalidTenant, profile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant state error = %v", err)
	}
	inactiveTenant := root.Clone()
	inactiveTenant.Status = tenant.StatusSuspended
	if err := validateExecutionState(inactiveTenant, profile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inactive tenant state error = %v", err)
	}
	if err := validateExecutionState(*root, nil, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil profile state error = %v", err)
	}
	if err := validateExecutionState(*root, profile, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil catalog state error = %v", err)
	}
	invalidProfile := profile.Clone()
	invalidProfile.ContentDigest = "wrong"
	if err := validateExecutionState(*root, &invalidProfile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid profile state error = %v", err)
	}
	differentTenantProfile := profile.Clone()
	differentTenantProfile.TenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := validateExecutionState(*root, &differentTenantProfile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cross-tenant profile state error = %v", err)
	}
	inactiveProfile := profile.Clone()
	inactiveProfile.Status = StatusSuspended
	if err := validateExecutionState(*root, &inactiveProfile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inactive profile state error = %v", err)
	}
}

func assertResolveAndBuildBoundaries(t *testing.T, snapshot ModelExecutionSnapshot, root *tenant.Tenant, tenantSnapshot tenant.ConfigurationSnapshot, catalog *ProviderCatalog) {
	t.Helper()
	if _, err := NewSecretValue(""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty secret error = %v", err)
	}
	if got := (SecretValue{}).String(); got != "<empty-secret>" {
		t.Fatalf("empty secret String() = %q", got)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if _, err := ResolveAndBuild(nilContext, input, nil, &recordingFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := ResolveAndBuild(context.Background(), input, nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil factory error = %v", err)
	}
	if _, err := ResolveAndBuild(context.Background(), ModelFactoryInput{}, nil, &recordingFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("incomplete factory input error = %v", err)
	}
	secretProfile, err := NewProfile(CreateInput{
		TenantID: root.TenantID, ProfileKey: "secret-input", DisplayName: "Secret Input",
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
	if _, err := ResolveAndBuild(context.Background(), secretInput, nil, &recordingFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing resolver error = %v", err)
	}
	resolver := &recordingResolver{err: errors.New("resolver secret")}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ResolveAndBuild(cancelled, secretInput, resolver, &recordingFactory{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled resolver error = %v", err)
	}
	factory := &recordingFactory{returnNil: true}
	if _, err := ResolveAndBuild(context.Background(), input, nil, factory); !errors.Is(err, ErrModelFactory) {
		t.Fatalf("nil model error = %v", err)
	}
	cancelled, cancel = context.WithCancel(context.Background())
	cancel()
	if _, err := ResolveAndBuild(cancelled, input, nil, &recordingFactory{err: errors.New("factory failure")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled factory error = %v", err)
	}
}

func assertInvalidModelFactoryInputs(t *testing.T, root *tenant.Tenant) {
	t.Helper()
	incompleteInputs := []ModelFactoryInput{
		{TenantID: root.TenantID},
		{TenantID: root.TenantID, ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", TenantVersion: 1, ProfileVersion: 1, SchemaVersion: 99, ContentDigest: "digest", Provider: "fake", Model: "deterministic"},
		{TenantID: root.TenantID, ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", TenantVersion: 1, ProfileVersion: 1, SchemaVersion: SchemaVersionV1, ContentDigest: "digest", Provider: "fake", Model: "deterministic", SecretRef: "bad ref"},
	}
	for _, incomplete := range incompleteInputs {
		if err := validateFactoryInput(incomplete); !errors.Is(err, ErrInvalid) {
			t.Errorf("factory input %+v error = %v", incomplete, err)
		}
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
	calls     int
	input     ModelFactoryInput
	secret    SecretValue
	err       error
	returnNil bool
}

type retryingFactory struct{ calls int }

func (factory *retryingFactory) New(_ context.Context, _ ModelFactoryInput, _ SecretValue) (trpcmodel.Model, error) {
	factory.calls++
	if factory.calls < 3 {
		return nil, errors.New("temporary provider failure")
	}
	return fakeModel{}, nil
}

func (factory *recordingFactory) New(_ context.Context, input ModelFactoryInput, secret SecretValue) (trpcmodel.Model, error) {
	factory.calls++
	factory.input = input
	factory.secret = secret
	if factory.err != nil {
		return nil, factory.err
	}
	if factory.returnNil {
		return nil, nil
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

package bootstrap

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestDefaultDemoConfigAndValidation(t *testing.T) {
	config := DefaultDemoConfig()
	if config.TenantKey != demoTenantKey || config.AppKey != demoAppKey || config.ModelProfileKey != demoModelProfileKey || config.BackendProfileKey != demoBackendProfileKey {
		t.Fatalf("default demo config = %+v", config)
	}
	if err := config.validate(); err != nil {
		t.Fatalf("default demo config is invalid: %v", err)
	}
	for _, mutate := range []func(*DemoConfig){
		func(value *DemoConfig) { value.TenantKey = "bad key" },
		func(value *DemoConfig) { value.AppKey = "bad key" },
		func(value *DemoConfig) { value.ModelProfileKey = "bad key" },
		func(value *DemoConfig) { value.BackendProfileKey = "bad key" },
	} {
		invalid := config
		mutate(&invalid)
		if err := invalid.validate(); !errors.Is(err, ErrInvalidConfig) && !errors.Is(err, ErrDemoState) {
			t.Fatalf("invalid demo config error = %v", err)
		}
	}
	trimmed := normalizeDemoConfig(DemoConfig{TenantKey: "  Tenant-Demo  ", AppKey: "  Assistant-Demo  ", ModelProfileKey: " DemoModel ", BackendProfileKey: " LocalStore "})
	if trimmed.TenantKey != "tenant-demo" || trimmed.AppKey != "assistant-demo" || trimmed.ModelProfileKey != "demomodel" || trimmed.BackendProfileKey != "localstore" || trimmed.AppDisplayName == "" {
		t.Fatalf("normalized demo config = %+v", trimmed)
	}
}

func TestWriteDemoResultOmitsSecrets(t *testing.T) {
	result := DemoResult{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackendProfileID: "bp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Revision: 1, Created: true,
	}
	var output strings.Builder
	if err := WriteDemoResult(&output, result); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, expected := range []string{"TRPC_DEMO_MODE='true'", "TRPC_MODEL_PROVIDER='fake'", "TRPC_TENANT_ID='t_01ARZ3NDEKTSV4RRFFQ69G5FAV'", "TRPC_APP_ID='app_01ARZ3NDEKTSV4RRFFQ69G5FAV'"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("demo output missing %q: %s", expected, text)
		}
	}
	for _, secret := range []string{"postgres://", "api-token", "model-secret", "secretref", "TRPC_POSTGRES_DSN", "TRPC_API_TOKEN", "TRPC_MODEL_API_KEY"} {
		if strings.Contains(strings.ToLower(text), strings.ToLower(secret)) {
			t.Fatalf("demo output leaked %q: %s", secret, text)
		}
	}
	if err := WriteDemoResult(io.Discard, DemoResult{}); !errors.Is(err, ErrDemoInitialization) {
		t.Fatalf("invalid demo result error = %v", err)
	}
}

func TestDeterministicModelContract(t *testing.T) {
	model := deterministicModel{model: demoModelName}
	if model.Info().Name != demoModelName {
		t.Fatalf("model info = %+v", model.Info())
	}
	if _, err := model.GenerateContent(nil, &trpcmodel.Request{}); err == nil {
		t.Fatal("nil context was accepted")
	}
	if _, err := model.GenerateContent(context.Background(), nil); err == nil {
		t.Fatal("nil request was accepted")
	}
	responses, err := model.GenerateContent(context.Background(), &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	response, ok := <-responses
	if !ok || response == nil || response.Error != nil || !response.Done || len(response.Choices) != 1 || response.Choices[0].Message.Content != deterministicDemoResponse {
		t.Fatalf("deterministic response = %#v, open=%v", response, ok)
	}
	if _, open := <-responses; open {
		t.Fatal("deterministic response channel remained open")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	responses, err = model.GenerateContent(canceled, &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if _, open := <-responses; open {
		t.Fatal("canceled deterministic model emitted a response")
	}
}

func TestDemoEnvironmentIsExplicitAndCredentialFree(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envDemoMode, "true")
	t.Setenv(envModelProvider, demoModelProvider)
	t.Setenv(envModelNames, demoModelName)
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !config.demoMode || config.modelProvider != demoModelProvider || config.secretRef != "" || config.modelAPIKey != "" || len(config.modelNames) != 1 || config.modelNames[0] != demoModelName {
		t.Fatalf("demo environment = %+v", config)
	}
	catalog, _, err := environmentCatalogs(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (environmentModelFactory{}).New(context.Background(), modelprofile.ModelFactoryInput{Provider: demoModelProvider, Model: demoModelName}, modelprofile.SecretValue{}); err != nil {
		t.Fatalf("credential-free demo model = %v", err)
	}
	if _, err := catalog.NormalizeConfiguration(modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}); err != nil {
		t.Fatalf("demo model catalog = %v", err)
	}

	t.Setenv(envDemoMode, "false")
	t.Setenv(envModelProvider, defaultModelProvider)
	t.Setenv(envModelAPIKey, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("production environment without model key = %v", err)
	}
	t.Setenv(envDemoMode, "true")
	t.Setenv(envModelProvider, defaultModelProvider)
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo environment with production provider = %v", err)
	}
	t.Setenv(envModelProvider, demoModelProvider)
	t.Setenv(envControlPlaneDriver, string(ControlPlaneDriverMySQL))
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo environment with MySQL control plane = %v", err)
	}
	t.Setenv(envControlPlaneDriver, string(ControlPlaneDriverPostgres))
	t.Setenv(envWeComCallbackToken, "callback")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo environment with WeCom credentials = %v", err)
	}
}

func TestEnsureDemoDefaultsFailsClosed(t *testing.T) {
	repo := tenantmemory.NewRepository()
	root, err := repo.Create(context.Background(), tenant.CreateInput{TenantKey: "demo", DisplayName: "Demo"})
	if err != nil {
		t.Fatal(err)
	}
	updated, changed, err := ensureDemoDefaults(context.Background(), repo, root, "app_demo", "backend_demo")
	if err != nil || !changed || updated.DefaultAgentAppID == nil || updated.DefaultBackendProfileID == nil {
		t.Fatalf("initial defaults update = %+v, changed=%v, err=%v", updated, changed, err)
	}
	stable, changed, err := ensureDemoDefaults(context.Background(), repo, updated, "app_demo", "backend_demo")
	if err != nil || changed || stable.DefaultAgentAppID == nil || *stable.DefaultAgentAppID != "app_demo" {
		t.Fatalf("matching defaults = %+v, changed=%v, err=%v", stable, changed, err)
	}
	wrong := *stable.DefaultAgentAppID
	wrong = "app_other"
	stable.DefaultAgentAppID = &wrong
	if _, _, err := ensureDemoDefaults(context.Background(), repo, stable, "app_demo", "backend_demo"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("incompatible defaults error = %v", err)
	}
	suspended := *root
	suspended.Status = tenant.StatusSuspended
	if _, _, err := ensureDemoDefaults(context.Background(), repo, &suspended, "app_demo", "backend_demo"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("suspended tenant error = %v", err)
	}
	if _, _, err := ensureDemoDefaults(context.Background(), repo, nil, "app_demo", "backend_demo"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("nil tenant error = %v", err)
	}
}

func TestEnsureDemoRevisionRejectsUnrunnableState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	app := &agent.App{TenantID: root.TenantID, AppID: testInitAppID, Status: agent.StatusActive}
	canary := int64(2)
	app.CanaryRevision = &canary
	if _, _, _, err := ensureDemoRevision(context.Background(), db, nil, root, app, "model"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("canary state error = %v", err)
	}
	app.CanaryRevision = nil
	suspended := *root
	suspended.Status = tenant.StatusSuspended
	if _, _, _, err := ensureDemoRevision(context.Background(), db, nil, &suspended, app, "model"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("suspended tenant error = %v", err)
	}
	if _, _, _, err := ensureDemoRevision(context.Background(), db, nil, root, nil, "model"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("nil app error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

package runnerfactory

import (
	"context"
	"errors"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestNewRuntimeRunnerRegistryValidatesDependencies(t *testing.T) {
	if _, err := NewRuntimeRunnerRegistry(Config{}); !errors.Is(err, runtimerunner.ErrInvalid) {
		t.Fatalf("missing dependencies error = %v", err)
	}
	if _, err := NewRuntimeRunnerRegistry(Config{ModelFactory: runnerFactoryModelFactory{}}); !errors.Is(err, runtimerunner.ErrInvalid) {
		t.Fatalf("missing session and storage error = %v", err)
	}
	if _, err := NewRuntimeRunnerRegistry(Config{Sessions: inmemory.NewSessionService()}); !errors.Is(err, runtimerunner.ErrInvalid) {
		t.Fatalf("missing model factory error = %v", err)
	}
}

func TestNewRuntimeRunnerRegistryBuildsAgentRunnerFromPlan(t *testing.T) {
	plan := newRunnerFactoryTestPlan(t)
	sessions := inmemory.NewSessionService()
	t.Cleanup(func() { _ = sessions.Close() })
	registry, err := NewRuntimeRunnerRegistry(Config{ModelFactory: runnerFactoryModelFactory{}, Sessions: sessions})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Runner() == nil {
		t.Fatal("registry returned a nil Agent runner")
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewRuntimeRunnerRegistryBuildsWithStorageFactory(t *testing.T) {
	plan := newRunnerFactoryTestPlan(t)
	storageSessions := inmemory.NewSessionService()
	var received backend.StorageFactoryInput
	storage := storagefactory.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		received = input
		return storagefactory.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: storageSessions})
	})
	registry, err := NewRuntimeRunnerRegistry(Config{ModelFactory: runnerFactoryModelFactory{}, StorageFactory: storage})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if received.TenantID == "" || len(received.Bindings) != 1 || received.Bindings[0].Capability != backend.CapabilitySession {
		t.Fatalf("storage factory input = %+v", received)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRunnerRegistryMaterializesPluginsFromPlan(t *testing.T) {
	plan := newRunnerFactoryTestPlan(t)
	sessions := inmemory.NewSessionService()
	defer sessions.Close()
	created := 0
	registry, err := NewRuntimeRunnerRegistry(Config{
		ModelFactory: runnerFactoryModelFactory{}, Sessions: sessions,
		PluginFactory: func(_ context.Context, received runtime.ExecutionPlan) ([]plugin.Plugin, error) {
			receivedKey, receivedErr := received.CacheKey()
			planKey, planErr := plan.CacheKey()
			if receivedErr != nil || planErr != nil || receivedKey != planKey {
				t.Fatalf("plugin factory execution plan: received=%v/%v want=%v/%v", receivedKey, receivedErr, planKey, planErr)
			}
			created++
			return []plugin.Plugin{runnerFactoryPlugin{}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("plugin factory calls = %d", created)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRunnerRegistryRejectsPluginFactoryFailure(t *testing.T) {
	plan := newRunnerFactoryTestPlan(t)
	sessions := inmemory.NewSessionService()
	defer sessions.Close()
	registry, err := NewRuntimeRunnerRegistry(Config{
		ModelFactory: runnerFactoryModelFactory{}, Sessions: sessions,
		PluginFactory: func(context.Context, runtime.ExecutionPlan) ([]plugin.Plugin, error) {
			return nil, errors.New("plugin configuration failed")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(context.Background(), plan); !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
		t.Fatalf("plugin factory failure = %v", err)
	}
}

type runnerFactoryPlugin struct{}

func (runnerFactoryPlugin) Name() string              { return "runner-factory-plugin" }
func (runnerFactoryPlugin) Register(*plugin.Registry) {}

func TestRunnerInputFromPlanRejectsInvalidPlan(t *testing.T) {
	if _, err := runnerInputFromPlan(runtime.ExecutionPlan{}); err == nil {
		t.Fatal("zero execution plan unexpectedly projected into RunnerInput")
	}
}

type runnerFactoryModelFactory struct{}

func (runnerFactoryModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (trpcmodel.Model, error) {
	return runnerFactoryModel{}, nil
}

type runnerFactoryModel struct{}

func (runnerFactoryModel) GenerateContent(context.Context, *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	responses := make(chan *trpcmodel.Response)
	close(responses)
	return responses, nil
}

func (runnerFactoryModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "deterministic"} }

func newRunnerFactoryTestPlan(t *testing.T) runtime.ExecutionPlan {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantValue, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "runner-factory-tenant", DisplayName: "Runner Factory Tenant", AuditRetentionDays: 30,
		LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{TenantID: tenantValue.TenantID, AppKey: "runner-factory-app", DisplayName: "Runner Factory App"})
	if err != nil {
		t.Fatal(err)
	}
	modelValue, err := modelprofile.NewProfile(modelprofile.CreateInput{
		TenantID: tenantValue.TenantID, ProfileKey: "runner-factory-model", DisplayName: "Runner Factory Model",
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"},
	}, modelCatalog)
	if err != nil {
		t.Fatal(err)
	}
	backendValue, err := backend.NewProfile(backend.CreateInput{
		TenantID: tenantValue.TenantID, ProfileKey: "runner-factory-backend", DisplayName: "Runner Factory Backend",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}},
	}, backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	draft, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: tenantValue.TenantID, AppID: appRoot.AppID, Revision: 1,
		Configuration: appmodel.DraftConfiguration{Description: "runner factory test", Instruction: "Answer clearly.", ModelProfileID: modelValue.ProfileID, Runtime: appmodel.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	published, err := draft.Publish(time.Now().UTC().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	appRoot.Status = appmodel.StatusActive
	appRoot.CurrentRevision = &published.Revision
	appID, backendID := appRoot.AppID, backendValue.ProfileID
	tenantValue.DefaultAgentAppID = &appID
	tenantValue.DefaultBackendProfileID = &backendID
	snapshot, err := tenant.NewConfigurationSnapshot(tenantValue)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runtime.NewExecutionPlanFromInput(runtime.ExecutionPlanInput{
		TenantSnapshot: snapshot, AppRoot: appRoot, Revision: &published,
		ModelProfile: modelValue, ModelCatalog: modelCatalog,
		BackendProfile: backendValue, BackendCatalog: backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

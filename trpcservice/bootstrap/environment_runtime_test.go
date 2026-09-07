package bootstrap

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestorageredis "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestEnvironmentControlPlaneRuntimeResolutionFailsClosed(t *testing.T) {
	const (
		tenantID  = "t_00000000000000000000000000"
		appID     = "app_00000000000000000000000000"
		backendID = "backend_00000000000000000000000000"
		modelID   = "model_00000000000000000000000000"
	)
	revisionNumber := int64(1)
	appIDValue, backendIDValue := appID, backendID
	root := tenant.Tenant{TenantID: tenantID, DefaultAgentAppID: &appIDValue, DefaultBackendProfileID: &backendIDValue}
	activeApp := &appmodel.App{TenantID: tenantID, AppID: appID, Status: appmodel.StatusActive, CurrentRevision: &revisionNumber}
	revision := &appmodel.Revision{TenantID: tenantID, AppID: appID, Revision: revisionNumber, ModelProfileID: modelID}
	modelValue := &modelprofile.Profile{TenantID: tenantID, ProfileID: modelID}
	base := environmentTenantRuntimeDependencies{
		modelCatalog:   &modelprofile.ProviderCatalog{},
		backendCatalog: &backend.ProviderCatalog{},
		secrets:        modelruntime.NewSecretRegistry(),
	}

	tests := []struct {
		name string
		deps environmentTenantRuntimeDependencies
	}{
		{
			name: "tenant defaults missing",
			deps: func() environmentTenantRuntimeDependencies {
				deps := base
				deps.tenants = environmentTenantRepositoryStub{get: func(context.Context, string) (*tenant.Tenant, error) {
					return &tenant.Tenant{TenantID: tenantID}, nil
				}}
				return deps
			}(),
		},
		{
			name: "app scope mismatch",
			deps: func() environmentTenantRuntimeDependencies {
				deps := base
				deps.tenants = environmentTenantRepositoryStub{get: func(context.Context, string) (*tenant.Tenant, error) { return &root, nil }}
				deps.apps = environmentAppRepositoryStub{get: func(context.Context, string, string) (*appmodel.App, error) {
					value := *activeApp
					value.TenantID = "t_00000000000000000000000001"
					return &value, nil
				}}
				return deps
			}(),
		},
		{
			name: "revision unavailable",
			deps: func() environmentTenantRuntimeDependencies {
				deps := base
				deps.tenants = environmentTenantRepositoryStub{get: func(context.Context, string) (*tenant.Tenant, error) { return &root, nil }}
				deps.apps = environmentAppRepositoryStub{
					get: func(context.Context, string, string) (*appmodel.App, error) { return activeApp, nil },
					getRevision: func(context.Context, string, string, int64) (*appmodel.Revision, error) {
						return nil, appmodel.ErrNotFound
					},
				}
				return deps
			}(),
		},
		{
			name: "model unavailable",
			deps: func() environmentTenantRuntimeDependencies {
				deps := base
				deps.tenants = environmentTenantRepositoryStub{get: func(context.Context, string) (*tenant.Tenant, error) { return &root, nil }}
				deps.apps = environmentAppRepositoryStub{
					get:         func(context.Context, string, string) (*appmodel.App, error) { return activeApp, nil },
					getRevision: func(context.Context, string, string, int64) (*appmodel.Revision, error) { return revision, nil },
				}
				deps.models = environmentModelRepositoryStub{get: func(context.Context, string, string) (*modelprofile.Profile, error) {
					return nil, modelprofile.ErrNotFound
				}}
				return deps
			}(),
		},
		{
			name: "backend unavailable",
			deps: func() environmentTenantRuntimeDependencies {
				deps := base
				deps.tenants = environmentTenantRepositoryStub{get: func(context.Context, string) (*tenant.Tenant, error) { return &root, nil }}
				deps.apps = environmentAppRepositoryStub{
					get:         func(context.Context, string, string) (*appmodel.App, error) { return activeApp, nil },
					getRevision: func(context.Context, string, string, int64) (*appmodel.Revision, error) { return revision, nil },
				}
				deps.models = environmentModelRepositoryStub{get: func(context.Context, string, string) (*modelprofile.Profile, error) { return modelValue, nil }}
				deps.backends = environmentBackendRepositoryStub{get: func(context.Context, string, string) (*backend.Profile, error) { return nil, backend.ErrNotFound }}
				return deps
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := test.deps.resolve(context.Background(), tenantID); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("resolve() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
	if _, _, err := base.resolve(nil, tenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, _, err := base.resolve(context.Background(), ""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty tenant error = %v", err)
	}
	if _, _, err := base.resolve(canceledContext(), tenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	if !errors.Is(controlPlaneDependencyError(canceledContext(), errors.New("dependency")), context.Canceled) {
		t.Fatal("dependency cancellation was not preserved")
	}
	if !errors.Is(controlPlaneDependencyError(context.Background(), nil), ErrInvalidConfig) {
		t.Fatal("nil dependency error was not normalized")
	}
}

func TestEnvironmentControlPlaneRuntimeMaterializerBoundaries(t *testing.T) {
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	secretRegistry := modelruntime.NewSecretRegistry()
	modelRegistry := modelruntime.NewModelProviderRegistry()
	backendRegistry := storagefactory.NewProviderRegistry()
	baseOptions := environmentTenantRuntimeOptions{
		config:           environmentConfig{runtimeStorage: "inmemory"},
		delegateSessions: nil,
		runtimeStores:    environmentRuntimeStores{primary: store, providers: map[string]environmentStorage{"inmemory": store}},
		secretRegistry:   secretRegistry,
		modelRegistry:    modelRegistry,
		backendRegistry:  backendRegistry,
	}
	if _, err := newEnvironmentTenantMaterializer(environmentTenantRuntimeOptions{
		config:          baseOptions.config,
		runtimeStores:   baseOptions.runtimeStores,
		modelRegistry:   modelRegistry,
		backendRegistry: backendRegistry,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing secret registry error = %v", err)
	}
	if _, err := newEnvironmentTenantMaterializer(environmentTenantRuntimeOptions{
		config:          baseOptions.config,
		runtimeStores:   baseOptions.runtimeStores,
		secretRegistry:  secretRegistry,
		modelRegistry:   modelRegistry,
		backendRegistry: backendRegistry,
		controlPlane:    &environmentTenantRuntimeDependencies{},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid control-plane dependencies error = %v", err)
	}
	if _, err := environmentTenantRuntimeForStores(environmentTenantRuntimeOptions{
		config:          baseOptions.config,
		runtimeStores:   baseOptions.runtimeStores,
		secretRegistry:  secretRegistry,
		modelRegistry:   modelRegistry,
		backendRegistry: backendRegistry,
	}); err != nil {
		t.Fatalf("static tenant runtime construction = %v", err)
	}

	const tenantID = "t_00000000000000000000000000"
	const secretRef = "env/model"
	if err := ensureEnvironmentSecret(nil, secretRegistry, secretRegistry, tenantID, secretRef, "fallback"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil secret context error = %v", err)
	}
	if err := ensureEnvironmentSecret(context.Background(), secretRegistry, nil, tenantID, secretRef, "fallback"); err != nil {
		t.Fatalf("static secret fallback = %v", err)
	}
	if err := ensureEnvironmentSecret(context.Background(), secretRegistry, nil, tenantID, "env/missing", ""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing secret error = %v", err)
	}
	if err := ensureEnvironmentSecret(canceledContext(), secretRegistry, secretRegistry, tenantID, secretRef, "fallback"); !errors.Is(err, context.Canceled) {
		t.Fatalf("secret cancellation = %v", err)
	}
	if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: "env/dynamic"}, "dynamic-secret"); err != nil {
		t.Fatal(err)
	}
	if err := ensureEnvironmentSecret(context.Background(), secretRegistry, secretRegistry, tenantID, "env/dynamic", ""); err != nil {
		t.Fatalf("dynamic secret resolver = %v", err)
	}

	options := baseOptions
	options.config = environmentConfig{s3AccessKeyID: "access", s3SecretKey: "secret", s3SecretRef: "env/s3"}
	options.controlPlane = &environmentTenantRuntimeDependencies{secrets: secretRegistry}
	providers := []environmentRuntimeProviderSpec{{name: "inmemory", capabilities: []backend.Capability{backend.CapabilitySession}, store: store}}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilityArtifact, Provider: "s3", SecretRef: "env/s3"}}}); err != nil {
		t.Fatalf("S3 control-plane provider = %v", err)
	}
	if _, err := backendRegistry.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: tenantID}, backend.CapabilityBinding{Capability: backend.CapabilityArtifact, Provider: "s3"}); err != nil {
		t.Fatalf("S3 provider registration = %v", err)
	}
	options.config.s3SecretRef = "env/other"
	options.controlPlane = &environmentTenantRuntimeDependencies{secrets: modelruntime.NewSecretRegistry()}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilityArtifact, Provider: "s3", SecretRef: "env/s3"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("S3 static secret mismatch = %v", err)
	}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "unknown"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown runtime provider = %v", err)
	}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilityMemory, Provider: "inmemory"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported runtime capability = %v", err)
	}

	redisSecretRef := "env/redis"
	if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: redisSecretRef}, "redis-password"); err != nil {
		t.Fatal(err)
	}
	options.controlPlane = &environmentTenantRuntimeDependencies{secrets: secretRegistry}
	options.config = environmentConfig{runtimeStorage: "redis", redisEndpoint: "redis://configured", redisSecretRef: redisSecretRef, redis: runtimestorageredis.Config{Password: "redis-password"}}
	providers = []environmentRuntimeProviderSpec{{name: "redis", capabilities: []backend.Capability{backend.CapabilitySession}, store: store}}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://configured", SecretRef: redisSecretRef}}}); err != nil {
		t.Fatalf("Redis control-plane provider = %v", err)
	}
	options.config.redis.Password = ""
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://configured", SecretRef: redisSecretRef}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Redis dynamic secret mismatch = %v", err)
	}
	if _, ok := environmentRuntimeProvider(providers, "missing"); ok {
		t.Fatal("missing runtime provider was found")
	}
	if environmentRuntimeProviderSupports(providers[0], backend.CapabilityMemory) {
		t.Fatal("unsupported runtime capability was found")
	}
}

type environmentTenantRepositoryStub struct {
	tenant.Repository
	get func(context.Context, string) (*tenant.Tenant, error)
}

func (stub environmentTenantRepositoryStub) Get(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
	return stub.get(ctx, tenantID)
}

type environmentAppRepositoryStub struct {
	appmodel.Repository
	get         func(context.Context, string, string) (*appmodel.App, error)
	getRevision func(context.Context, string, string, int64) (*appmodel.Revision, error)
}

func (stub environmentAppRepositoryStub) Get(ctx context.Context, tenantID, appID string) (*appmodel.App, error) {
	return stub.get(ctx, tenantID, appID)
}

func (stub environmentAppRepositoryStub) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (*appmodel.Revision, error) {
	return stub.getRevision(ctx, tenantID, appID, revision)
}

type environmentModelRepositoryStub struct {
	modelprofile.Repository
	get func(context.Context, string, string) (*modelprofile.Profile, error)
}

func (stub environmentModelRepositoryStub) Get(ctx context.Context, tenantID, profileID string) (*modelprofile.Profile, error) {
	return stub.get(ctx, tenantID, profileID)
}

type environmentBackendRepositoryStub struct {
	backend.Repository
	get func(context.Context, string, string) (*backend.Profile, error)
}

func (stub environmentBackendRepositoryStub) Get(ctx context.Context, tenantID, profileID string) (*backend.Profile, error) {
	return stub.get(ctx, tenantID, profileID)
}

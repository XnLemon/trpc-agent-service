package bootstrap

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestorageredis "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
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
	testEnvironmentMaterializerConstructionBoundaries(t, baseOptions)
	testEnvironmentSecretResolutionBoundaries(t, secretRegistry)
	testEnvironmentS3ProviderBoundaries(t, baseOptions, store, secretRegistry, backendRegistry)
	testEnvironmentGenericProviderBoundaries(t, baseOptions, store, secretRegistry)
	testEnvironmentRedisProviderBoundaries(t, baseOptions, store, secretRegistry)
	testEnvironmentProviderLookupBoundaries(t, store)
}

func testEnvironmentMaterializerConstructionBoundaries(t *testing.T, options environmentTenantRuntimeOptions) {
	t.Helper()
	if _, err := newEnvironmentTenantMaterializer(environmentTenantRuntimeOptions{
		config:          options.config,
		runtimeStores:   options.runtimeStores,
		modelRegistry:   options.modelRegistry,
		backendRegistry: options.backendRegistry,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing secret registry error = %v", err)
	}
	if _, err := newEnvironmentTenantMaterializer(environmentTenantRuntimeOptions{
		config:          options.config,
		runtimeStores:   options.runtimeStores,
		secretRegistry:  options.secretRegistry,
		modelRegistry:   options.modelRegistry,
		backendRegistry: options.backendRegistry,
		controlPlane:    &environmentTenantRuntimeDependencies{},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid control-plane dependencies error = %v", err)
	}
	if _, err := environmentTenantRuntimeForStores(options); err != nil {
		t.Fatalf("static tenant runtime construction = %v", err)
	}
}

func testEnvironmentSecretResolutionBoundaries(t *testing.T, secretRegistry *modelruntime.SecretRegistry) {
	t.Helper()
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
}

func testEnvironmentS3ProviderBoundaries(t *testing.T, baseOptions environmentTenantRuntimeOptions, store environmentStorage, secretRegistry *modelruntime.SecretRegistry, backendRegistry *storagefactory.ProviderRegistry) {
	t.Helper()
	const tenantID = "t_00000000000000000000000000"
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
	closedS3Registry := storagefactory.NewProviderRegistry()
	if err := closedS3Registry.Close(); err != nil {
		t.Fatal(err)
	}
	closedS3 := options
	closedS3.backendRegistry = closedS3Registry
	if err := closedS3.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilityArtifact, Provider: "s3", SecretRef: "env/s3"}}}); !errors.Is(err, storagefactory.ErrRegistryClosed) {
		t.Fatalf("closed S3 provider registry = %v", err)
	}
	options.config.s3SecretRef = "env/other"
	options.controlPlane = &environmentTenantRuntimeDependencies{secrets: modelruntime.NewSecretRegistry()}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilityArtifact, Provider: "s3", SecretRef: "env/s3"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("S3 static secret mismatch = %v", err)
	}
}

func testEnvironmentGenericProviderBoundaries(t *testing.T, baseOptions environmentTenantRuntimeOptions, store environmentStorage, secretRegistry *modelruntime.SecretRegistry) {
	t.Helper()
	const tenantID = "t_00000000000000000000000000"
	options := baseOptions
	options.config = environmentConfig{runtimeStorage: "inmemory"}
	options.controlPlane = &environmentTenantRuntimeDependencies{secrets: secretRegistry}
	providers := []environmentRuntimeProviderSpec{{name: "inmemory", capabilities: []backend.Capability{backend.CapabilitySession}, store: store}}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "unknown"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown runtime provider = %v", err)
	}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilityMemory, Provider: "inmemory"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported runtime capability = %v", err)
	}
	missingSecret := options
	missingSecret.controlPlane = &environmentTenantRuntimeDependencies{secrets: modelruntime.NewSecretRegistry()}
	if err := missingSecret.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", SecretRef: "env/missing"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unresolved runtime provider secret = %v", err)
	}
	closedGenericRegistry := storagefactory.NewProviderRegistry()
	if err := closedGenericRegistry.Close(); err != nil {
		t.Fatal(err)
	}
	closedGeneric := options
	closedGeneric.config = environmentConfig{runtimeStorage: "inmemory"}
	closedGeneric.backendRegistry = closedGenericRegistry
	closedGeneric.controlPlane = &environmentTenantRuntimeDependencies{secrets: secretRegistry}
	if err := closedGeneric.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}}); !errors.Is(err, storagefactory.ErrRegistryClosed) {
		t.Fatalf("closed runtime provider registry = %v", err)
	}
}

func testEnvironmentRedisProviderBoundaries(t *testing.T, baseOptions environmentTenantRuntimeOptions, store environmentStorage, secretRegistry *modelruntime.SecretRegistry) {
	t.Helper()
	const tenantID = "t_00000000000000000000000000"
	options := baseOptions
	redisSecretRef := "env/redis"
	if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: redisSecretRef}, "redis-password"); err != nil {
		t.Fatal(err)
	}
	options.controlPlane = &environmentTenantRuntimeDependencies{secrets: secretRegistry}
	options.config = environmentConfig{runtimeStorage: "redis", redisEndpoint: "redis://configured", redisSecretRef: redisSecretRef, redis: runtimestorageredis.Config{Password: "redis-password"}}
	providers := []environmentRuntimeProviderSpec{{name: "redis", capabilities: []backend.Capability{backend.CapabilitySession}, store: store}}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://configured", SecretRef: "env/other"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Redis static secret mismatch = %v", err)
	}
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://configured", SecretRef: redisSecretRef}}}); err != nil {
		t.Fatalf("Redis control-plane provider = %v", err)
	}
	options.config.redis.Password = ""
	if err := options.registerControlPlaneRuntimeProviders(context.Background(), tenantID, providers, backend.StorageFactoryInput{TenantID: tenantID, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://configured", SecretRef: redisSecretRef}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Redis dynamic secret mismatch = %v", err)
	}
}

func testEnvironmentProviderLookupBoundaries(t *testing.T, store environmentStorage) {
	t.Helper()
	providers := []environmentRuntimeProviderSpec{{name: "inmemory", capabilities: []backend.Capability{backend.CapabilitySession}, store: store}}
	if _, ok := environmentRuntimeProvider(providers, "missing"); ok {
		t.Fatal("missing runtime provider was found")
	}
	if environmentRuntimeProviderSupports(providers[0], backend.CapabilityMemory) {
		t.Fatal("unsupported runtime capability was found")
	}
}

func TestEnvironmentRuntimeCapabilityProviderCoversCapabilityBoundaries(t *testing.T) {
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	input := backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000"}
	for _, capability := range []backend.Capability{
		backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge,
		backend.CapabilityArtifact, backend.CapabilityAudit,
	} {
		provider := environmentRuntimeCapabilityProvider{capability: capability, store: store}
		value, err := provider.newCapability(context.Background(), input)
		if err != nil || value == nil {
			t.Fatalf("capability %s = %v, err=%v", capability, value, err)
		}
	}

	for _, capability := range []backend.Capability{
		backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge,
		backend.CapabilityArtifact, backend.CapabilityAudit,
	} {
		provider := environmentRuntimeCapabilityProvider{capability: capability, store: environmentStorageStub{}}
		if _, err := provider.newCapability(context.Background(), input); !errors.Is(err, storagefactory.ErrStorageFactory) {
			t.Fatalf("missing %s capability error = %v", capability, err)
		}
	}
	if _, err := (environmentRuntimeCapabilityProvider{capability: "unknown", store: store}).newCapability(context.Background(), input); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("unknown capability error = %v", err)
	}

	secret, err := modelprofile.NewSecretValue("password")
	if err != nil {
		t.Fatal(err)
	}
	redisProvider := environmentRuntimeCapabilityProvider{
		backend: "redis", capability: backend.CapabilitySession, redisEndpoint: "redis://configured",
		redisSecretRef: "env/redis", redisPasswordRequired: true,
	}
	for name, test := range map[string]struct {
		provider environmentRuntimeCapabilityProvider
		binding  backend.CapabilityBinding
		secret   modelprofile.SecretValue
	}{
		"unsupported capability": {provider: func() environmentRuntimeCapabilityProvider {
			value := redisProvider
			value.capability = backend.CapabilityArtifact
			return value
		}(), binding: backend.CapabilityBinding{Capability: backend.CapabilityArtifact, Provider: "redis"}, secret: secret},
		"endpoint mismatch":         {provider: redisProvider, binding: backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://other"}, secret: secret},
		"secret reference mismatch": {provider: redisProvider, binding: backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://configured", SecretRef: "env/other"}, secret: secret},
		"required secret missing":   {provider: redisProvider, binding: backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://configured", SecretRef: "env/redis"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := test.provider.validateRedisBinding(test.binding, test.secret); !errors.Is(err, storagefactory.ErrStorageFactory) {
				t.Fatalf("Redis binding error = %v", err)
			}
		})
	}
}

func TestEnvironmentTenantMaterializerRegistersStaticRuntime(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	secretRegistry := modelruntime.NewSecretRegistry()
	modelRegistry := modelruntime.NewModelProviderRegistry()
	backendRegistry := storagefactory.NewProviderRegistry()
	options := environmentTenantRuntimeOptions{
		config: environmentConfig{
			runtimeStorage: "inmemory", modelProvider: "openai", modelAPIKey: "global-secret",
			modelAPIKeys: map[string]string{tenantID: "tenant-secret"}, secretRef: "env/model",
			s3AccessKeyID: "access", s3SecretKey: "secret", s3SecretRef: "env/s3",
		},
		runtimeStores:   environmentRuntimeStores{primary: store, providers: map[string]environmentStorage{"inmemory": store}},
		secretRegistry:  secretRegistry,
		modelRegistry:   modelRegistry,
		backendRegistry: backendRegistry,
	}
	materializer := newEnvironmentStaticMaterializer(t, options)
	testEnvironmentStaticMaterializerRuntime(t, materializer, options, secretRegistry, backendRegistry, tenantID)
	testEnvironmentStaticMaterializerRegistryBoundaries(t, options, tenantID)
}

func newEnvironmentStaticMaterializer(t *testing.T, options environmentTenantRuntimeOptions) func(context.Context, string) error {
	t.Helper()
	if _, err := newEnvironmentTenantMaterializer(environmentTenantRuntimeOptions{
		config: options.config, runtimeStores: environmentRuntimeStores{}, secretRegistry: options.secretRegistry,
		modelRegistry: options.modelRegistry, backendRegistry: options.backendRegistry,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing runtime provider error = %v", err)
	}
	materializer, err := newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	return materializer
}

func testEnvironmentStaticMaterializerRuntime(t *testing.T, materializer func(context.Context, string) error, options environmentTenantRuntimeOptions, secretRegistry *modelruntime.SecretRegistry, backendRegistry *storagefactory.ProviderRegistry, tenantID string) {
	t.Helper()
	if err := materializer(nil, tenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil materializer context error = %v", err)
	}
	if err := materializer(context.Background(), ""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty materializer tenant error = %v", err)
	}
	if err := materializer(canceledContext(), tenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled materializer error = %v", err)
	}
	if err := materializer(context.Background(), tenantID); err != nil {
		t.Fatal(err)
	}
	secret, err := secretRegistry.Resolve(context.Background(), modelprofile.SecretScope{TenantID: tenantID, SecretRef: options.config.secretRef})
	if err != nil || secret.Value() != "tenant-secret" {
		t.Fatalf("tenant model secret = %s, err=%v", secret, err)
	}
	if _, err := backendRegistry.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: tenantID}, backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "inmemory"}); err != nil {
		t.Fatalf("static runtime provider registration = %v", err)
	}
}

func testEnvironmentStaticMaterializerRegistryBoundaries(t *testing.T, options environmentTenantRuntimeOptions, tenantID string) {
	t.Helper()
	demoOptions := options
	demoOptions.config = environmentConfig{runtimeStorage: "inmemory", demoMode: true}
	demoMaterializer, err := newEnvironmentTenantMaterializer(demoOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := demoMaterializer(context.Background(), tenantID); err != nil {
		t.Fatalf("demo materializer = %v", err)
	}
	closedSecretRegistry := modelruntime.NewSecretRegistry()
	if err := closedSecretRegistry.Close(); err != nil {
		t.Fatal(err)
	}
	closedStaticSecret := options
	closedStaticSecret.secretRegistry = closedSecretRegistry
	secretMaterializer, err := newEnvironmentTenantMaterializer(closedStaticSecret)
	if err != nil {
		t.Fatal(err)
	}
	if err := secretMaterializer(context.Background(), "t_00000000000000000000000001"); !errors.Is(err, modelruntime.ErrRegistryClosed) {
		t.Fatalf("closed static secret registry error = %v", err)
	}
	closedStaticModel := options
	closedStaticModel.modelRegistry = modelruntime.NewModelProviderRegistry()
	if err := closedStaticModel.modelRegistry.Close(); err != nil {
		t.Fatal(err)
	}
	modelMaterializer, err := newEnvironmentTenantMaterializer(closedStaticModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := modelMaterializer(context.Background(), "t_00000000000000000000000002"); !errors.Is(err, modelruntime.ErrRegistryClosed) {
		t.Fatalf("closed static model registry error = %v", err)
	}

	closedBackend := storagefactory.NewProviderRegistry()
	if err := closedBackend.Close(); err != nil {
		t.Fatal(err)
	}
	options.backendRegistry = closedBackend
	materializer, err := newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := materializer(context.Background(), "t_00000000000000000000000001"); !errors.Is(err, storagefactory.ErrRegistryClosed) {
		t.Fatalf("closed runtime provider registry error = %v", err)
	}
}

func TestEnvironmentControlPlaneRuntimeResolutionAndMaterialization(t *testing.T) {
	fixture := newEnvironmentControlPlaneFixture(t)
	modelInput := environmentControlPlaneRuntimeInputs(t, fixture)
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	options := environmentTenantRuntimeOptions{
		config:          environmentConfig{runtimeStorage: "inmemory", modelProvider: "fake", secretRef: modelInput.SecretRef, modelAPIKey: "fallback"},
		runtimeStores:   environmentRuntimeStores{primary: store, providers: map[string]environmentStorage{"inmemory": store}},
		secretRegistry:  fixture.dependencies.secrets.(*modelruntime.SecretRegistry),
		modelRegistry:   modelruntime.NewModelProviderRegistry(),
		backendRegistry: storagefactory.NewProviderRegistry(),
		controlPlane:    &fixture.dependencies,
	}
	testEnvironmentControlPlaneMaterializer(t, fixture, options)
	testEnvironmentControlPlaneMaterializerBoundaries(t, fixture, options)
	testEnvironmentControlPlaneSnapshotBoundaries(t, fixture)
}

func environmentControlPlaneRuntimeInputs(t *testing.T, fixture environmentControlPlaneFixture) modelprofile.ModelFactoryInput {
	t.Helper()
	modelInput, storageInput, err := fixture.dependencies.resolve(context.Background(), fixture.root.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if modelInput.Provider != "fake" || storageInput.Bindings[0].Provider != "inmemory" {
		t.Fatalf("resolved runtime inputs = model:%+v storage:%+v", modelInput, storageInput)
	}
	return modelInput
}

func testEnvironmentControlPlaneMaterializer(t *testing.T, fixture environmentControlPlaneFixture, options environmentTenantRuntimeOptions) {
	t.Helper()
	materializer, err := newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := materializer(context.Background(), fixture.root.TenantID); err != nil {
		t.Fatalf("control-plane materializer = %v", err)
	}
	if _, err := options.backendRegistry.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: fixture.root.TenantID}, backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "inmemory"}); err != nil {
		t.Fatalf("control-plane backend registration = %v", err)
	}
}

func testEnvironmentControlPlaneMaterializerBoundaries(t *testing.T, fixture environmentControlPlaneFixture, options environmentTenantRuntimeOptions) {
	t.Helper()
	providerMismatch := options
	providerMismatch.config.modelProvider = "openai"
	mismatchMaterializer, err := newEnvironmentTenantMaterializer(providerMismatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := mismatchMaterializer(context.Background(), fixture.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("model provider mismatch = %v", err)
	}

	secretMismatch := options
	secretMismatchDependencies := fixture.dependencies
	secretMismatchDependencies.secrets = modelruntime.NewSecretRegistry()
	secretMismatch.controlPlane = &secretMismatchDependencies
	secretMismatch.config.modelAPIKey = ""
	secretMismatch.config.modelAPIKeys = nil
	secretMaterializer, err := newEnvironmentTenantMaterializer(secretMismatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := secretMaterializer(context.Background(), fixture.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unresolved model secret = %v", err)
	}

	modelClosed := options
	modelClosed.modelRegistry = modelruntime.NewModelProviderRegistry()
	if err := modelClosed.modelRegistry.Close(); err != nil {
		t.Fatal(err)
	}
	closedModelMaterializer, err := newEnvironmentTenantMaterializer(modelClosed)
	if err != nil {
		t.Fatal(err)
	}
	if err := closedModelMaterializer(context.Background(), fixture.root.TenantID); !errors.Is(err, modelruntime.ErrRegistryClosed) {
		t.Fatalf("closed model registry = %v", err)
	}

	backendClosed := options
	backendClosed.backendRegistry = storagefactory.NewProviderRegistry()
	if err := backendClosed.backendRegistry.Close(); err != nil {
		t.Fatal(err)
	}
	closedBackendMaterializer, err := newEnvironmentTenantMaterializer(backendClosed)
	if err != nil {
		t.Fatal(err)
	}
	if err := closedBackendMaterializer(context.Background(), fixture.root.TenantID); !errors.Is(err, storagefactory.ErrRegistryClosed) {
		t.Fatalf("closed backend registry = %v", err)
	}

	demo := options
	demo.config.demoMode = true
	demoMaterializer, err := newEnvironmentTenantMaterializer(demo)
	if err != nil {
		t.Fatal(err)
	}
	if err := demoMaterializer(context.Background(), fixture.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo model with secret reference = %v", err)
	}
}

func testEnvironmentControlPlaneSnapshotBoundaries(t *testing.T, fixture environmentControlPlaneFixture) {
	t.Helper()
	invalidTenant := fixture.dependencies
	invalidTenant.tenants = environmentTenantRepositoryStub{get: func(context.Context, string) (*tenant.Tenant, error) {
		value := fixture.root.Clone()
		value.TenantKey = "invalid tenant key"
		return &value, nil
	}}
	if _, _, err := invalidTenant.resolve(context.Background(), fixture.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid tenant snapshot = %v", err)
	}
	invalidModel := fixture.dependencies
	invalidModel.models = environmentModelRepositoryStub{get: func(context.Context, string, string) (*modelprofile.Profile, error) {
		value, getErr := fixture.models.Get(context.Background(), fixture.root.TenantID, fixture.modelID)
		if getErr != nil {
			return nil, getErr
		}
		value.Configuration.Model = ""
		return value, nil
	}}
	if _, _, err := invalidModel.resolve(context.Background(), fixture.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid model snapshot = %v", err)
	}
	invalidBackend := fixture.dependencies
	invalidBackend.backends = environmentBackendRepositoryStub{get: func(context.Context, string, string) (*backend.Profile, error) {
		value, getErr := fixture.backends.Get(context.Background(), fixture.root.TenantID, fixture.backendID)
		if getErr != nil {
			return nil, getErr
		}
		value.Bindings = nil
		return value, nil
	}}
	if _, _, err := invalidBackend.resolve(context.Background(), fixture.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid backend snapshot = %v", err)
	}
}

type environmentControlPlaneFixture struct {
	dependencies environmentTenantRuntimeDependencies
	root         *tenant.Tenant
	models       *modelmemory.InMemoryRepository
	backends     *backendmemory.InMemoryRepository
	modelID      string
	backendID    string
}

func newEnvironmentControlPlaneFixture(t *testing.T) environmentControlPlaneFixture {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"model"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldRequired,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(
		backend.ProviderSpec{Provider: "memory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}}},
		backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantmemory.NewRepository()
	apps := appmemory.NewRepository()
	models := modelmemory.NewRepository(modelCatalog)
	backends := backendmemory.NewRepository(backendCatalog)
	root, published := createBootstrapTenantExecutionState(t, tenants, apps, models, backends, "environment-runtime", "environment-runtime", "model", "env/model")
	profile, _, err := backends.Create(context.Background(), backend.CreateInput{
		TenantID: root.TenantID, ProfileKey: "backend-inmemory", DisplayName: "In-memory backend",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "environment-runtime"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: "environment-runtime"},
	})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName, AuditRetentionDays: root.AuditRetentionDays,
		LogMaskingLevel: root.LogMaskingLevel, TraceSamplingRate: root.TraceSamplingRate, DefaultAgentAppID: root.DefaultAgentAppID, DefaultBackendProfileID: &profile.ProfileID,
	})
	if err != nil {
		t.Fatal(err)
	}
	root = updated
	revision, err := apps.GetRevision(context.Background(), root.TenantID, published.AppID, *published.CurrentRevision)
	if err != nil {
		t.Fatal(err)
	}
	modelValue, err := models.Get(context.Background(), root.TenantID, revision.ModelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	canary := revision.Revision + 1
	canaryApp := published.Clone()
	canaryApp.CanaryRevision = &canary
	appRepository := environmentAppRepositoryStub{
		get: func(context.Context, string, string) (*appmodel.App, error) {
			value := canaryApp.Clone()
			return &value, nil
		},
		getRevision: func(ctx context.Context, tenantID, appID string, revisionNumber int64) (*appmodel.Revision, error) {
			if revisionNumber == canary {
				value := revision.Clone()
				value.Revision = canary
				return &value, nil
			}
			return apps.GetRevision(ctx, tenantID, appID, revisionNumber)
		},
	}
	secrets := modelruntime.NewSecretRegistry()
	if err := secrets.RegisterValue(modelprofile.SecretScope{TenantID: root.TenantID, SecretRef: modelValue.Configuration.SecretRef}, "model-secret"); err != nil {
		t.Fatal(err)
	}
	return environmentControlPlaneFixture{
		dependencies: environmentTenantRuntimeDependencies{tenants: tenants, apps: appRepository, models: models, backends: backends, modelCatalog: modelCatalog, backendCatalog: backendCatalog, secrets: secrets},
		root:         root, models: models, backends: backends, modelID: modelValue.ProfileID, backendID: profile.ProfileID,
	}
}

type environmentStorageStub struct{ environmentStorage }

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

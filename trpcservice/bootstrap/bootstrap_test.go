package bootstrap

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	runtimeservice "github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestNewBuildsRealGraphAndGatesReadiness(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	var gate atomic.Bool
	config.ReadyGate = gate.Load
	config.CloseDependencies = nil
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if graph.Resolver == nil || graph.Registry == nil || graph.Dispatcher == nil || graph.HandlerValue() == nil {
		t.Fatal("bootstrap did not construct the real resolver/registry/dispatcher/handler graph")
	}
	readyRequest := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyResponse := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(readyResponse, readyRequest)
	if readyResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured readiness status = %d", readyResponse.Code)
	}

	gate.Store(true)
	readyResponse = httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(readyResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyResponse.Code != http.StatusOK {
		t.Fatalf("configured readiness status = %d", readyResponse.Code)
	}

	graph.BeginShutdown()
	if graph.Ready() || graph.HandlerValue().Ready() {
		t.Fatal("shutdown graph remained ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapServesConcurrentTenantsWithIndependentProviders(t *testing.T) {
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"model-one", "model-two"},
		EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldRequired,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "memory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString, Required: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantmemory.NewRepository()
	apps := agentmemory.NewRepository()
	models := modelmemory.NewRepository(modelCatalog)
	backends := backendmemory.NewRepository(backendCatalog)
	identities := make(map[string]gateway.APIIdentity)
	secrets := modelprofile.NewSecretRegistry()
	for _, configured := range []struct {
		token, tenantKey, appKey, modelName, secretRef, secretValue string
	}{
		{token: "token-one", tenantKey: "bootstrap-one", appKey: "app-one", modelName: "model-one", secretRef: "secret/one", secretValue: "value-one"},
		{token: "token-two", tenantKey: "bootstrap-two", appKey: "app-two", modelName: "model-two", secretRef: "secret/two", secretValue: "value-two"},
	} {
		root, app := createBootstrapTenantExecutionState(t, tenants, apps, models, backends, configured.tenantKey, configured.appKey, configured.modelName, configured.secretRef)
		if err := secrets.RegisterValue(modelprofile.SecretScope{TenantID: root.TenantID, SecretRef: configured.secretRef}, configured.secretValue); err != nil {
			t.Fatal(err)
		}
		identities[configured.token] = gateway.APIIdentity{TenantID: root.TenantID, AppID: app.AppID, SubjectID: configured.tenantKey}
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(identities)
	if err != nil {
		t.Fatal(err)
	}
	modelFactory := &bootstrapRecordingModelFactory{}
	storageFactory := &bootstrapRecordingStorageFactory{}
	graph, err := New(context.Background(), Config{
		Tenants: tenants, Apps: apps, Models: models, Backends: backends, Channels: channelmemory.NewRepository(),
		ModelCatalog: modelCatalog, BackendCatalog: backendCatalog, SecretResolver: secrets, ModelFactory: modelFactory,
		StorageFactory: storageFactory, Authenticator: authenticator,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	type result struct {
		tenantID string
		err      error
	}
	results := make(chan result, len(identities))
	var group sync.WaitGroup
	for token, identity := range identities {
		group.Add(1)
		go func(token string, identity gateway.APIIdentity) {
			defer group.Done()
			request := httptest.NewRequest(http.MethodGet, "/v1/chat", nil)
			request.Header.Set("Authorization", "Bearer "+token)
			authenticated, authErr := authenticator.Authenticate(context.Background(), request)
			if authErr != nil {
				results <- result{tenantID: identity.TenantID, err: authErr}
				return
			}
			plan, resolveErr := graph.Resolver.ResolveAuthenticatedAPI(context.Background(), authenticated)
			if resolveErr != nil {
				results <- result{tenantID: identity.TenantID, err: resolveErr}
				return
			}
			lease, acquireErr := graph.Registry.Acquire(context.Background(), plan)
			if acquireErr == nil {
				acquireErr = lease.Release()
			}
			results <- result{tenantID: identity.TenantID, err: acquireErr}
		}(token, identity)
	}
	group.Wait()
	close(results)
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("tenant %s execution setup = %v", outcome.tenantID, outcome.err)
		}
	}
	for _, identity := range identities {
		if modelFactory.Secret(identity.TenantID) == "" || storageFactory.SessionCount(identity.TenantID) != 1 {
			t.Fatalf("tenant %s did not receive independent provider materialization", identity.TenantID)
		}
	}
	if modelFactory.Secret(identities["token-one"].TenantID) == modelFactory.Secret(identities["token-two"].TenantID) {
		t.Fatal("tenant model secrets crossed provider scope")
	}
}

func TestRuntimeStartsAndStopsConfiguredOutboxWorker(t *testing.T) {
	store := runtimestorageinmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", EventID: "event", BindingID: "binding", ExternalMessageID: "external"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", EventID: "event", ReplyID: "reply", SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	provider := &bootstrapBlockingProvider{started: make(chan struct{}), canceled: make(chan struct{})}
	worker, err := outbox.New(outbox.Config{Store: store, Provider: provider, TenantID: "tenant-a", Owner: "bootstrap-worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.RuntimeStore = store
	config.OutboxWorker = worker
	config.OutboxPollInterval = time.Hour
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("bootstrap did not start the configured outbox worker")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.canceled:
	case <-time.After(time.Second):
		t.Fatal("runtime close did not cancel and join the outbox worker")
	}
}

func TestNewRejectsAlreadyRunningOutboxWorker(t *testing.T) {
	worker, err := outbox.New(outbox.Config{
		Store: runtimestorageinmemory.New(), Provider: &bootstrapBlockingProvider{started: make(chan struct{}), canceled: make(chan struct{})},
		TenantID: "tenant-a", Owner: "already-running", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = worker.Close() })
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.OutboxWorker = worker
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("already-running worker error = %v", err)
	}
}

func TestNewRejectsMissingExplicitDependency(t *testing.T) {
	if _, err := New(context.Background(), Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing dependency error = %v", err)
	}
}

func TestBootstrapFailureAndLifecycleBoundaries(t *testing.T) {
	if _, err := New(nilContextForTest(), Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil bootstrap context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(canceled, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bootstrap context error = %v", err)
	}

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	config, closeDependencies := testConfig(t)
	t.Cleanup(closeDependencies)
	config.DB = db
	mock.ExpectPing().WillReturnError(errors.New("database unavailable"))
	if _, err := New(context.Background(), config); !errors.Is(err, postgres.ErrStorage) {
		t.Fatalf("failed bootstrap ping error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	config, closeDependencies = testConfig(t)
	t.Cleanup(closeDependencies)
	config.Ping = func(context.Context) error { return errors.New("readiness ping failure") }
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if graph.Ready() {
		t.Fatal("graph remained ready after a ping failure")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}

	closeFailure := errors.New("dependency close failure")
	config, closeDependencies = testConfig(t)
	config.CloseDependencies = func() error {
		closeDependencies()
		return closeFailure
	}
	graph, err = New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("close failure = %v", err)
	}
	if err := graph.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("repeat close failure = %v", err)
	}
	if (&Runtime{}).Ready() {
		t.Fatal("uninitialized runtime reported ready")
	}
}

func TestBootstrapCoversConstructionFailureBoundaries(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.RuntimeTenantID = "invalid"
	config.Sessions = nil
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid runtime tenant configuration = %v", err)
	}

	config, closeDependencies = testConfig(t)
	config.Registry.Factory = func(context.Context, runtimeservice.ExecutionPlan) (gateway.Runner, error) { return nil, nil }
	config.HTTP.MaxBodyBytes = -1
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		closeDependencies()
		t.Fatalf("invalid handler configuration = %v", err)
	}
	closeDependencies()

	config, closeDependencies = testConfig(t)
	config.WeComHandler = &bootstrapWeComLifecycle{}
	config.WeComHandlerFactory = func(gateway.DispatchService) (http.Handler, error) { return nil, nil }
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		closeDependencies()
		t.Fatalf("conflicting callback handlers = %v", err)
	}
	closeDependencies()

	config, closeDependencies = testConfig(t)
	config.AdminAuthenticator, _ = admin.NewStaticAuthenticator("admin", []string{"*"})
	config.Channels = candidateOnly{}
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		closeDependencies()
		t.Fatalf("non-repository admin channel dependency = %v", err)
	}
	closeDependencies()
}

func TestBootstrapRoutesAdminCacheInvalidationsToRuntimeRegistry(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, change := range []admin.CacheInvalidation{
		{TenantID: tenantID, Kind: admin.CacheInvalidationTenant},
		{TenantID: tenantID, AppID: "app-1", Kind: admin.CacheInvalidationApp},
		{TenantID: tenantID, ProfileID: "model-1", Kind: admin.CacheInvalidationModel},
		{TenantID: tenantID, ProfileID: "backend-1", Kind: admin.CacheInvalidationBackend},
		{TenantID: tenantID, BindingID: "binding-1", Kind: admin.CacheInvalidationBinding},
	} {
		invalidateRuntimeCache(graph.Registry, change)
	}
}

func TestNewUnavailableUsesRealGraphButReturns503(t *testing.T) {
	graph, err := NewUnavailable()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	if graph.Resolver == nil || graph.Registry == nil || graph.Dispatcher == nil {
		t.Fatal("unavailable mode did not construct the real execution graph")
	}
	response := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable readiness status = %d", response.Code)
	}
}

func TestUnavailableBootstrapBoundariesAndHTTPServer(t *testing.T) {
	graph, err := NewUnavailable()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	if _, err := NewHTTPServer(nil, ":8080", 0, 0, 0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil graph server error = %v", err)
	}
	if _, err := NewHTTPServer(&Runtime{}, ":8080", 0, 0, 0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("handlerless graph server error = %v", err)
	}
	if _, err := NewHTTPServer(graph, "", 0, 0, 0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty address server error = %v", err)
	}
	server, err := NewHTTPServer(graph, ":8080", 0, 0, 0, 0)
	if err != nil || server.Handler != graph.HandlerValue().Handler() {
		t.Fatalf("HTTP server = %+v, err=%v", server, err)
	}
	if _, err := (unavailableSecretResolver{}).Resolve(context.Background(), modelprofile.SecretScope{}); !errors.Is(err, ErrBootstrapNotReady) {
		t.Fatalf("unavailable secret resolver error = %v", err)
	}
	if _, err := (unavailableModelFactory{}).New(context.Background(), modelprofile.ModelFactoryInput{}, modelprofile.SecretValue{}); !errors.Is(err, ErrBootstrapNotReady) {
		t.Fatalf("unavailable model factory error = %v", err)
	}
	var nilGraph *Runtime
	if nilGraph.HandlerValue() != nil || nilGraph.Ready() {
		t.Fatal("nil runtime reported a handler or readiness")
	}
}

func TestEnvironmentBootstrapRequiresExplicitConfigurationAndBuildsDependencies(t *testing.T) {
	setEnvironmentBootstrapTestVariables(t)
	config := assertEnvironmentConfigurationAndCatalogs(t)
	assertEnvironmentAuthenticationAndSecret(t, config)
	assertEnvironmentRequiredValues(t)
}

func setEnvironmentBootstrapTestVariables(t *testing.T) {
	t.Helper()
	t.Setenv(envPostgresDSN, "postgres://postgres:postgres@127.0.0.1:5432/control_plane")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envSubjectID, "service")
	t.Setenv(envModelAPIKey, "test-secret")
	t.Setenv(envModelProvider, "openai")
	t.Setenv(envModelNames, "gpt-4o-mini,custom.model")
	t.Setenv(envModelEndpointHost, "api.openai.com,proxy.example")
	t.Setenv(envModelSecretRef, "env/test-key")
	t.Setenv(envSessionBackend, "postgres")
}

func assertEnvironmentConfigurationAndCatalogs(t *testing.T) environmentConfig {
	t.Helper()
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.modelProvider != "openai" || len(config.modelNames) != 2 || config.secretRef != "env/test-key" {
		t.Fatalf("environment config = %+v", config)
	}
	t.Setenv(envSessionBackend, "inmemory")
	devConfig, err := loadEnvironment()
	if err != nil || devConfig.runtimeStorage != "inmemory" {
		t.Fatalf("documented in-memory backend = %+v, err=%v", devConfig, err)
	}
	t.Setenv(envSessionBackend, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing explicit session backend error = %v", err)
	}
	t.Setenv(envSessionBackend, "postgres")
	modelCatalog, backendCatalog, err := environmentCatalogs(config)
	if err != nil || modelCatalog == nil || backendCatalog == nil {
		t.Fatalf("environment catalogs = %v, %v, %v", modelCatalog, backendCatalog, err)
	}
	return config
}

func assertEnvironmentAuthenticationAndSecret(t *testing.T, config environmentConfig) {
	t.Helper()
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{
		config.apiToken: {TenantID: config.tenantID, AppID: config.appID, SubjectID: config.subjectID},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("Authorization", "Bearer "+config.apiToken)
	authenticated, err := authenticator.Authenticate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := authenticated.Identity()
	if err != nil || identity.TenantID != config.tenantID || identity.AppID != config.appID {
		t.Fatalf("environment identity = %+v, err=%v", identity, err)
	}

	resolver := environmentSecretResolver{reference: config.secretRef, value: config.modelAPIKey}
	secret, err := resolver.Resolve(context.Background(), modelprofile.SecretScope{TenantID: config.tenantID, SecretRef: config.secretRef})
	if err != nil || secret.Value() != config.modelAPIKey || secret.String() != "<redacted-secret>" {
		t.Fatalf("environment secret = %s, err=%v", secret, err)
	}
	model, err := (environmentModelFactory{}).New(context.Background(), modelprofile.ModelFactoryInput{Model: "gpt-4o-mini"}, secret)
	if err != nil || model == nil {
		t.Fatalf("environment model = %v, err=%v", model, err)
	}
}

func assertEnvironmentRequiredValues(t *testing.T) {
	t.Helper()
	for _, name := range []string{envPostgresDSN, envAPIToken, envTenantID, envAppID, envAdminToken, envAdminTenants, envModelAPIKey} {
		t.Setenv(name, "")
		if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("missing %s error = %v", name, err)
		}
		t.Setenv(name, "configured")
	}
	if _, err := NewFromEnvironment(nilContextForTest()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil environment bootstrap context error = %v", err)
	}
}

func TestEnvironmentWeComCredentialsMustBeConfiguredTogether(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envWeComCallbackToken, "callback-token")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("partial WeCom configuration error = %v", err)
	}
	t.Setenv(envWeComEncodingAESKey, "encoding-key")
	t.Setenv(envWeComAppSecret, "app-secret")
	t.Setenv(envWeComSecretRef, "env/wecom")
	config, err := loadEnvironment()
	if err != nil || config.wecom == nil {
		t.Fatalf("complete WeCom environment = %+v, %v", config.wecom, err)
	}
	if config.wecom.callbackToken != "callback-token" || config.wecom.secretRef != "env/wecom" {
		t.Fatalf("WeCom environment = %+v", config.wecom)
	}
	resolver := environmentWeComCredentialResolver{tenantID: config.tenantID, config: *config.wecom}
	credentials, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: config.tenantID, SecretRef: config.wecom.secretRef})
	if err != nil || credentials.CallbackToken != config.wecom.callbackToken || credentials.AppSecret != config.wecom.appSecret {
		t.Fatalf("WeCom credentials = %+v, %v", credentials, err)
	}
	if _, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: config.tenantID, SecretRef: "env/other"}); err == nil {
		t.Fatal("mismatched WeCom secret reference was accepted")
	}
}

func TestWeComHandlerFactoryIsWiredAndOwnedByRuntime(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	callback := &bootstrapWeComLifecycle{}
	var factoryCalls int
	config.WeComHandlerFactory = func(dispatcher gateway.DispatchService) (http.Handler, error) {
		if dispatcher == nil {
			t.Fatal("WeCom factory received nil dispatcher")
		}
		factoryCalls++
		return callback, nil
	}
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/wecom/callback/route", nil))
	if response.Code != http.StatusNoContent || factoryCalls != 1 || callback.calls.Load() != 1 {
		t.Fatalf("WeCom callback wiring = status %d factory %d calls %d", response.Code, factoryCalls, callback.calls.Load())
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if callback.beginShutdown.Load() != 1 || callback.closed.Load() != 1 {
		t.Fatalf("WeCom lifecycle = begin %d close %d", callback.beginShutdown.Load(), callback.closed.Load())
	}
}

func TestBootstrapBuildsRuntimeRegistryFromStorageFactory(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.Sessions = nil
	config.StorageFactory = backend.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		return backend.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: inmemory.NewSessionService()})
	})
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if !graph.Ready() {
		t.Fatal("storage-factory bootstrap graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func nilContextForTest() context.Context { return nil }

func setRequiredEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv(envPostgresDSN, "postgres://postgres:postgres@127.0.0.1:5432/control_plane")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envModelAPIKey, "model-secret")
	t.Setenv(envSessionBackend, "inmemory")
}

func TestEnvironmentBootstrapPreservesCancellationAndRejectsBadLists(t *testing.T) {
	t.Setenv(envPostgresDSN, "postgres://postgres:postgres@127.0.0.1:5432/control_plane")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envModelAPIKey, "test-secret")
	t.Setenv(envSessionBackend, "postgres")
	t.Setenv(envModelNames, "gpt-4o-mini,,custom.model")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty model list item error = %v", err)
	}
	t.Setenv(envModelNames, "gpt-4o-mini")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewFromEnvironment(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled environment bootstrap error = %v", err)
	}
}

func TestEnvironmentDependencyErrorBoundaries(t *testing.T) {
	if _, _, err := environmentCatalogs(environmentConfig{
		modelProvider: "invalid provider", modelNames: []string{"chat"}, endpointHosts: []string{"example.test"},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid model catalog error = %v", err)
	}
	if _, _, err := environmentCatalogs(environmentConfig{
		modelProvider: "anthropic", modelNames: []string{"chat"}, endpointHosts: []string{"example.test"},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported model provider error = %v", err)
	}
	values, err := environmentList("TEST_LIST", "A, B", false)
	if err != nil || len(values) != 2 || values[0] != "A" || values[1] != "B" {
		t.Fatalf("case-preserving environment list = %#v, err=%v", values, err)
	}
	if _, err := environmentList("TEST_LIST", ",", true); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty environment list error = %v", err)
	}

	resolver := environmentSecretResolver{reference: "env/test-key", value: "secret"}
	validScope := modelprofile.SecretScope{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", SecretRef: "env/test-key"}
	if _, err := resolver.Resolve(nilContextForTest(), validScope); err == nil {
		t.Fatal("nil resolver context unexpectedly succeeded")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, validScope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resolver error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), modelprofile.SecretScope{TenantID: "invalid", SecretRef: validScope.SecretRef}); err == nil {
		t.Fatal("invalid secret scope unexpectedly succeeded")
	}
	if _, err := resolver.Resolve(context.Background(), modelprofile.SecretScope{TenantID: validScope.TenantID, SecretRef: "env/other"}); err == nil {
		t.Fatal("mismatched secret reference unexpectedly succeeded")
	}
	if _, err := (environmentSecretResolver{reference: validScope.SecretRef}).Resolve(context.Background(), validScope); err == nil {
		t.Fatal("empty environment secret unexpectedly succeeded")
	}

	factory := environmentModelFactory{}
	secret, err := modelprofile.NewSecretValue("secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.New(nilContextForTest(), modelprofile.ModelFactoryInput{Model: "chat"}, secret); err == nil {
		t.Fatal("nil model factory context unexpectedly succeeded")
	}
	canceled, cancel = context.WithCancel(context.Background())
	cancel()
	if _, err := factory.New(canceled, modelprofile.ModelFactoryInput{Model: "chat"}, secret); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled model factory error = %v", err)
	}
	if _, err := factory.New(context.Background(), modelprofile.ModelFactoryInput{Model: "chat"}, modelprofile.SecretValue{}); err == nil {
		t.Fatal("empty model factory secret unexpectedly succeeded")
	}
	if _, err := factory.New(context.Background(), modelprofile.ModelFactoryInput{Model: "chat", Endpoint: "https://api.openai.com/v1"}, secret); err != nil {
		t.Fatalf("endpoint model factory error = %v", err)
	}
	if _, err := factory.New(context.Background(), modelprofile.ModelFactoryInput{Provider: "anthropic", Model: "chat"}, secret); err == nil {
		t.Fatal("unsupported model factory provider unexpectedly succeeded")
	}
}

func TestEnvironmentRuntimeStoreSelection(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if store, err := environmentRuntimeStore("inmemory", db); err != nil || store == nil {
		t.Fatalf("inmemory runtime store = %v, %v", store, err)
	}
	if store, err := environmentRuntimeStore("postgres", db); err != nil || store == nil {
		t.Fatalf("postgres runtime store = %v, %v", store, err)
	}
	if _, err := environmentRuntimeStore("unknown", db); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown runtime store = %v", err)
	}
	if _, err := environmentRuntimeStore("postgres", nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("postgres nil db = %v", err)
	}
}

func TestNewFromEnvironmentBuildsRealGraphWhenDatabaseOpens(t *testing.T) {
	t.Setenv(envPostgresDSN, "postgres://configured")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envModelAPIKey, "test-secret")
	t.Setenv(envSessionBackend, "postgres")

	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
		return db, nil
	}
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	defer func() { openEnvironmentDatabase = previousOpen }()
	defer func() { applyEnvironmentMigrations = previousApply; verifyEnvironmentMigrations = previousVerify }()

	graph, err := NewFromEnvironment(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if !graph.Ready() {
		t.Fatal("environment bootstrap graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}

	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
		return nil, errors.New("database open failure")
	}
	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("database open failure = %v", err)
	}
}

func TestNewFromEnvironmentInstallsWeComCallbackAndOutboxWorker(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envWeComCallbackToken, "callback-token")
	t.Setenv(envWeComEncodingAESKey, base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)))
	t.Setenv(envWeComAppSecret, "app-secret")
	t.Setenv(envWeComSecretRef, "env/wecom")

	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	previousWorker := newEnvironmentWeComWorker
	var workerConfig outbox.Config
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	newEnvironmentWeComWorker = func(config outbox.Config) (*outbox.Worker, error) {
		workerConfig = config
		return outbox.New(config)
	}
	defer func() { openEnvironmentDatabase = previousOpen }()
	defer func() { applyEnvironmentMigrations = previousApply }()
	defer func() { verifyEnvironmentMigrations = previousVerify }()
	defer func() { newEnvironmentWeComWorker = previousWorker }()

	graph, err := NewFromEnvironment(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if graph.OutboxWorker == nil || graph.wecomLifecycle == nil {
		_ = graph.Close()
		t.Fatal("WeCom environment did not install callback and outbox components")
	}
	if workerConfig.AuditWriter == nil {
		_ = graph.Close()
		t.Fatal("WeCom environment outbox worker did not receive an audit writer")
	}
	callback := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(callback, httptest.NewRequest(http.MethodPost, "/wecom/callback/environment-route", nil))
	if callback.Code != http.StatusForbidden {
		_ = graph.Close()
		t.Fatalf("WeCom environment callback status = %d", callback.Code)
	}
	if err := graph.OutboxWorker.Start(context.Background(), time.Second); !errors.Is(err, outbox.ErrAlreadyRunning) {
		_ = graph.Close()
		t.Fatalf("environment outbox worker was not started: %v", err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewFromEnvironmentCleansUpWhenWeComWorkerSetupFails(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envWeComCallbackToken, "callback-token")
	t.Setenv(envWeComEncodingAESKey, base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)))
	t.Setenv(envWeComAppSecret, "app-secret")
	t.Setenv(envWeComSecretRef, "env/wecom")

	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	previousOwner := environmentWeComOwnerFunc
	previousWorker := newEnvironmentWeComWorker
	defer func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		verifyEnvironmentMigrations = previousVerify
		environmentWeComOwnerFunc = previousOwner
		newEnvironmentWeComWorker = previousWorker
	}()

	tests := []struct {
		name      string
		ownerErr  error
		workerErr error
	}{
		{name: "owner", ownerErr: errors.New("owner unavailable")},
		{name: "worker", workerErr: errors.New("worker unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := sql.Open("trpc-service-bootstrap-ping", "")
			if err != nil {
				t.Fatal(err)
			}
			openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
			applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			if tt.ownerErr != nil {
				environmentWeComOwnerFunc = func() (string, error) { return "", tt.ownerErr }
			} else {
				environmentWeComOwnerFunc = func() (string, error) { return "test-owner", nil }
			}
			if tt.workerErr != nil {
				newEnvironmentWeComWorker = func(outbox.Config) (*outbox.Worker, error) { return nil, tt.workerErr }
			} else {
				newEnvironmentWeComWorker = outbox.New
			}

			if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("setup error = %v", err)
			}
			if err := db.Ping(); err == nil {
				t.Fatal("database was not closed after WeCom setup failure")
			}
		})
	}
}

var registerBootstrapPingDriver sync.Once

func TestNewUsesDatabasePingAndBuildsPostgreSQLRepositories(t *testing.T) {
	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	config, closeDependencies := testConfig(t)
	config.DB, config.OwnDB = db, true
	config.CloseDependencies = func() error {
		closeDependencies()
		return nil
	}
	config.Tenants, config.Apps, config.Models, config.Backends, config.Channels = nil, nil, nil, nil, nil
	graph, err := New(context.Background(), config)
	if err != nil {
		_ = db.Close()
		closeDependencies()
		t.Fatal(err)
	}
	if !graph.Ready() {
		t.Fatal("database-backed bootstrap graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

type bootstrapPingDriver struct{}

func (bootstrapPingDriver) Open(string) (driver.Conn, error) { return bootstrapPingConn{}, nil }

type bootstrapPingConn struct{}

func (bootstrapPingConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (bootstrapPingConn) Close() error                        { return nil }
func (bootstrapPingConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (bootstrapPingConn) Ping(context.Context) error          { return nil }

func testConfig(t *testing.T) (Config, func()) {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "test", Models: []string{"test-model"},
		EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "test", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{},
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	sessions := inmemory.NewSessionService()
	config := Config{
		Tenants: tenantmemory.NewRepository(), Apps: agentmemory.NewRepository(),
		Models: modelmemory.NewRepository(modelCatalog), Backends: backendmemory.NewRepository(backendCatalog),
		Channels: channelmemory.NewRepository(), ModelCatalog: modelCatalog, BackendCatalog: backendCatalog,
		SecretResolver: testSecretResolver{}, ModelFactory: testModelFactory{}, Sessions: sessions,
		Authenticator: authenticator,
	}
	return config, func() { _ = sessions.Close() }
}

type testSecretResolver struct{}

func (testSecretResolver) Resolve(context.Context, modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	return modelprofile.SecretValue{}, errors.New("test resolver failure")
}

type testModelFactory struct{}

func (testModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (trpcmodel.Model, error) {
	return nil, errors.New("test factory failure")
}

type bootstrapBlockingProvider struct {
	started  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

type candidateOnly struct{ channels.CandidateConsumer }

func createBootstrapTenantExecutionState(
	t *testing.T,
	tenants *tenantmemory.InMemoryRepository,
	apps *agentmemory.InMemoryRepository,
	models *modelmemory.InMemoryRepository,
	backends *backendmemory.InMemoryRepository,
	tenantKey, appKey, modelName, secretRef string,
) (*tenant.Tenant, *agent.App) {
	t.Helper()
	root, err := tenants.Create(context.Background(), tenant.CreateInput{TenantKey: tenantKey, DisplayName: tenantKey, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	profile, _, err := models.Create(context.Background(), modelprofile.CreateInput{
		TenantID: root.TenantID, ProfileKey: "model-" + tenantKey, DisplayName: "Model " + tenantKey,
		Configuration: modelprofile.Configuration{Provider: "fake", Model: modelName, SecretRef: secretRef},
		Metadata:      modelprofile.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: tenantKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	profileBackend, _, err := backends.Create(context.Background(), backend.CreateInput{
		TenantID: root.TenantID, ProfileKey: "backend-" + tenantKey, DisplayName: "Backend " + tenantKey,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "memory", Options: map[string]string{"namespace": tenantKey}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: tenantKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	app, err := apps.Create(context.Background(), agent.CreateInput{TenantID: root.TenantID, AppKey: appKey, DisplayName: appKey})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(context.Background(), agent.CreateDraftInput{
		TenantID: root.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Configuration: agent.DraftConfiguration{Instruction: "answer", ModelProfileID: profile.ProfileID, Runtime: agent.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	published, _, _, err := apps.Publish(context.Background(), agent.PublishInput{
		TenantID: root.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: tenantKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	appID, backendID := published.AppID, profileBackend.ProfileID
	updated, err := tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName, AuditRetentionDays: root.AuditRetentionDays,
		LogMaskingLevel: root.LogMaskingLevel, TraceSamplingRate: root.TraceSamplingRate, DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return updated, published
}

type bootstrapRecordingModelFactory struct {
	mu      sync.Mutex
	secrets map[string]string
}

func (factory *bootstrapRecordingModelFactory) New(_ context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.secrets == nil {
		factory.secrets = make(map[string]string)
	}
	factory.secrets[input.TenantID] = secret.Value()
	return bootstrapTestModel{}, nil
}

func (factory *bootstrapRecordingModelFactory) Secret(tenantID string) string {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.secrets[tenantID]
}

type bootstrapTestModel struct{}

func (bootstrapTestModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "bootstrap-test"} }
func (bootstrapTestModel) GenerateContent(context.Context, *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	return nil, errors.New("unused bootstrap test model")
}

type bootstrapRecordingStorageFactory struct {
	mu       sync.Mutex
	sessions map[string]int
}

func (factory *bootstrapRecordingStorageFactory) New(_ context.Context, input backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
	factory.mu.Lock()
	if factory.sessions == nil {
		factory.sessions = make(map[string]int)
	}
	factory.sessions[input.TenantID]++
	factory.mu.Unlock()
	return backend.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: inmemory.NewSessionService()})
}

func (factory *bootstrapRecordingStorageFactory) SessionCount(tenantID string) int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.sessions[tenantID]
}

type bootstrapWeComLifecycle struct {
	calls         atomic.Int32
	beginShutdown atomic.Int32
	closed        atomic.Int32
}

func (handler *bootstrapWeComLifecycle) ServeHTTP(writer http.ResponseWriter, _ *http.Request) {
	handler.calls.Add(1)
	writer.WriteHeader(http.StatusNoContent)
}
func (handler *bootstrapWeComLifecycle) BeginShutdown() { handler.beginShutdown.Add(1) }
func (handler *bootstrapWeComLifecycle) Close() error {
	handler.closed.Add(1)
	return nil
}

func (p *bootstrapBlockingProvider) Deliver(ctx context.Context, _ runtimestorage.ReplyOutbox) (string, error) {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	close(p.canceled)
	return "", ctx.Err()
}

func (*bootstrapBlockingProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryUnknown, "", nil
}

var (
	_ tenant.Repository          = (*tenantmemory.InMemoryRepository)(nil)
	_ agent.Repository           = (*agentmemory.InMemoryRepository)(nil)
	_ channels.CandidateConsumer = (*channelmemory.InMemoryRepository)(nil)
	_ session.Service            = (*inmemory.SessionService)(nil)
)

package bootstrap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
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

func nilContextForTest() context.Context { return nil }

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

var (
	_ tenant.Repository          = (*tenantmemory.InMemoryRepository)(nil)
	_ agent.Repository           = (*agentmemory.InMemoryRepository)(nil)
	_ channels.CandidateConsumer = (*channelmemory.InMemoryRepository)(nil)
	_ session.Service            = (*inmemory.SessionService)(nil)
)

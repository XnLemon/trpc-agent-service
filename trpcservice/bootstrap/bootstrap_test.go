package bootstrap

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
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

// Package bootstrap assembles the durable control-plane and the real Gateway
// execution spine. It keeps ownership explicit so tests can inject fakes and
// production callers can decide which resources to close.
package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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

var (
	// ErrInvalidConfig is returned when an explicit bootstrap dependency is
	// missing or cannot be assembled.
	ErrInvalidConfig = errors.New("invalid bootstrap configuration")
	// ErrBootstrapNotReady is used by the deliberately unconfigured local
	// server returned by NewUnavailable.
	ErrBootstrapNotReady = errors.New("bootstrap dependencies are not configured")
)

// Config is the complete, explicit dependency boundary for one process.
// Repositories are optional only when DB is supplied; they are then built as
// PostgreSQL implementations. All other runtime dependencies are required so
// a successful bootstrap always creates a real Resolver, Registry and HTTP
// handler.
type Config struct {
	DB       *sql.DB
	OwnDB    bool
	Tenants  tenant.Repository
	Apps     agent.Repository
	Models   modelprofile.Repository
	Backends backend.Repository
	Channels channels.CandidateConsumer

	ModelCatalog   *modelprofile.ProviderCatalog
	BackendCatalog *backend.ProviderCatalog
	SecretResolver modelprofile.SecretResolver
	ModelFactory   modelprofile.ModelFactory
	Sessions       session.Service
	Authenticator  gateway.APIAuthenticator

	Registry          gateway.RunnerRegistryConfig
	HTTP              gateway.HTTPConfig
	DrainTimeout      time.Duration
	ReadyGate         func() bool
	Ping              func(context.Context) error
	CloseDependencies func() error
}

// Runtime is the owned bootstrap graph. Handler.BeginShutdown should happen
// before the HTTP server is drained; Close then closes the Runner Registry and
// only after that resources explicitly owned by this graph.
type Runtime struct {
	Handler    *gateway.HTTPHandler
	Resolver   *gateway.PlanResolver
	Registry   *gateway.RunnerRegistry
	Dispatcher *gateway.Dispatcher

	db        *sql.DB
	ownDB     bool
	readyGate func() bool
	ping      func(context.Context) error
	closeDeps func() error
	closing   atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// NewWithDatabase is the normal constructor for a real PostgreSQL bootstrap.
// A concrete *sql.DB is accepted here while Config keeps the remainder of the
// dependency graph easy to construct in tests.
func NewWithDatabase(ctx context.Context, db *sql.DB, config Config) (*Runtime, error) {
	config.DB = db
	return New(ctx, config)
}

// New assembles the real PlanResolver, Runtime RunnerRegistry, Dispatcher and
// HTTPHandler from explicit dependencies.
func New(ctx context.Context, config Config) (*Runtime, error) {
	if ctx == nil {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.DB != nil {
		if err := config.DB.PingContext(ctx); err != nil {
			return nil, postgres.ErrStorage
		}
		if config.Tenants == nil {
			config.Tenants = postgres.NewTenantRepository(config.DB)
		}
		if config.Apps == nil {
			config.Apps = postgres.NewAgentRepository(config.DB)
		}
		if config.Models == nil {
			config.Models = postgres.NewModelRepository(config.DB, config.ModelCatalog)
		}
		if config.Backends == nil {
			config.Backends = postgres.NewBackendRepository(config.DB, config.BackendCatalog)
		}
		if config.Channels == nil {
			config.Channels = postgres.NewChannelRepository(config.DB)
		}
	}
	if config.Tenants == nil || config.Apps == nil || config.Models == nil || config.Backends == nil || config.Channels == nil || config.ModelCatalog == nil || config.BackendCatalog == nil || config.SecretResolver == nil || config.ModelFactory == nil || config.Sessions == nil || config.Authenticator == nil {
		return nil, ErrInvalidConfig
	}
	resolver, err := gateway.NewPlanResolver(gateway.PlanResolverConfig{
		Tenants: config.Tenants, Apps: config.Apps, Models: config.Models, Backends: config.Backends,
		ModelCatalog: config.ModelCatalog, BackendCatalog: config.BackendCatalog,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	registry, err := gateway.NewRuntimeRunnerRegistry(gateway.RuntimeRunnerRegistryConfig{
		Registry: config.Registry, SecretResolver: config.SecretResolver,
		ModelFactory: config.ModelFactory, Sessions: config.Sessions,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	dispatcher, err := gateway.NewDispatcher(gateway.DispatchConfig{
		Resolver: resolver, Registry: registry, DrainTimeout: config.DrainTimeout,
	})
	if err != nil {
		_ = registry.Close()
		return nil, ErrInvalidConfig
	}
	readyGate := config.ReadyGate
	if readyGate == nil {
		readyGate = func() bool { return true }
	}
	ping := config.Ping
	if ping == nil && config.DB != nil {
		ping = config.DB.PingContext
	}
	runtimeGraph := &Runtime{
		Resolver: resolver, Registry: registry, Dispatcher: dispatcher,
		db: config.DB, ownDB: config.OwnDB, readyGate: readyGate,
		ping: ping, closeDeps: config.CloseDependencies,
	}
	httpConfig := gateway.HTTPConfig{
		Dispatcher: dispatcher, Authenticator: config.Authenticator,
		Ready:   runtimeGraph.Ready,
		Limiter: config.HTTP.Limiter, Idempotency: config.HTTP.Idempotency,
		MaxBodyBytes: config.HTTP.MaxBodyBytes, RequestTimeout: config.HTTP.RequestTimeout,
	}
	runtimeGraph.Handler, err = gateway.NewHTTPHandler(httpConfig)
	if err != nil {
		_ = registry.Close()
		return nil, ErrInvalidConfig
	}
	return runtimeGraph, nil
}

// Ready is the single readiness gate used by HTTPHandler. It checks the
// database on demand (when present) and never returns true after shutdown has
// started.
func (graph *Runtime) Ready() bool {
	if graph == nil || graph.closing.Load() || graph.readyGate == nil || !graph.readyGate() {
		return false
	}
	if graph.ping != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		err := graph.ping(ctx)
		cancel()
		if err != nil {
			return false
		}
	}
	return graph.Resolver != nil && graph.Resolver.Ready() && graph.Registry != nil && graph.Registry.Ready() && graph.Dispatcher != nil && graph.Dispatcher.Ready() && graph.Handler != nil
}

// BeginShutdown immediately removes the graph from readiness and stops new
// HTTP admissions. The caller should then drain its net/http.Server.
func (graph *Runtime) BeginShutdown() {
	if graph == nil {
		return
	}
	graph.closing.Store(true)
	if graph.Handler != nil {
		graph.Handler.BeginShutdown()
	}
}

// Close performs bounded Registry shutdown, then closes explicitly owned
// dependencies. Borrowed repositories, sessions and factories are untouched.
func (graph *Runtime) Close() error {
	if graph == nil {
		return nil
	}
	graph.closeOnce.Do(func() {
		graph.BeginShutdown()
		var closeErr error
		if graph.Handler != nil {
			closeErr = errors.Join(closeErr, graph.Handler.Close())
		}
		if graph.Registry != nil {
			closeErr = errors.Join(closeErr, graph.Registry.Close())
		}
		if graph.closeDeps != nil {
			closeErr = errors.Join(closeErr, graph.closeDeps())
		}
		if graph.ownDB && graph.db != nil {
			closeErr = errors.Join(closeErr, graph.db.Close())
		}
		graph.closeErr = closeErr
	})
	return graph.closeErr
}

// Handler returns the real HTTP handler assembled by New.
func (graph *Runtime) HandlerValue() *gateway.HTTPHandler {
	if graph == nil {
		return nil
	}
	return graph.Handler
}

// NewUnavailable builds the command's deliberately unconfigured local mode.
// It still constructs the real resolver/registry/dispatcher graph; the
// explicit readiness gate remains false until a caller supplies durable
// control-plane configuration through Config.
func NewUnavailable() (*Runtime, error) {
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "unconfigured", Models: []string{"unconfigured"},
		EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldForbidden,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "unconfigured", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{},
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	sessions := inmemory.NewSessionService()
	config := Config{
		Tenants: tenantmemory.NewRepository(), Apps: agentmemory.NewRepository(),
		Models: modelmemory.NewRepository(modelCatalog), Backends: backendmemory.NewRepository(backendCatalog),
		Channels: channelmemory.NewRepository(), ModelCatalog: modelCatalog, BackendCatalog: backendCatalog,
		SecretResolver: unavailableSecretResolver{}, ModelFactory: unavailableModelFactory{},
		Sessions: sessions, Authenticator: authenticator,
		ReadyGate: func() bool { return false }, CloseDependencies: sessions.Close,
	}
	return New(context.Background(), config)
}

type unavailableSecretResolver struct{}

func (unavailableSecretResolver) Resolve(context.Context, modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	return modelprofile.SecretValue{}, ErrBootstrapNotReady
}

type unavailableModelFactory struct{}

func (unavailableModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (trpcmodel.Model, error) {
	return nil, ErrBootstrapNotReady
}

// NewHTTPServer is a small explicit server helper for command wiring.
func NewHTTPServer(graph *Runtime, address string, readHeaderTimeout, readTimeout, writeTimeout, idleTimeout time.Duration) (*http.Server, error) {
	if graph == nil || graph.HandlerValue() == nil || address == "" {
		return nil, ErrInvalidConfig
	}
	return &http.Server{Addr: address, Handler: graph.HandlerValue().Handler(), ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, WriteTimeout: writeTimeout, IdleTimeout: idleTimeout}, nil
}

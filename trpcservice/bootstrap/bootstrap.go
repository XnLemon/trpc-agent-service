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

	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	agentpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/agent/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	backendpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/backend/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimesessionpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/sessionpostgres"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
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
	StorageFactory backend.StorageFactory
	Sessions       session.Service
	// RuntimeStore is the tenant-scoped Session/Event/Outbox capability. It is
	// separate from upstream session.Service while the runtime adapter evolves.
	RuntimeStore runtimestorage.RuntimeStore
	// RuntimeTenantID fixes the tenant scope when Bootstrap wraps Sessions with
	// the RuntimeStore-backed capability. It must come from trusted config.
	RuntimeTenantID string
	// OutboxWorker is constructed from trusted provider routing configuration.
	// Bootstrap owns its lifecycle but never derives a recipient from HTTP.
	OutboxWorker       *outbox.Worker
	OutboxPollInterval time.Duration
	// AuditWriter receives execution and configured channel delivery facts.
	AuditWriter        audit.Writer
	Authenticator      gateway.APIAuthenticator
	AdminAuthenticator admin.Authenticator
	AdminHandler       http.Handler
	WeComHandler       http.Handler
	// WeComHandlerFactory is called after Dispatcher construction so a callback
	// handler cannot receive an uninitialized execution dependency.
	WeComHandlerFactory func(gateway.DispatchService) (http.Handler, error)

	Registry          gateway.RunnerRegistryConfig
	HTTP              gateway.HTTPConfig
	DrainTimeout      time.Duration
	ReadyGate         func() bool
	Ping              func(context.Context) error
	Migrate           func(context.Context, *sql.DB) error
	VerifyMigrations  func(context.Context, *sql.DB) error
	CloseDependencies func() error
}

// Runtime is the owned bootstrap graph. Handler.BeginShutdown should happen
// before the HTTP server is drained; Close then closes the Runner Registry and
// only after that resources explicitly owned by this graph.
type Runtime struct {
	Handler        *gateway.HTTPHandler
	Resolver       *gateway.PlanResolver
	Registry       *gateway.RunnerRegistry
	Dispatcher     *gateway.Dispatcher
	OutboxWorker   *outbox.Worker
	wecomLifecycle callbackLifecycle
	wecomHandler   http.Handler

	db               *sql.DB
	ownDB            bool
	readyGate        func() bool
	ping             func(context.Context) error
	verifyMigrations func(context.Context, *sql.DB) error
	closeDeps        func() error
	closing          atomic.Bool
	closeOnce        sync.Once
	closeErr         error
}

type callbackLifecycle interface {
	BeginShutdown()
	Close() error
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
	if err := prepareDatabaseConfig(ctx, &config); err != nil {
		return nil, err
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := prepareRuntimeConfig(&config); err != nil {
		return nil, err
	}
	runtimeGraph, err := newRuntimeGraph(config)
	if err != nil {
		return nil, err
	}
	if err := configureAdmin(&config, runtimeGraph.Registry); err != nil {
		return nil, err
	}
	if err := configureHandler(runtimeGraph, config); err != nil {
		return nil, err
	}
	if err := startOutboxWorker(runtimeGraph, config.OutboxPollInterval); err != nil {
		return nil, err
	}
	return runtimeGraph, nil
}

func prepareDatabaseConfig(ctx context.Context, config *Config) error {
	if config.DB == nil {
		return nil
	}
	if err := config.DB.PingContext(ctx); err != nil {
		return postgres.ErrStorage
	}
	if config.Migrate != nil {
		if err := config.Migrate(ctx, config.DB); err != nil {
			return ErrInvalidConfig
		}
	}
	if config.Tenants == nil {
		config.Tenants = tenantpostgres.NewRepository(config.DB)
	}
	if config.Apps == nil {
		config.Apps = agentpostgres.NewRepository(config.DB)
	}
	if config.Models == nil {
		config.Models = modelpostgres.NewRepository(config.DB, config.ModelCatalog)
	}
	if config.Backends == nil {
		config.Backends = backendpostgres.NewRepository(config.DB, config.BackendCatalog)
	}
	if config.Channels == nil {
		config.Channels = channelpostgres.NewRepository(config.DB)
	}
	return nil
}

func validateConfig(config Config) error {
	dependencies := []any{
		config.Tenants, config.Apps, config.Models, config.Backends, config.Channels,
		config.ModelCatalog, config.BackendCatalog, config.SecretResolver, config.ModelFactory,
		config.Authenticator,
	}
	for _, dependency := range dependencies {
		if dependency == nil {
			return ErrInvalidConfig
		}
	}
	if config.Sessions == nil && config.StorageFactory == nil {
		return ErrInvalidConfig
	}
	return nil
}

func prepareRuntimeConfig(config *Config) error {
	if config.RuntimeStore == nil {
		config.RuntimeStore = runtimestorageinmemory.New()
	}
	if config.RuntimeTenantID == "" {
		return nil
	}
	wrapped, err := runtimesessionpostgres.New(config.RuntimeTenantID, config.Sessions, config.RuntimeStore)
	if err != nil {
		return ErrInvalidConfig
	}
	config.Sessions = wrapped
	return nil
}

func newRuntimeGraph(config Config) (*Runtime, error) {
	resolver, err := gateway.NewPlanResolver(gateway.PlanResolverConfig{
		Tenants: config.Tenants, Apps: config.Apps, Models: config.Models, Backends: config.Backends,
		ModelCatalog: config.ModelCatalog, BackendCatalog: config.BackendCatalog,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	registry, err := gateway.NewRuntimeRunnerRegistry(gateway.RuntimeRunnerRegistryConfig{
		Registry: config.Registry, SecretResolver: config.SecretResolver,
		ModelFactory: config.ModelFactory, Sessions: config.Sessions, StorageFactory: config.StorageFactory,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	dispatcher, err := gateway.NewDispatcher(gateway.DispatchConfig{
		Resolver: resolver, Registry: registry, RuntimeStore: config.RuntimeStore, DrainTimeout: config.DrainTimeout, AuditWriter: config.AuditWriter,
	})
	if err != nil {
		_ = registry.Close()
		return nil, ErrInvalidConfig
	}
	if config.WeComHandler != nil && config.WeComHandlerFactory != nil {
		_ = registry.Close()
		return nil, ErrInvalidConfig
	}
	if config.WeComHandlerFactory != nil {
		config.WeComHandler, err = config.WeComHandlerFactory(dispatcher)
		if err != nil || config.WeComHandler == nil {
			_ = registry.Close()
			return nil, ErrInvalidConfig
		}
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
		OutboxWorker: config.OutboxWorker,
		wecomHandler: config.WeComHandler,
		db:           config.DB, ownDB: config.OwnDB, readyGate: readyGate,
		ping: ping, verifyMigrations: config.VerifyMigrations, closeDeps: config.CloseDependencies,
	}
	if lifecycle, ok := config.WeComHandler.(callbackLifecycle); ok {
		runtimeGraph.wecomLifecycle = lifecycle
	}
	return runtimeGraph, nil
}

func configureAdmin(config *Config, registry *gateway.RunnerRegistry) error {
	if config.AdminAuthenticator == nil {
		return nil
	}
	bindingRepository, ok := config.Channels.(channels.Repository)
	if !ok {
		_ = registry.Close()
		return ErrInvalidConfig
	}
	adminHandler, err := admin.NewHandler(admin.Config{
		Tenants: config.Tenants, Apps: config.Apps, Models: config.Models,
		Backends: config.Backends, Bindings: bindingRepository,
		Authenticator: config.AdminAuthenticator,
		ModelCatalog:  config.ModelCatalog, BackendCatalog: config.BackendCatalog,
		CacheInvalidator: admin.CacheInvalidatorFunc(func(change admin.CacheInvalidation) {
			invalidateRuntimeCache(registry, change)
		}),
	})
	if err != nil {
		_ = registry.Close()
		return ErrInvalidConfig
	}
	config.AdminHandler = adminHandler
	return nil
}

func invalidateRuntimeCache(registry *gateway.RunnerRegistry, change admin.CacheInvalidation) {
	// A closed registry cannot admit a future execution. Other errors are
	// impossible for Admin-derived non-empty IDs, so a committed control-
	// plane mutation remains successful during shutdown.
	switch change.Kind {
	case admin.CacheInvalidationTenant:
		_ = registry.InvalidateTenant(change.TenantID)
	case admin.CacheInvalidationApp:
		_ = registry.InvalidateApp(change.TenantID, change.AppID)
	case admin.CacheInvalidationModel:
		_ = registry.InvalidateModelProfile(change.TenantID, change.ProfileID)
	case admin.CacheInvalidationBackend:
		_ = registry.InvalidateBackendProfile(change.TenantID, change.ProfileID)
	case admin.CacheInvalidationBinding:
		// Bindings are resolved and verified on every channel request. They
		// do not key a Runner or provider cache in this process.
	}
}

func configureHandler(runtimeGraph *Runtime, config Config) error {
	handler, err := gateway.NewHTTPHandler(gateway.HTTPConfig{
		Dispatcher: runtimeGraph.Dispatcher, Authenticator: config.Authenticator, Admin: config.AdminHandler, WeCom: runtimeGraph.wecomHandler,
		Ready: runtimeGraph.Ready, Limiter: config.HTTP.Limiter, Idempotency: config.HTTP.Idempotency,
		MaxBodyBytes: config.HTTP.MaxBodyBytes, RequestTimeout: config.HTTP.RequestTimeout,
	})
	if err != nil {
		_ = runtimeGraph.Registry.Close()
		return ErrInvalidConfig
	}
	runtimeGraph.Handler = handler
	return nil
}

func startOutboxWorker(runtimeGraph *Runtime, pollInterval time.Duration) error {
	if runtimeGraph.OutboxWorker == nil {
		return nil
	}
	if err := runtimeGraph.OutboxWorker.Start(context.Background(), pollInterval); err != nil {
		_ = runtimeGraph.Handler.Close()
		_ = runtimeGraph.Registry.Close()
		return ErrInvalidConfig
	}
	return nil
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
	if graph.verifyMigrations != nil && graph.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		err := graph.verifyMigrations(ctx, graph.db)
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
	if graph.wecomLifecycle != nil {
		graph.wecomLifecycle.BeginShutdown()
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
		if graph.wecomLifecycle != nil {
			closeErr = errors.Join(closeErr, graph.wecomLifecycle.Close())
		}
		if graph.Registry != nil {
			closeErr = errors.Join(closeErr, graph.Registry.Close())
		}
		if graph.OutboxWorker != nil {
			closeErr = errors.Join(closeErr, graph.OutboxWorker.Close())
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

// HandlerValue returns the real HTTP handler assembled by New.
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

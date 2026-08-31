package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentmysql "github.com/XnLemon/trpc-agent-service/trpcservice/agent/mysql"
	agentpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/agent/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	auditpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/audit/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimesessionpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/sessionpostgres"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	envControlPlaneDriver = "TRPC_CONTROL_PLANE_DRIVER"
	envPostgresDSN        = "TRPC_POSTGRES_DSN"
	envMySQLDSN           = "TRPC_MYSQL_DSN"
	envMySQLMigrationDSN  = "TRPC_MYSQL_MIGRATION_DSN"
	// #nosec G101 -- environment variable name, not a credential.
	envAPIToken      = "TRPC_API_TOKEN"
	envAPIIdentities = "TRPC_API_IDENTITIES"
	envTenantID      = "TRPC_TENANT_ID"
	envAppID         = "TRPC_APP_ID"
	// #nosec G101 -- environment variable name, not a credential.
	envAdminToken   = "TRPC_ADMIN_TOKEN"
	envAdminTenants = "TRPC_ADMIN_TENANTS"
	envSubjectID    = "TRPC_SUBJECT_ID"
	// #nosec G101 -- environment variable name, not a credential.
	envModelAPIKey = "TRPC_MODEL_API_KEY"
	// #nosec G101 -- environment variable name, not a credential.
	envModelAPIKeys      = "TRPC_MODEL_API_KEYS"
	envModelProvider     = "TRPC_MODEL_PROVIDER"
	envModelNames        = "TRPC_MODEL_NAMES"
	envModelEndpointHost = "TRPC_MODEL_ENDPOINT_HOSTS"
	// #nosec G101 -- environment variable name, not a secret.
	envModelSecretRef = "TRPC_MODEL_SECRET_REF"
	envSessionBackend = "TRPC_SESSION_BACKEND"
	envDemoMode       = "TRPC_DEMO_MODE"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComCallbackToken  = "WECOM_CALLBACK_TOKEN"
	envWeComEncodingAESKey = "WECOM_ENCODING_AES_KEY"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComAppSecret = "WECOM_APP_SECRET"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComSecretRef  = "WECOM_SECRET_REF"
	envOTLPEndpoint    = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPHeaders     = "OTEL_EXPORTER_OTLP_HEADERS"
	envOTLPInsecure    = "OTEL_EXPORTER_OTLP_INSECURE"
	envOTELServiceName = "OTEL_SERVICE_NAME"

	defaultModelProvider = "openai"
	defaultModelNames    = "gpt-4o-mini"
	defaultEndpointHost  = "api.openai.com"
	demoModelProvider    = "fake"
	demoModelName        = "deterministic"
	// #nosec G101 -- symbolic secret reference, not secret material.
	defaultModelSecretRef = "env/trpc-model-api-key"
	defaultSubjectID      = "service"
)

var (
	openEnvironmentDatabase          = postgres.Open
	openMySQLEnvironmentDatabase     = mysql.Open
	applyEnvironmentMigrations       = migrations.Apply
	applyMySQLEnvironmentMigrations  = migrations.ApplyMySQL
	verifyEnvironmentMigrations      = migrations.Verify
	verifyMySQLEnvironmentMigrations = migrations.VerifyMySQL
	newEnvironmentRuntimeStore       = environmentRuntimeStore
	environmentWeComOwnerFunc        = environmentWeComOwner
	newEnvironmentWeComWorker        = outbox.New
)

// environmentConfig is intentionally private: it contains the one secret
// handed to the ModelFactory and must not become a serializable application
// configuration object.
type environmentConfig struct {
	driver         ControlPlaneDriver
	dsn            string
	migrationDSN   string
	apiToken       string
	apiIdentities  map[string]gateway.APIIdentity
	adminToken     string
	adminTenants   []string
	tenantID       string
	appID          string
	subjectID      string
	modelAPIKey    string
	modelAPIKeys   map[string]string
	modelProvider  string
	modelNames     []string
	endpointHosts  []string
	secretRef      string
	runtimeStorage string
	demoMode       bool
	wecom          *environmentWeComConfig
	telemetry      observability.Provider
	otlp           observability.OTLPConfig
}

type environmentWeComConfig struct {
	callbackToken  string
	encodingAESKey string
	appSecret      string
	secretRef      string
}

// NewFromEnvironment assembles the production bootstrap graph from explicit
// process configuration. It fails before binding an HTTP server when the
// durable control plane or required credentials are not configured.
func NewFromEnvironment(ctx context.Context) (*Runtime, error) {
	if ctx == nil {
		return nil, ErrInvalidConfig
	}
	config, err := loadEnvironment()
	if err != nil {
		return nil, err
	}
	telemetry, err := observability.NewOTLPProvider(ctx, config.otlp)
	if err != nil {
		return nil, fmt.Errorf("%w: telemetry exporter configuration is invalid", ErrInvalidConfig)
	}
	config.telemetry = telemetry
	telemetryOwned := true
	defer func() {
		if telemetryOwned {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = telemetry.Shutdown(shutdownCtx)
			cancel()
		}
	}()
	modelCatalog, backendCatalog, err := environmentCatalogs(config)
	if err != nil {
		return nil, err
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(config.apiIdentities)
	if err != nil {
		return nil, fmt.Errorf("%w: API authenticator configuration is invalid", ErrInvalidConfig)
	}
	adminAuthenticator, err := admin.NewStaticAuthenticator(config.adminToken, config.adminTenants)
	if err != nil {
		return nil, fmt.Errorf("%w: Admin authenticator configuration is invalid", ErrInvalidConfig)
	}
	db, applyMigrations, verifyMigrations, err := openEnvironmentDatabaseForConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	delegateSessions := inmemory.NewSessionService()
	runtimeStore, err := newEnvironmentRuntimeStore(config.runtimeStorage, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = db.Close()
		return nil, err
	}
	tenantRepo, appRepo, channelRepo, auditWriter, err := environmentRepositories(config, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStore.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: environment repositories: %v", ErrInvalidConfig, err)
	}
	auditWriter = metrics.WrapAuditWriter(auditWriter, config.telemetry)
	wecomFactory, wecomWorker, err := environmentWeComComponents(config, channelRepo, tenantRepo, appRepo, runtimeStore, auditWriter)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStore.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: wecom components: %v", ErrInvalidConfig, err)
	}
	secretRegistry, modelRegistry, backendRegistry, err := environmentRegistries(config, delegateSessions, runtimeStore)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStore.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: environment registries: %v", ErrInvalidConfig, err)
	}
	storageFactory, err := backend.NewRegistryStorageFactory(backendRegistry, secretRegistry)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStore.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: storage factory: %v", ErrInvalidConfig, err)
	}
	graph, err := NewWithDatabase(ctx, db, Config{
		OwnDB:               true,
		ControlPlaneDriver:  config.driver,
		Observability:       config.telemetry,
		Tenants:             tenantRepo,
		Apps:                appRepo,
		Channels:            channelRepo,
		ModelCatalog:        modelCatalog,
		BackendCatalog:      backendCatalog,
		SecretResolver:      secretRegistry,
		ModelFactory:        modelRegistry,
		StorageFactory:      storageFactory,
		Sessions:            delegateSessions,
		RuntimeStore:        runtimeStore,
		RuntimeTenantID:     "",
		Authenticator:       authenticator,
		AdminAuthenticator:  adminAuthenticator,
		WeComHandlerFactory: wecomFactory,
		OutboxWorker:        wecomWorker,
		OutboxPollInterval:  time.Second,
		AuditWriter:         auditWriter,
		Ping: func(pingContext context.Context) error {
			if config.driver == ControlPlaneDriverMySQL {
				return mysql.Ping(pingContext, db)
			}
			return postgres.Ping(pingContext, db)
		},
		Migrate:          applyMigrations,
		VerifyMigrations: verifyMigrations,
		CloseDependencies: func() error {
			return errors.Join(delegateSessions.Close(), runtimeStore.Close())
		},
	})
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStore.Close()
		_ = db.Close()
		return nil, err
	}
	telemetryOwned = false
	return graph, nil
}

func openEnvironmentDatabaseForConfig(ctx context.Context, config environmentConfig) (*sql.DB, func(context.Context, *sql.DB) error, func(context.Context, *sql.DB) error, error) {
	if config.driver != ControlPlaneDriverMySQL {
		db, err := openEnvironmentDatabase(ctx, config.dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, nil, ctx.Err()
			}
			return nil, nil, nil, fmt.Errorf("%w: %s control plane is unavailable", ErrInvalidConfig, config.driver)
		}
		return db, applyEnvironmentMigrations, verifyEnvironmentMigrations, nil
	}
	migrationDB, migrationErr := openMySQLEnvironmentDatabase(ctx, config.migrationDSN, mysql.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	var migrationUser, migrationDatabase string
	if migrationErr == nil {
		migrationUser, migrationErr = mysql.CurrentUser(ctx, migrationDB)
	}
	if migrationErr == nil {
		migrationDatabase, migrationErr = mysql.CurrentDatabase(ctx, migrationDB)
	}
	if migrationErr == nil {
		migrationErr = applyMySQLEnvironmentMigrations(ctx, migrationDB)
	}
	if migrationErr == nil {
		migrationErr = verifyMySQLEnvironmentMigrations(ctx, migrationDB)
	}
	if migrationDB != nil {
		if closeErr := migrationDB.Close(); migrationErr == nil {
			migrationErr = closeErr
		}
	}
	if migrationErr != nil {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		return nil, nil, nil, fmt.Errorf("%w: MySQL migrations are not ready", ErrInvalidConfig)
	}
	db, err := openMySQLEnvironmentDatabase(ctx, config.dsn, mysql.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		return nil, nil, nil, fmt.Errorf("%w: mysql control plane is unavailable", ErrInvalidConfig)
	}
	applicationUser, userErr := mysql.CurrentUser(ctx, db)
	applicationDatabase, databaseErr := mysql.CurrentDatabase(ctx, db)
	if userErr != nil || databaseErr != nil || applicationUser == migrationUser || applicationDatabase != migrationDatabase {
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		return nil, nil, nil, fmt.Errorf("%w: MySQL migration and application accounts/databases are invalid", ErrInvalidConfig)
	}
	// The application account is verification-only during bootstrap; migrations
	// and trigger metadata are handled through the migration account above.
	return db, nil, nil, nil
}

func environmentRepositories(config environmentConfig, db *sql.DB) (tenant.Repository, agent.Repository, channels.CandidateConsumer, audit.Writer, error) {
	if config.driver == ControlPlaneDriverMySQL {
		return tenantmysql.NewRepository(db), agentmysql.NewRepository(db), channelmysql.NewRepository(db), nil, nil
	}
	tenantRepo := tenantpostgres.NewRepository(db)
	appRepo := agentpostgres.NewRepository(db)
	channelRepo := channelpostgres.NewRepository(db)
	var auditWriter audit.Writer
	var err error
	if len(config.apiIdentities) > 1 {
		auditWriter = auditpostgres.NewMultiTenant(db)
	} else {
		auditWriter, err = auditpostgres.New(db, config.tenantID)
	}
	return tenantRepo, appRepo, channelRepo, auditWriter, err
}

func environmentWeComComponents(config environmentConfig, channelsRepo channels.CandidateConsumer, tenantsRepo tenant.Repository, appsRepo agent.Repository, runtimeStore runtimestorage.RuntimeStore, auditWriter audit.Writer) (func(gateway.DispatchService) (http.Handler, error), *outbox.Worker, error) {
	if config.wecom == nil {
		return nil, nil, nil
	}
	credentials := environmentWeComCredentialResolver{tenantID: config.tenantID, config: *config.wecom}
	var attachments runtimestorage.AttachmentStore
	if store, ok := runtimeStore.(runtimestorage.AttachmentStore); ok {
		attachments = store
	}
	var mediaDownloader wecom.MediaDownloader
	if attachments != nil {
		mediaDownloader = &wecom.HTTPMediaDownloader{}
	}
	factory := func(dispatcher gateway.DispatchService) (http.Handler, error) {
		return wecom.New(wecom.Config{Candidates: channelsRepo, Tenants: tenantsRepo, Apps: appsRepo, Credentials: credentials, Dispatcher: dispatcher, Attachments: attachments, MediaDownloader: mediaDownloader, AuditWriter: auditWriter, Observability: config.telemetry})
	}
	owner, err := environmentWeComOwnerFunc()
	if err != nil {
		return nil, nil, err
	}
	worker, err := newEnvironmentWeComWorker(outbox.Config{Store: runtimeStore, Provider: &wecom.BindingProvider{Bindings: channelsRepo, Credentials: credentials, Attachments: attachments}, Channel: "wecom", ProviderName: "wecom", TenantID: config.tenantID, Owner: owner, LeaseDuration: 30 * time.Second, AuditWriter: auditWriter, Observability: config.telemetry})
	return factory, worker, err
}

func environmentRegistries(config environmentConfig, delegateSessions session.Service, runtimeStore runtimestorage.RuntimeStore) (*modelprofile.SecretRegistry, *modelprofile.ModelProviderRegistry, *backend.ProviderRegistry, error) {
	secretRegistry := modelprofile.NewSecretRegistry()
	modelRegistry := modelprofile.NewModelProviderRegistry()
	backendRegistry := backend.NewProviderRegistry()
	for _, identity := range config.apiIdentities {
		if config.demoMode {
			if err := modelRegistry.Register(identity.TenantID, demoModelProvider, environmentModelFactory{}); err != nil {
				return nil, nil, nil, err
			}
			for _, capability := range []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit} {
				provider := environmentRuntimeCapabilityProvider{capability: capability, delegate: delegateSessions, store: runtimeStore, telemetry: config.telemetry, backend: config.runtimeStorage}
				if err := backendRegistry.Register(identity.TenantID, capability, "inmemory", provider); err != nil {
					return nil, nil, nil, err
				}
			}
			continue
		}
		modelAPIKey := config.modelAPIKey
		if len(config.modelAPIKeys) != 0 {
			modelAPIKey = config.modelAPIKeys[identity.TenantID]
		}
		if modelAPIKey == "" {
			return nil, nil, nil, ErrInvalidConfig
		}
		if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.secretRef}, modelAPIKey); err != nil {
			return nil, nil, nil, err
		}
		if err := modelRegistry.Register(identity.TenantID, config.modelProvider, environmentModelFactory{}); err != nil {
			return nil, nil, nil, err
		}
		for _, capability := range []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit} {
			provider := environmentRuntimeCapabilityProvider{capability: capability, delegate: delegateSessions, store: runtimeStore, telemetry: config.telemetry, backend: config.runtimeStorage}
			if err := backendRegistry.Register(identity.TenantID, capability, "inmemory", provider); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	return secretRegistry, modelRegistry, backendRegistry, nil
}

func loadEnvironment() (environmentConfig, error) {
	demoMode, err := environmentBool(envDemoMode)
	if err != nil {
		return environmentConfig{}, err
	}
	config := environmentConfig{
		driver:         ControlPlaneDriver(strings.ToLower(strings.TrimSpace(environmentOrDefault(envControlPlaneDriver, string(ControlPlaneDriverPostgres))))),
		modelProvider:  environmentOrDefault(envModelProvider, defaultModelProvider),
		secretRef:      environmentOrDefault(envModelSecretRef, defaultModelSecretRef),
		subjectID:      environmentOrDefault(envSubjectID, defaultSubjectID),
		runtimeStorage: strings.ToLower(strings.TrimSpace(os.Getenv(envSessionBackend))),
		demoMode:       demoMode,
		telemetry:      observability.NewNoopProvider(),
	}
	loaders := []func() error{config.loadDatabase, config.loadIdentities, config.loadAdmin, config.loadModel, config.loadRuntime, config.loadWeCom}
	for _, load := range loaders {
		if err := load(); err != nil {
			return environmentConfig{}, err
		}
	}
	if err := config.loadTelemetry(); err != nil {
		return environmentConfig{}, err
	}
	return config, nil
}

func (config *environmentConfig) loadTelemetry() error {
	endpoint := strings.TrimSpace(os.Getenv(envOTLPEndpoint))
	serviceName := strings.TrimSpace(environmentOrDefault(envOTELServiceName, "trpc-agent-service"))
	if strings.ContainsAny(serviceName, "\r\n") || serviceName == "" {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envOTELServiceName)
	}
	headers, err := parseEnvironmentOTLPHeaders(os.Getenv(envOTLPHeaders))
	if err != nil {
		return err
	}
	insecure := false
	if value := strings.TrimSpace(os.Getenv(envOTLPInsecure)); value != "" {
		insecure, err = strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("%w: %s must be true or false", ErrInvalidConfig, envOTLPInsecure)
		}
	}
	config.otlp = observability.OTLPConfig{ServiceName: serviceName, Endpoint: endpoint, Headers: headers, Insecure: insecure}
	return nil
}

func parseEnvironmentOTLPHeaders(value string) (map[string]string, error) {
	if strings.ContainsAny(value, "\r\n") {
		return nil, fmt.Errorf("%w: %s contains an invalid entry", ErrInvalidConfig, envOTLPHeaders)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	result := make(map[string]string)
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		separator := strings.IndexByte(entry, '=')
		if separator <= 0 || separator == len(entry)-1 {
			return nil, fmt.Errorf("%w: %s entries must use key=value", ErrInvalidConfig, envOTLPHeaders)
		}
		key, headerValue := strings.TrimSpace(entry[:separator]), strings.TrimSpace(entry[separator+1:])
		if key == "" || headerValue == "" || strings.ContainsAny(key, "\r\n\t ") || strings.ContainsAny(headerValue, "\r\n") {
			return nil, fmt.Errorf("%w: %s contains an invalid entry", ErrInvalidConfig, envOTLPHeaders)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: %s contains duplicate keys", ErrInvalidConfig, envOTLPHeaders)
		}
		result[key] = headerValue
	}
	return result, nil
}

func (config *environmentConfig) loadDatabase() error {
	if config.driver != ControlPlaneDriverPostgres && config.driver != ControlPlaneDriverMySQL {
		return fmt.Errorf("%w: %s must be postgres or mysql", ErrInvalidConfig, envControlPlaneDriver)
	}
	dsnName := envPostgresDSN
	if config.driver == ControlPlaneDriverMySQL {
		dsnName = envMySQLDSN
	}
	dsn, err := requiredEnvironment(dsnName)
	if err != nil {
		return err
	}
	config.dsn = dsn
	if config.driver == ControlPlaneDriverMySQL {
		config.migrationDSN, err = requiredEnvironment(envMySQLMigrationDSN)
		if err != nil {
			return err
		}
	}
	return nil
}

func (config *environmentConfig) loadIdentities() error {
	identities := strings.TrimSpace(os.Getenv(envAPIIdentities))
	if identities != "" {
		var err error
		config.apiIdentities, err = parseEnvironmentAPIIdentities(identities)
		if err != nil {
			return err
		}
		if len(config.apiIdentities) == 1 {
			for _, identity := range config.apiIdentities {
				config.tenantID, config.appID = identity.TenantID, identity.AppID
			}
		}
		return nil
	}
	var err error
	if config.apiToken, err = requiredEnvironment(envAPIToken); err != nil {
		return err
	}
	if config.tenantID, err = requiredEnvironment(envTenantID); err != nil {
		return err
	}
	if config.appID, err = requiredEnvironment(envAppID); err != nil {
		return err
	}
	config.apiIdentities = map[string]gateway.APIIdentity{config.apiToken: {TenantID: config.tenantID, AppID: config.appID, SubjectID: config.subjectID}}
	return nil
}

func (config *environmentConfig) loadAdmin() error {
	var err error
	if config.adminToken, err = requiredEnvironment(envAdminToken); err != nil {
		return err
	}
	adminTenantValue, err := requiredEnvironment(envAdminTenants)
	if err != nil {
		return err
	}
	config.adminTenants, err = environmentList(envAdminTenants, adminTenantValue, false)
	return err
}

func (config *environmentConfig) loadModel() error {
	if config.demoMode {
		if config.modelProvider != demoModelProvider {
			return fmt.Errorf("%w: %s requires %s provider", ErrInvalidConfig, envDemoMode, demoModelProvider)
		}
		config.modelProvider = demoModelProvider
		config.secretRef = ""
		config.modelAPIKey = ""
		config.modelAPIKeys = nil
		var err error
		if config.modelNames, err = environmentList(envModelNames, environmentOrDefault(envModelNames, demoModelName), true); err != nil {
			return err
		}
		return nil
	}
	var err error
	if mapped := strings.TrimSpace(os.Getenv(envModelAPIKeys)); mapped != "" {
		config.modelAPIKeys, err = parseEnvironmentModelAPIKeys(mapped)
		if err != nil {
			return err
		}
		for _, identity := range config.apiIdentities {
			if config.modelAPIKeys[identity.TenantID] == "" {
				return fmt.Errorf("%w: %s has no key for tenant", ErrInvalidConfig, envModelAPIKeys)
			}
		}
	} else {
		if len(config.apiIdentities) > 1 {
			return fmt.Errorf("%w: %s is required for multi-tenant bootstrap", ErrInvalidConfig, envModelAPIKeys)
		}
		config.modelAPIKey = strings.TrimSpace(os.Getenv(envModelAPIKey))
		if config.modelAPIKey == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidConfig, envModelAPIKey)
		}
	}
	config.modelProvider = strings.ToLower(strings.TrimSpace(config.modelProvider))
	config.secretRef = strings.TrimSpace(config.secretRef)
	if config.modelProvider == "" || config.secretRef == "" {
		return fmt.Errorf("%w: model provider and secret reference are required", ErrInvalidConfig)
	}
	if config.modelNames, err = environmentList(envModelNames, environmentOrDefault(envModelNames, defaultModelNames), true); err != nil {
		return err
	}
	config.endpointHosts, err = environmentList(envModelEndpointHost, environmentOrDefault(envModelEndpointHost, defaultEndpointHost), true)
	return err
}

func parseEnvironmentModelAPIKeys(value string) (map[string]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("%w: %s is required", ErrInvalidConfig, envModelAPIKeys)
	}
	keys := make(map[string]string)
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" || strings.ContainsAny(item, "\r\n") {
			return nil, fmt.Errorf("%w: %s contains an empty entry", ErrInvalidConfig, envModelAPIKeys)
		}
		separator := strings.IndexByte(item, '=')
		if separator < 1 || separator == len(item)-1 {
			return nil, fmt.Errorf("%w: %s entries must be tenant_id=api_key", ErrInvalidConfig, envModelAPIKeys)
		}
		tenantID := strings.TrimSpace(item[:separator])
		apiKey := strings.TrimSpace(item[separator+1:])
		if tenantID == "" || strings.ContainsAny(tenantID, "\r\n") || apiKey == "" {
			return nil, fmt.Errorf("%w: %s contains an invalid tenant entry", ErrInvalidConfig, envModelAPIKeys)
		}
		if _, exists := keys[tenantID]; exists {
			return nil, fmt.Errorf("%w: %s contains duplicate tenant entries", ErrInvalidConfig, envModelAPIKeys)
		}
		keys[tenantID] = apiKey
	}
	return keys, nil
}

func (config *environmentConfig) loadRuntime() error {
	config.subjectID = strings.TrimSpace(config.subjectID)
	if config.runtimeStorage != "postgres" && config.runtimeStorage != "inmemory" {
		return fmt.Errorf("%w: %s must be explicitly set to postgres or inmemory", ErrInvalidConfig, envSessionBackend)
	}
	if config.demoMode && (config.driver != ControlPlaneDriverPostgres || config.runtimeStorage != "inmemory") {
		return fmt.Errorf("%w: %s requires PostgreSQL control plane and inmemory session backend", ErrInvalidConfig, envDemoMode)
	}
	if config.driver == ControlPlaneDriverMySQL && config.runtimeStorage == "postgres" {
		return fmt.Errorf("%w: %s=postgres is not available with MySQL control plane; use inmemory until a MySQL runtime adapter is selected", ErrInvalidConfig, envSessionBackend)
	}
	return nil
}

func (config *environmentConfig) loadWeCom() error {
	values := []string{strings.TrimSpace(os.Getenv(envWeComCallbackToken)), strings.TrimSpace(os.Getenv(envWeComEncodingAESKey)), strings.TrimSpace(os.Getenv(envWeComAppSecret)), strings.TrimSpace(os.Getenv(envWeComSecretRef))}
	configured := 0
	for _, value := range values {
		if value != "" {
			configured++
		}
		if config.demoMode && configured != 0 {
			return fmt.Errorf("%w: %s cannot be enabled in demo mode", ErrInvalidConfig, envDemoMode)
		}
	}
	if configured != 0 && configured != len(values) {
		return fmt.Errorf("%w: WeCom credentials must be configured together", ErrInvalidConfig)
	}
	if configured == len(values) {
		config.wecom = &environmentWeComConfig{callbackToken: values[0], encodingAESKey: values[1], appSecret: values[2], secretRef: values[3]}
	}
	if config.wecom != nil && len(config.apiIdentities) != 1 {
		return fmt.Errorf("%w: WeCom credentials require exactly one API identity", ErrInvalidConfig)
	}
	return nil
}

func environmentRuntimeStore(kind string, db *sql.DB) (runtimestorage.RuntimeStore, error) {
	switch kind {
	case "postgres":
		if db == nil {
			return nil, fmt.Errorf("%w: PostgreSQL runtime storage requires a database", ErrInvalidConfig)
		}
		return runtimestoragepostgres.New(db), nil
	case "inmemory":
		return runtimestorageinmemory.New(), nil
	default:
		return nil, fmt.Errorf("%w: unsupported runtime storage", ErrInvalidConfig)
	}
}

func environmentCatalogs(config environmentConfig) (*modelprofile.ProviderCatalog, *backend.ProviderCatalog, error) {
	if config.demoMode {
		if config.modelProvider != demoModelProvider {
			return nil, nil, fmt.Errorf("%w: model provider %q is unsupported in demo mode", ErrInvalidConfig, config.modelProvider)
		}
		modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
			Provider:        demoModelProvider,
			Models:          config.modelNames,
			EndpointPolicy:  modelprofile.FieldForbidden,
			SecretRefPolicy: modelprofile.FieldForbidden,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("%w: demo model catalog is invalid", ErrInvalidConfig)
		}
		backendCatalog, err := newEnvironmentBackendCatalog()
		if err != nil {
			return nil, nil, err
		}
		return modelCatalog, backendCatalog, nil
	}
	if config.modelProvider != defaultModelProvider {
		return nil, nil, fmt.Errorf("%w: model provider %q is unsupported", ErrInvalidConfig, config.modelProvider)
	}
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider:        config.modelProvider,
		Models:          config.modelNames,
		EndpointPolicy:  modelprofile.FieldOptional,
		EndpointSchemes: []string{"https"},
		EndpointHosts:   config.endpointHosts,
		SecretRefPolicy: modelprofile.FieldRequired,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: model catalog is invalid", ErrInvalidConfig)
	}
	backendCatalog, err := newEnvironmentBackendCatalog()
	if err != nil {
		return nil, nil, err
	}
	return modelCatalog, backendCatalog, nil
}

func newEnvironmentBackendCatalog() (*backend.ProviderCatalog, error) {
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider:        "inmemory",
		Capabilities:    []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit},
		EndpointPolicy:  backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden,
		Options:         map[string]backend.OptionSpec{},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: backend catalog is invalid", ErrInvalidConfig)
	}
	return backendCatalog, nil
}

func environmentBool(name string) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%w: %s must be true or false", ErrInvalidConfig, name)
	}
	return parsed, nil
}

func requiredEnvironment(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%w: %s is required", ErrInvalidConfig, name)
	}
	return value, nil
}

func environmentOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func environmentList(name, value string, lowercase bool) ([]string, error) {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if lowercase {
			part = strings.ToLower(part)
		}
		if part == "" {
			return nil, fmt.Errorf("%w: %s contains an empty item", ErrInvalidConfig, name)
		}
		result = append(result, part)
	}
	return result, nil
}

// parseEnvironmentAPIIdentities accepts comma-separated token|tenant|app|subject
// entries. Tokens are used only as map keys and never included in errors.
func parseEnvironmentAPIIdentities(value string) (map[string]gateway.APIIdentity, error) {
	result := make(map[string]gateway.APIIdentity)
	for _, entry := range strings.Split(value, ",") {
		parts := strings.Split(entry, "|")
		if len(parts) != 4 {
			return nil, fmt.Errorf("%w: %s must use token|tenant|app|subject entries", ErrInvalidConfig, envAPIIdentities)
		}
		token := strings.TrimSpace(parts[0])
		identity := gateway.APIIdentity{TenantID: strings.TrimSpace(parts[1]), AppID: strings.TrimSpace(parts[2]), SubjectID: strings.TrimSpace(parts[3])}
		if token == "" || identity.TenantID == "" || identity.AppID == "" || identity.SubjectID == "" {
			return nil, fmt.Errorf("%w: %s contains an incomplete identity", ErrInvalidConfig, envAPIIdentities)
		}
		if _, exists := result[token]; exists {
			return nil, fmt.Errorf("%w: %s contains duplicate tokens", ErrInvalidConfig, envAPIIdentities)
		}
		result[token] = identity
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: %s is empty", ErrInvalidConfig, envAPIIdentities)
	}
	return result, nil
}

type environmentSecretResolver struct {
	reference string
	value     string
}

type environmentWeComCredentialResolver struct {
	tenantID string
	config   environmentWeComConfig
}

func (resolver environmentWeComCredentialResolver) Resolve(ctx context.Context, scope channels.SecretScope) (wecom.Credentials, error) {
	if ctx == nil {
		return wecom.Credentials{}, errors.New("wecom credential resolver context is required")
	}
	if err := ctx.Err(); err != nil {
		return wecom.Credentials{}, err
	}
	if err := scope.Validate(); err != nil || scope.TenantID != resolver.tenantID || scope.SecretRef != resolver.config.secretRef {
		return wecom.Credentials{}, errors.New("configured WeCom secret reference is unavailable")
	}
	return wecom.Credentials{CallbackToken: resolver.config.callbackToken, EncodingAESKey: resolver.config.encodingAESKey, AppSecret: resolver.config.appSecret}, nil
}

func environmentWeComOwner() (string, error) {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "", errors.New("WeCom worker hostname is unavailable")
	}
	return fmt.Sprintf("wecom-%s-%d", hostname, os.Getpid()), nil
}

func (resolver environmentSecretResolver) Resolve(ctx context.Context, scope modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	if ctx == nil {
		return modelprofile.SecretValue{}, errors.New("secret resolver context is required")
	}
	if err := ctx.Err(); err != nil {
		return modelprofile.SecretValue{}, err
	}
	if err := scope.Validate(); err != nil {
		return modelprofile.SecretValue{}, err
	}
	if scope.SecretRef != resolver.reference || resolver.value == "" {
		return modelprofile.SecretValue{}, errors.New("configured secret reference is unavailable")
	}
	return modelprofile.NewSecretValue(resolver.value)
}

type environmentModelFactory struct{}

type environmentSessionCapabilityProvider struct {
	delegate  session.Service
	store     runtimestorage.RuntimeStore
	telemetry observability.Provider
	backend   string
}

type environmentRuntimeCapabilityProvider struct {
	capability backend.Capability
	delegate   session.Service
	store      runtimestorage.RuntimeStore
	telemetry  observability.Provider
	backend    string
}

func (provider environmentRuntimeCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, _ backend.CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if provider.capability == backend.CapabilitySession {
		return runtimesessionpostgres.NewWithObservability(input.TenantID, provider.delegate, provider.store, provider.telemetry, provider.backend)
	}
	// The runtime store is owned by the environment, not by an individual
	// tenant CapabilitySet. Wrap it so factory cleanup cannot stop shared
	// workers when one runner is torn down.
	switch provider.capability {
	case backend.CapabilityMemory:
		store, ok := provider.store.(runtimestorage.MemoryStore)
		if !ok {
			return nil, backend.ErrStorageFactory
		}
		return borrowedMemoryStore{MemoryStore: store}, nil
	case backend.CapabilitySummary:
		store, ok := provider.store.(runtimestorage.SummaryStore)
		if !ok {
			return nil, backend.ErrStorageFactory
		}
		return borrowedSummaryStore{SummaryStore: store}, nil
	case backend.CapabilityKnowledge:
		knowledge, ok := provider.store.(runtimestorage.KnowledgeStore)
		if !ok {
			return nil, backend.ErrStorageFactory
		}
		vector, ok := provider.store.(runtimestorage.VectorStore)
		if !ok {
			return nil, backend.ErrStorageFactory
		}
		return borrowedKnowledgeStore{KnowledgeStore: knowledge, VectorStore: vector}, nil
	case backend.CapabilityArtifact:
		artifact, ok := provider.store.(runtimestorage.ArtifactStore)
		if !ok {
			return nil, backend.ErrStorageFactory
		}
		object, ok := provider.store.(runtimestorage.ObjectStore)
		if !ok {
			return nil, backend.ErrStorageFactory
		}
		return borrowedArtifactStore{ArtifactStore: artifact, ObjectStore: object}, nil
	case backend.CapabilityAudit:
		store, ok := provider.store.(runtimestorage.AuditStore)
		if !ok {
			return nil, backend.ErrStorageFactory
		}
		return borrowedAuditStore{AuditStore: store}, nil
	default:
		return nil, backend.ErrStorageFactory
	}
}

type borrowedMemoryStore struct{ runtimestorage.MemoryStore }
type borrowedSummaryStore struct{ runtimestorage.SummaryStore }
type borrowedKnowledgeStore struct {
	runtimestorage.KnowledgeStore
	runtimestorage.VectorStore
}
type borrowedArtifactStore struct {
	runtimestorage.ArtifactStore
	runtimestorage.ObjectStore
}
type borrowedAuditStore struct{ runtimestorage.AuditStore }

func (borrowedMemoryStore) Close() error    { return nil }
func (borrowedSummaryStore) Close() error   { return nil }
func (borrowedKnowledgeStore) Close() error { return nil }
func (borrowedArtifactStore) Close() error  { return nil }
func (borrowedAuditStore) Close() error     { return nil }

func (provider environmentSessionCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, _ backend.CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	return runtimesessionpostgres.NewWithObservability(input.TenantID, provider.delegate, provider.store, provider.telemetry, provider.backend)
}

func (environmentModelFactory) New(ctx context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	if ctx == nil {
		return nil, errors.New("model factory context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	provider := strings.ToLower(strings.TrimSpace(input.Provider))
	if provider == demoModelProvider {
		return deterministicModel{model: input.Model}, nil
	}
	apiKey := secret.Value()
	if apiKey == "" {
		return nil, errors.New("model factory secret is required")
	}
	if provider != "" && provider != defaultModelProvider {
		return nil, fmt.Errorf("model factory provider %q is unsupported", input.Provider)
	}
	endpoint := strings.TrimSpace(input.Endpoint)
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1"
	}
	return &responsesModel{apiKey: apiKey, endpoint: endpoint, model: input.Model}, nil
}

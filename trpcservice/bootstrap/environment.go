package bootstrap

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

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
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimesessionpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/sessionpostgres"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	runtimestorageredis "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	runtimestorages3 "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/s3"
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
	envRedisAddr      = "TRPC_REDIS_ADDR"
	// #nosec G101 -- environment variable name, not a credential.
	envRedisPassword  = "TRPC_REDIS_PASSWORD"
	envRedisDB        = "TRPC_REDIS_DB"
	envRedisKeyPrefix = "TRPC_REDIS_KEY_PREFIX"
	// #nosec G101 -- environment variable name, not a secret.
	envRedisSecretRef    = "TRPC_REDIS_SECRET_REF"
	envRedisDialTimeout  = "TRPC_REDIS_DIAL_TIMEOUT"
	envRedisReadTimeout  = "TRPC_REDIS_READ_TIMEOUT"
	envRedisWriteTimeout = "TRPC_REDIS_WRITE_TIMEOUT"
	envRedisPoolSize     = "TRPC_REDIS_POOL_SIZE"
	envS3AccessKeyID     = "TRPC_S3_ACCESS_KEY_ID"
	// #nosec G101 -- environment variable name, not a secret.
	envS3SecretKey = "TRPC_S3_SECRET_KEY"
	// #nosec G101 -- environment variable name, not a secret.
	envS3SecretRef = "TRPC_S3_SECRET_REF"
	envDemoMode    = "TRPC_DEMO_MODE"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComCallbackToken  = "WECOM_CALLBACK_TOKEN"
	envWeComEncodingAESKey = "WECOM_ENCODING_AES_KEY"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComAppSecret = "WECOM_APP_SECRET"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComSecretRef = "WECOM_SECRET_REF"
	// #nosec G101 -- environment variable name, not a credential.
	envWeComAIBotConnections = "WECOM_AIBOT_CONNECTIONS"
	// #nosec G101 -- environment variable name, not a credential.
	envTelegramBotToken  = "TELEGRAM_BOT_TOKEN"
	envTelegramBindingID = "TELEGRAM_BINDING_ID"
	envTelegramSecretRef = "TELEGRAM_SECRET_REF"
	envTelegramMode      = "TELEGRAM_MODE"
	envOTLPEndpoint      = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPHeaders       = "OTEL_EXPORTER_OTLP_HEADERS"
	envOTLPInsecure      = "OTEL_EXPORTER_OTLP_INSECURE"
	envOTELServiceName   = "OTEL_SERVICE_NAME"

	defaultModelProvider = "openai"
	defaultModelNames    = "gpt-4o-mini"
	defaultEndpointHost  = "api.openai.com"
	demoModelProvider    = "fake"
	demoModelName        = "deterministic"
	// #nosec G101 -- symbolic secret reference, not secret material.
	defaultModelSecretRef = "env/trpc-model-api-key"
	defaultSubjectID      = "service"
	maxRedisDB            = 1 << 15
)

var (
	openEnvironmentDatabase                         = postgres.Open
	openMySQLEnvironmentDatabase                    = mysql.Open
	applyEnvironmentMigrations                      = migrations.Apply
	applyMySQLEnvironmentMigrations                 = migrations.ApplyMySQL
	verifyEnvironmentMigrations                     = migrations.Verify
	verifyMySQLEnvironmentMigrations                = migrations.VerifyMySQL
	newEnvironmentRuntimeStore                      = environmentRuntimeStore
	newEnvironmentRedisRuntimeStore                 = environmentRedisRuntimeStore
	newEnvironmentInMemoryFallback                  = func() runtimestorage.RuntimeStore { return runtimestorageinmemory.New() }
	newEnvironmentS3Store            s3StoreFactory = newEnvironmentS3StoreFromConfig
	environmentWeComOwnerFunc                       = environmentWeComOwner
	newEnvironmentWeComWorker                       = outbox.New
	newEnvironmentTelegramAdapter                   = telegram.New
)

type s3StoreFactory func(context.Context, string, backend.CapabilityBinding, modelprofile.SecretValue) (environmentS3Store, error)

type environmentS3Store interface {
	runtimestorage.ArtifactStore
	runtimestorage.ObjectStore
	Probe(context.Context) error
}

// environmentConfig is intentionally private: it contains startup-only
// secrets handed to model and channel factories and must not become a
// serializable application configuration object.
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
	redis          runtimestorageredis.Config
	redisEndpoint  string
	redisSecretRef string
	s3AccessKeyID  string
	s3SecretKey    string
	s3SecretRef    string
	demoMode       bool
	wecom          *environmentWeComConfig
	wecomAIBots    []environmentWeComAIBotConfig
	telegram       *environmentTelegramConfig
	telemetry      observability.Provider
	otlp           observability.OTLPConfig
}

type environmentWeComConfig struct {
	callbackToken  string
	encodingAESKey string
	appSecret      string
	secretRef      string
}

// environmentWeComAIBotConfig is one operator-owned startup connection. Its
// SecretRef must match the immutable Binding before the secret is released.
type environmentWeComAIBotConfig struct {
	BindingID string `json:"binding_id"`
	SecretRef string `json:"secret_ref"`
	BotSecret string `json:"bot_secret"`
}

// environmentTelegramConfig contains one operator-selected Telegram binding
// and retains the Bot token only for the startup construction path.
type environmentTelegramConfig struct {
	bindingID string
	secretRef string
	botToken  string
	mode      string
}

// environmentRuntimeStores owns process-scoped runtime stores. The primary
// store serves ingress and outbox processing; provider stores serve Backend
// Profile capability materialization.
type environmentRuntimeStores struct {
	primary   runtimestorage.RuntimeStore
	providers map[string]runtimestorage.RuntimeStore
	owned     []runtimestorage.RuntimeStore
}

func (stores environmentRuntimeStores) Close() error {
	var errs []error
	for _, store := range stores.owned {
		if store != nil {
			errs = append(errs, store.Close())
		}
	}
	return errors.Join(errs...)
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
	runtimeStores, err := newEnvironmentRuntimeStoresForConfig(ctx, config, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if config.runtimeStorage == "redis" {
			return nil, fmt.Errorf("%w: Redis runtime storage is unavailable", ErrInvalidConfig)
		}
		return nil, err
	}
	runtimeStore := runtimeStores.primary
	tenantRepo, appRepo, channelRepo, auditWriter, err := environmentRepositories(config, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: environment repositories: %v", ErrInvalidConfig, err)
	}
	auditWriter = metrics.WrapAuditWriter(auditWriter, config.telemetry)
	wecomFactory, wecomProvider, telegramFactory, err := environmentChannelComponents(ctx, config, channelRepo, tenantRepo, appRepo, runtimeStore, auditWriter)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, err
	}
	secretRegistry, modelRegistry, backendRegistry, err := environmentRegistriesForStores(config, delegateSessions, runtimeStores)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: environment registries: %v", ErrInvalidConfig, err)
	}
	aiBotFactories, aiBotBindingIDs, err := environmentWeComAIBotComponents(ctx, config, channelRepo, tenantRepo, appRepo)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: wecom ai bot components: %v", ErrInvalidConfig, err)
	}
	workerFactory := environmentOutboxWorkerFactory(config, runtimeStore, auditWriter, wecomProvider, aiBotBindingIDs)
	storageFactory, err := backend.NewRegistryStorageFactory(backendRegistry, secretRegistry)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: storage factory: %v", ErrInvalidConfig, err)
	}
	graph, err := NewWithDatabase(ctx, db, Config{
		OwnDB:                  true,
		ControlPlaneDriver:     config.driver,
		Observability:          config.telemetry,
		Tenants:                tenantRepo,
		Apps:                   appRepo,
		Channels:               channelRepo,
		ModelCatalog:           modelCatalog,
		BackendCatalog:         backendCatalog,
		SecretResolver:         secretRegistry,
		ModelFactory:           modelRegistry,
		StorageFactory:         storageFactory,
		Sessions:               delegateSessions,
		RuntimeStore:           runtimeStore,
		RuntimeTenantID:        "",
		Authenticator:          authenticator,
		AdminAuthenticator:     adminAuthenticator,
		WeComHandlerFactory:    wecomFactory,
		WeComAIBotFactories:    aiBotFactories,
		TelegramPollingFactory: telegramFactory,
		OutboxWorkerFactory:    workerFactory,
		OutboxPollInterval:     time.Second,
		AuditWriter:            auditWriter,
		Ping: func(pingContext context.Context) error {
			return environmentPing(pingContext, config.driver, db, runtimeStore)
		},
		Migrate:          applyMigrations,
		VerifyMigrations: verifyMigrations,
		CloseDependencies: func() error {
			return errors.Join(delegateSessions.Close(), runtimeStores.Close())
		},
	})
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, err
	}
	telemetryOwned = false
	return graph, nil
}

func environmentChannelComponents(
	ctx context.Context,
	config environmentConfig,
	channelsRepo channels.CandidateConsumer,
	tenantsRepo tenant.Repository,
	appsRepo agent.Repository,
	runtimeStore runtimestorage.RuntimeStore,
	auditWriter audit.Writer,
) (func(gateway.DispatchService) (http.Handler, error), outbox.Provider, func(gateway.DispatchService) (channels.PollingAdapter, error), error) {
	wecomFactory, wecomProvider, err := environmentWeComComponents(config, channelsRepo, tenantsRepo, appsRepo, runtimeStore, auditWriter)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: wecom components: %v", ErrInvalidConfig, err)
	}
	telegramFactory, err := environmentTelegramComponents(ctx, config, channelsRepo, tenantsRepo, appsRepo, runtimeStore, auditWriter)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: telegram components: %v", ErrInvalidConfig, err)
	}
	return wecomFactory, wecomProvider, telegramFactory, nil
}

func openEnvironmentDatabaseForConfig(ctx context.Context, config environmentConfig) (*sql.DB, func(context.Context, *sql.DB) error, func(context.Context, *sql.DB) error, error) {
	if config.driver != ControlPlaneDriverMySQL {
		db, err := openPostgresEnvironmentDatabaseForConfig(ctx, config)
		if err != nil {
			return nil, nil, nil, err
		}
		return db, nil, nil, nil
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

func openPostgresEnvironmentDatabaseForConfig(ctx context.Context, config environmentConfig) (*sql.DB, error) {
	db, err := openEnvironmentDatabase(ctx, config.dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %s control plane is unavailable", ErrInvalidConfig, config.driver)
	}
	if err := applyEnvironmentMigrations(ctx, db); err != nil {
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: PostgreSQL migrations are not ready", ErrInvalidConfig)
	}
	if err := verifyEnvironmentMigrations(ctx, db); err != nil {
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: PostgreSQL migrations are not ready", ErrInvalidConfig)
	}
	return db, nil
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

func environmentWeComComponents(config environmentConfig, channelsRepo channels.CandidateConsumer, tenantsRepo tenant.Repository, appsRepo agent.Repository, runtimeStore runtimestorage.RuntimeStore, auditWriter audit.Writer) (func(gateway.DispatchService) (http.Handler, error), outbox.Provider, error) {
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
	return factory, &wecom.BindingProvider{Bindings: channelsRepo, Credentials: credentials}, nil
}

func environmentWeComAIBotComponents(ctx context.Context, config environmentConfig, channelsRepo channels.CandidateConsumer, tenantsRepo tenant.Repository, appsRepo agent.Repository) ([]func(gateway.DispatchService) (channels.PollingAdapter, error), map[string]struct{}, error) {
	if len(config.wecomAIBots) == 0 {
		return nil, nil, nil
	}
	secrets := make(map[string]string, len(config.wecomAIBots))
	targets := make([]channels.RoutingTarget, 0, len(config.wecomAIBots))
	for _, value := range config.wecomAIBots {
		if _, exists := secrets[value.SecretRef]; exists {
			return nil, nil, errors.New("wecom ai bot secret reference is duplicated")
		}
		secrets[value.SecretRef] = value.BotSecret
		target, err := channels.ResolveConfiguredRoutingTarget(ctx, channelsRepo, tenantsRepo, appsRepo, config.tenantID, value.BindingID)
		if err != nil || target.Channel != channels.ChannelWeComAIBot {
			return nil, nil, errors.New("wecom ai bot binding is unavailable")
		}
		targets = append(targets, target)
	}
	credentials := environmentWeComAIBotCredentialResolver{tenantID: config.tenantID, secrets: secrets}
	factories := make([]func(gateway.DispatchService) (channels.PollingAdapter, error), 0, len(targets))
	bindingIDs := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		target := target
		bindingIDs[target.BindingID] = struct{}{}
		factories = append(factories, func(dispatcher gateway.DispatchService) (channels.PollingAdapter, error) {
			return wecom_aibot.NewForBinding(ctx, wecom_aibot.BindingConfig{Target: target, Bindings: channelsRepo, Credentials: credentials, Dispatcher: dispatcher})
		})
	}
	return factories, bindingIDs, nil
}

func environmentOutboxWorkerFactory(config environmentConfig, runtimeStore runtimestorage.RuntimeStore, auditWriter audit.Writer, legacy outbox.Provider, aiBotBindingIDs map[string]struct{}) func([]channels.PollingAdapter) (*outbox.Worker, error) {
	if legacy == nil && len(aiBotBindingIDs) == 0 && config.telegram == nil {
		return nil
	}
	return func(adapters []channels.PollingAdapter) (*outbox.Worker, error) {
		provider, channel, providerName, leaseDuration, err := environmentOutboxProvider(config, runtimeStore, legacy, aiBotBindingIDs, adapters)
		if err != nil {
			return nil, err
		}
		owner, err := environmentWeComOwnerFunc()
		if err != nil {
			return nil, err
		}
		return newEnvironmentWeComWorker(outbox.Config{Store: runtimeStore, Provider: provider, Channel: channel, ProviderName: providerName, TenantID: config.tenantID, Owner: owner, LeaseDuration: leaseDuration, AuditWriter: auditWriter, Observability: config.telemetry})
	}
}

func environmentOutboxProvider(
	config environmentConfig,
	runtimeStore runtimestorage.RuntimeStore,
	legacy outbox.Provider,
	aiBotBindingIDs map[string]struct{},
	adapters []channels.PollingAdapter,
) (outbox.Provider, string, string, time.Duration, error) {
	aiBotProvider, err := environmentAIBotOutboxProvider(runtimeStore, aiBotBindingIDs, adapters)
	if err != nil {
		return nil, "", "", 0, err
	}
	telegramProvider, telegramBindingIDs, err := environmentTelegramOutboxProvider(config, adapters)
	if err != nil {
		return nil, "", "", 0, err
	}
	providerCount := 0
	if legacy != nil {
		providerCount++
	}
	if aiBotProvider != nil {
		providerCount++
	}
	if telegramProvider != nil {
		providerCount++
	}
	leaseDuration := 30 * time.Second
	switch providerCount {
	case 0:
		return nil, "", "", 0, errors.New("no outbox provider is configured")
	case 1:
		switch {
		case telegramProvider != nil:
			return telegramProvider, "telegram", "telegram", leaseDuration, nil
		case aiBotProvider != nil:
			return aiBotProvider, "wecom_aibot", "wecom_aibot", wecom_aibot.OutboxLeaseDuration, nil
		default:
			return legacy, "wecom", "wecom", leaseDuration, nil
		}
	default:
		return environmentReplyProvider{
			legacy: legacy, aiBot: aiBotProvider, telegram: telegramProvider,
			aiBotBindingIDs: aiBotBindingIDs, telegramBindingIDs: telegramBindingIDs,
		}, "mixed", "mixed", leaseDuration, nil
	}
}

func environmentAIBotOutboxProvider(runtimeStore runtimestorage.RuntimeStore, bindingIDs map[string]struct{}, adapters []channels.PollingAdapter) (outbox.Provider, error) {
	if len(bindingIDs) == 0 {
		return nil, nil
	}
	deliveryStore, ok := runtimeStore.(wecom_aibot.DeliveryStore)
	if !ok {
		return nil, errors.New("runtime store does not support durable reply acknowledgements")
	}
	managers := make([]*wecom_aibot.Manager, 0, len(bindingIDs))
	for _, adapter := range adapters {
		manager, ok := adapter.(*wecom_aibot.Manager)
		if !ok {
			continue
		}
		managers = append(managers, manager)
	}
	if len(managers) != len(bindingIDs) {
		return nil, errors.New("wecom ai bot manager count is invalid")
	}
	return wecom_aibot.NewBindingProvider(deliveryStore, managers...)
}

func environmentTelegramOutboxProvider(config environmentConfig, adapters []channels.PollingAdapter) (outbox.Provider, map[string]struct{}, error) {
	if config.telegram == nil {
		return nil, nil, nil
	}
	telegramAdapters := make([]*telegram.Adapter, 0, 1)
	for _, adapter := range adapters {
		telegramAdapter, ok := adapter.(*telegram.Adapter)
		if ok {
			telegramAdapters = append(telegramAdapters, telegramAdapter)
		}
	}
	if len(telegramAdapters) != 1 {
		return nil, nil, errors.New("telegram adapter count is invalid")
	}
	provider, err := telegram.NewBindingProvider(telegramAdapters...)
	if err != nil {
		return nil, nil, err
	}
	return provider, map[string]struct{}{config.telegram.bindingID: {}}, nil
}

type environmentReplyProvider struct {
	legacy             outbox.Provider
	aiBot              outbox.Provider
	telegram           outbox.Provider
	aiBotBindingIDs    map[string]struct{}
	telegramBindingIDs map[string]struct{}
}

func (p environmentReplyProvider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	provider, err := p.provider(value)
	if err != nil {
		return "", err
	}
	return provider.Deliver(ctx, value)
}

func (p environmentReplyProvider) Reconcile(ctx context.Context, value runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	provider, err := p.provider(value)
	if err != nil {
		return outbox.DeliveryUnknown, "", err
	}
	return provider.Reconcile(ctx, value)
}

func (p environmentReplyProvider) provider(value runtimestorage.ReplyOutbox) (outbox.Provider, error) {
	if _, ok := p.telegramBindingIDs[value.ReplyTarget.BindingID]; ok {
		if p.telegram == nil {
			return nil, environmentInvalidDelivery()
		}
		return p.telegram, nil
	}
	if _, ok := p.aiBotBindingIDs[value.ReplyTarget.BindingID]; ok {
		if p.aiBot == nil {
			return nil, environmentInvalidDelivery()
		}
		return p.aiBot, nil
	}
	if p.legacy == nil {
		return nil, environmentInvalidDelivery()
	}
	return p.legacy, nil
}

func environmentInvalidDelivery() error {
	return &outbox.DeliveryError{Class: "invalid", Retryable: false}
}

func environmentRegistries(config environmentConfig, delegateSessions session.Service, runtimeStore runtimestorage.RuntimeStore) (*modelprofile.SecretRegistry, *modelprofile.ModelProviderRegistry, *backend.ProviderRegistry, error) {
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	return environmentRegistriesForStores(config, delegateSessions, environmentRuntimeStores{
		primary:   runtimeStore,
		providers: map[string]runtimestorage.RuntimeStore{providerName: runtimeStore},
	})
}

type environmentRuntimeProviderSpec struct {
	name         string
	capabilities []backend.Capability
	store        runtimestorage.RuntimeStore
}

func environmentRegistriesForStores(config environmentConfig, delegateSessions session.Service, runtimeStores environmentRuntimeStores) (*modelprofile.SecretRegistry, *modelprofile.ModelProviderRegistry, *backend.ProviderRegistry, error) {
	secretRegistry := modelprofile.NewSecretRegistry()
	modelRegistry := modelprofile.NewModelProviderRegistry()
	backendRegistry := backend.NewProviderRegistry()
	runtimeProviders, err := environmentRuntimeProviders(config, runtimeStores)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, identity := range config.apiIdentities {
		if config.demoMode {
			if err := modelRegistry.Register(identity.TenantID, demoModelProvider, environmentModelFactory{}); err != nil {
				return nil, nil, nil, err
			}
			if err := registerEnvironmentRuntimeProviders(backendRegistry, identity.TenantID, delegateSessions, config, runtimeProviders); err != nil {
				return nil, nil, nil, err
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
		if config.runtimeStorage == "redis" && config.redis.Password != "" {
			if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.redisSecretRef}, config.redis.Password); err != nil {
				return nil, nil, nil, err
			}
		}
		if config.s3AccessKeyID != "" {
			if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.s3SecretRef}, config.s3AccessKeyID+":"+config.s3SecretKey); err != nil {
				return nil, nil, nil, err
			}
		}
		if err := registerEnvironmentRuntimeProviders(backendRegistry, identity.TenantID, delegateSessions, config, runtimeProviders); err != nil {
			return nil, nil, nil, err
		}
	}
	return secretRegistry, modelRegistry, backendRegistry, nil
}

func environmentRuntimeProviders(config environmentConfig, stores environmentRuntimeStores) ([]environmentRuntimeProviderSpec, error) {
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	primary := stores.providers[providerName]
	if primary == nil {
		return nil, fmt.Errorf("%w: primary runtime provider is unavailable", ErrInvalidConfig)
	}
	providers := []environmentRuntimeProviderSpec{{name: providerName, capabilities: environmentRuntimeCapabilities(config.runtimeStorage), store: primary}}
	if config.runtimeStorage != "redis" {
		return providers, nil
	}
	fallback := stores.providers["inmemory"]
	if fallback == nil {
		return nil, fmt.Errorf("%w: in-memory runtime provider is unavailable", ErrInvalidConfig)
	}
	return append(providers, environmentRuntimeProviderSpec{name: "inmemory", capabilities: environmentRuntimeCapabilities("inmemory"), store: fallback}), nil
}

func registerEnvironmentRuntimeProviders(registry *backend.ProviderRegistry, tenantID string, delegateSessions session.Service, config environmentConfig, runtimeProviders []environmentRuntimeProviderSpec) error {
	for _, runtimeProvider := range runtimeProviders {
		for _, capability := range runtimeProvider.capabilities {
			provider := environmentRuntimeCapabilityProvider{capability: capability, delegate: delegateSessions, store: runtimeProvider.store, telemetry: config.telemetry, backend: runtimeProvider.name}
			if runtimeProvider.name == "redis" {
				provider.redisEndpoint = config.redisEndpoint
				provider.redisSecretRef = config.redisSecretRef
				provider.redisPasswordRequired = config.redis.Password != ""
			}
			if err := registry.Register(tenantID, capability, runtimeProvider.name, provider); err != nil {
				return err
			}
		}
	}
	if err := registry.Register(tenantID, backend.CapabilityArtifact, "s3", environmentS3CapabilityProvider{tenantID: tenantID, secretRef: config.s3SecretRef}); err != nil {
		return err
	}
	return nil
}

func environmentRuntimeProviderName(runtimeStorage string) string {
	if runtimeStorage == "redis" {
		return "redis"
	}
	return "inmemory"
}

func environmentRuntimeCapabilities(runtimeStorage string) []backend.Capability {
	if runtimeStorage == "redis" {
		return []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory}
	}
	return []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit}
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
	loaders := []func() error{config.loadDatabase, config.loadIdentities, config.loadAdmin, config.loadModel, config.loadRuntime, config.loadS3, config.loadWeCom, config.loadWeComAIBots, config.loadTelegram}
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

func (config *environmentConfig) loadS3() error {
	config.s3AccessKeyID = strings.TrimSpace(os.Getenv(envS3AccessKeyID))
	config.s3SecretKey = os.Getenv(envS3SecretKey)
	config.s3SecretRef = environmentOrDefault(envS3SecretRef, "env/trpc-s3-credentials")
	configured := config.s3AccessKeyID != "" || config.s3SecretKey != ""
	if !configured {
		config.s3SecretRef = ""
		return nil
	}
	if config.s3AccessKeyID == "" || config.s3SecretKey == "" || strings.ContainsAny(config.s3AccessKeyID, "\r\n") || strings.ContainsAny(config.s3SecretKey, "\r\n") {
		return fmt.Errorf("%w: S3 credentials must be configured together", ErrInvalidConfig)
	}
	if _, err := modelprofile.NewSecretValue(config.s3AccessKeyID + ":" + config.s3SecretKey); err != nil {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envS3SecretKey)
	}
	for _, identity := range config.apiIdentities {
		if err := (modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.s3SecretRef}).Validate(); err != nil {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envS3SecretRef)
		}
	}
	return nil
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
	switch config.runtimeStorage {
	case "postgres", "inmemory":
	case "redis":
		if config.demoMode {
			return fmt.Errorf("%w: %s cannot use redis in demo mode", ErrInvalidConfig, envSessionBackend)
		}
		if err := config.loadRedis(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: %s must be explicitly set to postgres, redis or inmemory", ErrInvalidConfig, envSessionBackend)
	}
	if config.demoMode && (config.driver != ControlPlaneDriverPostgres || config.runtimeStorage != "inmemory") {
		return fmt.Errorf("%w: %s requires PostgreSQL control plane and inmemory session backend", ErrInvalidConfig, envDemoMode)
	}
	if config.driver == ControlPlaneDriverMySQL && config.runtimeStorage == "postgres" {
		return fmt.Errorf("%w: %s=postgres is not available with MySQL control plane; use inmemory until a MySQL runtime adapter is selected", ErrInvalidConfig, envSessionBackend)
	}
	return nil
}

func (config *environmentConfig) loadRedis() error {
	addr, err := requiredEnvironment(envRedisAddr)
	if err != nil {
		return err
	}
	if strings.ContainsAny(addr, "\r\n") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisAddr)
	}
	db, err := environmentInteger(envRedisDB, environmentOrDefault(envRedisDB, "0"), 0, maxRedisDB)
	if err != nil {
		return err
	}
	dialTimeout, err := environmentDuration(envRedisDialTimeout)
	if err != nil {
		return err
	}
	readTimeout, err := environmentDuration(envRedisReadTimeout)
	if err != nil {
		return err
	}
	writeTimeout, err := environmentDuration(envRedisWriteTimeout)
	if err != nil {
		return err
	}
	poolSize, err := environmentInteger(envRedisPoolSize, environmentOrDefault(envRedisPoolSize, "0"), 0, 0)
	if err != nil {
		return err
	}
	keyPrefix := environmentOrDefault(envRedisKeyPrefix, "trpc:runtime:v1")
	if strings.ContainsAny(keyPrefix, "\r\n") || strings.TrimSpace(keyPrefix) == "" {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisKeyPrefix)
	}
	password := os.Getenv(envRedisPassword)
	if strings.ContainsAny(password, "\r\n") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisPassword)
	}
	config.redis = runtimestorageredis.Config{Addr: addr, Password: password, DB: db, KeyPrefix: keyPrefix, DialTimeout: dialTimeout, ReadTimeout: readTimeout, WriteTimeout: writeTimeout, PoolSize: poolSize}
	config.redisEndpoint = redisEndpoint(addr)
	config.redisSecretRef = environmentOrDefault(envRedisSecretRef, "env/trpc-redis-password")
	if _, err := modelprofile.NewSecretValue(config.redis.Password); err != nil && config.redis.Password != "" {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisPassword)
	}
	for _, identity := range config.apiIdentities {
		if err := (modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.redisSecretRef}).Validate(); err != nil {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisSecretRef)
		}
	}
	return nil
}

func environmentInteger(name, value string, min, max int) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed < min || (max > 0 && parsed > max) {
		return 0, fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, name)
	}
	return parsed, nil
}

func environmentDuration(name string) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, name)
	}
	return parsed, nil
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

func (config *environmentConfig) loadWeComAIBots() error {
	raw := strings.TrimSpace(os.Getenv(envWeComAIBotConnections))
	if raw == "" {
		return nil
	}
	if config.demoMode || len(config.apiIdentities) != 1 {
		return fmt.Errorf("%w: %s requires one non-demo API identity", ErrInvalidConfig, envWeComAIBotConnections)
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var values []environmentWeComAIBotConfig
	if err := decoder.Decode(&values); err != nil || len(values) == 0 {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
	}
	seenBindings := make(map[string]struct{}, len(values))
	seenSecretRefs := make(map[string]struct{}, len(values))
	for index := range values {
		values[index].BindingID = strings.TrimSpace(values[index].BindingID)
		values[index].SecretRef = strings.TrimSpace(values[index].SecretRef)
		if values[index].BindingID == "" || values[index].BotSecret == "" || strings.ContainsAny(values[index].BindingID, "\r\n") || strings.ContainsAny(values[index].BotSecret, "\r\n") {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
		}
		if err := (channels.SecretScope{TenantID: config.tenantID, SecretRef: values[index].SecretRef}).Validate(); err != nil {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
		}
		if _, exists := seenBindings[values[index].BindingID]; exists {
			return fmt.Errorf("%w: %s contains duplicate binding IDs", ErrInvalidConfig, envWeComAIBotConnections)
		}
		if _, exists := seenSecretRefs[values[index].SecretRef]; exists {
			return fmt.Errorf("%w: %s contains duplicate secret references", ErrInvalidConfig, envWeComAIBotConnections)
		}
		seenBindings[values[index].BindingID] = struct{}{}
		seenSecretRefs[values[index].SecretRef] = struct{}{}
	}
	config.wecomAIBots = values
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

func newEnvironmentRuntimeStoreForConfig(ctx context.Context, config environmentConfig, db *sql.DB) (runtimestorage.RuntimeStore, error) {
	if config.runtimeStorage == "redis" {
		return newEnvironmentRedisRuntimeStore(ctx, config)
	}
	return newEnvironmentRuntimeStore(config.runtimeStorage, db)
}

func newEnvironmentRuntimeStoresForConfig(ctx context.Context, config environmentConfig, db *sql.DB) (environmentRuntimeStores, error) {
	primary, err := newEnvironmentRuntimeStoreForConfig(ctx, config, db)
	if err != nil {
		return environmentRuntimeStores{}, err
	}
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	stores := environmentRuntimeStores{
		primary:   primary,
		providers: map[string]runtimestorage.RuntimeStore{providerName: primary},
		owned:     []runtimestorage.RuntimeStore{primary},
	}
	if config.runtimeStorage != "redis" {
		return stores, nil
	}
	fallback := newEnvironmentInMemoryFallback()
	stores.providers["inmemory"] = fallback
	stores.owned = append(stores.owned, fallback)
	return stores, nil
}

func environmentRedisRuntimeStore(ctx context.Context, config environmentConfig) (runtimestorage.RuntimeStore, error) {
	store, err := runtimestorageredis.NewFromConfig(ctx, config.redis)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func environmentPing(ctx context.Context, driver ControlPlaneDriver, db *sql.DB, runtimeStore runtimestorage.RuntimeStore) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if driver == ControlPlaneDriverMySQL {
		if err := mysql.Ping(ctx, db); err != nil {
			return err
		}
	} else if err := postgres.Ping(ctx, db); err != nil {
		return err
	}
	if pinger, ok := runtimeStore.(interface{ Ping(context.Context) error }); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func redisEndpoint(addr string) string {
	return "redis://" + strings.TrimSpace(addr)
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
		backendCatalog, err := newEnvironmentBackendCatalog(config.runtimeStorage)
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
	backendCatalog, err := newEnvironmentBackendCatalog(config.runtimeStorage)
	if err != nil {
		return nil, nil, err
	}
	return modelCatalog, backendCatalog, nil
}

func newEnvironmentBackendCatalog(runtimeStorage string) (*backend.ProviderCatalog, error) {
	inMemory := backend.ProviderSpec{
		Provider:        "inmemory",
		Capabilities:    []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit},
		EndpointPolicy:  backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden,
		Options:         map[string]backend.OptionSpec{},
	}
	if runtimeStorage == "redis" {
		backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
			Provider:        "redis",
			Capabilities:    []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory},
			EndpointPolicy:  backend.FieldRequired,
			EndpointSchemes: []string{"redis"},
			SecretRefPolicy: backend.FieldOptional,
			Options:         map[string]backend.OptionSpec{},
		}, s3BackendProviderSpec(), inMemory)
		if err != nil {
			return nil, fmt.Errorf("%w: backend catalog is invalid", ErrInvalidConfig)
		}
		return backendCatalog, nil
	}
	backendCatalog, err := backend.NewProviderCatalog(s3BackendProviderSpec(), inMemory)
	if err != nil {
		return nil, fmt.Errorf("%w: backend catalog is invalid", ErrInvalidConfig)
	}
	return backendCatalog, nil
}

func s3BackendProviderSpec() backend.ProviderSpec {
	return backend.ProviderSpec{
		Provider:        "s3",
		Capabilities:    []backend.Capability{backend.CapabilityArtifact},
		EndpointPolicy:  backend.FieldRequired,
		EndpointSchemes: []string{"http", "https"},
		SecretRefPolicy: backend.FieldRequired,
		Options: map[string]backend.OptionSpec{
			"bucket":             {Kind: backend.OptionString, Required: true},
			"region":             {Kind: backend.OptionString, DefaultValue: stringOption("us-east-1")},
			"path_style":         {Kind: backend.OptionBoolean, DefaultValue: stringOption("false")},
			"allow_insecure":     {Kind: backend.OptionBoolean, DefaultValue: stringOption("false")},
			"max_bytes":          {Kind: backend.OptionInteger, DefaultValue: stringOption("33554432"), MinInteger: int64Option(1), MaxInteger: int64Option(1073741824)},
			"connect_timeout_ms": {Kind: backend.OptionInteger, DefaultValue: stringOption("15000"), MinInteger: int64Option(1), MaxInteger: int64Option(300000)},
			"read_timeout_ms":    {Kind: backend.OptionInteger, DefaultValue: stringOption("15000"), MinInteger: int64Option(1), MaxInteger: int64Option(300000)},
			"write_timeout_ms":   {Kind: backend.OptionInteger, DefaultValue: stringOption("15000"), MinInteger: int64Option(1), MaxInteger: int64Option(300000)},
		},
		ValidateBinding: validEnvironmentS3Binding,
	}
}

func validEnvironmentS3Binding(binding backend.CapabilityBinding) bool {
	return validEnvironmentS3Endpoint(binding.Endpoint, binding.Options["allow_insecure"] == "true") && validS3Bucket(binding.Options["bucket"])
}

func stringOption(value string) *string { return &value }
func int64Option(value int64) *int64    { return &value }

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

type environmentWeComAIBotCredentialResolver struct {
	tenantID string
	secrets  map[string]string
}

func (resolver environmentWeComAIBotCredentialResolver) Resolve(ctx context.Context, scope channels.SecretScope) (wecom_aibot.Credentials, error) {
	if ctx == nil {
		return wecom_aibot.Credentials{}, errors.New("wecom ai bot credential resolver context is required")
	}
	if err := ctx.Err(); err != nil {
		return wecom_aibot.Credentials{}, err
	}
	if err := scope.Validate(); err != nil || scope.TenantID != resolver.tenantID {
		return wecom_aibot.Credentials{}, errors.New("configured wecom ai bot secret reference is unavailable")
	}
	secret := resolver.secrets[scope.SecretRef]
	if secret == "" {
		return wecom_aibot.Credentials{}, errors.New("configured wecom ai bot secret reference is unavailable")
	}
	return wecom_aibot.Credentials{BotSecret: secret}, nil
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
	capability            backend.Capability
	delegate              session.Service
	store                 runtimestorage.RuntimeStore
	telemetry             observability.Provider
	backend               string
	redisEndpoint         string
	redisSecretRef        string
	redisPasswordRequired bool
}

type environmentS3CapabilityProvider struct {
	tenantID  string
	secretRef string
}

func (provider environmentS3CapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if provider.tenantID == "" || input.TenantID != provider.tenantID || binding.Capability != backend.CapabilityArtifact || strings.ToLower(strings.TrimSpace(binding.Provider)) != "s3" || provider.secretRef == "" || binding.SecretRef != provider.secretRef {
		return nil, backend.ErrStorageFactory
	}
	store, err := newEnvironmentS3Store(ctx, provider.tenantID, binding, secret)
	if err != nil || store == nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, backend.ErrStorageFactory
	}
	if err := store.Probe(ctx); err != nil {
		_ = store.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, backend.ErrStorageFactory
	}
	return store, nil
}

func newEnvironmentS3StoreFromConfig(ctx context.Context, tenantID string, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (environmentS3Store, error) {
	if ctx == nil || ctx.Err() != nil || tenantID == "" {
		return nil, backend.ErrStorageFactory
	}
	accessKey, secretKey, err := parseEnvironmentS3Credentials(binding.SecretRef, secret)
	if err != nil {
		return nil, backend.ErrStorageFactory
	}
	endpoint := strings.TrimSpace(binding.Endpoint)
	options, err := parseEnvironmentS3Options(binding.Options)
	if err != nil || !validEnvironmentS3Endpoint(endpoint, options.allowInsecure) {
		return nil, backend.ErrStorageFactory
	}
	cfg := awssdk.Config{
		Region:      options.region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	return runtimestorages3.NewFromConfig(cfg, options.bucket, tenantID, endpoint, options.pathStyle, options.allowInsecure, runtimestorages3.Options{
		MaxBytes: options.maxBytes, ConnectTimeout: options.connectTimeout, ReadTimeout: options.readTimeout, WriteTimeout: options.writeTimeout,
	})
}

func parseEnvironmentS3Credentials(secretRef string, secret modelprofile.SecretValue) (string, string, error) {
	if secretRef == "" || secret.Value() == "" {
		return "", "", backend.ErrStorageFactory
	}
	accessKey, secretKey, ok := strings.Cut(secret.Value(), ":")
	if !ok || strings.TrimSpace(accessKey) == "" || secretKey == "" || strings.ContainsAny(accessKey, "\r\n") || strings.ContainsAny(secretKey, "\r\n") {
		return "", "", backend.ErrStorageFactory
	}
	return accessKey, secretKey, nil
}

func validEnvironmentS3Endpoint(endpoint string, allowInsecure bool) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return false
	}
	return parsed.Scheme == "https" || (parsed.Scheme == "http" && allowInsecure)
}

type environmentS3Options struct {
	bucket, region string
	pathStyle      bool
	allowInsecure  bool
	maxBytes       int64
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

func parseEnvironmentS3Options(raw map[string]string) (environmentS3Options, error) {
	for key := range raw {
		switch key {
		case "bucket", "region", "path_style", "allow_insecure", "max_bytes", "connect_timeout_ms", "read_timeout_ms", "write_timeout_ms":
		default:
			return environmentS3Options{}, backend.ErrStorageFactory
		}
	}
	value := func(key, fallback string) string {
		if item := strings.TrimSpace(raw[key]); item != "" {
			return item
		}
		return fallback
	}
	result := environmentS3Options{bucket: value("bucket", ""), region: value("region", "us-east-1")}
	if result.bucket == "" || result.region == "" || len(result.region) > 128 || strings.ContainsAny(result.region, "\r\n\t ") || !validS3Bucket(result.bucket) {
		return environmentS3Options{}, backend.ErrStorageFactory
	}
	var err error
	if result.pathStyle, err = strconv.ParseBool(value("path_style", "false")); err != nil {
		return environmentS3Options{}, backend.ErrStorageFactory
	}
	if result.allowInsecure, err = strconv.ParseBool(value("allow_insecure", "false")); err != nil {
		return environmentS3Options{}, backend.ErrStorageFactory
	}
	maxBytes, err := strconv.ParseInt(value("max_bytes", "33554432"), 10, 64)
	if err != nil || maxBytes < 1 || maxBytes > 1<<30 {
		return environmentS3Options{}, backend.ErrStorageFactory
	}
	result.maxBytes = maxBytes
	for key, target := range map[string]*time.Duration{
		"connect_timeout_ms": &result.connectTimeout,
		"read_timeout_ms":    &result.readTimeout,
		"write_timeout_ms":   &result.writeTimeout,
	} {
		milliseconds, parseErr := strconv.ParseInt(value(key, "15000"), 10, 64)
		if parseErr != nil || milliseconds < 1 || milliseconds > 300000 {
			return environmentS3Options{}, backend.ErrStorageFactory
		}
		*target = time.Duration(milliseconds) * time.Millisecond
	}
	return result, nil
}

func validS3Bucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || strings.ToLower(bucket) != bucket || net.ParseIP(bucket) != nil || strings.Contains(bucket, "..") {
		return false
	}
	for _, label := range strings.Split(bucket, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, value := range label {
			if (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func (provider environmentRuntimeCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := provider.validateRedisBinding(binding, secret); err != nil {
		return nil, err
	}
	return provider.newCapability(ctx, input)
}

func (provider environmentRuntimeCapabilityProvider) validateRedisBinding(binding backend.CapabilityBinding, secret modelprofile.SecretValue) error {
	if provider.backend != "redis" {
		return nil
	}
	if provider.capability != backend.CapabilitySession && provider.capability != backend.CapabilityMemory {
		return backend.ErrStorageFactory
	}
	if provider.redisEndpoint != "" && binding.Endpoint != provider.redisEndpoint {
		return backend.ErrStorageFactory
	}
	if provider.redisSecretRef != "" && binding.SecretRef != "" && binding.SecretRef != provider.redisSecretRef {
		return backend.ErrStorageFactory
	}
	if provider.redisPasswordRequired && secret.Value() == "" {
		return backend.ErrStorageFactory
	}
	return nil
}

func (provider environmentRuntimeCapabilityProvider) newCapability(ctx context.Context, input backend.StorageFactoryInput) (any, error) {
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

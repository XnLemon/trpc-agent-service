package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
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
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	envControlPlaneDriver = "TRPC_CONTROL_PLANE_DRIVER"
	envPostgresDSN        = "TRPC_POSTGRES_DSN"
	envMySQLDSN           = "TRPC_MYSQL_DSN"
	envMySQLMigrationDSN  = "TRPC_MYSQL_MIGRATION_DSN"
	// #nosec G101 -- environment variable name, not a credential.
	envAPIToken = "TRPC_API_TOKEN"
	envTenantID = "TRPC_TENANT_ID"
	envAppID    = "TRPC_APP_ID"
	// #nosec G101 -- environment variable name, not a credential.
	envAdminToken   = "TRPC_ADMIN_TOKEN"
	envAdminTenants = "TRPC_ADMIN_TENANTS"
	envSubjectID    = "TRPC_SUBJECT_ID"
	// #nosec G101 -- environment variable name, not a credential.
	envModelAPIKey       = "TRPC_MODEL_API_KEY"
	envModelProvider     = "TRPC_MODEL_PROVIDER"
	envModelNames        = "TRPC_MODEL_NAMES"
	envModelEndpointHost = "TRPC_MODEL_ENDPOINT_HOSTS"
	// #nosec G101 -- environment variable name, not a secret.
	envModelSecretRef = "TRPC_MODEL_SECRET_REF"
	envSessionBackend = "TRPC_SESSION_BACKEND"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComCallbackToken  = "WECOM_CALLBACK_TOKEN"
	envWeComEncodingAESKey = "WECOM_ENCODING_AES_KEY"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComAppSecret = "WECOM_APP_SECRET"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComSecretRef = "WECOM_SECRET_REF"

	defaultModelProvider = "openai"
	defaultModelNames    = "gpt-4o-mini"
	defaultEndpointHost  = "api.openai.com"
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
	adminToken     string
	adminTenants   []string
	tenantID       string
	appID          string
	subjectID      string
	modelAPIKey    string
	modelProvider  string
	modelNames     []string
	endpointHosts  []string
	secretRef      string
	runtimeStorage string
	wecom          *environmentWeComConfig
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
	modelCatalog, backendCatalog, err := environmentCatalogs(config)
	if err != nil {
		return nil, err
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{
		config.apiToken: {TenantID: config.tenantID, AppID: config.appID, SubjectID: config.subjectID},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: API authenticator configuration is invalid", ErrInvalidConfig)
	}
	adminAuthenticator, err := admin.NewStaticAuthenticator(config.adminToken, config.adminTenants)
	if err != nil {
		return nil, fmt.Errorf("%w: Admin authenticator configuration is invalid", ErrInvalidConfig)
	}
	var db *sql.DB
	openDatabase := openEnvironmentDatabase
	applyMigrations := applyEnvironmentMigrations
	verifyMigrations := verifyEnvironmentMigrations
	if config.driver == ControlPlaneDriverMySQL {
		migrationDB, migrationErr := openMySQLEnvironmentDatabase(ctx, config.migrationDSN, mysql.Options{MaxOpenConns: 4, MaxIdleConns: 4})
		var migrationUser string
		if migrationErr == nil {
			migrationUser, migrationErr = mysql.CurrentUser(ctx, migrationDB)
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
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("%w: MySQL migrations are not ready", ErrInvalidConfig)
		}
		db, err = openMySQLEnvironmentDatabase(ctx, config.dsn, mysql.Options{MaxOpenConns: 8, MaxIdleConns: 8})
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("%w: mysql control plane is unavailable", ErrInvalidConfig)
		}
		// The application DSN is deliberately verification-only during bootstrap;
		// migration DDL is executed through the separately provisioned account.
		applyMigrations = nil
		// VerifyMySQL reads trigger metadata, which is intentionally unavailable to
		// the restricted application account. The migration connection has already
		// verified the shared schema before it is closed.
		verifyMigrations = nil
		applicationUser, userErr := mysql.CurrentUser(ctx, db)
		if userErr != nil || applicationUser == migrationUser {
			if db != nil {
				_ = db.Close()
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("%w: MySQL migration and application accounts must be distinct", ErrInvalidConfig)
		}
	} else {
		db, err = openDatabase(ctx, config.dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %s control plane is unavailable", ErrInvalidConfig, config.driver)
	}
	delegateSessions := inmemory.NewSessionService()
	runtimeStore, err := environmentRuntimeStore(config.runtimeStorage, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = db.Close()
		return nil, err
	}
	var tenantRepo tenant.Repository
	var appRepo agent.Repository
	var channelRepo channels.CandidateConsumer
	var auditWriter audit.Writer
	if config.driver == ControlPlaneDriverMySQL {
		tenantRepo = tenantmysql.NewRepository(db)
		appRepo = agentmysql.NewRepository(db)
		channelRepo = channelmysql.NewRepository(db)
	} else {
		tenantRepo = tenantpostgres.NewRepository(db)
		appRepo = agentpostgres.NewRepository(db)
		channelRepo = channelpostgres.NewRepository(db)
		auditWriter, err = auditpostgres.New(db, config.tenantID)
		if err != nil {
			_ = delegateSessions.Close()
			_ = runtimeStore.Close()
			_ = db.Close()
			return nil, ErrInvalidConfig
		}
	}
	var wecomFactory func(gateway.DispatchService) (http.Handler, error)
	var wecomWorker *outbox.Worker
	if config.wecom != nil {
		credentials := environmentWeComCredentialResolver{tenantID: config.tenantID, config: *config.wecom}
		wecomFactory = func(dispatcher gateway.DispatchService) (http.Handler, error) {
			return wecom.New(wecom.Config{Candidates: channelRepo, Tenants: tenantRepo, Apps: appRepo, Credentials: credentials, Dispatcher: dispatcher, AuditWriter: auditWriter})
		}
		owner, ownerErr := environmentWeComOwnerFunc()
		if ownerErr != nil {
			_ = delegateSessions.Close()
			_ = runtimeStore.Close()
			_ = db.Close()
			return nil, ErrInvalidConfig
		}
		wecomWorker, err = newEnvironmentWeComWorker(outbox.Config{Store: runtimeStore, Provider: &wecom.BindingProvider{Bindings: channelRepo, Credentials: credentials}, TenantID: config.tenantID, Owner: owner, LeaseDuration: 30 * time.Second, AuditWriter: auditWriter})
		if err != nil {
			_ = delegateSessions.Close()
			_ = runtimeStore.Close()
			_ = db.Close()
			return nil, ErrInvalidConfig
		}
	}
	graph, err := NewWithDatabase(ctx, db, Config{
		OwnDB:               true,
		ControlPlaneDriver:  config.driver,
		Tenants:             tenantRepo,
		Apps:                appRepo,
		Channels:            channelRepo,
		ModelCatalog:        modelCatalog,
		BackendCatalog:      backendCatalog,
		SecretResolver:      environmentSecretResolver{reference: config.secretRef, value: config.modelAPIKey},
		ModelFactory:        environmentModelFactory{},
		Sessions:            delegateSessions,
		RuntimeStore:        runtimeStore,
		RuntimeTenantID:     config.tenantID,
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
		_ = db.Close()
		return nil, err
	}
	return graph, nil
}

func loadEnvironment() (environmentConfig, error) {
	config := baseEnvironmentConfig()
	for _, load := range []func(*environmentConfig) error{
		selectEnvironmentDatabase,
		loadEnvironmentCredentials,
		loadEnvironmentModelConfig,
		validateEnvironmentRuntime,
	} {
		if err := load(&config); err != nil {
			return environmentConfig{}, err
		}
	}
	wecom, err := loadEnvironmentWeCom()
	if err != nil {
		return environmentConfig{}, err
	}
	config.wecom = wecom
	return config, nil
}

func baseEnvironmentConfig() environmentConfig {
	return environmentConfig{
		driver:         ControlPlaneDriver(strings.ToLower(strings.TrimSpace(environmentOrDefault(envControlPlaneDriver, string(ControlPlaneDriverPostgres))))),
		modelProvider:  environmentOrDefault(envModelProvider, defaultModelProvider),
		secretRef:      environmentOrDefault(envModelSecretRef, defaultModelSecretRef),
		subjectID:      environmentOrDefault(envSubjectID, defaultSubjectID),
		runtimeStorage: strings.ToLower(strings.TrimSpace(os.Getenv(envSessionBackend))),
	}
}

func selectEnvironmentDatabase(config *environmentConfig) error {
	if config.driver != ControlPlaneDriverPostgres && config.driver != ControlPlaneDriverMySQL {
		return fmt.Errorf("%w: %s must be postgres or mysql", ErrInvalidConfig, envControlPlaneDriver)
	}
	name := envPostgresDSN
	if config.driver == ControlPlaneDriverMySQL {
		name = envMySQLDSN
	}
	dsn, err := requiredEnvironment(name)
	if err != nil {
		return err
	}
	config.dsn = dsn
	if config.driver == ControlPlaneDriverMySQL {
		migrationDSN, err := requiredEnvironment(envMySQLMigrationDSN)
		if err != nil {
			return err
		}
		config.migrationDSN = migrationDSN
	}
	return nil
}

func loadEnvironmentCredentials(config *environmentConfig) error {
	values := []struct {
		name   string
		target *string
	}{
		{name: envAPIToken, target: &config.apiToken},
		{name: envTenantID, target: &config.tenantID},
		{name: envAppID, target: &config.appID},
		{name: envAdminToken, target: &config.adminToken},
		{name: envModelAPIKey, target: &config.modelAPIKey},
	}
	for _, value := range values {
		loaded, err := requiredEnvironment(value.name)
		if err != nil {
			return err
		}
		*value.target = loaded
	}
	adminTenantValue, err := requiredEnvironment(envAdminTenants)
	if err != nil {
		return err
	}
	config.adminTenants, err = environmentList(envAdminTenants, adminTenantValue, false)
	return err
}

func loadEnvironmentModelConfig(config *environmentConfig) error {
	config.modelProvider = strings.ToLower(strings.TrimSpace(config.modelProvider))
	config.secretRef = strings.TrimSpace(config.secretRef)
	if config.modelProvider == "" || config.secretRef == "" {
		return fmt.Errorf("%w: model provider and secret reference are required", ErrInvalidConfig)
	}
	var err error
	config.modelNames, err = environmentList(envModelNames, environmentOrDefault(envModelNames, defaultModelNames), true)
	if err != nil {
		return err
	}
	config.endpointHosts, err = environmentList(envModelEndpointHost, environmentOrDefault(envModelEndpointHost, defaultEndpointHost), true)
	if err != nil {
		return err
	}
	config.subjectID = strings.TrimSpace(config.subjectID)
	return nil
}

func validateEnvironmentRuntime(config *environmentConfig) error {
	if config.runtimeStorage != "postgres" && config.runtimeStorage != "inmemory" {
		return fmt.Errorf("%w: %s must be explicitly set to postgres or inmemory", ErrInvalidConfig, envSessionBackend)
	}
	if config.driver == ControlPlaneDriverMySQL && config.runtimeStorage == "postgres" {
		return fmt.Errorf("%w: %s=postgres is not available with MySQL control plane; use inmemory until a MySQL runtime adapter is selected", ErrInvalidConfig, envSessionBackend)
	}
	return nil
}

func loadEnvironmentWeCom() (*environmentWeComConfig, error) {
	values := []string{strings.TrimSpace(os.Getenv(envWeComCallbackToken)), strings.TrimSpace(os.Getenv(envWeComEncodingAESKey)), strings.TrimSpace(os.Getenv(envWeComAppSecret)), strings.TrimSpace(os.Getenv(envWeComSecretRef))}
	configured := 0
	for _, value := range values {
		if value != "" {
			configured++
		}
	}
	if configured != 0 && configured != len(values) {
		return nil, fmt.Errorf("%w: WeCom credentials must be configured together", ErrInvalidConfig)
	}
	if configured == 0 {
		return nil, nil
	}
	return &environmentWeComConfig{callbackToken: values[0], encodingAESKey: values[1], appSecret: values[2], secretRef: values[3]}, nil
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
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider:        "inmemory",
		Capabilities:    []backend.Capability{backend.CapabilitySession},
		EndpointPolicy:  backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden,
		Options:         map[string]backend.OptionSpec{},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: backend catalog is invalid", ErrInvalidConfig)
	}
	return modelCatalog, backendCatalog, nil
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

func (environmentModelFactory) New(ctx context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	if ctx == nil {
		return nil, errors.New("model factory context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	apiKey := secret.Value()
	if apiKey == "" {
		return nil, errors.New("model factory secret is required")
	}
	provider := strings.ToLower(strings.TrimSpace(input.Provider))
	if provider != "" && provider != defaultModelProvider {
		return nil, fmt.Errorf("model factory provider %q is unsupported", input.Provider)
	}
	options := []openai.Option{openai.WithAPIKey(apiKey)}
	if endpoint := strings.TrimSpace(input.Endpoint); endpoint != "" {
		options = append(options, openai.WithBaseURL(endpoint))
	}
	return openai.New(input.Model, options...), nil
}

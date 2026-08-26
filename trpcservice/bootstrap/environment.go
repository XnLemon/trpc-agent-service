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
	agentpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/agent/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	auditpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/audit/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimesessionpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/sessionpostgres"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	envPostgresDSN = "TRPC_POSTGRES_DSN"
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
	openEnvironmentDatabase     = postgres.Open
	applyEnvironmentMigrations  = migrations.Apply
	verifyEnvironmentMigrations = migrations.Verify
	environmentWeComOwnerFunc   = environmentWeComOwner
	newEnvironmentWeComWorker   = outbox.New
)

// environmentConfig is intentionally private: it contains the one secret
// handed to the ModelFactory and must not become a serializable application
// configuration object.
type environmentConfig struct {
	dsn            string
	apiToken       string
	apiIdentities  map[string]gateway.APIIdentity
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
	authenticator, err := gateway.NewStaticAPIAuthenticator(config.apiIdentities)
	if err != nil {
		return nil, fmt.Errorf("%w: API authenticator configuration is invalid", ErrInvalidConfig)
	}
	adminAuthenticator, err := admin.NewStaticAuthenticator(config.adminToken, config.adminTenants)
	if err != nil {
		return nil, fmt.Errorf("%w: Admin authenticator configuration is invalid", ErrInvalidConfig)
	}
	db, err := openEnvironmentDatabase(ctx, config.dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: PostgreSQL control plane is unavailable", ErrInvalidConfig)
	}
	delegateSessions := inmemory.NewSessionService()
	runtimeStore, err := environmentRuntimeStore(config.runtimeStorage, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = db.Close()
		return nil, err
	}
	tenantRepo := tenantpostgres.NewRepository(db)
	appRepo := agentpostgres.NewRepository(db)
	channelRepo := channelpostgres.NewRepository(db)
	var auditWriter audit.Writer
	if len(config.apiIdentities) > 1 {
		auditWriter = auditpostgres.NewMultiTenant(db)
	} else {
		auditWriter, err = auditpostgres.New(db, config.tenantID)
	}
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStore.Close()
		_ = db.Close()
		return nil, ErrInvalidConfig
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
	secretRegistry := modelprofile.NewSecretRegistry()
	modelRegistry := modelprofile.NewModelProviderRegistry()
	backendRegistry := backend.NewProviderRegistry()
	for _, identity := range config.apiIdentities {
		if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.secretRef}, config.modelAPIKey); err != nil {
			_ = delegateSessions.Close()
			_ = runtimeStore.Close()
			_ = db.Close()
			return nil, ErrInvalidConfig
		}
		if err := modelRegistry.Register(identity.TenantID, config.modelProvider, environmentModelFactory{}); err != nil {
			_ = delegateSessions.Close()
			_ = runtimeStore.Close()
			_ = db.Close()
			return nil, ErrInvalidConfig
		}
		if err := backendRegistry.Register(identity.TenantID, backend.CapabilitySession, "inmemory", environmentSessionCapabilityProvider{delegate: delegateSessions, store: runtimeStore}); err != nil {
			_ = delegateSessions.Close()
			_ = runtimeStore.Close()
			_ = db.Close()
			return nil, ErrInvalidConfig
		}
	}
	storageFactory, err := backend.NewRegistryStorageFactory(backendRegistry, secretRegistry)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStore.Close()
		_ = db.Close()
		return nil, ErrInvalidConfig
	}
	graph, err := NewWithDatabase(ctx, db, Config{
		OwnDB:               true,
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
			return postgres.Ping(pingContext, db)
		},
		Migrate:          applyEnvironmentMigrations,
		VerifyMigrations: verifyEnvironmentMigrations,
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
	config := environmentConfig{
		modelProvider:  environmentOrDefault(envModelProvider, defaultModelProvider),
		secretRef:      environmentOrDefault(envModelSecretRef, defaultModelSecretRef),
		subjectID:      environmentOrDefault(envSubjectID, defaultSubjectID),
		runtimeStorage: strings.ToLower(strings.TrimSpace(os.Getenv(envSessionBackend))),
	}
	var err error
	if config.dsn, err = requiredEnvironment(envPostgresDSN); err != nil {
		return environmentConfig{}, err
	}
	if identities := strings.TrimSpace(os.Getenv(envAPIIdentities)); identities != "" {
		config.apiIdentities, err = parseEnvironmentAPIIdentities(identities)
		if err != nil {
			return environmentConfig{}, err
		}
		if len(config.apiIdentities) == 1 {
			for _, identity := range config.apiIdentities {
				config.tenantID, config.appID = identity.TenantID, identity.AppID
			}
		}
	} else {
		if config.apiToken, err = requiredEnvironment(envAPIToken); err != nil {
			return environmentConfig{}, err
		}
		if config.tenantID, err = requiredEnvironment(envTenantID); err != nil {
			return environmentConfig{}, err
		}
		if config.appID, err = requiredEnvironment(envAppID); err != nil {
			return environmentConfig{}, err
		}
		config.apiIdentities = map[string]gateway.APIIdentity{config.apiToken: {TenantID: config.tenantID, AppID: config.appID, SubjectID: config.subjectID}}
	}
	if config.adminToken, err = requiredEnvironment(envAdminToken); err != nil {
		return environmentConfig{}, err
	}
	adminTenantValue, err := requiredEnvironment(envAdminTenants)
	if err != nil {
		return environmentConfig{}, err
	}
	adminTenants, err := environmentList(envAdminTenants, adminTenantValue, false)
	if err != nil {
		return environmentConfig{}, err
	}
	config.adminTenants = adminTenants
	if config.modelAPIKey, err = requiredEnvironment(envModelAPIKey); err != nil {
		return environmentConfig{}, err
	}
	config.modelProvider = strings.ToLower(strings.TrimSpace(config.modelProvider))
	config.secretRef = strings.TrimSpace(config.secretRef)
	if config.modelProvider == "" || config.secretRef == "" {
		return environmentConfig{}, fmt.Errorf("%w: model provider and secret reference are required", ErrInvalidConfig)
	}
	config.modelNames, err = environmentList(envModelNames, environmentOrDefault(envModelNames, defaultModelNames), true)
	if err != nil {
		return environmentConfig{}, err
	}
	config.endpointHosts, err = environmentList(envModelEndpointHost, environmentOrDefault(envModelEndpointHost, defaultEndpointHost), true)
	if err != nil {
		return environmentConfig{}, err
	}
	config.subjectID = strings.TrimSpace(config.subjectID)
	if config.runtimeStorage != "postgres" && config.runtimeStorage != "inmemory" {
		return environmentConfig{}, fmt.Errorf("%w: %s must be explicitly set to postgres or inmemory", ErrInvalidConfig, envSessionBackend)
	}
	wecomValues := []string{strings.TrimSpace(os.Getenv(envWeComCallbackToken)), strings.TrimSpace(os.Getenv(envWeComEncodingAESKey)), strings.TrimSpace(os.Getenv(envWeComAppSecret)), strings.TrimSpace(os.Getenv(envWeComSecretRef))}
	configured := 0
	for _, value := range wecomValues {
		if value != "" {
			configured++
		}
	}
	if configured != 0 && configured != len(wecomValues) {
		return environmentConfig{}, fmt.Errorf("%w: WeCom credentials must be configured together", ErrInvalidConfig)
	}
	if configured == len(wecomValues) {
		config.wecom = &environmentWeComConfig{callbackToken: wecomValues[0], encodingAESKey: wecomValues[1], appSecret: wecomValues[2], secretRef: wecomValues[3]}
	}
	if config.wecom != nil && len(config.apiIdentities) != 1 {
		return environmentConfig{}, fmt.Errorf("%w: WeCom credentials require exactly one API identity", ErrInvalidConfig)
	}
	return config, nil
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
	delegate session.Service
	store    runtimestorage.RuntimeStore
}

func (provider environmentSessionCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, _ backend.CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	return runtimesessionpostgres.New(input.TenantID, provider.delegate, provider.store)
}

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

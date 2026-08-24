package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/XnLemon/trpc-agent-service/migrations"
	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	envPostgresDSN       = "TRPC_POSTGRES_DSN"
	envAPIToken          = "TRPC_API_TOKEN"
	envTenantID          = "TRPC_TENANT_ID"
	envAppID             = "TRPC_APP_ID"
	envAdminToken        = "TRPC_ADMIN_TOKEN"
	envAdminTenants      = "TRPC_ADMIN_TENANTS"
	envSubjectID         = "TRPC_SUBJECT_ID"
	envModelAPIKey       = "TRPC_MODEL_API_KEY"
	envModelProvider     = "TRPC_MODEL_PROVIDER"
	envModelNames        = "TRPC_MODEL_NAMES"
	envModelEndpointHost = "TRPC_MODEL_ENDPOINT_HOSTS"
	envModelSecretRef    = "TRPC_MODEL_SECRET_REF"
	envRuntimeStorage    = "TRPC_RUNTIME_STORAGE"

	defaultModelProvider  = "openai"
	defaultModelNames     = "gpt-4o-mini"
	defaultEndpointHost   = "api.openai.com"
	defaultModelSecretRef = "env/trpc-model-api-key"
	defaultSubjectID      = "service"
)

var (
	openEnvironmentDatabase     = postgres.Open
	applyEnvironmentMigrations  = migrations.Apply
	verifyEnvironmentMigrations = migrations.Verify
)

// environmentConfig is intentionally private: it contains the one secret
// handed to the ModelFactory and must not become a serializable application
// configuration object.
type environmentConfig struct {
	dsn            string
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
	db, err := openEnvironmentDatabase(ctx, config.dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: PostgreSQL control plane is unavailable", ErrInvalidConfig)
	}
	sessions := inmemory.NewSessionService()
	runtimeStore, err := environmentRuntimeStore(config.runtimeStorage, db)
	if err != nil {
		_ = sessions.Close()
		_ = db.Close()
		return nil, err
	}
	graph, err := NewWithDatabase(ctx, db, Config{
		OwnDB:              true,
		ModelCatalog:       modelCatalog,
		BackendCatalog:     backendCatalog,
		SecretResolver:     environmentSecretResolver{reference: config.secretRef, value: config.modelAPIKey},
		ModelFactory:       environmentModelFactory{},
		Sessions:           sessions,
		RuntimeStore:       runtimeStore,
		Authenticator:      authenticator,
		AdminAuthenticator: adminAuthenticator,
		Ping: func(pingContext context.Context) error {
			return postgres.Ping(pingContext, db)
		},
		Migrate:          applyEnvironmentMigrations,
		VerifyMigrations: verifyEnvironmentMigrations,
		CloseDependencies: func() error {
			return errors.Join(sessions.Close(), runtimeStore.Close())
		},
	})
	if err != nil {
		_ = sessions.Close()
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
		runtimeStorage: strings.ToLower(environmentOrDefault(envRuntimeStorage, "postgres")),
	}
	var err error
	if config.dsn, err = requiredEnvironment(envPostgresDSN); err != nil {
		return environmentConfig{}, err
	}
	if config.apiToken, err = requiredEnvironment(envAPIToken); err != nil {
		return environmentConfig{}, err
	}
	if config.tenantID, err = requiredEnvironment(envTenantID); err != nil {
		return environmentConfig{}, err
	}
	if config.appID, err = requiredEnvironment(envAppID); err != nil {
		return environmentConfig{}, err
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
		return environmentConfig{}, fmt.Errorf("%w: %s must be postgres or inmemory", ErrInvalidConfig, envRuntimeStorage)
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

type environmentSecretResolver struct {
	reference string
	value     string
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

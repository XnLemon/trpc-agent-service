package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestParseEnvironmentAPIIdentities(t *testing.T) {
	value := "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a, token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b"
	identities, err := parseEnvironmentAPIIdentities(value)
	if err != nil || len(identities) != 2 {
		t.Fatalf("parse identities = %+v, %v", identities, err)
	}
	if identities["token-b"].TenantID != "t_00000000000000000000000001" {
		t.Fatalf("second identity = %+v", identities["token-b"])
	}
	for _, invalid := range []string{"token|tenant|app", "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service,token-a|t_00000000000000000000000001|app_00000000000000000000000001|service", "token| |app|subject"} {
		if _, err := parseEnvironmentAPIIdentities(invalid); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid identities %q error = %v", invalid, err)
		}
	}
}

func TestLoadEnvironmentSupportsIdentityListWithoutFixedTenantFields(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "")
	t.Setenv(envModelAPIKeys, "t_00000000000000000000000000=key-a,t_00000000000000000000000001=key-b")
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	config, err := loadEnvironment()
	if err != nil || len(config.apiIdentities) != 2 {
		t.Fatalf("multi-tenant environment = %+v, %v", config, err)
	}
	if config.tenantID != "" || config.appID != "" {
		t.Fatal("multi-tenant environment selected a process-fixed identity")
	}
	if _, err := gateway.NewStaticAPIAuthenticator(config.apiIdentities); err != nil {
		t.Fatal(err)
	}
}

func TestLoadEnvironmentUsesTenantModelAPIKeysForMultipleIdentities(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "")
	t.Setenv(envModelAPIKeys, "t_00000000000000000000000000=key-a,t_00000000000000000000000001=key-b")
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.modelAPIKey != "" || config.modelAPIKeys["t_00000000000000000000000000"] != "key-a" || config.modelAPIKeys["t_00000000000000000000000001"] != "key-b" {
		t.Fatalf("tenant model keys = %#v, global=%q", config.modelAPIKeys, config.modelAPIKey)
	}
}

func TestParseEnvironmentModelAPIKeysRejectsMalformedOrDuplicateEntries(t *testing.T) {
	for _, value := range []string{"", ",", "tenant=", "=key", "tenant\n=key", "tenant=one,tenant=two"} {
		if _, err := parseEnvironmentModelAPIKeys(value); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("parse %q error = %v", value, err)
		}
	}
	keys, err := parseEnvironmentModelAPIKeys("tenant-a=key-a, tenant-b=key-b")
	if err != nil || keys["tenant-a"] != "key-a" || keys["tenant-b"] != "key-b" {
		t.Fatalf("parsed model keys = %#v, err=%v", keys, err)
	}
}

func TestLoadEnvironmentRequiresEveryTenantModelAPIKey(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "")
	t.Setenv(envModelAPIKeys, "t_00000000000000000000000000=key-a")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("incomplete tenant model keys error = %v", err)
	}
}

func TestLoadEnvironmentRejectsGlobalModelKeyForMultipleIdentities(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "shared-key")
	t.Setenv(envModelAPIKeys, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("global model key in multi-tenant mode error = %v", err)
	}
}

func TestLoadEnvironmentRejectsSingleWeComCredentialSetForMultipleIdentities(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envWeComCallbackToken, "callback")
	t.Setenv(envWeComEncodingAESKey, "aes")
	t.Setenv(envWeComAppSecret, "secret")
	t.Setenv(envWeComSecretRef, "env/wecom")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("multi-identity WeCom config error = %v", err)
	}
}

func TestEnvironmentCredentialResolversFailClosedByScope(t *testing.T) {
	wecomResolver := environmentWeComCredentialResolver{tenantID: "t_00000000000000000000000000", config: environmentWeComConfig{callbackToken: "callback", encodingAESKey: "aes", appSecret: "app", secretRef: "env/wecom"}}
	scope := channels.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "env/wecom"}
	credentials, err := wecomResolver.Resolve(context.Background(), scope)
	if err != nil || credentials.CallbackToken != "callback" {
		t.Fatalf("WeCom Resolve() = %+v, %v", credentials, err)
	}
	foreign := scope
	foreign.TenantID = "t_00000000000000000000000001"
	if _, err := wecomResolver.Resolve(context.Background(), foreign); err == nil {
		t.Fatal("foreign WeCom scope was accepted")
	}
	if _, err := wecomResolver.Resolve(nil, scope); err == nil {
		t.Fatal("nil WeCom context was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := wecomResolver.Resolve(canceled, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WeCom Resolve() = %v", err)
	}

	modelResolver := environmentSecretResolver{reference: "env/model", value: "model-secret"}
	modelScope := modelprofile.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "env/model"}
	value, err := modelResolver.Resolve(context.Background(), modelScope)
	if err != nil || value.Value() != "model-secret" {
		t.Fatalf("model Resolve() = %q, %v", value.Value(), err)
	}
	modelScope.SecretRef = "other"
	if _, err := modelResolver.Resolve(context.Background(), modelScope); err == nil {
		t.Fatal("foreign model scope was accepted")
	}
}

func TestEnvironmentSessionCapabilityProviderBoundaries(t *testing.T) {
	provider := environmentSessionCapabilityProvider{}
	if _, err := provider.New(nil, backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000"}, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil provider context = %v", err)
	}
	store := runtimestorageinmemory.New()
	delegate := inmemory.NewSessionService()
	value, err := (environmentSessionCapabilityProvider{delegate: delegate, store: store}).New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000"}, backend.CapabilityBinding{}, modelprofile.SecretValue{})
	if err != nil || value == nil {
		t.Fatalf("session capability = %v, %v", value, err)
	}
	if err := value.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	_ = delegate.Close()
	_ = store.Close()
}

func TestNewFromEnvironmentMigrationAndReadinessFailures(t *testing.T) {
	setRequiredEnvironment(t)
	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	defer func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		verifyEnvironmentMigrations = previousVerify
	}()
	for _, test := range []struct {
		name     string
		applyErr error
	}{
		{name: "migration", applyErr: errors.New("migration failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open("trpc-service-bootstrap-ping", "")
			if err != nil {
				t.Fatal(err)
			}
			openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
			applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return test.applyErr }
			verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("migration error = %v", err)
			}
			_ = db.Close()
		})
	}

	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return errors.New("verification failed") }
	graph, err := NewFromEnvironment(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if graph.Ready() {
		_ = graph.Close()
		t.Fatal("graph reported ready with failed migration verification")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentCatalogsAndIdentityListBoundaries(t *testing.T) {
	config := environmentConfig{modelProvider: defaultModelProvider, modelNames: []string{"gpt-4o-mini"}, endpointHosts: []string{"api.openai.com"}, secretRef: "env/model"}
	if _, _, err := environmentCatalogs(config); err != nil {
		t.Fatal(err)
	}
	config.modelProvider = "unknown"
	if _, _, err := environmentCatalogs(config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported catalog = %v", err)
	}
	if _, err := parseEnvironmentAPIIdentities(""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty identity list = %v", err)
	}
}

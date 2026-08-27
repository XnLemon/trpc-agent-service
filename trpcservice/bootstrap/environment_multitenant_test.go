package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
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

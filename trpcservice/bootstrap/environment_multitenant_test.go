package bootstrap

import (
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
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

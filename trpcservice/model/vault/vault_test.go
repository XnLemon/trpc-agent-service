package vault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestManagerReadsTenantScopedKVAndRedactsFailures(t *testing.T) {
	var token string
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		token = request.Header.Get("X-Vault-Token")
		if request.URL.Path != "/v1/secret/data/t_00000000000000000000000000/secret/model" {
			t.Fatalf("Vault path = %q", request.URL.Path)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"data": map[string]any{"data": map[string]string{"value": "managed-secret"}}})
	}))
	defer server.Close()
	manager, err := New(Config{BaseURL: server.URL, Token: "vault-token", Mount: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	value, err := manager.Read(context.Background(), modelprofile.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "secret/model"})
	if err != nil || value.Value() != "managed-secret" || token != "vault-token" {
		t.Fatalf("Vault Read() = %q, %v, token=%q", value.Value(), err, token)
	}
	if value.String() != "<redacted-secret>" {
		t.Fatal("Vault secret was not redacted")
	}
	if _, err := manager.Read(context.Background(), modelprofile.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "../other"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("invalid path error = %v", err)
	}
	if _, err := New(Config{BaseURL: strings.Replace(server.URL, "https://", "http://", 1), Token: "token", Mount: "secret"}); err == nil {
		t.Fatal("Vault manager accepted non-HTTPS endpoint")
	}
	if strings.Contains(value.String(), "managed-secret") {
		t.Fatal("Vault secret leaked in String")
	}
}

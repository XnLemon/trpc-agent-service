package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

func testHandler(t *testing.T) (*Handler, *StaticAuthenticator) {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{Provider: "openai", Models: []string{"gpt-4o-mini"}, EndpointPolicy: modelprofile.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"api.openai.com"}, SecretRefPolicy: modelprofile.FieldRequired})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{}})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewStaticAuthenticator("admin-token", []string{"*"})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(Config{Tenants: tenantmemory.NewRepository(), Apps: inmemory.NewRepository(), Models: modelmemory.NewRepository(modelCatalog), Backends: backendmemory.NewRepository(backendCatalog), Bindings: channelmemory.NewRepository(), Authenticator: auth})
	if err != nil {
		t.Fatal(err)
	}
	return handler, auth
}

func TestAdminTenantCreateAndReadUseIndependentPrincipal(t *testing.T) {
	handler, _ := testHandler(t)
	create := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader("{\"tenant_key\":\"acme\",\"display_name\":\"Acme\"}"))
	create.Header.Set("Authorization", "Bearer admin-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, create)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", response.Code, response.Body.String())
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	var created struct {
		TenantID  string
		TenantKey string
	}
	if err := json.Unmarshal(envelope["data"], &created); err != nil {
		t.Fatal(err)
	}
	if created.TenantID == "" || created.TenantKey != "acme" {
		t.Fatalf("created tenant = %+v", created)
	}

	read := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/"+created.TenantID, nil)
	read.Header.Set("Authorization", "Bearer admin-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, read)
	if response.Code != http.StatusOK {
		t.Fatalf("read status = %d, body=%s", response.Code, response.Body.String())
	}

	ordinary := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/"+created.TenantID, nil)
	ordinary.Header.Set("Authorization", "Bearer chat-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, ordinary)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("ordinary token status = %d", response.Code)
	}
}

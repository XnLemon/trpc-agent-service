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

	// The platform wildcard is limited to first-tenant creation; subsequent
	// resource access requires an explicit tenant-scoped principal.
	scopedAuth, err := NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewHandler(Config{Tenants: handler.config.Tenants, Apps: handler.config.Apps, Models: handler.config.Models, Backends: handler.config.Backends, Bindings: handler.config.Bindings, Authenticator: scopedAuth})
	if err != nil {
		t.Fatal(err)
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

func TestAdminTenantAndAppMetadataUpdatesUsePathScopeAndExpectedVersion(t *testing.T) {
	handler, _ := testHandler(t)
	createTenant := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{"tenant_key":"acme","display_name":"Acme"}`))
	createTenant.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, createTenant)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("tenant status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var tenantEnvelope struct {
		Data struct {
			TenantID string
			Version  int64
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &tenantEnvelope); err != nil {
		t.Fatal(err)
	}
	scoped, err := NewStaticAuthenticator("admin-token", []string{tenantEnvelope.Data.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewHandler(Config{Tenants: handler.config.Tenants, Apps: handler.config.Apps, Models: handler.config.Models, Backends: handler.config.Backends, Bindings: handler.config.Bindings, Authenticator: scoped})
	if err != nil {
		t.Fatal(err)
	}
	createApp := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/"+tenantEnvelope.Data.TenantID+"/apps", strings.NewReader(`{"app_key":"support","display_name":"Support"}`))
	createApp.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, createApp)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("app status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var appEnvelope struct {
		Data struct {
			AppID   string
			Version int64
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &appEnvelope); err != nil {
		t.Fatal(err)
	}
	patchApp := httptest.NewRequest(http.MethodPatch, "/admin/v1/tenants/"+tenantEnvelope.Data.TenantID+"/apps/"+appEnvelope.Data.AppID, strings.NewReader(`{"expected_version":1,"display_name":"Support v2","description":"updated"}`))
	patchApp.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, patchApp)
	if recorder.Code != http.StatusOK {
		t.Fatalf("patch app status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	stale := httptest.NewRequest(http.MethodPatch, "/admin/v1/tenants/"+tenantEnvelope.Data.TenantID+"/apps/"+appEnvelope.Data.AppID, strings.NewReader(`{"expected_version":1,"display_name":"stale","description":"stale"}`))
	stale.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, stale)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("stale patch status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestDecodeBodyPreservesProviderOptionKeys(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/t_01ARZ3NDEKTSV4RRFFQ69G5FAV/models", strings.NewReader(`{"profile_key":"support","display_name":"Support","configuration":{"provider":"openai","model":"gpt-4o-mini","secret_ref":"env/key","options":{"x_custom_option":"keep"}}}`))
	var input modelprofile.CreateInput
	if err := decodeBody(request, &input); err != nil {
		t.Fatal(err)
	}
	if input.Configuration.Options["x_custom_option"] != "keep" {
		t.Fatalf("provider option key was normalized: %#v", input.Configuration.Options)
	}
}

func TestAdminMalformedBodyMapsToBadRequest(t *testing.T) {
	handler, _ := testHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{`))
	request.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("malformed body status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"error":"invalid_request"`) {
		t.Fatalf("malformed body category = %s", recorder.Body.String())
	}
}

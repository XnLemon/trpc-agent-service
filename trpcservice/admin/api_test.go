package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
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

func TestAdminGlobalFirstTenantCreationIsSerialized(t *testing.T) {
	handler, _ := testHandler(t)
	const attempts = 16
	statuses := make(chan int, attempts)
	var group sync.WaitGroup
	group.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(index int) {
			defer group.Done()
			body := "{\"tenant_key\":\"parallel-" + strconv.Itoa(index) + "\",\"display_name\":\"Parallel\"}"
			request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer admin-token")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			statuses <- recorder.Code
		}(i)
	}
	group.Wait()
	close(statuses)
	created := 0
	for status := range statuses {
		if status == http.StatusCreated {
			created++
		} else if status != http.StatusForbidden {
			t.Fatalf("parallel first-tenant status = %d, want 201 or 403", status)
		}
	}
	if created != 1 {
		t.Fatalf("parallel first-tenant creates = %d, want exactly one", created)
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
	var profile modelprofile.CreateInput
	profileRequest := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/t_01ARZ3NDEKTSV4RRFFQ69G5FAV/models", strings.NewReader(`{"profile_key":"support-model","display_name":"Support"}`))
	if err := decodeBody(profileRequest, &profile); err != nil || profile.ProfileKey != "support-model" {
		t.Fatalf("profile_key decode = %q, %v", profile.ProfileKey, err)
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

func TestAdminEmptyBodyMapsToBadRequest(t *testing.T) {
	handler, _ := testHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "\"error\":\"invalid_request\"") {
		t.Fatalf("empty body status/category = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminMapsMalformedBodiesAcrossWriteRoutes(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "malformed-routes", DisplayName: "Malformed Routes"})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator, err = NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	base := "/admin/v1/tenants/" + created.TenantID
	routes := []struct{ method, path string }{
		{http.MethodPost, base + "/status"}, {http.MethodPost, base + "/apps"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions"},
		{http.MethodPatch, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1/publish"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/rollback"},
		{http.MethodPost, base + "/models"}, {http.MethodPatch, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, {http.MethodPost, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodPost, base + "/backends"}, {http.MethodPatch, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, {http.MethodPost, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodPost, base + "/bindings"}, {http.MethodPatch, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, {http.MethodPost, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
	}
	for _, route := range routes {
		request := httptest.NewRequest(route.method, route.path, strings.NewReader(`{`))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s %s status = %d, body=%s", route.method, route.path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestAdminRejectsMalformedRevisionAndExtraRouteSegments(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "route-boundary", DisplayName: "Route Boundary"})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator, err = NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	base := "/admin/v1/tenants/" + created.TenantID + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	cases := []string{
		base + "/status/extra",
		base + "/rollback/extra",
		base + "/revisions/not-a-number/publish",
		base + "/revisions/1/publish/extra",
	}
	for _, path := range cases {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound && recorder.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	request := httptest.NewRequest(http.MethodPost, base+"/revisions/not-a-number/publish", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"error":"invalid_request"`) {
		t.Fatalf("malformed revision response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminRouteSurfaceDispatchesEveryControlPlaneOperation(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "route-test", DisplayName: "Route Test"})
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator = scoped
	base := "/admin/v1/tenants/" + created.TenantID
	routes := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPatch, base, `{}`},
		{http.MethodPost, base + "/status", `{}`},
		{http.MethodPost, base + "/apps", `{}`},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions", `{}`},
		{http.MethodPatch, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1/publish", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/rollback", `{}`},
		{http.MethodPost, base + "/models", `{}`},
		{http.MethodGet, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
		{http.MethodPost, base + "/backends", `{}`},
		{http.MethodGet, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
		{http.MethodPost, base + "/bindings", `{}`},
		{http.MethodGet, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
	}
	for _, route := range routes {
		request := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code == http.StatusInternalServerError {
			t.Fatalf("%s %s returned 500: %s", route.method, route.path, recorder.Body.String())
		}
	}
}

func TestAdminHappyPathCoversResourceMutations(t *testing.T) {
	handler, _ := testHandler(t)
	root, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "happy", DisplayName: "Happy"})
	if err != nil {
		t.Fatal(err)
	}
	principal := Principal{SubjectID: "admin", TenantScopes: map[string]struct{}{root.TenantID: {}}}
	request := func(method, body string) *http.Request {
		return httptest.NewRequest(method, "/admin/v1", strings.NewReader(body))
	}
	modelBody := "{\"profile_key\":\"primary\",\"display_name\":\"Primary\",\"reason\":\"create\",\"correlation_id\":\"happy-1\",\"configuration\":{\"provider\":\"openai\",\"model\":\"gpt-4o-mini\",\"secret_ref\":\"env/key\"}}"
	var decodedModel modelprofile.CreateInput
	if err := decodeBody(request(http.MethodPost, modelBody), &decodedModel); err != nil || decodedModel.Configuration.SecretRef == "" {
		t.Fatalf("decoded model input = %+v, normalized=%#v, err=%v", decodedModel, normalizeKeys(map[string]any{"configuration": map[string]any{"secret_ref": "env/key"}}), err)
	}
	status, modelValue, err := handler.models(context.Background(), request(http.MethodPost, modelBody), principal, root.TenantID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("model create = %d, %v", status, err)
	}
	modelID := modelValue.(map[string]any)["profile"].(*modelprofile.Profile).ProfileID
	if status, _, err := handler.models(context.Background(), request(http.MethodGet, ""), principal, root.TenantID, []string{modelID}); err != nil || status != http.StatusOK {
		t.Fatalf("model get = %d, %v", status, err)
	}
	backendBody := "{\"profile_key\":\"primary\",\"display_name\":\"Primary\",\"reason\":\"create\",\"correlation_id\":\"happy-2\",\"bindings\":[{\"capability\":\"session\",\"provider\":\"inmemory\"}]}"
	status, backendValue, err := handler.backends(context.Background(), request(http.MethodPost, backendBody), principal, root.TenantID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("backend create = %d, %v", status, err)
	}
	backendID := backendValue.(map[string]any)["profile"].(*backend.Profile).ProfileID
	if status, _, err := handler.backends(context.Background(), request(http.MethodGet, ""), principal, root.TenantID, []string{backendID}); err != nil || status != http.StatusOK {
		t.Fatalf("backend get = %d, %v", status, err)
	}
	app, err := handler.config.Apps.Create(context.Background(), agent.CreateInput{TenantID: root.TenantID, AppKey: "support", DisplayName: "Support"})
	if err != nil {
		t.Fatal(err)
	}
	draftBody := "{\"expected_app_version\":1,\"kind\":\"llm\",\"schema_version\":1,\"configuration\":{\"instruction\":\"answer\",\"model_profile_id\":\"mp_01ARZ3NDEKTSV4RRFFQ69G5FAV\"}}"
	status, draftValue, err := handler.revisions(context.Background(), request(http.MethodPost, draftBody), principal, root.TenantID, app.AppID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("draft create = %d, %v", status, err)
	}
	draft := draftValue.(*agent.Revision)
	updateBody := "{\"expected_app_version\":1,\"expected_draft_version\":1,\"configuration\":{\"instruction\":\"answer updated\",\"model_profile_id\":\"mp_01ARZ3NDEKTSV4RRFFQ69G5FAV\"}}"
	if status, _, err := handler.revisions(context.Background(), request(http.MethodPatch, updateBody), principal, root.TenantID, app.AppID, []string{strconv.FormatInt(draft.Revision, 10)}); err != nil || status != http.StatusOK {
		t.Fatalf("draft update = %d, %v", status, err)
	}
	publishBody := "{\"expected_app_version\":1,\"expected_draft_version\":2,\"reason\":\"publish\",\"correlation_id\":\"happy-publish\"}"
	if status, _, err := handler.revisions(context.Background(), request(http.MethodPost, publishBody), principal, root.TenantID, app.AppID, []string{strconv.FormatInt(draft.Revision, 10), "publish"}); err != nil || status != http.StatusOK {
		t.Fatalf("draft publish = %d, %v", status, err)
	}
	bindingBody := "{\"binding_key\":\"primary\",\"channel\":\"wecom\",\"provider_account_id\":\"corp\",\"public_route_key_digest\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"app_id\":\"" + app.AppID + "\",\"secret_ref\":\"secret/corp\",\"reason\":\"create\",\"correlation_id\":\"happy-3\",\"protocol\":{\"wecom\":{\"corp_id\":\"corp\"}}}"
	status, bindingValue, err := handler.bindings(context.Background(), request(http.MethodPost, bindingBody), principal, root.TenantID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("binding create = %d, %v", status, err)
	}
	bindingID := bindingValue.(map[string]any)["binding"].(*channels.Binding).BindingID
	if status, _, err := handler.bindings(context.Background(), request(http.MethodGet, ""), principal, root.TenantID, []string{bindingID}); err != nil || status != http.StatusOK {
		t.Fatalf("binding get = %d, %v", status, err)
	}
}

func TestAdminErrorMappingCategories(t *testing.T) {
	cases := []struct {
		err    error
		status int
	}{
		{ErrUnauthenticated, http.StatusUnauthorized}, {ErrForbidden, http.StatusForbidden}, {errNotFound, http.StatusNotFound},
		{tenant.ErrConflict, http.StatusConflict}, {agent.ErrConflict, http.StatusConflict}, {modelprofile.ErrConflict, http.StatusConflict},
		{backend.ErrConflict, http.StatusConflict}, {channels.ErrConflict, http.StatusConflict}, {postgres.ErrStorage, http.StatusServiceUnavailable},
		{tenant.ErrInvalid, http.StatusBadRequest}, {agent.ErrInvalidTransition, http.StatusBadRequest}, {modelprofile.ErrDisabled, http.StatusBadRequest},
		{backend.ErrInvalidTransition, http.StatusBadRequest}, {channels.ErrDisabled, http.StatusBadRequest}, {errInvalidRequest, http.StatusBadRequest},
		{tenant.ErrDuplicateKey, http.StatusConflict}, {agent.ErrDuplicateKey, http.StatusConflict}, {modelprofile.ErrDuplicateKey, http.StatusConflict},
		{backend.ErrDuplicateKey, http.StatusConflict}, {channels.ErrDuplicateKey, http.StatusConflict},
	}
	for _, tc := range cases {
		status, _ := mapError(tc.err)
		if status != tc.status {
			t.Errorf("mapError(%v) = %d, want %d", tc.err, status, tc.status)
		}
	}
	for _, err := range []error{nil, errors.New("unexpected")} {
		if status, _ := mapError(err); status != http.StatusInternalServerError {
			t.Errorf("mapError(%v) status = %d", err, status)
		}
	}
}

func TestAdminHandlerRejectsInvalidConfigurationAndPaths(t *testing.T) {
	if _, err := NewHandler(Config{}); err == nil {
		t.Fatal("NewHandler accepted an empty configuration")
	}
	handler, _ := testHandler(t)
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/other", nil),
		httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil),
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound && recorder.Code != http.StatusUnauthorized {
			t.Fatalf("unexpected status for invalid admin request: %d", recorder.Code)
		}
	}
}

func TestAdminRejectsUnsupportedMethodsAndRouteShapes(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "method-test", DisplayName: "Method Test"})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator, err = NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	base := "/admin/v1/tenants/" + created.TenantID
	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/admin/v1/tenants"},
		{http.MethodDelete, base},
		{http.MethodGet, base + "/status"},
		{http.MethodGet, base + "/status/extra"},
		{http.MethodGet, base + "/apps"},
		{http.MethodDelete, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1/publish"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/rollback"},
		{http.MethodGet, base + "/models"},
		{http.MethodDelete, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodGet, base + "/backends"},
		{http.MethodDelete, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodGet, base + "/bindings"},
		{http.MethodDelete, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
	}
	for _, tc := range cases {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code == http.StatusInternalServerError {
			t.Errorf("%s %s returned 500: %s", tc.method, tc.path, recorder.Body.String())
		}
	}
}

func TestAdminNormalizationAndBodyBoundaries(t *testing.T) {
	if err := decodeBody(nil, &struct{}{}); !errors.Is(err, errInvalidRequest) {
		t.Fatalf("nil request error = %v", err)
	}
	if got := normalizeKeys([]any{map[string]any{"reason": "why", "correlation_id": "corr"}}); got == nil {
		t.Fatal("normalizeKeys returned nil")
	}
	if toExported("x_unknown_key") != "x_unknown_key" {
		t.Fatal("unknown keys must remain unchanged")
	}
}

func TestGlobalAdminIsFirstTenantOnlyAndCrossTenantReadsAreHidden(t *testing.T) {
	handler, _ := testHandler(t)
	for _, key := range []string{"first", "second"} {
		request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{"tenant_key":"`+key+`","display_name":"Tenant"}`))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		want := http.StatusCreated
		if key == "second" {
			want = http.StatusForbidden
		}
		if recorder.Code != want {
			t.Fatalf("%s tenant status = %d, want %d", key, recorder.Code, want)
		}
	}
	cross := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/t_01ARZ3NDEKTSV4RRFFQ69G5FAV", nil)
	cross.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, cross)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant read status = %d, want 404", recorder.Code)
	}
}

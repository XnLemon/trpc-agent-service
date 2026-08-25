package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

// TestRestartBootstrapRoundTrip proves that two independent production
// bootstrap graphs recover the same Admin-written control-plane state.
func TestRestartBootstrapRoundTrip(t *testing.T) {
	dsn := os.Getenv("POSTGRES_RESTART_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_RESTART_TEST_DSN is not set")
	}
	t.Setenv(envPostgresDSN, dsn)
	t.Setenv(envAPIToken, "chat-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envModelAPIKey, "restart-test-secret")
	t.Setenv(envSessionBackend, "postgres")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	first, err := NewFromEnvironment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		first.HandlerValue().ServeHTTP(recorder, req)
		if strings.Contains(recorder.Body.String(), "restart-test-secret") {
			t.Fatalf("secret leaked in %s %s response: %s", method, path, recorder.Body.String())
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("%s %s response decode: %v; body=%s", method, path, err, recorder.Body.String())
		}
		var data map[string]any
		if err := json.Unmarshal(envelope["data"], &data); err != nil {
			t.Fatalf("%s %s data decode: %v; body=%s", method, path, err, recorder.Body.String())
		}
		return recorder, data
	}
	stringField := func(value map[string]any, names ...string) string {
		for _, name := range names {
			if result, ok := value[name].(string); ok && result != "" {
				return result
			}
		}
		return ""
	}
	nestedStringField := func(value map[string]any, container string, names ...string) string {
		if nested, ok := value[container].(map[string]any); ok {
			return stringField(nested, names...)
		}
		return ""
	}
	status := func(method, path, body string, want int) map[string]any {
		recorder, value := request(method, path, body)
		if recorder.Code != want {
			t.Fatalf("Process A %s %s status = %d, want %d, body=%s", method, path, recorder.Code, want, recorder.Body.String())
		}
		return value
	}
	tenantValue := status(http.MethodPost, "/admin/v1/tenants", "{\"tenant_key\":\"restart-"+suffix+"\",\"display_name\":\"Restart Round Trip\"}", http.StatusCreated)
	tenantID := stringField(tenantValue, "TenantID", "tenant_id")
	if tenantID == "" {
		t.Fatal("Process A tenant response omitted tenant ID")
	}
	appValue := status(http.MethodPost, "/admin/v1/tenants/"+tenantID+"/apps", "{\"app_key\":\"restart-app-"+suffix+"\",\"display_name\":\"Restart App\"}", http.StatusCreated)
	appID := stringField(appValue, "AppID", "app_id")
	modelValue := status(http.MethodPost, "/admin/v1/tenants/"+tenantID+"/models", "{\"profile_key\":\"restart-model-"+suffix+"\",\"display_name\":\"Restart Model\",\"reason\":\"restart-test\",\"correlation_id\":\"restart-model\",\"configuration\":{\"provider\":\"openai\",\"model\":\"gpt-4o-mini\",\"secret_ref\":\"env/trpc-model-api-key\"}}", http.StatusCreated)
	modelID := nestedStringField(modelValue, "profile", "ProfileID", "profile_id")
	backendValue := status(http.MethodPost, "/admin/v1/tenants/"+tenantID+"/backends", "{\"profile_key\":\"restart-backend-"+suffix+"\",\"display_name\":\"Restart Backend\",\"reason\":\"restart-test\",\"correlation_id\":\"restart-backend\",\"bindings\":[{\"capability\":\"session\",\"provider\":\"inmemory\"}]}", http.StatusCreated)
	backendID := nestedStringField(backendValue, "profile", "ProfileID", "profile_id")
	if appID == "" || modelID == "" || backendID == "" {
		t.Fatal("Process A omitted one of app/model/backend IDs")
	}
	status(http.MethodPatch, "/admin/v1/tenants/"+tenantID, "{\"expected_version\":1,\"display_name\":\"Restart Round Trip\",\"default_agent_app_id\":\""+appID+"\",\"default_backend_profile_id\":\""+backendID+"\"}", http.StatusOK)
	draftPath := "/admin/v1/tenants/" + tenantID + "/apps/" + appID + "/revisions"
	draftValue := status(http.MethodPost, draftPath, "{\"expected_app_version\":1,\"kind\":\"llm\",\"schema_version\":1,\"configuration\":{\"instruction\":\"restart\",\"model_profile_id\":\""+modelID+"\"}}", http.StatusCreated)
	revisionNumber, ok := draftValue["Revision"].(float64)
	if !ok || revisionNumber < 1 {
		t.Fatal("Process A revision response omitted revision")
	}
	revisionPath := draftPath + "/" + strconv.Itoa(int(revisionNumber))
	status(http.MethodPatch, revisionPath, "{\"expected_app_version\":1,\"expected_draft_version\":1,\"configuration\":{\"instruction\":\"restart updated\",\"model_profile_id\":\""+modelID+"\"}}", http.StatusOK)
	status(http.MethodPost, revisionPath+"/publish", "{\"expected_app_version\":1,\"expected_draft_version\":2,\"reason\":\"publish\",\"correlation_id\":\"restart-publish\"}", http.StatusOK)
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "restart-route-"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	bindingPath := "/admin/v1/tenants/" + tenantID + "/bindings"
	bindingValue := status(http.MethodPost, bindingPath, "{\"binding_key\":\"restart-binding-"+suffix+"\",\"channel\":\"wecom\",\"provider_account_id\":\"restart-corp-"+suffix+"\",\"public_route_key_digest\":\""+routeDigest+"\",\"app_id\":\""+appID+"\",\"secret_ref\":\"secret/restart-corp\",\"reason\":\"restart-test\",\"correlation_id\":\"restart-binding\",\"protocol\":{\"wecom\":{\"corp_id\":\"restart-corp\"}}}", http.StatusCreated)
	bindingID := nestedStringField(bindingValue, "binding", "BindingID", "binding_id")
	if bindingID == "" {
		t.Fatal("Process A binding response omitted binding ID")
	}
	status(http.MethodPost, bindingPath+"/"+bindingID+"/status", "{\"expected_version\":1,\"next_status\":\"active\",\"reason\":\"activate\",\"correlation_id\":\"restart-activate\"}", http.StatusOK)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv(envAdminTenants, tenantID)
	second, err := NewFromEnvironment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	read := func(path string) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		second.HandlerValue().ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "restart-test-secret") {
			t.Fatalf("Process B read %s status/body = %d %s", path, recorder.Code, recorder.Body.String())
		}
	}
	read("/admin/v1/tenants/" + tenantID)
	read("/admin/v1/tenants/" + tenantID + "/apps/" + appID)
	read("/admin/v1/tenants/" + tenantID + "/apps/" + appID + "/revisions/" + strconv.Itoa(int(revisionNumber)))
	read("/admin/v1/tenants/" + tenantID + "/models/" + modelID)
	read("/admin/v1/tenants/" + tenantID + "/backends/" + backendID)
	read("/admin/v1/tenants/" + tenantID + "/bindings/" + bindingID)
	if !second.Ready() {
		t.Fatal("Process B did not become ready after migration verification")
	}
	apiAuth, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{
		"chat-token": {TenantID: tenantID, AppID: appID, SubjectID: "restart-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	apiRequest := httptest.NewRequest(http.MethodGet, "/v1/chat", nil)
	apiRequest.Header.Set("Authorization", "Bearer chat-token")
	authenticated, err := apiAuth.Authenticate(context.Background(), apiRequest)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := second.Resolver.ResolveAuthenticatedAPI(context.Background(), authenticated)
	if err != nil {
		t.Fatalf("Process B PlanResolver.Resolve = %v", err)
	}
	cacheKey, err := plan.CacheKey()
	if err != nil || cacheKey.TenantID != tenantID || cacheKey.AppID != appID || cacheKey.ModelProfileID != modelID || cacheKey.BackendProfileID != backendID || cacheKey.Revision != int64(revisionNumber) {
		t.Fatalf("Process B execution plan key = %+v, err=%v", cacheKey, err)
	}
	db, err := storagepostgres.Open(context.Background(), dsn, storagepostgres.Options{MaxOpenConns: 2, MaxIdleConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	candidates, err := channelpostgres.NewRepository(db).LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("Process B candidate lookup = %d candidates, err=%v", len(candidates), err)
	}
	if candidates[0].CandidateToken == "" || candidates[0].ConfigDigest == "" {
		t.Fatal("Process B candidate lacked proof-bearing metadata")
	}
}

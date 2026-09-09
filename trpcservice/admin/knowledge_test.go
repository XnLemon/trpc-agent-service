package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	knowledgeadmin "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	vectorinmemory "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
)

type adminKnowledgeEmbedder struct{}

func (adminKnowledgeEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return []float64{1, 0}, nil
}

func (adminKnowledgeEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	value, err := (adminKnowledgeEmbedder{}).GetEmbedding(ctx, text)
	return value, nil, err
}

func (adminKnowledgeEmbedder) GetDimensions() int { return 2 }

var _ embedder.Embedder = adminKnowledgeEmbedder{}

func TestAdminKnowledgeIsAppBoundAndAudited(t *testing.T) {
	handler, _ := testHandler(t)
	root, err := handler.config.Tenants.Create(context.Background(), tenantCreateForKnowledge())
	if err != nil {
		t.Fatal(err)
	}
	first, err := handler.config.Apps.Create(context.Background(), appmodel.CreateInput{TenantID: root.TenantID, AppKey: "first", DisplayName: "First"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler.config.Apps.Create(context.Background(), appmodel.CreateInput{TenantID: root.TenantID, AppKey: "second", DisplayName: "Second"})
	if err != nil {
		t.Fatal(err)
	}
	store := vectorinmemory.New()
	manager, err := knowledgeadmin.NewManager(knowledgeadmin.ProviderFunc(func(context.Context, knowledgeadmin.Scope) (knowledgeadmin.Backend, error) {
		return knowledgeadmin.Backend{Store: store, Embedder: adminKnowledgeEmbedder{}}, nil
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	writer := &adminAuditWriter{}
	handler.config.Knowledge = manager
	handler.config.AuditWriter = writer

	request := func(method, appID, suffix, body string) *httptest.ResponseRecorder {
		path := "/admin/v1/tenants/" + root.TenantID + "/knowledge/apps/" + appID + suffix
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	created := request(http.MethodPost, first.AppID, "/documents", `{"id":"doc-shared","content":"first version"}`)
	if created.Code != http.StatusCreated || strings.Contains(created.Body.String(), "embedding") {
		t.Fatalf("create response = %d %s", created.Code, created.Body.String())
	}
	updated := request(http.MethodPost, first.AppID, "/import", `{"documents":[{"id":"doc-shared","content":"imported version"}]}`)
	if updated.Code != http.StatusCreated || !strings.Contains(updated.Body.String(), "imported version") {
		t.Fatalf("import response = %d %s", updated.Code, updated.Body.String())
	}
	crossApp := request(http.MethodPost, second.AppID, "/documents", `{"id":"doc-shared","content":"cross app"}`)
	if crossApp.Code != http.StatusBadRequest || !strings.Contains(crossApp.Body.String(), "invalid_request") {
		t.Fatalf("cross-app response = %d %s", crossApp.Code, crossApp.Body.String())
	}
	unknown := request(http.MethodGet, "app-does-not-exist", "/documents", "")
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown app response = %d %s", unknown.Code, unknown.Body.String())
	}
	published := request(http.MethodPost, first.AppID, "/publish", `{}`)
	if published.Code != http.StatusOK || !strings.Contains(published.Body.String(), `"content_digest"`) {
		t.Fatalf("publish response = %d %s", published.Code, published.Body.String())
	}
	var envelope struct {
		Data knowledgeadmin.Version `json:"data"`
	}
	if err := json.Unmarshal(published.Body.Bytes(), &envelope); err != nil || envelope.Data.Version != 1 {
		t.Fatalf("publish data = %#v err=%v", envelope.Data, err)
	}
	if len(writer.events) < 3 {
		t.Fatalf("knowledge audit events = %#v", writer.events)
	}
	for _, event := range writer.events {
		if event.TenantID != root.TenantID || event.AgentAppID != "" && event.AgentAppID != first.AppID {
			t.Fatalf("knowledge audit scope = %#v", event)
		}
	}
}

func TestAdminKnowledgeCRUDRebuildAndVersionRoutes(t *testing.T) {
	handler, _ := testHandler(t)
	root, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "knowledge-routes", DisplayName: "Knowledge Routes"})
	if err != nil {
		t.Fatal(err)
	}
	appValue, err := handler.config.Apps.Create(context.Background(), appmodel.CreateInput{TenantID: root.TenantID, AppKey: "routes", DisplayName: "Routes"})
	if err != nil {
		t.Fatal(err)
	}
	store := vectorinmemory.New()
	manager, err := knowledgeadmin.NewManager(knowledgeadmin.ProviderFunc(func(context.Context, knowledgeadmin.Scope) (knowledgeadmin.Backend, error) {
		return knowledgeadmin.Backend{Store: store, Embedder: adminKnowledgeEmbedder{}}, nil
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Knowledge = manager
	request := func(method, suffix, body string) *httptest.ResponseRecorder {
		path := "/admin/v1/tenants/" + root.TenantID + "/knowledge/apps/" + appValue.AppID + suffix
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-token")
		recording := httptest.NewRecorder()
		handler.ServeHTTP(recording, req)
		return recording
	}
	if response := request(http.MethodPost, "/documents", `{"id":"route-doc","name":"Initial","content":"route content"}`); response.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodGet, "/documents?limit=1", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "route-doc") {
		t.Fatalf("list response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodGet, "/documents/route-doc", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Initial") {
		t.Fatalf("get response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodPatch, "/documents/route-doc", `{"name":"Patched"}`); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Patched") {
		t.Fatalf("patch response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodPost, "/rebuild", `{}`); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"count":1`) {
		t.Fatalf("rebuild response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodGet, "/versions", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"items":[]`) {
		t.Fatalf("empty versions response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodPost, "/publish", `{}`); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"version":1`) {
		t.Fatalf("publish response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodGet, "/versions", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"version":1`) {
		t.Fatalf("versions response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodDelete, "/documents/route-doc", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), appValue.AppID) {
		t.Fatalf("delete response = %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodGet, "/documents/route-doc", ""); response.Code != http.StatusNotFound {
		t.Fatalf("deleted get status = %d, body = %s", response.Code, response.Body.String())
	}
	padded := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/"+root.TenantID+"/knowledge/documents?app_id=%20"+appValue.AppID, nil)
	padded.Header.Set("Authorization", "Bearer admin-token")
	paddedResponse := httptest.NewRecorder()
	handler.ServeHTTP(paddedResponse, padded)
	if paddedResponse.Code != http.StatusBadRequest || !strings.Contains(paddedResponse.Body.String(), "invalid_request") {
		t.Fatalf("padded scope response = %d %s", paddedResponse.Code, paddedResponse.Body.String())
	}
}

func TestKnowledgeRouteScopeRejectsConflictingOrPaddedAppIDs(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/t/knowledge/apps/app-a/documents?app_id=app-b", nil)
	if _, _, err := knowledgeRouteScope(request, []string{"apps", "app-a", "documents"}); !errors.Is(err, knowledgeadmin.ErrScope) {
		t.Fatalf("conflicting route scope error = %v", err)
	}
	padded := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/t/knowledge/documents?app_id=%20app-a", nil)
	if _, _, err := knowledgeRouteScope(padded, []string{"documents"}); !errors.Is(err, knowledgeadmin.ErrScope) {
		t.Fatalf("padded query scope error = %v", err)
	}
	if _, _, err := knowledgeRouteScope(request, []string{"apps", " app-a", "documents"}); !errors.Is(err, knowledgeadmin.ErrScope) {
		t.Fatalf("padded route scope error = %v", err)
	}
}

func tenantCreateForKnowledge() tenant.CreateInput {
	return tenant.CreateInput{TenantKey: "knowledge-admin", DisplayName: "Knowledge Admin"}
}

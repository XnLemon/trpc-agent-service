// Package admin exposes the tenant-scoped control-plane HTTP API.
package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	knowledgeadmin "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagemysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

// Config supplies the repositories and authentication policy for an admin handler.
type Config struct {
	Tenants        tenant.Repository
	Apps           appmodel.Repository
	Models         modelprofile.Repository
	Backends       backend.Repository
	Bindings       channels.Repository
	Authenticator  Authenticator
	ModelCatalog   *modelprofile.ProviderCatalog
	BackendCatalog *backend.ProviderCatalog
	// AuditWriter receives control-plane mutation facts returned by repositories.
	AuditWriter audit.Writer
	// CacheInvalidator is notified after a successful control-plane mutation.
	// It is intentionally best-effort during shutdown: a closed runtime cannot
	// admit a new execution with a stale Runner.
	CacheInvalidator CacheInvalidator
	// ToolInvocations exposes only the tenant/app-scoped side-effect ledger to
	// operators. It never exposes raw arguments or provider results.
	ToolInvocations runtimestorage.ToolInvocationStore
	// Knowledge manages the tenant/app-scoped upstream corpus. It is optional
	// when the selected runtime backend does not expose corpus administration.
	Knowledge knowledgeadmin.Service
}

// CacheInvalidator receives the smallest control-plane scope whose future
// runtime dependency may need rebuilding. It is owned by the Admin consumer so
// configuration repositories stay independent from process-local caches.
type CacheInvalidator interface {
	Invalidate(CacheInvalidation)
}

// CacheInvalidation identifies one successfully committed control-plane change.
// Binding changes are included even though the current channel resolver does
// not cache bindings; future provider caches can consume the same signal.
type CacheInvalidation struct {
	TenantID  string
	AppID     string
	ProfileID string
	BindingID string
	Kind      CacheInvalidationKind
}

// CacheInvalidationKind distinguishes the resource that changed.
type CacheInvalidationKind string

const (
	// CacheInvalidationTenant identifies a tenant configuration change.
	CacheInvalidationTenant CacheInvalidationKind = "tenant"
	// CacheInvalidationApp identifies an Agent App configuration change.
	CacheInvalidationApp CacheInvalidationKind = "app"
	// CacheInvalidationModel identifies a model profile change.
	CacheInvalidationModel CacheInvalidationKind = "model"
	// CacheInvalidationBackend identifies a backend profile change.
	CacheInvalidationBackend CacheInvalidationKind = "backend"
	// CacheInvalidationBinding identifies a channel binding change.
	CacheInvalidationBinding CacheInvalidationKind = "binding"
)

// CacheInvalidatorFunc adapts a function to CacheInvalidator.
type CacheInvalidatorFunc func(CacheInvalidation)

// Invalidate implements CacheInvalidator.
func (function CacheInvalidatorFunc) Invalidate(change CacheInvalidation) {
	if function != nil {
		function(change)
	}
}

// Handler serves the tenant-scoped control-plane HTTP API.
type Handler struct {
	config        Config
	firstTenantMu sync.Mutex
}

type tenantCounter interface {
	Count(context.Context) (int, error)
}

type firstTenantCreator interface {
	CreateFirst(context.Context, tenant.CreateInput) (*tenant.Tenant, bool, error)
}

// NewHandler validates dependencies and creates an admin HTTP handler.
func NewHandler(config Config) (*Handler, error) {
	if nilvalue.Is(config.Tenants) || nilvalue.Is(config.Apps) || nilvalue.Is(config.Models) || nilvalue.Is(config.Backends) || nilvalue.Is(config.Bindings) || nilvalue.Is(config.Authenticator) {
		return nil, errors.New("invalid admin handler configuration")
	}
	if nilvalue.Is(config.AuditWriter) {
		config.AuditWriter = nil
	}
	if nilvalue.Is(config.CacheInvalidator) {
		config.CacheInvalidator = nil
	}
	if nilvalue.Is(config.ToolInvocations) {
		config.ToolInvocations = nil
	}
	if nilvalue.Is(config.Knowledge) {
		config.Knowledge = nil
	}
	return &Handler{config: config}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if requestID == "" {
		requestID = uuid.NewString()
	}
	if r.URL.Path != "/admin/v1" && !strings.HasPrefix(r.URL.Path, "/admin/v1/") {
		writeError(w, requestID, http.StatusNotFound, "not_found")
		return
	}
	principal, err := h.config.Authenticator.Authenticate(r.Context(), r)
	if err != nil {
		writeError(w, requestID, http.StatusUnauthorized, "unauthorized")
		return
	}
	parts := splitPath(strings.TrimPrefix(r.URL.Path, "/admin/v1"))
	if len(parts) == 0 {
		writeError(w, requestID, http.StatusNotFound, "not_found")
		return
	}
	if parts[0] == "me" {
		if r.Method != http.MethodGet {
			writeError(w, requestID, http.StatusNotFound, "not_found")
			return
		}
		writeJSON(w, requestID, http.StatusOK, map[string]any{
			"subject_id":        principal.SubjectID,
			"global":            principal.Global,
			"tenant_scopes":     principal.ScopeIDs(),
			"can_create_tenant": principal.Global,
		})
		return
	}
	if parts[0] != "tenants" {
		writeError(w, requestID, http.StatusNotFound, "not_found")
		return
	}
	var status int
	var value any
	switch {
	case len(parts) == 1:
		status, value, err = h.tenants(r.Context(), r, principal)
	case len(parts) >= 2:
		status, value, err = h.tenantRoute(r.Context(), r, principal, parts[1:], requestID)
	default:
		err = errNotFound
	}
	if err != nil {
		writeMappedError(w, requestID, err)
		return
	}
	if r.Method != http.MethodGet {
		if err := h.recordMutation(r.Context(), principal, requestID, value); err != nil {
			writeMappedError(w, requestID, err)
			return
		}
		h.invalidateMutation(parts, r.Method)
	}
	writeJSON(w, requestID, status, value)
}

func (h *Handler) invalidateMutation(parts []string, method string) {
	if h == nil || nilvalue.Is(h.config.CacheInvalidator) || len(parts) < 2 || parts[0] != "tenants" || parts[1] == "" {
		return
	}
	change := CacheInvalidation{TenantID: parts[1]}
	if len(parts) == 2 || (len(parts) == 3 && parts[2] == "status") {
		change.Kind = CacheInvalidationTenant
		h.config.CacheInvalidator.Invalidate(change)
		return
	}
	if len(parts) < 4 {
		return
	}
	change = resourceInvalidation(change, parts[2:], method)
	if change.Kind != "" {
		h.config.CacheInvalidator.Invalidate(change)
	}
}

func resourceInvalidation(change CacheInvalidation, parts []string, method string) CacheInvalidation {
	if len(parts) < 2 || parts[1] == "" {
		return change
	}
	switch parts[0] {
	case "apps":
		if appMutation(parts, method) {
			change.AppID, change.Kind = parts[1], CacheInvalidationApp
		}
	case "models":
		if profileMutation(parts, method) {
			change.ProfileID, change.Kind = parts[1], CacheInvalidationModel
		}
	case "backends":
		if profileMutation(parts, method) {
			change.ProfileID, change.Kind = parts[1], CacheInvalidationBackend
		}
	case "bindings":
		if profileMutation(parts, method) {
			change.BindingID, change.Kind = parts[1], CacheInvalidationBinding
		}
	}
	return change
}

func appMutation(parts []string, method string) bool {
	return (len(parts) == 2 && method == http.MethodPatch) ||
		(len(parts) == 3 && (parts[2] == "status" || parts[2] == "rollback" || parts[2] == "canary")) ||
		(len(parts) == 5 && parts[2] == "revisions" && parts[4] == "publish")
}

func profileMutation(parts []string, method string) bool {
	return (len(parts) == 2 && method == http.MethodPatch) || (len(parts) == 3 && parts[2] == "status")
}

type auditMutationValue struct {
	value  any
	reason string
}

func (value auditMutationValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.value)
}

func (h *Handler) recordMutation(ctx context.Context, principal Principal, requestID string, value any) error {
	if h == nil || nilvalue.Is(h.config.AuditWriter) || nilvalue.Is(value) {
		return nil
	}
	auditReason := ""
	if wrapped, ok := value.(auditMutationValue); ok {
		value, auditReason = wrapped.value, wrapped.reason
		if value == nil {
			return nil
		}
	}
	// Tool invocation transitions emit their own stable lifecycle audit fact;
	// do not add a generic control-plane mutation event for the same transition.
	switch value.(type) {
	case runtimestorage.ToolInvocation, *runtimestorage.ToolInvocation:
		return nil
	}
	var change any
	if envelope, ok := value.(map[string]any); ok {
		change = envelope["event"]
	}
	if change == nil {
		return h.recordRawMutationWithReason(ctx, principal, requestID, value, auditReason)
	}
	v := reflect.ValueOf(change)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	fieldString := func(name string) string {
		field := v.FieldByName(name)
		if !field.IsValid() || field.Kind() != reflect.String {
			return ""
		}
		return field.String()
	}
	fieldInt := func(name string) (int64, bool) {
		field := v.FieldByName(name)
		if !field.IsValid() {
			return 0, false
		}
		if field.Kind() == reflect.Pointer {
			if field.IsNil() {
				return 0, false
			}
			field = field.Elem()
		}
		if field.Kind() != reflect.Int64 && field.Kind() != reflect.Int {
			return 0, false
		}
		return field.Int(), true
	}
	previous, previousOK := fieldInt("PreviousVersion")
	next, nextOK := fieldInt("NextVersion")
	if !previousOK || !nextOK {
		return nil
	}
	tenants := fieldString("TenantID")
	_ = principal
	return audit.NewRecorder(h.config.AuditWriter, tenants).Record(ctx, audit.Event{
		EventID:   audit.NewEventID(requestID, tenants, fieldString("CorrelationID"), fieldString("EventType")),
		EventType: audit.EventControlPlaneChanged, TenantID: tenants,
		ActorType: fieldString("ActorType"), ActorID: fieldString("ActorID"),
		Reason: fieldString("Reason"), CorrelationID: fieldString("CorrelationID"),
		PreviousVersion: &previous, NextVersion: &next,
	})
}

func (h *Handler) recordRawMutation(ctx context.Context, principal Principal, requestID string, value any) error {
	return h.recordRawMutationWithReason(ctx, principal, requestID, value, "")
}

func (h *Handler) recordRawMutationWithReason(ctx context.Context, principal Principal, requestID string, value any, reason string) error {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	fieldString := func(name string) string {
		field := v.FieldByName(name)
		if field.IsValid() && field.Kind() == reflect.String {
			return field.String()
		}
		return ""
	}
	next := int64(1)
	versionIdentity := fieldString("Version")
	for _, name := range []string{"Version", "DraftVersion", "Revision"} {
		fieldVersion := v.FieldByName(name)
		if fieldVersion.IsValid() && (fieldVersion.Kind() == reflect.Int64 || fieldVersion.Kind() == reflect.Int) && fieldVersion.Int() > 0 {
			next = fieldVersion.Int()
			if versionIdentity == "" {
				versionIdentity = strconv.FormatInt(next, 10)
			}
			break
		}
	}
	previous := next - 1
	tenantID := fieldString("TenantID")
	appID := fieldString("AppID")
	if tenantID == "" {
		return nil
	}
	actorID := principal.SubjectID
	if actorID == "" {
		actorID = "admin"
	}
	if strings.TrimSpace(reason) == "" {
		reason = "admin mutation"
	}
	return audit.NewRecorder(h.config.AuditWriter, tenantID).Record(ctx, audit.Event{
		EventID:   audit.NewEventID(requestID, tenantID, appID, versionIdentity, "raw"),
		EventType: audit.EventControlPlaneChanged, TenantID: tenantID, AgentAppID: appID,
		ActorType: "admin", ActorID: actorID, Reason: reason, CorrelationID: requestID,
		PreviousVersion: &previous, NextVersion: &next,
	})
}

var errNotFound = errors.New("admin route not found")
var errInvalidRequest = errors.New("invalid admin request")

func (h *Handler) tenants(ctx context.Context, r *http.Request, p Principal) (int, any, error) {
	if r.Method == http.MethodGet {
		lister, ok := h.config.Tenants.(TenantLister)
		if !ok {
			return 0, nil, errListUnsupported
		}
		o, err := repositoryListOptions(r)
		if err != nil {
			return 0, nil, err
		}
		items, next, err := lister.List(ctx, p.ScopeIDs(), o.Query, o.Status, o.Cursor, o.Limit)
		if err != nil {
			return 0, nil, err
		}
		value, err := newListEnvelope(items, next)
		return http.StatusOK, value, err
	}
	if r.Method != http.MethodPost || !p.Allows("", true) {
		return 0, nil, ErrForbidden
	}
	var input tenant.CreateInput
	if err := decodeBody(r, &input); err != nil {
		return 0, nil, err
	}
	if p.Global {
		if creator, ok := h.config.Tenants.(firstTenantCreator); ok {
			created, allowed, err := creator.CreateFirst(ctx, input)
			if err != nil {
				return 0, nil, err
			}
			if !allowed {
				return 0, nil, ErrForbidden
			}
			return http.StatusCreated, created, nil
		}
		h.firstTenantMu.Lock()
		defer h.firstTenantMu.Unlock()
		counter, ok := h.config.Tenants.(tenantCounter)
		if !ok {
			return 0, nil, ErrForbidden
		}
		count, err := counter.Count(ctx)
		if err != nil {
			return 0, nil, err
		}
		if count > 0 {
			return 0, nil, ErrForbidden
		}
	}
	created, err := h.config.Tenants.Create(ctx, input)
	return http.StatusCreated, created, err
}

func (h *Handler) tenantRoute(ctx context.Context, r *http.Request, p Principal, parts []string, requestID string) (int, any, error) {
	tenantID := parts[0]
	if tenantID == "" || !p.Allows(tenantID, false) {
		if r.Method == http.MethodGet {
			return 0, nil, errNotFound
		}
		return 0, nil, ErrForbidden
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			value, err := h.config.Tenants.Get(ctx, tenantID)
			return http.StatusOK, value, err
		case http.MethodPatch:
			var body tenant.UpdateConfigurationInput
			if err := decodeBody(r, &body); err != nil {
				return 0, nil, err
			}
			body.TenantID = tenantID
			value, err := h.config.Tenants.UpdateConfiguration(ctx, body)
			return http.StatusOK, value, err
		default:
			return 0, nil, errNotFound
		}
	}
	switch parts[1] {
	case "status":
		if len(parts) != 2 || r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body struct {
			ExpectedVersion int64
			NextStatus      tenant.Status
			Reason          string
			CorrelationID   string
		}
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		value, event, err := h.config.Tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: tenantID, ExpectedVersion: body.ExpectedVersion, NextStatus: body.NextStatus, Metadata: tenant.TransitionMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: body.Reason, CorrelationID: body.CorrelationID}})
		return http.StatusOK, map[string]any{"tenant": value, "event": event}, err
	case "apps":
		return h.apps(ctx, r, p, tenantID, parts[2:])
	case "models":
		return h.models(ctx, r, p, tenantID, parts[2:])
	case "backends":
		return h.backends(ctx, r, p, tenantID, parts[2:])
	case "bindings":
		return h.bindings(ctx, r, p, tenantID, parts[2:])
	case "tool-invocations":
		return h.toolInvocations(ctx, r, p, tenantID, parts[2:])
	case "knowledge":
		return h.knowledge(ctx, r, p, tenantID, parts[2:])
	default:
		return 0, nil, errNotFound
	}
}

func (h *Handler) knowledge(ctx context.Context, r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if nilvalue.Is(h.config.Knowledge) {
		return 0, nil, errListUnsupported
	}
	appID, resourceParts, err := knowledgeRouteScope(r, parts)
	if err != nil {
		return 0, nil, err
	}
	// Knowledge is partitioned by an existing control-plane App. Do not let a
	// caller probe or create an arbitrary app namespace through the corpus API.
	appValue, appErr := h.config.Apps.Get(ctx, tenantID, appID)
	if appErr != nil {
		// Knowledge routes must not distinguish a syntactically invalid or
		// unknown app from an app outside the caller's visible corpus. A GET
		// therefore presents every failed app lookup as the same route-level
		// 404 instead of leaking repository validation details.
		if errors.Is(appErr, appmodel.ErrNotFound) || errors.Is(appErr, appmodel.ErrInvalid) {
			return 0, nil, errNotFound
		}
		return 0, nil, appErr
	}
	if appValue == nil || appValue.TenantID != tenantID || appValue.AppID != appID {
		return 0, nil, errNotFound
	}
	scope := knowledgeadmin.Scope{TenantID: tenantID, AppID: appID}
	if len(resourceParts) == 0 {
		return 0, nil, errNotFound
	}
	switch resourceParts[0] {
	case "documents":
		return h.knowledgeDocuments(ctx, r, scope, resourceParts[1:])
	case "import", "imports":
		if len(resourceParts) != 1 || r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body knowledgeadmin.ImportInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		value, err := h.config.Knowledge.ImportDocuments(ctx, scope, body)
		return http.StatusCreated, value, err
	case "rebuild":
		if len(resourceParts) != 1 || r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		value, err := h.config.Knowledge.Rebuild(ctx, scope)
		return http.StatusOK, value, err
	case "versions":
		if len(resourceParts) == 1 && r.Method == http.MethodGet {
			value, err := h.config.Knowledge.ListVersions(ctx, scope)
			return http.StatusOK, map[string]any{"items": value}, err
		}
		return 0, nil, errNotFound
	case "publish":
		if len(resourceParts) != 1 || r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body struct {
			Reason string `json:"reason,omitempty"`
		}
		if r.Body != nil {
			if err := decodeBody(r, &body); err != nil {
				return 0, nil, err
			}
		}
		if !validAdminReason(body.Reason) {
			return 0, nil, fmt.Errorf("%w: publish reason is invalid", errInvalidRequest)
		}
		value, err := h.config.Knowledge.Publish(ctx, scope, p.SubjectID)
		// Return the manifest itself so the common mutation audit path can record
		// its tenant/app/version identity without serializing request content.
		// The private wrapper preserves the operator reason for audit without
		// adding request metadata to the public version response.
		return http.StatusOK, auditMutationValue{value: value, reason: body.Reason}, err
	default:
		return 0, nil, errNotFound
	}
}

func knowledgeRouteScope(r *http.Request, parts []string) (string, []string, error) {
	if r == nil {
		return "", nil, fmt.Errorf("%w: request is required", errInvalidRequest)
	}
	rawQueryAppID := r.URL.Query().Get("app_id")
	if rawQueryAppID != strings.TrimSpace(rawQueryAppID) {
		return "", nil, fmt.Errorf("%w: knowledge app_id is not normalized", knowledgeadmin.ErrScope)
	}
	queryAppID := rawQueryAppID
	appID := queryAppID
	if len(parts) > 0 && parts[0] == "apps" {
		if len(parts) < 2 || parts[1] == "" {
			return "", nil, fmt.Errorf("%w: knowledge app_id is required", errInvalidRequest)
		}
		if parts[1] != strings.TrimSpace(parts[1]) {
			return "", nil, fmt.Errorf("%w: knowledge app_id is not normalized", knowledgeadmin.ErrScope)
		}
		appID = parts[1]
		parts = parts[2:]
	} else if len(parts) >= 1 && parts[0] != "documents" && parts[0] != "import" && parts[0] != "imports" && parts[0] != "rebuild" && parts[0] != "versions" && parts[0] != "publish" {
		if parts[0] != strings.TrimSpace(parts[0]) {
			return "", nil, fmt.Errorf("%w: knowledge app_id is not normalized", knowledgeadmin.ErrScope)
		}
		appID = parts[0]
		parts = parts[1:]
	}
	if appID == "" {
		return "", nil, fmt.Errorf("%w: knowledge app_id is required", errInvalidRequest)
	}
	if queryAppID != "" && queryAppID != appID {
		return "", nil, fmt.Errorf("%w: knowledge app scope disagrees with app_id", knowledgeadmin.ErrScope)
	}
	return appID, parts, nil
}

type knowledgeDeleteResult struct {
	TenantID   string `json:"tenant_id"`
	AppID      string `json:"app_id"`
	DocumentID string `json:"document_id"`
}

func (h *Handler) knowledgeDocuments(ctx context.Context, r *http.Request, scope knowledgeadmin.Scope, parts []string) (int, any, error) {
	if len(parts) == 0 {
		switch r.Method {
		case http.MethodGet:
			o, err := repositoryListOptions(r)
			if err != nil {
				return 0, nil, err
			}
			if o.Query != "" {
				return 0, nil, fmt.Errorf("%w: knowledge document query is unsupported", errInvalidRequest)
			}
			value, err := h.config.Knowledge.ListDocuments(ctx, scope, o.Cursor, o.Limit)
			if err != nil {
				return 0, nil, err
			}
			envelope, envelopeErr := newListEnvelope(value.Items, value.NextCursor)
			return http.StatusOK, envelope, envelopeErr
		case http.MethodPost:
			var body knowledgeadmin.DocumentInput
			if err := decodeBody(r, &body); err != nil {
				return 0, nil, err
			}
			value, err := h.config.Knowledge.CreateDocument(ctx, scope, body)
			return http.StatusCreated, value, err
		default:
			return 0, nil, errNotFound
		}
	}
	if len(parts) != 1 {
		return 0, nil, errNotFound
	}
	id := parts[0]
	switch r.Method {
	case http.MethodGet:
		value, err := h.config.Knowledge.GetDocument(ctx, scope, id)
		return http.StatusOK, value, err
	case http.MethodPatch:
		var body knowledgeadmin.UpdateInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		value, err := h.config.Knowledge.UpdateDocument(ctx, scope, id, body)
		return http.StatusOK, value, err
	case http.MethodDelete:
		if err := h.config.Knowledge.DeleteDocument(ctx, scope, id); err != nil {
			return 0, nil, err
		}
		return http.StatusOK, knowledgeDeleteResult{TenantID: scope.TenantID, AppID: scope.AppID, DocumentID: id}, nil
	default:
		return 0, nil, errNotFound
	}
}

func (h *Handler) toolInvocations(ctx context.Context, r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if nilvalue.Is(h.config.ToolInvocations) {
		return 0, nil, errListUnsupported
	}
	rawAppID := r.URL.Query().Get("app_id")
	if rawAppID != strings.TrimSpace(rawAppID) {
		return 0, nil, fmt.Errorf("%w: tool invocation app_id is not normalized", errInvalidRequest)
	}
	appID := rawAppID
	if appID == "" {
		return 0, nil, fmt.Errorf("%w: tool invocation app_id is required", errInvalidRequest)
	}
	appValue, err := h.config.Apps.Get(ctx, tenantID, appID)
	if err != nil {
		return 0, nil, err
	}
	if appValue == nil || appValue.TenantID != tenantID || appValue.AppID != appID {
		return 0, nil, errNotFound
	}
	if len(parts) == 0 {
		if r.Method != http.MethodGet {
			return 0, nil, errNotFound
		}
		statusValues := strings.Split(strings.TrimSpace(r.URL.Query().Get("status")), ",")
		statuses := make([]runtimestorage.ToolInvocationStatus, 0, len(statusValues))
		if len(statusValues) == 1 && statusValues[0] == "" {
			statuses = nil
		} else {
			for _, status := range statusValues {
				status = strings.TrimSpace(status)
				value := runtimestorage.ToolInvocationStatus(status)
				if !runtimestorage.ValidToolInvocationStatus(value) {
					return 0, nil, fmt.Errorf("%w: invalid tool invocation status", errInvalidRequest)
				}
				statuses = append(statuses, value)
			}
		}
		items, err := h.config.ToolInvocations.ListToolInvocations(ctx, tenantID, appID, statuses)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, listEnvelope{Items: items}, nil
	}
	invocationID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		value, err := h.config.ToolInvocations.GetToolInvocation(ctx, tenantID, appID, invocationID)
		return http.StatusOK, value, err
	}
	if len(parts) != 2 || parts[1] != "transition" || r.Method != http.MethodPost {
		return 0, nil, errNotFound
	}
	var body struct {
		From         runtimestorage.ToolInvocationStatus `json:"from"`
		To           runtimestorage.ToolInvocationStatus `json:"to"`
		FencingToken int64                               `json:"fencing_token"`
		ErrorClass   string                              `json:"error_class"`
		Reason       string                              `json:"reason"`
	}
	if err := decodeBody(r, &body); err != nil {
		return 0, nil, err
	}
	reviewerID := p.SubjectID
	if body.To == runtimestorage.ToolInvocationManual {
		body.ErrorClass = "manual_review"
	} else if body.To == runtimestorage.ToolInvocationPrepared && body.From == runtimestorage.ToolInvocationManual {
		// A manual approval is represented by a durable marker on the next
		// prepared attempt; the reviewer consumes it without trusting request
		// payload fields supplied by the caller.
		reviewerID = "approved:" + p.SubjectID
		body.ErrorClass = ""
	}
	value, err := h.config.ToolInvocations.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: tenantID, AppID: appID, InvocationID: invocationID, From: body.From, To: body.To,
		Owner: p.SubjectID, FencingToken: body.FencingToken, ErrorClass: body.ErrorClass, ReviewerID: reviewerID,
	})
	if err != nil {
		return 0, nil, err
	}
	if h.config.AuditWriter != nil {
		eventType, decision := audit.EventToolApprovalRequired, audit.DecisionApprovalRequired
		switch value.Status {
		case runtimestorage.ToolInvocationAccepted, runtimestorage.ToolInvocationSucceeded:
			eventType, decision = audit.EventToolExecuted, audit.DecisionAccepted
		case runtimestorage.ToolInvocationDenied, runtimestorage.ToolInvocationFailed, runtimestorage.ToolInvocationUnknown:
			eventType, decision = audit.EventToolDenied, audit.DecisionDeny
		}
		errorType := ""
		if value.ErrorClass != "" {
			errorType = string(audit.ErrorTool)
		}
		if auditErr := audit.NewRecorder(h.config.AuditWriter, tenantID).Record(ctx, audit.Event{
			EventType: eventType, EventID: audit.NewEventID(string(eventType), tenantID, value.AppID, invocationID, string(value.Status)),
			TenantID: tenantID, AgentAppID: value.AppID, ToolName: value.ToolName, RequestID: value.RequestID, TraceID: value.TraceID,
			Decision: decision, ErrorType: errorType, ActorType: "admin", ActorID: p.SubjectID, Reason: body.Reason,
		}); auditErr != nil {
			return 0, nil, auditErr
		}
	}
	return http.StatusOK, value, nil
}

//nolint:gocyclo // App routes coordinate several independently authorized mutations.
func (h *Handler) apps(ctx context.Context, r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if len(parts) == 0 {
		if r.Method == http.MethodGet {
			lister, ok := h.config.Apps.(AppLister)
			if !ok {
				return 0, nil, errListUnsupported
			}
			o, err := repositoryListOptions(r)
			if err != nil {
				return 0, nil, err
			}
			items, next, err := lister.List(ctx, tenantID, o.Query, o.Status, o.Cursor, o.Limit)
			if err != nil {
				return 0, nil, err
			}
			value, err := newListEnvelope(items, next)
			return http.StatusOK, value, err
		}
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var input appmodel.CreateInput
		if err := decodeBody(r, &input); err != nil {
			return 0, nil, err
		}
		input.TenantID = tenantID
		value, err := h.config.Apps.Create(ctx, input)
		return http.StatusCreated, value, err
	}
	appID := parts[0]
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			value, err := h.config.Apps.Get(ctx, tenantID, appID)
			return http.StatusOK, value, err
		case http.MethodPatch:
			var body appmodel.UpdateMetadataInput
			if err := decodeBody(r, &body); err != nil {
				return 0, nil, err
			}
			body.TenantID, body.AppID = tenantID, appID
			value, err := h.config.Apps.UpdateMetadata(ctx, body)
			return http.StatusOK, value, err
		default:
			return 0, nil, errNotFound
		}
	}
	switch parts[1] {
	case "status":
		if len(parts) != 2 || r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body struct {
			ExpectedVersion int64
			NextStatus      appmodel.Status
			Reason          string
			CorrelationID   string
		}
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		value, event, err := h.config.Apps.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: tenantID, AppID: appID, ExpectedVersion: body.ExpectedVersion, NextStatus: body.NextStatus, Metadata: appmodel.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: body.Reason, CorrelationID: body.CorrelationID}})
		return http.StatusOK, map[string]any{"app": value, "event": event}, err
	case "revisions":
		return h.revisions(ctx, r, p, tenantID, appID, parts[2:])
	case "rollback":
		if len(parts) != 2 || r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body appmodel.RollbackInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.AppID = tenantID, appID
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Apps.Rollback(ctx, body)
		return http.StatusOK, map[string]any{"app": value, "event": event}, err
	case "canary":
		if len(parts) != 2 || r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body struct {
			ExpectedAppVersion int64
			CandidateRevision  *int64
			Reason             string
			CorrelationID      string
		}
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		tenantRoot, err := h.config.Tenants.Get(ctx, tenantID)
		if err != nil {
			return 0, nil, err
		}
		bodyInput := appmodel.SetCanaryInput{
			TenantID: tenantID, AppID: appID, CandidateRevision: body.CandidateRevision,
			ExpectedAppVersion: body.ExpectedAppVersion, TenantActive: tenantRoot.Status == tenant.StatusActive,
			Metadata: appmodel.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: body.Reason, CorrelationID: body.CorrelationID},
		}
		value, event, err := h.config.Apps.SetCanary(ctx, bodyInput)
		return http.StatusOK, map[string]any{"app": value, "event": event}, err
	default:
		return 0, nil, errNotFound
	}
}

func (h *Handler) revisions(ctx context.Context, r *http.Request, p Principal, tenantID, appID string, parts []string) (int, any, error) {
	if len(parts) == 0 {
		if r.Method == http.MethodGet {
			lister, ok := h.config.Apps.(RevisionLister)
			if !ok {
				return 0, nil, errListUnsupported
			}
			o, err := repositoryListOptions(r)
			if err != nil {
				return 0, nil, err
			}
			items, next, err := lister.ListRevisions(ctx, tenantID, appID, o.Query, o.Status, o.Cursor, o.Limit)
			if err != nil {
				return 0, nil, err
			}
			value, err := newListEnvelope(items, next)
			return http.StatusOK, value, err
		}
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body appmodel.CreateDraftInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.AppID = tenantID, appID
		value, err := h.config.Apps.CreateDraft(ctx, body)
		return http.StatusCreated, value, err
	}
	revision, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: revision must be numeric", errInvalidRequest)
	}
	if len(parts) == 1 {
		if r.Method != http.MethodPatch {
			return 0, nil, errNotFound
		}
		var body appmodel.UpdateDraftInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.AppID, body.Revision = tenantID, appID, revision
		value, err := h.config.Apps.UpdateDraft(ctx, body)
		return http.StatusOK, value, err
	}
	if len(parts) != 2 || parts[1] != "publish" || r.Method != http.MethodPost {
		return 0, nil, errNotFound
	}
	var body appmodel.PublishInput
	if err := decodeBody(r, &body); err != nil {
		return 0, nil, err
	}
	body.TenantID, body.AppID, body.Revision, body.TenantActive = tenantID, appID, revision, true
	body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
	tenantRoot, tenantErr := h.config.Tenants.Get(ctx, tenantID)
	if tenantErr != nil {
		return 0, nil, tenantErr
	}
	body.TenantActive = tenantRoot.Status == tenant.StatusActive
	value, published, event, err := h.config.Apps.Publish(ctx, body)
	return http.StatusOK, map[string]any{"app": value, "revision": published, "event": event}, err
}

func (h *Handler) models(ctx context.Context, r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if len(parts) == 0 {
		if r.Method == http.MethodGet {
			lister, ok := h.config.Models.(ModelLister)
			if !ok {
				return 0, nil, errListUnsupported
			}
			o, err := repositoryListOptions(r)
			if err != nil {
				return 0, nil, err
			}
			items, next, err := lister.List(ctx, tenantID, o.Query, o.Status, o.Cursor, o.Limit)
			if err != nil {
				return 0, nil, err
			}
			value, err := newListEnvelope(items, next)
			return http.StatusOK, value, err
		}
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body modelprofile.CreateInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID = tenantID
		body.Metadata = modelprofile.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: body.Metadata.Reason, CorrelationID: body.Metadata.CorrelationID}
		value, event, err := h.config.Models.Create(ctx, body)
		return http.StatusCreated, map[string]any{"profile": value, "event": event}, err
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		value, err := h.config.Models.Get(ctx, tenantID, id)
		return http.StatusOK, value, err
	}
	if len(parts) == 1 && r.Method == http.MethodPatch {
		var body modelprofile.UpdateConfigurationInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.ProfileID = tenantID, id
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Models.UpdateConfiguration(ctx, body)
		return http.StatusOK, map[string]any{"profile": value, "event": event}, err
	}
	if len(parts) == 2 && parts[1] == "status" && r.Method == http.MethodPost {
		var body modelprofile.TransitionStatusInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.ProfileID = tenantID, id
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Models.TransitionStatus(ctx, body)
		return http.StatusOK, map[string]any{"profile": value, "event": event}, err
	}
	return 0, nil, errNotFound
}

func (h *Handler) backends(ctx context.Context, r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if len(parts) == 0 {
		if r.Method == http.MethodGet {
			lister, ok := h.config.Backends.(BackendLister)
			if !ok {
				return 0, nil, errListUnsupported
			}
			o, err := repositoryListOptions(r)
			if err != nil {
				return 0, nil, err
			}
			items, next, err := lister.List(ctx, tenantID, o.Query, o.Status, o.Cursor, o.Limit)
			if err != nil {
				return 0, nil, err
			}
			value, err := newListEnvelope(items, next)
			return http.StatusOK, value, err
		}
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body backend.CreateInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID = tenantID
		body.Metadata = backend.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: body.Metadata.Reason, CorrelationID: body.Metadata.CorrelationID}
		value, event, err := h.config.Backends.Create(ctx, body)
		return http.StatusCreated, map[string]any{"profile": value, "event": event}, err
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		value, err := h.config.Backends.Get(ctx, tenantID, id)
		return http.StatusOK, value, err
	}
	if len(parts) == 1 && r.Method == http.MethodPatch {
		var body backend.UpdateConfigurationInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.ProfileID = tenantID, id
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Backends.UpdateConfiguration(ctx, body)
		return http.StatusOK, map[string]any{"profile": value, "event": event}, err
	}
	if len(parts) == 2 && parts[1] == "status" && r.Method == http.MethodPost {
		var body backend.TransitionStatusInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.ProfileID = tenantID, id
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Backends.TransitionStatus(ctx, body)
		return http.StatusOK, map[string]any{"profile": value, "event": event}, err
	}
	return 0, nil, errNotFound
}

func (h *Handler) bindings(ctx context.Context, r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if len(parts) == 0 {
		if r.Method == http.MethodGet {
			lister, ok := h.config.Bindings.(BindingLister)
			if !ok {
				return 0, nil, errListUnsupported
			}
			o, err := repositoryListOptions(r)
			if err != nil {
				return 0, nil, err
			}
			items, next, err := lister.List(ctx, tenantID, o.Query, o.Status, o.Cursor, o.Limit)
			if err != nil {
				return 0, nil, err
			}
			value, err := newListEnvelope(items, next)
			return http.StatusOK, value, err
		}
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body channels.CreateInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID = tenantID
		body.Metadata = channels.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: body.Metadata.Reason, CorrelationID: body.Metadata.CorrelationID}
		value, event, err := h.config.Bindings.Create(ctx, body)
		return http.StatusCreated, map[string]any{"binding": value, "event": event}, err
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		value, err := h.config.Bindings.Get(ctx, tenantID, id)
		return http.StatusOK, value, err
	}
	if len(parts) == 1 && r.Method == http.MethodPatch {
		var body channels.UpdateConfigurationInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.BindingID = tenantID, id
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Bindings.UpdateConfiguration(ctx, body)
		return http.StatusOK, map[string]any{"binding": value, "event": event}, err
	}
	if len(parts) == 2 && parts[1] == "status" && r.Method == http.MethodPost {
		var body channels.TransitionStatusInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.BindingID = tenantID, id
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Bindings.TransitionStatus(ctx, body)
		return http.StatusOK, map[string]any{"binding": value, "event": event}, err
	}
	return 0, nil, errNotFound
}

func splitPath(path string) []string {
	raw := strings.Split(strings.Trim(path, "/"), "/")
	out := raw[:0]
	for _, part := range raw {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func validAdminReason(value string) bool {
	if value == "" {
		return true
	}
	if !utf8.ValidString(value) || value != strings.TrimSpace(value) || len([]rune(value)) > 1000 {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func decodeBody(r *http.Request, dst any) error {
	if r == nil || r.Body == nil {
		return fmt.Errorf("%w: request body is required", errInvalidRequest)
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 2<<20+1))
	if err != nil || len(data) > 2<<20 {
		return fmt.Errorf("%w: read request body", errInvalidRequest)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return fmt.Errorf("%w: request body is required", errInvalidRequest)
	}
	if err := jsonstrict.Validate(data, true); err != nil {
		return fmt.Errorf("%w: request JSON is invalid", errInvalidRequest)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("%w: request JSON is invalid", errInvalidRequest)
	}
	value = normalizeKeys(value)
	data, err = json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: normalize request JSON", errInvalidRequest)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("%w: request fields are invalid", errInvalidRequest)
	}
	return nil
}

func normalizeKeys(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed)+1)
		for key, child := range typed {
			normalizedChild := normalizeKeys(child)
			exported := toExported(key)
			out[exported] = normalizedChild
			if key == "secret_ref" {
				// Model configuration uses a snake_case JSON tag while the
				// other control-plane inputs expose the exported Go field.
				// Keep both spellings at this boundary so every repository
				// receives the same secret-free reference value.
				out[key] = normalizedChild
			}
		}
		if _, ok := out["Metadata"]; !ok {
			metadata := map[string]any{}
			if reason, ok := out["Reason"]; ok {
				metadata["Reason"] = reason
			}
			if correlation, ok := out["CorrelationID"]; ok {
				metadata["CorrelationID"] = correlation
			}
			if len(metadata) > 0 {
				out["Metadata"] = metadata
			}
		}
		return out
	case []any:
		for i := range typed {
			typed[i] = normalizeKeys(typed[i])
		}
		return typed
	default:
		return value
	}
}

func toExported(key string) string {
	// Only normalize fields that belong to the public Admin wire contract.
	// Arbitrary map keys (for example provider Options) are data and must keep
	// their exact spelling.
	known := map[string]string{
		"tenant_id": "TenantID", "tenant_key": "TenantKey", "app_id": "AppID", "app_key": "AppKey", "model_profile_id": "ModelProfileID",
		"profile_id": "ProfileID", "profile_key": "ProfileKey", "binding_id": "BindingID", "display_name": "DisplayName",
		"description": "Description", "expected_version": "ExpectedVersion",
		"expected_app_version": "ExpectedAppVersion", "expected_draft_version": "ExpectedDraftVersion",
		"next_status": "NextStatus", "target_revision": "TargetRevision", "candidate_revision": "CandidateRevision", "reason": "Reason",
		"correlation_id": "CorrelationID", "schema_version": "SchemaVersion", "secret_ref": "SecretRef",
		"provider_account_id": "ProviderAccountID", "public_route_key_digest": "PublicRouteKeyDigest",
		"audit_retention_days": "AuditRetentionDays", "log_masking_level": "LogMaskingLevel",
		"trace_sampling_rate": "TraceSamplingRate", "rate_limit_rpm": "RateLimitRPM",
		"max_concurrent_executions": "MaxConcurrentExecutions", "monthly_token_budget": "MonthlyTokenBudget",
		"monthly_spend_limit_minor": "MonthlySpendLimitMinor", "billing_currency": "BillingCurrency",
		"default_agent_app_id": "DefaultAgentAppID", "default_backend_profile_id": "DefaultBackendProfileID",
		"configuration": "Configuration", "provider": "Provider", "model": "Model",
		"embedding_text": "EmbeddingText", "metadata": "Metadata", "documents": "Documents", "items": "Items", "document_id": "DocumentID", "document_count": "DocumentCount", "content_digest": "ContentDigest", "actor_id": "ActorID", "version": "Version",
		"endpoint": "Endpoint", "server_url": "ServerURL", "transport": "Transport", "command": "Command", "args": "Args", "name": "Name", "tool_allow": "ToolAllow", "tool_policies": "ToolPolicies", "timeout_seconds": "TimeoutSeconds", "mcp_bindings": "MCPBindings", "skills": "Skills", "skill_authorizations": "SkillAuthorizations", "options": "Options", "generation": "Generation",
		"temperature": "Temperature", "top_p": "TopP", "max_output_tokens": "MaxOutputTokens",
		"fencing_token": "FencingToken", "error_class": "ErrorClass", "tool_call_id": "ToolCallID", "invocation_id": "InvocationID",
		"binding_key": "BindingKey", "channel": "Channel", "protocol": "Protocol",
	}
	if exported, ok := known[key]; ok {
		return exported
	}
	return key
}

func writeJSON(w http.ResponseWriter, requestID string, status int, value any) {
	payload := map[string]any{"request_id": requestID, "data": value}
	data, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
func writeError(w http.ResponseWriter, requestID string, status int, category string) {
	payload := map[string]any{"request_id": requestID, "error": category}
	data, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeMappedError(w http.ResponseWriter, requestID string, err error) {
	status, category := mapError(err)
	logRequestFailure(requestID, status, category, err)
	writeError(w, requestID, status, category)
}

func mapError(err error) (int, string) {
	if err == nil {
		return http.StatusInternalServerError, "internal_error"
	}
	switch {
	case errors.Is(err, errInvalidRequest):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, ErrUnauthenticated):
		return http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, ErrForbidden):
		return http.StatusForbidden, "forbidden"
	case errors.Is(err, audit.ErrWriteFailed):
		return http.StatusServiceUnavailable, "audit_unavailable"
	case matchesAny(err, errNotFound, tenant.ErrNotFound, appmodel.ErrNotFound, modelprofile.ErrNotFound, backend.ErrNotFound, channels.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case matchesAny(err, tenant.ErrConflict, appmodel.ErrConflict, modelprofile.ErrConflict, backend.ErrConflict, channels.ErrConflict, tenant.ErrDuplicateKey, appmodel.ErrDuplicateKey, modelprofile.ErrDuplicateKey, backend.ErrDuplicateKey, channels.ErrDuplicateKey):
		return http.StatusConflict, "conflict"
	case errors.Is(err, postgres.ErrStorage), errors.Is(err, storagemysql.ErrStorage), errors.Is(err, runtimestorage.ErrStorage), errors.Is(err, knowledgeadmin.ErrUnavailable):
		return http.StatusServiceUnavailable, "storage_unavailable"
	case errors.Is(err, knowledgeadmin.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, knowledgeadmin.ErrConflict):
		return http.StatusConflict, "conflict"
	case errors.Is(err, knowledgeadmin.ErrInvalid), errors.Is(err, knowledgeadmin.ErrScope):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, runtimestorage.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, runtimestorage.ErrConflict), errors.Is(err, runtimestorage.ErrDuplicate):
		return http.StatusConflict, "conflict"
	case errors.Is(err, runtimestorage.ErrInvalid):
		return http.StatusBadRequest, "invalid_request"
	case matchesAny(err, tenant.ErrInvalid, appmodel.ErrInvalid, modelprofile.ErrInvalid, backend.ErrInvalid, channels.ErrInvalid):
		return http.StatusBadRequest, "invalid_request"
	case matchesAny(err, tenant.ErrInvalidTransition, appmodel.ErrInvalidTransition, modelprofile.ErrInvalidTransition, backend.ErrInvalidTransition, channels.ErrInvalidTransition, tenant.ErrDisabled, appmodel.ErrDisabled, modelprofile.ErrDisabled, backend.ErrDisabled, channels.ErrDisabled, appmodel.ErrImmutableRevision):
		return http.StatusBadRequest, "invalid_request"
	default:
		return http.StatusInternalServerError, "internal_error"
	}
}

func matchesAny(err error, candidates ...error) bool {
	for _, candidate := range candidates {
		if errors.Is(err, candidate) {
			return true
		}
	}
	return false
}

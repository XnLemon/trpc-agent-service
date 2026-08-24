package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

type Config struct {
	Tenants        tenant.Repository
	Apps           agent.Repository
	Models         modelprofile.Repository
	Backends       backend.Repository
	Bindings       channels.Repository
	Authenticator  Authenticator
	ModelCatalog   *modelprofile.ProviderCatalog
	BackendCatalog *backend.ProviderCatalog
}

type Handler struct{ config Config }

func NewHandler(config Config) (*Handler, error) {
	if config.Tenants == nil || config.Apps == nil || config.Models == nil || config.Backends == nil || config.Bindings == nil || config.Authenticator == nil {
		return nil, errors.New("invalid admin handler configuration")
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
	if len(parts) == 0 || parts[0] != "tenants" {
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
	writeJSON(w, requestID, status, value)
}

var errNotFound = errors.New("admin route not found")
var errInvalidRequest = errors.New("invalid admin request")

func (h *Handler) tenants(ctx context.Context, r *http.Request, p Principal) (int, any, error) {
	if r.Method != http.MethodPost || !p.Allows("", true) {
		return 0, nil, ErrForbidden
	}
	var input tenant.CreateInput
	if err := decodeBody(r, &input); err != nil {
		return 0, nil, err
	}
	created, err := h.config.Tenants.Create(ctx, input)
	return http.StatusCreated, created, err
}

func (h *Handler) tenantRoute(ctx context.Context, r *http.Request, p Principal, parts []string, requestID string) (int, any, error) {
	tenantID := parts[0]
	if tenantID == "" || !p.Allows(tenantID, false) {
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
		if r.Method != http.MethodPost {
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
	default:
		return 0, nil, errNotFound
	}
}

func (h *Handler) apps(ctx context.Context, r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if len(parts) == 0 {
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var input agent.CreateInput
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
			var body agent.UpdateMetadataInput
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
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body struct {
			ExpectedVersion int64
			NextStatus      agent.Status
			Reason          string
			CorrelationID   string
		}
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		value, event, err := h.config.Apps.TransitionStatus(ctx, agent.TransitionStatusInput{TenantID: tenantID, AppID: appID, ExpectedVersion: body.ExpectedVersion, NextStatus: body.NextStatus, Metadata: agent.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: body.Reason, CorrelationID: body.CorrelationID}})
		return http.StatusOK, map[string]any{"app": value, "event": event}, err
	case "revisions":
		return h.revisions(ctx, r, p, tenantID, appID, parts[2:])
	case "rollback":
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body agent.RollbackInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.AppID = tenantID, appID
		body.Metadata.ActorType, body.Metadata.ActorID = "admin", p.SubjectID
		value, event, err := h.config.Apps.Rollback(ctx, body)
		return http.StatusOK, map[string]any{"app": value, "event": event}, err
	default:
		return 0, nil, errNotFound
	}
}

func (h *Handler) revisions(ctx context.Context, r *http.Request, p Principal, tenantID, appID string, parts []string) (int, any, error) {
	if len(parts) == 0 {
		if r.Method != http.MethodPost {
			return 0, nil, errNotFound
		}
		var body agent.CreateDraftInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.AppID = tenantID, appID
		value, err := h.config.Apps.CreateDraft(ctx, body)
		return http.StatusCreated, value, err
	}
	revision, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, nil, err
	}
	if len(parts) == 1 {
		if r.Method != http.MethodPatch {
			return 0, nil, errNotFound
		}
		var body agent.UpdateDraftInput
		if err := decodeBody(r, &body); err != nil {
			return 0, nil, err
		}
		body.TenantID, body.AppID, body.Revision = tenantID, appID, revision
		value, err := h.config.Apps.UpdateDraft(ctx, body)
		return http.StatusOK, value, err
	}
	if parts[1] != "publish" || r.Method != http.MethodPost {
		return 0, nil, errNotFound
	}
	var body agent.PublishInput
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

func decodeBody(r *http.Request, dst any) error {
	if r == nil || r.Body == nil {
		return fmt.Errorf("%w: request body is required", errInvalidRequest)
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("%w: read request body", errInvalidRequest)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return fmt.Errorf("%w: request body is required", errInvalidRequest)
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
			out[toExported(key)] = normalizeKeys(child)
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
		"tenant_id": "TenantID", "tenant_key": "TenantKey", "app_id": "AppID", "app_key": "AppKey",
		"profile_id": "ProfileID", "binding_id": "BindingID", "display_name": "DisplayName",
		"description": "Description", "expected_version": "ExpectedVersion",
		"expected_app_version": "ExpectedAppVersion", "expected_draft_version": "ExpectedDraftVersion",
		"next_status": "NextStatus", "target_revision": "TargetRevision", "reason": "Reason",
		"correlation_id": "CorrelationID", "schema_version": "SchemaVersion", "secret_ref": "SecretRef",
		"provider_account_id": "ProviderAccountID", "public_route_key_digest": "PublicRouteKeyDigest",
		"audit_retention_days": "AuditRetentionDays", "log_masking_level": "LogMaskingLevel",
		"trace_sampling_rate": "TraceSamplingRate", "rate_limit_rpm": "RateLimitRPM",
		"max_concurrent_executions": "MaxConcurrentExecutions", "monthly_token_budget": "MonthlyTokenBudget",
		"monthly_spend_limit_minor": "MonthlySpendLimitMinor", "billing_currency": "BillingCurrency",
		"default_agent_app_id": "DefaultAgentAppID", "default_backend_profile_id": "DefaultBackendProfileID",
		"configuration": "Configuration", "provider": "Provider", "model": "Model",
		"endpoint": "Endpoint", "options": "Options", "generation": "Generation",
		"temperature": "Temperature", "top_p": "TopP", "max_output_tokens": "MaxOutputTokens",
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
	writeError(w, requestID, status, category)
}

func mapError(err error) (int, string) {
	if err == nil {
		return http.StatusInternalServerError, "internal_error"
	}
	if errors.Is(err, errInvalidRequest) {
		return http.StatusBadRequest, "invalid_request"
	}
	if errors.Is(err, ErrUnauthenticated) {
		return http.StatusUnauthorized, "unauthorized"
	}
	if errors.Is(err, ErrForbidden) {
		return http.StatusForbidden, "forbidden"
	}
	if errors.Is(err, errNotFound) || errors.Is(err, tenant.ErrNotFound) || errors.Is(err, agent.ErrNotFound) || errors.Is(err, modelprofile.ErrNotFound) || errors.Is(err, backend.ErrNotFound) || errors.Is(err, channels.ErrNotFound) {
		return http.StatusNotFound, "not_found"
	}
	if errors.Is(err, tenant.ErrConflict) || errors.Is(err, agent.ErrConflict) || errors.Is(err, modelprofile.ErrConflict) || errors.Is(err, backend.ErrConflict) || errors.Is(err, channels.ErrConflict) || errors.Is(err, tenant.ErrDuplicateKey) || errors.Is(err, agent.ErrDuplicateKey) || errors.Is(err, modelprofile.ErrDuplicateKey) || errors.Is(err, backend.ErrDuplicateKey) || errors.Is(err, channels.ErrDuplicateKey) {
		return http.StatusConflict, "conflict"
	}
	if errors.Is(err, postgres.ErrStorage) {
		return http.StatusServiceUnavailable, "storage_unavailable"
	}
	if errors.Is(err, tenant.ErrInvalid) || errors.Is(err, agent.ErrInvalid) || errors.Is(err, modelprofile.ErrInvalid) || errors.Is(err, backend.ErrInvalid) || errors.Is(err, channels.ErrInvalid) {
		return http.StatusBadRequest, "invalid_request"
	}
	if errors.Is(err, tenant.ErrInvalidTransition) || errors.Is(err, agent.ErrInvalidTransition) || errors.Is(err, modelprofile.ErrInvalidTransition) || errors.Is(err, backend.ErrInvalidTransition) || errors.Is(err, channels.ErrInvalidTransition) || errors.Is(err, tenant.ErrDisabled) || errors.Is(err, agent.ErrDisabled) || errors.Is(err, modelprofile.ErrDisabled) || errors.Is(err, backend.ErrDisabled) || errors.Is(err, channels.ErrDisabled) || errors.Is(err, agent.ErrImmutableRevision) {
		return http.StatusBadRequest, "invalid_request"
	}
	return http.StatusInternalServerError, "internal_error"
}

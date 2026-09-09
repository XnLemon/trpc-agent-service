package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

const toolInvocationColumns = "tenant_id,app_id,invocation_id,event_id,request_id,trace_id,tool_call_id,tool_name,args_sha256,status,owner,fencing_token,error_class,reviewer_id,created_at,updated_at"

var _ runtimestorage.ToolInvocationStore = (*Store)(nil)
var _ runtimestorage.ToolInvocationRecoveryStore = (*Store)(nil)
var _ runtimestorage.ToolInvocationReconciliationStore = (*Store)(nil)
var _ runtimestorage.ToolInvocationAuditStore = (*Store)(nil)

func (s *Store) PrepareToolInvocation(ctx context.Context, input runtimestorage.ToolInvocationInput) (runtimestorage.ToolInvocation, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if err := runtimestorage.ValidateToolInvocationInput(input); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	_, err := s.db.ExecContext(ctx, "INSERT INTO public.runtime_tool_invocation (tenant_id,app_id,invocation_id,event_id,request_id,trace_id,tool_call_id,tool_name,args_sha256,status,owner,fencing_token) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'prepared',$10,1) ON CONFLICT (tenant_id,app_id,invocation_id) DO NOTHING", input.TenantID, input.AppID, input.InvocationID, input.EventID, input.RequestID, input.TraceID, input.ToolCallID, input.ToolName, input.ArgsSHA256, input.Owner)
	if err != nil {
		return runtimestorage.ToolInvocation{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	value, err := s.GetToolInvocation(ctx, input.TenantID, input.AppID, input.InvocationID)
	if err != nil {
		if errors.Is(err, runtimestorage.ErrNotFound) {
			// The secondary (tenant,event,tool_call) key belongs to a
			// different derived identity. Never turn that collision into a
			// retryable not-found result.
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
		}
		return runtimestorage.ToolInvocation{}, err
	}
	if value.AppID != input.AppID || value.EventID != input.EventID || value.RequestID != input.RequestID || value.TraceID != input.TraceID || value.ToolCallID != input.ToolCallID || value.ToolName != input.ToolName || value.ArgsSHA256 != input.ArgsSHA256 {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
	}
	return value, nil
}

func (s *Store) TransitionToolInvocation(ctx context.Context, transition runtimestorage.ToolInvocationTransition) (runtimestorage.ToolInvocation, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if err := runtimestorage.ValidateToolInvocationTransitionRequest(transition); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	var value runtimestorage.ToolInvocation
	var status string
	err := s.db.QueryRowContext(ctx, "UPDATE public.runtime_tool_invocation SET status=$4,error_class=$5,reviewer_id=$6,fencing_token=fencing_token+1,updated_at=now() WHERE tenant_id=$1 AND app_id=$2 AND invocation_id=$3 AND status=$7 AND fencing_token=$8 AND ($9='' OR owner=$9 OR $4='manual' OR $7='manual') RETURNING "+toolInvocationColumns, transition.TenantID, transition.AppID, transition.InvocationID, string(transition.To), strings.TrimSpace(transition.ErrorClass), strings.TrimSpace(transition.ReviewerID), string(transition.From), transition.FencingToken, strings.TrimSpace(transition.Owner)).Scan(toolInvocationArgs(&value, &status)...)
	if err == nil {
		value.Status = runtimestorage.ToolInvocationStatus(status)
		if err := runtimestorage.ValidateToolInvocation(value); err != nil {
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
		}
		return value, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.ToolInvocation{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	current, getErr := s.GetToolInvocation(ctx, transition.TenantID, transition.AppID, transition.InvocationID)
	if getErr != nil {
		return runtimestorage.ToolInvocation{}, getErr
	}
	if current.Status == transition.To && current.FencingToken == transition.FencingToken+1 {
		return current, nil
	}
	return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
}

// RecoverStaleToolInvocations atomically fences provider hand-offs that
// survived neither a response nor the local process. The UPDATE is tenant
// independent because the recovery worker is a process-level safety net; the
// returned rows remain tenant-labelled for audit and reconciliation.
func (s *Store) RecoverStaleToolInvocations(ctx context.Context, before time.Time) ([]runtimestorage.ToolInvocation, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if before.IsZero() {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "UPDATE public.runtime_tool_invocation SET status='unknown',error_class='provider_uncertain',reviewer_id='',fencing_token=fencing_token+1,updated_at=now() WHERE status IN ('dispatching','accepted') AND updated_at < $1 RETURNING "+toolInvocationColumns, before.UTC())
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	values := make([]runtimestorage.ToolInvocation, 0)
	for rows.Next() {
		var value runtimestorage.ToolInvocation
		var status string
		if err := rows.Scan(toolInvocationArgs(&value, &status)...); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		value.Status = runtimestorage.ToolInvocationStatus(status)
		if err := runtimestorage.ValidateToolInvocation(value); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	sort.Slice(values, func(i, j int) bool {
		if !values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].CreatedAt.Before(values[j].CreatedAt)
		}
		if values[i].TenantID != values[j].TenantID {
			return values[i].TenantID < values[j].TenantID
		}
		if values[i].AppID != values[j].AppID {
			return values[i].AppID < values[j].AppID
		}
		return values[i].InvocationID < values[j].InvocationID
	})
	return values, nil
}

// ListToolInvocationReconciliation returns durable unknown provider hand-offs
// whose reconciliation audit may need retrying after a worker failure.
func (s *Store) ListToolInvocationReconciliation(ctx context.Context) ([]runtimestorage.ToolInvocation, error) {
	values, err := s.ListToolInvocationAuditCandidates(ctx)
	if err != nil {
		return nil, err
	}
	filtered := values[:0]
	for _, value := range values {
		if value.Status == runtimestorage.ToolInvocationUnknown && value.ErrorClass == "provider_uncertain" {
			filtered = append(filtered, value)
		}
	}
	return filtered, nil
}

// ListToolInvocationAuditCandidates returns terminal or provider-accepted rows
// whose deterministic audit facts can be repaired after a process failure.
func (s *Store) ListToolInvocationAuditCandidates(ctx context.Context) ([]runtimestorage.ToolInvocation, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+toolInvocationColumns+" FROM public.runtime_tool_invocation WHERE status IN ('accepted','succeeded','failed','denied','unknown','manual') ORDER BY tenant_id,app_id,created_at,invocation_id")
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	values := make([]runtimestorage.ToolInvocation, 0)
	for rows.Next() {
		var value runtimestorage.ToolInvocation
		var status string
		if err := rows.Scan(toolInvocationArgs(&value, &status)...); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		value.Status = runtimestorage.ToolInvocationStatus(status)
		if err := runtimestorage.ValidateToolInvocation(value); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

func (s *Store) GetToolInvocation(ctx context.Context, tenantID, appID, invocationID string) (runtimestorage.ToolInvocation, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if err := runtimestorage.ValidateToolInvocationLookup(tenantID, appID, invocationID); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	var value runtimestorage.ToolInvocation
	var status string
	err := s.db.QueryRowContext(ctx, "SELECT "+toolInvocationColumns+" FROM public.runtime_tool_invocation WHERE tenant_id=$1 AND app_id=$2 AND invocation_id=$3", tenantID, appID, invocationID).Scan(toolInvocationArgs(&value, &status)...)
	if err != nil {
		return runtimestorage.ToolInvocation{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	value.Status = runtimestorage.ToolInvocationStatus(status)
	if err := runtimestorage.ValidateToolInvocation(value); err != nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	return value, nil
}

func (s *Store) ListToolInvocations(ctx context.Context, tenantID, appID string, statuses []runtimestorage.ToolInvocationStatus) ([]runtimestorage.ToolInvocation, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if err := runtimestorage.ValidateToolInvocationTenantApp(tenantID, appID); err != nil {
		return nil, err
	}
	args := []any{tenantID, appID}
	query := "SELECT " + toolInvocationColumns + " FROM public.runtime_tool_invocation WHERE tenant_id=$1 AND app_id=$2"
	if len(statuses) > 0 {
		placeholders := make([]string, 0, len(statuses))
		for _, status := range statuses {
			if !runtimestorage.ValidToolInvocationStatus(status) {
				return nil, runtimestorage.ErrInvalid
			}
			args = append(args, string(status))
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		query += " AND status IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at,invocation_id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	values := make([]runtimestorage.ToolInvocation, 0)
	for rows.Next() {
		var value runtimestorage.ToolInvocation
		var status string
		if err := rows.Scan(toolInvocationArgs(&value, &status)...); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		value.Status = runtimestorage.ToolInvocationStatus(status)
		if err := runtimestorage.ValidateToolInvocation(value); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

func toolInvocationArgs(value *runtimestorage.ToolInvocation, status *string) []any {
	return []any{&value.TenantID, &value.AppID, &value.InvocationID, &value.EventID, &value.RequestID, &value.TraceID, &value.ToolCallID, &value.ToolName, &value.ArgsSHA256, status, &value.Owner, &value.FencingToken, &value.ErrorClass, &value.ReviewerID, &value.CreatedAt, &value.UpdatedAt}
}

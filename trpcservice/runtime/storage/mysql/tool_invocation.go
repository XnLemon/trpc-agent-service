package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	controlmysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
)

const toolInvocationColumns = "tenant_id,app_id,invocation_id,event_id,request_id,trace_id,tool_call_id,tool_name,args_sha256,status,owner,fencing_token,error_class,reviewer_id,created_at,updated_at"

var _ runtimestorage.ToolInvocationStore = (*Store)(nil)
var _ runtimestorage.ToolInvocationRecoveryStore = (*Store)(nil)
var _ runtimestorage.ToolInvocationReconciliationStore = (*Store)(nil)
var _ runtimestorage.ToolInvocationAuditStore = (*Store)(nil)

// Store implements the durable tool invocation ledger using the MySQL
// application connection. It borrows the pool and never closes it.
type Store struct{ db *sql.DB }

// New creates a MySQL tool invocation store. It does not query the database;
// migration and readiness checks remain owned by Bootstrap.
func New(db *sql.DB) *Store { return &Store{db: db} }

func (store *Store) PrepareToolInvocation(ctx context.Context, input runtimestorage.ToolInvocationInput) (runtimestorage.ToolInvocation, error) {
	if err := check(ctx, store); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if err := runtimestorage.ValidateToolInvocationInput(input); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	_, err := store.db.ExecContext(ctx, "INSERT INTO runtime_tool_invocation (tenant_id,app_id,invocation_id,event_id,request_id,trace_id,tool_call_id,tool_name,args_sha256,status,owner,fencing_token) VALUES (?,?,?,?,?,?,?,?,?,'prepared',?,1) ON DUPLICATE KEY UPDATE invocation_id=invocation_id", input.TenantID, input.AppID, input.InvocationID, input.EventID, input.RequestID, input.TraceID, input.ToolCallID, input.ToolName, input.ArgsSHA256, input.Owner)
	if err != nil {
		return runtimestorage.ToolInvocation{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return runtimestorage.ToolInvocation{}, contextError(err)
	}
	value, err := store.GetToolInvocation(ctx, input.TenantID, input.AppID, input.InvocationID)
	if err != nil {
		if errors.Is(err, runtimestorage.ErrNotFound) {
			// A duplicate secondary key with another derived ID is a
			// deterministic conflict, not a missing invocation.
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
		}
		return runtimestorage.ToolInvocation{}, err
	}
	if value.AppID != input.AppID || value.EventID != input.EventID || value.RequestID != input.RequestID || value.TraceID != input.TraceID || value.ToolCallID != input.ToolCallID || value.ToolName != input.ToolName || value.ArgsSHA256 != input.ArgsSHA256 || value.Owner != input.Owner {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
	}
	return value, nil
}

func (store *Store) TransitionToolInvocation(ctx context.Context, transition runtimestorage.ToolInvocationTransition) (runtimestorage.ToolInvocation, error) {
	if err := check(ctx, store); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if err := runtimestorage.ValidateToolInvocationTransitionRequest(transition); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	owner := strings.TrimSpace(transition.Owner)
	result, err := store.db.ExecContext(ctx, "UPDATE runtime_tool_invocation SET status=?,error_class=?,reviewer_id=?,fencing_token=fencing_token+1,updated_at=CURRENT_TIMESTAMP(6) WHERE tenant_id=? AND app_id=? AND invocation_id=? AND status=? AND fencing_token=? AND (?='' OR owner=? OR ?='manual' OR ?='manual')", string(transition.To), strings.TrimSpace(transition.ErrorClass), strings.TrimSpace(transition.ReviewerID), transition.TenantID, transition.AppID, transition.InvocationID, string(transition.From), transition.FencingToken, owner, owner, string(transition.To), string(transition.From))
	if err != nil {
		return runtimestorage.ToolInvocation{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	if rows > 1 {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	if rows == 1 {
		current, getErr := store.GetToolInvocation(ctx, transition.TenantID, transition.AppID, transition.InvocationID)
		if getErr != nil {
			return runtimestorage.ToolInvocation{}, getErr
		}
		if !matchesToolInvocationTransition(current, transition) {
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
		}
		return current, nil
	}
	current, getErr := store.GetToolInvocation(ctx, transition.TenantID, transition.AppID, transition.InvocationID)
	if getErr != nil {
		return runtimestorage.ToolInvocation{}, getErr
	}
	if current.Status == transition.To && current.FencingToken == transition.FencingToken+1 && current.ErrorClass == strings.TrimSpace(transition.ErrorClass) && current.ReviewerID == strings.TrimSpace(transition.ReviewerID) {
		return current, nil
	}
	return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
}

// RecoverStaleToolInvocations locks stale hand-offs in one transaction and
// fences each row before releasing it. InnoDB has no portable UPDATE RETURNING
// contract, so the explicit row lock is required to make reconciliation race
// safe with workers and Admin transitions.
func (store *Store) RecoverStaleToolInvocations(ctx context.Context, before time.Time) ([]runtimestorage.ToolInvocation, error) {
	if err := check(ctx, store); err != nil {
		return nil, err
	}
	if before.IsZero() {
		return nil, runtimestorage.ErrInvalid
	}
	tx, err := begin(ctx, store.db)
	if err != nil {
		return nil, err
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	rows, err := tx.QueryContext(ctx, "SELECT "+toolInvocationColumns+" FROM runtime_tool_invocation WHERE status IN ('dispatching','accepted') AND updated_at < ? ORDER BY created_at,tenant_id,app_id,invocation_id FOR UPDATE", before.UTC())
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	values := make([]runtimestorage.ToolInvocation, 0)
	for rows.Next() {
		var value runtimestorage.ToolInvocation
		var status string
		if scanErr := scanToolInvocation(rows, &value, &status); scanErr != nil {
			_ = rows.Close()
			return nil, normalizeScanError(ctx, scanErr)
		}
		values = append(values, value)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return nil, runtimestorage.ErrStorage
	}
	if closeErr := rows.Close(); closeErr != nil {
		return nil, runtimestorage.ErrStorage
	}
	recoveredAt := time.Now().UTC()
	for index := range values {
		value := &values[index]
		result, updateErr := tx.ExecContext(ctx, "UPDATE runtime_tool_invocation SET status='unknown',error_class='provider_uncertain',reviewer_id='',fencing_token=fencing_token+1,updated_at=? WHERE tenant_id=? AND app_id=? AND invocation_id=? AND status=? AND fencing_token=?", recoveredAt, value.TenantID, value.AppID, value.InvocationID, string(value.Status), value.FencingToken)
		if updateErr != nil {
			return nil, mapError(ctx, updateErr, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
		}
		affected, affectedErr := result.RowsAffected()
		if affectedErr != nil || affected != 1 {
			return nil, runtimestorage.ErrConflict
		}
		value.Status = runtimestorage.ToolInvocationUnknown
		value.ErrorClass = "provider_uncertain"
		value.ReviewerID = ""
		value.FencingToken++
		value.UpdatedAt = recoveredAt
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	rollback = false
	return values, nil
}

// ListToolInvocationReconciliation returns unknown provider hand-offs whose
// reconciliation audit may need retrying after a worker failure.
func (store *Store) ListToolInvocationReconciliation(ctx context.Context) ([]runtimestorage.ToolInvocation, error) {
	values, err := store.ListToolInvocationAuditCandidates(ctx)
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
func (store *Store) ListToolInvocationAuditCandidates(ctx context.Context) ([]runtimestorage.ToolInvocation, error) {
	if err := check(ctx, store); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, "SELECT "+toolInvocationColumns+" FROM runtime_tool_invocation WHERE status IN ('accepted','succeeded','failed','denied','unknown','manual') ORDER BY tenant_id,app_id,created_at,invocation_id")
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	values := make([]runtimestorage.ToolInvocation, 0)
	for rows.Next() {
		var value runtimestorage.ToolInvocation
		var status string
		if err := scanToolInvocation(rows, &value, &status); err != nil {
			return nil, normalizeScanError(ctx, err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, normalizeScanError(ctx, err)
	}
	if err := rows.Close(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

func (store *Store) GetToolInvocation(ctx context.Context, tenantID, appID, invocationID string) (runtimestorage.ToolInvocation, error) {
	if err := check(ctx, store); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if err := runtimestorage.ValidateToolInvocationLookup(tenantID, appID, invocationID); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	var value runtimestorage.ToolInvocation
	var status string
	err := scanToolInvocation(store.db.QueryRowContext(ctx, "SELECT "+toolInvocationColumns+" FROM runtime_tool_invocation WHERE tenant_id=? AND app_id=? AND invocation_id=?", tenantID, appID, invocationID), &value, &status)
	if err != nil {
		if errors.Is(err, runtimestorage.ErrStorage) {
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
		}
		return runtimestorage.ToolInvocation{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return runtimestorage.ToolInvocation{}, contextError(err)
	}
	if value.TenantID != tenantID || value.AppID != appID || value.InvocationID != invocationID {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	return value, nil
}

func (store *Store) ListToolInvocations(ctx context.Context, tenantID, appID string, statuses []runtimestorage.ToolInvocationStatus) ([]runtimestorage.ToolInvocation, error) {
	if err := check(ctx, store); err != nil {
		return nil, err
	}
	if err := runtimestorage.ValidateToolInvocationTenantApp(tenantID, appID); err != nil {
		return nil, err
	}
	args := []any{tenantID, appID}
	query := "SELECT " + toolInvocationColumns + " FROM runtime_tool_invocation WHERE tenant_id=? AND app_id=?"
	if len(statuses) > 0 {
		placeholders := make([]string, 0, len(statuses))
		for _, status := range statuses {
			if !runtimestorage.ValidToolInvocationStatus(status) {
				return nil, runtimestorage.ErrInvalid
			}
			args = append(args, string(status))
			placeholders = append(placeholders, "?")
		}
		query += " AND status IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at,invocation_id"
	rows, err := store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	values := make([]runtimestorage.ToolInvocation, 0)
	for rows.Next() {
		var value runtimestorage.ToolInvocation
		var status string
		if err := scanToolInvocation(rows, &value, &status); err != nil {
			return nil, normalizeScanError(ctx, err)
		}
		if value.TenantID != tenantID || value.AppID != appID {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, normalizeScanError(ctx, err)
	}
	if err := rows.Close(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

func toolInvocationArgs(value *runtimestorage.ToolInvocation, status *string) []any {
	return []any{&value.TenantID, &value.AppID, &value.InvocationID, &value.EventID, &value.RequestID, &value.TraceID, &value.ToolCallID, &value.ToolName, &value.ArgsSHA256, status, &value.Owner, &value.FencingToken, &value.ErrorClass, &value.ReviewerID, &value.CreatedAt, &value.UpdatedAt}
}

func check(ctx context.Context, store *Store) error {
	if nilvalue.Is(ctx) {
		return runtimestorage.ErrInvalid
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return contextError(err)
	}
	if store == nil || store.db == nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

// invocationScanner is implemented by both sql.Row and sql.Rows. Keeping the
// scan and validation path shared prevents one read path from accidentally
// returning a non-UTC or otherwise corrupt snapshot.
type invocationScanner interface {
	Scan(...any) error
}

func scanToolInvocation(scanner invocationScanner, value *runtimestorage.ToolInvocation, status *string) error {
	if scanner == nil || value == nil || status == nil {
		return runtimestorage.ErrStorage
	}
	if err := scanner.Scan(toolInvocationArgs(value, status)...); err != nil {
		return err
	}
	value.Status = runtimestorage.ToolInvocationStatus(*status)
	value.CreatedAt = controlmysql.AsUTC(value.CreatedAt)
	value.UpdatedAt = controlmysql.AsUTC(value.UpdatedAt)
	if err := runtimestorage.ValidateToolInvocation(*value); err != nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

func normalizeScanError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return mapError(ctx, err, runtimestorage.ErrStorage, runtimestorage.ErrStorage, runtimestorage.ErrStorage, runtimestorage.ErrStorage)
}

func matchesToolInvocationTransition(value runtimestorage.ToolInvocation, transition runtimestorage.ToolInvocationTransition) bool {
	return value.Status == transition.To && value.FencingToken == transition.FencingToken+1 && value.ErrorClass == strings.TrimSpace(transition.ErrorClass) && value.ReviewerID == strings.TrimSpace(transition.ReviewerID)
}

func contextError(err error) error {
	if errors.Is(err, nilvalue.ErrInvalidContext) {
		return runtimestorage.ErrInvalid
	}
	return err
}

func mapError(ctx context.Context, err error, notFound, duplicate, conflict, invalid error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, runtimestorage.ErrStorage) {
		return runtimestorage.ErrStorage
	}
	mapped := controlmysql.MapError(ctx, err, notFound, duplicate, conflict, invalid)
	if errors.Is(mapped, controlmysql.ErrStorage) {
		return runtimestorage.ErrStorage
	}
	return contextError(mapped)
}

func begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := controlmysql.Begin(ctx, db)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return tx, nil
}

func commit(ctx context.Context, tx *sql.Tx) error {
	return mapError(ctx, controlmysql.Commit(ctx, tx), runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
}

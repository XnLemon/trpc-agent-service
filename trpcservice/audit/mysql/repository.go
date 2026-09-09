// Package mysql implements the tenant-bound MySQL audit writer.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	controlmysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
)

// ErrStorage is returned when the MySQL audit table cannot be reached.
var ErrStorage = errors.New("mysql audit storage error")

// Store is permanently bound to one tenant. It only implements audit.Writer;
// reporting and retention queries remain separate operator concerns.
type Store struct {
	db       *sql.DB
	tenantID string
}

var _ audit.Writer = (*Store)(nil)

// New creates a tenant-bound MySQL audit writer. The database pool is borrowed.
func New(db *sql.DB, tenantID string) (*Store, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || strings.ContainsAny(tenantID, "\r\n") {
		return nil, audit.ErrInvalid
	}
	return &Store{db: db, tenantID: tenantID}, nil
}

// NewMultiTenant creates a writer that accepts the tenant scope already
// present on each validated audit event. Bootstrap uses it only when the
// process has explicitly configured multiple API identities.
func NewMultiTenant(db *sql.DB) *MultiTenantStore { return &MultiTenantStore{db: db} }

// MultiTenantStore is the process-level MySQL writer. Event validation and the
// tenant predicate are still enforced for every append.
type MultiTenantStore struct{ db *sql.DB }

var _ audit.Writer = (*MultiTenantStore)(nil)

func (store *MultiTenantStore) Append(ctx context.Context, event audit.Event) (audit.AppendResult, error) {
	if store == nil {
		return audit.AppendResult{}, ErrStorage
	}
	return appendEvent(ctx, store.db, event, "")
}

func (store *Store) Append(ctx context.Context, event audit.Event) (audit.AppendResult, error) {
	if store == nil {
		return audit.AppendResult{}, ErrStorage
	}
	return appendEvent(ctx, store.db, event, store.tenantID)
}

func appendEvent(ctx context.Context, db *sql.DB, event audit.Event, tenantID string) (audit.AppendResult, error) {
	if nilvalue.Is(ctx) {
		return audit.AppendResult{}, audit.ErrInvalid
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return audit.AppendResult{}, err
	}
	if db == nil {
		return audit.AppendResult{}, ErrStorage
	}
	if tenantID != "" && event.TenantID != tenantID {
		return audit.AppendResult{}, audit.ErrTenantScope
	}
	digest, err := event.Digest()
	if err != nil {
		return audit.AppendResult{}, err
	}
	result, err := db.ExecContext(ctx, `INSERT INTO audit_event (
		tenant_id,event_id,schema_version,event_type,channel,user_id,session_id,
		agent_app_id,revision,model_profile_id,tool_name,decision,latency_ms,error_type,
		input_tokens,output_tokens,model_cost_minor,tool_cost_minor,currency,
		budget_used_tokens,budget_used_minor,execution_result,provider,model,request_id,
		trace_id,correlation_id,actor_type,actor_id,reason,previous_version,next_version,
		occurred_at,digest
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON DUPLICATE KEY UPDATE event_id=event_id`,
		event.TenantID, event.EventID, event.SchemaVersion, string(event.EventType), event.Channel,
		event.UserID, event.SessionID, event.AgentAppID, event.Revision, event.ModelProfileID,
		event.ToolName, string(event.Decision), event.LatencyMS, event.ErrorType,
		usageInt(event.Cost, func(value *audit.Usage) *int64 { return value.InputTokens }),
		usageInt(event.Cost, func(value *audit.Usage) *int64 { return value.OutputTokens }),
		usageInt(event.Cost, func(value *audit.Usage) *int64 { return value.ModelCostMinor }),
		usageInt(event.Cost, func(value *audit.Usage) *int64 { return value.ToolCostMinor }),
		usageString(event.Cost, func(value *audit.Usage) string { return value.Currency }),
		usageInt(event.Cost, func(value *audit.Usage) *int64 { return value.BudgetUsedTokens }),
		usageInt(event.Cost, func(value *audit.Usage) *int64 { return value.BudgetUsedMinor }),
		usageString(event.Cost, func(value *audit.Usage) string { return string(value.ExecutionResult) }),
		usageString(event.Cost, func(value *audit.Usage) string { return value.Provider }),
		usageString(event.Cost, func(value *audit.Usage) string { return value.Model }),
		event.RequestID, event.TraceID, event.CorrelationID, event.ActorType, event.ActorID,
		event.Reason, event.PreviousVersion, event.NextVersion, event.OccurredAt.UTC(), digest)
	if err != nil {
		return audit.AppendResult{}, controlmysql.MapError(ctx, err, audit.ErrNotFound, audit.ErrConflict, audit.ErrConflict, audit.ErrInvalid)
	}
	duplicate := false
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return audit.AppendResult{}, ErrStorage
	} else {
		duplicate = affected == 0
	}
	var storedDigest string
	if err := db.QueryRowContext(ctx, "SELECT digest FROM audit_event WHERE tenant_id=? AND event_id=?", event.TenantID, event.EventID).Scan(&storedDigest); err != nil {
		return audit.AppendResult{}, controlmysql.MapError(ctx, err, audit.ErrNotFound, audit.ErrConflict, audit.ErrConflict, audit.ErrInvalid)
	}
	if storedDigest != digest {
		return audit.AppendResult{}, audit.ErrConflict
	}
	// The insert is idempotent; the digest comparison above is the security
	// boundary. Returning Duplicate is informational and does not affect retry.
	return audit.AppendResult{Event: event.Clone(), Duplicate: duplicate, Digest: storedDigest}, nil
}

func usageInt(value *audit.Usage, selectValue func(*audit.Usage) *int64) any {
	if value == nil {
		return nil
	}
	selected := selectValue(value)
	if selected == nil {
		return nil
	}
	return *selected
}

func usageString(value *audit.Usage, selectValue func(*audit.Usage) string) any {
	if value == nil {
		return ""
	}
	return selectValue(value)
}

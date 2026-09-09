package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func checkCapability(ctx context.Context, store *Store) error {
	if err := check(ctx); err != nil {
		return err
	}
	if store == nil || store.db == nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

func (s *Store) PutSummary(ctx context.Context, value runtimestorage.SummaryRecord) (runtimestorage.SummaryRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(value.TenantID, value.SessionID) != nil || strings.TrimSpace(value.Text) == "" || value.EventSeq < 0 {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	var out runtimestorage.SummaryRecord
	err := s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_summary (tenant_id,session_id,filter_key,text,event_seq,version) VALUES ($1,$2,$3,$4,$5,1) ON CONFLICT (tenant_id,session_id,filter_key) DO UPDATE SET text=EXCLUDED.text,event_seq=EXCLUDED.event_seq,version=public.runtime_summary.version+1,updated_at=now() WHERE EXCLUDED.event_seq >= public.runtime_summary.event_seq RETURNING tenant_id,session_id,filter_key,text,event_seq,version,created_at,updated_at", value.TenantID, value.SessionID, value.FilterKey, value.Text, value.EventSeq).Scan(&out.TenantID, &out.SessionID, &out.FilterKey, &out.Text, &out.EventSeq, &out.Version, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimestorage.SummaryRecord{}, runtimestorage.ErrConflict
		}
		return runtimestorage.SummaryRecord{}, mapSummaryError(ctx, err)
	}
	return out, nil
}

func (s *Store) GetSummary(ctx context.Context, tenantID, sessionID, filterKey string) (runtimestorage.SummaryRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.SummaryRecord
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,session_id,filter_key,text,event_seq,version,created_at,updated_at FROM public.runtime_summary WHERE tenant_id=$1 AND session_id=$2 AND filter_key=$3", tenantID, sessionID, filterKey).Scan(&value.TenantID, &value.SessionID, &value.FilterKey, &value.Text, &value.EventSeq, &value.Version, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return runtimestorage.SummaryRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return value, nil
}

func (s *Store) EnqueueSummary(ctx context.Context, value runtimestorage.SummaryRecord) error {
	_, err := s.PutSummary(ctx, value)
	return err
}

func (s *Store) AppendAudit(ctx context.Context, value runtimestorage.AuditRecord) (runtimestorage.AuditRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.AuditRecord{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.EventType, 128, true) || !runtimestorage.ValidateText(value.AuditID, 256, false) {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.AuditID == "" {
		value.AuditID = uuid.NewString()
	}
	payloadValue := value.Payload
	if payloadValue == nil {
		payloadValue = map[string]any{}
	}
	payload, err := pgstorage.EncodeJSON(payloadValue)
	if err != nil {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.OccurredAt.IsZero() {
		value.OccurredAt = time.Now().UTC()
	}
	var out runtimestorage.AuditRecord
	var raw []byte
	err = s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_audit_log (tenant_id,audit_id,event_type,payload,occurred_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,audit_id) DO UPDATE SET audit_id=EXCLUDED.audit_id WHERE public.runtime_audit_log.event_type=EXCLUDED.event_type AND public.runtime_audit_log.payload=EXCLUDED.payload RETURNING tenant_id,audit_id,event_type,payload,occurred_at", value.TenantID, value.AuditID, value.EventType, payload, value.OccurredAt).Scan(&out.TenantID, &out.AuditID, &out.EventType, &raw, &out.OccurredAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimestorage.AuditRecord{}, runtimestorage.ErrConflict
		}
		return runtimestorage.AuditRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if pgstorage.DecodeJSON(raw, &out.Payload) != nil {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrStorage
	}
	return out, nil
}

func (s *Store) ListAudit(ctx context.Context, tenantID string, since time.Time, limit int) ([]runtimestorage.AuditRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,audit_id,event_type,payload,occurred_at FROM public.runtime_audit_log WHERE tenant_id=$1 AND ($2::timestamptz IS NULL OR occurred_at >= $2) ORDER BY occurred_at,audit_id LIMIT NULLIF($3,0)", tenantID, nullTime(since), limit)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	values := make([]runtimestorage.AuditRecord, 0)
	for rows.Next() {
		var value runtimestorage.AuditRecord
		var raw []byte
		if err := rows.Scan(&value.TenantID, &value.AuditID, &value.EventType, &raw, &value.OccurredAt); err != nil || pgstorage.DecodeJSON(raw, &value.Payload) != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if rows.Err() != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

func mapSummaryError(ctx context.Context, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return runtimestorage.ErrNotFound
	}
	return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

var _ runtimestorage.SummaryStore = (*Store)(nil)
var _ runtimestorage.AuditStore = (*Store)(nil)

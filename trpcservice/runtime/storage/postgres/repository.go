// Package postgres implements the tenant-scoped runtime storage contract on PostgreSQL.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) GetSession(ctx context.Context, tenantID, sessionID string) (runtimestorage.Session, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return runtimestorage.Session{}, err
	}
	var value runtimestorage.Session
	var state []byte
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id, session_id, status, version, state, created_at, updated_at FROM public.runtime_session WHERE tenant_id=$1 AND session_id=$2", tenantID, sessionID).Scan(&value.TenantID, &value.SessionID, &value.Status, &value.Version, &state, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return runtimestorage.Session{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := pgstorage.DecodeJSON(state, &value.State); err != nil {
		return runtimestorage.Session{}, runtimestorage.ErrStorage
	}
	return cloneSession(value), nil
}

func (s *Store) CreateSession(ctx context.Context, tenantID, sessionID string, state map[string]any) (runtimestorage.Session, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return runtimestorage.Session{}, err
	}
	if state == nil {
		state = map[string]any{}
	}
	encoded, err := pgstorage.EncodeJSON(state)
	if err != nil {
		return runtimestorage.Session{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.Session
	var persisted []byte
	err = s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_session (tenant_id, session_id, status, version, state) VALUES ($1,$2,'active',1,$3) RETURNING tenant_id,session_id,status,version,state,created_at,updated_at", tenantID, sessionID, encoded).Scan(&value.TenantID, &value.SessionID, &value.Status, &value.Version, &persisted, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return runtimestorage.Session{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := pgstorage.DecodeJSON(persisted, &value.State); err != nil {
		return runtimestorage.Session{}, runtimestorage.ErrStorage
	}
	return cloneSession(value), nil
}

func (s *Store) UpdateSessionState(ctx context.Context, tenantID, sessionID string, expectedVersion int64, state map[string]any) (runtimestorage.Session, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return runtimestorage.Session{}, err
	}
	if state == nil {
		state = map[string]any{}
	}
	encoded, err := pgstorage.EncodeJSON(state)
	if err != nil {
		return runtimestorage.Session{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.Session
	var persisted []byte
	err = s.db.QueryRowContext(ctx, "UPDATE public.runtime_session SET version=version+1,state=$4,updated_at=now() WHERE tenant_id=$1 AND session_id=$2 AND version=$3 RETURNING tenant_id,session_id,status,version,state,created_at,updated_at", tenantID, sessionID, expectedVersion, encoded).Scan(&value.TenantID, &value.SessionID, &value.Status, &value.Version, &persisted, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if _, getErr := s.GetSession(ctx, tenantID, sessionID); getErr != nil {
				return runtimestorage.Session{}, getErr
			}
			return runtimestorage.Session{}, runtimestorage.ErrConflict
		}
		return runtimestorage.Session{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := pgstorage.DecodeJSON(persisted, &value.State); err != nil {
		return runtimestorage.Session{}, runtimestorage.ErrStorage
	}
	return cloneSession(value), nil
}

func (s *Store) DeleteSession(ctx context.Context, tenantID, sessionID string) error {
	if err := check(ctx); err != nil {
		return err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, "DELETE FROM public.runtime_session WHERE tenant_id=$1 AND session_id=$2", tenantID, sessionID)
	if err != nil {
		return pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimestorage.ErrStorage
	}
	if rows == 0 {
		return runtimestorage.ErrNotFound
	}
	return nil
}

func (s *Store) RecordMessage(ctx context.Context, input runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	if runtimestorage.ValidateSession(input.TenantID, input.SessionID) != nil || input.BindingID == "" || input.ExternalMessageID == "" || input.EventID == "" {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrInvalid
	}
	tx, err := pgstorage.Begin(ctx, s.db)
	if err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	defer pgstorage.Rollback(tx)
	var existing runtimestorage.MessageEvent
	err = tx.QueryRowContext(ctx, "SELECT tenant_id,event_id,session_id,binding_id,external_message_id,idempotency_key,event_seq,status,fencing_token,lease_owner,lease_expires_at,reply_id,segment_count,created_at,updated_at FROM public.message_event WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3", input.TenantID, input.BindingID, input.ExternalMessageID).Scan(eventArgs(&existing)...)
	if err == nil {
		return cloneEvent(existing), true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.MessageEvent{}, false, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	var version int64
	if err := tx.QueryRowContext(ctx, "UPDATE public.runtime_session SET version=version+1,updated_at=now() WHERE tenant_id=$1 AND session_id=$2 RETURNING version", input.TenantID, input.SessionID).Scan(&version); err != nil {
		return runtimestorage.MessageEvent{}, false, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	err = tx.QueryRowContext(ctx, "INSERT INTO public.message_event (tenant_id,event_id,session_id,binding_id,external_message_id,idempotency_key,event_seq,status) VALUES ($1,$2,$3,$4,$5,$6,$7,'received') RETURNING tenant_id,event_id,session_id,binding_id,external_message_id,idempotency_key,event_seq,status,fencing_token,lease_owner,lease_expires_at,reply_id,segment_count,created_at,updated_at", input.TenantID, input.EventID, input.SessionID, input.BindingID, input.ExternalMessageID, input.IdempotencyKey, version).Scan(eventArgs(&existing)...)
	if err != nil {
		mapped := pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
		if errors.Is(mapped, runtimestorage.ErrDuplicate) {
			pgstorage.Rollback(tx)
			if duplicate, lookupErr := s.lookupMessageByExternal(ctx, input.TenantID, input.BindingID, input.ExternalMessageID); lookupErr == nil {
				return duplicate, true, nil
			}
		}
		return runtimestorage.MessageEvent{}, false, mapped
	}
	if err := pgstorage.Commit(ctx, tx); err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	return cloneEvent(existing), false, nil
}

func (s *Store) lookupMessageByExternal(ctx context.Context, tenantID, bindingID, externalID string) (runtimestorage.MessageEvent, error) {
	var value runtimestorage.MessageEvent
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,event_id,session_id,binding_id,external_message_id,idempotency_key,event_seq,status,fencing_token,lease_owner,lease_expires_at,reply_id,segment_count,created_at,updated_at FROM public.message_event WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3", tenantID, bindingID, externalID).Scan(eventArgs(&value)...)
	if err != nil {
		return runtimestorage.MessageEvent{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneEvent(value), nil
}

func (s *Store) GetMessage(ctx context.Context, tenantID, eventID string) (runtimestorage.MessageEvent, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.MessageEvent
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,event_id,session_id,binding_id,external_message_id,idempotency_key,event_seq,status,fencing_token,lease_owner,lease_expires_at,reply_id,segment_count,created_at,updated_at FROM public.message_event WHERE tenant_id=$1 AND event_id=$2", tenantID, eventID).Scan(eventArgs(&value)...)
	if err != nil {
		return runtimestorage.MessageEvent{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneEvent(value), nil
}

func (s *Store) EnqueueReply(ctx context.Context, value runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || value.ReplyID == "" || value.EventID == "" || value.SegmentIndex < 0 || value.SegmentCount <= value.SegmentIndex {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if value.Status == "" {
		value.Status = runtimestorage.ReplyPending
	}
	if value.Status != runtimestorage.ReplyPending {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	var result runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "INSERT INTO public.reply_outbox (tenant_id,reply_id,event_id,segment_index,segment_count,payload,status) VALUES ($1,$2,$3,$4,$5,$6,'pending') ON CONFLICT (tenant_id,reply_id,segment_index) DO UPDATE SET updated_at=public.reply_outbox.updated_at RETURNING tenant_id,reply_id,event_id,segment_index,segment_count,payload,status,attempts,fencing_token,lease_owner,lease_expires_at,provider_message_id,last_error_class,created_at,updated_at", value.TenantID, value.ReplyID, value.EventID, value.SegmentIndex, value.SegmentCount, value.Payload).Scan(replyArgs(&result)...)
	if err != nil {
		return runtimestorage.ReplyOutbox{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneReply(result), nil
}

func (s *Store) GetReply(ctx context.Context, tenantID, replyID string, segment int) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || segment < 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,reply_id,event_id,segment_index,segment_count,payload,status,attempts,fencing_token,lease_owner,lease_expires_at,provider_message_id,last_error_class,created_at,updated_at FROM public.reply_outbox WHERE tenant_id=$1 AND reply_id=$2 AND segment_index=$3", tenantID, replyID, segment).Scan(replyArgs(&value)...)
	if err != nil {
		return runtimestorage.ReplyOutbox{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneReply(value), nil
}

func (s *Store) ClaimReply(ctx context.Context, tenantID, replyID string, segment int, owner string, leaseDuration time.Duration) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || owner == "" || leaseDuration <= 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	seconds := int64(leaseDuration / time.Second)
	if seconds == 0 {
		seconds = 1
	}
	var value runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "UPDATE public.reply_outbox SET status='sending', attempts=attempts+1, fencing_token=fencing_token+1, lease_owner=$4, lease_expires_at=now()+($5 * interval '1 second'), updated_at=now() WHERE tenant_id=$1 AND reply_id=$2 AND segment_index=$3 AND (status IN ('pending','retryable') OR (status='sending' AND lease_expires_at IS NOT NULL AND lease_expires_at <= now())) RETURNING tenant_id,reply_id,event_id,segment_index,segment_count,payload,status,attempts,fencing_token,lease_owner,lease_expires_at,provider_message_id,last_error_class,created_at,updated_at", tenantID, replyID, segment, owner, seconds).Scan(replyArgs(&value)...)
	if err == nil {
		return cloneReply(value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.ReplyOutbox{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if _, lookupErr := s.GetReply(ctx, tenantID, replyID, segment); lookupErr != nil {
		return runtimestorage.ReplyOutbox{}, lookupErr
	}
	return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
}

func (s *Store) TransitionReply(ctx context.Context, transition runtimestorage.ReplyTransition) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(transition.TenantID) != nil || transition.ReplyID == "" || transition.Owner == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateTransition(transition.From, transition.To) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrIllegalTransition
	}
	leaseSeconds := int64(0)
	if transition.LeaseDuration > 0 {
		leaseSeconds = int64(transition.LeaseDuration / time.Second)
		if leaseSeconds == 0 {
			leaseSeconds = 1
		}
	}
	var value runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "UPDATE public.reply_outbox SET status=$5, attempts=attempts+CASE WHEN $5='sending' THEN 1 ELSE 0 END, fencing_token=fencing_token+1, lease_owner=$6, lease_expires_at=CASE WHEN $7>0 THEN now()+($7 * interval '1 second') ELSE NULL END, provider_message_id=COALESCE(NULLIF($8,''),provider_message_id), last_error_class=COALESCE(NULLIF($9,''),last_error_class), updated_at=now() WHERE tenant_id=$1 AND reply_id=$2 AND segment_index=$3 AND status=$4 AND (lease_owner='' OR lease_owner=$6) AND ($10=0 OR fencing_token=$10) AND (status <> 'sending' OR lease_expires_at IS NULL OR lease_expires_at > now()) RETURNING tenant_id,reply_id,event_id,segment_index,segment_count,payload,status,attempts,fencing_token,lease_owner,lease_expires_at,provider_message_id,last_error_class,created_at,updated_at", transition.TenantID, transition.ReplyID, transition.SegmentIndex, transition.From, transition.To, transition.Owner, leaseSeconds, transition.ProviderID, transition.ErrorClass, transition.FencingToken).Scan(replyArgs(&value)...)
	if err == nil {
		return cloneReply(value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.ReplyOutbox{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if _, lookupErr := s.GetReply(ctx, transition.TenantID, transition.ReplyID, transition.SegmentIndex); lookupErr != nil {
		return runtimestorage.ReplyOutbox{}, lookupErr
	}
	return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
}

func (s *Store) Close() error { return nil }
func check(ctx context.Context) error {
	if ctx == nil {
		return runtimestorage.ErrInvalid
	}
	return ctx.Err()
}
func eventArgs(value *runtimestorage.MessageEvent) []any {
	return []any{&value.TenantID, &value.EventID, &value.SessionID, &value.BindingID, &value.ExternalMessageID, &value.IdempotencyKey, &value.EventSeq, &value.Status, &value.FencingToken, &value.LeaseOwner, &value.LeaseExpiresAt, &value.ReplyID, &value.SegmentCount, &value.CreatedAt, &value.UpdatedAt}
}
func replyArgs(value *runtimestorage.ReplyOutbox) []any {
	return []any{&value.TenantID, &value.ReplyID, &value.EventID, &value.SegmentIndex, &value.SegmentCount, &value.Payload, &value.Status, &value.Attempts, &value.FencingToken, &value.LeaseOwner, &value.LeaseExpiresAt, &value.ProviderMessageID, &value.LastErrorClass, &value.CreatedAt, &value.UpdatedAt}
}
func cloneSession(value runtimestorage.Session) runtimestorage.Session {
	if value.State != nil {
		copy := make(map[string]any, len(value.State))
		for k, v := range value.State {
			copy[k] = v
		}
		value.State = copy
	}
	return value
}
func cloneEvent(value runtimestorage.MessageEvent) runtimestorage.MessageEvent {
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}
func cloneReply(value runtimestorage.ReplyOutbox) runtimestorage.ReplyOutbox {
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}

var _ runtimestorage.RuntimeStore = (*Store)(nil)

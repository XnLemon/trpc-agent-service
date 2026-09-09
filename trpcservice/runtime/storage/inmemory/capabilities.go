package inmemory

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/google/uuid"
)

func (s *Store) PutSummary(ctx context.Context, value runtimestorage.SummaryRecord) (runtimestorage.SummaryRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(value.TenantID, value.SessionID) != nil || strings.TrimSpace(value.Text) == "" || value.EventSeq < 0 {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[key(value.TenantID, value.SessionID)]; !ok {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrNotFound
	}
	k := key(value.TenantID, value.SessionID, value.FilterKey)
	if existing, ok := s.summaries[k]; ok {
		if value.EventSeq < existing.EventSeq {
			return runtimestorage.SummaryRecord{}, runtimestorage.ErrConflict
		}
		value.Version, value.CreatedAt = existing.Version+1, existing.CreatedAt
	} else {
		value.Version, value.CreatedAt = 1, time.Now().UTC()
	}
	value.UpdatedAt = time.Now().UTC()
	s.summaries[k] = value
	return value, nil
}

func (s *Store) GetSummary(ctx context.Context, tenantID, sessionID, filterKey string) (runtimestorage.SummaryRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.summaries[key(tenantID, sessionID, filterKey)]
	if !ok {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrNotFound
	}
	return value, nil
}

func (s *Store) EnqueueSummary(ctx context.Context, value runtimestorage.SummaryRecord) error {
	_, err := s.PutSummary(ctx, value)
	return err
}

func (s *Store) AppendAudit(ctx context.Context, value runtimestorage.AuditRecord) (runtimestorage.AuditRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.AuditRecord{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.EventType, 128, true) || !runtimestorage.ValidateText(value.AuditID, 256, false) {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.Payload != nil && cloneMap(value.Payload) == nil {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.AuditID == "" {
		value.AuditID = uuid.NewString()
	}
	if value.OccurredAt.IsZero() {
		value.OccurredAt = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.audits[value.TenantID]
	for _, row := range rows {
		if row.AuditID == value.AuditID {
			if !reflect.DeepEqual(row.Payload, value.Payload) || row.EventType != value.EventType {
				return runtimestorage.AuditRecord{}, runtimestorage.ErrConflict
			}
			return cloneAudit(row), nil
		}
	}
	value.Payload = cloneMap(value.Payload)
	if value.Payload == nil {
		value.Payload = map[string]any{}
	}
	s.audits[value.TenantID] = append(rows, value)
	return cloneAudit(value), nil
}

func (s *Store) ListAudit(ctx context.Context, tenantID string, since time.Time, limit int) ([]runtimestorage.AuditRecord, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	values := make([]runtimestorage.AuditRecord, 0)
	for _, row := range s.audits[tenantID] {
		if since.IsZero() || !row.OccurredAt.Before(since) {
			values = append(values, cloneAudit(row))
		}
	}
	s.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].OccurredAt.Equal(values[j].OccurredAt) {
			return values[i].AuditID < values[j].AuditID
		}
		return values[i].OccurredAt.Before(values[j].OccurredAt)
	})
	if limit > 0 && len(values) > limit {
		values = values[:limit]
	}
	return values, nil
}

func cloneAudit(value runtimestorage.AuditRecord) runtimestorage.AuditRecord {
	value.Payload = cloneMap(value.Payload)
	return value
}

var _ runtimestorage.SummaryStore = (*Store)(nil)
var _ runtimestorage.AuditStore = (*Store)(nil)

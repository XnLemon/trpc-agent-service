package storage

import (
	"context"
	"time"
)

// SummaryRecord is the latest summary for one tenant/session/filter branch.
type SummaryRecord struct {
	TenantID  string
	SessionID string
	FilterKey string
	Text      string
	EventSeq  int64
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SummaryStore persists versioned, filter-aware summaries.
type SummaryStore interface {
	PutSummary(context.Context, SummaryRecord) (SummaryRecord, error)
	GetSummary(context.Context, string, string, string) (SummaryRecord, error)
	EnqueueSummary(context.Context, SummaryRecord) error
}

// AuditRecord is an append-only tenant audit fact. Payload must be redacted by
// the caller before it reaches a store.
type AuditRecord struct {
	TenantID   string
	AuditID    string
	EventType  string
	Payload    map[string]any
	OccurredAt time.Time
}

// AuditStore is the tenant-scoped append/read audit contract.
type AuditStore interface {
	AppendAudit(context.Context, AuditRecord) (AuditRecord, error)
	ListAudit(context.Context, string, time.Time, int) ([]AuditRecord, error)
}

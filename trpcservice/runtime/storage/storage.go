// Package storage defines the tenant-scoped runtime persistence contract.
package storage

import (
	"context"
	"errors"
	"time"

	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

var (
	ErrNotFound          = errors.New("runtime record not found")
	ErrDuplicate         = errors.New("runtime record already exists")
	ErrConflict          = errors.New("runtime version conflict")
	ErrInvalid           = errors.New("invalid runtime record")
	ErrIllegalTransition = errors.New("illegal runtime state transition")
	ErrStorage           = pgstorage.ErrStorage
)

const (
	SessionActive = "active"
	SessionClosed = "closed"

	EventReceived             = "received"
	EventRunning              = "running"
	EventCompleted            = "completed"
	EventExecutionReconciling = "execution_reconciling"
	EventReplyPending         = "reply_pending"
	EventReplied              = "replied"
	EventFailed               = "failed"

	ReplyPending    = "pending"
	ReplySending    = "sending"
	ReplySent       = "sent"
	ReplyRetryable  = "retryable"
	ReplyDeadLetter = "dead_letter"
)

type Session struct {
	TenantID  string
	SessionID string
	Status    string
	Version   int64
	State     map[string]any
	CreatedAt time.Time
	UpdatedAt time.Time
}

type MessageEvent struct {
	TenantID          string
	EventID           string
	SessionID         string
	BindingID         string
	ExternalMessageID string
	IdempotencyKey    string
	EventSeq          int64
	Status            string
	FencingToken      int64
	LeaseOwner        string
	LeaseExpiresAt    *time.Time
	ReplyID           string
	SegmentCount      int
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type MessageEventInput struct {
	TenantID          string
	EventID           string
	SessionID         string
	BindingID         string
	ExternalMessageID string
	IdempotencyKey    string
}

type ReplyOutbox struct {
	TenantID          string
	ReplyID           string
	EventID           string
	SegmentIndex      int
	SegmentCount      int
	Payload           string
	Status            string
	Attempts          int
	FencingToken      int64
	LeaseOwner        string
	LeaseExpiresAt    *time.Time
	ProviderMessageID string
	LastErrorClass    string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type ReplyTransition struct {
	TenantID      string
	ReplyID       string
	SegmentIndex  int
	From          string
	To            string
	Owner         string
	FencingToken  int64
	LeaseDuration time.Duration
	ErrorClass    string
	ProviderID    string
}

type RuntimeStore interface {
	GetSession(context.Context, string, string) (Session, error)
	CreateSession(context.Context, string, string, map[string]any) (Session, error)
	UpdateSessionState(context.Context, string, string, int64, map[string]any) (Session, error)
	RecordMessage(context.Context, MessageEventInput) (MessageEvent, bool, error)
	GetMessage(context.Context, string, string) (MessageEvent, error)
	EnqueueReply(context.Context, ReplyOutbox) (ReplyOutbox, error)
	GetReply(context.Context, string, string, int) (ReplyOutbox, error)
	TransitionReply(context.Context, ReplyTransition) (ReplyOutbox, error)
	Close() error
}

func ValidateTenant(tenantID string) error {
	if tenantID == "" {
		return ErrInvalid
	}
	return nil
}

func ValidateSession(tenantID, sessionID string) error {
	if ValidateTenant(tenantID) != nil || sessionID == "" {
		return ErrInvalid
	}
	return nil
}

func ValidateTransition(from, to string) bool {
	switch from {
	case ReplyPending:
		return to == ReplySending || to == ReplyRetryable
	case ReplySending:
		return to == ReplySent || to == ReplyRetryable || to == ReplyDeadLetter
	case ReplyRetryable:
		return to == ReplySending || to == ReplyDeadLetter
	case ReplySent, ReplyDeadLetter:
		return false
	default:
		return false
	}
}

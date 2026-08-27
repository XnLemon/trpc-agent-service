// Package storage defines the tenant-scoped runtime persistence contract.
package storage

import (
	"context"
	"errors"
	"strings"
	"time"

	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

const maxReplyTargetIDRunes = 1024

// ReplyTarget is the trusted, durable destination for a channel reply. A zero
// target is retained only for rows created before per-message routing existed.
type ReplyTarget struct {
	BindingID        string
	ConversationKind string
	ReceiverID       string
	ThreadID         string
}

var (
	// ErrNotFound reports a missing tenant-scoped runtime record.
	ErrNotFound = errors.New("runtime record not found")
	// ErrDuplicate reports an existing runtime record with the same identity.
	ErrDuplicate = errors.New("runtime record already exists")
	// ErrConflict reports an optimistic-concurrency conflict.
	ErrConflict = errors.New("runtime version conflict")
	// ErrInvalid reports malformed runtime input.
	ErrInvalid = errors.New("invalid runtime record")
	// ErrIllegalTransition reports a disallowed runtime lifecycle change.
	ErrIllegalTransition = errors.New("illegal runtime state transition")
	// ErrStorage reports unavailable runtime persistence.
	ErrStorage = pgstorage.ErrStorage
)

const (
	// SessionActive marks a session that accepts new events.
	SessionActive = "active"
	// SessionClosed marks a session that no longer accepts events.
	SessionClosed = "closed"

	// EventReceived marks a message accepted for execution.
	EventReceived = "received"
	// EventRunning marks a message currently being executed.
	EventRunning = "running"
	// EventCompleted marks a successfully executed message.
	EventCompleted = "completed"
	// EventExecutionReconciling marks an execution being reconciled after lease loss.
	EventExecutionReconciling = "execution_reconciling"
	// EventReplyPending marks a message waiting for durable reply delivery.
	EventReplyPending = "reply_pending"
	// EventReplied marks a message whose reply has been delivered.
	EventReplied = "replied"
	// EventFailed marks a message that cannot complete.
	EventFailed = "failed"

	// ReplyPending marks a reply segment waiting for delivery.
	ReplyPending = "pending"
	// ReplySending marks a reply segment currently being delivered.
	ReplySending = "sending"
	// ReplySent marks a reply segment confirmed by the provider.
	ReplySent = "sent"
	// ReplyRetryable marks a reply segment eligible for another attempt.
	ReplyRetryable = "retryable"
	// ReplyDeadLetter marks a reply segment that exhausted delivery attempts.
	ReplyDeadLetter = "dead_letter"
)

// Session is the durable tenant-scoped conversation state.
type Session struct {
	TenantID  string
	SessionID string
	Status    string
	Version   int64
	State     map[string]any
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MessageEvent is the durable inbound message lifecycle record.
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
	ReplyTarget       ReplyTarget
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// MessageEventInput contains the identity fields for recording an inbound message.
type MessageEventInput struct {
	TenantID          string
	EventID           string
	SessionID         string
	BindingID         string
	ExternalMessageID string
	IdempotencyKey    string
	ReplyTarget       ReplyTarget
}

// EventPayload is one immutable upstream Runner event retained for durable
// session recovery. Payload is JSON and must never be included in logs or
// returned through an unauthorised HTTP surface.
type EventPayload struct {
	TenantID   string
	SessionID  string
	EventID    string
	Payload    []byte
	HistorySeq int64
	CreatedAt  time.Time
}

// MessageTransition advances a persisted inbound message through its execution
// lifecycle. Transitions out of running require the current owner and fence.
type MessageTransition struct {
	TenantID      string
	EventID       string
	From          string
	To            string
	Owner         string
	FencingToken  int64
	LeaseDuration time.Duration
	// ReplyID and SegmentCount bind a completed Runner execution to the
	// materialized outbox identity. They are only set for a successful reply.
	ReplyID      string
	SegmentCount int
}

// ReplyOutbox is one durable, independently deliverable reply segment.
type ReplyOutbox struct {
	TenantID          string
	ReplyID           string
	EventID           string
	SegmentIndex      int
	SegmentCount      int
	Payload           string
	ReplyTarget       ReplyTarget
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

// ReplyCorrelation is the durable link between an execution request and its
// asynchronously delivered reply. It is kept separately so existing reply
// rows remain backwards compatible.
type ReplyCorrelation struct {
	TenantID  string
	EventID   string
	RequestID string
	TraceID   string
}

// ReplyTransition requests a fenced reply lifecycle transition.
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

// RuntimeStore is the tenant-scoped persistence contract used by Runner.
type RuntimeStore interface {
	GetSession(context.Context, string, string) (Session, error)
	CreateSession(context.Context, string, string, map[string]any) (Session, error)
	UpdateSessionState(context.Context, string, string, int64, map[string]any) (Session, error)
	DeleteSession(context.Context, string, string) error
	RecordMessage(context.Context, MessageEventInput) (MessageEvent, bool, error)
	GetMessage(context.Context, string, string) (MessageEvent, error)
	TransitionMessage(context.Context, MessageTransition) (MessageEvent, error)
	AppendEventPayload(context.Context, EventPayload) (EventPayload, error)
	ListEventPayloads(context.Context, string, string) ([]EventPayload, error)
	EnqueueReply(context.Context, ReplyOutbox) (ReplyOutbox, error)
	ListReplyCandidates(context.Context, string) ([]ReplyOutbox, error)
	GetReply(context.Context, string, string, int) (ReplyOutbox, error)
	ClaimReply(context.Context, string, string, int, string, time.Duration) (ReplyOutbox, error)
	TransitionReply(context.Context, ReplyTransition) (ReplyOutbox, error)
	Close() error
}

// ReplyBatchEnqueuer is the atomic reply-materialization capability. A batch
// either makes every segment durable or makes none of its new segments visible
// to a delivery worker. It remains separate from RuntimeStore so existing
// readers can keep a narrow dependency surface.
type ReplyBatchEnqueuer interface {
	EnqueueReplies(context.Context, []ReplyOutbox) ([]ReplyOutbox, error)
}

// ReplyBatchCorrelationEnqueuer atomically persists a reply correlation and
// its complete segment batch.
type ReplyBatchCorrelationEnqueuer interface {
	EnqueueRepliesWithCorrelation(context.Context, ReplyCorrelation, []ReplyOutbox) ([]ReplyOutbox, error)
}

// ReplyCorrelationStore persists request/trace identifiers for reply delivery
// audit and recovery. It is optional for legacy runtime stores.
type ReplyCorrelationStore interface {
	SaveReplyCorrelation(context.Context, ReplyCorrelation) error
	GetReplyCorrelation(context.Context, string, string) (ReplyCorrelation, error)
}

// ValidateTenant checks the required tenant identity.
func ValidateTenant(tenantID string) error {
	if tenantID == "" {
		return ErrInvalid
	}
	return nil
}

// ValidateSession checks a tenant and session identity pair.
func ValidateSession(tenantID, sessionID string) error {
	if ValidateTenant(tenantID) != nil || sessionID == "" {
		return ErrInvalid
	}
	return nil
}

// ValidateReplyTarget accepts either the legacy zero target or a complete
// direct/group destination. Partial values are never safe to route.
func ValidateReplyTarget(target ReplyTarget) error {
	if target == (ReplyTarget{}) {
		return nil
	}
	if !validReplyTargetID(target.BindingID) || !validReplyTargetID(target.ReceiverID) {
		return ErrInvalid
	}
	switch target.ConversationKind {
	case "direct", "group":
	default:
		return ErrInvalid
	}
	if target.ThreadID != "" && !validReplyTargetID(target.ThreadID) {
		return ErrInvalid
	}
	return nil
}

func validReplyTargetID(value string) bool {
	if strings.TrimSpace(value) == "" || len([]rune(value)) > maxReplyTargetIDRunes {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

// ValidateTransition reports whether a reply transition is legal.
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

// ValidateMessageTransition defines the durable inbound execution lifecycle.
func ValidateMessageTransition(from, to string) bool {
	switch from {
	case EventReceived:
		return to == EventRunning || to == EventFailed
	case EventRunning:
		return to == EventCompleted || to == EventExecutionReconciling || to == EventFailed
	case EventExecutionReconciling:
		return to == EventRunning || to == EventFailed
	case EventCompleted:
		return to == EventReplyPending || to == EventFailed
	case EventReplyPending:
		return to == EventReplied || to == EventFailed
	default:
		return false
	}
}

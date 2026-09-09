// Package storage defines the tenant-scoped runtime persistence contract.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
)

// ValidateText enforces the same character bounds used by the runtime DDL.
func ValidateText(value string, max int, requireNonEmpty bool) bool {
	if !utf8.ValidString(value) {
		return false
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return !requireNonEmpty && value == ""
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return max <= 0 || len([]rune(value)) <= max
}

// ValidateEmbedding rejects non-finite values that JSON/PostgreSQL cannot represent.
func ValidateEmbedding(values []float64) bool {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

const maxReplyTargetIDRunes = 1024

// ReplyKind identifies the durable representation a channel provider should
// attempt before falling back to text.
type ReplyKind string

const (
	// ReplyKindText identifies the legacy text reply path.
	ReplyKindText ReplyKind = "text"
	// ReplyKindImage identifies an image attachment reply.
	ReplyKindImage ReplyKind = "image"
	// ReplyKindVideo identifies a video attachment reply.
	ReplyKindVideo ReplyKind = "video"
	// ReplyKindAudio identifies an audio attachment reply.
	ReplyKindAudio ReplyKind = "audio"
	// ReplyKindDocument identifies a document attachment reply.
	ReplyKindDocument ReplyKind = "document"
)

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
	ErrNotFound = sessionstorage.ErrNotFound
	// ErrDuplicate reports an existing runtime record with the same identity.
	ErrDuplicate = sessionstorage.ErrDuplicate
	// ErrConflict reports an optimistic-concurrency conflict.
	ErrConflict = errors.New("runtime version conflict")
	// ErrInvalid reports malformed runtime input.
	ErrInvalid = sessionstorage.ErrInvalid
	// ErrIllegalTransition reports a disallowed runtime lifecycle change.
	ErrIllegalTransition = errors.New("illegal runtime state transition")
	// ErrStorage reports unavailable runtime persistence without coupling the
	// runtime contract to a particular database adapter. The legacy error text is
	// retained for callers that expose it in diagnostics.
	ErrStorage = sessionstorage.ErrStorage
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
	Kind              ReplyKind
	Payload           string
	Attachment        attachment.Reference
	Fallback          string
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
	TenantID    string
	EventID     string
	RequestID   string
	TraceID     string
	TraceParent string
}

// ReplyReceipt records a provider acknowledgement without advancing the reply
// lifecycle. The outbox worker owns the subsequent sending-to-sent transition.
type ReplyReceipt struct {
	TenantID     string
	ReplyID      string
	SegmentIndex int
	Owner        string
	FencingToken int64
	ProviderID   string
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

// ToolInvocationStatus identifies the durable side-effect state of one tool
// call. Unknown is intentionally distinct from failed: a remote provider may
// have accepted a request even when the local process did not receive a
// response.
type ToolInvocationStatus string

const (
	ToolInvocationPrepared    ToolInvocationStatus = "prepared"
	ToolInvocationDispatching ToolInvocationStatus = "dispatching"
	ToolInvocationAccepted    ToolInvocationStatus = "accepted"
	ToolInvocationSucceeded   ToolInvocationStatus = "succeeded"
	ToolInvocationFailed      ToolInvocationStatus = "failed"
	ToolInvocationDenied      ToolInvocationStatus = "denied"
	ToolInvocationUnknown     ToolInvocationStatus = "unknown"
	ToolInvocationManual      ToolInvocationStatus = "manual"
)

// ToolInvocation records metadata required to reconcile a tool side effect.
// Raw arguments, results, and provider credentials are deliberately excluded;
// ArgsSHA256 is sufficient to detect a conflicting retry.
type ToolInvocation struct {
	TenantID     string               `json:"tenant_id"`
	AppID        string               `json:"app_id"`
	InvocationID string               `json:"invocation_id"`
	EventID      string               `json:"event_id"`
	RequestID    string               `json:"request_id"`
	TraceID      string               `json:"trace_id,omitempty"`
	ToolCallID   string               `json:"tool_call_id"`
	ToolName     string               `json:"tool_name"`
	ArgsSHA256   string               `json:"args_sha256"`
	Status       ToolInvocationStatus `json:"status"`
	Owner        string               `json:"owner"`
	FencingToken int64                `json:"fencing_token"`
	ErrorClass   string               `json:"error_class,omitempty"`
	ReviewerID   string               `json:"reviewer_id,omitempty"`
	CreatedAt    time.Time            `json:"created_at"`
	UpdatedAt    time.Time            `json:"updated_at"`
}

// ToolInvocationInput prepares one idempotent tool call record.
type ToolInvocationInput struct {
	TenantID     string
	AppID        string
	InvocationID string
	EventID      string
	RequestID    string
	TraceID      string
	ToolCallID   string
	ToolName     string
	ArgsSHA256   string
	Owner        string
}

// ToolInvocationTransition advances a prepared call using an optimistic
// fencing token. From is mandatory so stale workers cannot overwrite a newer
// reconciliation decision.
type ToolInvocationTransition struct {
	TenantID     string
	AppID        string
	InvocationID string
	From         ToolInvocationStatus
	To           ToolInvocationStatus
	Owner        string
	FencingToken int64
	ErrorClass   string
	ReviewerID   string
}

// DeriveToolInvocationID creates a deterministic identity without retaining
// raw arguments. argsSHA256 must be the digest of canonical JSON arguments.
// The optional appID is included when supplied. The no-app form is retained
// only so old, already-materialized test fixtures can still be decoded; all
// new platform writes use DeriveToolInvocationIDForApp.
func DeriveToolInvocationID(tenantID, eventID, toolCallID, toolName, argsSHA256 string, appID ...string) string {
	values := []string{tenantID, eventID, toolCallID, toolName, argsSHA256}
	if len(appID) > 0 && strings.TrimSpace(appID[0]) != "" {
		values = []string{tenantID, appID[0], eventID, toolCallID, toolName, argsSHA256}
	}
	h := sha256.New()
	for _, value := range values {
		var length [4]byte
		length[0] = byte(len(value) >> 24)
		length[1] = byte(len(value) >> 16)
		length[2] = byte(len(value) >> 8)
		length[3] = byte(len(value))
		h.Write(length[:])
		h.Write([]byte(value))
	}
	return "toolinv_" + hex.EncodeToString(h.Sum(nil))[:32]
}

// DeriveToolInvocationIDForApp is the tenant/app-scoped identity function used
// by all new invocation records.
func DeriveToolInvocationIDForApp(tenantID, appID, eventID, toolCallID, toolName, argsSHA256 string) string {
	return DeriveToolInvocationID(tenantID, eventID, toolCallID, toolName, argsSHA256, appID)
}

// ValidateToolInvocationTenantApp validates a tenant/app lookup scope.
func ValidateToolInvocationTenantApp(tenantID, appID string) error {
	if ValidateTenant(tenantID) != nil || appmodel.ValidateAppID(appID) != nil {
		return ErrInvalid
	}
	return nil
}

// ValidateToolInvocationLookup validates the complete key used by a durable
// invocation read. It rejects padded or control-bearing identities instead of
// allowing a database adapter to interpret a different namespace.
func ValidateToolInvocationLookup(tenantID, appID, invocationID string) error {
	if err := ValidateToolInvocationTenantApp(tenantID, appID); err != nil || !validToolInvocationText(invocationID, 256, true) {
		return ErrInvalid
	}
	return nil
}

func validToolInvocationText(value string, max int, required bool) bool {
	return utf8.ValidString(value) && value == strings.TrimSpace(value) && strings.IndexFunc(value, unicode.IsControl) < 0 && !strings.Contains(value, "://") && len([]rune(value)) <= max && (!required || value != "")
}

// ValidateToolInvocationInput validates the secret-free prepare contract.
func ValidateToolInvocationInput(input ToolInvocationInput) error {
	if ValidateToolInvocationTenantApp(input.TenantID, input.AppID) != nil {
		return ErrInvalid
	}
	for _, value := range []string{input.InvocationID, input.EventID, input.RequestID, input.TraceID, input.ToolCallID, input.ToolName, input.ArgsSHA256, input.Owner} {
		if !utf8.ValidString(value) || value != strings.TrimSpace(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 || strings.Contains(value, "://") {
			return ErrInvalid
		}
	}
	if strings.TrimSpace(input.TenantID) == "" || strings.TrimSpace(input.AppID) == "" || strings.TrimSpace(input.InvocationID) == "" || strings.TrimSpace(input.EventID) == "" || strings.TrimSpace(input.RequestID) == "" || strings.TrimSpace(input.ToolCallID) == "" || strings.TrimSpace(input.ToolName) == "" || strings.TrimSpace(input.Owner) == "" || len(input.TenantID) > 256 || len(input.AppID) > 256 || len(input.InvocationID) > 256 || len(input.EventID) > 256 || len(input.RequestID) > 256 || len(input.TraceID) > 256 || len(input.ToolCallID) > 256 || len(input.ToolName) > 256 || len(input.Owner) > 256 || len(input.ArgsSHA256) != sha256.Size*2 || strings.ToLower(input.ArgsSHA256) != input.ArgsSHA256 {
		return ErrInvalid
	}
	decoded, err := hex.DecodeString(input.ArgsSHA256)
	if err != nil || len(decoded) != sha256.Size || input.InvocationID != DeriveToolInvocationIDForApp(input.TenantID, input.AppID, input.EventID, input.ToolCallID, input.ToolName, input.ArgsSHA256) {
		return ErrInvalid
	}
	return nil
}

// ValidateToolInvocation validates a persisted invocation snapshot before it
// crosses back into execution or recovery logic.
func ValidateToolInvocation(value ToolInvocation) error {
	input := ToolInvocationInput{
		TenantID: value.TenantID, AppID: value.AppID, InvocationID: value.InvocationID,
		EventID: value.EventID, RequestID: value.RequestID, TraceID: value.TraceID,
		ToolCallID: value.ToolCallID, ToolName: value.ToolName, ArgsSHA256: value.ArgsSHA256,
		Owner: value.Owner,
	}
	if err := ValidateToolInvocationInput(input); err != nil || !ValidToolInvocationStatus(value.Status) || value.FencingToken < 1 || value.CreatedAt.IsZero() || value.UpdatedAt.IsZero() || value.CreatedAt.Location() != time.UTC || value.UpdatedAt.Location() != time.UTC || value.UpdatedAt.Before(value.CreatedAt) || !validToolInvocationText(value.ErrorClass, 128, false) || !validToolInvocationText(value.ReviewerID, 256, false) {
		return ErrInvalid
	}
	return nil
}

// ValidToolInvocationStatus reports whether a persisted status is known.
func ValidToolInvocationStatus(status ToolInvocationStatus) bool {
	switch status {
	case ToolInvocationPrepared, ToolInvocationDispatching, ToolInvocationAccepted, ToolInvocationSucceeded, ToolInvocationFailed, ToolInvocationDenied, ToolInvocationUnknown, ToolInvocationManual:
		return true
	default:
		return false
	}
}

// ValidateToolInvocationTransitionRequest validates the full fenced
// transition envelope shared by every storage implementation.
func ValidateToolInvocationTransitionRequest(transition ToolInvocationTransition) error {
	if ValidateToolInvocationTenantApp(transition.TenantID, transition.AppID) != nil || !validToolInvocationText(transition.InvocationID, 256, true) || !validToolInvocationText(transition.Owner, 256, true) || !ValidToolInvocationStatus(transition.From) || !ValidToolInvocationStatus(transition.To) || !ValidateToolInvocationTransition(transition.From, transition.To) || transition.FencingToken < 1 {
		return ErrInvalid
	}
	for _, value := range []string{transition.TenantID, transition.AppID, transition.InvocationID, transition.Owner, transition.ErrorClass, transition.ReviewerID} {
		if !utf8.ValidString(value) || value != strings.TrimSpace(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 || strings.Contains(value, "://") {
			return ErrInvalid
		}
	}
	if len(transition.TenantID) > 256 || len(transition.AppID) > 256 || len(transition.InvocationID) > 256 || len(transition.Owner) > 256 || len(transition.ErrorClass) > 128 || len(transition.ReviewerID) > 256 {
		return ErrInvalid
	}
	if transition.To == ToolInvocationManual && strings.TrimSpace(transition.ReviewerID) == "" {
		return ErrInvalid
	}
	if transition.From == ToolInvocationManual && transition.To == ToolInvocationPrepared && !validToolInvocationApprovalMarker(transition.ReviewerID) {
		return ErrInvalid
	}
	return nil
}

func validToolInvocationApprovalMarker(value string) bool {
	if !strings.HasPrefix(value, "approved:") {
		return false
	}
	subject := strings.TrimPrefix(value, "approved:")
	return validToolInvocationText(subject, 256, true)
}

// ToolInvocationStore persists tool-call state for recovery and manual
// reconciliation. Interactive operations require both tenant and app
// predicates; process-wide recovery scans return the app identity on every
// row for downstream scoped handling.
type ToolInvocationStore interface {
	PrepareToolInvocation(context.Context, ToolInvocationInput) (ToolInvocation, error)
	TransitionToolInvocation(context.Context, ToolInvocationTransition) (ToolInvocation, error)
	GetToolInvocation(context.Context, string, string, string) (ToolInvocation, error)
	ListToolInvocations(context.Context, string, string, []ToolInvocationStatus) ([]ToolInvocation, error)
}

// ToolInvocationRecoveryStore atomically fences invocations left in the
// provider hand-off window by a crashed worker. Implementations must select
// only stale dispatching/accepted rows and return their post-transition
// snapshots. Unknown is intentionally a terminal automatic-retry boundary.
type ToolInvocationRecoveryStore interface {
	RecoverStaleToolInvocations(context.Context, time.Time) ([]ToolInvocation, error)
}

// ToolInvocationReconciliationStore lists unknown provider hand-offs so an
// audit/outbox worker can retry the reconciliation fact after its own process
// or database failure. Implementations must return only rows still requiring
// manual reconciliation; terminal/admin-resolved rows are excluded.
type ToolInvocationReconciliationStore interface {
	ListToolInvocationReconciliation(context.Context) ([]ToolInvocation, error)
}

// ToolInvocationAuditStore lists durable states whose corresponding audit
// fact may have been lost after a state commit. A worker can safely emit the
// deterministic event again because the event ID and timestamp are derived
// from the invocation identity/creation time.
type ToolInvocationAuditStore interface {
	ListToolInvocationAuditCandidates(context.Context) ([]ToolInvocation, error)
}

// MessageStore is the durable inbound message lifecycle contract. It owns
// idempotency, execution leases, and fenced message transitions.
type MessageStore interface {
	RecordMessage(context.Context, MessageEventInput) (MessageEvent, bool, error)
	GetMessage(context.Context, string, string) (MessageEvent, error)
	TransitionMessage(context.Context, MessageTransition) (MessageEvent, error)
}

// ReplyStore is the durable reply-segment lifecycle contract. Atomic batch
// materialization and optional correlation/receipt capabilities remain
// separate interfaces because not every legacy store provides them.
type ReplyStore interface {
	EnqueueReply(context.Context, ReplyOutbox) (ReplyOutbox, error)
	ListReplyCandidates(context.Context, string) ([]ReplyOutbox, error)
	GetReply(context.Context, string, string, int) (ReplyOutbox, error)
	ClaimReply(context.Context, string, string, int, string, time.Duration) (ReplyOutbox, error)
	TransitionReply(context.Context, ReplyTransition) (ReplyOutbox, error)
}

// ReplyBatchEnqueuer is the atomic reply-materialization capability. A batch
// either makes every segment durable or makes none of its new segments visible
// to a delivery worker. It remains separate from the segment lifecycle
// capabilities so consumers can keep a narrow dependency surface.
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
	GetReplyCorrelation(context.Context, string, string) (ReplyCorrelation, error)
}

// ReplyReceiptRecorder persists an acknowledged provider receipt while the
// caller still owns the sending lease. It lets a replacement worker reconcile
// a reply after a process restart before the normal sent transition commits.
type ReplyReceiptRecorder interface {
	RecordReplyReceipt(context.Context, ReplyReceipt) (ReplyOutbox, error)
}

// ValidateTenant checks the required tenant identity. It intentionally keeps
// the storage package compatible with legacy non-ULID fixtures; the control
// plane performs canonical tenant-ID validation before opening a runtime.
func ValidateTenant(tenantID string) error {
	if !validToolInvocationText(tenantID, 256, true) {
		return ErrInvalid
	}
	return nil
}

// ValidateSession checks a tenant and session identity pair.
func ValidateSession(tenantID, sessionID string) error {
	if ValidateTenant(tenantID) != nil || !validToolInvocationText(sessionID, 256, true) {
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
	return validToolInvocationText(value, maxReplyTargetIDRunes, true)
}

// NormalizeReplyOutbox validates a reply's protocol-neutral media contract and
// returns a canonical copy. A zero Kind preserves the historical text-only path.
func NormalizeReplyOutbox(value ReplyOutbox) (ReplyOutbox, error) {
	value.Kind = normalizedReplyKind(value.Kind)
	switch value.Kind {
	case ReplyKindText:
		if value.Attachment != (attachment.Reference{}) || value.Fallback != "" {
			return ReplyOutbox{}, ErrInvalid
		}
		return value, nil
	case ReplyKindImage, ReplyKindVideo, ReplyKindAudio, ReplyKindDocument:
		reference, err := value.Attachment.Normalize()
		if err != nil {
			return ReplyOutbox{}, err
		}
		if replyKindForAttachment(reference.Kind) != value.Kind {
			return ReplyOutbox{}, ErrInvalid
		}
		if !ValidateText(value.Fallback, 4096, true) {
			return ReplyOutbox{}, ErrInvalid
		}
		value.Attachment = reference
		return value, nil
	default:
		return ReplyOutbox{}, ErrInvalid
	}
}

func normalizedReplyKind(kind ReplyKind) ReplyKind {
	if kind == "" {
		return ReplyKindText
	}
	return ReplyKind(strings.ToLower(strings.TrimSpace(string(kind))))
}

func replyKindForAttachment(kind attachment.Kind) ReplyKind {
	switch kind {
	case attachment.KindImage:
		return ReplyKindImage
	case attachment.KindVideo:
		return ReplyKindVideo
	case attachment.KindAudio:
		return ReplyKindAudio
	case attachment.KindDocument:
		return ReplyKindDocument
	default:
		return ""
	}
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
func ValidateToolInvocationTransition(from, to ToolInvocationStatus) bool {
	switch from {
	case ToolInvocationPrepared:
		return to == ToolInvocationDispatching || to == ToolInvocationDenied || to == ToolInvocationManual
	case ToolInvocationDispatching:
		return to == ToolInvocationAccepted || to == ToolInvocationSucceeded || to == ToolInvocationFailed || to == ToolInvocationUnknown || to == ToolInvocationManual
	case ToolInvocationAccepted:
		return to == ToolInvocationSucceeded || to == ToolInvocationFailed || to == ToolInvocationUnknown || to == ToolInvocationManual
	case ToolInvocationUnknown:
		return to == ToolInvocationManual || to == ToolInvocationAccepted || to == ToolInvocationSucceeded || to == ToolInvocationFailed
	case ToolInvocationManual:
		return to == ToolInvocationPrepared || to == ToolInvocationAccepted || to == ToolInvocationSucceeded || to == ToolInvocationFailed || to == ToolInvocationDenied
	default:
		return false
	}
}

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

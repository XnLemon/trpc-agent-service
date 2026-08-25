package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// ErrWriteFailed identifies a mandatory producer fact that could not be
// persisted. Callers must surface this error instead of treating the action
// as a successful compliance decision.
var ErrWriteFailed = errors.New("audit write failed")

// Recorder turns protocol/domain outcomes into validated tenant-scoped audit
// events. A nil Writer deliberately makes recording a no-op for deployments
// that have not enabled audit persistence yet.
type Recorder struct {
	Writer   Writer
	TenantID string
	Now      func() time.Time
}

func (r Recorder) Record(ctx context.Context, event Event) error {
	if r.Writer == nil {
		return nil
	}
	if ctx == nil {
		return ErrWriteFailed
	}
	if event.TenantID == "" {
		event.TenantID = strings.TrimSpace(r.TenantID)
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = SchemaVersion
	}
	if event.EventID == "" {
		event.EventID = NewEventID(string(event.EventType), event.RequestID, event.TraceID, event.CorrelationID)
	}
	if event.OccurredAt.IsZero() {
		now := r.Now
		if now == nil {
			now = time.Now
		}
		event.OccurredAt = now().UTC()
	}
	if err := event.Validate(); err != nil {
		return err
	}
	if _, err := r.Writer.Append(ctx, event); err != nil {
		return errors.Join(ErrWriteFailed, err)
	}
	return nil
}

// NewEventID returns a deterministic, non-sensitive identifier suitable for
// idempotent retries. Inputs are length-delimited before hashing.
func NewEventID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte{byte(len(part) >> 24), byte(len(part) >> 16), byte(len(part) >> 8), byte(len(part))})
		h.Write([]byte(part))
	}
	return "audit_" + hex.EncodeToString(h.Sum(nil))[:32]
}

func (r Recorder) ControlPlane(ctx context.Context, eventID, tenantID, actorType, actorID, reason, correlationID string, previous, next int64) error {
	return r.Record(ctx, Event{EventID: eventID, EventType: EventControlPlaneChanged, TenantID: tenantID, ActorType: actorType, ActorID: actorID, Reason: reason, CorrelationID: correlationID, PreviousVersion: &previous, NextVersion: &next})
}

func (r Recorder) ToolDecision(ctx context.Context, eventType EventType, requestID, traceID, toolName string, decision Decision, errorType string) error {
	return r.Record(ctx, Event{EventType: eventType, RequestID: requestID, TraceID: traceID, ToolName: toolName, Decision: decision, ErrorType: errorType})
}

func (r Recorder) BudgetRejected(ctx context.Context, requestID, traceID string) error {
	return r.Record(ctx, Event{EventType: EventBudgetRejected, RequestID: requestID, TraceID: traceID, Decision: DecisionRejected, ErrorType: string(ErrorBudget)})
}

func (r Recorder) Redacted(ctx context.Context, requestID, traceID string) error {
	return r.Record(ctx, Event{EventType: EventContentRedacted, RequestID: requestID, TraceID: traceID, Decision: DecisionAccepted, ErrorType: string(ErrorRedacted)})
}

func (r Recorder) Fallback(ctx context.Context, requestID, traceID string) error {
	return r.Record(ctx, Event{EventType: EventExecutionFallback, RequestID: requestID, TraceID: traceID, Decision: DecisionAccepted})
}

func (r Recorder) IMAuthorization(ctx context.Context, requestID, traceID, userID, sessionID string, allowed bool) error {
	eventType, decision := EventIMAuthorizationDenied, DecisionRejected
	if allowed {
		eventType, decision = EventIMAuthorizationAllowed, DecisionAccepted
	}
	return r.IM(ctx, eventType, requestID, traceID, userID, sessionID, decision, "")
}

func (r Recorder) IMReconciled(ctx context.Context, requestID, traceID, errorType string) error {
	decision := DecisionAccepted
	if errorType != "" {
		decision = DecisionRejected
	}
	return r.IM(ctx, EventIMDeliveryReconciled, requestID, traceID, "", "", decision, errorType)
}

func (r Recorder) IM(ctx context.Context, eventType EventType, requestID, traceID, userID, sessionID string, decision Decision, errorType string) error {
	return r.Record(ctx, Event{EventType: eventType, RequestID: requestID, TraceID: traceID, UserID: userID, SessionID: sessionID, Decision: decision, ErrorType: errorType})
}

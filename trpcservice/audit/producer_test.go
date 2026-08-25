package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type producerWriter struct {
	events []Event
	err    error
}

func (w *producerWriter) Append(_ context.Context, event Event) (AppendResult, error) {
	w.events = append(w.events, event)
	if w.err != nil {
		return AppendResult{}, w.err
	}
	return AppendResult{Event: event}, nil
}

func TestRecorderDerivesBoundedStableIDsAndTenantScope(t *testing.T) {
	w := &producerWriter{}
	now := time.Date(2026, 8, 26, 1, 2, 3, 0, time.UTC)
	r := Recorder{Writer: w, TenantID: "tenant-a", Now: func() time.Time { return now }}
	if err := r.IM(context.Background(), EventIMIngressAccepted, strings.Repeat("r", 256), "trace", "user", "session", DecisionAccepted, ""); err != nil {
		t.Fatal(err)
	}
	if len(w.events) != 1 || w.events[0].TenantID != "tenant-a" || len(w.events[0].EventID) > 256 || w.events[0].OccurredAt != now {
		t.Fatalf("recorded event = %#v", w.events)
	}
	firstID := NewEventID("a", "b")
	if firstID != NewEventID("a", "b") || firstID == NewEventID("ab") {
		t.Fatal("event ID derivation is not stable and length-delimited")
	}
}

func TestRecorderPropagatesWriterFailure(t *testing.T) {
	w := &producerWriter{err: errors.New("storage unavailable")}
	r := Recorder{Writer: w, TenantID: "tenant-a"}
	err := r.BudgetRejected(context.Background(), "request", "trace")
	if !errors.Is(err, ErrWriteFailed) || !strings.Contains(err.Error(), "storage unavailable") {
		t.Fatalf("writer failure = %v", err)
	}
}

func TestRecorderConvenienceProducersAndNoop(t *testing.T) {
	w := &producerWriter{}
	r := Recorder{Writer: w, TenantID: "tenant-a"}
	previous, next := int64(1), int64(2)
	if err := r.ControlPlane(context.Background(), "", "tenant-a", "admin", "actor", "changed", "corr", previous, next); err != nil {
		t.Fatal(err)
	}
	if err := r.ToolDecision(context.Background(), EventToolDenied, "req", "trace", "tool", DecisionDeny, string(ErrorTool)); err != nil {
		t.Fatal(err)
	}
	if err := r.BudgetRejected(context.Background(), "req", "trace"); err != nil {
		t.Fatal(err)
	}
	if err := r.Redacted(context.Background(), "req", "trace"); err != nil {
		t.Fatal(err)
	}
	if err := r.IM(context.Background(), EventIMDeliverySent, "req", "trace", "user", "session", DecisionAccepted, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Fallback(context.Background(), "req", "trace"); err != nil {
		t.Fatal(err)
	}
	if err := r.IMAuthorization(context.Background(), "req", "trace", "user", "session", true); err != nil {
		t.Fatal(err)
	}
	if err := r.IMAuthorization(context.Background(), "req", "trace", "user", "session", false); err != nil {
		t.Fatal(err)
	}
	if err := r.IMReconciled(context.Background(), "req", "trace", ""); err != nil {
		t.Fatal(err)
	}
	if len(w.events) != 9 {
		t.Fatalf("events = %d", len(w.events))
	}
	if err := (Recorder{}).Record(context.Background(), Event{}); err != nil {
		t.Fatal(err)
	}
	if err := r.IMReconciled(context.Background(), "req", "trace", string(ErrorUnavailable)); err != nil {
		t.Fatal(err)
	}
	var nilCtx context.Context
	if err := r.Record(nilCtx, Event{EventType: EventContentRedacted}); !errors.Is(err, ErrWriteFailed) {
		t.Fatalf("nil context err=%v", err)
	}
}

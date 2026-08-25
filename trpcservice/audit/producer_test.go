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
	if NewEventID("a", "b") != NewEventID("a", "b") || NewEventID("a", "b") == NewEventID("ab") {
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

package audit

import (
	"context"
	"errors"
	"testing"
)

func TestInMemoryHandoffReserveFinalizeIdempotencyAndIsolation(t *testing.T) {
	s := NewInMemoryHandoffStore()
	p := ExecutionHandoff{TenantID: "tenant-a", HandoffID: "handoff-1", RequestID: "request", State: HandoffPending}
	got, err := s.Reserve(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != HandoffPending {
		t.Fatal(got.State)
	}
	if _, err := s.Reserve(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reserve(context.Background(), ExecutionHandoff{TenantID: "tenant-a", HandoffID: "handoff-1", RequestID: "other", State: HandoffPending}); !errors.Is(err, ErrHandoffConflict) {
		t.Fatal(err)
	}
	f, err := s.Finalize(context.Background(), ExecutionHandoff{TenantID: "tenant-a", HandoffID: "handoff-1", State: HandoffFinalized, Result: ResultSuccess})
	if err != nil {
		t.Fatal(err)
	}
	if f.RequestID != "request" || f.State != HandoffFinalized {
		t.Fatalf("%+v", f)
	}
	if _, err := s.Get(context.Background(), "tenant-b", "handoff-1"); !errors.Is(err, ErrHandoffNotFound) {
		t.Fatal(err)
	}
}

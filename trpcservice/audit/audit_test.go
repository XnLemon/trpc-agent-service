package audit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testEvent(tenantID, eventID string) Event {
	input, output := int64(4), int64(6)
	return Event{SchemaVersion: SchemaVersion, EventID: eventID, EventType: EventExecutionCompleted, TenantID: tenantID, AgentAppID: "app-1", Channel: "api", Cost: &Usage{InputTokens: &input, OutputTokens: &output, Currency: "USD", Provider: "provider", Model: "model", ExecutionResult: ResultSuccess}, OccurredAt: time.Now().UTC()}
}

func TestInMemoryTenantScopeAndDuplicateDigest(t *testing.T) {
	backend := &Backend{}
	one, err := NewInMemoryWithBackend("tenant-a", backend)
	if err != nil {
		t.Fatal(err)
	}
	two, err := NewInMemoryWithBackend("tenant-b", backend)
	if err != nil {
		t.Fatal(err)
	}
	event := testEvent("tenant-a", "event-1")
	first, err := one.Append(context.Background(), event)
	if err != nil || first.Duplicate {
		t.Fatalf("first append = %#v, %v", first, err)
	}
	duplicate, err := one.Append(context.Background(), event)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate append = %#v, %v", duplicate, err)
	}
	event.Cost.InputTokens = ptr(99)
	got, err := one.Get(context.Background(), "event-1")
	if err != nil {
		t.Fatal(err)
	}
	if *got.Cost.InputTokens != 4 {
		t.Fatalf("stored event was aliased: %d", *got.Cost.InputTokens)
	}
	if _, err := two.Append(context.Background(), event); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("cross-tenant append error = %v", err)
	}
	if _, err := two.Get(context.Background(), "event-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get error = %v", err)
	}
	event = testEvent("tenant-a", "event-1")
	event.Reason = "changed"
	if _, err := one.Append(context.Background(), event); !errors.Is(err, ErrConflict) {
		t.Fatalf("digest conflict = %v", err)
	}
}

func TestInMemoryConcurrentAppendAndAggregation(t *testing.T) {
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	errorsOut := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			event := testEvent("tenant-a", fmt.Sprintf("event-%02d", i))
			_, err := store.Append(context.Background(), event)
			if err != nil {
				errorsOut <- err
			}
		}(i)
	}
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
	events, err := store.List(context.Background(), Query{})
	if err != nil || len(events) != workers {
		t.Fatalf("list = %d, %v", len(events), err)
	}
	totals, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{GroupApp, GroupChannel, GroupProvider, GroupModel}})
	if err != nil {
		t.Fatal(err)
	}
	if len(totals) != 1 || totals[0].InputTokens != workers*4 || totals[0].OutputTokens != workers*6 {
		t.Fatalf("totals = %#v", totals)
	}
}

func TestEventValidationAndCancellation(t *testing.T) {
	event := testEvent("tenant-a", "event-1")
	event.SchemaVersion = 2
	if !errors.Is(event.Validate(), ErrInvalid) {
		t.Fatal("unknown schema version accepted")
	}
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Append(ctx, testEvent("tenant-a", "event-1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled append = %v", err)
	}
}

func ptr(value int64) *int64 { return &value }

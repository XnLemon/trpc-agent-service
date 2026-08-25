package audit

import (
	"context"
	"errors"
	"fmt"
	"math"
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

func TestEventRejectsSensitiveValuesAndUnknownErrorTypes(t *testing.T) {
	for _, value := range []string{"Authorization: Bearer secret", "API key abc", "token abc", "dsn=postgres://user:pass@db", "provider error: secret"} {
		event := testEvent("tenant-a", "event-1")
		event.ErrorType = value
		if !errors.Is(event.Validate(), ErrInvalid) {
			t.Fatalf("sensitive error type accepted: %q", value)
		}
	}
	event := testEvent("tenant-a", "event-1")
	event.Reason = "password=secret"
	if !errors.Is(event.Validate(), ErrInvalid) {
		t.Fatal("sensitive reason accepted")
	}
	event = testEvent("tenant-a", "event-1")
	event.ErrorType = "made_up"
	if !errors.Is(event.Validate(), ErrInvalid) {
		t.Fatal("unknown error type accepted")
	}
	usage := Usage{BudgetUsedMinor: ptr(1), Currency: "ZZZ"}
	if !errors.Is(usage.Validate(), ErrInvalid) {
		t.Fatal("unknown currency accepted")
	}
	for _, currency := range []string{"PLN", "BRL", "RUB", "TRY", "THB", "TWD"} {
		if err := (Usage{ModelCostMinor: ptr(1), Currency: currency}).Validate(); err != nil {
			t.Fatalf("valid ISO currency %s rejected: %v", currency, err)
		}
	}
}

func TestAggregationSeparatesCurrenciesAndIDsCannotCollide(t *testing.T) {
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	usd := testEvent("tenant-a", "event-a")
	eur := testEvent("tenant-a", "event-b")
	eur.Cost.Currency = "EUR"
	if _, err := store.Append(context.Background(), usd); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), eur); err != nil {
		t.Fatal(err)
	}
	totals, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{GroupApp}})
	if err != nil || len(totals) != 2 {
		t.Fatalf("currency totals = %#v, %v", totals, err)
	}
	backend := &Backend{}
	one, err := NewInMemoryWithBackend("tenant", backend)
	if err != nil {
		t.Fatal(err)
	}
	two, err := NewInMemoryWithBackend("tenant\\x00event", backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := one.Append(context.Background(), testEvent("tenant", "event\\x00b")); err != nil {
		t.Fatal(err)
	}
	if _, err := two.Append(context.Background(), testEvent("tenant\\x00event", "b")); err != nil {
		t.Fatal(err)
	}
}

func TestAuditValidationAndQueryBoundaries(t *testing.T) {
	base := testEvent("tenant-a", "event-1")
	for _, mutate := range []func(*Event){
		func(e *Event) { e.Decision = DecisionAllow },
		func(e *Event) { e.Decision = Decision("unknown") },
		func(e *Event) { e.EventType = EventType("unknown") },
		func(e *Event) { e.EventID = " event-1" },
		func(e *Event) { e.OccurredAt = time.Time{} },
		func(e *Event) { e.OccurredAt = time.Now() },
		func(e *Event) { e.PreviousVersion = ptr(2) },
		func(e *Event) { e.PreviousVersion, e.NextVersion = ptr(2), ptr(2) },
		func(e *Event) { e.PreviousVersion, e.NextVersion = ptr(math.MaxInt64), ptr(math.MinInt64) },
		func(e *Event) { e.LatencyMS = ptr(-1) },
		func(e *Event) { e.Revision = ptr(-1) },
	} {
		event := base
		mutate(&event)
		if event.Decision == DecisionAllow {
			if err := event.Validate(); err != nil {
				t.Fatalf("valid decision rejected: %v", err)
			}
		} else if !errors.Is(event.Validate(), ErrInvalid) {
			t.Fatalf("invalid event accepted: %#v", event)
		}
	}
	for _, usage := range []Usage{
		{InputTokens: ptr(-1)}, {ModelCostMinor: ptr(1)}, {ToolCostMinor: ptr(1), Currency: "usd"},
		{ExecutionResult: ExecutionResult("unknown")}, {Provider: "https://provider"}, {BudgetUsedMinor: ptr(1)},
	} {
		if usage.Validate() == nil {
			t.Fatalf("invalid usage accepted: %#v", usage)
		}
	}
	if _, err := NewInMemory(""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty tenant error = %v", err)
	}
	if _, err := NewInMemoryWithBackend("tenant", nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil backend error = %v", err)
	}
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if _, err := store.Append(nilContext, base); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context append = %v", err)
	}
	if _, err := store.Get(context.Background(), " "); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("empty get = %v", err)
	}
	if _, err := store.List(context.Background(), Query{EventTypes: []EventType{"unknown"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid query type = %v", err)
	}
	if _, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{"unknown"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid group = %v", err)
	}
	if _, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{GroupTenant}}); err != nil {
		t.Fatal(err)
	}
}

func ptr(value int64) *int64 { return &value }

package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

func TestStoreReserveIsAtomicAcrossConcurrentRequests(t *testing.T) {
	store := New()
	limit := int64(100)
	period := time.Date(2026, 9, 7, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	input := func(id string) budget.ReserveInput {
		return budget.ReserveInput{
			TenantID: "tenant", ReservationID: id, PeriodStart: period,
			Limits: budget.Limits{TokenBudget: &limit}, Estimate: budget.Estimate{InputTokens: 60},
		}
	}

	const attempts = 8
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, err := store.Reserve(context.Background(), input(fmt.Sprintf("request-%d", index)))
			results <- err
		}(index)
	}
	wait.Wait()
	close(results)

	var admitted int
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, budget.ErrExceeded):
		default:
			t.Fatalf("concurrent reserve error = %v", err)
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted reservations = %d, want 1", admitted)
	}
	snapshot, err := store.Snapshot(context.Background(), "tenant", period)
	if err != nil || snapshot.ReservedTokens != 60 {
		t.Fatalf("atomic ledger = %+v, err = %v", snapshot, err)
	}
}

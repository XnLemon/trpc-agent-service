package loadtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

func TestTenantAdmissionCapacityIsBounded(t *testing.T) {
	const (
		tenant        = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		concurrent    = 8
		attempts      = 32
		requestBudget = 60
	)
	limiter, err := gateway.NewTenantLimiter(gateway.TenantLimiterConfig{
		MaxConcurrent: concurrent,
		MaxRequests:   requestBudget,
		Window:        time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer limiter.Close()

	barrier := make(chan struct{})
	results := make(chan *gateway.TenantLimitLease, attempts)
	var waitGroup sync.WaitGroup
	var active, maximum atomic.Int32
	waitGroup.Add(attempts)
	for index := 0; index < attempts; index++ {
		go func() {
			defer waitGroup.Done()
			<-barrier
			lease, err := limiter.Acquire(context.Background(), tenant)
			if err != nil {
				results <- nil
				return
			}
			current := active.Add(1)
			for {
				old := maximum.Load()
				if current <= old || maximum.CompareAndSwap(old, current) {
					break
				}
			}
			results <- lease
		}()
	}
	close(barrier)

	leases := make([]*gateway.TenantLimitLease, 0, concurrent)
	for index := 0; index < attempts; index++ {
		lease := <-results
		if lease != nil {
			leases = append(leases, lease)
		}
	}
	if len(leases) != concurrent || int(maximum.Load()) != concurrent {
		t.Fatalf("accepted=%d maximum=%d, want exactly %d admitted calls", len(leases), maximum.Load(), concurrent)
	}
	for _, lease := range leases {
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
		active.Add(-1)
	}
	waitGroup.Wait()
	if active.Load() != 0 {
		t.Fatalf("active leases after release = %d, want zero", active.Load())
	}

	for index := 0; index < requestBudget-concurrent; index++ {
		lease, err := limiter.Acquire(context.Background(), tenant)
		if err != nil {
			t.Fatalf("request %d after capacity test: %v", index, err)
		}
		_ = lease.Release()
	}
	if _, err := limiter.Acquire(context.Background(), tenant); err != gateway.ErrRateLimited {
		t.Fatalf("request budget error = %v, want ErrRateLimited", err)
	}
}

func BenchmarkTenantAdmission(b *testing.B) {
	limiter, err := gateway.NewTenantLimiter(gateway.TenantLimiterConfig{MaxConcurrent: b.N + 1, MaxRequests: b.N + 1, Window: time.Minute})
	if err != nil {
		b.Fatal(err)
	}
	defer limiter.Close()
	const tenant = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := limiter.Acquire(context.Background(), tenant)
		if err != nil {
			b.Fatal(err)
		}
		_ = lease.Release()
	}
}

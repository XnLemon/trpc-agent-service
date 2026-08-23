package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTenantLimiterAdmissionWindowAndRelease(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock := now
	limiter, err := NewTenantLimiter(TenantLimiterConfig{MaxConcurrent: 1, MaxRequests: 2, Window: time.Minute, Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newGatewayFixture(t)
	tenantID := fixture.tenant.TenantID
	first, err := limiter.Acquire(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Acquire(context.Background(), tenantID); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("concurrent limit error = %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := limiter.Acquire(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Acquire(context.Background(), tenantID); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("window request limit error = %v", err)
	}
	clock = clock.Add(time.Minute)
	third, err := limiter.Acquire(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Release(); err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := limiter.Acquire(canceled, tenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire error = %v", err)
	}
	if _, err := limiter.Acquire(context.Background(), "bad-tenant"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant error = %v", err)
	}
	if !limiter.Ready() {
		t.Fatal("configured limiter is not ready")
	}
	if err := limiter.Close(); err != nil {
		t.Fatal(err)
	}
	if limiter.Ready() {
		t.Fatal("closed limiter is ready")
	}
	if _, err := limiter.Acquire(context.Background(), tenantID); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed limiter error = %v", err)
	}
}

func TestTenantLimiterConcurrentAdmissionAndConfigurationEdges(t *testing.T) {
	if _, err := NewTenantLimiter(TenantLimiterConfig{MaxConcurrent: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative concurrent limit error = %v", err)
	}
	if _, err := NewTenantLimiter(TenantLimiterConfig{MaxRequests: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative request limit error = %v", err)
	}
	if _, err := NewTenantLimiter(TenantLimiterConfig{Window: -time.Second}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative window error = %v", err)
	}
	var nilLimiter *TenantLimiter
	if nilLimiter.Ready() {
		t.Fatal("nil limiter is ready")
	}
	var nilContext context.Context
	if _, err := nilLimiter.Acquire(nilContext, "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil limiter acquire error = %v", err)
	}

	fixture := newGatewayFixture(t)
	limiter, err := NewTenantLimiter(TenantLimiterConfig{MaxConcurrent: 1, MaxRequests: 1, Window: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			lease, err := limiter.Acquire(context.Background(), fixture.tenant.TenantID)
			if err == nil {
				defer func() { _ = lease.Release() }()
			}
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrRateLimited) {
			t.Fatalf("concurrent acquire error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent limiter admitted %d workers", successes)
	}
	var nilLease *TenantLimitLease
	if err := nilLease.Release(); err != nil {
		t.Fatal(err)
	}
}

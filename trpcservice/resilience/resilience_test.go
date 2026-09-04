package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPolicyRetriesWithPerAttemptTimeout(t *testing.T) {
	policy, err := New(Config{
		Timeout: 20 * time.Millisecond, MaxAttempts: 3, Backoff: 0,
		MaxBackoff: 0, FailureThreshold: 3, OpenTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	err = policy.Execute(context.Background(), func(ctx context.Context) error {
		if calls.Add(1) < 3 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	if err != nil || calls.Load() != 3 {
		t.Fatalf("Execute() err=%v calls=%d, want success after three attempts", err, calls.Load())
	}
}

func TestPolicyDoesNotRetryPermanentErrorAndUsesFallback(t *testing.T) {
	permanent := errors.New("permanent")
	var calls, fallbacks atomic.Int32
	policy, err := New(Config{
		Timeout: time.Second, MaxAttempts: 4, FailureThreshold: 2, OpenTimeout: time.Second,
		Fallback:  func(context.Context, error) error { fallbacks.Add(1); return nil },
		Retryable: func(error) bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Execute(context.Background(), func(context.Context) error { calls.Add(1); return permanent }); err != nil {
		t.Fatalf("Execute() = %v, want fallback success", err)
	}
	if calls.Load() != 1 || fallbacks.Load() != 1 {
		t.Fatalf("calls=%d fallbacks=%d, want one each", calls.Load(), fallbacks.Load())
	}
}

func TestPolicyOpensAndAllowsOneHalfOpenProbe(t *testing.T) {
	dependency := errors.New("unavailable")
	policy, err := New(Config{
		Timeout: time.Second, MaxAttempts: 1, FailureThreshold: 1, OpenTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Execute(context.Background(), func(context.Context) error { return dependency }); !errors.Is(err, dependency) {
		t.Fatalf("first error = %v, want dependency error", err)
	}
	if policy.State() != StateOpen {
		t.Fatalf("state = %q, want open", policy.State())
	}
	if err := policy.Execute(context.Background(), func(context.Context) error { t.Fatal("open circuit called operation"); return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("open error = %v, want ErrCircuitOpen", err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := policy.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("half-open probe = %v, want success", err)
	}
	if policy.State() != StateClosed {
		t.Fatalf("state after probe = %q, want closed", policy.State())
	}
}

func TestPolicyCancellationSkipsFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var fallback atomic.Int32
	policy, err := New(Config{
		Timeout: time.Second, MaxAttempts: 2, FailureThreshold: 1, OpenTimeout: time.Second,
		Fallback: func(context.Context, error) error { fallback.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Execute(ctx, func(context.Context) error { return errors.New("must not run") }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() = %v, want cancellation", err)
	}
	if fallback.Load() != 0 {
		t.Fatalf("fallback calls = %d, want zero", fallback.Load())
	}
}

func TestPolicyZeroValueIsInvalid(t *testing.T) {
	var policy Policy
	if err := policy.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Validate() = %v, want ErrInvalid", err)
	}
	if err := policy.Execute(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Execute() = %v, want ErrInvalid", err)
	}
}

func TestPolicyReturnsFallbackErrorAfterRetryableAttempts(t *testing.T) {
	dependency := errors.New("unavailable")
	fallbackErr := errors.New("fallback failed")
	var calls int
	policy, err := New(Config{
		Timeout: time.Second, MaxAttempts: 2, FailureThreshold: 2, OpenTimeout: time.Second,
		Retryable: func(error) bool { return true },
		Fallback:  func(context.Context, error) error { return fallbackErr },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Execute(context.Background(), func(context.Context) error { calls++; return dependency }); !errors.Is(err, fallbackErr) {
		t.Fatalf("Execute() = %v, want fallback error", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want two bounded attempts", calls)
	}
}

func TestPolicyStopsRetryDuringBackoffCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy, err := New(Config{
		Timeout: time.Second, MaxAttempts: 2, Backoff: time.Second, MaxBackoff: time.Second,
		FailureThreshold: 2, OpenTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	if err := policy.Execute(ctx, func(context.Context) error { calls++; return errors.New("retryable") }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() = %v, want cancellation", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want cancellation before second attempt", calls)
	}
}

func TestCircuitBreakerCancellationAndInvalidState(t *testing.T) {
	breaker := &CircuitBreaker{state: StateHalfOpen, failureThreshold: 1, openTimeout: time.Second}
	breaker.cancel()
	if state := breaker.State(); state != StateOpen {
		t.Fatalf("state after canceled probe = %q, want open", state)
	}
	invalid := &CircuitBreaker{state: State("invalid"), failureThreshold: 1, openTimeout: time.Second}
	if err := invalid.before(time.Now()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid state error = %v, want ErrInvalid", err)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	cases := []Config{
		{MaxAttempts: 1, FailureThreshold: 1, OpenTimeout: time.Second},
		{Timeout: time.Second, MaxAttempts: 0, FailureThreshold: 1, OpenTimeout: time.Second},
		{Timeout: time.Second, MaxAttempts: 1, Backoff: time.Second, MaxBackoff: time.Millisecond, FailureThreshold: 1, OpenTimeout: time.Second},
		{Timeout: time.Second, MaxAttempts: 1, FailureThreshold: 0, OpenTimeout: time.Second},
		{Timeout: time.Second, MaxAttempts: 1, FailureThreshold: 1},
	}
	for _, config := range cases {
		if _, err := New(config); !errors.Is(err, ErrInvalid) {
			t.Fatalf("New(%+v) err=%v, want ErrInvalid", config, err)
		}
	}
}

// Package resilience provides bounded, provider-neutral execution policies.
package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrInvalid reports an invalid policy configuration or operation.
	ErrInvalid = errors.New("invalid resilience policy")
	// ErrCircuitOpen reports that the dependency is temporarily unavailable.
	ErrCircuitOpen = errors.New("resilience circuit is open")
)

// State is the observable state of a circuit breaker.
type State string

const (
	// StateClosed admits calls and records consecutive failures.
	StateClosed State = "closed"
	// StateOpen rejects calls until OpenTimeout elapses.
	StateOpen State = "open"
	// StateHalfOpen admits one probe call after an open interval.
	StateHalfOpen State = "half_open"
)

// Config controls one bounded execution policy. Retryable errors are retried
// up to MaxAttempts; a fallback is called once after a terminal dependency
// error. Context cancellation never invokes the fallback.
type Config struct {
	Timeout          time.Duration
	MaxAttempts      int
	Backoff          time.Duration
	MaxBackoff       time.Duration
	FailureThreshold int
	OpenTimeout      time.Duration
	Retryable        func(error) bool
	Fallback         func(context.Context, error) error
}

// DefaultConfig is a conservative policy for short provider operations.
var DefaultConfig = Config{
	Timeout:          5 * time.Second,
	MaxAttempts:      3,
	Backoff:          50 * time.Millisecond,
	MaxBackoff:       time.Second,
	FailureThreshold: 3,
	OpenTimeout:      5 * time.Second,
}

// Policy executes operations with a timeout, bounded retries, and a circuit
// breaker. A Policy is safe for concurrent use and should be shared by calls
// to the same dependency.
type Policy struct {
	timeout          time.Duration
	maxAttempts      int
	backoff          time.Duration
	maxBackoff       time.Duration
	failureThreshold int
	openTimeout      time.Duration
	retryable        func(error) bool
	fallback         func(context.Context, error) error
	breaker          *CircuitBreaker
}

// New validates and constructs a concurrent execution policy.
func New(config Config) (*Policy, error) {
	if config.Timeout <= 0 || config.MaxAttempts < 1 || config.Backoff < 0 || config.MaxBackoff < 0 || config.FailureThreshold < 1 || config.OpenTimeout <= 0 {
		return nil, ErrInvalid
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = config.Backoff
	}
	if config.MaxBackoff < config.Backoff {
		return nil, ErrInvalid
	}
	if config.Retryable == nil {
		config.Retryable = defaultRetryable
	}
	return &Policy{
		timeout: config.Timeout, maxAttempts: config.MaxAttempts,
		backoff: config.Backoff, maxBackoff: config.MaxBackoff,
		failureThreshold: config.FailureThreshold, openTimeout: config.OpenTimeout,
		retryable: config.Retryable, fallback: config.Fallback,
		breaker: &CircuitBreaker{failureThreshold: config.FailureThreshold, openTimeout: config.OpenTimeout},
	}, nil
}

// NewDefault constructs the default bounded policy. The defaults are fixed
// and do not depend on environment or wall-clock configuration.
func NewDefault() *Policy {
	policy, err := New(DefaultConfig)
	if err != nil {
		panic(err)
	}
	return policy
}

// State reports the current breaker state.
func (policy *Policy) State() State {
	if policy == nil || policy.breaker == nil {
		return StateClosed
	}
	return policy.breaker.State()
}

// Validate reports whether the policy was fully initialized by New.
func (policy *Policy) Validate() error {
	if policy == nil || policy.breaker == nil || policy.timeout <= 0 || policy.maxAttempts < 1 || policy.backoff < 0 || policy.maxBackoff < policy.backoff || policy.failureThreshold < 1 || policy.openTimeout <= 0 || policy.retryable == nil {
		return ErrInvalid
	}
	if policy.breaker.failureThreshold < 1 || policy.breaker.openTimeout <= 0 {
		return ErrInvalid
	}
	return nil
}

// Execute runs operation under the policy. The operation receives a fresh
// context bounded by Timeout for each attempt. A nil operation is invalid.
func (policy *Policy) Execute(ctx context.Context, operation func(context.Context) error) error {
	if policy == nil || ctx == nil || operation == nil {
		return ErrInvalid
	}
	if err := policy.Validate(); err != nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := policy.breaker.before(time.Now()); err != nil {
		return policy.terminal(ctx, err)
	}
	for attempt := 1; attempt <= policy.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			policy.breaker.cancel()
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, policy.timeout)
		err := operation(attemptCtx)
		cancel()
		if err == nil {
			policy.breaker.success()
			return nil
		}
		if ctx.Err() != nil {
			policy.breaker.cancel()
			return ctx.Err()
		}
		if !policy.retryable(err) || attempt == policy.maxAttempts {
			policy.breaker.failure()
			return policy.terminal(ctx, err)
		}
		if err := wait(ctx, retryDelay(policy.backoff, policy.maxBackoff, attempt)); err != nil {
			policy.breaker.cancel()
			return err
		}
	}
	return ErrInvalid
}

func (policy *Policy) terminal(ctx context.Context, err error) error {
	if err == nil || policy.fallback == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return err
	}
	if fallbackErr := policy.fallback(ctx, err); fallbackErr != nil {
		return fallbackErr
	}
	return nil
}

func defaultRetryable(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled)
}

func retryDelay(base, maximum time.Duration, attempt int) time.Duration {
	if base <= 0 || maximum <= 0 {
		return 0
	}
	delay := base
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// CircuitBreaker is a small concurrency-safe closed/open/half-open state
// machine. It is exposed for health reporting; callers normally use Policy.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            State
	failures         int
	openedAt         time.Time
	halfOpenInFlight bool
	failureThreshold int
	openTimeout      time.Duration
}

// State reports the current state of the breaker.
func (breaker *CircuitBreaker) State() State {
	if breaker == nil {
		return StateClosed
	}
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	return breaker.stateOrClosed()
}

func (breaker *CircuitBreaker) stateOrClosed() State {
	if breaker.state == "" {
		return StateClosed
	}
	return breaker.state
}

func (breaker *CircuitBreaker) before(now time.Time) error {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	switch breaker.stateOrClosed() {
	case StateClosed:
		return nil
	case StateOpen:
		if now.Sub(breaker.openedAt) < breaker.openTimeout {
			return ErrCircuitOpen
		}
		breaker.state = StateHalfOpen
		breaker.halfOpenInFlight = true
		return nil
	case StateHalfOpen:
		return ErrCircuitOpen
	default:
		return fmt.Errorf("%w: unknown circuit state", ErrInvalid)
	}
}

func (breaker *CircuitBreaker) success() {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	breaker.state = StateClosed
	breaker.failures = 0
	breaker.openedAt = time.Time{}
	breaker.halfOpenInFlight = false
}

func (breaker *CircuitBreaker) failure() {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.stateOrClosed() == StateHalfOpen || breaker.failures+1 >= breaker.failureThreshold {
		breaker.state = StateOpen
		breaker.openedAt = time.Now()
		breaker.failures = 0
		breaker.halfOpenInFlight = false
		return
	}
	breaker.failures++
}

func (breaker *CircuitBreaker) cancel() {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.stateOrClosed() == StateHalfOpen {
		breaker.state = StateOpen
		breaker.openedAt = time.Now()
		breaker.halfOpenInFlight = false
	}
}

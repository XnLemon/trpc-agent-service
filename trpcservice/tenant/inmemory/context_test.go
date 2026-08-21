package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestLockHonorsCancellationWhileWaiting(t *testing.T) {
	r := NewRepository()
	if err := r.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.mu.Unlock()
		r.unlock()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.lock(ctx)
	}()
	select {
	case <-time.After(25 * time.Millisecond):
		cancel()
	case err := <-done:
		t.Fatalf("lock unexpectedly acquired before cancellation: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellable lock did not return after cancellation")
	}
}

func TestInternalContextAndCloneBranches(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	gate := make(chan struct{}, 1)
	if err := acquire(cancelled, gate); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled acquire, got %v", err)
	}

	r := NewRepository()
	if err := r.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.mu.Unlock()
		r.unlock()
	}()
	if err := r.rLock(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled read lock, got %v", err)
	}

	value := int64(1)
	text := "value"
	if cloneInt64(&value) == nil || cloneString(&text) == nil {
		t.Fatal("non-nil clone helpers must return copies")
	}
	if cloneTenant(nil) != nil {
		t.Fatal("nil tenant clone must remain nil")
	}
	var _ tenant.Repository = r
}

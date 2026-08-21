package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"
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

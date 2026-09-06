package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestTenantRuntimeRegistryMaterializesOneTenantOnceConcurrently(t *testing.T) {
	var calls atomic.Int32
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	var group sync.WaitGroup
	errorsOut := make(chan error, workers)
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsOut <- registry.Ensure(context.Background(), "tenant-a")
		}()
	}
	group.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("materializer calls = %d, want 1", got)
	}
}

func TestTenantRuntimeRegistryRetriesFailureAndInvalidatesPublishedState(t *testing.T) {
	var calls atomic.Int32
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		if calls.Add(1) == 1 {
			return errors.New("temporary provider failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), "tenant-a"); err == nil {
		t.Fatal("first materialization unexpectedly succeeded")
	}
	if err := registry.Ensure(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	registry.InvalidateTenant("tenant-a")
	if err := registry.Ensure(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("materializer calls = %d, want 3", got)
	}
}

func TestTenantRuntimeRegistryRejectsInvalidAndClosedUse(t *testing.T) {
	if _, err := NewTenantRuntimeRegistry(nil); !errors.Is(err, ErrInvalidTenantRuntime) {
		t.Fatalf("nil materializer error = %v", err)
	}
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), ""); !errors.Is(err, ErrInvalidTenantRuntime) {
		t.Fatalf("empty tenant error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), "tenant-a"); !errors.Is(err, ErrTenantRuntimeClosed) {
		t.Fatalf("closed registry error = %v", err)
	}
}

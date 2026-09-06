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
	var nilRegistry *TenantRuntimeRegistry
	if err := nilRegistry.Ensure(context.Background(), "tenant-a"); !errors.Is(err, ErrInvalidTenantRuntime) {
		t.Fatalf("nil registry error = %v", err)
	}
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
	if err := registry.Ensure(context.Background(), " \t"); !errors.Is(err, ErrInvalidTenantRuntime) {
		t.Fatalf("blank tenant error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.Ensure(canceled, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled registry error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	registry.InvalidateTenant("")
	registry.InvalidateTenant("unknown")
	if err := registry.Ensure(context.Background(), "tenant-a"); !errors.Is(err, ErrTenantRuntimeClosed) {
		t.Fatalf("closed registry error = %v", err)
	}
	registry.InvalidateTenant("tenant-a")
}

func TestTenantRuntimeRegistryWaiterHonorsCancellationAndSharesFailure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	failure := errors.New("materialization failed")
	var calls atomic.Int32
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return failure
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerResult := make(chan error, 1)
	go func() { ownerResult <- registry.Ensure(context.Background(), "tenant-a") }()
	<-started
	canceled, cancel := context.WithCancel(context.Background())
	waiterResult := make(chan error, 1)
	go func() { waiterResult <- registry.Ensure(canceled, "tenant-a") }()
	cancel()
	if err := <-waiterResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v", err)
	}
	close(release)
	if err := <-ownerResult; !errors.Is(err, failure) {
		t.Fatalf("owner error = %v", err)
	}
	if err := registry.Ensure(context.Background(), "tenant-a"); !errors.Is(err, failure) {
		t.Fatalf("failure waiter/retry error = %v", err)
	}
}

func TestTenantRuntimeRegistryInvalidationRacesWithMaterialization(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.Ensure(context.Background(), "tenant-a") }()
	<-started
	registry.InvalidateTenant("tenant-a")
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("materializer calls after in-flight invalidation = %d, want 2", got)
	}
}

func TestTenantRuntimeRegistryCloseReleasesInFlightCall(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.Ensure(context.Background(), "tenant-a") }()
	<-started
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrTenantRuntimeClosed) {
		t.Fatalf("in-flight close error = %v", err)
	}
}

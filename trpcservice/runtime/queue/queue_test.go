package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryQueueIdempotencyAndTenantIsolation(t *testing.T) {
	store := NewMemory()
	defer store.Close()
	first, duplicate, err := store.Enqueue(context.Background(), TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte(`{"v":1}`)})
	if err != nil || duplicate || first.Status != StatusQueued {
		t.Fatalf("first enqueue = %+v duplicate=%v err=%v", first, duplicate, err)
	}
	replayed, duplicate, err := store.Enqueue(context.Background(), TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte(`{"v":1}`)})
	if err != nil || !duplicate || replayed.TaskID != first.TaskID {
		t.Fatalf("replay = %+v duplicate=%v err=%v", replayed, duplicate, err)
	}
	if _, _, err := store.Enqueue(context.Background(), TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte(`{"v":2}`)}); !errors.Is(err, ErrConflict) {
		t.Fatalf("payload conflict = %v", err)
	}
	if _, err := store.Get(context.Background(), "tenant-b", "task-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get = %v", err)
	}
}

func TestMemoryQueueFencingRejectsStaleWorker(t *testing.T) {
	store := NewMemory()
	defer store.Close()
	_, _, _ = store.Enqueue(context.Background(), TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte("payload")})
	first, err := store.Claim(context.Background(), "tenant-a", "worker-a", 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	second, err := store.Claim(context.Background(), "tenant-a", "worker-b", time.Second)
	if err != nil || second.FencingToken <= first.FencingToken {
		t.Fatalf("second claim = %+v err=%v", second, err)
	}
	if _, err := store.Complete(context.Background(), "tenant-a", "task-1", "worker-a", first.FencingToken); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale completion = %v", err)
	}
	if _, err := store.Complete(context.Background(), "tenant-a", "task-1", "worker-b", second.FencingToken); err != nil {
		t.Fatalf("current completion = %v", err)
	}
}

func TestMemoryQueueConcurrentClaimHasOneWinner(t *testing.T) {
	store := NewMemory()
	defer store.Close()
	_, _, _ = store.Enqueue(context.Background(), TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte("payload")})
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := store.Claim(context.Background(), "tenant-a", FormatTaskKey("worker", string(rune('a'+i))), time.Second); err == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("claim winners = %d", winners)
	}
}

func TestWorkerRetriesThenCompletesAndCloses(t *testing.T) {
	store := NewMemory()
	defer store.Close()
	_, _, _ = store.Enqueue(context.Background(), TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte("payload")})
	var calls int
	worker, err := New(Config{Store: store, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond, MaxAttempts: 2, Handler: func(context.Context, Task) error {
		calls++
		if calls == 1 {
			return Retry(errors.New("temporary"))
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := worker.RunOnce(context.Background()); !ok || err != nil {
		t.Fatalf("first run = %v %v", ok, err)
	}
	time.Sleep(2 * time.Millisecond)
	if ok, err := worker.RunOnce(context.Background()); !ok || err != nil {
		t.Fatalf("second run = %v %v", ok, err)
	}
	value, err := store.Get(context.Background(), "tenant-a", "task-1")
	if err != nil || value.Status != StatusCompleted || value.Attempts != 2 {
		t.Fatalf("task = %+v err=%v", value, err)
	}
	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerCancellationLeavesRecoverableLease(t *testing.T) {
	store := NewMemory()
	defer store.Close()
	_, _, _ = store.Enqueue(context.Background(), TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte("payload")})
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	worker, err := New(Config{Store: store, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: 20 * time.Millisecond, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond, Handler: func(ctx context.Context, _ Task) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if _, err := store.Claim(context.Background(), "tenant-a", "worker-b", time.Second); err != nil {
		t.Fatalf("reclaim after cancellation = %v", err)
	}
}

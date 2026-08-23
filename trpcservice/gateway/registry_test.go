package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type testRunner struct {
	runFn      func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error)
	closeErr   error
	closeCount atomic.Int32
}

func (runner *testRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	if runner.runFn != nil {
		return runner.runFn(ctx, userID, sessionID, message, options...)
	}
	events := make(chan *trpcevent.Event)
	close(events)
	return events, nil
}

func (runner *testRunner) Close() error {
	runner.closeCount.Add(1)
	return runner.closeErr
}

func testExecutionPlan(t *testing.T) runtime.ExecutionPlan {
	t.Helper()
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID))
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRunnerRegistryMergesConcurrentConstructionAndReusesCompleteKey(t *testing.T) {
	plan := testExecutionPlan(t)
	var calls atomic.Int32
	var started sync.Once
	startedCh := make(chan struct{})
	release := make(chan struct{})
	runnerValue := &testRunner{}
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(ctx context.Context, got runtime.ExecutionPlan) (Runner, error) {
			calls.Add(1)
			started.Do(func() { close(startedCh) })
			select {
			case <-release:
				return runnerValue, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		MaxEntries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()

	const workers = 12
	leases := make(chan *RunnerLease, workers)
	errorsCh := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			lease, acquireErr := registry.Acquire(context.Background(), plan)
			if acquireErr != nil {
				errorsCh <- acquireErr
				return
			}
			leases <- lease
		}()
	}
	<-startedCh
	close(release)
	wait.Wait()
	close(leases)
	close(errorsCh)
	for acquireErr := range errorsCh {
		t.Fatal(acquireErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
	var first *RunnerLease
	allLeases := make([]*RunnerLease, 0, workers)
	for lease := range leases {
		allLeases = append(allLeases, lease)
		if first == nil {
			first = lease
		} else if lease.Runner() != first.Runner() {
			t.Fatal("same CacheKey returned different Runner instances")
		}
	}
	if first == nil {
		t.Fatal("no Runner leases returned")
	}
	for _, lease := range allLeases {
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunnerRegistryInvalidationDefersCloseUntilRelease(t *testing.T) {
	plan := testExecutionPlan(t)
	var calls atomic.Int32
	runners := make(chan *testRunner, 2)
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			runnerValue := &testRunner{}
			runners <- runnerValue
			calls.Add(1)
			return runnerValue, nil
		},
		MaxEntries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	oldRunner := <-runners
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if got := oldRunner.closeCount.Load(); got != 0 {
		t.Fatalf("borrowed Runner close count = %d, want 0", got)
	}
	newLease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	newRunner := <-runners
	if newRunner == oldRunner || calls.Load() != 2 {
		t.Fatal("invalidation reused the old Runner")
	}
	if err := oldLease.Release(); err != nil {
		t.Fatal(err)
	}
	if got := oldRunner.closeCount.Load(); got != 1 {
		t.Fatalf("released invalidated Runner close count = %d, want 1", got)
	}
	if err := newLease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryCapacityEvictsIdleButNotBorrowedEntries(t *testing.T) {
	planOne := testExecutionPlan(t)
	planTwo := testExecutionPlan(t)
	var runnersMu sync.Mutex
	var runners []*testRunner
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			runnerValue := &testRunner{}
			runnersMu.Lock()
			runners = append(runners, runnerValue)
			runnersMu.Unlock()
			return runnerValue, nil
		},
		MaxEntries: 1,
		IdleTTL:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	leaseOne, err := registry.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(context.Background(), planTwo); !errors.Is(err, ErrRunnerCapacity) {
		t.Fatalf("borrowed capacity error = %v", err)
	}
	if err := leaseOne.Release(); err != nil {
		t.Fatal(err)
	}
	leaseTwo, err := registry.Acquire(context.Background(), planTwo)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runners); got != 2 {
		t.Fatalf("factory calls = %d, want 2", got)
	}
	if got := runners[0].closeCount.Load(); got != 1 {
		t.Fatalf("evicted idle Runner close count = %d, want 1", got)
	}
	if err := leaseTwo.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryDoesNotCacheFactoryFailures(t *testing.T) {
	plan := testExecutionPlan(t)
	var calls atomic.Int32
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("provider secret must not escape")
			}
			return &testRunner{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	if _, err := registry.Acquire(context.Background(), plan); !errors.Is(err, ErrRunnerUnavailable) {
		t.Fatalf("first factory error = %v", err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("factory calls = %d, want 2", calls.Load())
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryCloseWaitsForBorrowedLease(t *testing.T) {
	plan := testExecutionPlan(t)
	runnerValue := &testRunner{}
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory:      func(context.Context, runtime.ExecutionPlan) (Runner, error) { return runnerValue, nil },
		CloseTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- registry.Close() }()
	time.Sleep(20 * time.Millisecond)
	if got := runnerValue.closeCount.Load(); got != 0 {
		t.Fatalf("Runner closed while lease borrowed: %d", got)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if got := runnerValue.closeCount.Load(); got != 1 {
		t.Fatalf("Runner close count = %d, want 1", got)
	}
}

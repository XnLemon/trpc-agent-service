package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/resilience"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestNewRunnerMaterializesPlanStorageCapability(t *testing.T) {
	fixture := runtimeFixture(t)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	sessions := inmemory.NewSessionService()
	builds := 0
	factory := backend.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		if input.TenantID != fixture.root.TenantID {
			t.Fatalf("storage factory tenant = %q", input.TenantID)
		}
		builds++
		set, err := backend.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: sessions})
		if err != nil {
			t.Fatal(err)
		}
		return set, nil
	})
	runner, err := NewRunner(context.Background(), plan, nil, &runtimeModelFactory{}, nil, factory)
	if err != nil {
		t.Fatal(err)
	}
	if builds != 1 {
		t.Fatalf("storage factory builds = %d", builds)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sessions.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewRunnerWithToolRegistryAndResilienceRetriesModelAndStorage(t *testing.T) {
	fixture := runtimeFixture(t)
	plan := newExecutionPlanForRunner(t, fixture)
	modelPolicy, err := resilience.New(resilience.Config{Timeout: time.Second, MaxAttempts: 2, FailureThreshold: 2, OpenTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	storagePolicy, err := resilience.New(resilience.Config{Timeout: time.Second, MaxAttempts: 2, FailureThreshold: 2, OpenTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	sessions := inmemory.NewSessionService()
	modelFactory := &retryingRuntimeModelFactory{failures: 1}
	storageBuilds := 0
	storageFactory := backend.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		storageBuilds++
		if storageBuilds == 1 {
			return nil, errors.New("storage unavailable")
		}
		return backend.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: sessions})
	})
	runner, err := NewRunnerWithToolRegistryAndResilience(context.Background(), plan, nil, modelFactory, nil, nil, nil, ResiliencePolicies{Model: modelPolicy, Storage: storagePolicy}, storageFactory)
	if err != nil {
		_ = sessions.Close()
		t.Fatal(err)
	}
	if modelFactory.calls != 2 || storageBuilds != 2 {
		_ = runner.Close()
		_ = sessions.Close()
		t.Fatalf("model calls=%d storage builds=%d, want two each", modelFactory.calls, storageBuilds)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewRunnerClosesStorageCapabilityWhenModelBuildFails(t *testing.T) {
	fixture := runtimeFixture(t)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	base := inmemory.NewSessionService()
	var closes atomic.Int32
	service := &closeCountingSession{Service: base, closes: &closes}
	factory := backend.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		return backend.NewCapabilitySet(fixture.root.TenantID, map[backend.Capability]any{backend.CapabilitySession: service})
	})
	if _, err := NewRunner(context.Background(), plan, nil, &runtimeModelFactory{err: errors.New("factory failed")}, nil, factory); err == nil {
		t.Fatal("NewRunner unexpectedly succeeded")
	}
	if closes.Load() != 1 {
		t.Fatalf("storage capability close count = %d", closes.Load())
	}
	_ = base.Close()
}

func TestNewRunnerWithObservabilityRecordsStorageFactorySuccess(t *testing.T) {
	fixture := runtimeFixture(t)
	plan := newExecutionPlanForRunner(t, fixture)
	telemetry := &runtimeTelemetryProvider{}
	sessions := inmemory.NewSessionService()
	factory := backend.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		return backend.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: sessions})
	})
	runner, err := NewRunnerWithObservability(context.Background(), plan, nil, &runtimeModelFactory{}, nil, telemetry, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	assertTelemetrySpan(t, telemetry, observability.OperationStorageOperation, observability.StatusOK, false)
	assertTelemetryMetric(t, telemetry, metrics.RequestsTotal, 1, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other", "status": "started"})
	assertTelemetryMetric(t, telemetry, metrics.BackendOperationDuration, -1, map[string]string{"component": "storage", "provider": "other", "status": "success", "error_class": ""})
}

func TestNewRunnerWithObservabilityRecordsStorageFactoryFailure(t *testing.T) {
	fixture := runtimeFixture(t)
	plan := newExecutionPlanForRunner(t, fixture)
	telemetry := &runtimeTelemetryProvider{}
	factoryErr := errors.New("storage unavailable")
	factory := backend.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		return nil, factoryErr
	})
	runner, err := NewRunnerWithObservability(context.Background(), plan, nil, &runtimeModelFactory{}, nil, telemetry, factory)
	if runner != nil || !errors.Is(err, factoryErr) {
		t.Fatalf("runner = %v, err = %v", runner, err)
	}

	assertTelemetrySpan(t, telemetry, observability.OperationStorageOperation, observability.StatusError, true)
	assertTelemetryMetric(t, telemetry, metrics.RequestsTotal, 1, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other", "status": "started"})
	assertTelemetryMetric(t, telemetry, metrics.BackendOperationDuration, -1, map[string]string{"component": "storage", "provider": "other", "status": "error", "error_class": "error"})
}

type closeCountingSession struct {
	session.Service
	closes *atomic.Int32
}

type retryingRuntimeModelFactory struct {
	failures int
	calls    int
}

func (factory *retryingRuntimeModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (trpcmodel.Model, error) {
	factory.calls++
	if factory.failures > 0 {
		factory.failures--
		return nil, errors.New("model unavailable")
	}
	return runtimeFakeModel{}, nil
}

func (service *closeCountingSession) Close() error {
	service.closes.Add(1)
	return service.Service.Close()
}

package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestNewRunnerMaterializesPlanStorageCapability(t *testing.T) {
	fixture := runtimeFixture(t)
	plan := mustExecutionPlan(t, fixture)
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
	runner, err := agent.NewRunner(context.Background(), agentRunnerInputForTest(t, plan), nil, &runtimeModelFactory{}, nil, factory)
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

func TestNewRunnerClosesStorageCapabilityWhenModelBuildFails(t *testing.T) {
	fixture := runtimeFixture(t)
	plan := mustExecutionPlan(t, fixture)
	base := inmemory.NewSessionService()
	var closes atomic.Int32
	service := &closeCountingSession{Service: base, closes: &closes}
	factory := backend.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		return backend.NewCapabilitySet(fixture.root.TenantID, map[backend.Capability]any{backend.CapabilitySession: service})
	})
	if _, err := agent.NewRunner(context.Background(), agentRunnerInputForTest(t, plan), nil, &runtimeModelFactory{err: errors.New("factory failed")}, nil, factory); err == nil {
		t.Fatal("NewRunner unexpectedly succeeded")
	}
	if closes.Load() != 1 {
		t.Fatalf("storage capability close count = %d", closes.Load())
	}
	_ = base.Close()
}

func TestNewRunnerWithObservabilityRecordsStorageFactorySuccess(t *testing.T) {
	fixture := runtimeFixture(t)
	plan := mustExecutionPlan(t, fixture)
	telemetry := &runtimeTelemetryProvider{}
	sessions := inmemory.NewSessionService()
	factory := backend.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		return backend.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: sessions})
	})
	runner, err := agent.NewRunnerWithObservability(context.Background(), agentRunnerInputForTest(t, plan), nil, &runtimeModelFactory{}, nil, telemetry, factory)
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
	plan := mustExecutionPlan(t, fixture)
	telemetry := &runtimeTelemetryProvider{}
	factoryErr := errors.New("storage unavailable")
	factory := backend.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*backend.CapabilitySet, error) {
		return nil, factoryErr
	})
	runner, err := agent.NewRunnerWithObservability(context.Background(), agentRunnerInputForTest(t, plan), nil, &runtimeModelFactory{}, nil, telemetry, factory)
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

func (service *closeCountingSession) Close() error {
	service.closes.Add(1)
	return service.Service.Close()
}

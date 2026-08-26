package runtime

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
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

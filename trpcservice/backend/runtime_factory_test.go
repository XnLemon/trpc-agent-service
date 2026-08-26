package backend

import (
	"context"
	"errors"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRegistryStorageFactoryMaterializesTenantSession(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	providers := NewProviderRegistry()
	factory := &sessionCapabilityProvider{}
	if err := providers.Register(tenantID, CapabilitySession, "memory", factory); err != nil {
		t.Fatal(err)
	}
	secrets := modelprofile.NewSecretRegistry()
	storageFactory, err := NewRegistryStorageFactory(providers, secrets)
	if err != nil {
		t.Fatal(err)
	}
	input := StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "memory"}}}
	set, err := storageFactory.New(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if service, err := set.Session(); err != nil || service == nil {
		t.Fatalf("Session() = %v, %v", service, err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := set.Session(); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Session after Close() = %v", err)
	}
}

func TestRegistryStorageFactoryCancellationAndMissingSession(t *testing.T) {
	providers := NewProviderRegistry()
	secrets := modelprofile.NewSecretRegistry()
	factory, err := NewRegistryStorageFactory(providers, secrets)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := factory.New(ctx, StorageFactoryInput{TenantID: "t_00000000000000000000000000", Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "missing"}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled New() = %v", err)
	}
	if _, err := factory.New(context.Background(), StorageFactoryInput{TenantID: "t_00000000000000000000000000", Bindings: []CapabilityBinding{{Capability: CapabilityMemory, Provider: "missing"}}}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("missing provider New() = %v", err)
	}
}

type sessionCapabilityProvider struct{}

func (sessionCapabilityProvider) New(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
	return inmemory.NewSessionService(), nil
}

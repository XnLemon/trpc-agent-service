package backend

import (
	"context"
	"errors"
	"sync"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
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

func TestRegistryStorageFactoryCancellationAfterProviderSuccess(t *testing.T) {
	providers := NewProviderRegistry()
	closed := make(chan struct{})
	provider := &sessionCapabilityProvider{closed: closed}
	const tenantID = "t_00000000000000000000000000"
	if err := providers.Register(tenantID, CapabilitySession, "memory", provider); err != nil {
		t.Fatal(err)
	}
	factory, err := NewRegistryStorageFactory(providers, modelprofile.NewSecretRegistry())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	provider.cancel = cancel
	if _, err := factory.New(ctx, StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "memory"}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("provider-success cancellation = %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("provider capability was not closed after cancellation")
	}
}

func TestRegistryStorageFactoryBuildsTenantCapabilitiesConcurrently(t *testing.T) {
	const (
		tenantOne = "t_00000000000000000000000000"
		tenantTwo = "t_00000000000000000000000001"
	)
	providers := NewProviderRegistry()
	provider := &recordingSessionCapabilityProvider{}
	for _, tenantID := range []string{tenantOne, tenantTwo} {
		if err := providers.Register(tenantID, CapabilitySession, "memory", provider); err != nil {
			t.Fatal(err)
		}
	}
	factory, err := NewRegistryStorageFactory(providers, modelprofile.NewSecretRegistry())
	if err != nil {
		t.Fatal(err)
	}
	const workers = 20
	sets := make(chan *CapabilitySet, workers)
	errorsCh := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		tenantID := tenantOne
		if index%2 == 1 {
			tenantID = tenantTwo
		}
		group.Add(1)
		go func(tenantID string) {
			defer group.Done()
			set, newErr := factory.New(context.Background(), StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "memory"}}})
			if newErr != nil {
				errorsCh <- newErr
				return
			}
			sets <- set
		}(tenantID)
	}
	group.Wait()
	close(sets)
	close(errorsCh)
	for newErr := range errorsCh {
		t.Fatal(newErr)
	}
	for set := range sets {
		if service, sessionErr := set.Session(); sessionErr != nil || service == nil {
			t.Fatalf("materialized Session() = %v, %v", service, sessionErr)
		}
		if err := set.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if provider.Count(tenantOne) != workers/2 || provider.Count(tenantTwo) != workers/2 {
		t.Fatalf("provider tenant calls = one:%d two:%d", provider.Count(tenantOne), provider.Count(tenantTwo))
	}
}

type sessionCapabilityProvider struct {
	cancel context.CancelFunc
	closed chan struct{}
}

func (provider *sessionCapabilityProvider) New(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
	if provider.cancel != nil {
		provider.cancel()
	}
	service := inmemory.NewSessionService()
	if provider.closed != nil {
		return &closeTrackingSession{Service: service, closed: provider.closed}, nil
	}
	return service, nil
}

type closeTrackingSession struct {
	session.Service
	closed chan struct{}
	once   sync.Once
}

func (service *closeTrackingSession) Close() error {
	service.once.Do(func() { close(service.closed) })
	return nil
}

type recordingSessionCapabilityProvider struct {
	mu    sync.Mutex
	calls map[string]int
}

func (provider *recordingSessionCapabilityProvider) New(_ context.Context, input StorageFactoryInput, _ CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	provider.mu.Lock()
	if provider.calls == nil {
		provider.calls = make(map[string]int)
	}
	provider.calls[input.TenantID]++
	provider.mu.Unlock()
	return inmemory.NewSessionService(), nil
}

func (provider *recordingSessionCapabilityProvider) Count(tenantID string) int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls[tenantID]
}

package channels

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestProviderRegistryIsTenantChannelScoped(t *testing.T) {
	digest, err := DigestPublicRouteKey(ChannelWeCom, "registry-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := NewBinding(CreateInput{TenantID: "t_00000000000000000000000000", BindingKey: "registry", Channel: ChannelWeCom, ProviderAccountID: "corp", PublicRouteKeyDigest: digest, AppID: "app_00000000000000000000000000", SecretRef: "secret/wecom", Protocol: ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{CorpID: "corp"}}})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewProviderRegistry()
	factory := registryProviderFactory{}
	if err := registry.Register(binding.TenantID, binding.Channel, " "+binding.ProviderAccountID+" ", factory); err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.Resolve(context.Background(), *binding)
	if err != nil || resolved == nil {
		t.Fatalf("Resolve() = %v, %v", resolved, err)
	}
	foreign := binding.Clone()
	foreign.TenantID = "t_00000000000000000000000001"
	if _, err := registry.Resolve(context.Background(), foreign); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("foreign Resolve() = %v", err)
	}
}

func TestProviderRegistryCancellationAndClose(t *testing.T) {
	registry := NewProviderRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Resolve(ctx, Binding{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Resolve() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("t_00000000000000000000000000", ChannelWeCom, "corp", registryProviderFactory{}); !errors.Is(err, ErrProviderRegistryClosed) {
		t.Fatalf("Register after Close() = %v", err)
	}
}

func TestProviderRegistryRemovalAndValidationBoundaries(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	registry := NewProviderRegistry()
	if err := registry.Register(tenantID, ChannelWeCom, "corp", registryProviderFactory{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(tenantID, ChannelWeCom, "corp"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tenantID, ChannelWeCom, "", registryProviderFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty account Register() = %v", err)
	}
	if err := registry.Remove("", ChannelWeCom, "corp"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty tenant Remove() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(tenantID, ChannelWeCom, "corp"); !errors.Is(err, ErrProviderRegistryClosed) {
		t.Fatalf("closed Remove() = %v", err)
	}
	var nilRegistry *ProviderRegistry
	if _, err := registry.Resolve(nil, Binding{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context Resolve() = %v", err)
	}
	if _, err := registry.Resolve(context.Background(), Binding{}); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("invalid binding Resolve() = %v", err)
	}
	if _, err := nilRegistry.Resolve(context.Background(), Binding{}); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("nil registry Resolve() = %v", err)
	}
}

type registryProviderFactory struct{}

func (registryProviderFactory) New(context.Context, Binding) (outbox.Provider, error) {
	return registryProvider{}, nil
}

type registryProvider struct{}

func (registryProvider) Deliver(context.Context, storage.ReplyOutbox) (string, error) {
	return "id", nil
}
func (registryProvider) Reconcile(context.Context, storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryAccepted, "id", nil
}

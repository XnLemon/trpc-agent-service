package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var (
	// ErrStorageFactory reports a failed or incomplete capability materialization.
	ErrStorageFactory = errors.New("backend storage factory failed")
	// ErrCapabilityUnavailable reports a required capability missing from a set.
	ErrCapabilityUnavailable = errors.New("backend capability unavailable")
)

// CapabilitySet owns the capabilities materialized for one immutable plan.
// Values are inaccessible after Close and are never shared across tenants.
type CapabilitySet struct {
	tenantID     string
	capabilities map[Capability]any
	closeOnce    sync.Once
	closeErr     error
}

// NewCapabilitySet creates an owned capability set for a trusted storage
// adapter. The map is copied so callers cannot mutate the set after return.
func NewCapabilitySet(tenantID string, capabilities map[Capability]any) (*CapabilitySet, error) {
	if validateTenantID(tenantID) != nil || len(capabilities) == 0 {
		return nil, fmt.Errorf("%w: capability set is invalid", ErrStorageFactory)
	}
	copyValues := make(map[Capability]any, len(capabilities))
	for kind, value := range capabilities {
		if kind == "" || value == nil {
			return nil, fmt.Errorf("%w: capability set contains an invalid value", ErrStorageFactory)
		}
		copyValues[kind] = value
	}
	return &CapabilitySet{tenantID: tenantID, capabilities: copyValues}, nil
}

// Capability returns one materialized capability. Callers must not retain it
// beyond the lifetime of the owning Runner.
func (set *CapabilitySet) Capability(kind Capability) (any, bool) {
	if set == nil {
		return nil, false
	}
	value, ok := set.capabilities[kind]
	return value, ok
}

// Session returns the tenant-scoped session.Service capability.
func (set *CapabilitySet) Session() (session.Service, error) {
	value, ok := set.Capability(CapabilitySession)
	if !ok {
		return nil, ErrCapabilityUnavailable
	}
	service, ok := value.(session.Service)
	if !ok || service == nil {
		return nil, ErrCapabilityUnavailable
	}
	return service, nil
}

// Close releases all owned capability values exactly once. Capabilities that
// do not expose Close are intentionally left untouched.
func (set *CapabilitySet) Close() error {
	if set == nil {
		return nil
	}
	set.closeOnce.Do(func() {
		for _, value := range set.capabilities {
			if closer, ok := value.(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					set.closeErr = errors.Join(set.closeErr, ErrStorageFactory)
				}
			}
		}
		clear(set.capabilities)
	})
	return set.closeErr
}

// StorageFactory materializes capabilities from a secret-free plan input.
type StorageFactory interface {
	New(context.Context, StorageFactoryInput) (*CapabilitySet, error)
}

// StorageFactoryFunc adapts a function to StorageFactory.
type StorageFactoryFunc func(context.Context, StorageFactoryInput) (*CapabilitySet, error)

// New implements StorageFactory.
func (factory StorageFactoryFunc) New(ctx context.Context, input StorageFactoryInput) (*CapabilitySet, error) {
	return factory(ctx, input)
}

// RegistryStorageFactory materializes backend capabilities from the tenant
// provider registry and shared SecretResolver.
type RegistryStorageFactory struct {
	providers *ProviderRegistry
	secrets   modelprofile.SecretResolver
}

// NewRegistryStorageFactory creates a factory borrowing both registries.
func NewRegistryStorageFactory(providers *ProviderRegistry, secrets modelprofile.SecretResolver) (*RegistryStorageFactory, error) {
	if providers == nil || secrets == nil {
		return nil, fmt.Errorf("%w: provider registry and secret resolver are required", ErrStorageFactory)
	}
	return &RegistryStorageFactory{providers: providers, secrets: secrets}, nil
}

// New materializes every binding in input and requires a Session capability.
// Already-built capabilities are closed if a later binding fails.
func (factory *RegistryStorageFactory) New(ctx context.Context, input StorageFactoryInput) (*CapabilitySet, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrStorageFactory)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if factory == nil || factory.providers == nil || factory.secrets == nil || validateTenantID(input.TenantID) != nil || len(input.Bindings) == 0 {
		return nil, ErrStorageFactory
	}
	set := &CapabilitySet{tenantID: input.TenantID, capabilities: make(map[Capability]any, len(input.Bindings))}
	for _, binding := range input.Bindings {
		if err := ctx.Err(); err != nil {
			_ = set.Close()
			return nil, err
		}
		if binding.Capability == "" || binding.Provider == "" {
			_ = set.Close()
			return nil, ErrStorageFactory
		}
		provider, err := factory.providers.Resolve(ctx, input, binding)
		if err != nil {
			_ = set.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrStorageFactory
		}
		secret := modelprofile.SecretValue{}
		if binding.SecretRef != "" {
			secret, err = factory.secrets.Resolve(ctx, modelprofile.SecretScope{TenantID: input.TenantID, SecretRef: binding.SecretRef})
			if err != nil {
				_ = set.Close()
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, ErrStorageFactory
			}
		}
		value, err := provider.New(ctx, input.Clone(), binding.Clone(), secret)
		if err != nil || value == nil {
			_ = set.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrStorageFactory
		}
		if err := ctx.Err(); err != nil {
			_ = set.Close()
			return nil, err
		}
		set.capabilities[binding.Capability] = value
	}
	if _, err := set.Session(); err != nil {
		_ = set.Close()
		return nil, ErrCapabilityUnavailable
	}
	return set, nil
}

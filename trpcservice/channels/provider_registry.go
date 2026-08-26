package channels

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
)

var (
	// ErrProviderUnavailable is the redacted result of an unknown channel provider.
	ErrProviderUnavailable = errors.New("channel provider unavailable")
	// ErrProviderRegistryClosed reports use after close.
	ErrProviderRegistryClosed = errors.New("channel provider registry is closed")
)

// ProviderFactory constructs a protocol-neutral outbox provider for one
// tenant-scoped Binding. Concrete Telegram/WeCom packages implement this
// contract without being imported by the shared channel model.
type ProviderFactory interface {
	New(context.Context, Binding) (outbox.Provider, error)
}

type channelProviderKey struct {
	tenantID, channel, account string
}

// ProviderRegistry resolves a tenant/channel/account tuple to an adapter
// factory. It contains no tokens or protocol credentials.
type ProviderRegistry struct {
	mu        sync.RWMutex
	factories map[channelProviderKey]ProviderFactory
	closed    bool
}

// NewProviderRegistry creates an empty channel provider registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{factories: make(map[channelProviderKey]ProviderFactory)}
}

// Register installs or replaces one tenant/channel/provider-account factory.
func (registry *ProviderRegistry) Register(tenantID string, channel Channel, providerAccountID string, factory ProviderFactory) error {
	if registry == nil || validateTenantID(tenantID) != nil || channel.Validate() != nil || strings.TrimSpace(providerAccountID) == "" || factory == nil {
		return fmt.Errorf("%w: invalid channel provider registration", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrProviderRegistryClosed
	}
	registry.factories[channelProviderKey{tenantID: tenantID, channel: string(channel), account: providerAccountID}] = factory
	return nil
}

// Resolve finds a provider factory without falling back across tenants.
func (registry *ProviderRegistry) Resolve(ctx context.Context, binding Binding) (ProviderFactory, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if registry == nil || binding.Validate() != nil {
		return nil, ErrProviderUnavailable
	}
	registry.mu.RLock()
	factory := registry.factories[channelProviderKey{tenantID: binding.TenantID, channel: string(binding.Channel), account: binding.ProviderAccountID}]
	closed := registry.closed
	registry.mu.RUnlock()
	if closed || factory == nil {
		return nil, ErrProviderUnavailable
	}
	return factory, nil
}

// Remove deletes one registration.
func (registry *ProviderRegistry) Remove(tenantID string, channel Channel, providerAccountID string) error {
	if registry == nil || validateTenantID(tenantID) != nil {
		return fmt.Errorf("%w: invalid channel provider scope", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrProviderRegistryClosed
	}
	delete(registry.factories, channelProviderKey{tenantID: tenantID, channel: string(channel), account: providerAccountID})
	return nil
}

// Close prevents new resolutions and drops factory references. Existing
// adapter instances remain owned by their caller.
func (registry *ProviderRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	clear(registry.factories)
	return nil
}

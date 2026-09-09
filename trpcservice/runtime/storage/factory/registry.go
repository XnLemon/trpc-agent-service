package factory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	backendprofile "github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

var (
	// ErrProviderUnavailable is the redacted result of an unknown backend provider.
	ErrProviderUnavailable = errors.New("backend provider unavailable")
	// ErrRegistryClosed reports use after a backend registry has been closed.
	ErrRegistryClosed = errors.New("backend provider registry is closed")
)

// CapabilityProvider constructs one runtime capability from one normalized
// binding. The provider receives a temporary secret only for this call.
type CapabilityProvider interface {
	New(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error)
}

type providerRegistryKey struct {
	tenantID   string
	capability Capability
	provider   string
}

// ProviderRegistry is a tenant-scoped backend capability factory registry.
// Registration is replaceable to support rotation without mutating plans.
type ProviderRegistry struct {
	mu        sync.RWMutex
	providers map[providerRegistryKey]CapabilityProvider
	closed    bool
}

// NewProviderRegistry creates an empty backend provider registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{providers: make(map[providerRegistryKey]CapabilityProvider)}
}

// Register installs or replaces one tenant/capability/provider implementation.
func (registry *ProviderRegistry) Register(tenantID string, capability Capability, provider string, value CapabilityProvider) error {
	if registry == nil || backendprofile.ValidateTenantID(tenantID) != nil || !validCapability(capability) || isNilCapability(value) {
		return fmt.Errorf("%w: invalid backend provider registration", ErrInvalid)
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if !validProviderName(provider) {
		return fmt.Errorf("%w: provider is invalid", ErrInvalid)
	}
	if provider == "" {
		return fmt.Errorf("%w: provider is required", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	if registry.providers == nil {
		registry.providers = make(map[providerRegistryKey]CapabilityProvider)
	}
	registry.providers[providerRegistryKey{tenantID: tenantID, capability: capability, provider: provider}] = value
	return nil
}

// Remove deletes one tenant/capability/provider registration.
func (registry *ProviderRegistry) Remove(tenantID string, capability Capability, provider string) error {
	if registry == nil || backendprofile.ValidateTenantID(tenantID) != nil {
		return fmt.Errorf("%w: invalid backend provider scope", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if !validCapability(capability) || !validProviderName(provider) {
		return fmt.Errorf("%w: invalid backend provider scope", ErrInvalid)
	}
	delete(registry.providers, providerRegistryKey{tenantID: tenantID, capability: capability, provider: provider})
	return nil
}

// Resolve returns a registered provider after validating the explicit tenant
// and binding identity. It never falls back to another tenant's provider.
func (registry *ProviderRegistry) Resolve(ctx context.Context, input StorageFactoryInput, binding CapabilityBinding) (CapabilityProvider, error) {
	if nilvalue.Is(ctx) {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := storageContextErr(ctx); err != nil {
		return nil, err
	}
	if registry == nil || backendprofile.ValidateTenantID(input.TenantID) != nil || appmodel.ValidateAppID(input.AppID) != nil || !validCapability(binding.Capability) || !validCapabilityBinding(binding) {
		return nil, ErrProviderUnavailable
	}
	providerName := strings.ToLower(strings.TrimSpace(binding.Provider))
	registry.mu.RLock()
	provider := registry.providers[providerRegistryKey{tenantID: input.TenantID, capability: binding.Capability, provider: providerName}]
	closed := registry.closed
	registry.mu.RUnlock()
	if closed || isNilCapability(provider) {
		return nil, ErrProviderUnavailable
	}
	return provider, nil
}

func validProviderName(value string) bool {
	if value == "" || !utf8.ValidString(value) || strings.TrimSpace(value) != value || len(value) > 64 {
		return false
	}
	for index, character := range value {
		if (index == 0 && (character < 'a' || character > 'z')) ||
			(index > 0 && (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' && character != '_') {
			return false
		}
	}
	return true
}

func validCapabilityBinding(binding CapabilityBinding) bool {
	provider := strings.ToLower(strings.TrimSpace(binding.Provider))
	if !validCapability(binding.Capability) || !validProviderName(provider) || provider != binding.Provider || !validBindingText(binding.Endpoint, 4096, false) || !validBindingText(binding.SecretRef, 256, false) {
		return false
	}
	if binding.SecretRef != "" && !validStorageSecretRef(binding.SecretRef) {
		return false
	}
	for key, value := range binding.Options {
		if !validOptionKey(key) || !validBindingText(value, 4096, false) || sensitiveCapabilityOptionKey(key) {
			return false
		}
	}
	return true
}

func validBindingText(value string, max int, required bool) bool {
	return utf8.ValidString(value) && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) < 0 && len([]rune(value)) <= max && (!required || value != "")
}

func validOptionKey(value string) bool {
	if value == "" || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func validStorageSecretRef(value string) bool {
	if value == "" {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') {
			continue
		}
		if index == 0 || (character != '.' && character != '_' && character != ':' && character != '/' && character != '-') {
			return false
		}
	}
	return true
}

func sensitiveCapabilityOptionKey(key string) bool {
	for _, part := range strings.FieldsFunc(key, func(r rune) bool { return r == '_' || r == '-' }) {
		switch part {
		case "api", "access", "secret", "private", "password", "passwd", "pwd", "token", "credential", "credentials", "dsn":
			return true
		}
	}
	compact := strings.NewReplacer("_", "", "-", "").Replace(key)
	for _, sequence := range []string{"apikey", "accesskey", "secretkey", "privatekey", "connectionstring", "password", "passwd", "pwd", "passphrase", "token", "secret", "credential", "credentials", "dsn"} {
		if strings.Contains(compact, sequence) {
			return true
		}
	}
	return false
}

// Close prevents future resolution and removes all factory references. It does
// not close capabilities already materialized by a caller.
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
	clear(registry.providers)
	return nil
}

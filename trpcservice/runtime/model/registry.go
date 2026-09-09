package modelruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

var (
	// ErrRegistryClosed reports use after a registry has been closed.
	ErrRegistryClosed = errors.New("model provider registry is closed")
	// ErrProviderUnavailable is the redacted result of a missing provider.
	ErrProviderUnavailable = errors.New("model provider unavailable")
	// ErrSecretUnavailable is the redacted result of a missing secret.
	ErrSecretUnavailable = errors.New("model secret unavailable")
)

type secretRegistryKey struct {
	tenantID, secretRef string
}

// SecretRegistry is an in-process tenant-scoped SecretResolver. It is intended
// for local development and deterministic tests; production implementations
// may delegate the same interface to KMS or a Secret Manager.
type SecretRegistry struct {
	mu     sync.RWMutex
	values map[secretRegistryKey]SecretValue
	closed bool
}

// NewSecretRegistry creates an empty registry. Values are never returned by
// errors, String methods, or registry metadata.
func NewSecretRegistry() *SecretRegistry {
	return &SecretRegistry{values: make(map[secretRegistryKey]SecretValue)}
}

// Register stores or replaces one tenant-scoped secret. The registry copies
// the opaque value and rejects malformed scopes.
func (registry *SecretRegistry) Register(scope SecretScope, value SecretValue) error {
	if registry == nil {
		return ErrSecretUnavailable
	}
	if err := scope.Validate(); err != nil || value.Value() == "" {
		return fmt.Errorf("%w: invalid secret registration", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	if registry.values == nil {
		registry.values = make(map[secretRegistryKey]SecretValue)
	}
	registry.values[secretRegistryKey{tenantID: scope.TenantID, secretRef: scope.SecretRef}] = value
	return nil
}

// RegisterValue validates and stores a raw value for trusted bootstrap/test
// code. Callers must not retain or log the supplied value.
func (registry *SecretRegistry) RegisterValue(scope SecretScope, value string) error {
	secret, err := NewSecretValue(value)
	if err != nil {
		return err
	}
	return registry.Register(scope, secret)
}

// Remove deletes one tenant-scoped secret. Missing entries are harmless.
func (registry *SecretRegistry) Remove(scope SecretScope) error {
	if registry == nil {
		return ErrSecretUnavailable
	}
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: invalid secret scope", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	delete(registry.values, secretRegistryKey{tenantID: scope.TenantID, secretRef: scope.SecretRef})
	return nil
}

// Resolve implements SecretResolver. Tenant and reference are checked before
// lookup, and cancellation wins over a successful lookup.
func (registry *SecretRegistry) Resolve(ctx context.Context, scope SecretScope) (SecretValue, error) {
	if nilvalue.Is(ctx) {
		return SecretValue{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := modelContextErr(ctx); err != nil {
		return SecretValue{}, err
	}
	if registry == nil {
		return SecretValue{}, ErrSecretUnavailable
	}
	if err := scope.Validate(); err != nil {
		return SecretValue{}, ErrSecretUnavailable
	}
	registry.mu.RLock()
	value, ok := registry.values[secretRegistryKey{tenantID: scope.TenantID, secretRef: scope.SecretRef}]
	closed := registry.closed
	registry.mu.RUnlock()
	if closed || !ok {
		return SecretValue{}, ErrSecretUnavailable
	}
	if err := modelContextErr(ctx); err != nil {
		return SecretValue{}, err
	}
	return value, nil
}

// Close removes all values and prevents future registrations.
func (registry *SecretRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	clear(registry.values)
	return nil
}

var _ SecretResolver = (*SecretRegistry)(nil)

type modelProviderKey struct {
	tenantID, provider string
}

// ModelProviderRegistry routes a tenant/provider pair to a trusted ModelFactory.
// It never stores model credentials and clones every factory input before
// invoking the selected provider.
type ModelProviderRegistry struct {
	mu        sync.RWMutex
	factories map[modelProviderKey]ModelFactory
	closed    bool
}

// NewModelProviderRegistry creates an empty tenant-scoped model registry.
func NewModelProviderRegistry() *ModelProviderRegistry {
	return &ModelProviderRegistry{factories: make(map[modelProviderKey]ModelFactory)}
}

// Register installs or replaces one tenant/provider factory.
func (registry *ModelProviderRegistry) Register(tenantID, provider string, factory ModelFactory) error {
	if registry == nil || isNilModelValue(factory) || !validRegistryTenant(tenantID) {
		return fmt.Errorf("%w: invalid model provider registration", ErrInvalid)
	}
	normalizedProvider := strings.ToLower(strings.TrimSpace(provider))
	if normalizedProvider == "" || strings.TrimSpace(provider) != provider || !validRegistryProvider(normalizedProvider) {
		return fmt.Errorf("%w: provider is invalid", ErrInvalid)
	}
	provider = normalizedProvider
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	if registry.factories == nil {
		registry.factories = make(map[modelProviderKey]ModelFactory)
	}
	registry.factories[modelProviderKey{tenantID: tenantID, provider: provider}] = factory
	return nil
}

// Remove deletes one tenant/provider registration.
func (registry *ModelProviderRegistry) Remove(tenantID, provider string) error {
	if registry == nil || !validRegistryTenant(tenantID) {
		return fmt.Errorf("%w: invalid model provider scope", ErrInvalid)
	}
	normalizedProvider := strings.ToLower(strings.TrimSpace(provider))
	if normalizedProvider == "" || strings.TrimSpace(provider) != provider || !validRegistryProvider(normalizedProvider) {
		return fmt.Errorf("%w: provider is invalid", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	delete(registry.factories, modelProviderKey{tenantID: tenantID, provider: normalizedProvider})
	return nil
}

// New implements ModelFactory and fails closed for unknown tenant/provider
// pairs. SecretValue is passed only to the selected factory.
func (registry *ModelProviderRegistry) New(ctx context.Context, input ModelFactoryInput, secret SecretValue) (trpcmodel.Model, error) {
	if nilvalue.Is(ctx) {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := modelContextErr(ctx); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, ErrProviderUnavailable
	}
	provider := strings.ToLower(strings.TrimSpace(input.Provider))
	if !validRegistryTenant(input.TenantID) || strings.TrimSpace(input.Provider) != input.Provider || !validRegistryProvider(provider) || input.Model == "" {
		return nil, ErrProviderUnavailable
	}
	registry.mu.RLock()
	factory := registry.factories[modelProviderKey{tenantID: input.TenantID, provider: provider}]
	closed := registry.closed
	registry.mu.RUnlock()
	if closed || isNilModelValue(factory) {
		return nil, ErrProviderUnavailable
	}
	model, err := callModelFactory(ctx, factory, input.Clone(), secret)
	if err != nil || isNilModelValue(model) {
		_ = closeModel(model)
		if contextErr := modelContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, ErrProviderUnavailable
	}
	if err := modelContextErr(ctx); err != nil {
		_ = closeModel(model)
		return nil, err
	}
	return model, nil
}

// Close prevents future provider construction and drops all factory handles.
func (registry *ModelProviderRegistry) Close() error {
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

func validRegistryTenant(value string) bool {
	return modelprofile.ValidateTenantID(value) == nil
}

func validRegistryProvider(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' && character != '_' && character != '.' {
			return false
		}
	}
	return true
}

var _ ModelFactory = (*ModelProviderRegistry)(nil)

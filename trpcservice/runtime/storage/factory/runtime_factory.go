package factory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	backendprofile "github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
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
	mu           sync.RWMutex
	capabilities map[Capability]any
	closeOnce    sync.Once
	closeErr     error
}

// NewCapabilitySet creates an owned capability set for a trusted storage
// adapter. The map is copied so callers cannot mutate the set after return.
func NewCapabilitySet(tenantID string, capabilities map[Capability]any) (*CapabilitySet, error) {
	if backendprofile.ValidateTenantID(tenantID) != nil || len(capabilities) == 0 {
		return nil, fmt.Errorf("%w: capability set is invalid", ErrStorageFactory)
	}
	copyValues := make(map[Capability]any, len(capabilities))
	for kind, value := range capabilities {
		if !validCapability(kind) || isNilCapability(value) {
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
	set.mu.RLock()
	defer set.mu.RUnlock()
	value, ok := set.capabilities[kind]
	if ok && isNilCapability(value) {
		return nil, false
	}
	return value, ok
}

func isNilCapability(value any) bool { return nilvalue.Is(value) }

func validateStorageFactoryInput(input StorageFactoryInput) error {
	if backendprofile.ValidateTenantID(input.TenantID) != nil || appmodel.ValidateAppID(input.AppID) != nil {
		return ErrStorageFactory
	}
	// The profile fields are optional for legacy/direct test fixtures, but a
	// populated field is still an identity claim and must be canonical. This
	// prevents a provider from receiving a syntactically valid tenant/app pair
	// combined with a padded or ambiguous control-plane reference.
	if input.TenantVersion < 0 || input.ProfileVersion < 0 {
		return ErrStorageFactory
	}
	if input.ProfileID != "" && backendprofile.ValidateProfileID(input.ProfileID) != nil {
		return ErrStorageFactory
	}
	if input.ProfileKey != "" && !validProfileKey(input.ProfileKey) {
		return ErrStorageFactory
	}
	if input.ContentDigest != "" && !validDigest(input.ContentDigest) {
		return ErrStorageFactory
	}
	if input.SchemaVersion < 0 || input.SchemaVersion > 1 {
		return ErrStorageFactory
	}
	previousRank := -1
	for _, binding := range input.Bindings {
		if !validCapabilityBinding(binding) {
			return ErrStorageFactory
		}
		rank := capabilityOrder(binding.Capability)
		if rank < previousRank {
			return ErrStorageFactory
		}
		previousRank = rank
	}
	return nil
}

func capabilityOrder(capability Capability) int {
	switch capability {
	case CapabilitySession:
		return 0
	case CapabilityMemory:
		return 1
	case CapabilitySummary:
		return 2
	case CapabilityKnowledge:
		return 3
	case CapabilityArtifact:
		return 4
	case CapabilityAudit:
		return 5
	default:
		return -1
	}
}

func validProfileKey(value string) bool {
	if len(value) < 2 || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validStorageSecretValue(value string) bool {
	_, err := modelprofile.NewSecretValue(value)
	return err == nil
}

// Session returns the tenant-scoped session.Service capability.
func (set *CapabilitySet) Session() (session.Service, error) {
	value, ok := set.Capability(CapabilitySession)
	if !ok {
		return nil, ErrCapabilityUnavailable
	}
	service, ok := value.(session.Service)
	if !ok || isNilCapability(service) {
		return nil, ErrCapabilityUnavailable
	}
	return service, nil
}

// Memory returns the tRPC-Agent-Go memory service for this execution.
func (set *CapabilitySet) Memory() (memory.Service, error) {
	value, ok := set.Capability(CapabilityMemory)
	if !ok {
		return nil, ErrCapabilityUnavailable
	}
	service, ok := value.(memory.Service)
	if !ok || isNilCapability(service) {
		return nil, ErrCapabilityUnavailable
	}
	return service, nil
}

// Summary returns the tenant-scoped summary capability. Summary must be
// materialized through the explicit summary binding; another capability that
// happens to implement SummaryStore does not imply summary ownership.
func (set *CapabilitySet) Summary() (runtimestorage.SummaryStore, error) {
	value, ok := set.Capability(CapabilitySummary)
	if !ok {
		return nil, ErrCapabilityUnavailable
	}
	service, ok := value.(runtimestorage.SummaryStore)
	if !ok || isNilCapability(service) {
		return nil, ErrCapabilityUnavailable
	}
	return service, nil
}

// Knowledge returns the tRPC-Agent-Go knowledge service for this execution.
func (set *CapabilitySet) Knowledge() (knowledge.Knowledge, error) {
	value, ok := set.Capability(CapabilityKnowledge)
	if !ok {
		return nil, ErrCapabilityUnavailable
	}
	service, ok := value.(knowledge.Knowledge)
	if !ok || isNilCapability(service) {
		return nil, ErrCapabilityUnavailable
	}
	return service, nil
}

// Artifact returns the tRPC-Agent-Go artifact service for this execution.
func (set *CapabilitySet) Artifact() (artifact.Service, error) {
	value, ok := set.Capability(CapabilityArtifact)
	if !ok {
		return nil, ErrCapabilityUnavailable
	}
	service, ok := value.(artifact.Service)
	if !ok || isNilCapability(service) {
		return nil, ErrCapabilityUnavailable
	}
	return service, nil
}

// Audit returns the tenant-scoped audit capability.
func (set *CapabilitySet) Audit() (runtimestorage.AuditStore, error) {
	value, ok := set.Capability(CapabilityAudit)
	if !ok {
		return nil, ErrCapabilityUnavailable
	}
	service, ok := value.(runtimestorage.AuditStore)
	if !ok || isNilCapability(service) {
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
		set.mu.Lock()
		keys := make([]string, 0, len(set.capabilities))
		for kind := range set.capabilities {
			keys = append(keys, string(kind))
		}
		sort.Strings(keys)
		values := make([]any, 0, len(keys))
		for _, key := range keys {
			values = append(values, set.capabilities[Capability(key)])
		}
		clear(set.capabilities)
		set.mu.Unlock()
		for index, value := range values {
			if closer, ok := value.(interface{ Close() error }); ok && !isNilCapability(closer) {
				if alreadyClosed(closer, values[:index]) {
					continue
				}
				if err := closeCapability(closer); err != nil {
					set.closeErr = errors.Join(set.closeErr, ErrStorageFactory)
				}
			}
		}
	})
	return set.closeErr
}

func closeCapability(closer interface{ Close() error }) (err error) {
	if isNilCapability(closer) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = ErrStorageFactory
		}
	}()
	err = closer.Close()
	if nilvalue.Is(err) {
		err = nil
	}
	return err
}

func alreadyClosed(closer interface{ Close() error }, values []any) bool {
	current := reflect.ValueOf(closer)
	if !current.IsValid() {
		return false
	}
	for _, value := range values {
		other, ok := value.(interface{ Close() error })
		if !ok {
			continue
		}
		candidate := reflect.ValueOf(other)
		if !candidate.IsValid() || current.Type() != candidate.Type() {
			continue
		}
		if current.Kind() == reflect.Pointer && current.Pointer() == candidate.Pointer() {
			return true
		}
		if current.Type().Comparable() && current.Interface() == candidate.Interface() {
			return true
		}
	}
	return false
}

// StorageFactory materializes capabilities from a secret-free plan input.
type StorageFactory interface {
	New(context.Context, StorageFactoryInput) (*CapabilitySet, error)
}

// StorageFactoryFunc adapts a function to StorageFactory.
type StorageFactoryFunc func(context.Context, StorageFactoryInput) (*CapabilitySet, error)

// New implements StorageFactory.
func (factory StorageFactoryFunc) New(ctx context.Context, input StorageFactoryInput) (set *CapabilitySet, err error) {
	if factory == nil || nilvalue.Is(ctx) {
		return nil, ErrStorageFactory
	}
	if err := storageContextErr(ctx); err != nil {
		return nil, err
	}
	if err := validateStorageFactoryInput(input); err != nil {
		return nil, ErrStorageFactory
	}
	defer func() {
		if recover() != nil {
			if set != nil {
				_ = set.Close()
			}
			set = nil
			err = ErrStorageFactory
		}
	}()
	set, err = factory(ctx, input)
	if nilvalue.Is(err) {
		err = nil
	}
	if err != nil || set == nil {
		if set != nil {
			_ = set.Close()
			set = nil
		}
		if err == nil {
			err = ErrStorageFactory
		}
		return nil, err
	}
	if err := storageContextErr(ctx); err != nil {
		_ = set.Close()
		return nil, err
	}
	return set, nil
}

// RegistryStorageFactory materializes backend capabilities from the tenant
// provider registry and shared SecretResolver.
type RegistryStorageFactory struct {
	providers *ProviderRegistry
	secrets   modelprofile.SecretResolver
}

// NewRegistryStorageFactory creates a factory borrowing both registries.
func NewRegistryStorageFactory(providers *ProviderRegistry, secrets modelprofile.SecretResolver) (*RegistryStorageFactory, error) {
	if providers == nil || isNilCapability(secrets) {
		return nil, fmt.Errorf("%w: provider registry and secret resolver are required", ErrStorageFactory)
	}
	return &RegistryStorageFactory{providers: providers, secrets: secrets}, nil
}

// New materializes every binding in input and requires a Session capability.
// Already-built capabilities are closed if a later binding fails.
func (factory *RegistryStorageFactory) New(ctx context.Context, input StorageFactoryInput) (set *CapabilitySet, err error) {
	defer func() {
		if recover() != nil {
			if set != nil {
				_ = set.Close()
			}
			set = nil
			err = ErrStorageFactory
		}
	}()
	if nilvalue.Is(ctx) {
		return nil, fmt.Errorf("%w: context is required", ErrStorageFactory)
	}
	if err := storageContextErr(ctx); err != nil {
		return nil, err
	}
	if factory == nil || factory.providers == nil || isNilCapability(factory.secrets) || len(input.Bindings) == 0 || validateStorageFactoryInput(input) != nil {
		return nil, ErrStorageFactory
	}
	set = &CapabilitySet{tenantID: input.TenantID, capabilities: make(map[Capability]any, len(input.Bindings))}
	for _, binding := range input.Bindings {
		if !validCapabilityBinding(binding) {
			_ = set.Close()
			return nil, ErrStorageFactory
		}
		if err := storageContextErr(ctx); err != nil {
			_ = set.Close()
			return nil, err
		}
		if _, exists := set.capabilities[binding.Capability]; exists {
			_ = set.Close()
			return nil, fmt.Errorf("%w: duplicate capability", ErrStorageFactory)
		}
		value, err := factory.materializeBinding(ctx, input, binding)
		if err != nil {
			_ = set.Close()
			return nil, err
		}
		set.capabilities[binding.Capability] = value
	}
	if _, err := set.Session(); err != nil {
		_ = set.Close()
		return nil, ErrCapabilityUnavailable
	}
	if err := storageContextErr(ctx); err != nil {
		_ = set.Close()
		return nil, err
	}
	return set, nil
}

func (factory *RegistryStorageFactory) materializeBinding(ctx context.Context, input StorageFactoryInput, binding CapabilityBinding) (any, error) {
	if nilvalue.Is(ctx) || binding.Capability == "" || binding.Provider == "" || appmodel.ValidateAppID(input.AppID) != nil || !validCapabilityBinding(binding) {
		return nil, ErrStorageFactory
	}
	if err := storageContextErr(ctx); err != nil {
		return nil, err
	}
	provider, err := factory.providers.Resolve(ctx, input, binding)
	if err != nil || isNilCapability(provider) {
		if contextErr := storageContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, ErrStorageFactory
	}
	secret := modelprofile.SecretValue{}
	if binding.SecretRef != "" {
		if isNilCapability(factory.secrets) {
			return nil, ErrStorageFactory
		}
		scope := modelprofile.SecretScope{TenantID: input.TenantID, SecretRef: binding.SecretRef}
		if scopeErr := scope.Validate(); scopeErr != nil {
			return nil, ErrStorageFactory
		}
		secret, err = callStorageSecretResolver(ctx, factory.secrets, scope)
		if err != nil || !validStorageSecretValue(secret.Value()) {
			if contextErr := storageContextErr(ctx); contextErr != nil {
				return nil, contextErr
			}
			return nil, ErrStorageFactory
		}
		if err := storageContextErr(ctx); err != nil {
			return nil, err
		}
	}
	value, err := callCapabilityProvider(ctx, provider, input.Clone(), binding.Clone(), secret)
	if err != nil || isNilCapability(value) {
		if closer, ok := value.(interface{ Close() error }); ok && !isNilCapability(closer) {
			_ = closeCapability(closer)
		}
		if contextErr := storageContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, ErrStorageFactory
	}
	if err := storageContextErr(ctx); err != nil {
		if closer, ok := value.(interface{ Close() error }); ok && !isNilCapability(closer) {
			_ = closeCapability(closer)
		}
		return nil, err
	}
	if !matchesCapability(binding.Capability, value) {
		if closer, ok := value.(interface{ Close() error }); ok && !isNilCapability(closer) {
			_ = closeCapability(closer)
		}
		return nil, ErrStorageFactory
	}
	return value, nil
}

func storageContextErr(ctx context.Context) error {
	if err := nilvalue.ContextErr(ctx); err != nil {
		if errors.Is(err, nilvalue.ErrInvalidContext) {
			return ErrStorageFactory
		}
		return err
	}
	return nil
}

func callStorageSecretResolver(ctx context.Context, resolver modelprofile.SecretResolver, scope modelprofile.SecretScope) (secret modelprofile.SecretValue, err error) {
	if nilvalue.Is(ctx) || nilvalue.Is(resolver) {
		return modelprofile.SecretValue{}, ErrStorageFactory
	}
	defer func() {
		if recover() != nil {
			secret = modelprofile.SecretValue{}
			err = ErrStorageFactory
		}
	}()
	secret, resolveErr := resolver.Resolve(ctx, scope)
	if nilvalue.Is(resolveErr) {
		resolveErr = nil
	}
	return secret, resolveErr
}

func callCapabilityProvider(ctx context.Context, provider CapabilityProvider, input StorageFactoryInput, binding CapabilityBinding, secret modelprofile.SecretValue) (value any, err error) {
	if isNilCapability(provider) {
		return nil, ErrStorageFactory
	}
	defer func() {
		if recover() != nil {
			value = nil
			err = ErrStorageFactory
		}
	}()
	value, providerErr := provider.New(ctx, input, binding, secret)
	if nilvalue.Is(providerErr) {
		providerErr = nil
	}
	return value, providerErr
}

func matchesCapability(kind Capability, value any) bool {
	if isNilCapability(value) {
		return false
	}
	switch kind {
	case CapabilitySession:
		_, ok := value.(session.Service)
		return ok
	case CapabilityMemory:
		_, ok := value.(memory.Service)
		return ok
	case CapabilitySummary:
		_, ok := value.(runtimestorage.SummaryStore)
		return ok
	case CapabilityKnowledge:
		_, ok := value.(knowledge.Knowledge)
		return ok
	case CapabilityArtifact:
		_, ok := value.(artifact.Service)
		return ok
	case CapabilityAudit:
		_, ok := value.(runtimestorage.AuditStore)
		return ok
	default:
		return false
	}
}

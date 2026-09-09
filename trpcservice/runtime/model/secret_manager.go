package modelruntime

import (
	"context"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
)

// SecretManagerResolver adapts a SecretManager to the runtime SecretResolver
// boundary. It keeps provider-specific clients out of execution plans.
type SecretManagerResolver struct {
	manager SecretManager
}

// NewSecretManagerResolver creates a resolver backed by one SecretManager.
func NewSecretManagerResolver(manager SecretManager) (*SecretManagerResolver, error) {
	if isNilSecretManager(manager) {
		return nil, fmt.Errorf("%w: secret manager is required", ErrInvalid)
	}
	return &SecretManagerResolver{manager: manager}, nil
}

// Resolve returns a secret only after validating its tenant scope. Provider
// failures are reduced to ErrSecretUnavailable so diagnostic paths cannot
// disclose credential values or backend details.
func (resolver *SecretManagerResolver) Resolve(ctx context.Context, scope SecretScope) (SecretValue, error) {
	if nilvalue.Is(ctx) {
		return SecretValue{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := modelContextErr(ctx); err != nil {
		return SecretValue{}, err
	}
	if resolver == nil || isNilSecretManager(resolver.manager) || scope.Validate() != nil {
		return SecretValue{}, ErrSecretUnavailable
	}
	value, err := callSecretManager(ctx, resolver.manager, scope)
	if err != nil {
		if contextErr := modelContextErr(ctx); contextErr != nil {
			return SecretValue{}, contextErr
		}
		return SecretValue{}, ErrSecretUnavailable
	}
	if value.Value() == "" {
		return SecretValue{}, ErrSecretUnavailable
	}
	if err := modelContextErr(ctx); err != nil {
		return SecretValue{}, err
	}
	return value, nil
}

func isNilSecretManager(value SecretManager) bool { return nilvalue.Is(value) }

func callSecretManager(ctx context.Context, manager SecretManager, scope SecretScope) (value SecretValue, err error) {
	if isNilSecretManager(manager) {
		return SecretValue{}, ErrSecretUnavailable
	}
	defer func() {
		if recover() != nil {
			value = SecretValue{}
			err = ErrSecretUnavailable
		}
	}()
	return manager.Read(ctx, scope)
}

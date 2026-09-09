package modelruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

var (
	// ErrSecretResolution is returned when a resolver cannot provide a secret.
	ErrSecretResolution = errors.New("model secret resolution failed")
	// ErrModelFactory is returned when a Model Factory cannot build a model.
	ErrModelFactory = errors.New("model factory failed")
)

// ResolveAndBuild resolves the optional secret and passes it directly to the
// ModelFactory. Resolver and Factory errors are intentionally sanitized so
// provider credentials cannot escape through an error chain.
func ResolveAndBuild(ctx context.Context, input modelprofile.ModelFactoryInput, resolver modelprofile.SecretResolver, factory modelprofile.ModelFactory) (trpcmodel.Model, error) {
	if nilvalue.Is(ctx) {
		return nil, fmt.Errorf("%w: context is required", modelprofile.ErrInvalid)
	}
	if isNilModelValue(factory) {
		return nil, fmt.Errorf("%w: model factory is required", modelprofile.ErrInvalid)
	}
	if err := modelContextErr(ctx); err != nil {
		return nil, err
	}
	if err := input.Validate(); err != nil {
		return nil, err
	}
	secret := modelprofile.SecretValue{}
	if input.SecretRef != "" {
		if isNilModelValue(resolver) {
			return nil, fmt.Errorf("%w: secret resolver is required", modelprofile.ErrInvalid)
		}
		scope := modelprofile.SecretScope{TenantID: input.TenantID, SecretRef: input.SecretRef}
		if err := scope.Validate(); err != nil {
			return nil, err
		}
		resolved, err := callSecretResolver(ctx, resolver, scope)
		if err != nil || resolved.Value() == "" {
			if contextErr := modelContextErr(ctx); contextErr != nil {
				return nil, contextErr
			}
			return nil, ErrSecretResolution
		}
		secret = resolved
		// Do not invoke a provider after a secret has crossed the process
		// boundary if the request was canceled while resolving it.
		if err := modelContextErr(ctx); err != nil {
			return nil, err
		}
	}
	model, err := callModelFactory(ctx, factory, input.Clone(), secret)
	if err != nil || isNilModelValue(model) {
		_ = closeModel(model)
		if contextErr := modelContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		if err != nil {
			return nil, ErrModelFactory
		}
		return nil, fmt.Errorf("%w: returned nil model", ErrModelFactory)
	}
	if err := modelContextErr(ctx); err != nil {
		_ = closeModel(model)
		return nil, err
	}
	return model, nil
}

func callSecretResolver(ctx context.Context, resolver modelprofile.SecretResolver, scope modelprofile.SecretScope) (secret modelprofile.SecretValue, err error) {
	if nilvalue.Is(ctx) || isNilModelValue(resolver) {
		return modelprofile.SecretValue{}, ErrSecretResolution
	}
	defer func() {
		if recover() != nil {
			secret = modelprofile.SecretValue{}
			err = ErrSecretResolution
		}
	}()
	secret, resolveErr := resolver.Resolve(ctx, scope)
	if nilvalue.Is(resolveErr) {
		resolveErr = nil
	}
	return secret, resolveErr
}

func callModelFactory(ctx context.Context, factory modelprofile.ModelFactory, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (model trpcmodel.Model, err error) {
	if nilvalue.Is(ctx) || isNilModelValue(factory) {
		return nil, ErrModelFactory
	}
	defer func() {
		if recover() != nil {
			model = nil
			err = ErrModelFactory
		}
	}()
	model, factoryErr := factory.New(ctx, input, secret)
	if nilvalue.Is(factoryErr) {
		factoryErr = nil
	}
	return model, factoryErr
}

func modelContextErr(ctx context.Context) error {
	if nilvalue.Is(ctx) {
		return modelprofile.ErrInvalid
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		if errors.Is(err, nilvalue.ErrInvalidContext) {
			return modelprofile.ErrInvalid
		}
		return err
	}
	return nil
}

func closeModel(model trpcmodel.Model) (err error) {
	if isNilModelValue(model) {
		return nil
	}
	closer, ok := model.(interface{ Close() error })
	if !ok || isNilModelValue(closer) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = ErrModelFactory
		}
	}()
	err = closer.Close()
	if nilvalue.Is(err) {
		err = nil
	}
	return err
}

// isNilModelValue handles interfaces containing typed nil pointers. A typed
// nil factory/model otherwise passes an ordinary interface comparison and can
// panic only after the runtime has crossed the materialization boundary.
func isNilModelValue(value any) bool { return nilvalue.Is(value) }

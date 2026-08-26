package model

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSecretManagerResolverValidatesScopeRedactsFailuresAndHonorsCancellation(t *testing.T) {
	secret, err := NewSecretValue("manager-secret")
	if err != nil {
		t.Fatal(err)
	}
	manager := secretManagerFunc(func(_ context.Context, scope SecretScope) (SecretValue, error) {
		if scope.TenantID != registryTenant || scope.SecretRef != "secret/manager" {
			return SecretValue{}, errors.New("foreign scope")
		}
		return secret, nil
	})
	resolver, err := NewSecretManagerResolver(manager)
	if err != nil {
		t.Fatal(err)
	}
	scope := SecretScope{TenantID: registryTenant, SecretRef: "secret/manager"}
	value, err := resolver.Resolve(context.Background(), scope)
	if err != nil || value.Value() != secret.Value() {
		t.Fatalf("Resolve() = %q, %v", value.Value(), err)
	}
	_, err = resolver.Resolve(context.Background(), SecretScope{TenantID: registryTenant, SecretRef: "secret/other"})
	if !errors.Is(err, ErrSecretUnavailable) || strings.Contains(err.Error(), secret.Value()) {
		t.Fatalf("foreign Resolve() = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Resolve() = %v", err)
	}
	if _, err := NewSecretManagerResolver(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil manager = %v", err)
	}
}

type secretManagerFunc func(context.Context, SecretScope) (SecretValue, error)

func (function secretManagerFunc) Read(ctx context.Context, scope SecretScope) (SecretValue, error) {
	return function(ctx, scope)
}

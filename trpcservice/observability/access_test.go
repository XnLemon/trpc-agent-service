package observability

import (
	"context"
	"errors"
	"testing"
)

func TestAuthorizeTenantEnforcesTrustedScope(t *testing.T) {
	authorizer := NewStaticTenantAuthorizer(map[string][]string{"admin": {"tenant-a"}, "user": {"tenant-b"}})
	if err := AuthorizeTenant(context.Background(), authorizer, "admin", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(AuthorizeTenant(context.Background(), authorizer, "admin", "tenant-b"), ErrTenantAccessDenied) {
		t.Fatal("cross-tenant query must be denied")
	}
	if !errors.Is(AuthorizeTenant(context.Background(), authorizer, "", "tenant-a"), ErrInvalidTenantScope) {
		t.Fatal("empty actor must be rejected")
	}
}

func TestValidateTenantQueryScopeRejectsUnboundedInput(t *testing.T) {
	if err := ValidateTenantQueryScope([]string{"tenant-a", "tenant-b"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTenantQueryScope(nil); err == nil {
		t.Fatal("empty scope must be rejected")
	}
	if err := ValidateTenantQueryScope([]string{"tenant-a", "tenant-a"}); err == nil {
		t.Fatal("duplicate scope must be rejected")
	}
}

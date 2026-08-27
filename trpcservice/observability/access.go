package observability

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrTenantAccessDenied reports that an actor cannot query a tenant aggregate.
	ErrTenantAccessDenied = errors.New("tenant observability access denied")
	// ErrInvalidTenantScope reports an empty or malformed tenant scope.
	ErrInvalidTenantScope = errors.New("invalid tenant observability scope")
)

// TenantAuthorizer is the narrow boundary used by dashboards and aggregate
// readers. Implementations must derive actor identity from trusted auth.
type TenantAuthorizer interface {
	AllowTenant(context.Context, string, string) bool
}

// AuthorizeTenant validates an authorized actor/tenant pair before a dashboard
// or aggregate query is constructed. It never builds a query expression.
func AuthorizeTenant(ctx context.Context, authorizer TenantAuthorizer, actorID, tenantID string) error {
	if ctx == nil || strings.TrimSpace(actorID) == "" || strings.TrimSpace(tenantID) == "" || strings.ContainsAny(actorID+tenantID, "\r\n") {
		return ErrInvalidTenantScope
	}
	if authorizer == nil || !authorizer.AllowTenant(ctx, actorID, tenantID) {
		return ErrTenantAccessDenied
	}
	return nil
}

// StaticTenantAuthorizer is a small immutable authorizer for process-local
// dashboard adapters and tests. The map is copied at construction.
type StaticTenantAuthorizer struct {
	allowed map[string]map[string]struct{}
}

// NewStaticTenantAuthorizer creates an actor-to-tenant allowlist.
func NewStaticTenantAuthorizer(allowed map[string][]string) StaticTenantAuthorizer {
	copyAllowed := make(map[string]map[string]struct{}, len(allowed))
	for actor, tenants := range allowed {
		set := make(map[string]struct{}, len(tenants))
		for _, tenant := range tenants {
			if strings.TrimSpace(tenant) != "" {
				set[tenant] = struct{}{}
			}
		}
		copyAllowed[actor] = set
	}
	return StaticTenantAuthorizer{allowed: copyAllowed}
}

// AllowTenant reports whether actorID may read tenantID aggregates.
func (a StaticTenantAuthorizer) AllowTenant(_ context.Context, actorID, tenantID string) bool {
	_, ok := a.allowed[actorID][tenantID]
	return ok
}

// ValidateTenantQueryScope rejects unbounded dashboard scope requests.
func ValidateTenantQueryScope(tenantIDs []string) error {
	if len(tenantIDs) == 0 {
		return fmt.Errorf("%w: at least one tenant is required", ErrInvalidTenantScope)
	}
	seen := make(map[string]struct{}, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		if strings.TrimSpace(tenantID) == "" || strings.ContainsAny(tenantID, "\r\n") {
			return ErrInvalidTenantScope
		}
		if _, duplicate := seen[tenantID]; duplicate {
			return fmt.Errorf("%w: duplicate tenant", ErrInvalidTenantScope)
		}
		seen[tenantID] = struct{}{}
	}
	return nil
}

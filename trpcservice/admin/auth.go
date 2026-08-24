package admin

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

var (
	ErrUnauthenticated = errors.New("admin authentication required")
	ErrForbidden       = errors.New("admin principal is outside tenant scope")
)

// Principal is the proof-bearing identity for the control-plane API. A global
// principal is reserved for the configured first-tenant/platform boundary.
type Principal struct {
	SubjectID    string
	TenantScopes map[string]struct{}
	Global       bool
}

func (p Principal) Allows(tenantID string, creating bool) bool {
	if p.Global {
		return tenantID != "" || creating
	}
	_, ok := p.TenantScopes[tenantID]
	return ok && tenantID != ""
}

// Authenticator is deliberately separate from gateway.APIAuthenticator so a
// chat token can never be upgraded into an Admin principal by path routing.
type Authenticator interface {
	Authenticate(context.Context, *http.Request) (Principal, error)
}

// StaticAuthenticator is the production bootstrap's small explicit token
// boundary. A later identity provider can implement Authenticator without
// changing handlers or domain repositories.
type StaticAuthenticator struct {
	token     string
	principal Principal
}

func NewStaticAuthenticator(token string, scopes []string) (*StaticAuthenticator, error) {
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, ErrUnauthenticated
	}
	principal := Principal{SubjectID: "admin", TenantScopes: map[string]struct{}{}}
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "*" {
			principal.Global = true
			continue
		}
		if scope != "" {
			principal.TenantScopes[scope] = struct{}{}
		}
	}
	if !principal.Global && len(principal.TenantScopes) == 0 {
		return nil, ErrForbidden
	}
	return &StaticAuthenticator{token: token, principal: principal}, nil
}

func (a *StaticAuthenticator) Authenticate(ctx context.Context, request *http.Request) (Principal, error) {
	if a == nil || request == nil || ctx == nil {
		return Principal{}, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(header, "Bearer ")) != a.token {
		return Principal{}, ErrUnauthenticated
	}
	return a.principal, nil
}

package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStaticAuthenticatorConfigurationAndBoundary(t *testing.T) {
	for _, token := range []string{"", "bad\n-token"} {
		if _, err := NewStaticAuthenticator(token, []string{"tenant"}); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("token %q error = %v", token, err)
		}
	}
	if _, err := NewStaticAuthenticator("token", nil); !errors.Is(err, ErrForbidden) {
		t.Fatalf("empty scopes error = %v", err)
	}
	auth, err := NewStaticAuthenticator("token", []string{"tenant-a", "*", "tenant-b"})
	if err != nil {
		t.Fatal(err)
	}
	if !auth.principal.Global || !auth.principal.Allows("", true) || !auth.principal.Allows("tenant-a", true) || !auth.principal.Allows("tenant-a", false) || auth.principal.Allows("missing", false) {
		t.Fatalf("unexpected principal scopes = %+v", auth.principal)
	}

	request := httptest.NewRequest(http.MethodGet, "/admin/v1", nil)
	for _, tc := range []struct {
		name string
		ctx  context.Context
		req  *http.Request
		want error
	}{
		{"nil context", nil, request, ErrUnauthenticated},
		{"nil request", context.Background(), nil, ErrUnauthenticated},
		{"missing bearer", context.Background(), request, ErrUnauthenticated},
		{"wrong token", context.Background(), func() *http.Request {
			r := request.Clone(context.Background())
			r.Header.Set("Authorization", "Bearer wrong")
			return r
		}(), ErrUnauthenticated},
		{"canceled", func() context.Context { c, cancel := context.WithCancel(context.Background()); cancel(); return c }(), request, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, got := auth.Authenticate(tc.ctx, tc.req)
			if !errors.Is(got, tc.want) {
				t.Fatalf("Authenticate error = %v, want %v", got, tc.want)
			}
		})
	}
	request.Header.Set("Authorization", "Bearer token")
	principal, err := auth.Authenticate(context.Background(), request)
	if err != nil || !principal.Global {
		t.Fatalf("valid authentication = %+v, %v", principal, err)
	}
}

package admin

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrUnauthenticated reports a missing or invalid admin credential.
	ErrUnauthenticated = errors.New("admin authentication required")
	// ErrForbidden reports a principal outside the requested tenant scope.
	ErrForbidden = errors.New("admin principal is outside tenant scope")
)

// Principal is the proof-bearing identity for the control-plane API. A global
// principal is reserved for the configured first-tenant/platform boundary.
type Principal struct {
	SubjectID    string
	TenantScopes map[string]struct{}
	Global       bool
}

// ScopeIDs returns a stable, defensive list for the /me response.
func (p Principal) ScopeIDs() []string {
	values := make([]string, 0, len(p.TenantScopes))
	for scope := range p.TenantScopes {
		values = append(values, scope)
	}
	sort.Strings(values)
	return values
}

// Allows reports whether the principal may access the tenant operation.
func (p Principal) Allows(tenantID string, creating bool) bool {
	// A global scope is intentionally limited to the controlled first-tenant
	// creation boundary. It never becomes an implicit wildcard for reads or
	// writes, which keeps every existing resource operation tenant-scoped.
	if creating {
		return p.Global
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

// AdminSessionCookie is the browser-only session cookie name. It is not a
// bearer credential and is deliberately scoped to the same-origin admin API.
const AdminSessionCookie = "trpc_admin_session"

const defaultSessionTTL = 8 * time.Hour

type adminSession struct {
	principal Principal
	expiresAt time.Time
}

// SessionAuthenticator provides the browser authentication boundary for the
// admin console. The optional static authenticator is retained as a private
// service-to-service compatibility path; browsers never receive its token.
type SessionAuthenticator struct {
	username string
	password string
	static   *StaticAuthenticator
	ttl      time.Duration

	mu       sync.Mutex
	sessions map[string]adminSession
	now      func() time.Time
	rand     func([]byte) error
}

// NewSessionAuthenticator creates an account/password authenticator backed by
// process-local opaque sessions. Deployments with multiple replicas should
// put a sticky/session-aware BFF in front or replace this implementation with
// a shared identity provider.
func NewSessionAuthenticator(username, password string, static *StaticAuthenticator) (*SessionAuthenticator, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" || strings.ContainsAny(username, "\r\n") || strings.ContainsAny(password, "\r\n") {
		return nil, ErrUnauthenticated
	}
	if static == nil {
		return nil, ErrUnauthenticated
	}
	return &SessionAuthenticator{
		username: username,
		password: password,
		static:   static,
		ttl:      defaultSessionTTL,
		sessions: make(map[string]adminSession),
		now:      time.Now,
		rand:     secureRandom,
	}, nil
}

// Authenticate accepts a valid session cookie, or the configured static
// bearer token when no browser session is present.
func (a *SessionAuthenticator) Authenticate(ctx context.Context, request *http.Request) (Principal, error) {
	if a == nil || request == nil || ctx == nil {
		return Principal{}, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	if cookie, err := request.Cookie(AdminSessionCookie); err == nil && cookie.Value != "" {
		principal, ok := a.sessionPrincipal(cookie.Value)
		if !ok {
			return Principal{}, ErrUnauthenticated
		}
		if request.Method != http.MethodGet && request.Method != http.MethodHead && !sameOrigin(request) {
			return Principal{}, ErrForbidden
		}
		return principal, nil
	}
	if a.static != nil {
		return a.static.Authenticate(ctx, request)
	}
	return Principal{}, ErrUnauthenticated
}

// ServeHTTP exposes /admin/auth/login, /admin/auth/session and
// /admin/auth/logout. It is intentionally separate from /admin/v1 so the
// resource handler can keep its existing proof-bearing Authenticator API.
func (a *SessionAuthenticator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request == nil {
		writeError(writer, uuid.NewString(), http.StatusBadRequest, "invalid_request")
		return
	}
	requestID := strings.TrimSpace(request.Header.Get("X-Request-ID"))
	if requestID == "" {
		requestID = uuid.NewString()
	}
	switch request.URL.Path {
	case "/admin/auth/login":
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeError(writer, requestID, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		a.login(writer, request, requestID)
	case "/admin/auth/session":
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writeError(writer, requestID, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		a.session(writer, request, requestID)
	case "/admin/auth/logout":
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeError(writer, requestID, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		a.logout(writer, request, requestID)
	default:
		writeError(writer, requestID, http.StatusNotFound, "not_found")
	}
}

func (a *SessionAuthenticator) login(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !sameOriginOrNoOrigin(request) {
		writeError(writer, requestID, http.StatusForbidden, "forbidden")
		return
	}
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := decodeAuthBody(request, &input); err != nil {
		writeError(writer, requestID, http.StatusBadRequest, "invalid_request")
		return
	}
	if !constantTimeEqual(input.Username, a.username) || !constantTimeEqual(input.Password, a.password) {
		writeError(writer, requestID, http.StatusUnauthorized, "unauthorized")
		return
	}
	token, err := a.newSession()
	if err != nil {
		writeError(writer, requestID, http.StatusInternalServerError, "internal_error")
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name: AdminSessionCookie, Value: token, Path: "/", HttpOnly: true,
		Secure: request.TLS != nil, SameSite: http.SameSiteLaxMode,
		MaxAge: int(a.ttl.Seconds()), Expires: a.now().Add(a.ttl),
	})
	writeJSON(writer, requestID, http.StatusOK, principalResponse(a.static.principal))
}

func constantTimeEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func (a *SessionAuthenticator) session(writer http.ResponseWriter, request *http.Request, requestID string) {
	principal, err := a.Authenticate(request.Context(), request)
	if err != nil {
		writeError(writer, requestID, http.StatusUnauthorized, "unauthorized")
		return
	}
	writeJSON(writer, requestID, http.StatusOK, principalResponse(principal))
}

func (a *SessionAuthenticator) logout(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !sameOrigin(request) {
		writeError(writer, requestID, http.StatusForbidden, "forbidden")
		return
	}
	if cookie, err := request.Cookie(AdminSessionCookie); err == nil {
		a.mu.Lock()
		delete(a.sessions, cookie.Value)
		a.mu.Unlock()
	}
	http.SetCookie(writer, &http.Cookie{Name: AdminSessionCookie, Value: "", Path: "/", HttpOnly: true, Secure: request.TLS != nil, SameSite: http.SameSiteLaxMode, MaxAge: -1, Expires: time.Unix(1, 0).UTC()})
	writeJSON(writer, requestID, http.StatusOK, map[string]any{"logged_out": true})
}

func (a *SessionAuthenticator) newSession() (string, error) {
	bytes := make([]byte, 32)
	if a.rand == nil {
		a.rand = secureRandom
	}
	if err := a.rand(bytes); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(bytes)
	now := a.now()
	a.mu.Lock()
	for key, session := range a.sessions {
		if !now.Before(session.expiresAt) {
			delete(a.sessions, key)
		}
	}
	a.sessions[token] = adminSession{principal: a.static.principal, expiresAt: now.Add(a.ttl)}
	a.mu.Unlock()
	return token, nil
}

func (a *SessionAuthenticator) sessionPrincipal(token string) (Principal, bool) {
	now := a.now()
	a.mu.Lock()
	defer a.mu.Unlock()
	session, ok := a.sessions[token]
	if !ok || !now.Before(session.expiresAt) {
		if ok {
			delete(a.sessions, token)
		}
		return Principal{}, false
	}
	return session.principal, true
}

func decodeAuthBody(request *http.Request, target any) error {
	if request == nil || request.Body == nil {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func principalResponse(principal Principal) map[string]any {
	return map[string]any{
		"subject_id": principal.SubjectID, "global": principal.Global,
		"tenant_scopes": principal.ScopeIDs(), "can_create_tenant": principal.Global,
	}
}

func sameOriginOrNoOrigin(request *http.Request) bool {
	if request == nil {
		return false
	}
	if strings.TrimSpace(request.Header.Get("Origin")) == "" && strings.TrimSpace(request.Header.Get("Referer")) == "" {
		return true
	}
	return sameOrigin(request)
}

func sameOrigin(request *http.Request) bool {
	if request == nil {
		return false
	}
	value := strings.TrimSpace(request.Header.Get("Origin"))
	if value == "" {
		value = strings.TrimSpace(request.Header.Get("Referer"))
	}
	if value == "" {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return false
	}
	host := request.Host
	if host == "" {
		host = request.URL.Host
	}
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	return strings.EqualFold(parsed.Host, host) && strings.EqualFold(parsed.Scheme, scheme)
}

func secureRandom(buffer []byte) error {
	_, err := rand.Read(buffer)
	return err
}

// NewStaticAuthenticator creates a bearer-token authenticator with fixed scopes.
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

// Authenticate validates the request bearer token and returns its principal.
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

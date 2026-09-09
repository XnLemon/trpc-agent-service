// Package vault provides a small HashiCorp Vault KV v2 SecretManager adapter.
package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

var (
	// ErrInvalid indicates invalid Vault manager configuration or input.
	ErrInvalid = errors.New("invalid vault secret manager")
	// ErrUnavailable indicates that the Vault secret could not be read.
	ErrUnavailable = errors.New("vault secret unavailable")
	// ErrUnauthorized indicates that Vault rejected the configured token.
	ErrUnauthorized = errors.New("vault secret manager unauthorized")
)

// Config selects a Vault KV v2 mount. Token is runtime-only and is never
// included in errors or serialized configuration.
type Config struct {
	BaseURL    string
	Token      string
	Mount      string
	HTTPClient *http.Client
}

// Manager reads the conventional KV v2 `value` field under tenant-scoped
// paths: <tenant-id>/<secret-ref>.
type Manager struct {
	baseURL string
	token   string
	mount   string
	client  *http.Client
}

// New validates configuration and creates a Vault KV v2 manager.
func New(config Config) (*Manager, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.Mount) == "" {
		return nil, ErrInvalid
	}
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &Manager{baseURL: baseURL, token: strings.TrimSpace(config.Token), mount: strings.Trim(config.Mount, "/"), client: client}, nil
}

func vaultContextErr(ctx context.Context) error {
	if nilvalue.Is(ctx) {
		return ErrInvalid
	}
	return ctx.Err()
}

// Read implements model.SecretManager. Vault response and transport details
// are reduced to stable redacted errors.
func (manager *Manager) Read(ctx context.Context, scope modelprofile.SecretScope) (value modelprofile.SecretValue, err error) {
	defer func() {
		if recover() != nil {
			value = modelprofile.SecretValue{}
			err = ErrUnavailable
		}
	}()
	if nilvalue.Is(ctx) {
		return modelprofile.SecretValue{}, ErrInvalid
	}
	if err := vaultContextErr(ctx); err != nil {
		return modelprofile.SecretValue{}, err
	}
	if manager == nil || manager.client == nil || scope.Validate() != nil {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	path := strings.Trim(scope.SecretRef, "/")
	if path == "" || strings.Contains(path, "..") || strings.ContainsAny(path, "\r\n") {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	endpoint := manager.baseURL + "/v1/" + manager.mount + "/data/" + url.PathEscape(scope.TenantID+"/"+path)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	request.Header.Set("X-Vault-Token", manager.token)
	response, err := manager.client.Do(request)
	if err != nil {
		if contextErr := vaultContextErr(ctx); contextErr != nil {
			return modelprofile.SecretValue{}, contextErr
		}
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	if response == nil || response.Body == nil {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return modelprofile.SecretValue{}, ErrUnauthorized
	}
	if response.StatusCode != http.StatusOK {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 || jsonstrict.Validate(body, true) != nil {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	var payload struct {
		Data struct {
			Data map[string]any `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	raw, ok := payload.Data.Data["value"].(string)
	if !ok || raw == "" {
		return modelprofile.SecretValue{}, ErrUnavailable
	}
	value, err = modelprofile.NewSecretValue(raw)
	if err != nil {
		return modelprofile.SecretValue{}, fmt.Errorf("%w: secret value is invalid", ErrUnavailable)
	}
	if err := vaultContextErr(ctx); err != nil {
		return modelprofile.SecretValue{}, err
	}
	return value, nil
}

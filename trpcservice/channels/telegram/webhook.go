package telegram

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/go-telegram/bot/models"
)

// WebhookConfig configures the HTTP boundary around an existing Telegram
// Adapter. The listener itself remains owned by the caller.
type WebhookConfig struct {
	Path string
	// SecretToken is the value expected in Telegram's secret header.
	SecretToken string
	// Secret is retained as a concise alias for callers constructing config
	// literals; SecretToken takes precedence when both are set.
	Secret       string
	MaxBodyBytes int64
}

// Webhook is a replay-safe, secret-header authenticated Telegram HTTP handler.
// It does not create or own an http.Server.
type Webhook struct {
	adapter *Adapter
	path    string
	secret  []byte
	maxBody int64

	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
}

// NewWebhook validates configuration and binds one Adapter to one exact path.
func NewWebhook(adapter *Adapter, config WebhookConfig) (*Webhook, error) {
	if config.SecretToken == "" {
		config.SecretToken = config.Secret
	}
	if adapter == nil || strings.TrimSpace(config.Path) == "" || !strings.HasPrefix(config.Path, "/") || strings.ContainsAny(config.Path, "?#") || strings.TrimSpace(config.SecretToken) == "" || strings.TrimSpace(config.SecretToken) != config.SecretToken {
		return nil, fmt.Errorf("%w: webhook configuration is invalid", ErrInvalid)
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = 1 << 20
	}
	if config.MaxBodyBytes < 1 || len([]rune(config.SecretToken)) > maximumTokenRunes || hasControl(config.SecretToken) {
		return nil, fmt.Errorf("%w: webhook configuration is invalid", ErrInvalid)
	}
	return &Webhook{adapter: adapter, path: strings.TrimRight(config.Path, "/"), secret: []byte(config.SecretToken), maxBody: config.MaxBodyBytes}, nil
}

// Channel identifies the wrapped Telegram adapter.
func (w *Webhook) Channel() channels.Channel { return channels.ChannelTelegram }

// ServeHTTP authenticates, decodes and dispatches one Telegram update.
func (w *Webhook) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if w == nil || request == nil || request.URL.Path != w.path {
		http.NotFound(response, request)
		return
	}
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	provided := []byte(request.Header.Get("X-Telegram-Bot-Api-Secret-Token"))
	if subtle.ConstantTimeCompare(provided, w.secret) != 1 {
		http.Error(response, "forbidden", http.StatusForbidden)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, w.maxBody+1))
	if err != nil || int64(len(body)) > w.maxBody {
		http.Error(response, "bad request", http.StatusBadRequest)
		return
	}
	var update models.Update
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&update); err != nil {
		http.Error(response, "bad request", http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		http.Error(response, "bad request", http.StatusBadRequest)
		return
	}
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		http.Error(response, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.wg.Add(1)
	w.mu.Unlock()
	defer w.wg.Done()
	err = w.adapter.HandleUpdate(request.Context(), &update)
	switch {
	case err == nil, errors.Is(err, ErrDuplicateUpdate):
		response.WriteHeader(http.StatusOK)
	case errors.Is(err, ErrClosed), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		http.Error(response, "unavailable", http.StatusServiceUnavailable)
	case errors.Is(err, ErrInvalidUpdate), errors.Is(err, ErrUnsupportedUpdate):
		http.Error(response, "bad request", http.StatusBadRequest)
	default:
		http.Error(response, "unavailable", http.StatusServiceUnavailable)
	}
}

// BeginShutdown prevents new webhook admissions.
func (w *Webhook) BeginShutdown() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.closing = true
	w.mu.Unlock()
}

// Close stops admissions, waits for accepted updates, and closes the wrapped
// adapter. The caller continues to own the HTTP listener.
func (w *Webhook) Close() error {
	if w == nil {
		return nil
	}
	w.BeginShutdown()
	w.wg.Wait()
	return w.adapter.Close()
}

package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/go-telegram/bot/models"
)

func TestWebhookAuthenticatesPathAndReplaysSafely(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "webhook", "12345")
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	var dispatches atomic.Int32
	dispatcher := &dispatchStub{stream: func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		dispatches.Add(1)
		return eventStream(gateway.DispatchEvent{Type: gateway.DispatchEventMessage, Text: "ok"}, gateway.DispatchEvent{Type: gateway.DispatchEventDone, Done: true}), nil
	}}
	adapter := newTestAdapter(t, target, dispatcher, client)
	defer adapter.Close()
	webhook, err := NewWebhook(adapter, WebhookConfig{Path: "/telegram/hook", SecretToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer webhook.Close()
	body, _ := json.Marshal(models.Update{ID: 99, Message: &models.Message{ID: 1, Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}, From: &models.User{ID: 42}, Text: "hello"}})
	request := httptest.NewRequest(http.MethodPost, "http://example.test/telegram/hook", bytesReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "secret")
	response := httptest.NewRecorder()
	webhook.ServeHTTP(response, request)
	if response.Code != http.StatusOK || dispatches.Load() != 1 {
		t.Fatalf("first webhook response=%d dispatches=%d", response.Code, dispatches.Load())
	}
	request = httptest.NewRequest(http.MethodPost, "http://example.test/telegram/hook", bytesReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "secret")
	response = httptest.NewRecorder()
	webhook.ServeHTTP(response, request)
	if response.Code != http.StatusOK || dispatches.Load() != 1 {
		t.Fatalf("replay response=%d dispatches=%d", response.Code, dispatches.Load())
	}
	bad := httptest.NewRecorder()
	webhook.ServeHTTP(bad, httptest.NewRequest(http.MethodPost, "http://example.test/other", bytesReader(body)))
	if bad.Code != http.StatusNotFound {
		t.Fatalf("wrong path status=%d", bad.Code)
	}
}

func bytesReader(value []byte) *bytes.Reader { return bytes.NewReader(value) }

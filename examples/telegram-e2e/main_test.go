package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

func TestLoadConfigUsesSafeDefaults(t *testing.T) {
	values := map[string]string{"TELEGRAM_BOT_TOKEN": "receiver-token"}
	configuration, err := loadConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if configuration.botToken != values["TELEGRAM_BOT_TOKEN"] || configuration.senderBotToken != "" {
		t.Fatalf("unexpected token configuration: %+v", configuration)
	}
	if configuration.testMessage == "" || !strings.HasPrefix(configuration.testMessage, "telegram-e2e-") {
		t.Fatalf("generated marker = %q", configuration.testMessage)
	}
	if configuration.runTimeout != defaultRunTimeout || configuration.pollTimeout != defaultPollTimeout {
		t.Fatalf("unexpected defaults: %+v", configuration)
	}
	if configuration.deleteWebhook || configuration.dropPendingUpdate {
		t.Fatalf("destructive webhook defaults must be false: %+v", configuration)
	}
}

func TestLoadConfigRejectsSecretBearingOrUnsafeValuesWithoutEchoingThem(t *testing.T) {
	tests := []map[string]string{
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_SENDER_BOT_TOKEN": "receiver-token"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_TEST_MESSAGE": "contains-receiver-token"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_TIMEOUT": "not-a-duration"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_POLL_TIMEOUT": "1s"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_DELETE_WEBHOOK": "not-a-bool"},
	}
	for _, values := range tests {
		_, err := loadConfig(func(name string) string { return values[name] })
		if !errors.Is(err, errConfiguration) {
			t.Fatalf("values=%v error=%v, want errConfiguration", values, err)
		}
		for _, value := range values {
			if value != "" && strings.Contains(err.Error(), value) {
				t.Fatalf("error %q echoed configured value %q", err, value)
			}
		}
	}
}

func TestLoadConfigParsesExplicitSettings(t *testing.T) {
	values := map[string]string{
		"TELEGRAM_BOT_TOKEN":            "receiver-token",
		"TELEGRAM_SENDER_BOT_TOKEN":     "sender-token",
		"TELEGRAM_TEST_MESSAGE":         "telegram-e2e-marker",
		"TELEGRAM_TIMEOUT":              "45s",
		"TELEGRAM_POLL_TIMEOUT":         "3s",
		"TELEGRAM_DELETE_WEBHOOK":       "true",
		"TELEGRAM_DROP_PENDING_UPDATES": "true",
	}
	configuration, err := loadConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if configuration.testMessage != values["TELEGRAM_TEST_MESSAGE"] || configuration.runTimeout != 45*time.Second || configuration.pollTimeout != 3*time.Second || !configuration.deleteWebhook || !configuration.dropPendingUpdate {
		t.Fatalf("explicit settings were not parsed: %+v", configuration)
	}
}

func TestPrepareLongPollingHandlesWebhookSafely(t *testing.T) {
	noWebhook := &fakeWebhookClient{}
	if err := prepareLongPolling(context.Background(), noWebhook, false, false); err != nil {
		t.Fatal(err)
	}
	if noWebhook.deleted {
		t.Fatal("did not expect DeleteWebhook without a webhook")
	}

	configured := &fakeWebhookClient{info: &models.WebhookInfo{URL: "https://example.test/telegram"}}
	if err := prepareLongPolling(context.Background(), configured, false, false); !errors.Is(err, errWebhookConfigured) {
		t.Fatalf("configured webhook error = %v", err)
	}
	if configured.deleted {
		t.Fatal("must not delete webhook without explicit permission")
	}

	configured = &fakeWebhookClient{info: &models.WebhookInfo{URL: "https://example.test/telegram"}}
	if err := prepareLongPolling(context.Background(), configured, true, true); err != nil {
		t.Fatal(err)
	}
	if !configured.deleted || !configured.dropPending {
		t.Fatalf("DeleteWebhook options were not preserved: %+v", configured)
	}
}

func TestNewTrustedTargetUsesTheTrustedBoundary(t *testing.T) {
	target, err := newTrustedTarget("123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Validate(); err != nil {
		t.Fatal(err)
	}
	if target.Channel != channels.ChannelTelegram || target.ProviderAccountID != "123456789" {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestNewTrustedTargetRejectsNonCanonicalAccountID(t *testing.T) {
	for _, value := range []string{"", "0", "+123456789", "0123456789", "bot"} {
		if _, err := newTrustedTarget(value); !errors.Is(err, errConfiguration) {
			t.Fatalf("provider account %q error = %v", value, err)
		}
	}
}

func TestDeterministicDispatcherEmitsCompleteReplyAndMarksInput(t *testing.T) {
	dispatcher := newDeterministicDispatcher("marker")
	stream, err := dispatcher.Dispatch(context.Background(), gateway.DispatchRequest{
		Message: gateway.InboundMessage{Content: "marker"}, RequestID: "request-id",
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []gateway.DispatchEvent
	for event := range stream {
		events = append(events, event)
	}
	if len(events) != 2 || events[0].Type != gateway.DispatchEventMessage || events[0].Text != e2eReply || !events[1].Done {
		t.Fatalf("unexpected dispatch events: %+v", events)
	}
	select {
	case message := <-dispatcher.seen:
		if message.Content != "marker" {
			t.Fatalf("seen message = %+v", message)
		}
	default:
		t.Fatal("dispatcher did not mark the expected message")
	}
}

type fakeWebhookClient struct {
	info        *models.WebhookInfo
	deleted     bool
	dropPending bool
}

func (client *fakeWebhookClient) GetWebhookInfo(context.Context) (*models.WebhookInfo, error) {
	return client.info, nil
}

func (client *fakeWebhookClient) DeleteWebhook(_ context.Context, params *bot.DeleteWebhookParams) (bool, error) {
	client.deleted = true
	client.dropPending = params != nil && params.DropPendingUpdates
	return true, nil
}

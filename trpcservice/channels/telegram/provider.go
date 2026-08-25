package telegram

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/go-telegram/bot"
)

// Provider delivers durable outbox segments through one trusted Telegram
// destination. The stable outbox key is also used as the local provider
// idempotency key; Telegram itself remains at-least-once unless it supports a
// matching external idempotency facility.
type Provider struct {
	client   BotClient
	chatID   int64
	threadID int
	mu       sync.Mutex
	receipts map[string]string
}

// NewProvider creates a Telegram reply provider for a chat and optional thread.
func NewProvider(client BotClient, chatID int64, threadID int) (*Provider, error) {
	if client == nil || chatID == 0 || threadID < 0 {
		return nil, outbox.ErrInvalid
	}
	return &Provider{client: client, chatID: chatID, threadID: threadID, receipts: map[string]string{}}, nil
}

// Deliver sends one durable reply segment and returns the provider message ID.
func (p *Provider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	if p == nil || p.client == nil || ctx == nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	key := deliveryKey(value)
	p.mu.Lock()
	if receipt := p.receipts[key]; receipt != "" {
		p.mu.Unlock()
		return receipt, nil
	}
	p.mu.Unlock()
	message, err := p.client.SendMessage(ctx, &bot.SendMessageParams{ChatID: p.chatID, MessageThreadID: p.threadID, Text: value.Payload})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return "", &outbox.DeliveryError{Class: "canceled", Retryable: true}
		}
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	if message == nil || message.ID <= 0 {
		return "", &outbox.DeliveryError{Class: "provider_invalid_receipt", Retryable: false}
	}
	receipt := strconv.Itoa(message.ID)
	p.mu.Lock()
	p.receipts[key] = receipt
	p.mu.Unlock()
	return receipt, nil
}

// Reconcile checks whether a previously attempted segment can be confirmed.
func (p *Provider) Reconcile(_ context.Context, value runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if p == nil {
		return outbox.DeliveryUnknown, "", nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if receipt := p.receipts[deliveryKey(value)]; receipt != "" {
		return outbox.DeliveryAccepted, receipt, nil
	}
	return outbox.DeliveryUnknown, "", nil
}

func deliveryKey(value runtimestorage.ReplyOutbox) string {
	return value.TenantID + "\x00" + value.ReplyID + "\x00" + strconv.Itoa(value.SegmentIndex)
}

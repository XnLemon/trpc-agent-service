package telegram

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

// Provider delivers durable outbox segments through one trusted Telegram
// destination. The stable outbox key is also used as the local provider
// idempotency key; Telegram itself remains at-least-once unless it supports a
// matching external idempotency facility.
type Provider struct {
	client      BotClient
	chatID      int64
	threadID    int
	attachments attachment.Reader
	tenantID    string
	bindingID   string
	dynamic     bool
	mu          sync.Mutex
	receipts    map[string]string
	inflight    map[string]*deliveryCall
}

type deliveryCall struct {
	done    chan struct{}
	receipt string
	err     error
}

type telegramPhotoSender interface {
	SendPhoto(context.Context, *bot.SendPhotoParams) (*models.Message, error)
}

type telegramDocumentSender interface {
	SendDocument(context.Context, *bot.SendDocumentParams) (*models.Message, error)
}

// ProviderOption configures optional Telegram reply-provider dependencies.
type ProviderOption func(*Provider)

// WithAttachmentReader enables native media delivery from durable attachment
// references. Nil readers are ignored and keep the text-only fallback path.
func WithAttachmentReader(reader attachment.Reader) ProviderOption {
	return func(provider *Provider) {
		if reader != nil {
			provider.attachments = reader
		}
	}
}

// NewProvider creates a Telegram reply provider for a chat and optional thread.
func NewProvider(client BotClient, chatID int64, threadID int, options ...ProviderOption) (*Provider, error) {
	if client == nil || chatID == 0 || threadID < 0 {
		return nil, outbox.ErrInvalid
	}
	provider := &Provider{client: client, chatID: chatID, threadID: threadID, receipts: map[string]string{}, inflight: map[string]*deliveryCall{}}
	for _, option := range options {
		if option != nil {
			option(provider)
		}
	}
	return provider, nil
}

// BindingProvider routes durable replies to one of the Telegram adapters
// created from trusted startup configuration. Chat and thread IDs are read
// only from the durable ReplyTarget; callers cannot select a Bot or token.
type BindingProvider struct {
	mu        sync.RWMutex
	providers map[string]*Provider
}

var _ outbox.Provider = (*BindingProvider)(nil)

// NewBindingProvider registers one or more already-authenticated Telegram
// adapters. The adapters retain client and attachment ownership; this provider
// only owns binding selection and process-local receipt state.
func NewBindingProvider(adapters ...*Adapter) (*BindingProvider, error) {
	if len(adapters) == 0 {
		return nil, outbox.ErrInvalid
	}
	providers := make(map[string]*Provider, len(adapters))
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, outbox.ErrInvalid
		}
		adapter.mu.RLock()
		client, target, attachments := adapter.client, adapter.target, adapter.attachments
		adapter.mu.RUnlock()
		if client == nil || target.Channel != channels.ChannelTelegram || target.BindingID == "" || target.TenantID == "" || target.Validate() != nil {
			return nil, outbox.ErrInvalid
		}
		if _, exists := providers[target.BindingID]; exists {
			return nil, outbox.ErrInvalid
		}
		providers[target.BindingID] = &Provider{
			client: client, attachments: attachments, tenantID: target.TenantID,
			bindingID: target.BindingID, dynamic: true, receipts: map[string]string{}, inflight: map[string]*deliveryCall{},
		}
	}
	return &BindingProvider{providers: providers}, nil
}

// Deliver routes a durable reply through the provider selected by BindingID.
func (p *BindingProvider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	provider, err := p.provider(value)
	if err != nil {
		return "", err
	}
	return provider.Deliver(ctx, value)
}

// Reconcile returns the process-local receipt for the selected binding.
func (p *BindingProvider) Reconcile(ctx context.Context, value runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	provider, err := p.provider(value)
	if err != nil {
		return outbox.DeliveryUnknown, "", err
	}
	return provider.Reconcile(ctx, value)
}

func (p *BindingProvider) provider(value runtimestorage.ReplyOutbox) (*Provider, error) {
	if p == nil || value.ReplyTarget.BindingID == "" {
		return nil, invalidDelivery()
	}
	p.mu.RLock()
	provider := p.providers[value.ReplyTarget.BindingID]
	p.mu.RUnlock()
	if provider == nil {
		return nil, invalidDelivery()
	}
	return provider, nil
}

// Deliver sends one durable reply segment and returns the provider message ID.
func (p *Provider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	if p == nil || p.client == nil || ctx == nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	if _, _, err := p.destination(value); err != nil {
		return "", err
	}
	key := deliveryKey(value)
	p.mu.Lock()
	if receipt := p.receipts[key]; receipt != "" {
		p.mu.Unlock()
		return receipt, nil
	}
	if call := p.inflight[key]; call != nil {
		p.mu.Unlock()
		select {
		case <-call.done:
			return call.receipt, call.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	call := &deliveryCall{done: make(chan struct{})}
	p.inflight[key] = call
	p.mu.Unlock()
	message, err := p.deliverMessage(ctx, value)
	if err == nil && (message == nil || message.ID <= 0) {
		err = &outbox.DeliveryError{Class: "provider_invalid_receipt", Retryable: false}
	}
	receipt := ""
	if err == nil {
		receipt = strconv.Itoa(message.ID)
	}
	p.mu.Lock()
	if err == nil {
		p.receipts[key] = receipt
	}
	call.receipt, call.err = receipt, err
	delete(p.inflight, key)
	close(call.done)
	p.mu.Unlock()
	return receipt, err
}

func (p *Provider) deliverMessage(ctx context.Context, value runtimestorage.ReplyOutbox) (*models.Message, error) {
	chatID, threadID, err := p.destination(value)
	if err != nil {
		return nil, err
	}
	if value.Kind == "" || value.Kind == runtimestorage.ReplyKindText {
		return p.sendText(ctx, chatID, threadID, value.Payload)
	}
	normalized, err := runtimestorage.NormalizeReplyOutbox(value)
	if err != nil {
		return nil, &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	switch normalized.Kind {
	case runtimestorage.ReplyKindImage:
		return p.sendPhoto(ctx, chatID, threadID, normalized)
	case runtimestorage.ReplyKindDocument:
		return p.sendDocument(ctx, chatID, threadID, normalized)
	default:
		return p.sendText(ctx, chatID, threadID, normalized.Fallback)
	}
}

func (p *Provider) sendPhoto(ctx context.Context, chatID int64, threadID int, value runtimestorage.ReplyOutbox) (*models.Message, error) {
	sender, ok := p.client.(telegramPhotoSender)
	if !ok || p.attachments == nil {
		return p.sendText(ctx, chatID, threadID, value.Fallback)
	}
	file, ok, err := p.attachmentUpload(ctx, value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return p.sendText(ctx, chatID, threadID, value.Fallback)
	}
	message, err := sender.SendPhoto(ctx, &bot.SendPhotoParams{ChatID: chatID, MessageThreadID: threadID, Photo: file, Caption: value.Payload})
	if err != nil {
		return nil, telegramDeliveryError(ctx, err)
	}
	return message, nil
}

func (p *Provider) sendDocument(ctx context.Context, chatID int64, threadID int, value runtimestorage.ReplyOutbox) (*models.Message, error) {
	sender, ok := p.client.(telegramDocumentSender)
	if !ok || p.attachments == nil {
		return p.sendText(ctx, chatID, threadID, value.Fallback)
	}
	file, ok, err := p.attachmentUpload(ctx, value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return p.sendText(ctx, chatID, threadID, value.Fallback)
	}
	message, err := sender.SendDocument(ctx, &bot.SendDocumentParams{ChatID: chatID, MessageThreadID: threadID, Document: file, Caption: value.Payload})
	if err != nil {
		return nil, telegramDeliveryError(ctx, err)
	}
	return message, nil
}

func (p *Provider) attachmentUpload(ctx context.Context, value runtimestorage.ReplyOutbox) (*models.InputFileUpload, bool, error) {
	content, err := p.attachments.Load(ctx, value.TenantID, value.EventID, value.Attachment)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return nil, false, &outbox.DeliveryError{Class: "canceled", Retryable: true}
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, false, &outbox.DeliveryError{Class: "timeout", Retryable: true}
		}
		return nil, false, nil
	}
	if err := content.Validate(value.Attachment); err != nil {
		return nil, false, nil
	}
	name := value.Attachment.Name
	if name == "" {
		name = value.Attachment.ID
	}
	return &models.InputFileUpload{Filename: name, Data: bytes.NewReader(content.Data)}, true, nil
}

func (p *Provider) sendText(ctx context.Context, chatID int64, threadID int, text string) (*models.Message, error) {
	message, err := p.client.SendMessage(ctx, &bot.SendMessageParams{ChatID: chatID, MessageThreadID: threadID, Text: text})
	if err != nil {
		return nil, telegramDeliveryError(ctx, err)
	}
	return message, nil
}

func telegramDeliveryError(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return &outbox.DeliveryError{Class: "canceled", Retryable: true}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &outbox.DeliveryError{Class: "timeout", Retryable: true}
	}
	return &outbox.DeliveryError{Class: "provider_error", Retryable: true}
}

// Reconcile checks whether a previously attempted segment can be confirmed.
func (p *Provider) Reconcile(_ context.Context, value runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if p == nil {
		return outbox.DeliveryUnknown, "", nil
	}
	if _, _, err := p.destination(value); err != nil {
		return outbox.DeliveryUnknown, "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if receipt := p.receipts[deliveryKey(value)]; receipt != "" {
		return outbox.DeliveryAccepted, receipt, nil
	}
	return outbox.DeliveryUnknown, "", nil
}

func (p *Provider) destination(value runtimestorage.ReplyOutbox) (int64, int, error) {
	if p == nil || p.client == nil {
		return 0, 0, invalidDelivery()
	}
	if !p.dynamic {
		return p.chatID, p.threadID, nil
	}
	target := value.ReplyTarget
	if target.BindingID != p.bindingID || value.TenantID != p.tenantID || runtimestorage.ValidateReplyTarget(target) != nil {
		return 0, 0, invalidDelivery()
	}
	chatID, err := strconv.ParseInt(target.ReceiverID, 10, 64)
	if err != nil || chatID == 0 || strconv.FormatInt(chatID, 10) != target.ReceiverID {
		return 0, 0, invalidDelivery()
	}
	threadID := 0
	if target.ThreadID != "" {
		threadID, err = strconv.Atoi(target.ThreadID)
		if err != nil || threadID < 0 || strconv.Itoa(threadID) != target.ThreadID {
			return 0, 0, invalidDelivery()
		}
	}
	return chatID, threadID, nil
}

func invalidDelivery() error {
	return &outbox.DeliveryError{Class: "invalid", Retryable: false}
}

func deliveryKey(value runtimestorage.ReplyOutbox) string {
	return value.TenantID + "\x00" + value.ReplyID + "\x00" + strconv.Itoa(value.SegmentIndex)
}

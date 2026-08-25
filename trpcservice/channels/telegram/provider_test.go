package telegram

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

type providerBot struct {
	message *models.Message
	err     error
	params  *bot.SendMessageParams
	calls   int
}

func (b *providerBot) Start(context.Context) {}
func (b *providerBot) GetMe(context.Context) (*models.User, error) {
	return &models.User{ID: 1, IsBot: true}, nil
}
func (b *providerBot) SendMessage(_ context.Context, params *bot.SendMessageParams) (*models.Message, error) {
	b.calls++
	b.params = params
	return b.message, b.err
}

func TestProviderUsesStableReceiptAndReconcile(t *testing.T) {
	botClient := &providerBot{message: &models.Message{ID: 42}}
	provider, err := NewProvider(botClient, 99, 7)
	if err != nil {
		t.Fatal(err)
	}
	reply := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, Payload: "hello"}
	receipt, err := provider.Deliver(context.Background(), reply)
	if err != nil || receipt != "42" || botClient.calls != 1 {
		t.Fatalf("deliver = %q calls=%d err=%v", receipt, botClient.calls, err)
	}
	if _, err := provider.Deliver(context.Background(), reply); err != nil || botClient.calls != 1 {
		t.Fatalf("duplicate deliver calls=%d err=%v", botClient.calls, err)
	}
	status, reconciled, err := provider.Reconcile(context.Background(), reply)
	if err != nil || status != outbox.DeliveryAccepted || reconciled != "42" {
		t.Fatalf("reconcile = %s/%q err=%v", status, reconciled, err)
	}
	if botClient.params.ChatID != int64(99) || botClient.params.MessageThreadID != 7 || botClient.params.Text != "hello" {
		t.Fatalf("send params = %+v", botClient.params)
	}
}

func TestProviderRedactsAndClassifiesSendFailures(t *testing.T) {
	botClient := &providerBot{err: errors.New("secret provider response")}
	provider, err := NewProvider(botClient, 99, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, Payload: "hello"})
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "provider_error" || !deliveryErr.Retryable {
		t.Fatalf("delivery error = %#v", err)
	}
}

func TestProviderValidationAndReceiptFailureBranches(t *testing.T) {
	client := &providerBot{message: &models.Message{ID: 1}}
	for name, chatID := range map[string]int64{"zero chat": 0, "valid chat": 9} {
		if name == "valid chat" {
			if _, err := NewProvider(client, chatID, -1); !errors.Is(err, outbox.ErrInvalid) {
				t.Fatalf("negative thread = %v", err)
			}
			continue
		}
		if _, err := NewProvider(client, chatID, 0); !errors.Is(err, outbox.ErrInvalid) {
			t.Fatalf("zero chat = %v", err)
		}
	}
	if _, err := NewProvider(nil, 9, 0); !errors.Is(err, outbox.ErrInvalid) {
		t.Fatalf("nil client = %v", err)
	}
	value := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-invalid", SegmentIndex: 0, Payload: "payload"}
	for name, botValue := range map[string]*models.Message{"nil receipt": nil, "zero receipt": {ID: 0}} {
		t.Run(name, func(t *testing.T) {
			provider, err := NewProvider(&providerBot{message: botValue}, 9, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Deliver(context.Background(), value)
			var deliveryErr *outbox.DeliveryError
			if !errors.As(err, &deliveryErr) || deliveryErr.Class != "provider_invalid_receipt" || deliveryErr.Retryable {
				t.Fatalf("invalid receipt error = %#v", err)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	provider, err := NewProvider(&providerBot{err: context.Canceled}, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Deliver(canceled, value)
	var canceledErr *outbox.DeliveryError
	if !errors.As(err, &canceledErr) || canceledErr.Class != "canceled" || !canceledErr.Retryable {
		t.Fatalf("canceled delivery error = %#v", err)
	}
	var nilProvider *Provider
	if _, err := nilProvider.Deliver(context.Background(), value); err == nil {
		t.Fatal("nil provider deliver unexpectedly succeeded")
	}
	if status, _, err := nilProvider.Reconcile(context.Background(), value); err != nil || status != outbox.DeliveryUnknown {
		t.Fatalf("nil provider reconcile = %s/%v", status, err)
	}
	if _, err := provider.Deliver(nil, value); err == nil {
		t.Fatal("nil context deliver unexpectedly succeeded")
	}
}

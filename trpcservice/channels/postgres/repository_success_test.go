package postgres

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestChannelRepositoryGetDecodesStoredBinding(t *testing.T) {
	digest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "repository-success")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", BindingKey: "primary", Channel: channels.ChannelTelegram,
		ProviderAccountID: "account", PublicRouteKeyDigest: digest, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		SecretRef: "secret://tenant/channel", Status: channels.StatusActive,
		Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{APIBaseURL: "https://api.telegram.org", WebhookPath: "/inbound"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	protocol, err := encodeProtocol(binding.Protocol)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "binding_id", "binding_key", "channel", "provider_account_id", "public_route_key_digest", "app_id", "secret_ref",
		"protocol_config", "schema_version", "status", "version", "config_digest", "created_at", "updated_at",
	}).AddRow(
		binding.TenantID, binding.BindingID, binding.BindingKey, string(binding.Channel), binding.ProviderAccountID, binding.PublicRouteKeyDigest,
		binding.AppID, binding.SecretRef, protocol, channels.SchemaVersionV1, string(binding.Status), binding.Version, binding.ConfigDigest,
		binding.CreatedAt, binding.UpdatedAt,
	))

	stored, err := NewRepository(db).Get(context.Background(), binding.TenantID, binding.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.BindingID != binding.BindingID || stored.Protocol.Telegram == nil || stored.Protocol.Telegram.WebhookPath != "/inbound" {
		t.Fatalf("stored channel binding = %+v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

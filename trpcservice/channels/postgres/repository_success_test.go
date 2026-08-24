package postgres

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestChannelRepositoryGetDecodesStoredBinding(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))

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

func TestChannelRepositoryCandidateLookupAndConsumption(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository := NewRepository(db)
	mock.ExpectQuery(".*").WithArgs(string(binding.Channel), binding.PublicRouteKeyDigest).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "binding_id", "version", "config_digest",
	}).AddRow(binding.TenantID, binding.BindingID, binding.Version, binding.ConfigDigest))

	candidates, err := repository.LookupCandidates(context.Background(), binding.Channel, binding.PublicRouteKeyDigest)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].CandidateToken == "" || candidates[0].BindingVersion != binding.Version {
		t.Fatalf("candidate contexts = %+v", candidates)
	}
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))
	consumed, err := repository.ConsumeCandidate(context.Background(), candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	if consumed.BindingID != binding.BindingID || consumed.ConfigDigest != binding.ConfigDigest {
		t.Fatalf("consumed binding = %+v", consumed)
	}
	if _, err := repository.ConsumeCandidate(context.Background(), candidates[0]); err != channels.ErrCandidateUnavailable {
		t.Fatalf("reused candidate error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func newStoredChannelBinding(t *testing.T) *channels.Binding {
	t.Helper()
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
	return binding
}

func testChannelBindingRows(t *testing.T, binding *channels.Binding) *sqlmock.Rows {
	t.Helper()
	protocol, err := encodeProtocol(binding.Protocol)
	if err != nil {
		t.Fatal(err)
	}
	return sqlmock.NewRows([]string{
		"tenant_id", "binding_id", "binding_key", "channel", "provider_account_id", "public_route_key_digest", "app_id", "secret_ref",
		"protocol_config", "schema_version", "status", "version", "config_digest", "created_at", "updated_at",
	}).AddRow(
		binding.TenantID, binding.BindingID, binding.BindingKey, string(binding.Channel), binding.ProviderAccountID, binding.PublicRouteKeyDigest,
		binding.AppID, binding.SecretRef, protocol, channels.SchemaVersionV1, string(binding.Status), binding.Version, binding.ConfigDigest,
		binding.CreatedAt, binding.UpdatedAt,
	)
}

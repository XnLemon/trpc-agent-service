package channels

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testTenantID = "t_00000000000000000000000000"
	testAppID    = "app_00000000000000000000000000"
)

func TestBindingDomainInvariantsAndDefensiveConfiguration(t *testing.T) {
	routeDigest, err := DigestPublicRouteKey(ChannelWeCom, "shared-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := NewBinding(CreateInput{
		TenantID: testTenantID, BindingKey: " Support-Channel ", Channel: ChannelWeCom,
		ProviderAccountID: " corp-1 ", PublicRouteKeyDigest: routeDigest, AppID: testAppID,
		SecretRef: "secret/wecom", Protocol: ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{
			CorpID: " corp-id ", ReceiveID: " receive-id ",
		}}, Metadata: validChangeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if binding.BindingKey != "support-channel" || binding.ProviderAccountID != "corp-1" || binding.Protocol.WeCom.CorpID != "corp-id" {
		t.Fatalf("input was not normalized: %+v", binding)
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(binding.ConfigDigest, "secret-value") || strings.Contains(fmt.Sprintf("%+v", binding), "secret-value") {
		t.Fatal("secret value appeared in the binding or config digest")
	}

	clone := binding.Clone()
	clone.Protocol.WeCom.CorpID = "changed"
	if binding.Protocol.WeCom.CorpID == "changed" {
		t.Fatal("binding clone leaked protocol pointer state")
	}

	encoded, err := json.Marshal(CandidateBindingContext{
		Channel: ChannelWeCom, PublicRouteKeyDigest: routeDigest, BindingVersion: binding.Version,
		ConfigDigest: binding.ConfigDigest, Purpose: PurposeWebhookVerification, CandidateToken: "opaque",
		IssuedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedText := string(encoded)
	for _, forbidden := range []string{testTenantID, testAppID, binding.SecretRef, "secret-value"} {
		if strings.Contains(encodedText, forbidden) {
			t.Fatalf("candidate context leaked %q: %s", forbidden, encodedText)
		}
	}
	if _, exists := reflectField(CandidateBindingContext{}, "TenantID"); exists {
		t.Fatal("candidate context contains a tenant identity field")
	}
}

func TestBindingRejectsUnknownChannelAndProtocolCrossing(t *testing.T) {
	routeDigest, err := DigestPublicRouteKey(ChannelTelegram, "telegram-route")
	if err != nil {
		t.Fatal(err)
	}
	base := CreateInput{
		TenantID: testTenantID, BindingKey: "telegram", Channel: ChannelTelegram,
		ProviderAccountID: "bot-1", PublicRouteKeyDigest: routeDigest, AppID: testAppID,
		SecretRef: "secret/telegram", Protocol: ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{}},
	}
	if _, err := NewBinding(base); err != nil {
		t.Fatal(err)
	}

	unknown := base
	unknown.Channel = Channel("line")
	if _, err := NewBinding(unknown); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown channel was accepted: %v", err)
	}
	crossed := base
	crossed.Protocol = ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{CorpID: "wrong"}}
	if _, err := NewBinding(crossed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cross-channel protocol was accepted: %v", err)
	}
	badURL := base
	badURL.Protocol = ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{APIBaseURL: "http://localhost"}}
	if _, err := NewBinding(badURL); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-HTTPS Telegram API URL was accepted: %v", err)
	}
	var configuration ProtocolConfiguration
	if err := json.Unmarshal([]byte(`{"wecom":{"token":"must-not-be-stored"}}`), &configuration); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown or credential-shaped protocol field was accepted: %v", err)
	}
}

func TestRouteDigestSeparatesChannelAndNeverReturnsRouteKey(t *testing.T) {
	wecomDigest, err := DigestPublicRouteKey(ChannelWeCom, "same-public-key")
	if err != nil {
		t.Fatal(err)
	}
	telegramDigest, err := DigestPublicRouteKey(ChannelTelegram, "same-public-key")
	if err != nil {
		t.Fatal(err)
	}
	if wecomDigest == telegramDigest || len(wecomDigest) != 64 || ValidatePublicRouteKeyDigest(wecomDigest) != nil {
		t.Fatalf("channel route namespaces are not separated: %q %q", wecomDigest, telegramDigest)
	}
	if strings.Contains(wecomDigest, "same-public-key") || strings.Contains(telegramDigest, "same-public-key") {
		t.Fatal("route key was returned in its digest")
	}
}

func TestCandidateLifetimePurposeAndOpaqueHandleBoundaries(t *testing.T) {
	routeDigest, _ := DigestPublicRouteKey(ChannelWeCom, "route")
	configDigest := strings.Repeat("a", 64)
	now := time.Now().UTC()
	candidate, err := NewCandidateBindingContext(ChannelWeCom, routeDigest, 1, configDigest, PurposeWebhookVerification, "opaque-token", now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := candidate.Validate(now); err != nil {
		t.Fatal(err)
	}
	if err := candidate.Validate(now.Add(time.Second)); !errors.Is(err, ErrCandidateUnavailable) {
		t.Fatalf("expired candidate was accepted: %v", err)
	}
	if _, err := NewCandidateBindingContext(ChannelWeCom, routeDigest, 1, configDigest, VerificationPurpose(""), "opaque-token", now, now.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty candidate purpose was accepted: %v", err)
	}
	if _, err := NewCandidateBindingContext(ChannelWeCom, routeDigest, 1, configDigest, PurposeWebhookVerification, "opaque-token", now, now.Add(MaxCandidateLifetime+time.Nanosecond)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unbounded candidate lifetime was accepted: %v", err)
	}

	handle, err := NewScopedVerifierHandle("private-token", PurposeWebhookVerification, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if handle.Token() != "private-token" {
		t.Fatal("resolver could not read its own opaque handle")
	}
	if _, err := NewScopedVerifierHandle("private-token", VerificationPurpose(""), now.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty handle purpose was accepted: %v", err)
	}
}

func validChangeMetadata() ChangeMetadata {
	return ChangeMetadata{ActorType: "test", ActorID: "test-actor", Reason: "test change", CorrelationID: "corr-1"}
}

func reflectField(value any, name string) (any, bool) {
	field, ok := reflect.TypeOf(value).FieldByName(name)
	return field, ok
}

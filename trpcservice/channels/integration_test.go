package channels_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestFakeTrustedInboundRouteAndBindingAwareIdentity(t *testing.T) {
	root, snapshot, app := activeTenantApp(t, "trusted-route")
	repo := inmemory.NewInMemoryRepository()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "public-wecom-route")
	if err != nil {
		t.Fatal(err)
	}
	binding := createActiveBinding(t, repo, root.TenantID, app.AppID, "wecom", channels.ChannelWeCom, "corp-trusted", routeDigest, "secret/wecom")
	secret := "offline-fake-secret"
	resolver := inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{{TenantID: root.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("route discovery failed: candidates=%d err=%v", len(candidates), err)
	}
	request := fakeRequest("nonce-trusted", "message body")
	request.RouteHints = channels.UntrustedRouteHints{TenantID: "t_77777777777777777777777777", BindingID: "cb_77777777777777777777777777", AppID: "app_77777777777777777777777777"}
	request.Signature = inmemory.SignFakeRequest(secret, request)
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	if verified.TenantID != root.TenantID || verified.BindingID != binding.BindingID || verified.AppID != app.AppID || verified.BindingVersion != binding.Version || verified.Channel != binding.Channel || verified.ProviderAccountID != binding.ProviderAccountID {
		t.Fatalf("verified result was not fixed to the binding: %+v", verified)
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, app, verified)
	if err != nil {
		t.Fatal(err)
	}
	if target.TenantID != root.TenantID || target.AppID != app.AppID || target.BindingID != binding.BindingID {
		t.Fatalf("unexpected trusted target: %+v", target)
	}

	direct, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationDirect, ExternalPeerID: "peer-1"})
	if err != nil {
		t.Fatal(err)
	}
	directAgain, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationDirect, ExternalPeerID: "peer-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(direct, directAgain) {
		t.Fatal("same identity input was not stable")
	}
	group, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationGroup, ExternalChatID: "chat-1"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationGroup, ExternalChatID: "chat-1", ExternalThreadID: "topic-7"})
	if err != nil {
		t.Fatal(err)
	}
	if direct.SessionID == group.SessionID || group.SessionID == thread.SessionID || direct.UserID == "" || direct.SessionID == "" {
		t.Fatal("direct, group, and thread identities were not separated")
	}

	otherRoute, _ := channels.DigestPublicRouteKey(channels.ChannelTelegram, "public-telegram-route")
	otherBinding := createActiveBinding(t, repo, root.TenantID, app.AppID, "telegram", channels.ChannelTelegram, "bot-trusted", otherRoute, "secret/telegram")
	otherSecret := "offline-telegram-secret"
	resolver = inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{
		{TenantID: root.TenantID, SecretRef: binding.SecretRef}:      secret,
		{TenantID: root.TenantID, SecretRef: otherBinding.SecretRef}: otherSecret,
	})
	otherVerified := resolveOne(t, repo, resolver, channels.ChannelTelegram, otherRoute, otherSecret, "nonce-other")
	otherTarget, err := channels.NewRoutingTarget(snapshot, otherBinding, app, otherVerified)
	if err != nil {
		t.Fatal(err)
	}
	otherIdentity, err := otherTarget.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationDirect, ExternalPeerID: "peer-1"})
	if err != nil {
		t.Fatal(err)
	}
	if direct.UserID == otherIdentity.UserID || direct.SessionID == otherIdentity.SessionID {
		t.Fatal("different channel bindings collided in Runner identity")
	}

	suspendedApp := app.Clone()
	suspendedApp.Status = agent.StatusSuspended
	if _, err := channels.NewRoutingTarget(snapshot, binding, &suspendedApp, verified); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("suspended App was accepted by trusted target: %v", err)
	}
	disabledBinding := binding.Clone()
	disabledBinding.Status = channels.StatusDisabled
	if _, err := channels.NewRoutingTarget(snapshot, &disabledBinding, app, verified); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("disabled Binding was accepted by trusted target: %v", err)
	}
	if _, err := channels.NewRoutingTarget(tenant.ConfigurationSnapshot{}, binding, app, verified); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("zero/non-active Tenant snapshot was accepted: %v", err)
	}
}

func TestFakeResolverRejectsPurposeMismatchBadProofExpiryAndReplay(t *testing.T) {
	clock := &integrationClock{now: time.Now().UTC().Add(time.Hour)}
	repo := inmemory.NewInMemoryRepository(inmemory.Options{Clock: clock.Now, CandidateTTL: time.Second})
	root, _, app := activeTenantApp(t, "fake-failures")
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "failure-route")
	binding := createActiveBinding(t, repo, root.TenantID, app.AppID, "wecom", channels.ChannelWeCom, "corp-failure", routeDigest, "secret/failure")
	secret := "failure-secret"
	resolver := inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{{TenantID: root.TenantID, SecretRef: binding.SecretRef}: secret}, inmemory.FakeResolverOptions{Clock: clock.Now})

	candidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.VerificationPurpose("wrong-purpose")}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("purpose mismatch was accepted: %v", err)
	}
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification}); err != nil {
		t.Fatal(err)
	}
	// The candidate is consumed above, so a second resolution is a replay.
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("candidate replay was accepted: %v", err)
	}

	badCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	badHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: badCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	badRequest := fakeRequestAt(clock.now, "nonce-bad", "bad proof")
	badRequest.Signature = inmemory.SignFakeRequest("wrong-secret", badRequest)
	if _, err := resolver.Verify(context.Background(), badHandle, badRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("bad signature was accepted: %v", err)
	}
	if _, err := resolver.Verify(context.Background(), badHandle, badRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("verifier handle was replayable: %v", err)
	}

	successCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	successHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: successCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	successRequest := fakeRequestAt(clock.now, "nonce-replay", "replay proof")
	successRequest.Signature = inmemory.SignFakeRequest(secret, successRequest)
	if _, err := resolver.Verify(context.Background(), successHandle, successRequest); err != nil {
		t.Fatal(err)
	}
	replayCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	replayHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: replayCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Verify(context.Background(), replayHandle, successRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nonce replay on a new candidate was accepted: %v", err)
	}

	expiredCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	expiredHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: expiredCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Second)
	expiredRequest := fakeRequestAt(clock.now, "nonce-expired", "expired proof")
	expiredRequest.Signature = inmemory.SignFakeRequest(secret, expiredRequest)
	if _, err := resolver.Verify(context.Background(), expiredHandle, expiredRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("expired verifier handle was accepted: %v", err)
	}
}

func TestFakeResolverKeepsTwoTenantCandidatesSeparated(t *testing.T) {
	clock := &integrationClock{now: time.Now().UTC().Add(time.Hour)}
	repo := inmemory.NewInMemoryRepository(inmemory.Options{Clock: clock.Now})
	firstRoot, firstSnapshot, firstApp := activeTenantApp(t, "same-route-one")
	secondRoot, secondSnapshot, secondApp := activeTenantApp(t, "same-route-two")
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "same-public-route")
	first := createActiveBinding(t, repo, firstRoot.TenantID, firstApp.AppID, "shared", channels.ChannelWeCom, "corp-one", routeDigest, "secret/first")
	second := createActiveBinding(t, repo, secondRoot.TenantID, secondApp.AppID, "shared", channels.ChannelWeCom, "corp-two", routeDigest, "secret/second")
	const sharedSecret = "same-offline-secret"
	resolver := inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{
		{TenantID: firstRoot.TenantID, SecretRef: first.SecretRef}:   sharedSecret,
		{TenantID: secondRoot.TenantID, SecretRef: second.SecretRef}: sharedSecret,
	}, inmemory.FakeResolverOptions{Clock: clock.Now})
	candidates, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("expected two candidates for the same public route: %d %v", len(candidates), err)
	}
	seen := map[string]bool{}
	for index, candidate := range candidates {
		request := fakeRequestAt(clock.now, "nonce-tenant-"+string(rune('a'+index)), "tenant proof")
		request.Signature = inmemory.SignFakeRequest(sharedSecret, request)
		handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
		if err != nil {
			t.Fatal(err)
		}
		verified, err := resolver.Verify(context.Background(), handle, request)
		if err != nil {
			t.Fatal(err)
		}
		seen[verified.TenantID] = true
		switch verified.TenantID {
		case firstRoot.TenantID:
			if _, err := channels.NewRoutingTarget(firstSnapshot, first, firstApp, verified); err != nil {
				t.Fatal(err)
			}
		case secondRoot.TenantID:
			if _, err := channels.NewRoutingTarget(secondSnapshot, second, secondApp, verified); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("candidate crossed tenant boundary: %+v", verified)
		}
	}
	if len(seen) != 2 || !seen[firstRoot.TenantID] || !seen[secondRoot.TenantID] {
		t.Fatalf("same-route candidates did not preserve both tenant scopes: %v", seen)
	}
}

type integrationClock struct {
	now time.Time
}

func (c *integrationClock) Now() time.Time { return c.now }

func activeTenantApp(t *testing.T, key string) (*tenant.Tenant, tenant.ConfigurationSnapshot, *agent.App) {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{TenantKey: key, DisplayName: "Integration Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	app, err := agent.NewApp(agent.CreateInput{TenantID: root.TenantID, AppKey: "support", DisplayName: "Support", Description: "offline integration"})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(1)
	app.Status = agent.StatusActive
	app.CurrentRevision = &revision
	app.Version = 2
	app.UpdatedAt = app.CreatedAt.Add(time.Second)
	if err := app.Validate(); err != nil {
		t.Fatal(err)
	}
	return root, snapshot, app
}

func createActiveBinding(t *testing.T, repo *inmemory.InMemoryRepository, tenantID, appID, key string, channel channels.Channel, providerAccountID, routeDigest, secretRef string) *channels.Binding {
	t.Helper()
	protocol := channels.ProtocolConfiguration{}
	if channel == channels.ChannelWeCom {
		protocol.WeCom = &channels.WeComProtocolConfiguration{CorpID: providerAccountID, ReceiveID: "receive"}
	} else {
		protocol.Telegram = &channels.TelegramProtocolConfiguration{WebhookPath: "/callback"}
	}
	binding, _, err := repo.Create(context.Background(), channels.CreateInput{TenantID: tenantID, BindingKey: key, Channel: channel, ProviderAccountID: providerAccountID, PublicRouteKeyDigest: routeDigest, AppID: appID, SecretRef: secretRef, Protocol: protocol, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	return active
}

func resolveOne(t *testing.T, repo *inmemory.InMemoryRepository, resolver *inmemory.FakeCandidateResolver, channel channels.Channel, routeDigest, secret, nonce string) channels.VerifiedBinding {
	t.Helper()
	candidate := oneCandidate(t, repo, channel, routeDigest)
	request := fakeRequest(nonce, "message")
	request.Signature = inmemory.SignFakeRequest(secret, request)
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	return verified
}

func oneCandidate(t *testing.T, repo *inmemory.InMemoryRepository, channel channels.Channel, routeDigest string) channels.CandidateBindingContext {
	t.Helper()
	candidates, err := repo.LookupCandidates(context.Background(), channel, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("expected one candidate, got %d: %v", len(candidates), err)
	}
	return candidates[0]
}

func fakeRequest(nonce, body string) channels.VerificationRequest {
	return fakeRequestAt(time.Now().UTC(), nonce, body)
}

func fakeRequestAt(timestamp time.Time, nonce, body string) channels.VerificationRequest {
	digest := sha256.Sum256([]byte(body))
	return channels.VerificationRequest{Purpose: channels.PurposeWebhookVerification, Timestamp: timestamp.UTC(), Nonce: nonce, MessageDigest: hex.EncodeToString(digest[:]), ReceiveID: "receive"}
}

func validMetadata() channels.ChangeMetadata {
	return channels.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "integration change", CorrelationID: "integration-correlation"}
}

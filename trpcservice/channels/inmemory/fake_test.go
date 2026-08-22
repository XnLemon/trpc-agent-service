package inmemory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestFakeResolverIsCoveredInsideItsOwningPackage(t *testing.T) {
	base := time.Now().UTC().Add(time.Hour)
	clock := &testClock{now: base}
	repo := NewRepository(Options{Clock: clock.Now, CandidateTTL: 2 * time.Second})
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "fake-package-route")
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000005", "fake-package", "corp-fake", routeDigest))
	binding, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	secret := "package-fake-secret"
	scope := channels.SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef}
	resolver := NewFakeResolver(repo, map[channels.SecretScope]string{scope: secret}, FakeResolverOptions{Clock: clock.Now, MaxClockSkew: time.Minute})
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}

	purposeMismatch := candidate
	purposeMismatch.Purpose = channels.VerificationPurpose("other-purpose")
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: purposeMismatch, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("candidate purpose mutation was accepted: %v", err)
	}
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	request := packageFakeRequest(base, "package-nonce", "package message")
	request.Signature = SignFakeRequest(secret, request)
	verified, err := resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	if verified.TenantID != binding.TenantID || verified.BindingID != binding.BindingID {
		t.Fatalf("fake resolver returned the wrong trusted scope: %+v", verified)
	}
	if _, err := resolver.Verify(context.Background(), handle, request); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("fake verifier handle was reusable: %v", err)
	}

	badCandidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	badHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: badCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	badRequest := packageFakeRequest(base, "bad-proof", "bad message")
	badRequest.Signature = SignFakeRequest("wrong-secret", badRequest)
	if _, err := resolver.Verify(context.Background(), badHandle, badRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("bad fake proof was accepted: %v", err)
	}

	successRequest := packageFakeRequest(base, "replayed-nonce", "replayed message")
	successRequest.Signature = SignFakeRequest(secret, successRequest)
	successHandle := resolvePackageHandle(t, repo, resolver, routeDigest)
	if _, err := resolver.Verify(context.Background(), successHandle, successRequest); err != nil {
		t.Fatal(err)
	}
	replayHandle := resolvePackageHandle(t, repo, resolver, routeDigest)
	if _, err := resolver.Verify(context.Background(), replayHandle, successRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nonce replay on a new candidate was accepted: %v", err)
	}

	invalidCases := []channels.VerificationRequest{
		packageFakeRequest(base.Add(10*time.Minute), "future", "future"),
		packageFakeRequest(base, "", "empty nonce"),
		packageFakeRequest(base, "bad-digest", "bad digest"),
		packageFakeRequest(base, "bad-signature", "bad signature"),
		packageFakeRequest(base, "control", "control receive"),
	}
	invalidCases[2].MessageDigest = "not-a-digest"
	invalidCases[3].Signature = ""
	invalidCases[4].ReceiveID = string(unicode.ReplacementChar) + "\n"
	for index, invalidRequest := range invalidCases {
		handle := resolvePackageHandle(t, repo, resolver, routeDigest)
		if index != 0 {
			invalidRequest.Timestamp = base
		}
		invalidRequest.Signature = SignFakeRequest(secret, invalidRequest)
		if index == 3 {
			invalidRequest.Signature = ""
		}
		if _, err := resolver.Verify(context.Background(), handle, invalidRequest); !errors.Is(err, channels.ErrVerificationFailed) {
			t.Fatalf("invalid fake request %d was accepted: %v", index, err)
		}
	}

	expiringHandle := resolvePackageHandle(t, repo, resolver, routeDigest)
	clock.now = base.Add(3 * time.Second)
	expiringRequest := packageFakeRequest(clock.now, "expired", "expired")
	expiringRequest.Signature = SignFakeRequest(secret, expiringRequest)
	if _, err := resolver.Verify(context.Background(), expiringHandle, expiringRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("expired fake handle was accepted: %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.ResolveCandidate(canceled, channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fake resolve returned %v", err)
	}
	if _, err := resolver.Verify(canceled, channels.ScopedVerifierHandle{}, channels.VerificationRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fake verify returned %v", err)
	}
	var nilResolver *FakeCandidateResolver
	if _, err := nilResolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nil fake resolver returned %v", err)
	}
	if _, err := nilResolver.Verify(context.Background(), channels.ScopedVerifierHandle{}, channels.VerificationRequest{}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nil fake verifier returned %v", err)
	}

	if !validFakeText("ok", 2) || validFakeText("", 2) || validFakeText("bad\n", 10) || validFakeText("long", 2) {
		t.Fatal("fake text validation boundaries are incorrect")
	}
	if !validLowerHexDigest(strings.Repeat("a", 64)) || validLowerHexDigest(strings.Repeat("A", 64)) || validLowerHexDigest("short") {
		t.Fatal("fake digest validation boundaries are incorrect")
	}
	if (FakeResolverOptions{}).MaxClockSkew != 0 {
		t.Fatal("zero fake options unexpectedly changed")
	}
	zeroClock := NewFakeCandidateResolver(repo, map[channels.SecretScope]string{}, FakeResolverOptions{Clock: func() time.Time { return time.Time{} }})
	if zeroClock.nowUTC().IsZero() {
		t.Fatal("zero fake clock was not replaced by wall clock")
	}
}

func resolvePackageHandle(t *testing.T, repo *InMemoryRepository, resolver *FakeCandidateResolver, routeDigest string) channels.ScopedVerifierHandle {
	t.Helper()
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func packageFakeRequest(timestamp time.Time, nonce, body string) channels.VerificationRequest {
	digest := sha256.Sum256([]byte(body))
	return channels.VerificationRequest{Purpose: channels.PurposeWebhookVerification, Timestamp: timestamp.UTC(), Nonce: nonce, MessageDigest: hex.EncodeToString(digest[:]), ReceiveID: "receive"}
}

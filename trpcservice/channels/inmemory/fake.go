package inmemory

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

// DefaultFakeMaxClockSkew is the timestamp window used by the offline fake.
// Production adapters must apply their provider-specific replay policy.
const DefaultFakeMaxClockSkew = 5 * time.Minute

// FakeResolverOptions configures the deterministic clock and timestamp window
// used by FakeCandidateResolver.
type FakeResolverOptions struct {
	Clock        func() time.Time
	MaxClockSkew time.Duration
}

type fakeVerifierState struct {
	binding   channels.Binding
	secret    string
	purpose   channels.VerificationPurpose
	expiresAt time.Time
}

// FakeCandidateResolver is an offline candidate-scoped resolver and verifier.
// It deliberately models the security boundary without implementing a real
// WeCom or Telegram protocol. Secrets remain private to this test double.
type FakeCandidateResolver struct {
	repo         *InMemoryRepository
	secrets      map[channels.SecretScope]string
	clock        func() time.Time
	maxClockSkew time.Duration

	mu         sync.Mutex
	handles    map[string]fakeVerifierState
	usedNonces map[string]time.Time
}

// NewFakeCandidateResolver creates an offline resolver backed by the concrete
// InMemory candidate consumer. The input secret map is copied and never
// returned by this package.
func NewFakeCandidateResolver(repo *InMemoryRepository, secrets map[channels.SecretScope]string, options ...FakeResolverOptions) *FakeCandidateResolver {
	configuration := FakeResolverOptions{
		Clock:        func() time.Time { return time.Now().UTC() },
		MaxClockSkew: DefaultFakeMaxClockSkew,
	}
	if len(options) > 0 {
		if options[0].Clock != nil {
			configuration.Clock = options[0].Clock
		}
		if options[0].MaxClockSkew > 0 {
			configuration.MaxClockSkew = options[0].MaxClockSkew
		}
	}
	secretCopy := make(map[channels.SecretScope]string, len(secrets))
	for scope, secret := range secrets {
		secretCopy[scope] = secret
	}
	return &FakeCandidateResolver{
		repo: repo, secrets: secretCopy, clock: configuration.Clock,
		maxClockSkew: configuration.MaxClockSkew, handles: make(map[string]fakeVerifierState),
		usedNonces: make(map[string]time.Time),
	}
}

// NewFakeResolver is a concise compatibility alias for the offline fake.
func NewFakeResolver(repo *InMemoryRepository, secrets map[channels.SecretScope]string, options ...FakeResolverOptions) *FakeCandidateResolver {
	return NewFakeCandidateResolver(repo, secrets, options...)
}

var _ channels.CandidateResolver = (*FakeCandidateResolver)(nil)
var _ channels.CandidateVerifier = (*FakeCandidateResolver)(nil)

// ResolveCandidate consumes a public candidate and mints one purpose-bound
// verifier handle. Tenant identity is obtained only from the consumed Binding;
// request fields never select it.
func (r *FakeCandidateResolver) ResolveCandidate(ctx context.Context, request channels.CandidateSecretRequest) (channels.ScopedVerifierHandle, error) {
	if err := checkContext(ctx); err != nil {
		return channels.ScopedVerifierHandle{}, err
	}
	if r == nil || r.repo == nil || request.Purpose != channels.PurposeWebhookVerification || request.Candidate.Purpose != request.Purpose {
		return channels.ScopedVerifierHandle{}, channels.ErrVerificationFailed
	}
	now := r.nowUTC()
	if err := request.Candidate.Validate(now); err != nil {
		return channels.ScopedVerifierHandle{}, channels.ErrVerificationFailed
	}
	binding, err := r.repo.ConsumeCandidate(ctx, request.Candidate)
	if err != nil {
		if channels.IsContextCancellation(err) {
			return channels.ScopedVerifierHandle{}, err
		}
		return channels.ScopedVerifierHandle{}, channels.ErrVerificationFailed
	}
	if err := checkContext(ctx); err != nil {
		return channels.ScopedVerifierHandle{}, err
	}
	scope := channels.SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef}
	if err := scope.Validate(); err != nil {
		return channels.ScopedVerifierHandle{}, channels.ErrVerificationFailed
	}
	secret, ok := r.secrets[scope]
	if !ok || secret == "" {
		return channels.ScopedVerifierHandle{}, channels.ErrVerificationFailed
	}
	token, err := newFakeHandleToken()
	if err != nil {
		return channels.ScopedVerifierHandle{}, channels.ErrVerificationFailed
	}
	handle, err := channels.NewScopedVerifierHandle(token, request.Purpose, request.Candidate.ExpiresAt)
	if err != nil {
		return channels.ScopedVerifierHandle{}, channels.ErrVerificationFailed
	}
	r.mu.Lock()
	r.handles[token] = fakeVerifierState{binding: binding.Clone(), secret: secret, purpose: request.Purpose, expiresAt: request.Candidate.ExpiresAt}
	r.mu.Unlock()
	return handle, nil
}

// Verify authenticates a fake HMAC proof, consumes the handle, rechecks the
// current active Binding version, and returns only trusted routing identity.
func (r *FakeCandidateResolver) Verify(ctx context.Context, handle channels.ScopedVerifierHandle, request channels.VerificationRequest) (channels.VerifiedBinding, error) {
	if err := checkContext(ctx); err != nil {
		return channels.VerifiedBinding{}, err
	}
	if r == nil || r.repo == nil || handle.Token() == "" {
		return channels.VerifiedBinding{}, channels.ErrVerificationFailed
	}
	now := r.nowUTC()
	r.mu.Lock()
	state, exists := r.handles[handle.Token()]
	delete(r.handles, handle.Token())
	r.mu.Unlock()
	if !exists || state.purpose != handle.Purpose || !state.expiresAt.Equal(handle.ExpiresAt) || handle.Purpose != request.Purpose || !now.Before(handle.ExpiresAt) {
		return channels.VerifiedBinding{}, channels.ErrVerificationFailed
	}
	if !validFakeVerificationRequest(request, now, r.maxClockSkew) {
		return channels.VerifiedBinding{}, channels.ErrVerificationFailed
	}
	current, err := r.repo.Get(ctx, state.binding.TenantID, state.binding.BindingID)
	if err != nil {
		if channels.IsContextCancellation(err) {
			return channels.VerifiedBinding{}, err
		}
		return channels.VerifiedBinding{}, channels.ErrVerificationFailed
	}
	if current.Status != channels.StatusActive || current.Version != state.binding.Version || current.ConfigDigest != state.binding.ConfigDigest || current.Channel != state.binding.Channel || current.ProviderAccountID != state.binding.ProviderAccountID {
		return channels.VerifiedBinding{}, channels.ErrVerificationFailed
	}
	expected := SignFakeRequest(state.secret, request)
	if !hmac.Equal([]byte(expected), []byte(request.Signature)) {
		return channels.VerifiedBinding{}, channels.ErrVerificationFailed
	}
	nonceKey := state.binding.TenantID + "\x00" + state.binding.BindingID + "\x00" + request.Nonce
	r.mu.Lock()
	for key, expiry := range r.usedNonces {
		if !now.Before(expiry) {
			delete(r.usedNonces, key)
		}
	}
	if _, replayed := r.usedNonces[nonceKey]; replayed {
		r.mu.Unlock()
		return channels.VerifiedBinding{}, channels.ErrVerificationFailed
	}
	r.usedNonces[nonceKey] = state.expiresAt
	r.mu.Unlock()
	return channels.NewVerifiedBinding(*current)
}

// SignFakeRequest creates the deterministic HMAC proof accepted by the fake.
// RouteHints are intentionally excluded: they are untrusted and changing them
// must never change the verified Binding selected by the resolver.
func SignFakeRequest(secret string, request channels.VerificationRequest) string {
	mac := hmac.New(sha256.New, []byte(secret))
	writeFakePart(mac, string(request.Purpose))
	writeFakePart(mac, request.Timestamp.UTC().Format(time.RFC3339Nano))
	writeFakePart(mac, request.Nonce)
	writeFakePart(mac, request.MessageDigest)
	writeFakePart(mac, request.ReceiveID)
	return hex.EncodeToString(mac.Sum(nil))
}

func validFakeVerificationRequest(request channels.VerificationRequest, now time.Time, maxClockSkew time.Duration) bool {
	if request.Purpose != channels.PurposeWebhookVerification || request.Timestamp.IsZero() || request.Timestamp.Location() != time.UTC || maxClockSkew <= 0 {
		return false
	}
	if request.Timestamp.Before(now.Add(-maxClockSkew)) || request.Timestamp.After(now.Add(maxClockSkew)) {
		return false
	}
	if !validFakeText(request.Nonce, 256) || !validLowerHexDigest(request.MessageDigest) || !validFakeText(request.Signature, 128) {
		return false
	}
	return request.ReceiveID == "" || validFakeText(request.ReceiveID, 256)
}

func validFakeText(value string, maxLength int) bool {
	return value != "" && len([]rune(value)) <= maxLength && !strings.ContainsFunc(value, unicode.IsControl)
}

func validLowerHexDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func writeFakePart(builder interface{ Write([]byte) (int, error) }, value string) {
	_, _ = builder.Write([]byte(strconv.Itoa(len([]byte(value)))))
	_, _ = builder.Write([]byte{':'})
	_, _ = builder.Write([]byte(value))
}

func newFakeHandleToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func (r *FakeCandidateResolver) nowUTC() time.Time {
	now := r.clock()
	if now.IsZero() {
		now = time.Now()
	}
	return now.UTC()
}
